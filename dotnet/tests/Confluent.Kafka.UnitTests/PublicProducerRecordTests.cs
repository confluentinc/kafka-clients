// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Java.Lang;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="ProducerRecord{TKey, TValue}"/>'s constructor checks (a null topic, a negative timestamp, a negative
/// partition, a null header, in Java's order and with Java's messages), its properties and headers, and Java's
/// equality, hash and string form: Java's <c>ProducerRecordTest</c> and Python's <c>test_producer_types.py</c>, as
/// far as .NET has the constructor forms.
/// </summary>
public sealed class PublicProducerRecordTests
{
    private const string Topic = "record-topic";
    private const string NullTopic = "Topic cannot be null.";
    private const string NegativeTimestamp = "Invalid timestamp: -1. Timestamp should always be non-negative or null.";
    private const string NegativePartition =
        "Invalid partition: -1. Partition number should always be non-negative or null.";

    [Fact]
    public void Ctor_NullTopic_ThrowsIllegalArgumentWithJavasMessage()
    {
        IllegalArgumentException ex = Assert.Throws<IllegalArgumentException>(
            () => new ProducerRecord<byte[], byte[]>(null!, Encoding.UTF8.GetBytes("v")));
        Assert.Equal(NullTopic, ex.Message);
    }

    [Fact]
    public void Ctor_NegativePartition_ThrowsIllegalArgumentWithJavasMessage()
    {
        IllegalArgumentException ex = Assert.Throws<IllegalArgumentException>(
            () => new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: -1));
        Assert.Equal(NegativePartition, ex.Message);
    }

    [Fact]
    public void InvalidRecords()
    {
        IllegalArgumentException nullTopic = Assert.Throws<IllegalArgumentException>(
            () => new ProducerRecord<string, int>(null!, 1, "key", partition: 0));
        Assert.Equal(NullTopic, nullTopic.Message);

        IllegalArgumentException negativeTimestamp = Assert.Throws<IllegalArgumentException>(
            () => new ProducerRecord<string, int>("test", 1, "key", partition: 0, timestamp: -1L));
        Assert.Equal(NegativeTimestamp, negativeTimestamp.Message);

        IllegalArgumentException negativePartition = Assert.Throws<IllegalArgumentException>(
            () => new ProducerRecord<string, int>("test", 1, "key", partition: -1));
        Assert.Equal(NegativePartition, negativePartition.Message);
    }

    [Fact]
    public void Ctor_NegativeTimestampAndPartition_ReportsTheTimestamp()
    {
        IllegalArgumentException ex = Assert.Throws<IllegalArgumentException>(
            () => new ProducerRecord<string, int>("test", 1, "key", partition: -1, timestamp: -1L));
        Assert.Equal(NegativeTimestamp, ex.Message);
    }

    [Fact]
    public void Ctor_NullTopicWithANegativeTimestampAndPartition_ReportsTheTopic()
    {
        IllegalArgumentException ex = Assert.Throws<IllegalArgumentException>(
            () => new ProducerRecord<string, int>(null!, 1, "key", partition: -1, timestamp: -1L));
        Assert.Equal(NullTopic, ex.Message);
    }

    [Fact]
    public void Ctor_ZeroTimestampAndPartition_AreAccepted()
    {
        ProducerRecord<string, int> record =
            new ProducerRecord<string, int>("test", 1, "key", partition: 0, timestamp: 0L);

        Assert.Equal(0, record.Partition);
        Assert.Equal(0L, record.Timestamp);
    }

    [Fact]
    public void Ctor_TheMessagesFormatTheNumberInTheInvariantCulture()
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        CultureInfo tilde = (CultureInfo)CultureInfo.InvariantCulture.Clone();
        tilde.NumberFormat.NegativeSign = "~";
        CultureInfo.CurrentCulture = tilde;
        try
        {
            Assert.Equal(
                "Invalid timestamp: -7. Timestamp should always be non-negative or null.",
                Assert.Throws<IllegalArgumentException>(
                    () => new ProducerRecord<string, int>("test", 1, "key", timestamp: -7L)).Message);
            Assert.Equal(
                "Invalid partition: -7. Partition number should always be non-negative or null.",
                Assert.Throws<IllegalArgumentException>(
                    () => new ProducerRecord<string, int>("test", 1, "key", partition: -7)).Message);
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Fact]
    public void Ctor_Fullyspecified_ExposesFields()
    {
        byte[] key = Encoding.UTF8.GetBytes("key");
        byte[] value = Encoding.UTF8.GetBytes("value");

        ProducerRecord<byte[], byte[]> record =
            new ProducerRecord<byte[], byte[]>(Topic, value, key, partition: 3, timestamp: 42L);

        Assert.Equal(Topic, record.Topic);
        Assert.Equal(3, record.Partition);
        Assert.Equal(42L, record.Timestamp);
        Assert.NotNull(record.Key);
        Assert.NotNull(record.Value);
        Assert.Equal(key, record.Key);
        Assert.Equal(value, record.Value);
    }

    [Fact]
    public void Ctor_MinimalTopicAndValue_DefaultsOptionalsToNull()
    {
        ProducerRecord<byte[], byte[]> record =
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("value"));

        Assert.Null(record.Partition);
        Assert.Null(record.Timestamp);
        Assert.Null(record.Key);
        Assert.NotNull(record.Value);
    }

    [Fact]
    public void Ctor_NullValue_IsTombstone()
    {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(Topic, value: null);

        Assert.Null(record.Value);
    }

    [Fact]
    public void Ctor_EmptyValue_IsPresentButZeroLength()
    {
        // Empty is distinct from absent: non-null, length 0.
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(Topic, Array.Empty<byte>());

        Assert.NotNull(record.Value);
        Assert.Empty(record.Value!);
    }

    [Fact]
    public void ToString_SummarizesFields()
    {
        // The record renders its key and value through their own ToString, as Java's does.
        ProducerRecord<string, string> record = new ProducerRecord<string, string>(
            Topic, "12345", "ab", partition: 1, timestamp: 7L);

        Assert.Equal(
            "ProducerRecord(topic=record-topic, partition=1, headers=RecordHeaders(headers = [], isReadOnly = false), "
                + "key=ab, value=12345, timestamp=7)",
            record.ToString());
    }

    [Fact]
    public void ToString_NullOptionals_RenderNull()
    {
        ProducerRecord<string, string> record = new ProducerRecord<string, string>(Topic, value: null);

        Assert.Equal(
            "ProducerRecord(topic=record-topic, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), "
                + "key=null, value=null, timestamp=null)",
            record.ToString());
    }

    [Fact]
    public void ToString_RendersTheHeaders()
    {
        ProducerRecord<string, string> record = new ProducerRecord<string, string>(
            Topic, "v", "k", headers: new[] { new Header("h", new byte[] { 0x01, 0xff }), new Header("n", null) });

        Assert.Equal(
            "ProducerRecord(topic=record-topic, partition=null, headers=RecordHeaders(headers = "
                + "[RecordHeader(key = h, value = [1, -1]), RecordHeader(key = n, value = null)], isReadOnly = false), "
                + "key=k, value=v, timestamp=null)",
            record.ToString());
    }

    [Fact]
    public void EqualsAndHashCode()
    {
        ProducerRecord<string, int> producerRecord = new ProducerRecord<string, int>("test", 1, "key", partition: 1);
        Assert.Equal(producerRecord, producerRecord);
        Assert.Equal(producerRecord.GetHashCode(), producerRecord.GetHashCode());

        ProducerRecord<string, int> equalRecord = new ProducerRecord<string, int>("test", 1, "key", partition: 1);
        Assert.Equal(producerRecord, equalRecord);
        Assert.Equal(producerRecord.GetHashCode(), equalRecord.GetHashCode());

        ProducerRecord<string, int> topicMisMatch = new ProducerRecord<string, int>("test-1", 1, "key", partition: 1);
        Assert.NotEqual(producerRecord, topicMisMatch);

        ProducerRecord<string, int> partitionMismatch = new ProducerRecord<string, int>("test", 1, "key", partition: 2);
        Assert.NotEqual(producerRecord, partitionMismatch);

        ProducerRecord<string, int> keyMisMatch = new ProducerRecord<string, int>("test", 1, "key-1", partition: 1);
        Assert.NotEqual(producerRecord, keyMisMatch);

        ProducerRecord<string, int> valueMisMatch = new ProducerRecord<string, int>("test", 2, "key", partition: 1);
        Assert.NotEqual(producerRecord, valueMisMatch);

        ProducerRecord<string, int?> nullFieldsRecord = new ProducerRecord<string, int?>("topic", null, null, null, null, null);
        Assert.Equal(nullFieldsRecord, nullFieldsRecord);
        Assert.Equal(nullFieldsRecord.GetHashCode(), nullFieldsRecord.GetHashCode());
    }

    [Fact]
    public void Equals_ComparesTheHeadersAndTimestamp_AndAByteArrayByReference()
    {
        ProducerRecord<string, string> record = new ProducerRecord<string, string>(
            "t", "v", "k", timestamp: 5L, headers: new[] { new Header("k", Bytes("v")) });

        ProducerRecord<string, string> equalHeaders = new ProducerRecord<string, string>(
            "t", "v", "k", timestamp: 5L, headers: new[] { new Header("k", Bytes("v")) });
        Assert.Equal(record, equalHeaders);
        Assert.Equal(record.GetHashCode(), equalHeaders.GetHashCode());

        Assert.NotEqual(record, new ProducerRecord<string, string>("t", "v", "k", timestamp: 5L));
        Assert.NotEqual(
            record,
            new ProducerRecord<string, string>("t", "v", "k", timestamp: 6L, headers: new[] { new Header("k", Bytes("v")) }));
        Assert.False(record.Equals(null));

        byte[] value = Bytes("v");
        Assert.Equal(new ProducerRecord<byte[], byte[]>("t", value), new ProducerRecord<byte[], byte[]>("t", value));
        Assert.NotEqual(new ProducerRecord<byte[], byte[]>("t", value), new ProducerRecord<byte[], byte[]>("t", Bytes("v")));
    }

    [Fact]
    public void FormsAndHeaders()
    {
        ProducerRecord<byte[], byte[]> r = new ProducerRecord<byte[], byte[]>("t", Bytes("v"));
        Assert.Equal("t", r.Topic);
        Assert.Null(r.Partition);
        Assert.Null(r.Timestamp);
        Assert.Null(r.Key);
        Assert.Equal(Bytes("v"), r.Value);
        Assert.Equal(new Headers(), r.Headers);
        Assert.Empty(r.Headers.ToArray());

        byte[] raw = Bytes("h");
        ProducerRecord<byte[], byte[]> tombstone = new ProducerRecord<byte[], byte[]>(
            "t", value: null, key: Bytes("k"), partition: null, headers: new[] { new Header("a", raw), new Header("b", null) });
        Assert.Null(tombstone.Value);
        Header[] headers = tombstone.Headers.ToArray();
        Assert.Equal(2, headers.Length);
        Assert.Equal("a", headers[0].Key);
        Assert.Equal("b", headers[1].Key);
        Assert.Null(headers[1].Value);

        raw[0] = (byte)'H';
        Assert.Same(raw, headers[0].Value);
        Assert.Equal(Bytes("H"), headers[0].Value);

        Assert.Equal(
            "ProducerRecord(topic=t, partition=1, headers=RecordHeaders(headers = [], isReadOnly = false), key=k, "
                + "value=v, timestamp=null)",
            new ProducerRecord<string, string>("t", "v", "k", partition: 1).ToString());

        ProducerRecord<byte[], byte[]> nullKey = new ProducerRecord<byte[], byte[]>("t", Bytes("v"), key: null, partition: 0);
        Assert.Equal(0, nullKey.Partition);
        Assert.Null(nullKey.Key);
        Assert.Equal(Bytes("v"), nullKey.Value);
        Assert.Null(new ProducerRecord<byte[], byte[]>("t", Bytes("v"), Bytes("k"), partition: null).Partition);

        ProducerRecord<byte[], byte[]> nullTimestamp = new ProducerRecord<byte[], byte[]>(
            "t", Bytes("v"), Bytes("k"), partition: 0, timestamp: null);
        Assert.Null(nullTimestamp.Timestamp);
        Assert.Equal(new Headers(), nullTimestamp.Headers);

        ProducerRecord<byte[], byte[]> emptyHeaders = new ProducerRecord<byte[], byte[]>(
            "t", Bytes("v"), key: null, partition: null, headers: Array.Empty<Header>());
        Assert.Null(emptyHeaders.Timestamp);
        Assert.Equal(new Headers(), emptyHeaders.Headers);
    }

    [Fact]
    public void Ctor_NullHeader_ThrowsArgumentNull_AfterTheOtherChecks()
    {
        Header[] withNull = { new Header("k", null), null! };

        ArgumentNullException nullHeader = Assert.Throws<ArgumentNullException>(
            () => new ProducerRecord<string, string>("t", "v", headers: withNull));
        Assert.Equal("headers", nullHeader.ParamName);
        Assert.Equal("Header cannot be null.", WithoutDotNetSuffix(nullHeader));

        IllegalArgumentException nullTopic = Assert.Throws<IllegalArgumentException>(
            () => new ProducerRecord<string, string>(null!, "v", headers: withNull));
        Assert.Equal(NullTopic, nullTopic.Message);

        Assert.Equal(
            NegativeTimestamp,
            Assert.Throws<IllegalArgumentException>(
                () => new ProducerRecord<string, string>("t", "v", timestamp: -1L, headers: withNull)).Message);
        Assert.Equal(
            NegativePartition,
            Assert.Throws<IllegalArgumentException>(
                () => new ProducerRecord<string, string>("t", "v", partition: -1, headers: withNull)).Message);
    }

    [Fact]
    public void Ctor_HeadersArgument_IsCopied_NotSharedOrSealed()
    {
        Header given = new Header("k", Bytes("v"));
        Headers source = new Headers(new[] { given });
        source.SetReadOnly();

        ProducerRecord<string, string> record = new ProducerRecord<string, string>("t", "v", "k", headers: source);
        Assert.NotSame(source, record.Headers);
        Assert.Equal(source, record.Headers);
        Assert.Same(record.Headers, record.Headers);
        Assert.Same(given, record.Headers.ToArray()[0]);
        Assert.False(record.Headers.IsReadOnly);
        Assert.True(source.IsReadOnly);

        record.Headers.Add("more", null);
        Assert.Equal(2, record.Headers.ToArray().Length);
        Assert.Single(source.ToArray());

        Headers writable = new Headers(new[] { given });
        ProducerRecord<string, string> fromHeaders = new ProducerRecord<string, string>("t", "v", headers: writable);
        writable.Add("later", null);
        Assert.Single(fromHeaders.Headers.ToArray());

        List<Header> list = new List<Header> { given };
        ProducerRecord<string, string> fromList = new ProducerRecord<string, string>("t", "v", headers: list);
        list.Add(new Header("later", null));
        fromList.Headers.Add("own", null);
        Assert.Equal(new[] { "k", "own" }, Keys(fromList.Headers));
        Assert.Equal(2, list.Count);
    }

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    private static List<string> Keys(Headers headers)
    {
        List<string> keys = new List<string>();
        foreach (Header header in headers)
        {
            keys.Add(header.Key);
        }

        return keys;
    }

    // The message without what .NET appends to it (the parameter name), read from a probe of the same type, so it
    // matches this runtime and culture.
    private static string WithoutDotNetSuffix(ArgumentException ex)
    {
        string suffix = new ArgumentException(string.Empty, ex.ParamName).Message;
        Assert.EndsWith(suffix, ex.Message, StringComparison.Ordinal);
        return ex.Message.Substring(0, ex.Message.Length - suffix.Length);
    }
}
