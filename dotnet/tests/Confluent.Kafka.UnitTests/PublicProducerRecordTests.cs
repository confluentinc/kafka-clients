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
using System.Globalization;
using System.Text;

using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="ProducerRecord{TKey, TValue}"/>'s constructor checks (a null topic, a negative timestamp, a negative
/// partition, in Java's order and with Java's messages), its properties and its ToString.
/// </summary>
public sealed class PublicProducerRecordTests
{
    private const string Topic = "record-topic";
    private const string NullTopic = "Topic cannot be null.";
    private const string NegativeTimestamp = "Invalid timestamp: -1. Timestamp should always be non-negative or null.";
    private const string NegativePartition =
        "Invalid partition: -1. Partition number should always be non-negative or null.";

    [Fact]
    public void Ctor_NullTopic_ThrowsArgumentNull()
    {
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(
            () => new ProducerRecord<byte[], byte[]>(null!, Encoding.UTF8.GetBytes("v")));
        Assert.Equal("topic", ex.ParamName);
        Assert.Equal(NullTopic, WithoutDotNetSuffix(ex));
    }

    [Fact]
    public void Ctor_NegativePartition_ThrowsArgumentOutOfRange()
    {
        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: -1));
        AssertOutOfRange(ex, "partition", -1, NegativePartition);
    }

    [Fact]
    public void InvalidRecords()
    {
        ArgumentNullException nullTopic = Assert.Throws<ArgumentNullException>(
            () => new ProducerRecord<string, int>(null!, 1, "key", partition: 0));
        Assert.Equal("topic", nullTopic.ParamName);
        Assert.Equal(NullTopic, WithoutDotNetSuffix(nullTopic));

        AssertOutOfRange(
            Assert.Throws<ArgumentOutOfRangeException>(
                () => new ProducerRecord<string, int>("test", 1, "key", partition: 0, timestamp: -1L)),
            "timestamp",
            -1L,
            NegativeTimestamp);

        AssertOutOfRange(
            Assert.Throws<ArgumentOutOfRangeException>(
                () => new ProducerRecord<string, int>("test", 1, "key", partition: -1)),
            "partition",
            -1,
            NegativePartition);
    }

    [Fact]
    public void Ctor_NegativeTimestampAndPartition_ReportsTheTimestamp()
    {
        AssertOutOfRange(
            Assert.Throws<ArgumentOutOfRangeException>(
                () => new ProducerRecord<string, int>("test", 1, "key", partition: -1, timestamp: -1L)),
            "timestamp",
            -1L,
            NegativeTimestamp);
    }

    [Fact]
    public void Ctor_NullTopicWithANegativeTimestampAndPartition_ReportsTheTopic()
    {
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(
            () => new ProducerRecord<string, int>(null!, 1, "key", partition: -1, timestamp: -1L));
        Assert.Equal("topic", ex.ParamName);
        Assert.Equal(NullTopic, WithoutDotNetSuffix(ex));
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
            AssertOutOfRange(
                Assert.Throws<ArgumentOutOfRangeException>(
                    () => new ProducerRecord<string, int>("test", 1, "key", timestamp: -7L)),
                "timestamp",
                -7L,
                "Invalid timestamp: -7. Timestamp should always be non-negative or null.");
            AssertOutOfRange(
                Assert.Throws<ArgumentOutOfRangeException>(
                    () => new ProducerRecord<string, int>("test", 1, "key", partition: -7)),
                "partition",
                -7,
                "Invalid partition: -7. Partition number should always be non-negative or null.");
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
            "ProducerRecord(topic=record-topic, partition=1, timestamp=7, key=ab, value=12345)",
            record.ToString());
    }

    [Fact]
    public void ToString_NullOptionals_RenderNull()
    {
        ProducerRecord<string, string> record = new ProducerRecord<string, string>(Topic, value: null);

        Assert.Equal(
            "ProducerRecord(topic=record-topic, partition=null, timestamp=null, key=null, value=null)",
            record.ToString());
    }

    private static void AssertOutOfRange(
        ArgumentOutOfRangeException ex, string paramName, object actualValue, string message)
    {
        Assert.Equal(paramName, ex.ParamName);
        Assert.Equal(actualValue, ex.ActualValue);
        Assert.Equal(message, WithoutDotNetSuffix(ex));
    }

    // The message without what .NET appends to it (the parameter name and the actual value), read from a probe of
    // the same type, so it matches this runtime and culture.
    private static string WithoutDotNetSuffix(ArgumentException ex)
    {
        string suffix = ex is ArgumentOutOfRangeException outOfRange
            ? new ArgumentOutOfRangeException(outOfRange.ParamName, outOfRange.ActualValue, string.Empty).Message
            : new ArgumentException(string.Empty, ex.ParamName).Message;
        Assert.EndsWith(suffix, ex.Message, StringComparison.Ordinal);
        return ex.Message.Substring(0, ex.Message.Length - suffix.Length);
    }
}
