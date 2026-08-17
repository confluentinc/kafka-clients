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
using System.Text;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Unit tests for the public generic <see cref="ProducerRecord{TKey, TValue}"/> — the send-path
/// preconditions it validates (Java-faithful, in the constructor: null topic →
/// <see cref="ArgumentNullException"/>, negative partition → <see cref="ArgumentOutOfRangeException"/>),
/// its field exposure, and its <see cref="object.ToString"/>. These fire entirely in managed code —
/// before any native call — so a constructed record handed to
/// <see cref="IAsyncProducer{TKey, TValue}.Send"/> is already valid (ffi §A5). Bytes cases use
/// <c>ProducerRecord&lt;byte[], byte[]&gt;</c> (generic-only, M11/P5 — the former bytes surface).
/// </summary>
public sealed class PublicProducerRecordTests
{
    private const string Topic = "record-topic";

    [Fact]
    public void Ctor_NullTopic_ThrowsArgumentNull()
    {
        // ArgumentNullException(nameof(topic)) sets no custom message → ParamName is the contract
        // (DoD §3).
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(
            () => new ProducerRecord<byte[], byte[]>(null!, Encoding.UTF8.GetBytes("v")));
        Assert.Equal("topic", ex.ParamName);
    }

    [Fact]
    public void Ctor_NegativePartition_ThrowsArgumentOutOfRange()
    {
        // The binding rejects an explicitly-negative partition (the ABI silently maps negative to
        // "unset", ffi §A5). Pin ParamName + the custom message content (DoD §3).
        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: -1));
        Assert.Equal("partition", ex.ParamName);
        Assert.Contains("must not be negative", ex.Message, StringComparison.Ordinal);
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
        // Empty is distinct from absent (the §A4 sentinel distinction): non-null, Length 0.
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(Topic, Array.Empty<byte>());

        Assert.NotNull(record.Value);
        Assert.Empty(record.Value!);
    }

    [Fact]
    public void ToString_SummarizesFields()
    {
        // The generic record renders key/value via their own ToString (Java-faithful shape), so a
        // string-typed record gives a readable, assertable line (a byte-length summary is impossible
        // for a generic T — M11/P5). Uses the record ctor directly (no serializer needed).
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
}
