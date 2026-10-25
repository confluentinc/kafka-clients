// Copyright 2026 Confluent Inc.
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
using System.Linq;
using System.Text;

using Xunit;

using Origin = Confluent.Kafka.RecordDeserializationException.DeserializationExceptionOrigin;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The sync <c>Poll</c>'s deserialization: a null key or value is not deserialized, and a record that fails stops
/// the batch, throws <see cref="RecordDeserializationException"/> when nothing precedes it, and is polled again.
/// </summary>
public sealed class RecordDeserializationTests
{
    private const string Topic = "deserialization-topic";

    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    private static readonly TopicPartition s_tp1 = new TopicPartition(Topic, 1);

    [Fact]
    public void ANullKeyOrValue_IsNotDeserialized()
    {
        RecordingDeserializer keys = new RecordingDeserializer();
        RecordingDeserializer values = new RecordingDeserializer();
        MockConsumer<string?, string?> consumer = Ready(keys, values, s_tp0);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, 0, 0, key: null, value: Bytes("v"));
        consumer.AddRecord(Topic, 0, 1, key: Bytes("k"), value: null);
        consumer.AddRecord(Topic, 0, 2, key: null, value: null);

        ConsumerRecord<string?, string?>[] records = consumer.Poll(s_pollTimeout).OrderBy(r => r.Offset).ToArray();

        Assert.Equal(3, records.Length);
        Assert.Null(records[0].Key);
        Assert.Equal("v", records[0].Value);
        Assert.Equal("k", records[1].Key);
        Assert.Null(records[1].Value);
        Assert.Null(records[2].Key);
        Assert.Null(records[2].Value);
        Assert.Equal(new[] { "k" }, keys.Seen);
        Assert.Equal(new[] { "v" }, values.Seen);
    }

    [Fact]
    public void AFailingValue_IsReturnedAfterTheRecordsBeforeIt_ThenThrown_ThenSkippedBySeekingPastIt()
    {
        RecordingDeserializer values = new RecordingDeserializer(failOn: "x");
        MockConsumer<string?, string?> consumer = Ready(new RecordingDeserializer(), values, s_tp0);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, 0, 0, key: null, value: null);
        consumer.AddRecord(Topic, 0, 1, key: null, value: Bytes("x"));

        ConsumerRecord<string?, string?> first = Assert.Single(consumer.Poll(s_pollTimeout));
        Assert.Equal(0L, first.Offset);
        Assert.Null(first.Value);
        Assert.Equal(1L, consumer.Position(s_tp0));

        consumer.AddRecord(Topic, 0, 1, key: null, value: Bytes("x"));
        RecordDeserializationException e = Assert.Throws<RecordDeserializationException>(() => consumer.Poll(s_pollTimeout));

        Assert.Equal(Origin.Value, e.Origin);
        Assert.Equal(s_tp0, e.TopicPartition);
        Assert.Equal(1L, e.Offset);
        Assert.Null(e.KeyBuffer);
        Assert.Equal(Bytes("x"), e.ValueBuffer);
        Assert.Equal(-1L, e.Timestamp);
        Assert.Equal(TimestampType.NoTimestampType, e.TimestampType);
        Assert.Empty(e.Headers!);
        Assert.Equal(
            "Error deserializing VALUE for partition deserialization-topic-0 at offset 1. If needed, please seek past "
                + "the record to continue consumption.",
            e.Message);
        Assert.Same(values.Thrown.Last(), e.InnerException);
        Assert.Equal(1L, consumer.Position(s_tp0));

        consumer.Seek(s_tp0, e.Offset + 1);
        consumer.AddRecord(Topic, 0, 2, key: null, value: Bytes("y"));
        ConsumerRecord<string?, string?> next = Assert.Single(consumer.Poll(s_pollTimeout));
        Assert.Equal(2L, next.Offset);
        Assert.Equal("y", next.Value);
    }

    [Fact]
    public void AFailingKey_ThrowsForTheKey_WithBothBuffers_AndLeavesTheValueUnread()
    {
        RecordingDeserializer values = new RecordingDeserializer();
        MockConsumer<string?, string?> consumer = Ready(new RecordingDeserializer(failOn: "k"), values, s_tp0);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, 0, 4, key: Bytes("k"), value: Bytes("v"));

        RecordDeserializationException e = Assert.Throws<RecordDeserializationException>(() => consumer.Poll(s_pollTimeout));

        Assert.Equal(Origin.Key, e.Origin);
        Assert.Equal(Bytes("k"), e.KeyBuffer);
        Assert.Equal(Bytes("v"), e.ValueBuffer);
        Assert.Equal(
            "Error deserializing KEY for partition deserialization-topic-0 at offset 4. If needed, please seek past "
                + "the record to continue consumption.",
            e.Message);
        Assert.Empty(values.Seen);
        Assert.Equal(4L, consumer.Position(s_tp0));
    }

    [Fact]
    public void AFailureInOnePartition_SeeksEveryPartitionBackToItsFirstRecordNotReturned()
    {
        MockConsumer<string?, string?> consumer =
            Ready(new RecordingDeserializer(), new RecordingDeserializer(failOn: "bad"), s_tp0, s_tp1);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, 0, 0, key: null, value: Bytes("a"));
        consumer.AddRecord(Topic, 0, 1, key: null, value: Bytes("bad"));
        consumer.AddRecord(Topic, 0, 2, key: null, value: Bytes("c"));
        consumer.AddRecord(Topic, 1, 0, key: null, value: Bytes("d"));
        consumer.AddRecord(Topic, 1, 1, key: null, value: Bytes("e"));

        ConsumerRecord<string?, string?>[] records = consumer.Poll(s_pollTimeout).ToArray();

        Assert.Equal(new[] { 0L }, records.Where(r => r.Partition == 0).Select(r => r.Offset));
        long[] partition1 = records.Where(r => r.Partition == 1).Select(r => r.Offset).ToArray();
        bool partition1Returned = partition1.Length > 0;
        if (partition1Returned)
        {
            Assert.Equal(new[] { 0L, 1L }, partition1);
        }

        Assert.Equal(1L, consumer.Position(s_tp0));
        Assert.Equal(partition1Returned ? 2L : 0L, consumer.Position(s_tp1));
    }

    [Fact]
    public void AFailureInTheFirstPartitionRead_SeeksTheLaterPartitionBack_InEitherOrder()
    {
        MockConsumer<string?, string?> consumer =
            Ready(new RecordingDeserializer(), new RecordingDeserializer(failOn: "bad"), s_tp0, s_tp1);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, 0, 0, key: null, value: Bytes("bad"));
        consumer.AddRecord(Topic, 0, 1, key: null, value: Bytes("a"));
        consumer.AddRecord(Topic, 1, 0, key: null, value: Bytes("bad"));
        consumer.AddRecord(Topic, 1, 1, key: null, value: Bytes("b"));

        RecordDeserializationException e = Assert.Throws<RecordDeserializationException>(() => consumer.Poll(s_pollTimeout));

        Assert.Contains(e.TopicPartition, new[] { s_tp0, s_tp1 });
        Assert.Equal(0L, e.Offset);
        Assert.Equal(0L, consumer.Position(s_tp0));
        Assert.Equal(0L, consumer.Position(s_tp1));
    }

    private static MockConsumer<string?, string?> Ready(
        IDeserializer<string?> keys, IDeserializer<string?> values, params TopicPartition[] partitions)
    {
        MockConsumer<string?, string?> consumer = new MockConsumer<string?, string?>(keys, values);
        consumer.Assign(partitions);
        foreach (TopicPartition partition in partitions)
        {
            consumer.Seek(partition, 0);
        }

        return consumer;
    }

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    /// <summary>A UTF-8 deserializer that records each value and throws for the one it is told to fail on.</summary>
    private sealed class RecordingDeserializer : IDeserializer<string?>
    {
        private readonly string? _failOn;

        internal RecordingDeserializer(string? failOn = null)
        {
            _failOn = failOn;
        }

        internal List<string> Seen { get; } = new List<string>();

        internal List<Exception> Thrown { get; } = new List<Exception>();

        public string? Deserialize(string topic, ReadOnlySpan<byte> data)
        {
            string text = Encoding.UTF8.GetString(data.ToArray());
            Seen.Add(text);
            if (text == _failOn)
            {
                Exception thrown = new FormatException("bad " + text);
                Thrown.Add(thrown);
                throw thrown;
            }

            return text;
        }
    }
}
