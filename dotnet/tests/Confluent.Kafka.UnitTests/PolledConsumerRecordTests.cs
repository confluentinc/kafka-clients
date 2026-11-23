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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Record;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A polled <see cref="ConsumerRecord{TKey, TValue}"/> reads its fields from the poll's native batch on each call:
/// the same values after the next poll, after the consumer is disposed and when held without its
/// <see cref="ConsumerRecords{TKey, TValue}"/>, and the batch is released once no record of it is reachable.
/// </summary>
public sealed class PolledConsumerRecordTests
{
    private const string Topic = "polled-record-topic";

    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    [Fact]
    public void APolledRecord_ReadsTheNativeRecordsFields_OnEveryCall()
    {
        MockConsumer<string, string> consumer = Ready();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, 0, 0, Bytes("key"), Bytes("value"));

        ConsumerRecord<string, string> record = Assert.Single(consumer.Poll(s_pollTimeout));

        for (int i = 0; i < 2; i++)
        {
            Assert.Equal(Topic, record.Topic());
            Assert.Equal(0, record.Partition());
            Assert.Equal(0, record.Offset());
            Assert.Equal("key", record.Key());
            Assert.Equal("value", record.Value());
            Assert.Equal(ConsumerRecord<string, string>.NO_TIMESTAMP, record.Timestamp());
            Assert.Equal(TimestampType.NO_TIMESTAMP_TYPE, record.TimestampType());
            Assert.Equal(-1, record.SerializedKeySize());
            Assert.Equal(-1, record.SerializedValueSize());
            Assert.Null(record.LeaderEpoch());
            Assert.Null(record.DeliveryCount());
            Assert.Empty(record.Headers());
        }

        Assert.Equal(
            "ConsumerRecord(topic = polled-record-topic, partition = 0, leaderEpoch = null, offset = 0, "
            + "NoTimestampType = -1, deliveryCount = null, serialized key size = -1, serialized value size = -1, "
            + "headers = RecordHeaders(headers = [], isReadOnly = false), key = key, value = value)",
            record.ToString());
    }

    [Fact]
    public void APolledRecord_ReadsTheSameFields_AfterTheNextPoll_AndAfterTheConsumerIsDisposed()
    {
        MockConsumer<string, string> consumer = Ready();
        ConsumerRecord<string, string> record;
        string rendering;
        using (TestTimeout.Disposal(consumer))
        {
            consumer.AddRecord(Topic, 0, 0, Bytes("key-0"), Bytes("value-0"));
            record = Assert.Single(consumer.Poll(s_pollTimeout));
            rendering = record.ToString();

            consumer.AddRecord(Topic, 0, 1, Bytes("key-1"), Bytes("value-1"));
            Assert.Equal(1, Assert.Single(consumer.Poll(s_pollTimeout)).Offset());
            Collect();
            AssertFields(record, 0, "key-0", "value-0");
            Assert.Equal(rendering, record.ToString());
        }

        Collect();

        AssertFields(record, 0, "key-0", "value-0");
        Assert.Equal(rendering, record.ToString());
    }

    [Fact]
    public void APolledRecord_HeldWithoutItsRecords_KeepsItsBatch_WhichIsReleasedOnceTheRecordIsUnreachable()
    {
        MockConsumer<string, string> consumer = Ready();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, 0, 0, Bytes("first"), Bytes("one"));
        consumer.AddRecord(Topic, 0, 1, Bytes("second"), Bytes("two"));

        StrongBox<ConsumerRecord<string, string>?> held = new StrongBox<ConsumerRecord<string, string>?>();
        WeakReference batch = PollSecondRecord(consumer, held);
        Collect();

        Assert.True(batch.IsAlive);
        AssertFields(held.Value!, 1, "second", "two");

        held.Value = null;
        Collect();

        Assert.False(batch.IsAlive);
        consumer.AddRecord(Topic, 0, 2, Bytes("third"), Bytes("three"));
        AssertFields(Assert.Single(consumer.Poll(s_pollTimeout)), 2, "third", "three");
    }

    [Fact]
    public void TheMocksRecordBytes_AreFreed_AfterTheLastBatchIsReleased_OnceTheConsumerIsDisposed()
    {
        MockConsumer<string, string> consumer = Ready();
        List<IntPtr> recordBytes = RecordBytes(consumer);
        StrongBox<ConsumerRecord<string, string>?> held = new StrongBox<ConsumerRecord<string, string>?>();
        WeakReference batch;
        using (TestTimeout.Disposal(consumer))
        {
            consumer.AddRecord(Topic, 0, 0, Bytes("key"), Bytes("kept"));
            batch = PollFirstRecord(consumer, held);
        }

        // The held record roots its batch, so no finalizer releases it and the list is read while nothing else
        // touches it: the consumer is disposed, but the batch it handed out is not released yet.
        Collect();
        Assert.True(batch.IsAlive);
        lock (recordBytes)
        {
            Assert.NotEmpty(recordBytes);
        }

        Assert.Equal("kept", ValueOf(held));

        held.Value = null;
        Collect();

        Assert.False(batch.IsAlive);
        lock (recordBytes)
        {
            Assert.Empty(recordBytes);
        }
    }

    private static void AssertFields(ConsumerRecord<string, string> record, long offset, string key, string value)
    {
        Assert.Equal(offset, record.Offset());
        Assert.Equal(key, record.Key());
        Assert.Equal(value, record.Value());
        Assert.Equal(ConsumerRecord<string, string>.NO_TIMESTAMP, record.Timestamp());
        Assert.Equal(TimestampType.NO_TIMESTAMP_TYPE, record.TimestampType());
        Assert.Equal(-1, record.SerializedKeySize());
        Assert.Null(record.LeaderEpoch());
    }

    // Not inlined, so the records and every record but the held one are unreachable once it returns.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference PollSecondRecord(
        MockConsumer<string, string> consumer, StrongBox<ConsumerRecord<string, string>?> held)
    {
        ConsumerRecords<string, string> records = consumer.Poll(s_pollTimeout);
        Assert.Equal(2, records.Count());
        held.Value = records.Records(s_tp0)[1];
        return BatchOf(held.Value);
    }

    // Not inlined, so no reference to the held record is left in the caller's frame once it returns.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static string? ValueOf(StrongBox<ConsumerRecord<string, string>?> held) => held.Value!.Value();

    // Not inlined, so the records are unreachable once it returns and only the held record roots the batch.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference PollFirstRecord(
        MockConsumer<string, string> consumer, StrongBox<ConsumerRecord<string, string>?> held)
    {
        held.Value = Assert.Single(consumer.Poll(s_pollTimeout));
        return BatchOf(held.Value);
    }

    // The reference tracks resurrection, so it dies only after the finalizer that releases the batch.
    private static WeakReference BatchOf(ConsumerRecord<string, string> record)
    {
        SafeConsumerRecordsHandle batch = Assert.IsType<SafeConsumerRecordsHandle>(
            typeof(ConsumerRecord<string, string>)
                .GetField("_batch", BindingFlags.NonPublic | BindingFlags.Instance)!
                .GetValue(record));
        return new WeakReference(batch, trackResurrection: true);
    }

    private static List<IntPtr> RecordBytes(MockConsumer<string, string> consumer)
    {
        NativeConsumer native = Assert.IsType<NativeConsumer>(
            typeof(MockConsumer<string, string>)
                .GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!
                .GetValue(consumer));
        return Assert.IsType<List<IntPtr>>(
            typeof(NativeConsumer)
                .GetField("_recordBytes", BindingFlags.NonPublic | BindingFlags.Instance)!
                .GetValue(native));
    }

    private static MockConsumer<string, string> Ready()
    {
        MockConsumer<string, string> consumer = new MockConsumer<string, string>(
            Serdes.String().Deserializer(), Serdes.String().Deserializer());
        consumer.Assign(new[] { s_tp0 });
        consumer.Seek(s_tp0, 0);
        return consumer;
    }

    private static void Collect()
    {
        for (int i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
    }

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);
}
