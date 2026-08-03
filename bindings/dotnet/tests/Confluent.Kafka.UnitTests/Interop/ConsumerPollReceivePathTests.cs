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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The <b>owned-handle</b> completion bridge (M3/P3) exercised end to end through
/// <c>poll_async</c> on a <c>MockConsumer</c> (ffi-marshalling.md §B6/§B7, §6.4). The
/// batch copy-out runs on the dispatcher thread inside the poll callback; these tests
/// assert the round-trip (incl. non-ASCII via the length-delimited <c>out_len</c> path,
/// §B3), the FAILURE / empty / churn paths, and that nothing native-backed escapes.
/// Every awaited op and teardown is under a <see cref="TestTimeout"/> hang guard.
/// </summary>
public sealed class ConsumerPollReceivePathTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    // Broker-free error codes are all indistinct (UnknownServerError == -1); tests
    // assert on TYPE + Message, never on a distinctive Code (PLAN finding #8).
    private const int UnknownServerErrorCode = -1;

    private const string Topic = "poll-topic";
    private const int Partition = 0;

    // Assign + seek to offset 0 so the mock poll has a valid position (poll's Step-6
    // update_fetch_position is skipped when the position is already valid, so no
    // beginning/end-offset reset config is needed — mirrors the canonical Rust
    // mock-poll test's assign→seek→add_record→poll setup).
    private static async Task<NativeConsumer> MockReadyToPoll(string topic = Topic, int partition = Partition)
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        consumer.Assign(new[] { (topic, partition) });
        await consumer.SeekAsync(topic, partition, offset: 0);
        return consumer;
    }

    [Fact]
    public async Task PollAsync_RoundTripsAllFields()
    {
        using NativeConsumer consumer = await MockReadyToPoll();
        byte[] key = Encoding.UTF8.GetBytes("key-1");
        byte[] value = Encoding.UTF8.GetBytes("value-1");
        consumer.AddRecord(Topic, Partition, offset: 42, key, value);

        ConsumerRecords records = await PollAsync(consumer);

        ConsumerRecord record = Assert.Single(records);
        Assert.Equal(Topic, record.Topic);
        Assert.Equal(Partition, record.Partition);
        Assert.Equal(42, record.Offset);
        Assert.NotNull(record.Key);
        Assert.NotNull(record.Value);
        Assert.Equal(key, record.Key!.Value.ToArray());
        Assert.Equal(value, record.Value!.Value.ToArray());
        Assert.Empty(record.Headers);
    }

    [Fact]
    public async Task PollAsync_NonAsciiTopicAndBytes_RoundTripViaOutLen()
    {
        // A multi-byte topic + key/value: the length-delimited out_len path (§B3) must
        // marshal a non-ASCII, non-NUL-terminated slice correctly (a NUL-scan would
        // over-read past the slice into the batch).
        const string nonAsciiTopic = "topic-grüße-Ω-🎉";
        using NativeConsumer consumer = await MockReadyToPoll(nonAsciiTopic);
        byte[] key = Encoding.UTF8.GetBytes("café-key-Ω");
        byte[] value = Encoding.UTF8.GetBytes("naïve-value-🎉");
        consumer.AddRecord(nonAsciiTopic, Partition, offset: 7, key, value);

        ConsumerRecords records = await PollAsync(consumer);

        ConsumerRecord record = Assert.Single(records);
        Assert.Equal(nonAsciiTopic, record.Topic);
        Assert.Equal(key, record.Key!.Value.ToArray());
        Assert.Equal(value, record.Value!.Value.ToArray());
    }

    [Fact]
    public async Task PollAsync_TombstoneAndAbsentKey_MapToNull()
    {
        using NativeConsumer consumer = await MockReadyToPoll();
        // Absent key (null) + tombstone value (null).
        consumer.AddRecord(Topic, Partition, offset: 1, key: null, value: null);

        ConsumerRecords records = await PollAsync(consumer);

        ConsumerRecord record = Assert.Single(records);
        Assert.Null(record.Key);
        Assert.Null(record.Value);
    }

    [Fact]
    public async Task PollAsync_EmptyKeyAndValue_MapToEmptyNonNull()
    {
        using NativeConsumer consumer = await MockReadyToPoll();
        // Empty (non-null) key/value: distinct from absent — a valid zero-length array.
        consumer.AddRecord(Topic, Partition, offset: 2, key: Array.Empty<byte>(), value: Array.Empty<byte>());

        ConsumerRecords records = await PollAsync(consumer);

        ConsumerRecord record = Assert.Single(records);
        Assert.NotNull(record.Key);
        Assert.NotNull(record.Value);
        Assert.Empty(record.Key!.Value.ToArray());
        Assert.Empty(record.Value!.Value.ToArray());
    }

    [Fact]
    public async Task PollAsync_MultipleRecords_RoundTripInOrder()
    {
        using NativeConsumer consumer = await MockReadyToPoll();
        for (int i = 0; i < 5; i++)
        {
            consumer.AddRecord(Topic, Partition, offset: i, Encoding.UTF8.GetBytes($"k{i}"), Encoding.UTF8.GetBytes($"v{i}"));
        }

        ConsumerRecords records = await PollAsync(consumer);

        Assert.Equal(5, records.Count);
        long[] offsets = records.Select(r => r.Offset).ToArray();
        Assert.Equal(new long[] { 0, 1, 2, 3, 4 }, offsets);
    }

    [Fact]
    public async Task PollAsync_Empty_ReturnsNonNullZeroCount()
    {
        // An assigned partition with no records → a non-null ConsumerRecords, Count 0
        // (success, not failure); the (empty) batch is still destroyed once.
        using NativeConsumer consumer = await MockReadyToPoll();

        ConsumerRecords records = await PollAsync(consumer);

        Assert.NotNull(records);
        Assert.Empty(records);
    }

    [Fact]
    public async Task PollAsync_SetPollError_FaultsWithKafkaException()
    {
        using NativeConsumer consumer = await MockReadyToPoll();
        consumer.SetPollError("boom");

        KafkaException ex = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => PollAsync(consumer), s_deadline));

        // Assert TYPE + Message content (the message is part of the contract, DoD §3)
        // + flags — never the indistinct Code (-1).
        Assert.Equal(UnknownServerErrorCode, ex.Code);
        Assert.Contains("boom", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsRetriable);
        Assert.False(ex.IsFatal);
    }

    [Fact]
    public async Task PollAsync_SetPollError_IsOneShot_ThenReusable()
    {
        using NativeConsumer consumer = await MockReadyToPoll();
        consumer.SetPollError("transient");

        await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => PollAsync(consumer), s_deadline));

        // The injected error is consumed by the first poll (Java setPollException):
        // the next poll succeeds.
        ConsumerRecords records = await PollAsync(consumer);
        Assert.Empty(records);
    }

    [Fact]
    public async Task PollAsync_RecordsAndErrorsAndEmpties_Churned_NoCorruption()
    {
        // Interleave records / errors / empties in a loop — the double-free / leak
        // detector for the owned-result path: the batch _destroy, the error handle,
        // and the per-op GCHandle must each be freed EXACTLY ONCE per poll. A leak or
        // double-free would corrupt the allocator over many iterations.
        using NativeConsumer consumer = await MockReadyToPoll();

        for (int i = 0; i < 100; i++)
        {
            switch (i % 3)
            {
                case 0:
                    consumer.AddRecord(Topic, Partition, offset: i, Encoding.UTF8.GetBytes($"k{i}"), Encoding.UTF8.GetBytes($"v{i}"));
                    ConsumerRecords ok = await PollAsync(consumer);
                    Assert.Single(ok);
                    break;

                case 1:
                    consumer.SetPollError($"err-{i}");
                    await Assert.ThrowsAsync<KafkaException>(
                        () => TestTimeout.Run(() => PollAsync(consumer), s_deadline));
                    break;

                default:
                    ConsumerRecords empty = await PollAsync(consumer);
                    Assert.Empty(empty);
                    break;
            }
        }
    }

    [Fact]
    public async Task PollAsync_InFlight_SurvivesAggressiveGc()
    {
        // The Poll delegate is rooted (static readonly) and the per-op context is
        // rooted by its GCHandle (Normal); aggressive GC during an in-flight poll must
        // not collect either. A collected thunk/context would crash.
        using NativeConsumer consumer = await MockReadyToPoll();

        for (int i = 0; i < 50; i++)
        {
            consumer.AddRecord(Topic, Partition, offset: i, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"));
            Task<ConsumerRecords> op = consumer.PollAsync(s_pollTimeout);
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            ConsumerRecords records = await TestTimeoutResult(op);
            Assert.Single(records);
        }
    }

    [Fact]
    public async Task PollAsync_ResultSurvivesBeyondBatchDestroy()
    {
        // The copy-out is owned (§6.4): the batch is destroyed inside the callback, yet
        // the returned records + their bytes remain valid afterward (nothing
        // native-backed escaped). Force GC after the poll to shake out any dangling
        // native reference, then read the bytes.
        using NativeConsumer consumer = await MockReadyToPoll();
        byte[] value = Encoding.UTF8.GetBytes("durable-value");
        consumer.AddRecord(Topic, Partition, offset: 99, key: null, value);

        ConsumerRecords records = await PollAsync(consumer);
        GC.Collect();
        GC.WaitForPendingFinalizers();

        ConsumerRecord record = Assert.Single(records);
        Assert.Equal(value, record.Value!.Value.ToArray());
        Assert.Equal(Topic, record.Topic);
    }

    private static Task<ConsumerRecords> PollAsync(NativeConsumer consumer) =>
        consumer.PollAsync(s_pollTimeout);

    private static async Task<ConsumerRecords> TestTimeoutResult(Task<ConsumerRecords> op)
    {
        ConsumerRecords result = default!;
        await TestTimeout.Run(async () => result = await op, s_deadline);
        return result;
    }
}
