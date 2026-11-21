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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.UnitTests.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

// GC.GetAllocatedBytesForCurrentThread has no net462 equivalent, so the class is net8.0 and later only.
#if NET8_0_OR_GREATER

/// <summary>
/// The async <c>Poll</c>'s per-record receive allocation on <see cref="AsyncMockConsumer{TKey, TValue}"/>: with the
/// built-in deserializers it stays within its measured budget, less than one object over the sync <c>Poll</c>'s
/// cost, and an awaited deserializer adds one small manager per key or value with bytes, and no copy of them.
/// </summary>
/// <remarks>
/// The copy-out runs on a pool thread. A built-in runs no user code there, so its test reads the process-wide counter,
/// in a collection that runs alone, and keeps the best of several attempts; what other threads allocate meanwhile
/// is spread over two thousand records, so it moves the result by about 1 B and one more object per record (24 B or
/// more) is caught. That test cannot tell which object was added, and a cost paid once per poll cancels in it. An
/// awaited deserializer that completes at once runs the whole copy-out on one thread, so its tests read that thread's
/// counter from inside the key's calls and their bounds are exact.
/// </remarks>
[Collection(SerialMemoryMeasurementCollection.Name)]
public sealed class PublicAsyncConsumerAllocationBudgetTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "async-alloc-topic";
    private const int Partition = 0;

    private const int KeySize = 16;
    private const int ValueSize = 64;
    private const int LargeSize = 64 * 1024;
    private const int RecordCount = 64;

    // The measured 263 B or 264 B (net8.0, net10.0; the sync Poll's copy-out, with the built-in deserializers called
    // without awaiting) plus 8 B, less than one more object.
    private const long PerRecordBudgetBytes = 272;

    // A manager holds a reference, a pointer, a length and a flag: 40 B on a 64-bit runtime.
    private const long ManagerBytes = 40;

    // On an unoptimized (Debug) library each awaited call's state machine is a heap object although the deserializer
    // completes at once: the measured 112 B per key or value with bytes (net8.0, net10.0).
    private const long UnoptimizedAwaitedCallBytes = 112;

    private const int Attempts = 8;

    [Fact]
    public async Task BuiltInPoll_PerRecordAllocation_WithinTheSyncPollsCopyOutBudget()
    {
        byte[] key = new byte[KeySize];
        byte[] value = new byte[ValueSize];
        for (int i = 0; i < 5; i++)
        {
            await MeasureBuiltInPoll(recordCount: 8, key, value);
        }

        const int smallCount = 64;
        const int largeCount = 2112;
        long smallBytes = long.MaxValue;
        long largeBytes = long.MaxValue;
        for (int attempt = 0; attempt < Attempts; attempt++)
        {
            smallBytes = Math.Min(smallBytes, await MeasureBuiltInPoll(smallCount, key, value));
            largeBytes = Math.Min(largeBytes, await MeasureBuiltInPoll(largeCount, key, value));
        }

        long perRecord = (largeBytes - smallBytes) / (largeCount - smallCount);

        Assert.True(
            perRecord <= PerRecordBudgetBytes,
            $"Per-record receive-path allocation {perRecord} B exceeded the measured budget " +
            $"{PerRecordBudgetBytes} B (small={smallBytes} B/{smallCount} rec, large={largeBytes} B/{largeCount} rec, " +
            $"best of {Attempts}): the built-in deserializers are called without awaiting, so nothing is added " +
            "per record.");
    }

    [Fact]
    public async Task AwaitedPoll_AllocatesOneManagerPerKeyOrValueWithBytes_AndNoneForAnEmptyOne()
    {
        await WarmUp();

        long emptyBytes = await MeasureAwaitedPoll(RecordCount, 0);
        long filledBytes = await MeasureAwaitedPoll(RecordCount, KeySize);

        long managersPerRecord = (filledBytes - emptyBytes) / (RecordCount - 1);
        long budget = LibraryBuild.IsOptimized
            ? 2 * ManagerBytes
            : 2 * (ManagerBytes + UnoptimizedAwaitedCallBytes);

        Assert.True(
            managersPerRecord > 0 && managersPerRecord <= budget,
            $"A record with a key and a value cost {managersPerRecord} B more than one with both empty (budget " +
            $"{budget} B for the two managers, optimized library: {LibraryBuild.IsOptimized}; empty={emptyBytes} B, " +
            $"filled={filledBytes} B over {RecordCount - 1} records).");
    }

    [Fact]
    public async Task AwaitedPoll_LargeKeyAndValue_AddNoAllocationOverSmallOnes()
    {
        await WarmUp();

        long smallBytes = await MeasureAwaitedPoll(RecordCount, KeySize);
        long largeBytes = await MeasureAwaitedPoll(RecordCount, LargeSize);

        long marginalPerRecord = (largeBytes - smallBytes) / (RecordCount - 1);

        Assert.True(
            marginalPerRecord <= 0,
            $"An awaited deserializer's poll added {marginalPerRecord} B/record going from {KeySize} B to " +
            $"{LargeSize} B keys and values (small={smallBytes} B, large={largeBytes} B over {RecordCount - 1} " +
            "records): the bytes are copied.");
    }

    private static async Task WarmUp()
    {
        for (int i = 0; i < 5; i++)
        {
            await MeasureAwaitedPoll(RecordCount, KeySize);
            await MeasureAwaitedPoll(RecordCount, 0);
        }
    }

    private static async Task<long> MeasureBuiltInPoll(int recordCount, byte[] key, byte[] value)
    {
        AsyncMockConsumer<byte[], byte[]> consumer =
            new AsyncMockConsumer<byte[], byte[]>(new AsyncByteArrayDeserializer(), new AsyncByteArrayDeserializer());
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await Ready(consumer, recordCount, key, value);

        long before = GC.GetTotalAllocatedBytes(precise: true);
        ConsumerRecords<byte[], byte[]> records = await consumer.Poll(s_pollTimeout);
        long after = GC.GetTotalAllocatedBytes(precise: true);

        Assert.Equal(recordCount, records.Count());
        return after - before;
    }

    // What the copy-out's thread allocated from the first record's key to the last one's: every record but the last,
    // with its value's manager, and the key's manager of every record but the first.
    private static async Task<long> MeasureAwaitedPoll(int recordCount, int size)
    {
        SamplingDeserializer keys = new SamplingDeserializer();
        SamplingDeserializer values = new SamplingDeserializer();
        AsyncMockConsumer<object, object> consumer = new AsyncMockConsumer<object, object>(keys, values);
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await Ready(consumer, recordCount, new byte[size], new byte[size]);

        ConsumerRecords<object, object> records = await consumer.Poll(s_pollTimeout);

        Assert.Equal(recordCount, records.Count());
        Assert.Equal(recordCount, keys.Calls);
        Assert.Equal(recordCount, values.Calls);
        Assert.Equal(size, keys.LastLength);
        Assert.Equal(size, values.LastLength);
        Assert.Equal(keys.FirstThread, keys.LastThread);
        return keys.Last - keys.First;
    }

    private static async Task Ready<TKey, TValue>(
        AsyncMockConsumer<TKey, TValue> consumer, int recordCount, byte[] key, byte[] value)
    {
        TopicPartition partition = new TopicPartition(Topic, Partition);
        await TestTimeout.Run(() => consumer.Assign(new[] { partition }), s_deadline);
        await TestTimeout.Run(() => consumer.Seek(partition, offset: 0), s_deadline);
        for (int i = 0; i < recordCount; i++)
        {
            consumer.AddRecord(Topic, Partition, offset: i, key, value);
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }

    /// <summary>
    /// A deserializer that completes at once with one shared object, allocating nothing, and reads its thread's
    /// allocation counter on its first and last calls.
    /// </summary>
    private sealed class SamplingDeserializer : IAsyncDeserializer<object>
    {
        private static readonly object s_decoded = new object();

        internal int Calls { get; private set; }

        internal int LastLength { get; private set; }

        internal long First { get; private set; }

        internal long Last { get; private set; }

        internal int FirstThread { get; private set; }

        internal int LastThread { get; private set; }

        public ValueTask<object?> Deserialize(
            string topic, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            Last = GC.GetAllocatedBytesForCurrentThread();
            LastThread = Environment.CurrentManagedThreadId;
            if (Calls == 0)
            {
                First = Last;
                FirstThread = LastThread;
            }

            Calls++;
            LastLength = data.Length;
            return new ValueTask<object?>(s_decoded);
        }
    }
}

#endif
