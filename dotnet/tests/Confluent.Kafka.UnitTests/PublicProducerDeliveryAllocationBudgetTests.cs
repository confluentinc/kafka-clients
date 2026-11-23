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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;
using Xunit;
using Xunit.Abstractions;

namespace Confluent.Kafka.UnitTests;

// GC.GetAllocatedBytesForCurrentThread has no net462 equivalent, so the class is net8.0+ only.
#if NET8_0_OR_GREATER

/// <summary>
/// The delivery-path allocation audit: what the producer's completion thread allocates per delivered record, where
/// the record's <see cref="RecordMetadata"/> is copied out and its <see cref="ICallback"/> runs. Each callback reads
/// the completion thread's own allocation counter, so the cost per record is the growth between the first and the
/// last callback over the records in between, best of N attempts.
/// </summary>
public sealed class PublicProducerDeliveryAllocationBudgetTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "delivery-thread-alloc-topic";

    private const int RecordCount = 128;

    private const int ValueSize = 64;

    // Best of N: a one-off allocation on the completion thread (a growth, a first use) raises one attempt, while a
    // real per-record regression raises every attempt, the minimum included.
    private const int MeasurementAttempts = 8;

    private const int WarmupAttempts = 3;

    // The completion thread's per-record budget: the measured 136 B (net8.0, net10.0, both producers; the
    // RecordMetadata, 56 B, and its topic string, 80 B for this 27-character topic) plus 16 B, less than one more
    // object (24 B).
    private const long PerRecordBudgetBytes = 152;

    private readonly ITestOutputHelper _output;

    public PublicProducerDeliveryAllocationBudgetTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void SyncProducer_CompletionThread_PerRecordAllocation_StaysWithinItsMeasuredBudget()
    {
        byte[] key = MakeBytes(16, 0xAB);
        byte[] value = MakeBytes(ValueSize, 0xCD);
        SamplingCallback callback = new SamplingCallback(RecordCount);

        MockProducer<byte[], byte[]> producer =
            new MockProducer<byte[], byte[]>(Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        // Pre-allocated outside every attempt; storing a struct into it allocates nothing.
        KafkaFuture<RecordMetadata>[] futures = new KafkaFuture<RecordMetadata>[RecordCount];

        Measurement best = Measure(callback, () =>
        {
            for (int i = 0; i < RecordCount; i++)
            {
                futures[i] = producer.Send(
                    new ProducerRecord<byte[], byte[]>(Topic, value: value, key: key, partition: 0), callback);
            }
        },
        () => TestTimeout.Run(
            () =>
            {
                foreach (KafkaFuture<RecordMetadata> future in futures)
                {
                    _ = future.Get();
                }
            },
            s_deadline));

        AssertWithinBudget("MockProducer", best);
    }

    [Fact]
    public void AsyncProducer_CompletionThread_PerRecordAllocation_StaysWithinItsMeasuredBudget()
    {
        byte[] key = MakeBytes(16, 0xAB);
        byte[] value = MakeBytes(ValueSize, 0xCD);
        SamplingCallback callback = new SamplingCallback(RecordCount);

        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(new AsyncByteArraySerializer(), new AsyncByteArraySerializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        // Pre-allocated outside every attempt. The sends are awaited only after the last callback has read its
        // counter, so no continuation runs on the completion thread while it is measured.
        ValueTask<AsyncKafkaFuture<RecordMetadata>>[] sends = new ValueTask<AsyncKafkaFuture<RecordMetadata>>[RecordCount];

        Measurement best = Measure(callback, () =>
        {
            for (int i = 0; i < RecordCount; i++)
            {
                sends[i] = producer.Send(
                    new ProducerRecord<byte[], byte[]>(Topic, value: value, key: key, partition: 0), callback);
            }
        },
        () => TestTimeout.Run(
            () =>
            {
                foreach (ValueTask<AsyncKafkaFuture<RecordMetadata>> send in sends)
                {
                    _ = send.Delivery().GetAwaiter().GetResult();
                }
            },
            s_deadline));

        AssertWithinBudget("AsyncMockProducer", best);
    }

    private void AssertWithinBudget(string producer, Measurement best)
    {
        _output.WriteLine(
            $"{producer} completion thread: {best.PerRecord} B/record ({best.Bytes} B over {RecordCount - 1} " +
            $"records, best of {best.Qualified} single-thread attempts of {MeasurementAttempts}).");

        Assert.True(
            best.PerRecord <= PerRecordBudgetBytes,
            $"{producer} per-record completion-thread allocation {best.PerRecord} B exceeded the budget " +
            $"{PerRecordBudgetBytes} B ({best.Bytes} B over {RecordCount - 1} records, best of {best.Qualified}) — " +
            "the delivery of a record must not add an object to the RecordMetadata copy and its topic.");
    }

    private static Measurement Measure(SamplingCallback callback, Action sendAll, Action awaitAll)
    {
        // Warm up so the measured runs are steady-state (JIT, first-use allocations on the completion thread).
        for (int attempt = 0; attempt < WarmupAttempts; attempt++)
        {
            _ = RunAttempt(callback, sendAll, awaitAll);
        }

        long bytes = long.MaxValue;
        int qualified = 0;
        for (int attempt = 0; attempt < MeasurementAttempts; attempt++)
        {
            // Null when the attempt's callbacks did not all run on one thread: its counter readings then belong to
            // different threads and are not comparable.
            long? measured = RunAttempt(callback, sendAll, awaitAll);
            if (measured is long attemptBytes)
            {
                qualified++;
                bytes = Math.Min(bytes, attemptBytes);
            }
        }

        Assert.True(
            qualified > 0,
            $"No attempt of {MeasurementAttempts} ran all its {RecordCount} callbacks on one thread.");
        return new Measurement(bytes, qualified);
    }

    private static long? RunAttempt(SamplingCallback callback, Action sendAll, Action awaitAll)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        callback.Reset();
        sendAll();
        Assert.True(
            callback.WaitForAll(s_deadline),
            $"Only {callback.Delivered} of {RecordCount} callbacks ran within {s_deadline}.");
        awaitAll();

        Assert.Equal(0, callback.Failures);
        return callback.OnOneThread ? callback.LastMinusFirst : null;
    }

    private static byte[] MakeBytes(int size, byte fill)
    {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++)
        {
            bytes[i] = fill;
        }

        return bytes;
    }

    /// <summary>
    /// The best attempt's growth of the completion thread's allocation counter, and how many attempts ran all their
    /// callbacks on one thread.
    /// </summary>
    private readonly struct Measurement
    {
        internal Measurement(long bytes, int qualified)
        {
            Bytes = bytes;
            Qualified = qualified;
        }

        internal long Bytes { get; }

        internal int Qualified { get; }

        internal long PerRecord => Bytes / (RecordCount - 1);
    }

    /// <summary>
    /// Records, for each delivered record, the calling thread's allocation counter and its id, into arrays allocated
    /// once, so the callback itself allocates nothing. A failed record, or the placeholder metadata, counts as a
    /// failure: the measured path is the copy-out of a delivered record's metadata.
    /// </summary>
    private sealed class SamplingCallback : ICallback
    {
        private readonly long[] _allocated;
        private readonly int[] _threads;
        private readonly ManualResetEventSlim _all = new ManualResetEventSlim(false);
        private int _next;
        private int _failures;

        internal SamplingCallback(int count)
        {
            _allocated = new long[count];
            _threads = new int[count];
        }

        internal int Delivered => Math.Min(Volatile.Read(ref _next), _allocated.Length);

        internal int Failures => Volatile.Read(ref _failures);

        internal long LastMinusFirst => _allocated[_allocated.Length - 1] - _allocated[0];

        internal bool OnOneThread
        {
            get
            {
                for (int i = 1; i < _threads.Length; i++)
                {
                    if (_threads[i] != _threads[0])
                    {
                        return false;
                    }
                }

                return true;
            }
        }

        internal void Reset()
        {
            Volatile.Write(ref _next, 0);
            Volatile.Write(ref _failures, 0);
            _all.Reset();
        }

        internal bool WaitForAll(TimeSpan timeout) => _all.Wait(timeout);

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            // First, so that the reading excludes this callback's own work.
            long allocated = GC.GetAllocatedBytesForCurrentThread();
            int thread = Environment.CurrentManagedThreadId;

            int index = Interlocked.Increment(ref _next) - 1;
            if (index >= _allocated.Length)
            {
                return;
            }

            _allocated[index] = allocated;
            _threads[index] = thread;
            if (exception is not null || metadata.Offset() < 0 || !string.Equals(metadata.Topic(), Topic, StringComparison.Ordinal))
            {
                Interlocked.Increment(ref _failures);
            }

            if (index == _allocated.Length - 1)
            {
                _all.Set();
            }
        }
    }
}

#endif
