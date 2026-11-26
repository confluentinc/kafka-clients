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
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal.Interop;
using Confluent.Kafka.Producer;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// A non-parallel xUnit collection for tests that read <see cref="ConsumerGroupMetadataValues.Live"/>, which the
/// other tests change when they pass a group metadata of their own.
/// </summary>
[CollectionDefinition(SerialGroupMetadataValuesCollection.Name, DisableParallelization = true)]
public sealed class SerialGroupMetadataValuesCollection
{
    internal const string Name = "serial-group-metadata-values";
}

/// <summary>
/// The native copy of a user's group metadata lives until the operation it was passed to completes, also when the
/// caller stopped waiting before then, and is freed once.
/// </summary>
[Collection(SerialGroupMetadataValuesCollection.Name)]
public sealed class ConsumerGroupMetadataValuesLifetimeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly Dictionary<TopicPartition, OffsetAndMetadata> s_offsets =
        new Dictionary<TopicPartition, OffsetAndMetadata>
        {
            [new TopicPartition("topic", 0)] = new OffsetAndMetadata(42L),
        };

    [Fact]
    public async Task TheValues_OutliveACancelledWait_AndAreFreedOnceWhenTheOperationCompletes()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(
            autoComplete: true, new AsyncByteArraySerializer(), new AsyncByteArraySerializer());
        using ManualResetEventSlim entered = new ManualResetEventSlim();
        using ManualResetEventSlim release = new ManualResetEventSlim();
        int baseline;
        try
        {
            await producer.InitTransactions();
            await producer.BeginTransaction();
            CollectGarbage();
            baseline = ConsumerGroupMetadataValues.Live;

            // A delivery callback that blocks holds the producer's completion thread, so the completion of the
            // operation below is queued behind it.
            _ = await producer.Send(
                new ProducerRecord<byte[], byte[]>("topic", new byte[] { 1 }), new BlockingCallback(entered, release));
            Assert.True(entered.Wait(s_deadline), "the delivery callback never ran");

            using (CancellationTokenSource cancellation = new CancellationTokenSource())
            {
                Task sent = producer.SendOffsetsToTransaction(
                    s_offsets, new TestGroupMetadata("user-group", 3, "member-1", null), cancellation.Token);
                cancellation.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => sent);
            }

            // The caller has stopped waiting, but the operation has not completed: its values are still there.
            CollectGarbage();
            Assert.Equal(baseline + 1, ConsumerGroupMetadataValues.Live);

            // The completion runs once the completion thread is free, and frees them.
            release.Set();
            Assert.True(
                await Eventually(() => ConsumerGroupMetadataValues.Live == baseline),
                $"the values were not freed: {ConsumerGroupMetadataValues.Live} live, {baseline} expected");

            // The core read them while they were valid: the offsets are committed under the user's group.
            await producer.CommitTransaction();
            IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>> commit =
                Assert.Single(producer.ConsumerGroupOffsetsHistory());
            Assert.Equal(new[] { "user-group" }, commit.Keys);
        }
        finally
        {
            release.Set();
            await TestTimeout.DisposeAsync(producer);
        }

        // Freed once: neither the producer's teardown nor a finalizer frees them again.
        CollectGarbage();
        Assert.Equal(baseline, ConsumerGroupMetadataValues.Live);
    }

    private static void CollectGarbage()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }

    private static async Task<bool> Eventually(Func<bool> condition)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (!condition())
        {
            if (elapsed.Elapsed > s_deadline)
            {
                return false;
            }

            await Task.Delay(10);
        }

        return true;
    }

    /// <summary>A delivery callback that signals it has started, then waits until the test releases it.</summary>
    private sealed class BlockingCallback : ICallback
    {
        private readonly ManualResetEventSlim _entered;
        private readonly ManualResetEventSlim _release;

        internal BlockingCallback(ManualResetEventSlim entered, ManualResetEventSlim release)
        {
            _entered = entered;
            _release = release;
        }

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            _entered.Set();
            _release.Wait(s_deadline);
        }
    }
}
