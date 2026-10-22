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
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The consumer handle cannot be freed under a native call: every family throws
/// <see cref="ObjectDisposedException"/> after Dispose, and a teardown racing Wakeup, short sync calls or unawaited
/// async calls neither crashes nor throws anything else.
/// </summary>
/// <remarks>
/// The mock's Poll returns at once, so no long blocking Poll can race a Dispose here; a use-after-free in the raced
/// calls crashes the test host.
/// </remarks>
public sealed class PublicConsumerHandleProtectionTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private static TopicPartition Tp() => new TopicPartition("h1-topic", 0);

    private static string[] ProofTopic() => new[] { "h1-topic" };

    [Fact]
    public void SyncQueryFamily_AfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);

        TopicPartition[] partitions = new[] { Tp() };

        Assert.Throws<ObjectDisposedException>(() => consumer.Committed(partitions));
        Assert.Throws<ObjectDisposedException>(
            () => consumer.OffsetsForTimes(new Dictionary<TopicPartition, long> { [Tp()] = 0L }));
        Assert.Throws<ObjectDisposedException>(() => consumer.BeginningOffsets(partitions));
        Assert.Throws<ObjectDisposedException>(() => consumer.EndOffsets(partitions));
        Assert.Throws<ObjectDisposedException>(() => consumer.PartitionsFor("h1-topic"));
        Assert.Throws<ObjectDisposedException>(() => consumer.ListTopics());
    }

    [Fact]
    public void SyncStateReads_AfterDispose_ThrowObjectDisposed()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);

        Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
        Assert.Throws<ObjectDisposedException>(() => consumer.Subscription());
        Assert.Throws<ObjectDisposedException>(() => consumer.Paused());
        Assert.Throws<ObjectDisposedException>(() => consumer.GroupMetadata());
        Assert.Throws<ObjectDisposedException>(() => consumer.Metrics());
        Assert.Throws<ObjectDisposedException>(() => consumer.EnforceRebalance());
        Assert.Throws<ObjectDisposedException>(() => consumer.EnforceRebalance("a reason"));
        Assert.Throws<ObjectDisposedException>(() => consumer.CurrentLag(Tp()));
        Assert.Throws<ObjectDisposedException>(() => consumer.CommitAsync());
        Assert.Throws<ObjectDisposedException>(
            () => consumer.Seek(Tp(), new OffsetAndMetadata(0L, null, null)));
    }

    [Fact]
    public void MockDriverHelpers_AfterDispose_ThrowObjectDisposed()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);

        Assert.Throws<ObjectDisposedException>(() => consumer.AddRecord("h1-topic", 0, 0L, null, null));
        Assert.Throws<ObjectDisposedException>(() => consumer.SetPollException(new KafkaException("boom")));
        Assert.Throws<ObjectDisposedException>(() => consumer.UpdateBeginningOffset("h1-topic", 0, 0L));
        Assert.Throws<ObjectDisposedException>(() => consumer.UpdateEndOffset("h1-topic", 0, 5L));
        Assert.Throws<ObjectDisposedException>(
            () => consumer.UpdatePartitions("h1-topic", 1, 1, "localhost", 9092));
    }

    [Fact]
    public async Task AsyncQueryFamily_AfterDisposeAsync_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer =
            new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.DisposeAsync(consumer);

        TopicPartition[] partitions = new[] { Tp() };

        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.Committed(partitions));
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => consumer.OffsetsForTimes(new Dictionary<TopicPartition, long> { [Tp()] = 0L }));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.BeginningOffsets(partitions));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.EndOffsets(partitions));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.PartitionsFor("h1-topic"));
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.ListTopics());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.Commit());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => consumer.Position(Tp()));
    }

    [Fact]
    public void Wakeup_AfterDispose_IsANoOp_DoesNotThrow()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        TestTimeout.Dispose(consumer);

        consumer.Wakeup();
        consumer.Wakeup();
    }

    [Fact]
    public async Task Wakeup_AfterDisposeAsync_IsANoOp_DoesNotThrow()
    {
        AsyncMockConsumer<byte[], byte[]> consumer =
            new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.DisposeAsync(consumer);

        consumer.Wakeup();
    }

    [Fact]
    public void WakeupFromAnotherThread_RacingDispose_NeitherCrashesNorThrows()
    {
        TestTimeout.Run(
            () =>
            {
                for (int i = 0; i < 200; i++)
                {
                    MockConsumer<byte[], byte[]> consumer =
                        new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

                    using ManualResetEventSlim ready = new ManualResetEventSlim(false);
                    Exception? waker = null;
                    Thread thread = new Thread(() =>
                    {
                        try
                        {
                            ready.Set();
                            for (int j = 0; j < 50; j++)
                            {
                                consumer.Wakeup();
                            }
                        }
                        catch (Exception ex)
                        {
                            waker = ex;
                        }
                    });

                    thread.Start();
                    ready.Wait();
                    consumer.Dispose();
                    thread.Join();

                    Assert.Null(waker);
                }
            },
            s_deadline);
    }

    [Fact]
    public void SyncOps_RacingDisposeFromAnotherThread_NeverUseAfterFree()
    {
        TestTimeout.Run(
            () =>
            {
                for (int i = 0; i < 100; i++)
                {
                    MockConsumer<byte[], byte[]> consumer =
                        new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
                    consumer.Assign(new[] { Tp() });

                    using ManualResetEventSlim ready = new ManualResetEventSlim(false);
                    Exception? unexpected = null;
                    Thread thread = new Thread(() =>
                    {
                        ready.Set();
                        for (int j = 0; j < 50; j++)
                        {
                            try
                            {
                                _ = consumer.Assignment();
                                _ = consumer.Poll(s_pollTimeout);
                            }
                            catch (ObjectDisposedException)
                            {
                            }
                            catch (KafkaException)
                            {
                            }
                            catch (InvalidOperationException)
                            {
                            }
                            catch (Exception ex)
                            {
                                unexpected = ex;
                                return;
                            }
                        }
                    });

                    thread.Start();
                    ready.Wait();
                    consumer.Dispose();
                    thread.Join();

                    Assert.Null(unexpected);
                }
            },
            s_deadline);
    }

    [Fact]
    public void ManyConsumers_SyncOpsThenDispose_NoCrash()
    {
        TestTimeout.Run(
            () =>
            {
                for (int i = 0; i < 100; i++)
                {
                    MockConsumer<byte[], byte[]> consumer =
                        new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
                    consumer.Assign(new[] { Tp() });
                    consumer.AddRecord("h1-topic", 0, 0L, null, new byte[] { 1, 2, 3 });
                    consumer.Dispose();
                }
            },
            s_deadline);
    }

    [Fact]
    public async Task ManyConsumers_UnawaitedAsyncOpThenDispose_NoCrash()
    {
        await TestTimeout.Run(
            async () =>
            {
                for (int i = 0; i < 100; i++)
                {
                    AsyncMockConsumer<byte[], byte[]> consumer =
                        new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
                    _ = consumer.Subscribe(ProofTopic());
                    await consumer.DisposeAsync();
                }
            },
            s_deadline);
    }

    [Fact]
    public void UnawaitedOpThenDispose_FromSeveralThreads_NoCrash()
    {
        TestTimeout.Run(
            () =>
            {
                const int threadCount = 4;
                Thread[] threads = new Thread[threadCount];
                Exception?[] failures = new Exception?[threadCount];

                for (int t = 0; t < threadCount; t++)
                {
                    int index = t;
                    threads[index] = new Thread(() =>
                    {
                        try
                        {
                            for (int i = 0; i < 50; i++)
                            {
                                AsyncMockConsumer<byte[], byte[]> consumer =
                                    new AsyncMockConsumer<byte[], byte[]>(
                                        Serdes.ByteArray, Serdes.ByteArray);
                                _ = consumer.Subscribe(ProofTopic());
                                consumer.Dispose();
                            }
                        }
                        catch (Exception ex)
                        {
                            failures[index] = ex;
                        }
                    });
                }

                foreach (Thread thread in threads)
                {
                    thread.Start();
                }

                foreach (Thread thread in threads)
                {
                    thread.Join();
                }

                Assert.All(failures, Assert.Null);
            },
            s_deadline);
    }
}
