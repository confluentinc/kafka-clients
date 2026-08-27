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
/// M9/P4 H1 — the consumer handle can no longer be freed out from under a native call.
/// Every synchronous consumer P/Invoke now declares its handle parameter as the
/// <c>SafeConsumerHandle</c>, so the interop marshaller holds a reference for the whole
/// call (ffi-marshalling.md §A2). What that buys is checked here in the two ways a
/// broker-free suite can check it:
/// <list type="bullet">
/// <item>
/// <b>Post-close <see cref="ObjectDisposedException"/> across every migrated family</b> —
/// ffi §B2's already-mandated "a call after Dispose throws
/// <see cref="ObjectDisposedException"/>" test, now covering the families H1 migrated
/// rather than only the handful the earlier teardown tests reached. It also pins the
/// error contract H1 must NOT have changed: <c>ThrowIfClosed()</c> still runs first at
/// every site, so the type stays <see cref="ObjectDisposedException"/> for an
/// already-closed consumer.
/// </item>
/// <item>
/// <b>Race canaries</b> — churn a teardown against the one deliberately cross-thread call
/// (<see cref="IConsumerCommon.Wakeup"/>) and against unawaited in-flight operations. These
/// cannot assert on timing, but a use-after-free regression in the migrated surface shows up
/// as a native crash that takes the test host down, which is a loud failure.
/// </item>
/// </list>
/// <b>Determinism ceiling, stated rather than papered over:</b> the canonical H1 scenario is
/// a <see cref="IConsumer{TKey, TValue}.Poll"/> that blocks for seconds while another thread
/// disposes. It is not reachable broker-free — <c>MockConsumer</c>'s poll returns immediately
/// regardless of the timeout, so there is no multi-second window to race. The window itself is
/// therefore verified by inspection (the marshaller's AddRef/Release brackets the native call);
/// what is tested here is the observable contract around it.
/// </summary>
public sealed class PublicConsumerHandleProtectionTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private static TopicPartition Tp() => new TopicPartition("h1-topic", 0);

    private static string[] ProofTopic() => new[] { "h1-topic" };

    [Fact]
    public void SyncQueryFamily_AfterDispose_ThrowsObjectDisposed()
    {
        // H1a migrated Consumer_committed / _offsets_for_times / _beginning_offsets /
        // _end_offsets / _partitions_for / _list_topics (the last two directly, the first
        // three through NativeCollectionQuerySync in H1b). ThrowIfClosed still fires first,
        // so the observable type is unchanged by the retype.
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Dispose();

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
        // H1c migrated Consumer_assignment / _subscription / _paused / _group_metadata /
        // _metrics / _client_id / _enforce_rebalance, and H1a migrated _current_lag and
        // _commit_async (an `_async` name but a sync ABI fn). All still guard first.
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => consumer.Assignment());
        Assert.Throws<ObjectDisposedException>(() => consumer.Subscription());
        Assert.Throws<ObjectDisposedException>(() => consumer.Paused());
        Assert.Throws<ObjectDisposedException>(() => consumer.GroupMetadata());
        Assert.Throws<ObjectDisposedException>(() => consumer.Metrics());
        Assert.Throws<ObjectDisposedException>(() => consumer.ClientId());
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
        // H1b/H1c migrated MockConsumer_add_record / _set_poll_error / _update_partitions /
        // _update_beginning_offsets / _update_end_offsets. They are broker-free test drivers,
        // but they take the consumer handle like everything else, so they are part of the
        // migrated surface and get the same guard.
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => consumer.AddRecord("h1-topic", 0, 0L, null, null));
        Assert.Throws<ObjectDisposedException>(() => consumer.SetPollError("boom"));
        Assert.Throws<ObjectDisposedException>(() => consumer.UpdateBeginningOffset("h1-topic", 0, 0L));
        Assert.Throws<ObjectDisposedException>(() => consumer.UpdateEndOffset("h1-topic", 0, 5L));
        Assert.Throws<ObjectDisposedException>(
            () => consumer.UpdatePartitions("h1-topic", 1, 1, "localhost", 9092));
    }

    [Fact]
    public async Task AsyncQueryFamily_AfterDisposeAsync_ThrowsObjectDisposed()
    {
        // The async surface keeps its span-the-op AddRef (deliberately NOT retyped by H1),
        // so its post-close contract must be unchanged. Asserted here so the H1 slices cannot
        // silently perturb it.
        AsyncMockConsumer<byte[], byte[]> consumer =
            new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await consumer.DisposeAsync();

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
        // H1d's contract, and the reason it needed a catch rather than a bare conversion:
        // Wakeup is documented best-effort and a no-op once closing/closed. The `_closed`
        // flag short-circuits the already-closed case; the catch covers the race where the
        // handle closes between that read and the marshaller's AddRef. Neither may surface an
        // exception from a documented no-op.
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Dispose();

        consumer.Wakeup();
        consumer.Wakeup();
    }

    [Fact]
    public async Task Wakeup_AfterDisposeAsync_IsANoOp_DoesNotThrow()
    {
        AsyncMockConsumer<byte[], byte[]> consumer =
            new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await consumer.DisposeAsync();

        consumer.Wakeup();
    }

    [Fact]
    public void WakeupFromAnotherThread_RacingDispose_NeitherCrashesNorThrows()
    {
        // The L8 shape, which the .NET gRPC harness server reaches by design (its Wakeup RPC
        // is deliberately gate-exempt, because gating it would deadlock behind the poll it
        // must wake). Before H1d, the closed-flag read and the raw handle deref were not
        // atomic, so a teardown landing between them left Consumer_wakeup running on freed
        // memory — a native crash that takes the whole test host with it, not a catchable
        // exception. The canary churns the window many times: post-H1d each Wakeup either
        // succeeds, short-circuits on the flag, or swallows the marshaller's
        // ObjectDisposedException. No other exception is acceptable.
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
        // The H1 hazard on the sync surface, exercised as a crash canary. A blocking Poll
        // cannot be held open broker-free (MockConsumer returns immediately), so this instead
        // hammers the short migrated calls against a concurrent teardown. Pre-H1 a losing
        // racer passed a freed pointer to native; post-H1 the marshaller either completes the
        // call or throws ObjectDisposedException, and ThrowIfClosed catches the common case.
        // Only those two exception types are tolerated; anything else fails, and a genuine
        // use-after-free crashes the host.
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
                                // Expected once teardown wins: either ThrowIfClosed or the
                                // marshaller's closed-handle rejection.
                            }
                            catch (KafkaException)
                            {
                                // Expected: the core rejects a concurrent op / a wakeup lands.
                            }
                            catch (InvalidOperationException)
                            {
                                // Expected: the core's concurrent sync-state-read rejection
                                // (ffi §B5) — a null owned handle mapped by
                                // ThrowIfConcurrentNull. H1 does not change this contract.
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
    public void ManyConsumers_UnawaitedSyncOpThenDispose_NoCrash()
    {
        // The M4 deferred-destroy path as a crash canary (plan §4.5): it cannot prove timing
        // — there is no native-liveness probe, and adding one would be a Rust-core change
        // that decision Q1 rules out — but it does exercise the shape where the handle's
        // reference count is non-zero at Dispose, so the release (and possibly the destroy)
        // happens later and elsewhere. A self-join or a use-after-free in that relocated
        // destroy would crash here.
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
        // The variant ManyConsumers_CreateAndDispose_NoLeakOrCrash cannot reach: that one
        // AWAITS Subscribe first, so the reference count is 1 at teardown and the destroy is
        // immediate. Here the op is deliberately not awaited, so teardown races it and the
        // destroy is deferred to the completion callback — i.e. it runs on the core's own
        // dispatcher thread. That is the leg plan §4.2(d) argues is safe by construction; this
        // is its crash canary.
        await TestTimeout.Run(
            async () =>
            {
                for (int i = 0; i < 100; i++)
                {
                    AsyncMockConsumer<byte[], byte[]> consumer =
                        new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
                    _ = consumer.Subscribe(ProofTopic()); // unawaited, deliberately
                    await consumer.DisposeAsync();
                }
            },
            s_deadline);
    }

    [Fact]
    public void UnawaitedOpThenDispose_FromSeveralThreads_NoCrash()
    {
        // The multi-threaded soak plan §4.5 asks for: several threads each creating a
        // consumer, starting an operation they do not await, and disposing. The point is
        // volume through the deferred-destroy path, not a timing assertion.
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
                                _ = consumer.Subscribe(ProofTopic()); // unawaited
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
