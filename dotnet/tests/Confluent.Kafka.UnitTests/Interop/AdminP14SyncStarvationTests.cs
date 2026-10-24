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

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The serial collection for tests that cap the process-wide thread pool. Nothing may run beside them: xUnit runs a
/// collection with parallelization disabled after every parallel one has finished.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ThreadPoolStarvationCollection
{
    internal const string Name = "thread-pool-starvation";
}

/// <summary>
/// The sync admin flavour needs no thread-pool thread. With the pool capped at
/// <see cref="Environment.ProcessorCount"/> and every worker held on a gate, a sync <c>CreateTopics</c> on the public
/// <see cref="MockAdminClient"/> still returns its <c>All().Get()</c> and <c>NumPartitions(t).Get()</c>, and the sync
/// <c>Close(TimeSpan)</c> of a <see cref="KafkaAdminClient"/> still returns within its timeout.
/// </summary>
/// <remarks>
/// The saturation is deterministic: with the pool capped at <c>P</c> workers and <c>2P</c> gate-blocked items queued
/// first, at least <c>P</c> stay queued ahead of anything queued later, which a probe queued behind them confirms. So a
/// sync call that resumes on a pool thread misses its deadline.
/// </remarks>
[Collection(ThreadPoolStarvationCollection.Name)]
public sealed class AdminP14SyncStarvationTests
{
    private const string SyncTopic = "starve-sync";

    private static readonly TimeSpan s_syncDeadline = TimeSpan.FromSeconds(10);

    private static readonly TimeSpan s_closeTimeout = TimeSpan.FromSeconds(10);

    private static readonly TimeSpan s_probeWindow = TimeSpan.FromMilliseconds(500);

    private static readonly TimeSpan s_settleDeadline = TimeSpan.FromSeconds(10);

    private static readonly TimeSpan s_settleQuiet = TimeSpan.FromMilliseconds(200);

    private static readonly TimeSpan s_releaseDeadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_blockerBound = TimeSpan.FromSeconds(120);

    [Fact]
    public void CreateTopics_OnAStarvedThreadPool_TheSyncGetsReturn()
    {
        MockAdminClient sync = MockAdminClient.Create().Build();
        using IDisposable syncDisposal = TestTimeout.Disposal(sync);

        StarvedPool pool = new StarvedPool();
        Thread? syncRow = null;

        try
        {
            pool.Saturate();

            // Sync row: the public mock, on a thread of its own, with every pool worker held.
            int partitions = -1;
            Exception? syncFailure = null;
            syncRow = new Thread(() =>
            {
                try
                {
                    CreateTopicsResult result = sync.CreateTopics(new[] { new NewTopic(SyncTopic, 3, 1) });
                    result.All().Get();
                    partitions = result.NumPartitions(SyncTopic).Get();
                }
                catch (Exception exception)
                {
                    syncFailure = exception;
                }
            })
            {
                IsBackground = true,
                Name = "starvation-sync-row",
            };
            syncRow.Start();
            Assert.True(
                syncRow.Join(s_syncDeadline),
                "the sync CreateTopics All().Get() / NumPartitions(t).Get() did not return on a starved pool");
            Assert.Null(syncFailure);
            Assert.Equal(3, partitions);
            pool.AssertStillSaturated();
        }
        finally
        {
            pool.Release(syncRow);
        }

        pool.AssertReleased();
    }

    [Fact]
    public void Close_OnAStarvedThreadPool_TheSyncCloseReturnsWithinItsTimeout()
    {
        KafkaAdminClient sync = new KafkaAdminClient(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["bootstrap.servers"] = "127.0.0.1:1",
        });

        StarvedPool pool = new StarvedPool();
        Thread? closeRow = null;

        try
        {
            pool.Saturate();

            Exception? closeFailure = null;
            closeRow = new Thread(() =>
            {
                try
                {
                    sync.Close(s_closeTimeout);
                }
                catch (Exception exception)
                {
                    closeFailure = exception;
                }
            })
            {
                IsBackground = true,
                Name = "starvation-close-row",
            };
            closeRow.Start();
            Assert.True(
                closeRow.Join(s_closeTimeout),
                "the sync Close(TimeSpan) did not return within its timeout on a starved pool");
            Assert.Null(closeFailure);
            pool.AssertStillSaturated();
        }
        finally
        {
            pool.Release(closeRow);
        }

        pool.AssertReleased();

        Assert.Throws<ObjectDisposedException>(() => sync.ListTopics());
        TestTimeout.Run(sync.Dispose, s_releaseDeadline);
    }

    /// <summary>
    /// The process-wide thread pool, capped at <see cref="Environment.ProcessorCount"/> workers with every one held on
    /// a gate. Construct it before the <c>try</c>, <see cref="Saturate"/> inside it, <see cref="Release"/> in the
    /// <c>finally</c> and <see cref="AssertReleased"/> after it, so the pool's limits are restored on every path.
    /// </summary>
    private sealed class StarvedPool
    {
        private readonly Gate _gate = new Gate();
        private readonly int _processors = Environment.ProcessorCount;
        private readonly int _minWorkers;
        private readonly int _minIo;
        private readonly int _maxWorkers;
        private readonly int _maxIo;
        private int _blockers;
        private int _started;
        private int _finished;
        private int _probeRan;
        private bool _restored;

        internal StarvedPool()
        {
            ThreadPool.GetMinThreads(out _minWorkers, out _minIo);
            ThreadPool.GetMaxThreads(out _maxWorkers, out _maxIo);
        }

        internal void Saturate()
        {
            Assert.True(ThreadPool.SetMinThreads(_processors, _minIo), "SetMinThreads(ProcessorCount) was refused");
            Assert.True(ThreadPool.SetMaxThreads(_processors, _maxIo), "SetMaxThreads(ProcessorCount) was refused");

            _blockers = 2 * _processors;
            for (int i = 0; i < _blockers; i++)
            {
                ThreadPool.QueueUserWorkItem(_ =>
                {
                    Interlocked.Increment(ref _started);
                    _gate.Wait(s_blockerBound);
                    Interlocked.Increment(ref _finished);
                });
            }

            // Settle only, not an assertion: let the workers take their blockers. How many can is not knowable here —
            // this test may itself run on a pool worker, and the runner can hold another — and it does not matter:
            // the blockers still queued sit ahead of anything queued later, which the probe below proves.
            Settle();

            ThreadPool.QueueUserWorkItem(_ => Interlocked.Exchange(ref _probeRan, 1));
            Thread.Sleep(s_probeWindow);
            Assert.True(
                Volatile.Read(ref _probeRan) == 0,
                "the pool is not saturated: a work item queued behind the blockers ran");
        }

        internal void AssertStillSaturated() =>
            Assert.True(Volatile.Read(ref _probeRan) == 0, "the pool stayed saturated for the whole test");

        internal void Release(Thread? row)
        {
            _gate.Open();
            row?.Join(s_releaseDeadline);
            _restored = ThreadPool.SetMaxThreads(_maxWorkers, _maxIo) & ThreadPool.SetMinThreads(_minWorkers, _minIo);
        }

        internal void AssertReleased()
        {
            Assert.True(_restored, "the thread pool's min/max could not be restored");
            Assert.True(
                SpinWait.SpinUntil(
                    () => Volatile.Read(ref _finished) == _blockers && Volatile.Read(ref _probeRan) == 1,
                    s_releaseDeadline),
                "the blockers and the probe did not drain once the gate opened");
        }

        private void Settle()
        {
            DateTime deadline = DateTime.UtcNow + s_settleDeadline;
            int seen = Volatile.Read(ref _started);
            DateTime quietSince = DateTime.UtcNow;
            while (DateTime.UtcNow < deadline)
            {
                Thread.Sleep(10);
                int now = Volatile.Read(ref _started);
                if (now != seen)
                {
                    seen = now;
                    quietSince = DateTime.UtcNow;
                }
                else if (now > 0 && DateTime.UtcNow - quietSince >= s_settleQuiet)
                {
                    return;
                }
            }
        }
    }

    /// <summary>
    /// A shut gate the blockers wait on. <see cref="Monitor"/>-based so it owns nothing disposable: a blocker still
    /// queued when the test returns runs later against a gate that is simply open.
    /// </summary>
    private sealed class Gate
    {
        private readonly object _lock = new object();
        private bool _open;

        internal void Open()
        {
            lock (_lock)
            {
                _open = true;
                Monitor.PulseAll(_lock);
            }
        }

        internal void Wait(TimeSpan bound)
        {
            DateTime deadline = DateTime.UtcNow + bound;
            lock (_lock)
            {
                while (!_open)
                {
                    TimeSpan remaining = deadline - DateTime.UtcNow;
                    if (remaining <= TimeSpan.Zero)
                    {
                        return;
                    }

                    Monitor.Wait(_lock, remaining);
                }
            }
        }
    }
}
