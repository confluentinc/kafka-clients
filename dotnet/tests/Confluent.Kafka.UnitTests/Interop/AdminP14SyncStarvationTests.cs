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

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The serial collection for tests that cap the process-wide thread pool. Nothing may run beside them: xUnit runs a
/// collection with parallelization disabled after every parallel one has finished.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class ThreadPoolStarvationCollection
{
    /// <summary>The collection name — see <see cref="AdminP14SyncStarvationTests"/>.</summary>
    internal const string Name = "thread-pool-starvation";
}

/// <summary>
/// M15/P14 — the sync admin flavour needs no thread-pool thread. With the pool capped at
/// <see cref="Environment.ProcessorCount"/> and every worker held on a gate, a sync <c>CreateTopics</c> on the public
/// <see cref="MockAdminClient"/> still returns its <c>All().Get()</c> and <c>NumPartitions(t).Get()</c>, while the
/// async flavour's <c>All()</c>, composed the same way, cannot complete until the gate opens; and the sync
/// <c>Close(TimeSpan)</c> still returns within its timeout.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why the saturation is deterministic.</b> The pool's worker count is capped at <c>P</c> (min and max), and
/// <c>2P</c> gate-blocked items are queued on the global queue first. At most <c>P</c> of them can be dequeued while
/// the gate is shut, so the other <c>P</c> stay queued ahead of anything queued later — a work item, a
/// <c>Task.Run</c>, or a continuation that a <see cref="System.Threading.Tasks.TaskCreationOptions.RunContinuationsAsynchronously"/>
/// source schedules. A probe item queued after the blockers confirms it: it must not run before the gate opens.
/// </para>
/// <para>
/// <b>Why the control goes through the submit seam, not <see cref="AsyncMockAdminClient"/>.</b> The core fires the
/// mock's callbacks on its own dispatcher thread, so a real async mock can complete the per-key sources before
/// <c>All()</c> is composed, and <c>Task.WhenAll</c> over completed tasks completes on the spot — the control would
/// pass or fail by a race. The seam holds the completion until after <c>All()</c> and <c>NumPartitions</c> are
/// composed; the production callback then completes the source on this thread, and only the composites'
/// continuations are left for the starved pool. The sync row needs no seam: its latches are set by the dispatcher
/// whenever it fires, before or after <c>Get()</c> is called.
/// </para>
/// <para>
/// <b>What it catches.</b> A sync composite that hands its work to the pool — e.g. a projection computed through
/// <c>Task.Run(...).GetAwaiter().GetResult()</c> — blocks behind the queued blockers, so the sync row's thread does not
/// return within its deadline. Likewise a sync <c>Close</c> built over the async close (<c>close_async</c> plus
/// <c>GetAwaiter().GetResult()</c>): the async close resumes on a pool thread, so the close row does not return.
/// </para>
/// </remarks>
[Collection(ThreadPoolStarvationCollection.Name)]
public sealed class AdminP14SyncStarvationTests
{
    private const string SyncTopic = "starve-sync";
    private const string ControlTopic = "starve-control";

    /// <summary>How long the sync rows may take. They need no pool thread, so they return in well under this.</summary>
    private static readonly TimeSpan s_syncDeadline = TimeSpan.FromSeconds(10);

    /// <summary>The close row's own timeout — the bound its <c>Close</c> must return within.</summary>
    private static readonly TimeSpan s_closeTimeout = TimeSpan.FromSeconds(10);

    /// <summary>How long the control is watched for a completion it must not reach while the pool is starved.</summary>
    private static readonly TimeSpan s_controlWindow = TimeSpan.FromSeconds(2);

    /// <summary>How long the probe is watched for a run it must not reach while the pool is starved.</summary>
    private static readonly TimeSpan s_probeWindow = TimeSpan.FromMilliseconds(500);

    /// <summary>How long the blockers get to occupy the workers before the probe is queued, at most.</summary>
    private static readonly TimeSpan s_settleDeadline = TimeSpan.FromSeconds(10);

    /// <summary>How long the count of started blockers must hold still to count as settled.</summary>
    private static readonly TimeSpan s_settleQuiet = TimeSpan.FromMilliseconds(200);

    /// <summary>How long everything gets to drain once the gate is open.</summary>
    private static readonly TimeSpan s_releaseDeadline = TimeSpan.FromSeconds(30);

    /// <summary>A blocker's own bound, so a test that dies before its <c>finally</c> cannot hold the pool forever.</summary>
    private static readonly TimeSpan s_blockerBound = TimeSpan.FromSeconds(120);

    [Fact]
    public void CreateTopics_OnAStarvedThreadPool_TheSyncGetsReturn_AndTheAsyncControlDoesNot()
    {
        using MockAdminClient sync = new MockAdminClient(1);
        NativeAdminClient control = NativeAdminClient.CreateMock(1);

        StarvedPool pool = new StarvedPool();
        Thread? syncRow = null;
        AsyncKafkaFuture controlAll = default;
        AsyncKafkaFuture<int> controlPartitions = default;

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

            // Control row: the async flavour's composites, built before the completion arrives.
            AdminCallbacks.CreateTopicsCallback? callback = null;
            IntPtr userData = IntPtr.Zero;
            AsyncCreateTopicsResult controlResult = AsyncCreateTopicsResult.From(control.CreateTopics(
                new[] { new NewTopic(ControlTopic, 1, 1) },
                options: null,
                AdminFlavour.Async,
                (nativeHandle, topics, count, timeoutMs, validateOnly, retryOnQuotaViolation, submitCallback, submitUserData) =>
                {
                    callback = submitCallback;
                    userData = submitUserData;
                }));
            controlAll = controlResult.All();
            controlPartitions = controlResult.NumPartitions(ControlTopic);

            using (Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(ControlTopic))
            {
                Assert.NotNull(callback);
                callback!(key.Pointer, IntPtr.Zero, AdminP4OperationLifetimeTests.MakeError(41, "control failed"), userData);
            }

            Assert.True(
                controlResult.Values[ControlTopic].Get().IsCompleted,
                "the key's own source completes on the completing thread; only the composites need the pool");
            Assert.False(Completes(controlAll, s_controlWindow), "the async All().Get() completed on a starved pool");
            Assert.False(
                controlPartitions.Get().IsCompleted,
                "the async NumPartitions(t).Get() completed on a starved pool");
            pool.AssertStillSaturated();
        }
        finally
        {
            pool.Release(syncRow);
        }

        pool.AssertReleased();

        // Once the pool is free the control completes with the failure it was given: it was starved, not broken.
        AssertFailedWith(controlAll.Get(), 41, "control failed");
        AssertFailedWith(controlPartitions.Get(), 41, "control failed");

        TestTimeout.Run(control.Dispose, s_releaseDeadline);
    }

    /// <summary>
    /// The close row (PLAN §7.4, S5g): the sync <c>Close(TimeSpan)</c> on the public <see cref="MockAdminClient"/>
    /// returns within its timeout, without error, while every pool worker is held — it parks its own thread inside the
    /// native client's synchronous close and needs no pool thread to resume. Afterwards the client is closed.
    /// </summary>
    [Fact]
    public void Close_OnAStarvedThreadPool_TheSyncCloseReturnsWithinItsTimeout()
    {
        MockAdminClient sync = new MockAdminClient(1);

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

        ObjectDisposedException closed = Assert.Throws<ObjectDisposedException>(() => sync.ListTopics());
        Assert.Equal(nameof(MockAdminClient), closed.ObjectName);
        TestTimeout.Run(sync.Dispose, s_releaseDeadline);
    }

    private static bool Completes(AsyncKafkaFuture future, TimeSpan window)
    {
        try
        {
            return future.Get().Wait(window);
        }
        catch (AggregateException)
        {
            // Wait throws for a task that completed faulted: it completed.
            return true;
        }
    }

    private static void AssertFailedWith(System.Threading.Tasks.Task task, int code, string message)
    {
        Assert.True(SpinWait.SpinUntil(() => task.IsCompleted, s_releaseDeadline), "the control did not complete");
        Assert.True(task.IsFaulted, "the control must fault with the failure it was given");
        KafkaException failure = Assert.IsType<KafkaException>(task.Exception!.InnerException);
        Assert.Equal(code, failure.Code);
        Assert.Equal(message, failure.Message);
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

        /// <summary>
        /// Caps the pool, queues <c>2P</c> gate-blocked items, and asserts a probe queued behind them does not run.
        /// </summary>
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

        /// <summary>Asserts the probe has still not run: the pool stayed saturated for the whole row.</summary>
        internal void AssertStillSaturated() =>
            Assert.True(Volatile.Read(ref _probeRan) == 0, "the pool stayed saturated for the whole test");

        /// <summary>Opens the gate, joins the row's thread, and restores the pool's original limits (max first).</summary>
        internal void Release(Thread? row)
        {
            _gate.Open();
            row?.Join(s_releaseDeadline);
            _restored = ThreadPool.SetMaxThreads(_maxWorkers, _maxIo) & ThreadPool.SetMinThreads(_minWorkers, _minIo);
        }

        /// <summary>Asserts the limits were restored and every blocker and the probe drained.</summary>
        internal void AssertReleased()
        {
            Assert.True(_restored, "the thread pool's min/max could not be restored");
            Assert.True(
                SpinWait.SpinUntil(
                    () => Volatile.Read(ref _finished) == _blockers && Volatile.Read(ref _probeRan) == 1,
                    s_releaseDeadline),
                "the blockers and the probe did not drain once the gate opened");
        }

        /// <summary>
        /// Waits until the started count has held still for <see cref="s_settleQuiet"/>, bounded by
        /// <see cref="s_settleDeadline"/>.
        /// </summary>
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
