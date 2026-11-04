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
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Tests <see cref="CompletionThread"/>, the drain thread the notify hook signals, over fake drain functions driven
/// through the production hook.
/// </summary>
public sealed class CompletionThreadTests
{
    private const int SteadyArrivalCalls = 100;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void Post_FromAnotherThread_RunsOnTheDrainThread_InPostingOrder_AndAfterTheStopRunsInline()
    {
        using var blocked = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        var thread = new CompletionThread(UniqueName(), () => 0);
        try
        {
            int drainThreadId = 0;
            thread.Post(() =>
            {
                drainThreadId = Environment.CurrentManagedThreadId;
                blocked.Set();
                release.Wait(s_deadline);
            });
            Assert.True(blocked.Wait(s_deadline));

            const int Count = 64;
            var ran = new ConcurrentQueue<(int Index, int ThreadId)>();
            using var done = new CountdownEvent(Count);
            var poster = new Thread(() =>
            {
                for (int i = 0; i < Count; i++)
                {
                    int index = i;
                    thread.Post(() =>
                    {
                        ran.Enqueue((index, Environment.CurrentManagedThreadId));
                        done.Signal();
                    });
                }
            });
            poster.Start();
            Assert.True(poster.Join(s_deadline));
            release.Set();
            Assert.True(done.Wait(s_deadline));

            Assert.Equal(Enumerable.Range(0, Count), ran.Select(entry => entry.Index));
            Assert.All(ran, entry => Assert.Equal(drainThreadId, entry.ThreadId));
            Assert.NotEqual(poster.ManagedThreadId, drainThreadId);
            Assert.NotEqual(Environment.CurrentManagedThreadId, drainThreadId);

            thread.Dispose();
            int inlineThreadId = 0;
            thread.Post(() => inlineThreadId = Environment.CurrentManagedThreadId);
            Assert.Equal(Environment.CurrentManagedThreadId, inlineThreadId);
        }
        finally
        {
            release.Set();
            thread.Dispose();
            thread.ReleaseNotifyOpaque();
        }
    }

    [Fact]
    public void Post_RunsAfterEachDrainCall_NotOnlyOnceACallRunsNothing()
    {
        var log = new ConcurrentQueue<string>();
        using var inFirstDrain = new ManualResetEventSlim(false);
        using var posted = new ManualResetEventSlim(false);
        using var thirdDrain = new ManualResetEventSlim(false);
        int calls = 0;
        var thread = new CompletionThread(UniqueName(), () =>
        {
            int call = Interlocked.Increment(ref calls);
            if (call > 3)
            {
                return 0;
            }

            log.Enqueue("drain " + call);
            if (call == 1)
            {
                inFirstDrain.Set();
                posted.Wait(s_deadline);
            }

            if (call == 3)
            {
                thirdDrain.Set();
            }

            return call < 3 ? 1 : 0;
        });
        try
        {
            CompletionThread.NotifyHook(thread.NotifyOpaque);
            Assert.True(inFirstDrain.Wait(s_deadline));
            thread.Post(() => log.Enqueue("posted"));
            posted.Set();
            Assert.True(thirdDrain.Wait(s_deadline));
            thread.Dispose();

            Assert.Equal(new[] { "drain 1", "posted", "drain 2", "drain 3" }, log);
        }
        finally
        {
            posted.Set();
            thread.Dispose();
            thread.ReleaseNotifyOpaque();
        }
    }

    [Fact]
    public void NotifyHook_OnlySignals_TheDrainRunsOnTheDrainThread()
    {
        var drainThreadIds = new ConcurrentQueue<int>();
        using var inFirstDrain = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        using var secondDrain = new ManualResetEventSlim(false);
        int calls = 0;
        var thread = new CompletionThread(UniqueName(), () =>
        {
            drainThreadIds.Enqueue(Environment.CurrentManagedThreadId);
            if (Interlocked.Increment(ref calls) == 1)
            {
                inFirstDrain.Set();
                release.Wait(s_deadline);
            }
            else
            {
                secondDrain.Set();
            }

            return 0;
        });
        try
        {
            CompletionThread.NotifyHook(thread.NotifyOpaque);
            Assert.True(inFirstDrain.Wait(s_deadline));

            int hookThreadId = 0;
            var hook = new Thread(() =>
            {
                hookThreadId = Environment.CurrentManagedThreadId;
                CompletionThread.NotifyHook(thread.NotifyOpaque);
            });
            hook.Start();
            Assert.True(hook.Join(s_deadline));
            Assert.Equal(1, Volatile.Read(ref calls));

            release.Set();
            Assert.True(secondDrain.Wait(s_deadline));
            Assert.Equal(2, Volatile.Read(ref calls));
            Assert.Single(drainThreadIds.Distinct());
            Assert.DoesNotContain(hookThreadId, drainThreadIds);
            Assert.DoesNotContain(Environment.CurrentManagedThreadId, drainThreadIds);
        }
        finally
        {
            release.Set();
            thread.Dispose();
            thread.ReleaseNotifyOpaque();
        }
    }

    [Fact]
    public void RequestTeardown_FromACallback_RunsOnTheDrainThread_AfterTheDrainReturns_ThenTheThreadEnds()
    {
        var log = new ConcurrentQueue<string>();
        using var tornDown = new ManualResetEventSlim(false);
        int drainThreadId = 0;
        int teardownThreadId = 0;
        Thread? drainThread = null;
        CompletionThread thread = null!;
        thread = new CompletionThread(UniqueName(), () =>
        {
            if (drainThread != null)
            {
                return 0;
            }

            drainThread = Thread.CurrentThread;
            drainThreadId = Environment.CurrentManagedThreadId;
            log.Enqueue("drain start");
            thread.RequestTeardown(() =>
            {
                teardownThreadId = Environment.CurrentManagedThreadId;
                log.Enqueue("teardown");
                tornDown.Set();
            });
            log.Enqueue("drain end");
            return 0;
        });
        try
        {
            CompletionThread.NotifyHook(thread.NotifyOpaque);
            Assert.True(tornDown.Wait(s_deadline));
            Assert.True(drainThread!.Join(s_deadline));

            Assert.Equal(new[] { "drain start", "drain end", "teardown" }, log);
            Assert.Equal(drainThreadId, teardownThreadId);

            int inlineThreadId = 0;
            thread.Post(() => inlineThreadId = Environment.CurrentManagedThreadId);
            Assert.Equal(Environment.CurrentManagedThreadId, inlineThreadId);
        }
        finally
        {
            thread.Dispose();
            thread.ReleaseNotifyOpaque();
        }
    }

    [Fact]
    public void AnActionPostedWhileTheTeardownRuns_RunsOnceOnTheDrainThread_AfterTheTeardown()
    {
        var log = new ConcurrentQueue<string>();
        using var inTeardown = new ManualResetEventSlim(false);
        using var releaseTeardown = new ManualResetEventSlim(false);
        int drainThreadId = 0;
        int postedThreadId = 0;
        int postedRuns = 0;
        Thread? drainThread = null;
        CompletionThread thread = null!;
        thread = new CompletionThread(UniqueName(), () =>
        {
            if (drainThread != null)
            {
                return 0;
            }

            drainThread = Thread.CurrentThread;
            drainThreadId = Environment.CurrentManagedThreadId;
            thread.RequestTeardown(() =>
            {
                log.Enqueue("teardown start");
                inTeardown.Set();
                releaseTeardown.Wait(s_deadline);
                log.Enqueue("teardown end");
            });
            return 0;
        });
        try
        {
            CompletionThread.NotifyHook(thread.NotifyOpaque);
            Assert.True(inTeardown.Wait(s_deadline));

            thread.Post(() =>
            {
                Interlocked.Increment(ref postedRuns);
                postedThreadId = Environment.CurrentManagedThreadId;
                log.Enqueue("posted");
            });
            Assert.Equal(0, Volatile.Read(ref postedRuns));
            releaseTeardown.Set();
            Assert.True(drainThread!.Join(s_deadline));

            Assert.Equal(1, Volatile.Read(ref postedRuns));
            Assert.Equal(drainThreadId, postedThreadId);
            Assert.Equal(new[] { "teardown start", "teardown end", "posted" }, log);

            int inlineThreadId = 0;
            thread.Post(() => inlineThreadId = Environment.CurrentManagedThreadId);
            Assert.Equal(Environment.CurrentManagedThreadId, inlineThreadId);
            Assert.Equal(1, Volatile.Read(ref postedRuns));
        }
        finally
        {
            releaseTeardown.Set();
            thread.Dispose();
            thread.ReleaseNotifyOpaque();
        }
    }

    [Fact]
    public void ATeardownRequestedFromACallback_EndsTheDrainLoop_WhileEveryDrainCallStillRunsCallbacks()
    {
        var log = new ConcurrentQueue<string>();
        int calls = 0;
        Thread? drainThread = null;
        CompletionThread thread = null!;
        thread = new CompletionThread(UniqueName(), () =>
        {
            int call = Interlocked.Increment(ref calls);
            log.Enqueue("drain " + call);
            if (call == 1)
            {
                drainThread = Thread.CurrentThread;
                thread.RequestTeardown(() => log.Enqueue("teardown"));
            }

            return call < SteadyArrivalCalls ? 1 : 0;
        });
        try
        {
            CompletionThread.NotifyHook(thread.NotifyOpaque);
            Assert.True(SpinWait.SpinUntil(() => Volatile.Read(ref drainThread) != null, s_deadline));
            Assert.True(drainThread!.Join(s_deadline));

            Assert.Equal(1, Volatile.Read(ref calls));
            Assert.Equal(new[] { "drain 1", "teardown" }, log);
        }
        finally
        {
            thread.Dispose();
            thread.ReleaseNotifyOpaque();
        }
    }

    [Fact]
    public void ADisposeFromACallback_EndsTheDrainLoop_WhileEveryDrainCallStillRunsCallbacks()
    {
        int calls = 0;
        Thread? drainThread = null;
        CompletionThread thread = null!;
        thread = new CompletionThread(UniqueName(), () =>
        {
            int call = Interlocked.Increment(ref calls);
            if (call == 1)
            {
                drainThread = Thread.CurrentThread;
                thread.Dispose();
            }

            return call < SteadyArrivalCalls ? 1 : 0;
        });
        try
        {
            CompletionThread.NotifyHook(thread.NotifyOpaque);
            Assert.True(SpinWait.SpinUntil(() => Volatile.Read(ref drainThread) != null, s_deadline));
            Assert.True(drainThread!.Join(s_deadline));

            Assert.Equal(1, Volatile.Read(ref calls));
        }
        finally
        {
            thread.Dispose();
            thread.ReleaseNotifyOpaque();
        }
    }

    [Fact]
    public void Dispose_JoinsTheThread_AndASecondDisposeReturns()
    {
        using var inDrain = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        Thread? drainThread = null;
        var thread = new CompletionThread(UniqueName(), () =>
        {
            drainThread = Thread.CurrentThread;
            inDrain.Set();
            release.Wait(s_deadline);
            return 0;
        });
        try
        {
            CompletionThread.NotifyHook(thread.NotifyOpaque);
            Assert.True(inDrain.Wait(s_deadline));

            var disposer = new Thread(thread.Dispose);
            disposer.Start();
            Assert.False(disposer.Join(TimeSpan.FromMilliseconds(200)));
            release.Set();
            Assert.True(disposer.Join(s_deadline));
            Assert.False(drainThread!.IsAlive);

            thread.Dispose();
        }
        finally
        {
            release.Set();
            thread.Dispose();
            thread.ReleaseNotifyOpaque();
        }
    }

    [Fact]
    public void TheThread_IsABackgroundThread_WithTheGivenName_AndKnowsItIsCurrent()
    {
        const string Name = "confluent-kafka-producer-completion";
        using var drained = new ManualResetEventSlim(false);
        string? seenName = null;
        bool background = false;
        bool current = false;
        CompletionThread thread = null!;
        thread = new CompletionThread(Name, () =>
        {
            seenName = Thread.CurrentThread.Name;
            background = Thread.CurrentThread.IsBackground;
            current = thread.IsCurrentThread;
            drained.Set();
            return 0;
        });
        try
        {
            CompletionThread.NotifyHook(thread.NotifyOpaque);
            Assert.True(drained.Wait(s_deadline));

            Assert.Equal(Name, seenName);
            Assert.True(background);
            Assert.True(current);
            Assert.False(thread.IsCurrentThread);
        }
        finally
        {
            thread.Dispose();
            thread.ReleaseNotifyOpaque();
        }
    }

    [Fact]
    public void ADrainThatThrows_IsTraced_AndTheNextSignalDrainsAgain()
    {
        string name = UniqueName();
        using TraceCapture trace = TraceCapture.Start();
        using var secondDrain = new ManualResetEventSlim(false);
        int calls = 0;
        var thread = new CompletionThread(name, () =>
        {
            if (Interlocked.Increment(ref calls) == 1)
            {
                throw new InvalidOperationException("drain failed in " + name);
            }

            secondDrain.Set();
            return 0;
        });
        try
        {
            CompletionThread.NotifyHook(thread.NotifyOpaque);
            Assert.True(SpinWait.SpinUntil(() => trace.LinesContaining(name).Count > 0, s_deadline));
            CompletionThread.NotifyHook(thread.NotifyOpaque);
            Assert.True(secondDrain.Wait(s_deadline));

            string line = Assert.Single(trace.LinesContaining(name));
            Assert.Contains("Confluent.Kafka: error running the callbacks of '" + name + "'", line, StringComparison.Ordinal);
            Assert.Contains("System.InvalidOperationException: drain failed in " + name, line, StringComparison.Ordinal);
        }
        finally
        {
            thread.Dispose();
            thread.ReleaseNotifyOpaque();
        }
    }

    [Fact]
    public void APostedActionThatThrows_IsTraced_AndTheNextPostedActionStillRuns()
    {
        string name = UniqueName();
        using TraceCapture trace = TraceCapture.Start();
        using var next = new ManualResetEventSlim(false);
        var thread = new CompletionThread(name, () => 0);
        try
        {
            thread.Post(() => throw new InvalidOperationException("posted action failed in " + name));
            thread.Post(next.Set);
            Assert.True(next.Wait(s_deadline));

            string line = Assert.Single(trace.LinesContaining(name));
            Assert.Contains("Confluent.Kafka: error running a posted action of '" + name + "'", line, StringComparison.Ordinal);
            Assert.Contains("System.InvalidOperationException: posted action failed in " + name, line, StringComparison.Ordinal);
        }
        finally
        {
            thread.Dispose();
            thread.ReleaseNotifyOpaque();
        }
    }

    [Fact]
    public void APostedActionThatThrows_AfterTheStop_IsTraced_NotThrownToThePoster()
    {
        string name = UniqueName();
        using TraceCapture trace = TraceCapture.Start();
        var thread = new CompletionThread(name, () => 0);
        thread.Dispose();
        try
        {
            thread.Post(() => throw new InvalidOperationException("inline action failed in " + name));

            string line = Assert.Single(trace.LinesContaining(name));
            Assert.Contains("Confluent.Kafka: error running a posted action of '" + name + "'", line, StringComparison.Ordinal);
            Assert.Contains("System.InvalidOperationException: inline action failed in " + name, line, StringComparison.Ordinal);
        }
        finally
        {
            thread.ReleaseNotifyOpaque();
        }
    }

    private static string UniqueName() => "completion-thread-test-" + Guid.NewGuid().ToString("N");
}
