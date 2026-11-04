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
using System.Diagnostics;
using System.Threading;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Tests <see cref="Completion{T}"/>: <c>Wait</c> takes a payload that arrives before or during it, and a payload no
/// wait takes is freed once.
/// </summary>
public sealed class CompletionTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void Wait_ReturnsAPayloadCompletedBeforeIt_AndFreesNothing()
    {
        var freed = new ConcurrentQueue<string>();
        var completion = new Completion<string>(freed.Enqueue);
        completion.Complete("payload");

        Assert.True(completion.Wait(TimeSpan.Zero, out string payload));
        Assert.Equal("payload", payload);
        Assert.Empty(freed);
    }

    [Fact]
    public void Wait_ReturnsAPayloadCompletedFromAnotherThreadWhileItWaits_AndFreesNothing()
    {
        var freed = new ConcurrentQueue<string>();
        var completion = new Completion<string>(freed.Enqueue);
        PreJit();
        var untimed = new Waiter(completion, Timeout.InfiniteTimeSpan);
        Assert.True(untimed.IsBlocked());

        RunOnNewThread(() => completion.Complete("payload"));

        Assert.True(untimed.Join());
        Assert.Null(untimed.Error);
        Assert.True(untimed.Arrived);
        Assert.Equal("payload", untimed.Payload);
        Assert.Empty(freed);
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(-1L)]
    [InlineData(long.MinValue)]
    public void Wait_AZeroOrNegativeTimeout_GivesUp_AndALatePayloadIsFreedOnTheDeliveringThread(long ticks)
    {
        var freed = new ConcurrentQueue<(string Payload, int Thread)>();
        var completion = new Completion<string>(payload => freed.Enqueue((payload, Environment.CurrentManagedThreadId)));

        Assert.False(completion.Wait(TimeSpan.FromTicks(ticks), out string payload));
        Assert.Null(payload);
        Assert.Empty(freed);

        int deliverer = RunOnNewThread(() => completion.Complete("late"));

        Assert.Equal(new[] { ("late", deliverer) }, freed.ToArray());
    }

    [Fact]
    public void Wait_APositiveTimeout_GivesUpOnceItPasses_AndALatePayloadIsFreed()
    {
        var timeout = TimeSpan.FromMilliseconds(50);
        var freed = new ConcurrentQueue<int>();
        var completion = new Completion<int>(freed.Enqueue);
        var clock = Stopwatch.StartNew();

        Assert.False(completion.Wait(timeout, out int payload));
        Assert.True(clock.Elapsed >= timeout, $"gave up after {clock.Elapsed}");
        Assert.Equal(0, payload);

        completion.Complete(7);
        Assert.Equal(new[] { 7 }, freed.ToArray());
    }

    [Fact]
    public void Wait_ATimeoutPastIntMaxValueMilliseconds_Waits()
    {
        var freed = new ConcurrentQueue<string>();
        var pastIntMax = new Completion<string>(freed.Enqueue);
        var longest = new Completion<string>(freed.Enqueue);
        PreJit();
        var first = new Waiter(pastIntMax, TimeSpan.FromMilliseconds(int.MaxValue + 1.0));
        var second = new Waiter(longest, TimeSpan.MaxValue);
        Assert.True(first.IsBlocked());
        Assert.True(second.IsBlocked());

        pastIntMax.Complete("first");
        longest.Complete("second");

        Assert.True(first.Join());
        Assert.True(second.Join());
        Assert.Null(first.Error);
        Assert.Null(second.Error);
        Assert.Equal("first", first.Payload);
        Assert.Equal("second", second.Payload);
        Assert.Empty(freed);
    }

    [Fact]
    public void Wait_Interrupted_RethrowsTheInterruption_AndALatePayloadIsFreed()
    {
        var freed = new ConcurrentQueue<string>();
        var completion = new Completion<string>(freed.Enqueue);
        PreJit();
        var waiter = new Waiter(completion, Timeout.InfiniteTimeSpan);
        Assert.True(waiter.IsBlocked());

        waiter.Interrupt();

        Assert.True(waiter.Join());
        Assert.IsType<ThreadInterruptedException>(waiter.Error);
        Assert.False(waiter.Arrived);
        Assert.Null(waiter.Payload);
        Assert.Empty(freed);
        completion.Complete("late");
        Assert.Equal(new[] { "late" }, freed.ToArray());
        Assert.False(completion.Wait(TimeSpan.Zero, out string? after));
        Assert.Null(after);
        Assert.Equal(new[] { "late" }, freed.ToArray());
    }

    [Fact]
    public void Wait_InterruptedWhileThePayloadArrives_FreesThePayloadItLeavesBehind_OnTheWaitingThread()
    {
        var freed = new ConcurrentQueue<(string Payload, int Thread)>();
        var completion = new Completion<string>(payload => freed.Enqueue((payload, Environment.CurrentManagedThreadId)));
        PreJit();
        var waiter = new Waiter(completion, Timeout.InfiniteTimeSpan);
        Assert.True(waiter.IsBlocked());

        lock (completion)
        {
            waiter.Interrupt();
            completion.Complete("left behind");
        }

        Assert.True(waiter.Join());
        Assert.IsType<ThreadInterruptedException>(waiter.Error);
        Assert.False(waiter.Arrived);
        Assert.Equal(new[] { ("left behind", waiter.ThreadId) }, freed.ToArray());
    }

    private static void PreJit()
    {
        var done = new Completion<string>(_ => { });
        done.Complete("warm");
        bool arrived = false;
        RunOnNewThread(() => arrived = done.Wait(TimeSpan.MaxValue, out _));
        Assert.True(arrived);
        var undone = new Completion<string>(_ => { });
        Assert.False(undone.Wait(TimeSpan.Zero, out _));
    }

    private static int RunOnNewThread(Action action)
    {
        int threadId = 0;
        Exception? error = null;
        var thread = new Thread(() =>
        {
            threadId = Environment.CurrentManagedThreadId;
            try
            {
                action();
            }
            catch (Exception e)
            {
                error = e;
            }
        });
        thread.IsBackground = true;
        thread.Start();
        Assert.True(thread.Join(s_deadline));
        Assert.Null(error);
        return threadId;
    }

    /// <summary>
    /// A <c>Wait</c> run on its own background thread, which reports whether that thread is parked in it and keeps the
    /// payload it took, the exception it threw and the thread it ran on.
    /// </summary>
    private sealed class Waiter
    {
        private readonly Thread _thread;

        private volatile bool _calling;

        internal Waiter(Completion<string> completion, TimeSpan timeout)
        {
            _thread = new Thread(() =>
            {
                ThreadId = Environment.CurrentManagedThreadId;
                _calling = true;
                try
                {
                    Arrived = completion.Wait(timeout, out string payload);
                    Payload = payload;
                }
                catch (Exception e)
                {
                    Error = e;
                }
            });
            _thread.IsBackground = true;
            _thread.Start();
        }

        internal int ThreadId { get; private set; }

        internal bool Arrived { get; private set; }

        internal string? Payload { get; private set; }

        internal Exception? Error { get; private set; }

        internal bool IsBlocked() =>
            SpinWait.SpinUntil(
                () => !_thread.IsAlive || (_calling && (_thread.ThreadState & System.Threading.ThreadState.WaitSleepJoin) != 0),
                s_deadline)
            && _thread.IsAlive;

        internal void Interrupt() => _thread.Interrupt();

        internal bool Join() => _thread.Join(s_deadline);
    }
}
