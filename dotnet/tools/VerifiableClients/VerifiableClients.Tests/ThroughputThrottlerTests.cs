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
using System.Diagnostics;
using System.Threading;

using Xunit;

namespace Confluent.Kafka.Tools.Tests;

/// <summary>
/// Python's <c>test_throughput_throttler.py</c>: the Rust tool's throttler tests, plus the timed sleep and the
/// deficit carried below the minimum sleep.
/// </summary>
public sealed class ThroughputThrottlerTests
{
    private const long NsPerSec = 1_000_000_000;

    [Fact]
    public void ShouldNeverThrottleWithNegativeTarget()
    {
        var throttler = new ThroughputThrottler(-1, 0);
        Assert.False(throttler.ShouldThrottle(0, 0));
        Assert.False(throttler.ShouldThrottle(1_000_000, 10_000));
    }

    [Fact]
    public void ShouldNotThrottleWhenNoTimeHasElapsed()
    {
        var throttler = new ThroughputThrottler(100, 1000);
        Assert.False(throttler.ShouldThrottle(10, 1000));
    }

    [Fact]
    public void ShouldThrottleWhenRateExceedsTarget()
    {
        var throttler = new ThroughputThrottler(100, 0);
        Assert.True(throttler.ShouldThrottle(200, 1000));
        Assert.False(throttler.ShouldThrottle(50, 1000));
        Assert.False(throttler.ShouldThrottle(100, 1000));
    }

    [Fact]
    public void ShouldThrottleWithZeroTarget()
    {
        var throttler = new ThroughputThrottler(0, 0);
        Assert.True(throttler.ShouldThrottle(1, 1000));
        Assert.False(throttler.ShouldThrottle(0, 1000));
        Assert.False(throttler.ShouldThrottle(1, 0));
    }

    [Fact]
    public void SleepTimeIsNsPerSecOverTarget()
    {
        Assert.Equal(NsPerSec / 1000, new ThroughputThrottler(1000, 0).SleepTimeNs);
        Assert.Equal(NsPerSec, new ThroughputThrottler(1, 0).SleepTimeNs);
        Assert.Equal(333_333_333, new ThroughputThrottler(3, 0).SleepTimeNs);
    }

    [Fact]
    public void SleepTimeIsMaxWhenTargetNotPositive()
    {
        Assert.Equal(long.MaxValue, new ThroughputThrottler(0, 0).SleepTimeNs);
        Assert.Equal(long.MaxValue, new ThroughputThrottler(-5, 0).SleepTimeNs);
    }

    [Fact]
    public void ZeroTargetBlocksUntilWakeup()
    {
        var throttler = new ThroughputThrottler(0, 0);
        Thread thread = ThrottleInThread(throttler);
        Assert.False(thread.Join(TimeSpan.FromMilliseconds(200)));
        throttler.Wakeup();
        Assert.True(thread.Join(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public void ZeroTargetReturnsImmediatelyIfAlreadyWoken()
    {
        var throttler = new ThroughputThrottler(0, 0);
        throttler.Wakeup();
        Assert.True(ThrottleInThread(throttler).Join(TimeSpan.FromSeconds(5)));
        Assert.True(ThrottleInThread(throttler).Join(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public void TimedPathSleepsForTheDeficit()
    {
        var throttler = new ThroughputThrottler(10, 0);
        var started = Stopwatch.StartNew();
        throttler.Throttle();
        Assert.True(started.Elapsed >= TimeSpan.FromMilliseconds(90), started.Elapsed.ToString());
        Assert.Equal(0, throttler.SleepDeficitNs);
    }

    [Fact]
    public void DeficitBelowMinimumSleepIsCarried()
    {
        var throttler = new ThroughputThrottler(1000, 0);
        throttler.Throttle();
        Assert.Equal(1_000_000, throttler.SleepDeficitNs);
        throttler.Throttle();
        Assert.Equal(0, throttler.SleepDeficitNs);
    }

    [Fact]
    public void TimedPathSleepsThenWakeupInterrupts()
    {
        var throttler = new ThroughputThrottler(1, 0);
        var started = Stopwatch.StartNew();
        Thread thread = ThrottleInThread(throttler);
        Thread.Sleep(TimeSpan.FromMilliseconds(50));
        throttler.Wakeup();
        Assert.True(thread.Join(TimeSpan.FromSeconds(5)));
        Assert.True(started.Elapsed < TimeSpan.FromMilliseconds(500), started.Elapsed.ToString());
        Assert.False(throttler.WakeupPending);
    }

    [Fact]
    public void TimedPathWakeupBeforeThrottleIsNotLost()
    {
        var throttler = new ThroughputThrottler(1, 0);
        throttler.Wakeup();
        var started = Stopwatch.StartNew();
        throttler.Throttle();
        Assert.True(started.Elapsed < TimeSpan.FromMilliseconds(500), started.Elapsed.ToString());
    }

    private static Thread ThrottleInThread(ThroughputThrottler throttler)
    {
        var thread = new Thread(throttler.Throttle) { IsBackground = true };
        thread.Start();
        return thread;
    }
}
