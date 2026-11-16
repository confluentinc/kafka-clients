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

namespace Confluent.Kafka.Tools;

/// <summary>
/// Java's <c>org.apache.kafka.server.util.ThroughputThrottler</c>: helps producers throttle throughput. With
/// <c>targetThroughput &gt;= 0</c> the average throughput is about <c>min(targetThroughput, maximum possible)</c>;
/// with <c>targetThroughput &lt; 0</c> nothing is throttled. Between sends, call <see cref="Throttle"/> when
/// <see cref="ShouldThrottle"/> answers <c>true</c>.
/// </summary>
internal sealed class ThroughputThrottler
{
    private const long NsPerMs = 1000000L;

    private const long NsPerSec = 1000 * NsPerMs;

    private const long MinSleepNs = 2 * NsPerMs;

    private readonly object _lock = new object();

    private readonly long _startMs;

    private readonly double _targetThroughput;

    private bool _wakeup;

    internal ThroughputThrottler(double targetThroughput, long startMs)
    {
        _startMs = startMs;
        _targetThroughput = targetThroughput;
        SleepTimeNs = targetThroughput > 0 ? (long)(NsPerSec / targetThroughput) : long.MaxValue;
    }

    internal long SleepTimeNs { get; }

    internal long SleepDeficitNs { get; private set; }

    internal bool ShouldThrottle(long amountSoFar, long sendStartMs)
    {
        if (_targetThroughput < 0)
        {
            return false;
        }

        float elapsedSec = (sendStartMs - _startMs) / 1000.0f;
        return elapsedSec > 0 && ((double)amountSoFar / elapsedSec) > _targetThroughput;
    }

    internal void Throttle()
    {
        if (_targetThroughput == 0)
        {
            lock (_lock)
            {
                while (!_wakeup)
                {
                    Monitor.Wait(_lock);
                }
            }

            return;
        }

        SleepDeficitNs += SleepTimeNs;
        if (SleepDeficitNs >= MinSleepNs)
        {
            long sleepStart = Stopwatch.GetTimestamp();
            lock (_lock)
            {
                long remaining = SleepDeficitNs;
                while (!_wakeup && remaining > 0)
                {
                    Monitor.Wait(_lock, ToTimeout(remaining));
                    long elapsed = Stopwatch.GetElapsedTime(sleepStart).Ticks * 100;
                    remaining = SleepDeficitNs - elapsed;
                }

                _wakeup = false;
            }

            SleepDeficitNs = 0;
        }
    }

    internal void Wakeup()
    {
        lock (_lock)
        {
            _wakeup = true;
            Monitor.PulseAll(_lock);
        }
    }

    private static TimeSpan ToTimeout(long nanoseconds) =>
        TimeSpan.FromMilliseconds(Math.Min(Math.Ceiling(nanoseconds / (double)NsPerMs), int.MaxValue));
}
