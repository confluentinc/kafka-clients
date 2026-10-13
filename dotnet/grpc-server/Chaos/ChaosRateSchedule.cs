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

namespace Confluent.Kafka.GrpcServer.Chaos;

/// <summary>
/// The producer's absolute send schedule, shared by both flavours (PLAN §3.3 "Rate") — the
/// <c>interval</c> / <c>next_due</c> arithmetic of Python's <c>_run_producer_sync</c>, lifted
/// into a pure function of the clock so it can be tested without sleeping (T13).
/// </summary>
/// <remarks>
/// <para>
/// After every send, <see cref="Advance"/> moves the due time on by one interval
/// (<c>1 / target_rps</c>; <c>target_rps == 0</c> means as fast as the client accepts) and
/// returns how long to wait: until the due time when ahead, nothing when behind, and, when more
/// than <see cref="MaxLagSeconds"/> behind (a long <c>Send</c> block while the client waits out
/// a fault), it resumes the schedule from now rather than replaying the backlog.
/// </para>
/// <para>
/// The wait is rounded <b>up</b> to whole milliseconds, the resolution of the waits the loops
/// use (<see cref="System.Threading.ManualResetEventSlim.Wait(TimeSpan)"/> truncates). Rounding
/// down would turn every sub-millisecond wait into none and spin the loop; the overshoot
/// rounding up causes is repaid by not waiting while behind, so the average rate holds (R12,
/// the same property Python's schedule has).
/// </para>
/// </remarks>
internal sealed class ChaosRateSchedule
{
    /// <summary>Python <c>_MAX_SCHEDULE_LAG_S</c>.</summary>
    internal const double MaxLagSeconds = 1.0;

    private readonly double _intervalSeconds;
    private double _nextDueSeconds;

    /// <param name="targetRps">Records per second; 0 = unlimited.</param>
    /// <param name="startSeconds">The clock reading the schedule starts from.</param>
    internal ChaosRateSchedule(uint targetRps, double startSeconds)
    {
        _intervalSeconds = targetRps == 0 ? 0.0 : 1.0 / targetRps;
        _nextDueSeconds = startSeconds;
    }

    /// <summary>The next due time, on the caller's clock (seconds).</summary>
    internal double NextDueSeconds => _nextDueSeconds;

    /// <summary>
    /// Advances the schedule by one record and returns how long to wait at
    /// <paramref name="nowSeconds"/> (<see cref="TimeSpan.Zero"/> for no wait).
    /// </summary>
    internal TimeSpan Advance(double nowSeconds)
    {
        if (_intervalSeconds == 0.0)
        {
            return TimeSpan.Zero;
        }

        _nextDueSeconds += _intervalSeconds;
        if (_nextDueSeconds > nowSeconds)
        {
            // The epsilon keeps a floating-point residue (100.00000000000001 ms) from costing
            // a whole extra millisecond.
            double milliseconds = Math.Ceiling(((_nextDueSeconds - nowSeconds) * 1000.0) - 1e-6);
            return TimeSpan.FromMilliseconds(Math.Max(milliseconds, 1.0));
        }

        if (nowSeconds - _nextDueSeconds > MaxLagSeconds)
        {
            _nextDueSeconds = nowSeconds;
        }

        return TimeSpan.Zero;
    }
}
