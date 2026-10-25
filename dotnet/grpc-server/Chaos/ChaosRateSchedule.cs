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
/// The producer's absolute send schedule for both flavours, Python's <c>interval</c> / <c>next_due</c>
/// arithmetic as a pure function of the clock; more than <see cref="MaxLagSeconds"/> behind, it resumes
/// from now rather than replaying the backlog.
/// </summary>
internal sealed class ChaosRateSchedule
{
    internal const double MaxLagSeconds = 1.0;

    private readonly double _intervalSeconds;
    private double _nextDueSeconds;

    internal ChaosRateSchedule(uint targetRps, double startSeconds)
    {
        _intervalSeconds = targetRps == 0 ? 0.0 : 1.0 / targetRps;
        _nextDueSeconds = startSeconds;
    }

    internal double NextDueSeconds => _nextDueSeconds;

    internal TimeSpan Advance(double nowSeconds)
    {
        if (_intervalSeconds == 0.0)
        {
            return TimeSpan.Zero;
        }

        _nextDueSeconds += _intervalSeconds;
        if (_nextDueSeconds > nowSeconds)
        {
            // Rounded up to whole milliseconds, the waits' resolution: rounding down would turn
            // every sub-millisecond wait into none and spin the loop, and not waiting while behind
            // repays the overshoot. The epsilon keeps a floating-point residue (100.00000000000001
            // ms) from costing a whole extra millisecond.
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
