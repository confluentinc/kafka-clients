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

using Confluent.Kafka.GrpcServer.Chaos;

using Xunit;

namespace Confluent.Kafka.GrpcServer.UnitTests;

/// <summary>
/// The producer's absolute send schedule as a pure function of the clock (PLAN §7.1 T13), the
/// arithmetic of Python's <c>_run_producer_sync</c>: no sleeping, every clock reading supplied.
/// </summary>
public sealed class ChaosRateScheduleTests
{
    [Fact]
    public void ZeroRps_NeverWaits_AndNeverMovesTheSchedule()
    {
        ChaosRateSchedule schedule = new ChaosRateSchedule(0, 5.0);

        for (int i = 0; i < 1000; i++)
        {
            Assert.Equal(TimeSpan.Zero, schedule.Advance(5.0));
        }

        Assert.Equal(5.0, schedule.NextDueSeconds);
    }

    [Fact]
    public void OnSchedule_WaitsUntilTheNextDueTime()
    {
        ChaosRateSchedule schedule = new ChaosRateSchedule(10, 0.0);

        // Sent at t=0.04: the next record is due at 0.1, 60 ms away.
        Assert.Equal(TimeSpan.FromMilliseconds(60), schedule.Advance(0.04));
        Assert.Equal(0.1, schedule.NextDueSeconds, 9);

        // Sent exactly on time at t=0.1: the next is due at 0.2, a whole interval away.
        Assert.Equal(TimeSpan.FromMilliseconds(100), schedule.Advance(0.1));
        Assert.Equal(0.2, schedule.NextDueSeconds, 9);
    }

    [Fact]
    public void TheDueTimeIsAbsolute_SoASlowSendShortensTheNextWait()
    {
        ChaosRateSchedule schedule = new ChaosRateSchedule(10, 0.0);

        Assert.Equal(TimeSpan.FromMilliseconds(100), schedule.Advance(0.0));
        Assert.Equal(TimeSpan.FromMilliseconds(30), schedule.Advance(0.17));
    }

    [Fact]
    public void ASubMillisecondWait_RoundsUpToOneMillisecond_NeverToNone()
    {
        // 10 000 rps: a 0.1 ms interval. Rounding down would spin; the overshoot is repaid by
        // not waiting while behind.
        ChaosRateSchedule schedule = new ChaosRateSchedule(10_000, 0.0);

        Assert.Equal(TimeSpan.FromMilliseconds(1), schedule.Advance(0.0));
    }

    [Fact]
    public void Behind_ByLessThanTheMaxLag_DoesNotWait_AndKeepsTheBacklog()
    {
        ChaosRateSchedule schedule = new ChaosRateSchedule(10, 0.0);

        Assert.Equal(TimeSpan.Zero, schedule.Advance(0.5));
        Assert.Equal(0.1, schedule.NextDueSeconds, 9);

        // Still catching up: due 0.2, now 0.5.
        Assert.Equal(TimeSpan.Zero, schedule.Advance(0.5));
        Assert.Equal(0.2, schedule.NextDueSeconds, 9);
    }

    [Fact]
    public void Behind_ByMoreThanTheMaxLag_ResumesFromNow()
    {
        ChaosRateSchedule schedule = new ChaosRateSchedule(10, 0.0);

        // Due 0.1, now 2.0: 1.9 s behind, more than _MAX_SCHEDULE_LAG_S, so no backlog replay.
        Assert.Equal(TimeSpan.Zero, schedule.Advance(2.0));
        Assert.Equal(2.0, schedule.NextDueSeconds);

        // The schedule now runs from 2.0: the next record is due at 2.1.
        Assert.Equal(TimeSpan.FromMilliseconds(100), schedule.Advance(2.0));
    }

    [Fact]
    public void TheMaxLag_IsOneSecond()
    {
        Assert.Equal(1.0, ChaosRateSchedule.MaxLagSeconds);
    }
}
