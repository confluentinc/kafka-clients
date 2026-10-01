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

using Xunit;

namespace Confluent.Kafka.Soak.Tests;

/// <summary>
/// The producer's pacing arithmetic — what decides the soak's actual message rate, and the
/// one part of the producer loop that can be checked without a broker.
/// </summary>
public sealed class ProducerPacingTests
{
    private const double Interval = 0.010;   // 10 ms, i.e. the 1000 msg/s soak rate

    /// <summary>
    /// The deadline advances by exactly one interval from its own prior value, independent
    /// of when the caller happened to wake. This is the whole point of the absolute
    /// checkpoint: a sleep that overshot is repaid out of the next one rather than
    /// lengthening every period after it.
    /// </summary>
    [Fact]
    public void AdvancesFromThePriorDeadlineNotFromNow()
    {
        // Woke 3 ms late — a Task.Delay overshoot, the common case.
        double next = SoakClient.NextPacingCheckpoint(checkpoint: 100.0, now: 100.003, Interval);

        Assert.Equal(100.010, next, precision: 9);
    }

    /// <summary>
    /// Over a run the deadlines are exact multiples of the interval, so the achieved rate
    /// converges on the configured one however each individual sleep lands. Drives the same
    /// arithmetic the loop does, with a wake that is late every iteration.
    /// </summary>
    [Fact]
    public void HoldsTheConfiguredRateAcrossManyIterations()
    {
        const int iterations = 10_000;
        double start = 1_000.0;
        double checkpoint = start + Interval;

        for (int i = 0; i < iterations; i++)
        {
            // Every wake is 0.4 ms late: in the relative form that is 4 s of lost time
            // over this run (a 4% rate error); here it must not accumulate at all.
            double now = checkpoint + 0.0004;
            checkpoint = SoakClient.NextPacingCheckpoint(checkpoint, now, Interval);
        }

        Assert.Equal(start + (iterations + 1) * Interval, checkpoint, precision: 6);
    }

    /// <summary>
    /// A deadline still in the future is returned untouched, so a batch that overran by
    /// less than the bound IS caught up — the clamp must not swallow ordinary jitter.
    /// </summary>
    [Fact]
    public void CatchesUpWhenTheDebtIsWithinTheBound()
    {
        // 0.5 s behind schedule: inside MaxPacingCatchUpSeconds, so the debt stands.
        double next = SoakClient.NextPacingCheckpoint(checkpoint: 100.0, now: 100.5, Interval);

        Assert.Equal(100.010, next, precision: 9);
    }

    /// <summary>
    /// Past the bound the debt is dropped instead of repaid. Without this, a Send blocked
    /// on admission for max.block.ms while the broker is unreachable leaves a schedule
    /// minutes behind, and the loop then produces the entire backlog at unbounded speed the
    /// moment the broker returns.
    /// </summary>
    [Fact]
    public void ReAnchorsRatherThanBurstingAfterALongStall()
    {
        // 60 s behind — a max.block.ms admission block.
        double next = SoakClient.NextPacingCheckpoint(checkpoint: 100.0, now: 160.0, Interval);

        Assert.Equal(160.0 + Interval, next, precision: 9);
    }

    /// <summary>
    /// The re-anchor threshold is the documented constant, checked either side of it rather
    /// than at a hardcoded distance, so the test tracks the constant if it is retuned.
    /// </summary>
    [Fact]
    public void ReAnchorsExactlyAtTheDocumentedBound()
    {
        double bound = SoakClient.MaxPacingCatchUpSeconds;

        // Just inside the bound: still catching up.
        Assert.Equal(
            100.0 + Interval,
            SoakClient.NextPacingCheckpoint(100.0, 100.0 + Interval + bound - 0.001, Interval),
            precision: 6);

        // Just outside it: re-anchored to the present.
        double past = 100.0 + Interval + bound + 0.001;
        Assert.Equal(
            past + Interval,
            SoakClient.NextPacingCheckpoint(100.0, past, Interval),
            precision: 6);
    }
}
