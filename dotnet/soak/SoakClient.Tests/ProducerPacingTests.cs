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
    private const double Interval = 0.010;

    [Fact]
    public void AdvancesFromThePriorDeadlineNotFromNow()
    {
        double next = SoakClient.NextPacingCheckpoint(checkpoint: 100.0, now: 100.003, Interval);

        Assert.Equal(100.010, next, precision: 9);
    }

    [Fact]
    public void HoldsTheConfiguredRateAcrossManyIterations()
    {
        const int iterations = 10_000;
        double start = 1_000.0;
        double checkpoint = start + Interval;

        for (int i = 0; i < iterations; i++)
        {
            double now = checkpoint + 0.0004;
            checkpoint = SoakClient.NextPacingCheckpoint(checkpoint, now, Interval);
        }

        Assert.Equal(start + (iterations + 1) * Interval, checkpoint, precision: 6);
    }

    [Fact]
    public void CatchesUpWhenTheDebtIsWithinTheBound()
    {
        double next = SoakClient.NextPacingCheckpoint(checkpoint: 100.0, now: 100.5, Interval);

        Assert.Equal(100.010, next, precision: 9);
    }

    [Fact]
    public void ReAnchorsRatherThanBurstingAfterALongStall()
    {
        double next = SoakClient.NextPacingCheckpoint(checkpoint: 100.0, now: 160.0, Interval);

        Assert.Equal(160.0 + Interval, next, precision: 9);
    }

    [Fact]
    public void ReAnchorsExactlyAtTheDocumentedBound()
    {
        double bound = SoakClient.MaxPacingCatchUpSeconds;

        Assert.Equal(
            100.0 + Interval,
            SoakClient.NextPacingCheckpoint(100.0, 100.0 + Interval + bound - 0.001, Interval),
            precision: 6);

        double past = 100.0 + Interval + bound + 0.001;
        Assert.Equal(
            past + Interval,
            SoakClient.NextPacingCheckpoint(100.0, past, Interval),
            precision: 6);
    }
}
