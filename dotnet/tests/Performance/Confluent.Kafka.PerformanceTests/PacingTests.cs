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
using System.Collections.Generic;
using System.Globalization;

using Xunit;

namespace Confluent.Kafka.Performance.Tests;

/// <summary>
/// The rate limiter (<c>LIMIT_RPS</c>), as in Python's <c>pacing_wait_s</c> and the C, Rust and Java
/// harnesses: every <c>max(floor(LIMIT_RPS / 10), 1)</c> sends (about 100 ms of traffic) the sender sleeps
/// until <c>first send + sends / LIMIT_RPS</c>. The deadline is cumulative, so a late checkpoint is made up
/// at the next one. A <c>LIMIT_RPS</c> of zero or less means max rate.
/// </summary>
[Collection(PerfEngineCollection.Name)]
public sealed class PacingTests
{
    [Theory]
    [InlineData(500000, 50000)]
    [InlineData(333333, 33333)] // rounded down, as Python's //
    [InlineData(20, 2)]
    [InlineData(19, 1)]
    [InlineData(5, 1)] // never below 1, otherwise messagesSent % 0 would throw
    [InlineData(1, 1)]
    public void Checkpoint_IsATenthOfTheRate_ButNeverBelowOne(int limitRps, long expected)
    {
        Apply(limitRps: limitRps.ToString(CultureInfo.InvariantCulture));

        ProducerBenchmarkConfig config = ProducerBenchmarkConfig.FromEnv();

        Assert.Equal(limitRps, config.LimitRps);
        Assert.Equal(expected, config.PacingCheckpoint);
    }

    [Fact]
    public void Limit_SetsTheMessageCount_ToRateTimesDuration()
    {
        Apply(limitRps: "1000", numMessages: "7", testDurationSeconds: "60");

        Assert.Equal(60_000L, ProducerBenchmarkConfig.FromEnv().NumMessages);
    }

    [Fact]
    public void Limit_MessageCount_IsComputedIn64Bits()
    {
        // 5,000,000 × 600 = 3e9, past int.MaxValue.
        Apply(limitRps: "5000000", testDurationSeconds: "600");

        Assert.Equal(3_000_000_000L, ProducerBenchmarkConfig.FromEnv().NumMessages);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("0")]
    [InlineData("-5")]
    public void NoLimit_ZeroOrNegative_MeansMaxRate(string? limitRps)
    {
        // Python sets limit_rps to None for these: no pacing, and the message count stays NUM_MESSAGES.
        Apply(limitRps: limitRps, numMessages: "123");

        ProducerBenchmarkConfig config = ProducerBenchmarkConfig.FromEnv();

        Assert.Null(config.LimitRps);
        Assert.Equal(0L, config.PacingCheckpoint);
        Assert.Equal(123L, config.NumMessages);
        Assert.Equal(0.0, ProducerBenchmark.PacingWaitSeconds(config, messagesSent: 100, elapsedSeconds: 0.0));
    }

    [Fact]
    public void Wait_IsOnlyAtACheckpoint_UntilTheCumulativeDeadline()
    {
        Apply(limitRps: "1000"); // checkpoint 100: 100 ms of traffic
        ProducerBenchmarkConfig config = ProducerBenchmarkConfig.FromEnv();

        // Between checkpoints there is no wait, however early the sender is.
        Assert.Equal(0.0, ProducerBenchmark.PacingWaitSeconds(config, messagesSent: 99, elapsedSeconds: 0.0));

        // At a checkpoint, the time left until sends / LIMIT_RPS.
        Assert.Equal(0.05, ProducerBenchmark.PacingWaitSeconds(config, messagesSent: 100, elapsedSeconds: 0.05), 9);

        // Late at a checkpoint: no wait (the engine sleeps only for a positive value)...
        Assert.True(ProducerBenchmark.PacingWaitSeconds(config, messagesSent: 200, elapsedSeconds: 0.25) <= 0.0);

        // ...and the deadline does not move, so the next checkpoint waits for the full 300 ms mark.
        Assert.Equal(0.05, ProducerBenchmark.PacingWaitSeconds(config, messagesSent: 300, elapsedSeconds: 0.25), 9);
    }

    [Fact]
    public void Wait_HonoursARateThatIsNotAMultipleOfTen()
    {
        // The deadline divides by LIMIT_RPS, not by the checkpoint, so the rounded-down checkpoint changes only
        // how often the clock is read, not the long-run rate: 33,333 sends are due at 0.099999 s.
        Apply(limitRps: "333333");
        ProducerBenchmarkConfig config = ProducerBenchmarkConfig.FromEnv();

        Assert.Equal(33_333.0 / 333_333.0, ProducerBenchmark.PacingWaitSeconds(config, messagesSent: 33_333, elapsedSeconds: 0.0), 12);
    }

    private static void Apply(string? limitRps, string numMessages = "0", string testDurationSeconds = "600")
    {
        var values = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["NUM_MESSAGES"] = numMessages,
            ["TEST_DURATION_SECONDS"] = testDurationSeconds,
        };
        if (limitRps is not null)
        {
            values["LIMIT_RPS"] = limitRps;
        }

        PerfEngineFixture.Apply(values);
    }
}
