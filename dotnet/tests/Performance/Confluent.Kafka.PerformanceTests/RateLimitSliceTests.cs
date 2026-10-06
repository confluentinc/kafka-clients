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

using Xunit;

namespace Confluent.Kafka.Performance.Tests;

/// <summary>
/// The rate limiter's pacing slice (<c>LIMIT_RPS_SLICE_MS</c>). The default must reproduce the original
/// per-second quota exactly (Python parity: the limiter checks the clock every <c>limit_rps</c> messages),
/// and a finer slice must scale the per-slice quota so the long-run rate stays <c>LIMIT_RPS</c>.
/// </summary>
[Collection(PerfEngineCollection.Name)]
public sealed class RateLimitSliceTests
{
    [Fact]
    public void DefaultSlice_IsOneSecond_AndQuotaIsLimitRps()
    {
        Apply(new Dictionary<string, string>(StringComparer.Ordinal) { ["LIMIT_RPS"] = "500000" });

        ProducerBenchmarkConfig config = ProducerBenchmarkConfig.FromEnv();

        Assert.Equal(1000, config.LimitRpsSliceMs);
        Assert.Equal(500_000L, config.LimitRpsSliceMessages);
    }

    [Fact]
    public void FinerSlice_ScalesTheQuota()
    {
        Apply(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["LIMIT_RPS"] = "400000",
            ["LIMIT_RPS_SLICE_MS"] = "10",
        });

        ProducerBenchmarkConfig config = ProducerBenchmarkConfig.FromEnv();

        Assert.Equal(10, config.LimitRpsSliceMs);
        Assert.Equal(4_000L, config.LimitRpsSliceMessages);
    }

    [Fact]
    public void SliceQuota_RoundsDown_ButNeverBelowOne()
    {
        // 333,333 × 10 / 1000 = 3,333.33 → 3,333. The tick schedule divides by LIMIT_RPS, not by the
        // slice, so the rounding changes the slice length, not the long-run rate.
        Apply(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["LIMIT_RPS"] = "333333",
            ["LIMIT_RPS_SLICE_MS"] = "10",
        });
        Assert.Equal(3_333L, ProducerBenchmarkConfig.FromEnv().LimitRpsSliceMessages);

        // 50 × 1 / 1000 = 0 → clamp to 1, otherwise messagesSent % 0 would throw.
        Apply(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["LIMIT_RPS"] = "50",
            ["LIMIT_RPS_SLICE_MS"] = "1",
        });
        Assert.Equal(1L, ProducerBenchmarkConfig.FromEnv().LimitRpsSliceMessages);
    }

    [Fact]
    public void NoLimit_SliceIsUnused()
    {
        Apply(new Dictionary<string, string>(StringComparer.Ordinal) { ["LIMIT_RPS_SLICE_MS"] = "10" });

        ProducerBenchmarkConfig config = ProducerBenchmarkConfig.FromEnv();

        Assert.Null(config.LimitRps);
        Assert.Equal(0L, config.LimitRpsSliceMessages);
        Assert.Equal(1000, config.LimitRpsSliceMs);
    }

    [Fact]
    public void NonPositiveSlice_Throws()
    {
        Apply(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["LIMIT_RPS"] = "1000",
            ["LIMIT_RPS_SLICE_MS"] = "0",
        });

        ArgumentException ex = Assert.Throws<ArgumentException>(() => ProducerBenchmarkConfig.FromEnv());
        Assert.Equal("LIMIT_RPS_SLICE_MS must be positive", ex.Message);
    }

    private static void Apply(IReadOnlyDictionary<string, string> values) => PerfEngineFixture.Apply(values);
}
