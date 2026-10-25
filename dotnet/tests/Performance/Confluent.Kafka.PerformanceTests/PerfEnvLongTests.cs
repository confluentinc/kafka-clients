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
/// Guards the 64-bit message count: Python computes <c>limit_rps * test_duration_s</c> in arbitrary precision, and a
/// 32-bit count would overflow to a negative value that <c>ProducerBenchmark.ContinueSending</c> reads as "no cap".
/// </summary>
[Collection(PerfEngineCollection.Name)]
public sealed class PerfEnvLongTests
{
    [Fact]
    public void GetLong_ParsesBeyondInt32()
    {
        const long Beyond = 3_000_000_000L;
        Apply(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["NUM_MESSAGES"] = "3000000000",
        });

        Assert.Equal(Beyond, PerfEnv.GetLong("NUM_MESSAGES", 0));
        Assert.Equal(Beyond, ProducerBenchmarkConfig.FromEnv().NumMessages);
        Assert.Equal(Beyond, ConsumerBenchmarkConfig.FromEnv().NumMessages);
    }

    [Fact]
    public void GetLong_UnsetOrEmpty_UsesFallback()
    {
        Apply(new Dictionary<string, string>(StringComparer.Ordinal));
        Assert.Equal(7L, PerfEnv.GetLong("NUM_MESSAGES", 7));

        Apply(new Dictionary<string, string>(StringComparer.Ordinal) { ["NUM_MESSAGES"] = string.Empty });
        Assert.Equal(7L, PerfEnv.GetLong("NUM_MESSAGES", 7));
    }

    [Fact]
    public void NumMessages_LimitRpsTimesDuration_DoesNotOverflow()
    {
        Apply(new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["LIMIT_RPS"] = "5000000",
            ["TEST_DURATION_SECONDS"] = "600",
        });

        ProducerBenchmarkConfig config = ProducerBenchmarkConfig.FromEnv();

        Assert.Equal(3_000_000_000L, config.NumMessages);
        Assert.True(config.NumMessages > 0, "an overflowed count goes negative and disables the cap");
    }

    private static void Apply(IReadOnlyDictionary<string, string> values) => PerfEngineFixture.Apply(values);
}
