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
/// Regression guards for the 64-bit message count (M13/P3 item C). Python computes
/// <c>num_messages = limit_rps * test_duration_s</c> in arbitrary precision; the .NET port did it in
/// 32 bits, so an extreme-but-legal setting overflowed to a NEGATIVE value — which
/// <c>ProducerBenchmark.ContinueSending</c> reads as "no cap", silently changing what was measured.
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

        // int.Parse would throw OverflowException here, where Python accepts the value.
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

        // 5_000_000 * 600 = 3e9. In 32 bits this wrapped to -1_294_967_296, which ContinueSending reads
        // as "no cap, run until the clock runs out".
        Assert.Equal(3_000_000_000L, config.NumMessages);
        Assert.True(config.NumMessages > 0, "an overflowed count goes negative and disables the cap");
    }

    private static void Apply(IReadOnlyDictionary<string, string> values) => PerfEngineFixture.Apply(values);
}
