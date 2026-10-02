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
using System.Reflection;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// M15/P13.5 X2: a negative <c>TimeoutMs</c> is sent as 0, Java's <c>calcDeadlineMs</c> clamp
/// (<c>KafkaAdminClient.java:496-499</c>), instead of being rejected synchronously. The
/// per-RPC submit-seam tests pin the value each RPC sends; this class pins the shared helper
/// and what a real client then does with the clamped value.
/// </summary>
public sealed class PublicAdminNegativeTimeoutTests
{
    /// <summary>REQUEST_TIMED_OUT, the code the core's admin timeout error carries.</summary>
    private const int RequestTimedOutCode = 7;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// <see langword="null"/> maps onto the ABI's "unset" sentinel, a negative value onto 0,
    /// and everything else passes through — <c>Math.max(0, timeoutMs)</c>.
    /// </summary>
    [Theory]
    [InlineData(null, -1)]
    [InlineData(0, 0)]
    [InlineData(-1, 0)]
    [InlineData(int.MinValue, 0)]
    [InlineData(5, 5)]
    public void X2_ToNativeTimeoutMs_IsJavasCalcDeadlineClamp(int? timeoutMs, int expected)
    {
        MethodInfo helper = typeof(NativeAdminClient).GetMethod(
            "ToNativeTimeoutMs", BindingFlags.NonPublic | BindingFlags.Static)!;

        Assert.Equal(expected, (int)helper.Invoke(null, new object?[] { timeoutMs })!);
    }

    /// <summary>
    /// The differential: on a real client that cannot reach a broker, a negative timeout fails
    /// through the result exactly as a zero timeout does — the same <c>Code</c> and the same
    /// <c>Message</c> — and that code is the timeout code. Comparing with 0 asserts the message
    /// without hard-coding the core's wording.
    /// </summary>
    [Fact]
    public async Task X2_RealClient_ANegativeTimeout_FailsExactlyAsAZeroTimeout()
    {
        using KafkaAdminClient admin = new KafkaAdminClient(
            new Dictionary<string, string> { ["bootstrap.servers"] = "127.0.0.1:1" });

        KafkaException zero = await Failure(admin, 0);
        Assert.Equal(RequestTimedOutCode, zero.Code);

        foreach (int negative in new[] { -1, int.MinValue })
        {
            KafkaException failure = await Failure(admin, negative);
            Assert.Equal(zero.Code, failure.Code);
            Assert.Equal(zero.Message, failure.Message);
        }
    }

    private static Task<KafkaException> Failure(IAdmin admin, int timeoutMs)
    {
        ListTopicsResult result = admin.ListTopics(new ListTopicsOptions { TimeoutMs = timeoutMs });
        return TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(result.Names), s_deadline);
    }
}
