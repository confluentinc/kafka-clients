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
/// A negative <c>TimeoutMs</c> is sent as 0, as Java's <c>calcDeadlineMs</c> clamps it, instead of being rejected: the
/// clamp helper as a table, and through <c>ListTopics</c> what a real client does with the clamped value. The value
/// each RPC sends is pinned where that RPC's submit test has a negative-timeout row.
/// </summary>
public sealed class PublicAdminNegativeTimeoutTests
{
    private const int RequestTimedOutCode = 7;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

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

    [Fact]
    public async Task X2_RealClient_ANegativeTimeout_FailsExactlyAsAZeroTimeout()
    {
        AsyncKafkaAdminClient admin = new AsyncKafkaAdminClient(
            new Dictionary<string, string> { ["bootstrap.servers"] = "127.0.0.1:1" });
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        KafkaException zero = await Failure(admin, 0);
        Assert.Equal(RequestTimedOutCode, zero.Code);

        foreach (int negative in new[] { -1, int.MinValue })
        {
            KafkaException failure = await Failure(admin, negative);
            Assert.Equal(zero.Code, failure.Code);
            Assert.Equal(zero.Message, failure.Message);
        }
    }

    private static Task<KafkaException> Failure(IAsyncAdmin admin, int timeoutMs)
    {
        AsyncListTopicsResult result = admin.ListTopics(new ListTopicsOptions { TimeoutMs = timeoutMs });
        return TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(() => result.Names().Get()), s_deadline);
    }
}
