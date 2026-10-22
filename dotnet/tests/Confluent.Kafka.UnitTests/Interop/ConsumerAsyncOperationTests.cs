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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// <see cref="NativeConsumer"/>'s Wakeup when idle and during an operation, a pre-cancelled token on an async
/// operation, and the group-metadata read of a real consumer before it joins, a non-ASCII group id included.
/// </summary>
/// <remarks>
/// Subscribe does not consume a wakeup, so the operation still completes normally after one.
/// </remarks>
public sealed class ConsumerAsyncOperationTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static string[] ProofTopic() => new[] { "proof-topic" };

    private static Dictionary<string, string> RealConsumerConfig(string groupId) => new()
    {
        ["bootstrap.servers"] = "localhost:9092",
        ["group.protocol"] = "consumer",
        ["group.id"] = groupId,
    };

    [Fact]
    public async Task Wakeup_WhenIdle_IsSafe_AndConsumerRemainsReusable()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        consumer.Wakeup();

        await TestTimeout.Run(() => consumer.SubscribeWithCallback(ProofTopic()), s_deadline);
    }

    [Fact]
    public async Task Wakeup_DuringInFlightOp_DoesNotCorrupt()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        Task op = consumer.SubscribeWithCallback(ProofTopic());
        consumer.Wakeup();

        await TestTimeout.Run(() => op, s_deadline);
    }

    [Fact]
    public async Task SubscribeWithCallback_PreCanceledToken_ThrowsOperationCanceled()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => consumer.SubscribeWithCallback(ProofTopic(), cts.Token));
    }

    [Fact]
    public void GroupId_WhenIdle_ReturnsConfiguredId()
    {
        NativeConsumer consumer = NativeConsumer.Create(RealConsumerConfig("proof-group"));
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        Assert.Equal("proof-group", consumer.GroupMetadata().GroupId);
    }

    [Fact]
    public void GroupId_NonAsciiId_RoundTrips()
    {
        NativeConsumer consumer = NativeConsumer.Create(RealConsumerConfig("grüße-café-Ω"));
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        Assert.Equal("grüße-café-Ω", consumer.GroupMetadata().GroupId);
    }
}
