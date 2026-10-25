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
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// <see cref="NativeConsumer"/>'s SubscribeWithCallback on the mock (churned, chained from a continuation, under GC)
/// and the sync Seek error path.
/// </summary>
public sealed class ConsumerCompletionBridgeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static string[] ProofTopic() => new[] { "proof-topic" };

    [Fact]
    public async Task SubscribeWithCallback_OnMock_ResolvesSuccessfully()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        await TestTimeout.Run(() => consumer.SubscribeWithCallback(ProofTopic()), s_deadline);
    }

    [Fact]
    public async Task SubscribeWithCallback_Churned_ResolvesEachTime_NoCorruption()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        for (int i = 0; i < 200; i++)
        {
            await TestTimeout.Run(() => consumer.SubscribeWithCallback(ProofTopic()), s_deadline);
        }
    }

    [Fact]
    public void Seek_UnassignedPartition_ThrowsInvalidOperationException()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => consumer.Seek("proof-topic", 0, 0L));

        Assert.Equal("No current assignment for partition proof-topic-0", ex.Message);
    }

    [Fact]
    public void Seek_Churned_ThrowsEachTime_NoCorruption()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        for (int i = 0; i < 50; i++)
        {
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => consumer.Seek("proof-topic", 0, 0L));
            Assert.Equal("No current assignment for partition proof-topic-0", ex.Message);
        }
    }

    [Fact]
    public async Task ChainedOps_FromContinuation_Succeed_GuardReleasedBeforeCompletion()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        await TestTimeout.Run(
            async () =>
            {
                await consumer.SubscribeWithCallback(ProofTopic());
                await consumer.SubscribeWithCallback(ProofTopic());
                await consumer.SubscribeWithCallback(ProofTopic());
            },
            s_deadline);
    }

    [Fact]
    public async Task InFlightOperation_SurvivesAggressiveGc()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        for (int i = 0; i < 50; i++)
        {
            Task op = consumer.SubscribeWithCallback(ProofTopic());
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            await TestTimeout.Run(() => op, s_deadline);
        }
    }
}
