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
using System.Linq;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// <see cref="NativeConsumer"/>'s teardown: DisposeAsync and Dispose return with an unawaited operation in flight,
/// under GC churn too; double, concurrent and mixed teardown is safe, and a call after it throws
/// <see cref="ObjectDisposedException"/>.
/// </summary>
public sealed class ConsumerAsyncTeardownTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static string[] ProofTopic() => new[] { "proof-topic" };

    [Fact]
    public async Task DisposeAsync_WithOpInFlight_Returns()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        _ = consumer.SubscribeWithCallback(ProofTopic());

        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public async Task DisposeAsync_WhenIdle_Returns()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public void Dispose_WithOpInFlight_Returns()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        _ = consumer.SubscribeWithCallback(ProofTopic());

        TestTimeout.Run(consumer.Dispose, s_deadline);
    }

    [Fact]
    public void Dispose_WithOpInFlight_Churned_ReturnsUnderChurn()
    {
        for (int i = 0; i < 100; i++)
        {
            NativeConsumer consumer = NativeConsumer.CreateMock();
            _ = consumer.SubscribeWithCallback(ProofTopic());

            TestTimeout.Run(consumer.Dispose, s_deadline);

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_IsSafe()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);
        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public async Task Dispose_ThenDisposeAsync_IsSafe()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        TestTimeout.Run(consumer.Dispose, s_deadline);
        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public async Task ConcurrentDispose_IsSafe()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        Task[] disposers = Enumerable
            .Range(0, 8)
            .Select(_ => Task.Run(() => consumer.Dispose()))
            .ToArray();

        await TestTimeout.Run(() => Task.WhenAll(disposers), s_deadline);
    }

    [Fact]
    public async Task Operation_AfterDisposeAsync_ThrowsObjectDisposed()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => consumer.SubscribeWithCallback(ProofTopic()));
    }

    [Fact]
    public async Task Handle_AfterDisposeAsync_ThrowsObjectDisposed()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        await TestTimeout.Run(async () => await consumer.DisposeAsync(), s_deadline);

        Assert.Throws<ObjectDisposedException>(() => consumer.Handle);
    }
}
