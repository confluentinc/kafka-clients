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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The async producer's teardown: DisposeAsync and Dispose close, then destroy, and return without hanging, also
/// with an unawaited operation in flight; both are idempotent under one latch shared with Close, and churn under
/// GC pressure does not crash.
/// </summary>
public sealed class PublicProducerTeardownTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task DisposeAsync_WhenIdle_Returns()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public void Dispose_WhenIdle_Returns()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    [Fact]
    public async Task DisposeAsync_WithFlushInFlight_Returns()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        // Not awaited: DisposeAsync must return with the flush still in flight.
        _ = producer.Flush();

        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public void Dispose_WithFlushInFlight_Returns()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        _ = producer.Flush();

        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    [Fact]
    public async Task DisposeAsync_CalledTwice_IsSafe()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
        // The second call is a no-op: no double close or destroy.
        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public async Task Dispose_ThenDisposeAsync_IsSafe()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        TestTimeout.Run(producer.Dispose, s_deadline);
        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public async Task Close_ThenDispose_IsSafe()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        // Close wins the latch; the following Dispose is a no-op.
        await TestTimeout.Run(async () => await producer.Close(), s_deadline);
        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    [Fact]
    public async Task ConcurrentDispose_IsSafe()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        // Many threads race Dispose: one wins the latch, the rest are no-ops.
        Task[] disposers = Enumerable
            .Range(0, 8)
            .Select(_ => Task.Run(() => producer.Dispose()))
            .ToArray();

        await TestTimeout.Run(() => Task.WhenAll(disposers), s_deadline);
    }

    [Fact]
    public async Task Flush_AfterDisposeAsync_ThrowsObjectDisposed()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => producer.Flush());
    }

    [Fact]
    public void Dispose_WithFlushInFlight_Churned_ReturnsUnderChurn()
    {
        // Churns create, submit and Dispose under GC pressure: a hang fails the timeout, and a double free or
        // use-after-free on a late callback crashes.
        for (int i = 0; i < 100; i++)
        {
            AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
            _ = producer.Flush();

            TestTimeout.Run(producer.Dispose, s_deadline);

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }
    }

    [Fact]
    public async Task RealProducer_DisposeAsync_Returns()
    {
        // The real AsyncKafkaProducer with no broker tears down without hanging.
        AsyncKafkaProducer<byte[], byte[]> producer = new AsyncKafkaProducer<byte[], byte[]>(
            new System.Collections.Generic.Dictionary<string, string>
            {
                ["bootstrap.servers"] = "localhost:9092",
            },
            Serdes.ByteArray,
            Serdes.ByteArray);

        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
    }
}
