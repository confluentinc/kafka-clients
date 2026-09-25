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
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The M9/P2 consumer <see cref="IConsumerCommon.ClientId"/> surface — the Python sibling's
/// <c>client_id()</c> — through the <b>public</b> consumer types (sync + async, real + mock),
/// broker-free. A non-blocking sync state read on <see cref="IConsumerCommon"/>, so every
/// consumer flavor inherits it.
/// </summary>
/// <remarks>
/// <b>Recorded deviations (PLAN D4/D5).</b> <c>ClientId</c> is a deliberate <b>beyond-Java</b>
/// addition (Java's <c>clientId()</c> is package-private, not on the <c>Consumer</c>
/// interface) and is <b>stricter than Python</b> on concurrent access: the return is a
/// non-nullable <see cref="string"/>, so the concurrent-access null return throws
/// <see cref="InvalidOperationException"/> where Python's unguarded <c>client_id()</c> would
/// return <c>None</c>. The mock's client id is the sentinel <c>"mock-consumer"</c>; a real
/// consumer returns the configured <c>client.id</c>.
/// </remarks>
public sealed class PublicConsumerClientIdTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string MockClientId = "mock-consumer";

    private static Dictionary<string, string> RealConfig(string groupId, string clientId) => new()
    {
        ["bootstrap.servers"] = "localhost:9092",
        ["group.protocol"] = "consumer",
        ["group.id"] = groupId,
        ["client.id"] = clientId,
    };

    // ---- Mock: the sentinel client id, sync + async ----

    [Fact]
    public void ClientId_AsyncMock_ReturnsSentinel()
    {
        using AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        Assert.Equal(MockClientId, consumer.ClientId());
    }

    [Fact]
    public void ClientId_SyncMock_ReturnsSentinel()
    {
        using MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        Assert.Equal(MockClientId, consumer.ClientId());
    }

    // ---- Real consumer: the configured client.id round-trips (sync + async) ----

    [Fact]
    public void ClientId_AsyncRealConsumer_ReturnsConfiguredValue()
    {
        using AsyncKafkaConsumer<byte[], byte[]> consumer = new AsyncKafkaConsumer<byte[], byte[]>(
            RealConfig("clientid-async", "my-async-client"), Serdes.ByteArray, Serdes.ByteArray);
        Assert.Equal("my-async-client", consumer.ClientId());
    }

    [Fact]
    public void ClientId_SyncRealConsumer_ReturnsConfiguredValue()
    {
        using KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(
            RealConfig("clientid-sync", "my-sync-client"), Serdes.ByteArray, Serdes.ByteArray);
        Assert.Equal("my-sync-client", consumer.ClientId());
    }

    [Fact]
    public void ClientId_RealConsumer_NonAsciiRoundTrips()
    {
        // Non-ASCII round-trip through the owned char* (the §B3 UTF-8 contract on
        // Utf8Marshal.PtrToString + string_destroy). The client id is copied out before the
        // native string is freed.
        const string clientId = "café-Ω-日本語-😀";
        using AsyncKafkaConsumer<byte[], byte[]> consumer = new AsyncKafkaConsumer<byte[], byte[]>(
            RealConfig("clientid-nonascii", clientId), Serdes.ByteArray, Serdes.ByteArray);
        Assert.Equal(clientId, consumer.ClientId());
    }

    [Fact]
    public void ClientId_RealConsumer_RepeatedCalls_AreStable()
    {
        // Each call is a fresh owned char* copied out + freed; the value is stable and no
        // handle is leaked/double-freed across repeated reads.
        using AsyncKafkaConsumer<byte[], byte[]> consumer = new AsyncKafkaConsumer<byte[], byte[]>(
            RealConfig("clientid-stable", "stable-client"), Serdes.ByteArray, Serdes.ByteArray);
        for (int i = 0; i < 100; i++)
        {
            Assert.Equal("stable-client", consumer.ClientId());
        }
    }

    // ---- Post-dispose → ObjectDisposedException (sync + async) ----

    [Fact]
    public void ClientId_AfterDispose_ThrowsObjectDisposed()
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        consumer.Dispose();
        Assert.Throws<ObjectDisposedException>(() => consumer.ClientId());
    }

    [Fact]
    public async Task ClientId_AfterDisposeAsync_ThrowsObjectDisposed()
    {
        AsyncMockConsumer<byte[], byte[]> consumer = new AsyncMockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await consumer.DisposeAsync();
        Assert.Throws<ObjectDisposedException>(() => consumer.ClientId());
    }

    // ---- Reachable via the interface ----

    [Fact]
    public void ClientId_ReachableViaIConsumerCommon()
    {
        using MockConsumer<byte[], byte[]> mock = new MockConsumer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        IConsumerCommon consumer = mock;
        Assert.Equal(MockClientId, consumer.ClientId());
    }

    // ---- Concurrent access → InvalidOperationException with the exact message ----

    [Fact]
    public async Task ClientId_ConcurrentWithInFlightPoll_ThrowsInvalidOperationWithExactMessage()
    {
        // Deterministic concurrent-use proof (DoD §3 error-message content), stricter than
        // Python (which returns None). See the metrics sibling for why a real consumer's
        // in-flight poll deterministically holds the single-owner guard (synchronous acquire in
        // Consumer_poll_async before Poll() returns), unlike the non-blocking mock poll.
        using AsyncKafkaConsumer<byte[], byte[]> consumer = new AsyncKafkaConsumer<byte[], byte[]>(
            RealConfig("clientid-concurrent", "concurrent-client"), Serdes.ByteArray, Serdes.ByteArray);

        await TestTimeout.Run(() => consumer.Subscribe(new[] { "clientid-concurrent-probe" }), s_deadline);

        Task<ConsumerRecords<byte[], byte[]>> inFlight = consumer.Poll(TimeSpan.FromSeconds(20));
        try
        {
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => consumer.ClientId());
            Assert.Equal("KafkaConsumer is not safe for multi-threaded access.", ex.Message);
        }
        finally
        {
            consumer.Wakeup();
            try
            {
                await TestTimeout.Run(
                    async () =>
                    {
                        try
                        {
                            await inFlight;
                        }
                        catch (KafkaException)
                        {
                            // Expected: Wakeup faults the in-flight poll.
                        }
                    },
                    s_deadline);
            }
            catch (TimeoutException)
            {
                // Best-effort — the using-scope Dispose still tears the consumer down.
            }
        }
    }
}
