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
using System.Globalization;
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
/// consumer returns the configured <c>client.id</c> or, when it is unset, the id the core
/// derives as Java does (master #223, see the "default client.id" section below).
/// </remarks>
public sealed class PublicConsumerClientIdTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string MockClientId = "mock-consumer";

    /// <summary>
    /// Nothing listens on port 1: construction never touches the network, and the
    /// never-subscribed consumers below tear down without a coordinator (measured at
    /// ≤ 1 ms per sync <c>Dispose</c> on net10.0, M17/P2 S2e).
    /// </summary>
    private const string UnreachableBootstrap = "127.0.0.1:1";

    /// <summary>The core's message for a <c>group.instance.id</c> that breaks the topic-name rules.</summary>
    private const string InvalidGroupInstanceIdMessage =
        "Group instance id is invalid: 'bad/id' contains one or more characters other than ASCII "
        + "alphanumerics, '.', '_' and '-'";

    /// <summary>Kafka's <c>INVALID_CONFIG</c> (Java <c>InvalidConfigurationException</c>).</summary>
    private const int InvalidConfigCode = 40;

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

    // ---- Real consumer without client.id: the default the core derives (#223, D7) ----
    //
    // Java ConsumerConfig.maybeOverrideClientId (ConsumerConfig.java:723-735):
    // consumer-<group.id>-<group.instance.id>, or consumer-<group.id>-<n> from the
    // process-wide CONSUMER_CLIENT_ID_SEQUENCE starting at 1; a missing group.id formats as
    // the literal "null". The sequence is shared by every consumer in the process (other
    // tests included), so only its shape and its monotonicity are stable — not its value.

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ClientId_RealConsumer_WithoutClientId_DerivesGroupIdAndSequence(bool sync)
    {
        string clientId = ConstructAndReadClientId(sync, UnsetClientIdConfig("clientid-default"));

        SequenceSuffix(clientId, "consumer-clientid-default-");
    }

    [Fact]
    public void ClientId_RealConsumers_WithoutClientId_TakeIncreasingSequenceSuffixes()
    {
        // One process-wide counter for the sync and the async consumer alike.
        int first = SequenceSuffix(
            ConstructAndReadClientId(sync: true, UnsetClientIdConfig("clientid-sequence")),
            "consumer-clientid-sequence-");
        int second = SequenceSuffix(
            ConstructAndReadClientId(sync: false, UnsetClientIdConfig("clientid-sequence")),
            "consumer-clientid-sequence-");

        Assert.True(second > first, $"expected a later suffix than {first}, got {second}");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ClientId_RealConsumer_StaticMember_DerivesGroupIdAndInstanceId(bool sync)
    {
        string clientId = ConstructAndReadClientId(
            sync, UnsetClientIdConfig("clientid-static", groupInstanceId: "instance-1"));

        Assert.Equal("consumer-clientid-static-instance-1", clientId);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ClientId_RealConsumer_WithoutGroupId_FormatsTheNullGroupId(bool sync)
    {
        // An assign-only consumer: Java's String.format("%s", null) yields "null".
        string clientId = ConstructAndReadClientId(sync, UnsetClientIdConfig(groupId: null));

        SequenceSuffix(clientId, "consumer-null-");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ClientId_RealConsumer_GroupInstanceIdWithoutGroupId_IsAcceptedAndFormatsTheNullGroupId(bool sync)
    {
        // Neither Java nor the core rejects a group.instance.id without a group.id at
        // construction; the derived id carries both, the group as "null".
        string clientId = ConstructAndReadClientId(
            sync, UnsetClientIdConfig(groupId: null, groupInstanceId: "instance-2"));

        Assert.Equal("consumer-null-instance-2", clientId);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Ctor_InvalidGroupInstanceId_WithoutClientId_ThrowsInvalidConfig(bool sync)
    {
        // Deriving the id validates group.instance.id first (Java runs
        // JoinGroupRequest.validateGroupInstanceId from ConsumerConfig's
        // postProcessParsedConfig), so the config error is thrown unwrapped.
        KafkaException ex = Assert.Throws<KafkaException>(() => ConstructAndReadClientId(
            sync, UnsetClientIdConfig("clientid-invalid", groupInstanceId: "bad/id")));

        Assert.Equal(InvalidGroupInstanceIdMessage, ex.Message);
        Assert.Equal(InvalidConfigCode, ex.Code);
        Assert.Null(ex.InnerException);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Ctor_InvalidGroupInstanceId_WithExplicitClientId_ThrowsFailedToConstructWithTheCause(bool sync)
    {
        // An explicit client.id skips the derivation, but the constructor validates
        // group.instance.id anyway (Java's GroupRebalanceConfig) and wraps the failure in
        // KafkaConsumer's "Failed to construct kafka consumer".
        Dictionary<string, string> config = UnsetClientIdConfig("clientid-invalid", groupInstanceId: "bad/id");
        config["client.id"] = "explicit-client";

        KafkaException ex = Assert.Throws<KafkaException>(() => ConstructAndReadClientId(sync, config));

        Assert.Equal("Failed to construct kafka consumer", ex.Message);
        KafkaException cause = Assert.IsType<KafkaException>(ex.InnerException);
        Assert.Equal(InvalidGroupInstanceIdMessage, cause.Message);
        Assert.Equal(InvalidConfigCode, cause.Code);
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

    /// <summary>
    /// A real-consumer config with no <c>client.id</c>, so the core derives one.
    /// </summary>
    private static Dictionary<string, string> UnsetClientIdConfig(string? groupId, string? groupInstanceId = null)
    {
        Dictionary<string, string> config = new()
        {
            ["bootstrap.servers"] = UnreachableBootstrap,
            ["group.protocol"] = "consumer",
        };
        if (groupId is not null)
        {
            config["group.id"] = groupId;
        }

        if (groupInstanceId is not null)
        {
            config["group.instance.id"] = groupInstanceId;
        }

        return config;
    }

    /// <summary>
    /// Builds the sync <see cref="KafkaConsumer{TKey, TValue}"/> or the async
    /// <see cref="AsyncKafkaConsumer{TKey, TValue}"/>, reads its client id and disposes it.
    /// </summary>
    private static string ConstructAndReadClientId(bool sync, IReadOnlyDictionary<string, string> config)
    {
        if (sync)
        {
            using KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(
                config, Serdes.ByteArray, Serdes.ByteArray);
            return consumer.ClientId();
        }

        using AsyncKafkaConsumer<byte[], byte[]> asyncConsumer = new AsyncKafkaConsumer<byte[], byte[]>(
            config, Serdes.ByteArray, Serdes.ByteArray);
        return asyncConsumer.ClientId();
    }

    /// <summary>
    /// Asserts <paramref name="clientId"/> is <paramref name="prefix"/> followed by a plain
    /// decimal sequence value of at least 1, and returns that value.
    /// </summary>
    private static int SequenceSuffix(string clientId, string prefix)
    {
        Assert.StartsWith(prefix, clientId, StringComparison.Ordinal);
        string suffix = clientId.Substring(prefix.Length);
        Assert.True(
            int.TryParse(suffix, NumberStyles.None, CultureInfo.InvariantCulture, out int value) && value >= 1,
            $"expected a sequence value >= 1 after \"{prefix}\", got \"{clientId}\"");
        return value;
    }
}
