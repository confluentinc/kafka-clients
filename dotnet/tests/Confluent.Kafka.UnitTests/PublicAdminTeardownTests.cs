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

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Admin construction and teardown as a user drives it: close before destroy, idempotent
/// disposal, and a clear <see cref="ObjectDisposedException"/> afterwards. Every wait is
/// bounded by <see cref="TestTimeout"/>, so a teardown that hangs fails the run instead
/// of blocking it.
/// </summary>
public sealed class PublicAdminTeardownTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly IReadOnlyDictionary<string, string> s_unreachable =
        new Dictionary<string, string>(StringComparer.Ordinal) { ["bootstrap.servers"] = "127.0.0.1:1" };

    [Fact]
    public void Dispose_IsIdempotent()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();

        TestTimeout.Run(admin.Dispose, s_deadline);
        TestTimeout.Run(admin.Dispose, s_deadline);
    }

    [Fact]
    public async Task DisposeAsync_IsIdempotent_AndMixesWithDispose()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();

        await TestTimeout.Run(async () => await admin.DisposeAsync(), s_deadline);
        await TestTimeout.Run(async () => await admin.DisposeAsync(), s_deadline);

        // A blocking Dispose after an async one must not double-close either.
        TestTimeout.Run(admin.Dispose, s_deadline);
    }

    [Fact]
    public async Task Close_WithATimeout_ThenDispose_IsSafe()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();

        await TestTimeout.Run(() => admin.Close(TimeSpan.FromSeconds(5)), s_deadline);

        // Close already won the teardown; Dispose must be a no-op, not a second close and destroy.
        TestTimeout.Run(admin.Dispose, s_deadline);
    }

    [Fact]
    public async Task Close_WithZeroTimeout_IsValid()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();

        await TestTimeout.Run(() => admin.Close(TimeSpan.Zero), s_deadline);
    }

    [Fact]
    public async Task Close_WithANegativeTimeout_ThrowsJavasMessage_BeforeTheClosedCheck()
    {
        AsyncKafkaAdminClient admin = new AsyncKafkaAdminClient(s_unreachable);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        string expected = new ArgumentOutOfRangeException("timeout", "The timeout cannot be negative.").Message;

        ArgumentOutOfRangeException failure = Assert.Throws<ArgumentOutOfRangeException>(
            () => { _ = admin.Close(TimeSpan.FromMilliseconds(-1)); });
        Assert.Equal("timeout", failure.ParamName);
        Assert.Equal(expected, failure.Message);

        await TestTimeout.Run(() => admin.Close(TimeSpan.Zero), s_deadline);
        Assert.Equal(
            expected,
            Assert.Throws<ArgumentOutOfRangeException>(() => { _ = admin.Close(TimeSpan.FromMilliseconds(-1)); }).Message);
    }

    [Fact]
    public async Task AMockClose_ReadsNoTimeout_AndLeavesTheMockOpenUntilItIsDisposed()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();

        await TestTimeout.Run(() => admin.Close(TimeSpan.FromMilliseconds(-1)), s_deadline);
        await TestTimeout.Run(() => admin.Close(), s_deadline);
        using (CancellationTokenSource cancelled = new CancellationTokenSource())
        {
            cancelled.Cancel();
            await TestTimeout.Run(() => admin.Close(cancelled.Token), s_deadline);
        }

        await TestTimeout.DisposeAsync(admin);
        Assert.Equal(
            nameof(MockAdminClient),
            Assert.Throws<ObjectDisposedException>(() => admin.ListTopics()).ObjectName);
    }

    [Fact]
    public void TheSyncCloseWithoutATimeout_ClosesTheClient()
    {
        KafkaAdminClient admin = new KafkaAdminClient(s_unreachable);

        TestTimeout.Run(admin.Close, s_deadline);
        TestTimeout.Run(admin.Close, s_deadline);
        Assert.Throws<ObjectDisposedException>(() => admin.ListTopics());
        TestTimeout.Run(admin.Dispose, s_deadline);
    }

    [Fact]
    public async Task TheAsyncCloseWithoutATimeout_ClosesTheClient()
    {
        AsyncKafkaAdminClient admin = new AsyncKafkaAdminClient(s_unreachable);

        await TestTimeout.Run(() => admin.Close(), s_deadline);
        await TestTimeout.Run(() => admin.Close(CancellationToken.None), s_deadline);
        Assert.Throws<ObjectDisposedException>(() => admin.ListTopics());
        await TestTimeout.DisposeAsync(admin);
    }

    [Fact]
    public async Task AnAlreadyCancelledToken_ThrowsBeforeTheCloseBegins()
    {
        AsyncKafkaAdminClient admin = new AsyncKafkaAdminClient(s_unreachable);
        using CancellationTokenSource cancelled = new CancellationTokenSource();
        cancelled.Cancel();

        Assert.Throws<OperationCanceledException>(() => { _ = admin.Close(cancelled.Token); });
        Assert.Throws<OperationCanceledException>(() => { _ = admin.Close(TimeSpan.Zero, cancelled.Token); });

        await TestTimeout.Run(() => admin.Close(), s_deadline);
        Assert.Throws<ObjectDisposedException>(() => admin.ListTopics());
    }

    [Fact]
    public void SaslPlaintextWithoutAMechanism_FailsWithTheCoresConfigError()
    {
        KafkaException failure = Assert.Throws<KafkaException>(() => new KafkaAdminClient(
            new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["bootstrap.servers"] = "127.0.0.1:1",
                ["security.protocol"] = "SASL_PLAINTEXT",
            }));

        Assert.Equal(-10, failure.Code);
        Assert.Equal(
            "Invalid value GSSAPI for configuration sasl.mechanism: SASL mechanism 'GSSAPI' is not supported by this "
            + "client: only PLAIN is implemented",
            failure.Message);
    }

    [Fact]
    public void CallAfterDispose_ThrowsObjectDisposed()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        TestTimeout.Run(admin.Dispose, s_deadline);

        Assert.Throws<ObjectDisposedException>(
            () => admin.CreateTopics(new[] { new NewTopic("after-dispose", 1, 1) }));
    }

    [Fact]
    public void X9_CallAfterDispose_NamesThePublicType()
    {
        MockAdminClient mock = MockAdminClient.Create().Build();
        TestTimeout.Run(mock.Dispose, s_deadline);
        Assert.Equal(
            nameof(MockAdminClient),
            Assert.Throws<ObjectDisposedException>(() => mock.ListTopics()).ObjectName);

        KafkaAdminClient real = new KafkaAdminClient(
            new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" });
        TestTimeout.Run(real.Dispose, s_deadline);
        Assert.Equal(
            nameof(KafkaAdminClient),
            Assert.Throws<ObjectDisposedException>(() => real.ListTopics()).ObjectName);
    }

    [Fact]
    public async Task CloseAfterDispose_IsANoOp()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        TestTimeout.Run(admin.Dispose, s_deadline);

        // Java's close() is idempotent; a second close must not throw.
        await TestTimeout.Run(() => admin.Close(TimeSpan.FromSeconds(1)), s_deadline);

        AsyncKafkaAdminClient real = new AsyncKafkaAdminClient(s_unreachable);
        await TestTimeout.DisposeAsync(real);
        await TestTimeout.Run(() => real.Close(TimeSpan.FromSeconds(1)), s_deadline);
        await TestTimeout.Run(() => real.Close(), s_deadline);
    }

    [Fact]
    public void ManyClients_CreateAndDisposeCleanly()
    {
        TestTimeout.Run(
            () =>
            {
                for (int i = 0; i < 25; i++)
                {
                    AsyncMockAdminClient.Create().Build().Dispose();
                    MockAdminClient.Create().NumBrokers(1 + (i % 3)).Build().Dispose();
                }

                for (int i = 0; i < 5; i++)
                {
                    new KafkaAdminClient(s_unreachable).Dispose();
                    new AsyncKafkaAdminClient(s_unreachable).DisposeAsync().AsTask().GetAwaiter().GetResult();
                }
            },
            s_deadline);
    }

    [Fact]
    public async Task RealClient_ConstructsAndTearsDownWithoutABroker()
    {
        AsyncKafkaAdminClient admin = new AsyncKafkaAdminClient(
            new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" });

        await TestTimeout.Run(async () => await admin.DisposeAsync(), s_deadline);
    }

    [Fact]
    public void RealClient_RejectsANullConfigValue()
    {
        ArgumentException failure = Assert.Throws<ArgumentException>(
            () => new KafkaAdminClient(new Dictionary<string, string> { ["bootstrap.servers"] = null! }));
        Assert.Equal("config", failure.ParamName);
        Assert.StartsWith(
            "Configuration value for key 'bootstrap.servers' must not be null.",
            failure.Message,
            StringComparison.Ordinal);

        Assert.Equal(
            "config",
            Assert.Throws<ArgumentNullException>(() => new KafkaAdminClient(null!)).ParamName);
    }
}
