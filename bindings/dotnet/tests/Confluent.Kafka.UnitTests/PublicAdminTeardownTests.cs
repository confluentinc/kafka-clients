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

    [Fact]
    public void Dispose_IsIdempotent()
    {
        MockAdminClient admin = new MockAdminClient(1);

        TestTimeout.Run(admin.Dispose, s_deadline);
        TestTimeout.Run(admin.Dispose, s_deadline);
    }

    [Fact]
    public async Task DisposeAsync_IsIdempotent_AndMixesWithDispose()
    {
        MockAdminClient admin = new MockAdminClient(1);

        await TestTimeout.Run(async () => await admin.DisposeAsync(), s_deadline);
        await TestTimeout.Run(async () => await admin.DisposeAsync(), s_deadline);

        // A blocking Dispose after an async one must not double-close either.
        TestTimeout.Run(admin.Dispose, s_deadline);
    }

    [Fact]
    public async Task Close_WithATimeout_ThenDispose_IsSafe()
    {
        MockAdminClient admin = new MockAdminClient(1);

        await TestTimeout.Run(() => admin.Close(TimeSpan.FromSeconds(5)), s_deadline);

        // Close already won the teardown latch; Dispose must be a no-op, not a
        // second close-and-destroy.
        TestTimeout.Run(admin.Dispose, s_deadline);
    }

    [Fact]
    public async Task Close_WithZeroTimeout_IsValid()
    {
        MockAdminClient admin = new MockAdminClient(1);

        await TestTimeout.Run(() => admin.Close(TimeSpan.Zero), s_deadline);
    }

    [Fact]
    public async Task Close_WithANegativeTimeout_ThrowsBeforeAnyNativeCall()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        ArgumentOutOfRangeException failure = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => admin.Close(TimeSpan.FromMilliseconds(-1)));
        Assert.Equal("timeout", failure.ParamName);
        Assert.StartsWith("Timeout must not be negative.", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CallAfterDispose_ThrowsObjectDisposed()
    {
        MockAdminClient admin = new MockAdminClient(1);
        TestTimeout.Run(admin.Dispose, s_deadline);

        Assert.Throws<ObjectDisposedException>(
            () => admin.CreateTopics(new[] { new NewTopic("after-dispose", 1, 1) }));
    }

    [Fact]
    public async Task CloseAfterDispose_IsANoOp()
    {
        MockAdminClient admin = new MockAdminClient(1);
        TestTimeout.Run(admin.Dispose, s_deadline);

        // Java's close() is idempotent; a second close must not throw.
        await TestTimeout.Run(() => admin.Close(TimeSpan.FromSeconds(1)), s_deadline);
    }

    [Fact]
    public void ManyClients_CreateAndDisposeCleanly()
    {
        TestTimeout.Run(
            () =>
            {
                for (int i = 0; i < 25; i++)
                {
                    using MockAdminClient admin = new MockAdminClient(1);
                    CreateTopicsResult result = admin.CreateTopics(new[] { new NewTopic($"churn-{i}", 1, 1) });
                    result.All().GetAwaiter().GetResult();
                }
            },
            s_deadline);
    }

    /// <summary>
    /// The real client constructs against an unreachable broker without blocking — Java's
    /// <c>Admin.create</c> does not connect eagerly — and tears down cleanly.
    /// </summary>
    [Fact]
    public async Task RealClient_ConstructsAndTearsDownWithoutABroker()
    {
        KafkaAdminClient admin = new KafkaAdminClient(
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
