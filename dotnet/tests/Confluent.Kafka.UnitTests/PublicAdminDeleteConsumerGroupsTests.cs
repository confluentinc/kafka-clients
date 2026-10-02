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
/// The end-to-end behaviour of M15/P5's RPC 4.8 (<c>deleteConsumerGroups</c>) against
/// <see cref="MockAdminClient"/> — no broker.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The mock has no success path, and that is FAITHFUL, not a gap.</b> Java's
/// <c>MockAdminClient.deleteConsumerGroups</c> throws
/// <c>UnsupportedOperationException("Not implemented yet")</c>
/// (<c>MockAdminClient.java:773-775</c>), and the Rust core surfaces that as a resolved map
/// where <b>every requested group id independently</b> carries the identical "unsupported"
/// error — result shape 2 (a <c>Map&lt;String, KafkaFuture&lt;Void&gt;&gt;</c>), the same
/// per-key void bridge as <c>DeleteTopics</c> / <c>CreatePartitions</c>.
/// </para>
/// </remarks>
public sealed class PublicAdminDeleteConsumerGroupsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The code Kafka assigns to <c>UNSUPPORTED_VERSION</c>.</summary>
    private const int UnsupportedVersionCode = 35;

    /// <summary>
    /// The exact message Java's <c>MockAdminClient</c> throws and the Rust mock translates
    /// verbatim.
    /// </summary>
    private const string NotImplemented = "Not implemented yet";

    /// <summary>
    /// Every requested group id gets its own awaitable in <see cref="DeleteConsumerGroupsResult.DeletedGroups"/>,
    /// and each one independently faults with the mock's documented refusal.
    /// </summary>
    [Fact]
    public async Task DeleteConsumerGroups_SurfacesTheMocksDocumentedRefusal_PerGroup()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        DeleteConsumerGroupsResult result = admin.DeleteConsumerGroups(new[] { "p5-group-a", "p5-group-b" });

        Assert.Equal(2, result.DeletedGroups.Count);

        foreach (KeyValuePair<string, Task> entry in result.DeletedGroups)
        {
            KafkaException thrown = await TestTimeout.Run(
                () => Assert.ThrowsAsync<KafkaException>(() => entry.Value), s_deadline);
            Assert.Equal(UnsupportedVersionCode, thrown.Code);
            Assert.Equal(NotImplemented, thrown.Message);
        }

        KafkaException fromAll = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
        Assert.Equal(UnsupportedVersionCode, fromAll.Code);
        Assert.Equal(NotImplemented, fromAll.Message);
    }

    /// <summary>A closed client rejects the call before reaching the core.</summary>
    [Fact]
    public async Task DeleteConsumerGroups_ThrowsAfterDispose()
    {
        MockAdminClient admin = new MockAdminClient(1);
        await admin.DisposeAsync();

        Assert.Throws<ObjectDisposedException>(() => admin.DeleteConsumerGroups(new[] { "p5-disposed" }));
    }

    /// <summary>Null group ids are rejected before any native call — ffi §B5.</summary>
    [Fact]
    public void DeleteConsumerGroups_NullGroupIds_ThrowsArgumentNullException()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        Assert.Throws<ArgumentNullException>(() => admin.DeleteConsumerGroups(null!));
    }

    /// <summary>
    /// ⚠⚠ <b>A per-group failure faults ONLY that group's own <see cref="Task"/></b> — Java's
    /// <c>Map&lt;String, KafkaFuture&lt;Void&gt;&gt;</c>, exercised directly against
    /// <see cref="DeleteConsumerGroupsResult"/>'s constructor rather than the mock.
    /// </summary>
    [Fact]
    public async Task DeletedGroups_EachEntryIsIndependentlyFaultable()
    {
        KafkaException error = new KafkaException(11, "group failure", isRetriable: false);

        Dictionary<string, Task<bool>> sources = new Dictionary<string, Task<bool>>(StringComparer.Ordinal)
        {
            ["good"] = Task.FromResult(true),
            ["bad"] = Task.FromException<bool>(error),
        };

        DeleteConsumerGroupsResult result = new DeleteConsumerGroupsResult(sources, StringComparer.Ordinal);

        await TestTimeout.Run(() => result.DeletedGroups["good"], s_deadline);

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.DeletedGroups["bad"]), s_deadline);
        Assert.Same(error, thrown);
    }

    /// <summary>
    /// <see cref="DeleteConsumerGroupsResult.All"/> faults with the first failure when any
    /// group failed — Java's <c>KafkaFuture.allOf</c>.
    /// </summary>
    [Fact]
    public async Task All_FaultsWithTheFirstFailure_WhenAnyGroupFailed()
    {
        KafkaException error = new KafkaException(11, "group failure", isRetriable: false);

        Dictionary<string, Task<bool>> sources = new Dictionary<string, Task<bool>>(StringComparer.Ordinal)
        {
            ["good"] = Task.FromResult(true),
            ["bad"] = Task.FromException<bool>(error),
        };

        DeleteConsumerGroupsResult mixed = new DeleteConsumerGroupsResult(sources, StringComparer.Ordinal);

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(mixed.All), s_deadline);
        Assert.Same(error, thrown);

        DeleteConsumerGroupsResult clean = new DeleteConsumerGroupsResult(
            new Dictionary<string, Task<bool>>(StringComparer.Ordinal) { ["good"] = Task.FromResult(true) },
            StringComparer.Ordinal);
        await TestTimeout.Run(clean.All, s_deadline);
    }
}
