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
using System.Linq;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Confluent.Kafka.Common;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>DeleteConsumerGroups</c> against the mocks, which fail every group with the mock's own answer, as Java's mock
/// does; a mixed outcome is built on the result directly, as the mock has no success path.
/// </summary>
public sealed class PublicAdminDeleteConsumerGroupsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string NotImplemented = "Not implemented yet";

    [Fact]
    public async Task DeleteConsumerGroups_SurfacesTheMocksDocumentedRefusal_PerGroup()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        AsyncDeleteConsumerGroupsResult result = admin.DeleteConsumerGroups(new[] { "p5-group-a", "p5-group-b" });

        Assert.Equal(2, result.DeletedGroups.Count);

        foreach (KeyValuePair<string, AsyncKafkaFuture> entry in result.DeletedGroups)
        {
            NotSupportedException thrown = await TestTimeout.Run(
                () => Assert.ThrowsAsync<NotSupportedException>(() => entry.Value.Get()), s_deadline);
            Assert.Equal(NotImplemented, thrown.Message);
        }

        NotSupportedException fromAll = await TestTimeout.Run(
            () => Assert.ThrowsAsync<NotSupportedException>(() => result.All().Get()), s_deadline);
        Assert.Equal(NotImplemented, fromAll.Message);
    }

    [Fact]
    public async Task DeleteConsumerGroups_ARepeatedGroupId_HasOneFuture()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        AsyncDeleteConsumerGroupsResult result = admin.DeleteConsumerGroups(new[] { "p5-group-a", "p5-group-b", "p5-group-a" });

        Assert.Equal(new[] { "p5-group-a", "p5-group-b" }, result.DeletedGroups.Keys.OrderBy(key => key, StringComparer.Ordinal));
        NotSupportedException fromAll = await TestTimeout.Run(
            () => Assert.ThrowsAsync<NotSupportedException>(() => result.All().Get()), s_deadline);
        Assert.Equal(NotImplemented, fromAll.Message);
    }

    [Fact]
    public async Task DeleteConsumerGroups_ThrowsAfterDispose()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await TestTimeout.DisposeAsync(admin);

        Assert.Throws<ObjectDisposedException>(() => admin.DeleteConsumerGroups(new[] { "p5-disposed" }));
    }

    [Fact]
    public void DeleteConsumerGroups_NullGroupIds_ThrowsArgumentNullException()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Throws<ArgumentNullException>(() => admin.DeleteConsumerGroups(null!));
    }

    [Fact]
    public async Task DeletedGroups_EachEntryIsIndependentlyFaultable()
    {
        KafkaException error = new KafkaException(11, "group failure", isRetriable: false);

        Dictionary<string, AsyncKafkaFuture> sources = new Dictionary<string, AsyncKafkaFuture>(StringComparer.Ordinal)
        {
            ["good"] = new AsyncKafkaFuture(TestFutures.Done()),
            ["bad"] = new AsyncKafkaFuture(TestFutures.Failed(error)),
        };

        AsyncDeleteConsumerGroupsResult result = new AsyncDeleteConsumerGroupsResult(sources);

        await TestTimeout.Run(() => result.DeletedGroups["good"].Get(), s_deadline);

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.DeletedGroups["bad"].Get()), s_deadline);
        Assert.Same(error, thrown);
    }

    [Fact]
    public async Task All_FaultsWithTheFirstFailure_WhenAnyGroupFailed()
    {
        KafkaException error = new KafkaException(11, "group failure", isRetriable: false);

        Dictionary<string, AsyncKafkaFuture> sources = new Dictionary<string, AsyncKafkaFuture>(StringComparer.Ordinal)
        {
            ["good"] = new AsyncKafkaFuture(TestFutures.Done()),
            ["bad"] = new AsyncKafkaFuture(TestFutures.Failed(error)),
        };

        AsyncDeleteConsumerGroupsResult mixed = new AsyncDeleteConsumerGroupsResult(sources);

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => mixed.All().Get()), s_deadline);
        Assert.Same(error, thrown);

        AsyncDeleteConsumerGroupsResult clean = new AsyncDeleteConsumerGroupsResult(
            new Dictionary<string, AsyncKafkaFuture>(StringComparer.Ordinal) { ["good"] = new AsyncKafkaFuture(TestFutures.Done()) });
        await TestTimeout.Run(() => clean.All().Get(), s_deadline);
    }
}
