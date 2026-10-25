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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>DescribeConsumerGroups</c> end to end against the mocks, which fail every group with the mock's own answer, as
/// Java's mock does: one future per distinct group id, <c>All()</c>, the options and the preconditions.
/// </summary>
public sealed class PublicAdminDescribeConsumerGroupsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string NotImplemented = "Not implemented yet";

    [Fact]
    public async Task DescribeConsumerGroups_FansOutOneFuturePerGroupId()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        AsyncDescribeConsumerGroupsResult result =
            admin.DescribeConsumerGroups(new[] { "p5-dcg-a", "p5-dcg-b", "p5-dcg-c" });

        Assert.Equal(
            new[] { "p5-dcg-a", "p5-dcg-b", "p5-dcg-c" },
            result.DescribedGroups.Keys.OrderBy(id => id, StringComparer.Ordinal));

        Assert.Equal(3, result.DescribedGroups.Values.Distinct().Count());

        foreach (KeyValuePair<string, AsyncKafkaFuture<ConsumerGroupDescription>> described in result.DescribedGroups)
        {
            NotSupportedException failure = await TestTimeout.Run(
                () => Assert.ThrowsAsync<NotSupportedException>(() => described.Value.Get()), s_deadline);

            Assert.Equal(NotImplemented, failure.Message);
        }
    }

    [Fact]
    public async Task DescribeConsumerGroups_AllFaultsWhenAKeyFails()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        AsyncDescribeConsumerGroupsResult result = admin.DescribeConsumerGroups(new[] { "p5-dcg-all" });

        NotSupportedException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<NotSupportedException>(() => result.All().Get()), s_deadline);

        Assert.Equal(NotImplemented, failure.Message);
    }

    [Fact]
    public async Task DescribeConsumerGroups_DeduplicatesIdsOrdinally()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        AsyncDescribeConsumerGroupsResult result =
            admin.DescribeConsumerGroups(new[] { "p5-dup", "p5-dup", "P5-DUP" });

        Assert.Equal(2, result.DescribedGroups.Count);
        Assert.Contains("p5-dup", result.DescribedGroups.Keys);
        Assert.Contains("P5-DUP", result.DescribedGroups.Keys);

        await TestTimeout.Run(
            () => Assert.ThrowsAsync<NotSupportedException>(() => result.All().Get()), s_deadline);
    }

    [Fact]
    public async Task DescribeConsumerGroups_AnEmptyRequestCompletesEmpty()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        AsyncDescribeConsumerGroupsResult result =
            admin.DescribeConsumerGroups(Array.Empty<string>());

        Assert.Empty(result.DescribedGroups);

        IReadOnlyDictionary<string, ConsumerGroupDescription> all =
            await TestTimeout.Run(() => result.All().Get(), s_deadline);

        Assert.Empty(all);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task DescribeConsumerGroups_AcceptsBothAuthorizedOperationsSettings(bool include)
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        AsyncDescribeConsumerGroupsResult result = admin.DescribeConsumerGroups(
            new[] { "p5-dcg-flag" },
            new DescribeConsumerGroupsOptions { IncludeAuthorizedOperations = include });

        NotSupportedException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<NotSupportedException>(() => result.All().Get()), s_deadline);

        Assert.Equal(NotImplemented, failure.Message);
    }

    [Fact]
    public async Task DescribeConsumerGroups_IsReachableThroughTheInterface()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        IAsyncAdmin api = admin;

        AsyncDescribeConsumerGroupsResult result = api.DescribeConsumerGroups(new[] { "p5-dcg-iface" });

        Assert.Equal(new[] { "p5-dcg-iface" }, result.DescribedGroups.Keys);
        await TestTimeout.Run(() => Assert.ThrowsAsync<NotSupportedException>(() => result.All().Get()), s_deadline);
    }

    [Fact]
    public async Task DescribeConsumerGroups_RejectsAMalformedIdList()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        ArgumentNullException missing = Assert.Throws<ArgumentNullException>(
            () => { admin.DescribeConsumerGroups(null!); });
        Assert.Equal("groupIds", missing.ParamName);

        ArgumentException nullElement = Assert.Throws<ArgumentException>(
            () => { admin.DescribeConsumerGroups(new string?[] { "p5-dcg-null", null }!); });
        Assert.Equal("groupIds", nullElement.ParamName);
    }

    [Fact]
    public async Task DescribeConsumerGroups_ANegativeTimeout_IsNotRejectedSynchronously()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        Assert.Null(Record.Exception(
            () =>
            {
                admin.DescribeConsumerGroups(
                    new[] { "p5-dcg-timeout" },
                    new DescribeConsumerGroupsOptions { TimeoutMs = -1 });
            }));
    }

    [Fact]
    public async Task DescribeConsumerGroups_AfterDispose_Throws()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await TestTimeout.DisposeAsync(admin);

        Assert.Throws<ObjectDisposedException>(
            () => { admin.DescribeConsumerGroups(new[] { "p5-dcg-disposed" }); });
    }
}
