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
using System.Reflection;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>RemoveMembersFromConsumerGroup</c> against the mocks, and the synchronous precondition checks of its result.
/// </summary>
/// <remarks>
/// The mock has no success path: Java's refuses the RPC with "Not implemented yet", and so does the core's. Java has no
/// options-free overload of this RPC, so <c>options</c> is required here too.
/// </remarks>
public sealed class PublicAdminRemoveMembersFromConsumerGroupTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string NotImplemented = "Not implemented yet";

    [Fact]
    public async Task RemoveMembersFromConsumerGroup_SurfacesTheMocksDocumentedRefusal()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        MemberToRemove member = new MemberToRemove("instance-1");
        RemoveMembersFromConsumerGroupOptions options =
            new RemoveMembersFromConsumerGroupOptions(new[] { member });

        AsyncRemoveMembersFromConsumerGroupResult result =
            admin.RemoveMembersFromConsumerGroup("p5-group", options);

        NotSupportedException fromAll = await TestTimeout.Run(
            () => Assert.ThrowsAsync<NotSupportedException>(() => result.All().Get()), s_deadline);
        Assert.Equal(NotImplemented, fromAll.Message);

        // The core attaches no cause to the mock's refusal, so InnerException stays null.
        Assert.Null(fromAll.InnerException);

        NotSupportedException fromMember = await TestTimeout.Run(
            () => Assert.ThrowsAsync<NotSupportedException>(() => result.MemberResult(member).Get()), s_deadline);
        Assert.Equal(NotImplemented, fromMember.Message);
        Assert.Null(fromMember.InnerException);
    }

    [Fact]
    public async Task RemoveMembersFromConsumerGroup_RemoveAllMode_SurfacesTheMocksDocumentedRefusal()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        RemoveMembersFromConsumerGroupOptions options = new RemoveMembersFromConsumerGroupOptions();

        AsyncRemoveMembersFromConsumerGroupResult result =
            admin.RemoveMembersFromConsumerGroup("p5-group-all", options);

        NotSupportedException fromAll = await TestTimeout.Run(
            () => Assert.ThrowsAsync<NotSupportedException>(() => result.All().Get()), s_deadline);
        Assert.Equal(NotImplemented, fromAll.Message);

        ArgumentException thrown = Assert.Throws<ArgumentException>(
            () => result.MemberResult(new MemberToRemove("instance-1")));
        Assert.StartsWith(
            "The method: memberResult is not applicable in 'removeAll' mode", thrown.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RemoveMembersFromConsumerGroup_ThrowsAfterDispose()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await TestTimeout.DisposeAsync(admin);

        RemoveMembersFromConsumerGroupOptions options = new RemoveMembersFromConsumerGroupOptions();

        Assert.Throws<ObjectDisposedException>(
            () => admin.RemoveMembersFromConsumerGroup("p5-disposed", options));
    }

    [Fact]
    public void RemoveMembersFromConsumerGroup_NullGroupId_ThrowsArgumentNullException()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        RemoveMembersFromConsumerGroupOptions options = new RemoveMembersFromConsumerGroupOptions();

        Assert.Throws<ArgumentNullException>(() => admin.RemoveMembersFromConsumerGroup(null!, options));
    }

    [Fact]
    public void RemoveMembersFromConsumerGroup_NullOptions_ThrowsArgumentNullException()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Throws<ArgumentNullException>(() => admin.RemoveMembersFromConsumerGroup("p5-group", null!));
    }

    [Fact]
    public void Options_EmptyMembers_ThrowsArgumentException()
    {
        ArgumentException thrown = Assert.Throws<ArgumentException>(
            () => new RemoveMembersFromConsumerGroupOptions(Array.Empty<MemberToRemove>()));
        Assert.StartsWith("Invalid empty members has been provided", thrown.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Options_NullMembers_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new RemoveMembersFromConsumerGroupOptions(null!));
    }

    [Fact]
    public void MemberToRemove_NullGroupInstanceId_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new MemberToRemove(null!));
    }

    [Fact]
    public async Task MemberResult_NullMember_ThrowsArgumentNullException_Synchronously()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        AsyncRemoveMembersFromConsumerGroupResult result = admin.RemoveMembersFromConsumerGroup(
            "p5-group", new RemoveMembersFromConsumerGroupOptions(new[] { new MemberToRemove("instance-1") }));

        ArgumentNullException thrown = Assert.Throws<ArgumentNullException>(() => result.MemberResult(null!));
        Assert.Equal("member", thrown.ParamName);
    }

    [Fact]
    public async Task MemberResult_MemberNotInOriginalRequest_ThrowsArgumentException_Synchronously()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        AsyncRemoveMembersFromConsumerGroupResult result = admin.RemoveMembersFromConsumerGroup(
            "p5-group", new RemoveMembersFromConsumerGroupOptions(new[] { new MemberToRemove("instance-1") }));

        ArgumentException thrown = Assert.Throws<ArgumentException>(
            () => result.MemberResult(new MemberToRemove("instance-9")));
        Assert.StartsWith("Member instance-9 was not included in the original request", thrown.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ManyMembers_ARepeatCollapsed_EveryAccessorCarriesTheMocksRefusal()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        MemberToRemove first = new MemberToRemove("instance-1");
        MemberToRemove second = new MemberToRemove("instance-2");
        AsyncRemoveMembersFromConsumerGroupResult result = admin.RemoveMembersFromConsumerGroup(
            "p5-group",
            new RemoveMembersFromConsumerGroupOptions(new[] { first, second, new MemberToRemove("instance-1") }));

        NotSupportedException fromAll = await TestTimeout.Run(
            () => Assert.ThrowsAsync<NotSupportedException>(() => result.All().Get()), s_deadline);
        Assert.Equal(NotImplemented, fromAll.Message);

        foreach (MemberToRemove member in new[] { first, second, new MemberToRemove("instance-2") })
        {
            NotSupportedException fromMember = await TestTimeout.Run(
                () => Assert.ThrowsAsync<NotSupportedException>(() => result.MemberResult(member).Get()), s_deadline);
            Assert.Equal(NotImplemented, fromMember.Message);
        }
    }

    [Fact]
    public void MemberResult_OnTheSyncResult_ChecksAlike()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        RemoveMembersFromConsumerGroupResult removeAll =
            admin.RemoveMembersFromConsumerGroup("p5-group", new RemoveMembersFromConsumerGroupOptions());
        RemoveMembersFromConsumerGroupResult some = admin.RemoveMembersFromConsumerGroup(
            "p5-group", new RemoveMembersFromConsumerGroupOptions(new[] { new MemberToRemove("instance-1") }));

        Assert.StartsWith(
            "The method: memberResult is not applicable in 'removeAll' mode",
            Assert.Throws<ArgumentException>(() => removeAll.MemberResult(new MemberToRemove("instance-1"))).Message,
            StringComparison.Ordinal);
        Assert.StartsWith(
            "Member instance-9 was not included in the original request",
            Assert.Throws<ArgumentException>(() => some.MemberResult(new MemberToRemove("instance-9"))).Message,
            StringComparison.Ordinal);
        Assert.Equal("member", Assert.Throws<ArgumentNullException>(() => some.MemberResult(null!)).ParamName);

        NotSupportedException fromMember = Assert.Throws<NotSupportedException>(() => some.MemberResult(new MemberToRemove("instance-1")).Get());
        Assert.Equal(NotImplemented, fromMember.Message);
    }

    [Fact]
    public void TheResultHasNoRemoveAll_TheOptionsOneIsPublic()
    {
        Assert.Null(typeof(RemoveMembersFromConsumerGroupResult).GetProperty(
            "RemoveAll", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance));
        Assert.Null(typeof(AsyncRemoveMembersFromConsumerGroupResult).GetProperty(
            "RemoveAll", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance));

        PropertyInfo? onOptions = typeof(RemoveMembersFromConsumerGroupOptions).GetProperty(
            nameof(RemoveMembersFromConsumerGroupOptions.RemoveAll), BindingFlags.Public | BindingFlags.Instance);
        Assert.NotNull(onOptions);
        Assert.Equal(typeof(bool), onOptions!.PropertyType);
        Assert.True(new RemoveMembersFromConsumerGroupOptions().RemoveAll);
        Assert.False(new RemoveMembersFromConsumerGroupOptions(new[] { new MemberToRemove("instance-1") }).RemoveAll);
    }
}
