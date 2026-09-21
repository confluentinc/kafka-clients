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
/// The end-to-end behaviour of M15/P5's RPC 4.9 (<c>removeMembersFromConsumerGroup</c>)
/// against <see cref="MockAdminClient"/> — no broker — plus the direct-construction tests for
/// <see cref="RemoveMembersFromConsumerGroupResult"/>'s synchronous precondition checks.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The mock has no success path, and that is FAITHFUL, not a gap.</b> Java's
/// <c>MockAdminClient.removeMembersFromConsumerGroup</c> throws
/// <c>UnsupportedOperationException("Not implemented yet")</c>
/// (<c>MockAdminClient.java:801-803</c>), and the Rust core surfaces that as a single faulted
/// future — result shape 3 (one <c>KafkaFuture&lt;Map&lt;MemberIdentity, Errors&gt;&gt;</c>).
/// </para>
/// <para>
/// ⚠ Unlike every other RPC on <see cref="IAdmin"/>, Java has <b>no</b> options-free overload
/// for this one (<c>Admin.java:1269</c>), so <c>options</c> is a required parameter here too —
/// several tests below assert that a <see langword="null"/> <c>options</c> is rejected
/// synchronously.
/// </para>
/// </remarks>
public sealed class PublicAdminRemoveMembersFromConsumerGroupTests
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
    /// Non-removeAll mode: the mock's single call-level failure surfaces through
    /// <see cref="RemoveMembersFromConsumerGroupResult.All"/> and
    /// <see cref="RemoveMembersFromConsumerGroupResult.MemberResult"/> alike.
    /// </summary>
    [Fact]
    public async Task RemoveMembersFromConsumerGroup_SurfacesTheMocksDocumentedRefusal()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        MemberToRemove member = new MemberToRemove("instance-1");
        RemoveMembersFromConsumerGroupOptions options =
            new RemoveMembersFromConsumerGroupOptions(new[] { member });

        RemoveMembersFromConsumerGroupResult result =
            admin.RemoveMembersFromConsumerGroup("p5-group", options);

        Assert.False(result.RemoveAll);

        KafkaException fromAll = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
        Assert.Equal(UnsupportedVersionCode, fromAll.Code);
        Assert.Equal(NotImplemented, fromAll.Message);

        KafkaException fromMember = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.MemberResult(member)), s_deadline);
        Assert.Equal(UnsupportedVersionCode, fromMember.Code);
        Assert.Equal(NotImplemented, fromMember.Message);
    }

    /// <summary>
    /// removeAll mode: same call-level failure, but only <c>All()</c> is reachable —
    /// <c>MemberResult</c> is synchronously rejected in this mode (see the dedicated test below).
    /// </summary>
    [Fact]
    public async Task RemoveMembersFromConsumerGroup_RemoveAllMode_SurfacesTheMocksDocumentedRefusal()
    {
        await using MockAdminClient admin = new MockAdminClient(1);

        RemoveMembersFromConsumerGroupOptions options = new RemoveMembersFromConsumerGroupOptions();

        RemoveMembersFromConsumerGroupResult result =
            admin.RemoveMembersFromConsumerGroup("p5-group-all", options);

        Assert.True(result.RemoveAll);

        KafkaException fromAll = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
        Assert.Equal(UnsupportedVersionCode, fromAll.Code);
        Assert.Equal(NotImplemented, fromAll.Message);
    }

    /// <summary>A closed client rejects the call before reaching the core.</summary>
    [Fact]
    public async Task RemoveMembersFromConsumerGroup_ThrowsAfterDispose()
    {
        MockAdminClient admin = new MockAdminClient(1);
        await admin.DisposeAsync();

        RemoveMembersFromConsumerGroupOptions options = new RemoveMembersFromConsumerGroupOptions();

        Assert.Throws<ObjectDisposedException>(
            () => admin.RemoveMembersFromConsumerGroup("p5-disposed", options));
    }

    /// <summary>Null group id is rejected before any native call — ffi §B5.</summary>
    [Fact]
    public void RemoveMembersFromConsumerGroup_NullGroupId_ThrowsArgumentNullException()
    {
        using MockAdminClient admin = new MockAdminClient(1);
        RemoveMembersFromConsumerGroupOptions options = new RemoveMembersFromConsumerGroupOptions();

        Assert.Throws<ArgumentNullException>(() => admin.RemoveMembersFromConsumerGroup(null!, options));
    }

    /// <summary>
    /// Null <c>options</c> is rejected before any native call — the deviation from every other
    /// RPC's optional-<c>options</c> pattern, forced by Java having no default overload here.
    /// </summary>
    [Fact]
    public void RemoveMembersFromConsumerGroup_NullOptions_ThrowsArgumentNullException()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        Assert.Throws<ArgumentNullException>(() => admin.RemoveMembersFromConsumerGroup("p5-group", null!));
    }

    /// <summary>
    /// An empty member collection is rejected by <see cref="RemoveMembersFromConsumerGroupOptions"/>'s
    /// constructor itself — Java's <c>IllegalArgumentException</c> (<c>:35</c>) — with the exact
    /// message text.
    /// </summary>
    [Fact]
    public void Options_EmptyMembers_ThrowsArgumentException()
    {
        ArgumentException thrown = Assert.Throws<ArgumentException>(
            () => new RemoveMembersFromConsumerGroupOptions(Array.Empty<MemberToRemove>()));
        Assert.StartsWith("Invalid empty members has been provided", thrown.Message, StringComparison.Ordinal);
    }

    /// <summary>Null members collection is rejected before the emptiness check.</summary>
    [Fact]
    public void Options_NullMembers_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new RemoveMembersFromConsumerGroupOptions(null!));
    }

    /// <summary>Null <c>groupInstanceId</c> is rejected by <see cref="MemberToRemove"/>'s constructor.</summary>
    [Fact]
    public void MemberToRemove_NullGroupInstanceId_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new MemberToRemove(null!));
    }

    /// <summary>
    /// ⚠⚠ Direct-construction test for <see cref="RemoveMembersFromConsumerGroupResult.MemberResult"/>'s
    /// synchronous preconditions (Java's <c>:82-87</c>) — these throw <b>out of the call itself</b>,
    /// never through the returned <see cref="Task"/>.
    /// </summary>
    [Fact]
    public void MemberResult_NullMember_ThrowsArgumentNullException_Synchronously()
    {
        RemoveMembersFromConsumerGroupResult result = new RemoveMembersFromConsumerGroupResult(
            Task.FromResult<IReadOnlyDictionary<string, KafkaException?>>(
                new Dictionary<string, KafkaException?>(StringComparer.Ordinal)),
            new[] { new MemberToRemove("instance-1") });

        ArgumentNullException? thrown = null;
        try
        {
            result.MemberResult(null!);
        }
        catch (ArgumentNullException ex)
        {
            thrown = ex;
        }

        Assert.NotNull(thrown);
    }

    /// <summary>
    /// In removeAll mode, <c>MemberResult</c> throws synchronously with Java's exact message
    /// regardless of what the future resolves to.
    /// </summary>
    [Fact]
    public void MemberResult_RemoveAllMode_ThrowsArgumentException_Synchronously()
    {
        RemoveMembersFromConsumerGroupResult result = new RemoveMembersFromConsumerGroupResult(
            Task.FromResult<IReadOnlyDictionary<string, KafkaException?>>(
                new Dictionary<string, KafkaException?>(StringComparer.Ordinal)),
            Array.Empty<MemberToRemove>());

        ArgumentException? thrown = null;
        try
        {
            result.MemberResult(new MemberToRemove("instance-1"));
        }
        catch (ArgumentException ex)
        {
            thrown = ex;
        }

        Assert.NotNull(thrown);
        Assert.StartsWith(
            "The method: memberResult is not applicable in 'removeAll' mode",
            thrown!.Message,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// A member never included in the original request is rejected synchronously with Java's
    /// exact (unquoted) message — distinct from the "missing from the response" message below.
    /// </summary>
    [Fact]
    public void MemberResult_MemberNotInOriginalRequest_ThrowsArgumentException_Synchronously()
    {
        MemberToRemove requested = new MemberToRemove("instance-1");
        MemberToRemove notRequested = new MemberToRemove("instance-2");

        RemoveMembersFromConsumerGroupResult result = new RemoveMembersFromConsumerGroupResult(
            Task.FromResult<IReadOnlyDictionary<string, KafkaException?>>(
                new Dictionary<string, KafkaException?>(StringComparer.Ordinal)),
            new[] { requested });

        ArgumentException? thrown = null;
        try
        {
            result.MemberResult(notRequested);
        }
        catch (ArgumentException ex)
        {
            thrown = ex;
        }

        Assert.NotNull(thrown);
        Assert.Equal("Member instance-2 was not included in the original request", thrown!.Message);
    }

    /// <summary>
    /// A requested member missing from the resolved map faults <em>asynchronously</em> with a
    /// distinct, quoted message — Java's <c>getSubLevelError</c> (<c>:100-111</c>).
    /// </summary>
    [Fact]
    public async Task MemberResult_MemberMissingFromResponse_ThrowsArgumentException_Asynchronously()
    {
        MemberToRemove member = new MemberToRemove("instance-1");

        RemoveMembersFromConsumerGroupResult result = new RemoveMembersFromConsumerGroupResult(
            Task.FromResult<IReadOnlyDictionary<string, KafkaException?>>(
                new Dictionary<string, KafkaException?>(StringComparer.Ordinal)),
            new[] { member });

        // Captured BEFORE the assertion: a synchronous throw here would surface at this line,
        // outside Assert.ThrowsAsync's catch — which is what proves the fault is genuinely
        // asynchronous rather than merely tolerated by an assertion helper that accepts either.
        Task task = result.MemberResult(member);

        ArgumentException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<ArgumentException>(() => task), s_deadline);
        Assert.Equal("Member \"instance-1\" was not included in the removal response", thrown.Message);
    }

    /// <summary>A present, null-valued map entry means that member succeeded.</summary>
    [Fact]
    public async Task MemberResult_SuccessfulMember_Completes()
    {
        MemberToRemove member = new MemberToRemove("instance-1");

        RemoveMembersFromConsumerGroupResult result = new RemoveMembersFromConsumerGroupResult(
            Task.FromResult<IReadOnlyDictionary<string, KafkaException?>>(
                new Dictionary<string, KafkaException?>(StringComparer.Ordinal) { ["instance-1"] = null }),
            new[] { member });

        await TestTimeout.Run(() => result.MemberResult(member), s_deadline);
    }

    /// <summary>
    /// A present, non-null-valued map entry re-throws that exact <see cref="KafkaException"/>
    /// instance — asserted via <see cref="Assert.Same"/>.
    /// </summary>
    [Fact]
    public async Task MemberResult_FailedMember_RethrowsTheSameException()
    {
        MemberToRemove member = new MemberToRemove("instance-1");
        KafkaException error = new KafkaException(11, "member failure", isRetriable: false);

        RemoveMembersFromConsumerGroupResult result = new RemoveMembersFromConsumerGroupResult(
            Task.FromResult<IReadOnlyDictionary<string, KafkaException?>>(
                new Dictionary<string, KafkaException?>(StringComparer.Ordinal) { ["instance-1"] = error }),
            new[] { member });

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.MemberResult(member)), s_deadline);
        Assert.Same(error, thrown);
    }

    /// <summary>
    /// <see cref="RemoveMembersFromConsumerGroupResult.All"/> in non-removeAll mode reports the
    /// first member-level failure it encounters, iterating <c>_memberInfos</c> in order.
    /// </summary>
    [Fact]
    public async Task All_NonRemoveAllMode_ThrowsFirstMemberFailure()
    {
        MemberToRemove good = new MemberToRemove("instance-good");
        MemberToRemove bad = new MemberToRemove("instance-bad");
        KafkaException error = new KafkaException(11, "member failure", isRetriable: false);

        RemoveMembersFromConsumerGroupResult result = new RemoveMembersFromConsumerGroupResult(
            Task.FromResult<IReadOnlyDictionary<string, KafkaException?>>(
                new Dictionary<string, KafkaException?>(StringComparer.Ordinal)
                {
                    ["instance-good"] = null,
                    ["instance-bad"] = error,
                }),
            new[] { good, bad });

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
        Assert.Same(error, thrown);
    }

    /// <summary>
    /// removeAll mode's <c>All()</c> iterates the resolved map (guaranteed empty by the core) —
    /// a clean resolve with an empty map completes successfully.
    /// </summary>
    [Fact]
    public async Task All_RemoveAllMode_CompletesOnEmptyResolvedMap()
    {
        RemoveMembersFromConsumerGroupResult result = new RemoveMembersFromConsumerGroupResult(
            Task.FromResult<IReadOnlyDictionary<string, KafkaException?>>(
                new Dictionary<string, KafkaException?>(StringComparer.Ordinal)),
            Array.Empty<MemberToRemove>());

        Assert.True(result.RemoveAll);
        await TestTimeout.Run(result.All, s_deadline);
    }
}
