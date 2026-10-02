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

        // The real-native no-cause case (M15/P13.3, D11): the core attaches no cause to the
        // mock's refusal, so kafka_common_Error_cause returns null and InnerException stays null.
        Assert.Null(fromAll.InnerException);

        KafkaException fromMember = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.MemberResult(member)), s_deadline);
        Assert.Equal(UnsupportedVersionCode, fromMember.Code);
        Assert.Equal(NotImplemented, fromMember.Message);
        Assert.Null(fromMember.InnerException);
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
        RemoveMembersFromConsumerGroupResult result = Resolved(
            new Dictionary<string, KafkaException?>(StringComparer.Ordinal),
            all: null,
            new MemberToRemove("instance-1"));

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
        RemoveMembersFromConsumerGroupResult result = Resolved(
            new Dictionary<string, KafkaException?>(StringComparer.Ordinal), all: null);

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
    /// exact (unquoted) message — distinct from the core's "not included in the removal
    /// response" error, which is a stored map value (see below).
    /// </summary>
    [Fact]
    public void MemberResult_MemberNotInOriginalRequest_ThrowsArgumentException_Synchronously()
    {
        MemberToRemove requested = new MemberToRemove("instance-1");
        MemberToRemove notRequested = new MemberToRemove("instance-2");

        RemoveMembersFromConsumerGroupResult result = Resolved(
            new Dictionary<string, KafkaException?>(StringComparer.Ordinal) { ["instance-1"] = null },
            all: null,
            requested);

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
    /// A requested member's map value is thrown <b>asynchronously</b>, via the returned
    /// <see cref="Task"/> — including the core's own "not included in the removal response"
    /// error for a requested member the broker did not answer (Java's
    /// <c>KafkaAdminClient.getSubLevelError</c>, reached from
    /// <c>RemoveMembersFromConsumerGroupResult.java:100-111</c>). The binding no longer
    /// composes that message; it rethrows the stored value. The text is the header's
    /// (<c>kafka_admin_RemoveMembersFromConsumerGroupResult_member_result</c>).
    /// </summary>
    [Fact]
    public async Task MemberResult_ARequestedMembersStoredError_IsThrownAsynchronously()
    {
        MemberToRemove member = new MemberToRemove("instance-1");
        KafkaException stored = new KafkaException(
            -1,
            "Member \"MemberIdentity(memberId='', groupInstanceId='instance-1', reason=null)\" "
                + "was not included in the removal response",
            isRetriable: false);

        RemoveMembersFromConsumerGroupResult result = Resolved(
            new Dictionary<string, KafkaException?>(StringComparer.Ordinal) { ["instance-1"] = stored },
            all: stored,
            member);

        // Captured BEFORE the assertion: a synchronous throw here would surface at this line,
        // outside Assert.ThrowsAsync's catch — which is what proves the fault is genuinely
        // asynchronous rather than merely tolerated by an assertion helper that accepts either.
        Task task = result.MemberResult(member);

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => task), s_deadline);
        Assert.Same(stored, thrown);
    }

    /// <summary>
    /// ⚠ A requested member missing from the resolved map is a <b>core contract violation</b>
    /// — the core reports one row per requested member — and it faults the task rather than
    /// reporting a success nobody observed.
    /// </summary>
    [Fact]
    public async Task MemberResult_ARequestedMemberMissingFromTheMap_FaultsInsteadOfSucceeding()
    {
        MemberToRemove member = new MemberToRemove("instance-1");

        RemoveMembersFromConsumerGroupResult result = Resolved(
            new Dictionary<string, KafkaException?>(StringComparer.Ordinal), all: null, member);

        Task task = result.MemberResult(member);

        await TestTimeout.Run(
            () => Assert.ThrowsAsync<KeyNotFoundException>(() => task), s_deadline);
    }

    /// <summary>A present, null-valued map entry means that member succeeded.</summary>
    [Fact]
    public async Task MemberResult_SuccessfulMember_Completes()
    {
        MemberToRemove member = new MemberToRemove("instance-1");

        RemoveMembersFromConsumerGroupResult result = Resolved(
            new Dictionary<string, KafkaException?>(StringComparer.Ordinal) { ["instance-1"] = null },
            all: null,
            member);

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

        RemoveMembersFromConsumerGroupResult result = Resolved(
            new Dictionary<string, KafkaException?>(StringComparer.Ordinal) { ["instance-1"] = error },
            all: error,
            member);

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.MemberResult(member)), s_deadline);
        Assert.Same(error, thrown);
    }

    /// <summary>
    /// ⚠ <see cref="RemoveMembersFromConsumerGroupResult.All"/> rethrows the <b>stored</b>
    /// outcome — the same instance, unchanged. Which failing member is "first"
    /// (<c>RemoveMembersFromConsumerGroupResult.java:47</c>, <c>:66-70</c>) is the core's choice
    /// now (group-instance-id order); the binding only carries it, so the request order below
    /// is deliberately the reverse of it.
    /// </summary>
    [Fact]
    public async Task All_NonRemoveAllMode_RethrowsTheStoredOutcomeUnchanged()
    {
        MemberToRemove good = new MemberToRemove("instance-a");
        MemberToRemove bad = new MemberToRemove("instance-b");
        MemberToRemove worse = new MemberToRemove("instance-c");

        KafkaException first = new KafkaException(25, "first failure", isRetriable: false);

        RemoveMembersFromConsumerGroupResult result = Resolved(
            new Dictionary<string, KafkaException?>(StringComparer.Ordinal)
            {
                ["instance-a"] = null,
                ["instance-b"] = first,
                ["instance-c"] = new KafkaException(11, "second failure", isRetriable: false),
            },
            first,
            worse,
            bad,
            good);

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
        Assert.Same(first, thrown);
    }

    /// <summary>
    /// ⚠ The control for <see cref="All_NonRemoveAllMode_RethrowsTheStoredOutcomeUnchanged"/>: a
    /// <b>null</b> stored outcome completes <see cref="RemoveMembersFromConsumerGroupResult.All"/>
    /// even though the map carries a failure — so nothing is derived from the map.
    /// </summary>
    [Fact]
    public async Task All_NonRemoveAllMode_CompletesOnANullStoredOutcome_WhateverTheMapHolds()
    {
        MemberToRemove bad = new MemberToRemove("instance-bad");
        KafkaException perMember = new KafkaException(25, "member failure", isRetriable: false);

        RemoveMembersFromConsumerGroupResult result = Resolved(
            new Dictionary<string, KafkaException?>(StringComparer.Ordinal) { ["instance-bad"] = perMember },
            all: null,
            bad);

        await TestTimeout.Run(result.All, s_deadline);

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.MemberResult(bad)), s_deadline);
        Assert.Same(perMember, thrown);
    }

    /// <summary>
    /// removeAll mode: the map is empty (the header's <c>count</c> is 0 in that mode) and a
    /// null stored outcome completes <see cref="RemoveMembersFromConsumerGroupResult.All"/>.
    /// </summary>
    [Fact]
    public async Task All_RemoveAllMode_CompletesOnANullStoredOutcome()
    {
        RemoveMembersFromConsumerGroupResult result = Resolved(
            new Dictionary<string, KafkaException?>(StringComparer.Ordinal), all: null);

        Assert.True(result.RemoveAll);
        await TestTimeout.Run(result.All, s_deadline);
    }

    /// <summary>
    /// ⚠⚠ removeAll mode: the stored outcome is the <b>only</b> carrier of a member failure
    /// (the map is empty), and <see cref="RemoveMembersFromConsumerGroupResult.All"/> rethrows
    /// it unchanged — the shape the core builds, with code -1 (<c>UNKNOWN_SERVER_ERROR</c>),
    /// not retriable, the header's
    /// (<c>kafka_admin_RemoveMembersFromConsumerGroupResult_all</c>) message for a dynamic
    /// member with no reason, and — since M15/P13.3 (D11) — the member's own error as its
    /// <see cref="Exception.InnerException"/>, Java's <c>getCause()</c>. Nothing here rewrites
    /// the code, the message or the cause the way the old binding-side "Encounter exception"
    /// loop did.
    /// </summary>
    /// <remarks>
    /// Built by hand because the mock cannot reach a partial removeAll failure (it refuses the
    /// whole call). <see cref="KafkaException.FromBorrowedHandle(IntPtr)"/>'s cause read
    /// is proven against the real native elsewhere
    /// (<c>KafkaExceptionCauseTests</c>); this test pins only that the result carries the
    /// outcome, cause included, through to the caller.
    /// </remarks>
    [Fact]
    public async Task All_RemoveAllMode_RethrowsTheStoredOutcomeUnchanged()
    {
        // UNKNOWN_MEMBER_ID (25) is the header's own example of the member's error.
        KafkaException memberError = new KafkaException(
            25, "The coordinator is not aware of this member.", isRetriable: false);
        KafkaException stored = new KafkaException(
            -1,
            "Encounter error when trying to remove: "
                + "MemberIdentity(memberId='member-1', groupInstanceId=null, reason=null)",
            isRetriable: false,
            memberError);

        RemoveMembersFromConsumerGroupResult result = Resolved(
            new Dictionary<string, KafkaException?>(StringComparer.Ordinal), stored);

        Assert.True(result.RemoveAll);

        KafkaException thrown = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
        Assert.Same(stored, thrown);
        Assert.Equal(-1, thrown.Code);
        Assert.False(thrown.IsRetriable);
        Assert.Same(memberError, thrown.InnerException);
        Assert.Equal(
            "Encounter error when trying to remove: "
                + "MemberIdentity(memberId='member-1', groupInstanceId=null, reason=null)",
            thrown.Message);
    }

    /// <summary>A call-level failure of the single awaitable propagates from both accessors.</summary>
    [Fact]
    public async Task AFaultedFuture_PropagatesFromBothAccessors()
    {
        MemberToRemove member = new MemberToRemove("instance-1");
        KafkaException callLevel = new KafkaException(35, "call failed", isRetriable: false);
        TaskCompletionSource<(IReadOnlyDictionary<string, KafkaException?> PerKey, KafkaException? All)> source =
            new TaskCompletionSource<(IReadOnlyDictionary<string, KafkaException?> PerKey, KafkaException? All)>();
        source.SetException(callLevel);

        RemoveMembersFromConsumerGroupResult result =
            new RemoveMembersFromConsumerGroupResult(source.Task, new[] { member });

        KafkaException fromMemberResult = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.MemberResult(member)), s_deadline);
        Assert.Same(callLevel, fromMemberResult);

        KafkaException fromAll = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
        Assert.Same(callLevel, fromAll);
    }

    /// <summary>A result over an already-resolved outcome, the shape the trampoline builds.</summary>
    private static RemoveMembersFromConsumerGroupResult Resolved(
        IReadOnlyDictionary<string, KafkaException?> perKey,
        KafkaException? all,
        params MemberToRemove[] requested) =>
        new RemoveMembersFromConsumerGroupResult(
            Task.FromResult<(IReadOnlyDictionary<string, KafkaException?> PerKey, KafkaException? All)>(
                (perKey, all)),
            requested);
}
