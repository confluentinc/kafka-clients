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
using System.Reflection;
using System.Runtime.InteropServices;

using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Pins <b>which ABI accessor</b> each composite-key and optional-error reader is built
/// on — the one axis of <c>electLeaders</c>' walk that no behavioural test can reach.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>Why this file exists (M15/P4 round 1, finding 70.2).</b> <c>electLeaders</c>'
/// readers are only executed on the success branch of a real
/// <c>kafka_admin_ElectLeadersResult_t</c>, and none is obtainable without a broker:
/// Java's <c>MockAdminClient.electLeaders</c> throws
/// <c>UnsupportedOperationException("Not implemented yet")</c>
/// (<c>MockAdminClient.java:797</c>) and the core mirrors that faithfully. Measured on the
/// shipped commit: replacing <c>AdminCallbacks.ElectLeadersKey</c> with a constant, and
/// swapping its value reader's <c>FromBorrowedHandle</c> for <c>FromHandle</c>, each left
/// the suite at <b>1231/1231 green</b>.
/// </para>
/// <para>
/// The reader <em>bodies</em> are now shared factories
/// (<see cref="AdminCallbacks.TopicPartitionKey"/>,
/// <see cref="AdminCallbacks.BorrowedOptionalError"/>) that <c>deleteRecords</c> and
/// <c>alterPartitionReassignments</c> drive over real result roots, so a defect in a body
/// is caught there. What remains unreachable is the <b>wiring</b>: that
/// <c>electLeaders</c>' instances capture <em>its</em> accessors and not another RPC's.
/// This file reads that off the delegate itself.
/// </para>
/// <para>
/// ⚠⚠ <b>EVERY reader built on a shared factory must appear here, and the obligation is
/// now a pre-write checklist item (PLAN §6.1 item 9) rather than a review finding.</b>
/// M15/P4 Stage 2 added two <see cref="AdminCallbacks.TopicPartitionKey"/> readers —
/// <c>ListPartitionReassignmentsKey</c> and <c>ListOffsetsKey</c> — and did not extend
/// this file, which reopened the exact gap it was written to close (finding 70.12).
/// Measured before the fix: cross-wiring <c>ListOffsetsKey</c> to <c>deleteRecords</c>'
/// accessors left the suite at <b>1297/1297 green</b>, because the sibling accessors are
/// layout-compatible — the wrong call returns a plausible answer. A shared factory makes
/// that mistake <em>easier</em> to write, not harder, which is why the guard is
/// per-reader and not per-factory.
/// </para>
/// <para>
/// ⚠ <b>The accessors are identified by their ABI <c>EntryPoint</c>, not by their C#
/// name</b> — the entry point is the contract with the core, and a C# rename that kept the
/// wrong symbol would still be a defect. Two properties are asserted per reader: the exact
/// set of symbols it captures, and — across readers — that no two RPCs capture the same
/// set, which is what a copy-paste wiring error produces.
/// </para>
/// <para>
/// ⚠ <b>Reading captured state is deliberate, and it is guarded against becoming vacuous
/// by TWO different mechanisms — which one fires depends on the refactor, and the
/// difference was measured, not reasoned about.</b> A factory-built reader closes over its
/// accessors, so its <see cref="Delegate.Target"/> is a display class whose fields hold
/// them. Probed on net10.0:
/// </para>
/// <list type="table">
/// <item>
/// <term>factory-built closure (the shipped shape)</term>
/// <description><c>&lt;&gt;c__DisplayClass…</c> — the fields hold the accessors.</description>
/// </item>
/// <item>
/// <term>the factory inlined back into a <c>static</c> lambda</term>
/// <description>
/// <c>&lt;&gt;c</c> — <b>NOT <see langword="null"/></b>. A non-capturing lambda is cached
/// on the compiler's singleton, so <see cref="CapturedEntryPoints"/> reaches the field
/// scan and finds no <see cref="Delegate"/>; <b><see cref="Assert.NotEmpty{T}"/> is what
/// fails</b>. Measured by mutation: constant-folding <c>ElectLeadersKey</c> to a
/// <c>static</c> lambda gives <b>2 RED</b>, both "Collection was empty" — and the throw
/// below fired <b>0</b> times.
/// </description>
/// </item>
/// <item>
/// <term>a plain <c>static</c> method group</term>
/// <description>
/// <see langword="null"/> — and <em>this</em> is the shape the <c>?? throw</c> in
/// <see cref="CapturedEntryPoints"/> catches, so that clause is reachable rather than
/// dead. Measured the same way, as the exact complement: rebinding
/// <c>ElectLeadersKey</c> to a static method group gives <b>2 RED</b>, both the throw's
/// own message — and "Collection was empty" fired <b>0</b> times.
/// </description>
/// </item>
/// </list>
/// <para>
/// ⚠ An earlier version of this remark credited the <c>?? throw</c> with catching the
/// static-lambda refactor. That was an unmeasured danger model in a code comment — PLAN
/// §6.1 item 7 — and it was false: the guard held, but by the other mechanism. Both are
/// kept, and they are exhaustive <em>structurally</em> rather than by enumeration: a
/// reader that captures no P/Invoke delegate has either a null
/// <see cref="Delegate.Target"/>, and throws, or a non-null one whose fields yield an
/// empty set, and fails the assertion. The table above is which branch the three known
/// shapes take, not the argument that there are only three.
/// </para>
/// <para>
/// ⚠⚠ <b>A shared accessor <em>bundle</em> is the same hazard one level up, and the
/// closure scan above cannot see it (M15/P6, finding 76.1).</b> A bundle —
/// <see cref="AclRowMarshal.NativeFilterAccessors"/>,
/// <see cref="ClientQuotaMarshal.NativeEntityAccessors"/> — is a plain static object on a
/// marshaller, not a <see cref="Delegate"/> on <see cref="AdminCallbacks"/>, so neither
/// <see cref="TheTrackedSet_CoversEveryFactoryBuiltReader"/> nor
/// <see cref="CapturedEntryPoints"/> reaches it. Its constructor takes N accessors
/// <b>positionally</b>, so transposing two of the same delegate type compiles. Measured on
/// the reviewed commit: transposing <c>NativeFilterAccessors</c>' <c>Operation</c> and
/// <c>PermissionType</c> left the suite at <b>1966/1966 green</b> — the only one of seven
/// injections that survived, because the two tests able to observe a mis-decode both used
/// <c>AclOperation.Read</c> (3) / <c>AclPermissionType.Allow</c> (3), identical codes.
/// </para>
/// <para>
/// The bundle assertions below therefore pin each member to its own ABI symbol
/// <b>positionally</b>, one row per member — a sorted set, as used for the readers above,
/// is invariant under exactly the transposition being guarded against and would prove
/// nothing. <see cref="TheTrackedBundleSet_CoversEveryAccessorBundle"/> discovers the
/// bundles the same way the reader set is discovered, so a new one added without rows here
/// turns red on its first run. Its criterion is the hazard itself — <b>two or more
/// <em>same-typed</em></b> P/Invoke delegates held by one object — which is why
/// <see cref="KeyedResultMarshal.Accessors"/> is correctly out of scope: its two members
/// have distinct delegate types, so a transposition does not compile.
/// </para>
/// </remarks>
public sealed class AdminP4ReaderWiringTests
{
    /// <summary>The one namespace an accessor bundle may live in (CLAUDE.md §2).</summary>
    private const string InteropNamespace = "Confluent.Kafka.Internal.Interop";

    /// <summary>
    /// Each composite-key reader captures <b>its own</b> result type's
    /// <c>get_topic</c> / <c>get_partition</c> pair.
    /// </summary>
    /// <remarks>
    /// The three share one body, so this is the assertion that keeps them distinguishable:
    /// pointing <c>electLeaders</c>' reader at <c>deleteRecords</c>' accessors would be
    /// invisible to every behavioural test and is caught here.
    /// </remarks>
    [Theory]
    [InlineData(
        nameof(AdminCallbacks.ElectLeadersKey),
        "kafka_admin_ElectLeadersResult_get_topic",
        "kafka_admin_ElectLeadersResult_get_partition")]
    [InlineData(
        nameof(AdminCallbacks.AlterPartitionReassignmentsKey),
        "kafka_admin_AlterPartitionReassignmentsResult_get_topic",
        "kafka_admin_AlterPartitionReassignmentsResult_get_partition")]
    [InlineData(
        nameof(AdminCallbacks.DeleteRecordsKey),
        "kafka_admin_DeleteRecordsResult_get_topic",
        "kafka_admin_DeleteRecordsResult_get_partition")]
    [InlineData(
        nameof(AdminCallbacks.ListPartitionReassignmentsKey),
        "kafka_admin_ListPartitionReassignmentsResult_get_topic",
        "kafka_admin_ListPartitionReassignmentsResult_get_partition")]
    [InlineData(
        nameof(AdminCallbacks.ListOffsetsKey),
        "kafka_admin_ListOffsetsResult_get_topic",
        "kafka_admin_ListOffsetsResult_get_partition")]
    [InlineData(
        nameof(AdminCallbacks.AlterConsumerGroupOffsetsKey),
        "kafka_admin_AlterConsumerGroupOffsetsResult_get_topic",
        "kafka_admin_AlterConsumerGroupOffsetsResult_get_partition")]
    [InlineData(
        nameof(AdminCallbacks.DeleteConsumerGroupOffsetsKey),
        "kafka_admin_DeleteConsumerGroupOffsetsResult_get_topic",
        "kafka_admin_DeleteConsumerGroupOffsetsResult_get_partition")]
    [InlineData(
        nameof(AdminCallbacks.DescribeProducersKey),
        "kafka_admin_DescribeProducersResult_get_topic",
        "kafka_admin_DescribeProducersResult_get_partition")]
    public void EachCompositeKeyReader_CapturesItsOwnAccessors(
        string readerName, string getTopic, string getPartition) =>
        Assert.Equal(
            new[] { getPartition, getTopic }.OrderBy(name => name, StringComparer.Ordinal),
            CapturedEntryPoints(Reader(readerName)));

    /// <summary>
    /// <c>electLeaders</c>' optional-error <b>value</b> reader captures
    /// <c>kafka_admin_ElectLeadersResult_get_error</c> — its own, not the byte-identical
    /// twin's.
    /// </summary>
    [Fact]
    public void ElectLeadersOptionalError_CapturesElectLeadersOwnErrorAccessor() =>
        Assert.Equal(
            new[] { "kafka_admin_ElectLeadersResult_get_error" },
            CapturedEntryPoints(Reader(nameof(AdminCallbacks.ElectLeadersOptionalError))));

    /// <summary>
    /// <c>alterConsumerGroupOffsets</c>' optional-error <b>value</b> reader captures
    /// <c>kafka_admin_AlterConsumerGroupOffsetsResult_get_error</c> — its own, not the
    /// byte-identical twin's.
    /// </summary>
    [Fact]
    public void AlterConsumerGroupOffsetsOptionalError_CapturesItsOwnErrorAccessor() =>
        Assert.Equal(
            new[] { "kafka_admin_AlterConsumerGroupOffsetsResult_get_error" },
            CapturedEntryPoints(Reader(nameof(AdminCallbacks.AlterConsumerGroupOffsetsOptionalError))));

    /// <summary>
    /// <c>deleteConsumerGroupOffsets</c>' optional-error <b>value</b> reader captures
    /// <c>kafka_admin_DeleteConsumerGroupOffsetsResult_get_error</c> — its own, not the
    /// byte-identical twin's.
    /// </summary>
    [Fact]
    public void DeleteConsumerGroupOffsetsOptionalError_CapturesItsOwnErrorAccessor() =>
        Assert.Equal(
            new[] { "kafka_admin_DeleteConsumerGroupOffsetsResult_get_error" },
            CapturedEntryPoints(Reader(nameof(AdminCallbacks.DeleteConsumerGroupOffsetsOptionalError))));

    /// <summary>
    /// <c>removeMembersFromConsumerGroup</c>' optional-error <b>value</b> reader captures
    /// <c>kafka_admin_RemoveMembersFromConsumerGroupResult_get_error</c> — its own, not the
    /// byte-identical twin's.
    /// </summary>
    [Fact]
    public void RemoveMembersFromConsumerGroupOptionalError_CapturesItsOwnErrorAccessor() =>
        Assert.Equal(
            new[] { "kafka_admin_RemoveMembersFromConsumerGroupResult_get_error" },
            CapturedEntryPoints(Reader(nameof(AdminCallbacks.RemoveMembersFromConsumerGroupOptionalError))));

    /// <summary>
    /// <c>createAcls</c>' <b>key</b> reader captures
    /// <c>kafka_admin_CreateAclsResult_get_binding</c> — its own, not another ACL result's
    /// (M15/P6).
    /// </summary>
    /// <remarks>
    /// ⚠ The ACL results expose byte-identical accessor sets, so a cross-wired
    /// <c>get_binding</c> returns a plausible binding rather than failing; and the Rust mock
    /// does not implement any of these RPCs, so no behavioural test can tell them apart
    /// either. The seven <c>kafka_common_AclBinding_*</c> accessors the reader then calls are
    /// <em>shared</em> by every ACL result and so are not cross-wirable — which is why
    /// <c>get_binding</c> is the only symbol this reader has to capture, and the only one
    /// worth asserting.
    /// </remarks>
    [Fact]
    public void CreateAclsKey_CapturesCreateAclsOwnBindingAccessor() =>
        Assert.Equal(
            new[] { "kafka_admin_CreateAclsResult_get_binding" },
            CapturedEntryPoints(Reader(nameof(AdminCallbacks.CreateAclsKey))));

    /// <summary>
    /// <c>deleteAcls</c>' <b>key</b> reader captures
    /// <c>kafka_admin_DeleteAclsResult_get_filter</c> — its own, not another ACL result's
    /// (M15/P6).
    /// </summary>
    [Fact]
    public void DeleteAclsKey_CapturesDeleteAclsOwnFilterAccessor() =>
        Assert.Equal(
            new[] { "kafka_admin_DeleteAclsResult_get_filter" },
            CapturedEntryPoints(Reader(nameof(AdminCallbacks.DeleteAclsKey))));

    /// <summary>
    /// <c>deleteAcls</c>' <b>value</b> reader captures all three of its own inner-axis
    /// accessors — the <c>(i, j)</c> walk lives inside the reader, so all three are wired
    /// here rather than in the accessor set (M15/P6).
    /// </summary>
    /// <remarks>
    /// ⚠ <c>get_result_error</c> is the <b>value</b> channel; the accessor set's
    /// <c>get_error</c> is the fault channel. A reader wired to the latter would look
    /// plausible and would silently turn every inner failure into a filter-level fault.
    /// </remarks>
    [Fact]
    public void DeleteAclsFilterResults_CapturesItsOwnInnerAccessors() =>
        Assert.Equal(
            new[]
            {
                "kafka_admin_DeleteAclsResult_get_binding",
                "kafka_admin_DeleteAclsResult_get_result_count",
                "kafka_admin_DeleteAclsResult_get_result_error",
            },
            CapturedEntryPoints(Reader(nameof(AdminCallbacks.DeleteAclsFilterResults))));

    /// <summary>
    /// <c>describeAcls</c>' <b>element</b> reader captures
    /// <c>kafka_admin_DescribeAclsResult_get_binding</c> — its own, not
    /// <c>createAcls</c>' or <c>deleteAcls</c>' byte-identical twin (M15/P6).
    /// </summary>
    [Fact]
    public void DescribeAclsValue_CapturesDescribeAclsOwnBindingAccessor() =>
        Assert.Equal(
            new[] { "kafka_admin_DescribeAclsResult_get_binding" },
            CapturedEntryPoints(Reader(nameof(AdminCallbacks.DescribeAclsValue))));

    /// <summary>
    /// ⚠⚠ Each P7 <b>string-key</b> reader captures <b>its own</b> result's key accessor
    /// (M15/P7, finding 77.3).
    /// </summary>
    /// <remarks>
    /// <c>AlterUserScramCredentialsResult_get_user</c>, <c>UpdateFeaturesResult_get_feature</c>
    /// and P6's <c>CreateAclsResult_get_binding</c> are byte-identical, so a cross-wired reader
    /// returns a plausible name. Both were inline lambdas until this finding and were therefore
    /// invisible to <see cref="TheTrackedSet_CoversEveryFactoryBuiltReader"/>'s scan;
    /// re-measured on the reviewed commit, a throw planted in <c>UpdateFeaturesKey</c> left
    /// <c>UpdateFeatures_SurfacesTheRejectionOnEveryKey</c> <b>green</b> — the reader was never
    /// executed at all. They are now built through
    /// <see cref="KeyedResultMarshal.StringKeyReader"/>, so the capture exists and the
    /// discovery reaches them by construction rather than by a name list.
    /// </remarks>
    [Theory]
    [InlineData(
        nameof(AdminCallbacks.AlterUserScramCredentialsKey),
        "kafka_admin_AlterUserScramCredentialsResult_get_user")]
    [InlineData(
        nameof(AdminCallbacks.UpdateFeaturesKey),
        "kafka_admin_UpdateFeaturesResult_get_feature")]
    public void EachP7StringKeyReader_CapturesItsOwnAccessor(string readerName, string entryPoint) =>
        Assert.Equal(new[] { entryPoint }, CapturedEntryPoints(Reader(readerName)));

    /// <summary>
    /// <c>fenceProducers</c>' <b>key</b> reader captures
    /// <c>kafka_admin_FenceProducersResult_get_transactional_id</c> — not
    /// <c>describeTransactions</c>' byte-identical twin (M15/P8).
    /// </summary>
    [Fact]
    public void FenceProducersKey_CapturesItsOwnTransactionalIdAccessor() =>
        Assert.Equal(
            new[] { "kafka_admin_FenceProducersResult_get_transactional_id" },
            CapturedEntryPoints(Reader(nameof(AdminCallbacks.FenceProducersKey))));

    /// <summary>
    /// ⚠⚠ <c>fenceProducers</c>' <b>value</b> reader captures <b>both</b> of its own inline
    /// scalars. The pair is the hazard: the producer id and the epoch are read at the same
    /// index from two accessors, and both report <c>-1</c> for a failed id, so a transposition
    /// returns a plausible answer (M15/P8).
    /// </summary>
    /// <remarks>
    /// The capture set is order-independent, so it catches a cross-wire to another RPC's
    /// accessors but says nothing about which slot each lands in. That axis needs no guard
    /// <em>here</em>: the two factory parameters have distinct delegate types
    /// (<c>…, long&gt;</c> and <c>…, short&gt;</c>), so transposing them does not compile —
    /// unlike the same-typed bundles below. The reader's <em>body</em> — which field each
    /// scalar reaches, and at which index — is asserted in <c>AdminP8ResultMarshalTests</c>
    /// over injected accessors.
    /// </remarks>
    [Fact]
    public void FenceProducersValue_CapturesBothOfItsOwnScalarAccessors() =>
        Assert.Equal(
            new[]
            {
                "kafka_admin_FenceProducersResult_get_epoch_id",
                "kafka_admin_FenceProducersResult_get_producer_id",
            },
            CapturedEntryPoints(Reader(nameof(AdminCallbacks.FenceProducersValue))));

    /// <summary>
    /// <c>listTransactions</c>' per-broker optional-error <b>value</b> reader captures
    /// <c>kafka_admin_ListTransactionsResult_get_error</c> — its own, not one of the four
    /// byte-identical twins already tracked above (M15/P8).
    /// </summary>
    [Fact]
    public void ListTransactionsOptionalError_CapturesItsOwnErrorAccessor() =>
        Assert.Equal(
            new[] { "kafka_admin_ListTransactionsResult_get_error" },
            CapturedEntryPoints(Reader(nameof(AdminCallbacks.ListTransactionsOptionalError))));

    /// <summary>
    /// <c>describeTransactions</c>' <b>key</b> reader captures
    /// <c>kafka_admin_DescribeTransactionsResult_get_transactional_id</c> — not
    /// <c>fenceProducers</c>' byte-identical twin (M15/P8).
    /// </summary>
    [Fact]
    public void DescribeTransactionsKey_CapturesItsOwnTransactionalIdAccessor() =>
        Assert.Equal(
            new[] { "kafka_admin_DescribeTransactionsResult_get_transactional_id" },
            CapturedEntryPoints(Reader(nameof(AdminCallbacks.DescribeTransactionsKey))));

    /// <summary>
    /// <c>describeDelegationToken</c>' <b>element</b> reader captures
    /// <c>kafka_admin_DescribeDelegationTokenResult_get_token</c> (M15/P7).
    /// </summary>
    /// <remarks>
    /// The <c>createDelegationToken</c> / <c>renew</c> / <c>expire</c> results declare a
    /// byte-identical <c>get_token</c>, so a cross-wired reader returns a plausible token
    /// rather than failing.
    /// </remarks>
    [Fact]
    public void DescribeDelegationTokenValue_CapturesItsOwnTokenAccessor() =>
        Assert.Equal(
            new[] { "kafka_admin_DescribeDelegationTokenResult_get_token" },
            CapturedEntryPoints(Reader(nameof(AdminCallbacks.DescribeDelegationTokenValue))));

    /// <summary>
    /// <c>describeClientQuotas</c>' <b>key</b> reader captures
    /// <c>kafka_admin_DescribeClientQuotasResult_get_entity</c> — its own, not
    /// <c>alterClientQuotas</c>' byte-identical twin (M15/P6).
    /// </summary>
    [Fact]
    public void DescribeClientQuotasKey_CapturesItsOwnEntityAccessor() =>
        Assert.Equal(
            new[] { "kafka_admin_DescribeClientQuotasResult_get_entity" },
            CapturedEntryPoints(Reader(nameof(AdminCallbacks.DescribeClientQuotasKey))));

    /// <summary>
    /// <c>describeClientQuotas</c>' <b>value</b> reader captures all three of its own
    /// inner-axis accessors — the <c>(i, j)</c> quota walk lives inside the reader (M15/P6).
    /// </summary>
    [Fact]
    public void DescribeClientQuotasValue_CapturesItsOwnInnerAccessors() =>
        Assert.Equal(
            new[]
            {
                "kafka_admin_DescribeClientQuotasResult_get_quota_count",
                "kafka_admin_DescribeClientQuotasResult_get_quota_key",
                "kafka_admin_DescribeClientQuotasResult_get_quota_value",
            },
            CapturedEntryPoints(Reader(nameof(AdminCallbacks.DescribeClientQuotasValue))));

    /// <summary>
    /// ⚠⚠ <c>alterClientQuotas</c>' <b>key</b> reader captures
    /// <c>kafka_admin_AlterClientQuotasResult_get_entity</c> — its own, not
    /// <c>describeClientQuotas</c>' twin (M15/P6).
    /// </summary>
    /// <remarks>
    /// <c>kafka_admin_CreateAclsResult_t</c> and <c>kafka_admin_AlterClientQuotasResult_t</c>
    /// declare byte-identical accessor sets (<c>count</c> / <c>get_X</c> / <c>get_error</c> /
    /// <c>destroy</c>), so a cross-wire between them compiles only for the <em>key</em>
    /// reader's entity/binding types — but the quota pair does cross-wire silently, and both
    /// are what this file exists for.
    /// </remarks>
    [Fact]
    public void AlterClientQuotasKey_CapturesItsOwnEntityAccessor() =>
        Assert.Equal(
            new[] { "kafka_admin_AlterClientQuotasResult_get_entity" },
            CapturedEntryPoints(Reader(nameof(AdminCallbacks.AlterClientQuotasKey))));

    /// <summary>
    /// No two of the factory-built readers capture the same accessors — the control that
    /// makes the per-reader assertions above more than three restatements of one source
    /// line.
    /// </summary>
    /// <remarks>
    /// A copy-paste wiring error produces exactly this collision, and it is the failure the
    /// shared factory makes <em>easier</em> to write than the three separate lambdas did.
    /// </remarks>
    [Fact]
    public void NoTwoFactoryBuiltReaders_CaptureTheSameAccessors()
    {
        string[] readers =
        {
            nameof(AdminCallbacks.ElectLeadersKey),
            nameof(AdminCallbacks.AlterPartitionReassignmentsKey),
            nameof(AdminCallbacks.DeleteRecordsKey),
            nameof(AdminCallbacks.ListPartitionReassignmentsKey),
            nameof(AdminCallbacks.ListOffsetsKey),
            nameof(AdminCallbacks.ElectLeadersOptionalError),
            nameof(AdminCallbacks.AlterConsumerGroupOffsetsKey),
            nameof(AdminCallbacks.AlterConsumerGroupOffsetsOptionalError),
            nameof(AdminCallbacks.DeleteConsumerGroupOffsetsKey),
            nameof(AdminCallbacks.DeleteConsumerGroupOffsetsOptionalError),
            nameof(AdminCallbacks.RemoveMembersFromConsumerGroupOptionalError),
            nameof(AdminCallbacks.CreateAclsKey),
            nameof(AdminCallbacks.DeleteAclsKey),
            nameof(AdminCallbacks.DeleteAclsFilterResults),
            nameof(AdminCallbacks.DescribeAclsValue),
            nameof(AdminCallbacks.DescribeClientQuotasKey),
            nameof(AdminCallbacks.DescribeClientQuotasValue),
            nameof(AdminCallbacks.AlterClientQuotasKey),
            nameof(AdminCallbacks.DescribeDelegationTokenValue),
            nameof(AdminCallbacks.AlterUserScramCredentialsKey),
            nameof(AdminCallbacks.UpdateFeaturesKey),
            nameof(AdminCallbacks.FenceProducersKey),
            nameof(AdminCallbacks.FenceProducersValue),
            nameof(AdminCallbacks.DescribeTransactionsKey),
            nameof(AdminCallbacks.DescribeProducersKey),
            nameof(AdminCallbacks.ListTransactionsOptionalError),
        };

        List<string> signatures = readers
            .Select(name => string.Join("|", CapturedEntryPoints(Reader(name))))
            .ToList();

        // Control-positive: every reader really did yield a non-empty signature, so a
        // decoder that silently returned nothing could not make this pass.
        Assert.All(signatures, signature => Assert.NotEqual(string.Empty, signature));
        Assert.Equal(readers.Length, signatures.Distinct(StringComparer.Ordinal).Count());
    }

    /// <summary>
    /// ⚠⚠ <b>The tracked set is COMPLETE: every factory-built reader on
    /// <see cref="AdminCallbacks"/> is covered by the assertions above.</b> This is
    /// PLAN §6.1 item 9 enforced mechanically rather than by checklist.
    /// </summary>
    /// <remarks>
    /// <para>
    /// M15/P4 Stage 2 added two readers and did not extend this file; the gap was found in
    /// review (70.12), not by the suite. A checklist prevents that only if it is read. This
    /// discovers the readers instead: any <see cref="AdminCallbacks"/> field holding a
    /// delegate that <em>closes over</em> at least one <c>DllImport</c> is factory-built by
    /// construction, because that capture is exactly what the factories do — so a new
    /// reader added without a row here turns this red on its first run.
    /// </para>
    /// <para>
    /// ⚠ Hand-written <c>static</c> lambdas — <c>TopicListingValue</c>,
    /// <c>PartitionReassignmentValue</c> and their kin — capture nothing and are correctly
    /// out of scope: their wiring is visible in one line at the field, which is the
    /// property the factories removed and this file restores.
    /// </para>
    /// </remarks>
    [Fact]
    public void TheTrackedSet_CoversEveryFactoryBuiltReader()
    {
        string[] discovered = typeof(AdminCallbacks)
            .GetFields(BindingFlags.NonPublic | BindingFlags.Static)
            .Where(field => typeof(Delegate).IsAssignableFrom(field.FieldType))
            .Where(field => CapturesAnyImport((Delegate?)field.GetValue(null)))
            .Select(field => field.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        // Control-positive: the discovery really finds delegates, so an empty result
        // could not make this pass vacuously.
        Assert.NotEmpty(discovered);

        Assert.Equal(
            new[]
            {
                nameof(AdminCallbacks.AlterClientQuotasKey),
                nameof(AdminCallbacks.AlterConsumerGroupOffsetsKey),
                nameof(AdminCallbacks.AlterConsumerGroupOffsetsOptionalError),
                nameof(AdminCallbacks.AlterPartitionReassignmentsKey),
                nameof(AdminCallbacks.AlterUserScramCredentialsKey),
                nameof(AdminCallbacks.CreateAclsKey),
                nameof(AdminCallbacks.DeleteAclsFilterResults),
                nameof(AdminCallbacks.DeleteAclsKey),
                nameof(AdminCallbacks.DeleteConsumerGroupOffsetsKey),
                nameof(AdminCallbacks.DeleteConsumerGroupOffsetsOptionalError),
                nameof(AdminCallbacks.DeleteRecordsKey),
                nameof(AdminCallbacks.DescribeAclsValue),
                nameof(AdminCallbacks.DescribeClientQuotasKey),
                nameof(AdminCallbacks.DescribeClientQuotasValue),
                nameof(AdminCallbacks.DescribeDelegationTokenValue),
                nameof(AdminCallbacks.DescribeProducersKey),
                nameof(AdminCallbacks.DescribeTransactionsKey),
                nameof(AdminCallbacks.ElectLeadersKey),
                nameof(AdminCallbacks.ElectLeadersOptionalError),
                nameof(AdminCallbacks.FenceProducersKey),
                nameof(AdminCallbacks.FenceProducersValue),
                nameof(AdminCallbacks.ListOffsetsKey),
                nameof(AdminCallbacks.ListPartitionReassignmentsKey),
                nameof(AdminCallbacks.ListTransactionsOptionalError),
                nameof(AdminCallbacks.RemoveMembersFromConsumerGroupOptionalError),
                nameof(AdminCallbacks.UpdateFeaturesKey),
            },
            discovered);
    }

    /// <summary>
    /// ⚠⚠ Every member of the shared <c>kafka_common_AclBindingFilter_*</c> bundle is bound
    /// to <b>its own</b> ABI symbol — the positional guard the closure scan cannot give
    /// (M15/P6, finding 76.1).
    /// </summary>
    /// <remarks>
    /// The rows are compared in declaration order, member name included, so a transposition
    /// of any same-typed pair — <c>Operation</c>/<c>PermissionType</c>,
    /// <c>ResourceType</c>/<c>PatternType</c>, or any two of
    /// <c>ResourceName</c>/<c>Principal</c>/<c>Host</c> — moves a symbol to the wrong row
    /// and fails here. The expected symbols are the header's own accessor order
    /// (<c>confluent_kafka.h:7525/7537/7550/7561/7572/7584/7596</c>).
    /// </remarks>
    [Fact]
    public void NativeFilterAccessors_BindEveryMemberToItsOwnAbiSymbol()
    {
        AclRowMarshal.FilterAccessors accessors = AclRowMarshal.NativeFilterAccessors;

        Assert.Equal(
            new[]
            {
                "ResourceType=kafka_common_AclBindingFilter_resource_type",
                "ResourceName=kafka_common_AclBindingFilter_resource_name",
                "PatternType=kafka_common_AclBindingFilter_pattern_type",
                "Principal=kafka_common_AclBindingFilter_principal",
                "Host=kafka_common_AclBindingFilter_host",
                "Operation=kafka_common_AclBindingFilter_operation",
                "PermissionType=kafka_common_AclBindingFilter_permission_type",
            },
            new[]
            {
                "ResourceType=" + EntryPointOf(accessors.ResourceType),
                "ResourceName=" + EntryPointOf(accessors.ResourceName),
                "PatternType=" + EntryPointOf(accessors.PatternType),
                "Principal=" + EntryPointOf(accessors.Principal),
                "Host=" + EntryPointOf(accessors.Host),
                "Operation=" + EntryPointOf(accessors.Operation),
                "PermissionType=" + EntryPointOf(accessors.PermissionType),
            });
    }

    /// <summary>
    /// Every member of the shared <c>kafka_common_ClientQuotaEntity_*</c> bundle is bound to
    /// <b>its own</b> ABI symbol (M15/P6).
    /// </summary>
    /// <remarks>
    /// <c>GetEntryType</c>/<c>GetEntryName</c> are the one same-typed pair here;
    /// <c>EntryCount</c> has its own delegate shape and cannot be transposed with either.
    /// </remarks>
    [Fact]
    public void NativeEntityAccessors_BindEveryMemberToItsOwnAbiSymbol()
    {
        ClientQuotaMarshal.EntityAccessors accessors = ClientQuotaMarshal.NativeEntityAccessors;

        Assert.Equal(
            new[]
            {
                "EntryCount=kafka_common_ClientQuotaEntity_entry_count",
                "GetEntryType=kafka_common_ClientQuotaEntity_get_entry_type",
                "GetEntryName=kafka_common_ClientQuotaEntity_get_entry_name",
            },
            new[]
            {
                "EntryCount=" + EntryPointOf(accessors.EntryCount),
                "GetEntryType=" + EntryPointOf(accessors.GetEntryType),
                "GetEntryName=" + EntryPointOf(accessors.GetEntryName),
            });
    }

    /// <summary>
    /// Every member of the <c>kafka_admin_OffsetAndMetadataMap_*</c> bundle is bound to
    /// <b>its own</b> ABI symbol.
    /// </summary>
    /// <remarks>
    /// ⚠ Found by <see cref="TheTrackedBundleSet_CoversEveryAccessorBundle"/> rather than by
    /// hand — it predates M15/P6 and was unpinned, which is the whole argument for a
    /// mechanical discovery over a checklist. Its same-typed pair is
    /// <c>GetTopic</c>/<c>GetMetadata</c>, and transposing those swaps a key for a value.
    /// </remarks>
    [Fact]
    public void OffsetAndMetadataMapAccessors_BindEveryMemberToItsOwnAbiSymbol()
    {
        AdminCallbacks.OffsetAndMetadataMapAccessors accessors =
            (AdminCallbacks.OffsetAndMetadataMapAccessors)typeof(AdminCallbacks)
                .GetField("s_offsetAndMetadataMapAccessors", BindingFlags.NonPublic | BindingFlags.Static)!
                .GetValue(null)!;

        Assert.Equal(
            new[]
            {
                "Count=kafka_admin_OffsetAndMetadataMap_count",
                "GetTopic=kafka_admin_OffsetAndMetadataMap_get_topic",
                "GetPartition=kafka_admin_OffsetAndMetadataMap_get_partition",
                "HasOffset=kafka_admin_OffsetAndMetadataMap_has_offset",
                "GetOffset=kafka_admin_OffsetAndMetadataMap_get_offset",
                "GetMetadata=kafka_admin_OffsetAndMetadataMap_get_metadata",
                "GetLeaderEpoch=kafka_admin_OffsetAndMetadataMap_get_leader_epoch",
            },
            new[]
            {
                "Count=" + EntryPointOf(accessors.Count),
                "GetTopic=" + EntryPointOf(accessors.GetTopic),
                "GetPartition=" + EntryPointOf(accessors.GetPartition),
                "HasOffset=" + EntryPointOf(accessors.HasOffset),
                "GetOffset=" + EntryPointOf(accessors.GetOffset),
                "GetMetadata=" + EntryPointOf(accessors.GetMetadata),
                "GetLeaderEpoch=" + EntryPointOf(accessors.GetLeaderEpoch),
            });
    }

    /// <summary>
    /// ⚠⚠ Every member of <c>describeUserScramCredentials</c>' bundle is bound to <b>its
    /// own</b> ABI symbol, positionally (M15/P7).
    /// </summary>
    /// <remarks>
    /// Two same-typed pairs are transposable here, and each transposition is silent:
    /// <c>GetUser</c>/<c>GetError</c> swaps a key for a borrowed error, and
    /// <c>GetCredentialMechanism</c>/<c>GetCredentialIterations</c> swaps a mechanism code
    /// for an iteration count — <c>4096</c> decoding to <see cref="ScramMechanism.Unknown"/>
    /// rather than to an error.
    /// </remarks>
    [Fact]
    public void UserScramCredentialAccessors_BindEveryMemberToItsOwnAbiSymbol()
    {
        UserScramCredentialMarshal.Accessors accessors = UserScramCredentialMarshal.NativeAccessors;

        Assert.Equal(
            new[]
            {
                "GetUser=kafka_admin_DescribeUserScramCredentialsResult_get_user",
                "GetError=kafka_admin_DescribeUserScramCredentialsResult_get_error",
                "GetCredentialCount=kafka_admin_DescribeUserScramCredentialsResult_get_credential_count",
                "GetCredentialMechanism=kafka_admin_DescribeUserScramCredentialsResult_get_credential_mechanism",
                "GetCredentialIterations=kafka_admin_DescribeUserScramCredentialsResult_get_credential_iterations",
            },
            new[]
            {
                "GetUser=" + EntryPointOf(accessors.GetUser),
                "GetError=" + EntryPointOf(accessors.GetError),
                "GetCredentialCount=" + EntryPointOf(accessors.GetCredentialCount),
                "GetCredentialMechanism=" + EntryPointOf(accessors.GetCredentialMechanism),
                "GetCredentialIterations=" + EntryPointOf(accessors.GetCredentialIterations),
            });
    }

    /// <summary>
    /// ⚠⚠ Every member of <c>describeFeatures</c>' bundle is bound to <b>its own</b> ABI
    /// symbol, positionally (M15/P7).
    /// </summary>
    /// <remarks>
    /// This is the bundle with the most transposable members in the binding: the two counts,
    /// the two name accessors and the <b>four</b> version accessors are each same-typed, so a
    /// finalized/supported mix-up compiles and returns plausible version numbers. It is also
    /// the one bundle whose mis-wiring the mock cannot surface — it derives both tables from
    /// one seeded key set, so finalized and supported always agree there.
    /// </remarks>
    [Fact]
    public void FeatureMetadataAccessors_BindEveryMemberToItsOwnAbiSymbol()
    {
        FeatureMetadataMarshal.Accessors accessors = FeatureMetadataMarshal.NativeAccessors;

        Assert.Equal(
            new[]
            {
                "FinalizedCount=kafka_admin_DescribeFeaturesResult_finalized_count",
                "GetFinalizedFeature=kafka_admin_DescribeFeaturesResult_get_finalized_feature",
                "GetFinalizedMinVersionLevel=kafka_admin_DescribeFeaturesResult_get_finalized_min_version_level",
                "GetFinalizedMaxVersionLevel=kafka_admin_DescribeFeaturesResult_get_finalized_max_version_level",
                "SupportedCount=kafka_admin_DescribeFeaturesResult_supported_count",
                "GetSupportedFeature=kafka_admin_DescribeFeaturesResult_get_supported_feature",
                "GetSupportedMinVersion=kafka_admin_DescribeFeaturesResult_get_supported_min_version",
                "GetSupportedMaxVersion=kafka_admin_DescribeFeaturesResult_get_supported_max_version",
                "FinalizedFeaturesEpoch=kafka_admin_DescribeFeaturesResult_finalized_features_epoch",
            },
            new[]
            {
                "FinalizedCount=" + EntryPointOf(accessors.FinalizedCount),
                "GetFinalizedFeature=" + EntryPointOf(accessors.GetFinalizedFeature),
                "GetFinalizedMinVersionLevel=" + EntryPointOf(accessors.GetFinalizedMinVersionLevel),
                "GetFinalizedMaxVersionLevel=" + EntryPointOf(accessors.GetFinalizedMaxVersionLevel),
                "SupportedCount=" + EntryPointOf(accessors.SupportedCount),
                "GetSupportedFeature=" + EntryPointOf(accessors.GetSupportedFeature),
                "GetSupportedMinVersion=" + EntryPointOf(accessors.GetSupportedMinVersion),
                "GetSupportedMaxVersion=" + EntryPointOf(accessors.GetSupportedMaxVersion),
                "FinalizedFeaturesEpoch=" + EntryPointOf(accessors.FinalizedFeaturesEpoch),
            });
    }

    /// <summary>
    /// ⚠⚠ Every member of <c>describeTransactions</c>' bundle is bound to <b>its own</b> ABI
    /// symbol, positionally (M15/P8).
    /// </summary>
    /// <remarks>
    /// Three same-typed groups are transposable here and every transposition is silent:
    /// <c>GetCoordinatorId</c>/<c>GetProducerEpoch</c>/<c>GetTopicPartitionCount</c> are all
    /// <c>…, int&gt;</c> — and a count read from the coordinator id would drive the inner walk
    /// off the end of the row — while <c>GetProducerId</c>/<c>GetTransactionTimeoutMs</c> are
    /// both <c>…, long&gt;</c>, and both report <c>-1</c> for a failed row, so a swap returns a
    /// plausible number. The remaining four each have a unique delegate type.
    /// </remarks>
    [Fact]
    public void TransactionDescriptionAccessors_BindEveryMemberToItsOwnAbiSymbol()
    {
        TransactionDescriptionMarshal.Accessors accessors =
            TransactionDescriptionMarshal.NativeAccessors;

        Assert.Equal(
            new[]
            {
                "GetCoordinatorId=kafka_admin_DescribeTransactionsResult_get_coordinator_id",
                "GetState=kafka_admin_DescribeTransactionsResult_get_state",
                "GetProducerId=kafka_admin_DescribeTransactionsResult_get_producer_id",
                "GetProducerEpoch=kafka_admin_DescribeTransactionsResult_get_producer_epoch",
                "GetTransactionTimeoutMs=kafka_admin_DescribeTransactionsResult_get_transaction_timeout_ms",
                "TryGetTransactionStartTimeMs=kafka_admin_DescribeTransactionsResult_get_transaction_start_time_ms",
                "GetTopicPartitionCount=kafka_admin_DescribeTransactionsResult_get_topic_partition_count",
                "GetTopicPartitionTopic=kafka_admin_DescribeTransactionsResult_get_topic_partition_topic",
                "GetTopicPartitionPartition=kafka_admin_DescribeTransactionsResult_get_topic_partition_partition",
            },
            new[]
            {
                "GetCoordinatorId=" + EntryPointOf(accessors.GetCoordinatorId),
                "GetState=" + EntryPointOf(accessors.GetState),
                "GetProducerId=" + EntryPointOf(accessors.GetProducerId),
                "GetProducerEpoch=" + EntryPointOf(accessors.GetProducerEpoch),
                "GetTransactionTimeoutMs=" + EntryPointOf(accessors.GetTransactionTimeoutMs),
                "TryGetTransactionStartTimeMs=" + EntryPointOf(accessors.TryGetTransactionStartTimeMs),
                "GetTopicPartitionCount=" + EntryPointOf(accessors.GetTopicPartitionCount),
                "GetTopicPartitionTopic=" + EntryPointOf(accessors.GetTopicPartitionTopic),
                "GetTopicPartitionPartition=" + EntryPointOf(accessors.GetTopicPartitionPartition),
            });
    }

    /// <summary>
    /// ⚠⚠ Every member of <c>describeProducers</c>' bundle is bound to <b>its own</b> ABI
    /// symbol, positionally (M15/P8).
    /// </summary>
    /// <remarks>
    /// Two same-typed pairs are transposable and both are silent:
    /// <c>GetProducerId</c>/<c>GetLastTimestamp</c> are both <c>(i, j) -&gt; long</c> and
    /// <c>GetProducerEpoch</c>/<c>GetLastSequence</c> are both <c>(i, j) -&gt; int</c>, and
    /// every one of the four reports <c>-1</c> for an out-of-range index, so a swap returns a
    /// plausible number. The two optionals and the count each have a unique delegate type.
    /// </remarks>
    [Fact]
    public void PartitionProducerStateAccessors_BindEveryMemberToItsOwnAbiSymbol()
    {
        PartitionProducerStateMarshal.Accessors accessors =
            PartitionProducerStateMarshal.NativeAccessors;

        Assert.Equal(
            new[]
            {
                "GetProducerCount=kafka_admin_DescribeProducersResult_get_producer_count",
                "GetProducerId=kafka_admin_DescribeProducersResult_get_producer_id",
                "GetProducerEpoch=kafka_admin_DescribeProducersResult_get_producer_epoch",
                "GetLastSequence=kafka_admin_DescribeProducersResult_get_last_sequence",
                "GetLastTimestamp=kafka_admin_DescribeProducersResult_get_last_timestamp",
                "TryGetCurrentTransactionStartOffset=kafka_admin_DescribeProducersResult_get_current_transaction_start_offset",
                "TryGetCoordinatorEpoch=kafka_admin_DescribeProducersResult_get_coordinator_epoch",
            },
            new[]
            {
                "GetProducerCount=" + EntryPointOf(accessors.GetProducerCount),
                "GetProducerId=" + EntryPointOf(accessors.GetProducerId),
                "GetProducerEpoch=" + EntryPointOf(accessors.GetProducerEpoch),
                "GetLastSequence=" + EntryPointOf(accessors.GetLastSequence),
                "GetLastTimestamp=" + EntryPointOf(accessors.GetLastTimestamp),
                "TryGetCurrentTransactionStartOffset="
                    + EntryPointOf(accessors.TryGetCurrentTransactionStartOffset),
                "TryGetCoordinatorEpoch=" + EntryPointOf(accessors.TryGetCoordinatorEpoch),
            });
    }

    /// <summary>
    /// ⚠⚠ Every member of <c>listTransactions</c>' bundle is bound to <b>its own</b> ABI
    /// symbol, positionally (M15/P8).
    /// </summary>
    /// <remarks>
    /// <c>GetTransactionalId</c>/<c>GetState</c> are the same-typed pair, and transposing them
    /// is the worst of the three P8 bundles: a transactional id decodes through
    /// <see cref="TransactionMarshal"/> to <c>Unknown</c> rather than throwing, so every
    /// listing would report a plausible state and an id that is a state name.
    /// </remarks>
    [Fact]
    public void TransactionListingAccessors_BindEveryMemberToItsOwnAbiSymbol()
    {
        TransactionListingMarshal.Accessors accessors = TransactionListingMarshal.NativeAccessors;

        Assert.Equal(
            new[]
            {
                "GetListingCount=kafka_admin_ListTransactionsResult_get_listing_count",
                "GetTransactionalId=kafka_admin_ListTransactionsResult_get_transactional_id",
                "GetProducerId=kafka_admin_ListTransactionsResult_get_producer_id",
                "GetState=kafka_admin_ListTransactionsResult_get_state",
            },
            new[]
            {
                "GetListingCount=" + EntryPointOf(accessors.GetListingCount),
                "GetTransactionalId=" + EntryPointOf(accessors.GetTransactionalId),
                "GetProducerId=" + EntryPointOf(accessors.GetProducerId),
                "GetState=" + EntryPointOf(accessors.GetState),
            });
    }

    /// <summary>
    /// ⚠⚠ <b>The four count-less P7 trampolines each destroy THEIR OWN result root.</b> The
    /// four types declare byte-identical destroys, so a cross-wired one frees the right
    /// pointer through the wrong destructor — undetectable by any behavioural assertion
    /// (M15/P7).
    /// </summary>
    [Theory]
    [InlineData(
        "s_destroyCreateDelegationTokenResult",
        "kafka_admin_CreateDelegationTokenResult_destroy")]
    [InlineData(
        "s_destroyRenewDelegationTokenResult",
        "kafka_admin_RenewDelegationTokenResult_destroy")]
    [InlineData(
        "s_destroyExpireDelegationTokenResult",
        "kafka_admin_ExpireDelegationTokenResult_destroy")]
    [InlineData(
        "s_destroyDescribeDelegationTokenResult",
        "kafka_admin_DescribeDelegationTokenResult_destroy")]
    [InlineData(
        "s_destroyDescribeFeaturesResult",
        "kafka_admin_DescribeFeaturesResult_destroy")]
    [InlineData(
        "s_destroyDescribeUserScramCredentialsResult",
        "kafka_admin_DescribeUserScramCredentialsResult_destroy")]
    [InlineData(
        "s_destroyAlterUserScramCredentialsResult",
        "kafka_admin_AlterUserScramCredentialsResult_destroy")]
    [InlineData(
        "s_destroyUpdateFeaturesResult",
        "kafka_admin_UpdateFeaturesResult_destroy")]
    public void EachP7Destroy_BindsItsOwnAbiSymbol(string fieldName, string entryPoint) =>
        Assert.Equal(entryPoint, EntryPointOf(Reader(fieldName)));

    /// <summary>
    /// ⚠⚠ <b>The tracked bundle set is COMPLETE: every shared accessor bundle in the binding
    /// is pinned by one of the three assertions above.</b> The bundle-level twin of
    /// <see cref="TheTrackedSet_CoversEveryFactoryBuiltReader"/>.
    /// </summary>
    /// <remarks>
    /// Discovery is the hazard's own definition — an object held in an interop static field
    /// that carries <b>two or more same-typed</b> P/Invoke delegates, which is exactly what a
    /// positional constructor lets you transpose without a compile error. A bundle whose
    /// delegate types are all distinct (<see cref="KeyedResultMarshal.Accessors"/>) is not
    /// discovered because it is not transposable, so this does not widen into a checklist
    /// over every accessor set in the binding.
    /// </remarks>
    [Fact]
    public void TheTrackedBundleSet_CoversEveryAccessorBundle()
    {
        string[] discovered = typeof(AdminCallbacks).Assembly
            .GetTypes()
            .Where(type => type.Namespace == InteropNamespace && !type.ContainsGenericParameters)
            .SelectMany(type =>
                type.GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static))
            .Where(field => !typeof(Delegate).IsAssignableFrom(field.FieldType)
                && !field.FieldType.IsPrimitive
                && field.FieldType != typeof(string))
            .Where(field => HasTransposableAccessorPair(field.GetValue(null)))
            .Select(field => field.DeclaringType!.Name + "." + field.Name)
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        // Control-positive: the discovery really reaches bundles, so a criterion that
        // silently matched nothing could not make this pass vacuously.
        Assert.NotEmpty(discovered);

        Assert.Equal(
            new[]
            {
                "AclRowMarshal.NativeFilterAccessors",
                "AdminCallbacks.s_offsetAndMetadataMapAccessors",
                "ClientQuotaMarshal.NativeEntityAccessors",
                "FeatureMetadataMarshal.NativeAccessors",
                "PartitionProducerStateMarshal.NativeAccessors",
                "TransactionDescriptionMarshal.NativeAccessors",
                "TransactionListingMarshal.NativeAccessors",
                "UserScramCredentialMarshal.NativeAccessors",
            },
            discovered);
    }

    /// <summary>
    /// Whether an object holds two or more P/Invoke delegates of the <b>same</b> delegate
    /// type — the shape a positional constructor lets a caller transpose silently.
    /// </summary>
    private static bool HasTransposableAccessorPair(object? bundle) =>
        bundle is not null
        && bundle.GetType()
            .GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
            .Where(field => field.GetValue(bundle) is Delegate accessor
                && accessor.Method.GetCustomAttribute<DllImportAttribute>() is not null)
            .GroupBy(field => field.FieldType)
            .Any(sameTyped => sameTyped.Count() >= 2);

    /// <summary>
    /// The ABI <c>EntryPoint</c> one bundle member is bound to.
    /// </summary>
    /// <exception cref="Xunit.Sdk.XunitException">
    /// The member is not a direct P/Invoke, so the row below it would assert nothing.
    /// </exception>
    private static string EntryPointOf(Delegate accessor) =>
        accessor.Method.GetCustomAttribute<DllImportAttribute>()?.EntryPoint
        ?? throw new Xunit.Sdk.XunitException(
            "the bundle member is not a direct P/Invoke, so its ABI symbol is no longer "
            + "readable here — this assertion needs a different mechanism, not deleting");

    /// <summary>
    /// Whether a delegate closes over at least one <c>DllImport</c> — the signature of a
    /// factory-built reader.
    /// </summary>
    private static bool CapturesAnyImport(Delegate? candidate)
    {
        object? target = candidate?.Target;
        if (target is null)
        {
            return false;
        }

        return target.GetType()
            .GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
            .Select(field => field.GetValue(target))
            .OfType<Delegate>()
            .Any(captured => captured.Method.GetCustomAttribute<DllImportAttribute>() is not null);
    }

    /// <summary>
    /// The ABI <c>EntryPoint</c>s of every P/Invoke a reader closes over, sorted.
    /// </summary>
    /// <exception cref="Xunit.Sdk.XunitException">
    /// The reader captures nothing. Thrown explicitly for a <c>static</c> <b>method
    /// group</b> (<see cref="Delegate.Target"/> is <see langword="null"/>); a
    /// non-capturing <b>lambda</b> instead reaches <see cref="Assert.NotEmpty{T}"/> below,
    /// because its target is the compiler's <c>&lt;&gt;c</c> singleton rather than
    /// <see langword="null"/>. See the measured table in the type remarks.
    /// </exception>
    private static string[] CapturedEntryPoints(Delegate reader)
    {
        // Null target == a static method group. A non-capturing LAMBDA does not land here —
        // its target is <>c — and is caught by the Assert.NotEmpty below instead.
        object target = reader.Target
            ?? throw new Xunit.Sdk.XunitException(
                "the reader is a static method group, so it captures nothing and its wiring is "
                + "no longer readable here — this file needs a different mechanism, not deleting");

        string[] entryPoints = target.GetType()
            .GetFields(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
            .Select(field => field.GetValue(target))
            .OfType<Delegate>()
            .Select(captured => captured.Method.GetCustomAttribute<DllImportAttribute>()?.EntryPoint)
            .Where(entryPoint => entryPoint is not null)
            .Select(entryPoint => entryPoint!)
            .OrderBy(entryPoint => entryPoint, StringComparer.Ordinal)
            .ToArray();

        // ⚠ THE mechanism for a non-capturing lambda — see the type remarks' measured table.
        Assert.NotEmpty(entryPoints);
        return entryPoints;
    }

    private static Delegate Reader(string fieldName) =>
        (Delegate)typeof(AdminCallbacks)
            .GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;
}
