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
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Drives <c>deleteAcls</c>' two-level walk — <see cref="KeyedResultMarshal.Complete"/> with
/// <see cref="DeleteAclsResultMarshal.FilterResultsReader"/> as its value reader (M15/P6).
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b>The defect this file exists to catch is conflating the two error channels.</b>
/// <c>get_error(i)</c> is the <em>filter's</em> future failing and faults that filter's
/// <see cref="Task"/>; <c>get_result_error(i, j)</c> is Java's <c>FilterResult.error()</c>
/// and is a stored <b>value</b> inside a <em>successfully completed</em>
/// <c>FilterResults</c>. A filter-level success carrying an inner error therefore
/// <b>completes</b> <c>Values[filter]</c> while <b>faulting</b> <c>All()</c> — the two are
/// supposed to disagree, and an implementation that routes the inner error to the fault
/// channel passes every one-channel test.
/// </para>
/// <para>
/// <b>Why the walk is driven directly.</b> The Rust mock fails every filter
/// (<c>MockAdminClient.java:816-818</c>, mirrored per <c>admin-client.md</c> §9), so a
/// filter-level <em>success</em> — and with it every inner entry — is unreachable end to
/// end. <c>PublicAdminDeleteAclsTests</c> covers what the mock <em>can</em> produce over
/// real native memory; this file covers the rest by injecting the accessors into
/// production's own walk (<c>definition-of-done.md</c> §12).
/// </para>
/// <para>
/// ⚠ <b>Every error handle here is a real <c>kafka_common_KafkaError_t</c> that the
/// <em>fixture</em> owns and destroys.</b> Both channels are <c>const</c>, so the walk must
/// treat both as borrowed; if it destroyed either, the fixture's own destroy would be a
/// double free and would abort the process rather than fail an assertion (T-N10).
/// </para>
/// </remarks>
public sealed class AdminP6ResultMarshalTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>A stand-in for the <c>DeleteAclsResult_t *</c>; never dereferenced.</summary>
    private static readonly IntPtr s_root = new IntPtr(0x6060);

    // ------------------------------------------------------------------------------------
    // AclRowMarshal.ReadFilter — the key reader's null-versus-absent rule.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ On the <b>read</b> path too, <see langword="null"/> and <c>""</c> are different
    /// values: a null pointer means "matches any" and must stay <see langword="null"/>.
    /// Coalescing it to <c>""</c> would make every <c>Values[filter]</c> lookup miss.
    /// </summary>
    [Fact]
    public void ReadFilter_KeepsNullApartFromEmpty()
    {
        using Fixture fixture = new Fixture();

        IntPtr wildcard = fixture.AddFilterPointer(
            ResourceType.Topic, null, PatternType.Any, null, null,
            AclOperation.Any, AclPermissionType.Any);
        IntPtr literalEmpty = fixture.AddFilterPointer(
            ResourceType.Topic, string.Empty, PatternType.Any, string.Empty, string.Empty,
            AclOperation.Any, AclPermissionType.Any);

        AclBindingFilter any = AclRowMarshal.ReadFilter(wildcard, fixture.FilterAccessors);
        Assert.Null(any.PatternFilter.Name);
        Assert.Null(any.EntryFilter.Principal);
        Assert.Null(any.EntryFilter.Host);

        AclBindingFilter empty = AclRowMarshal.ReadFilter(literalEmpty, fixture.FilterAccessors);
        Assert.Equal(string.Empty, empty.PatternFilter.Name);
        Assert.Equal(string.Empty, empty.EntryFilter.Principal);
        Assert.Equal(string.Empty, empty.EntryFilter.Host);

        // The two are different filters, which is the whole point.
        Assert.NotEqual(any, empty);
    }

    /// <summary>
    /// The seven flat accessors land in their own seven slots, asserted with seven
    /// <em>distinct</em> values so a swapped pair cannot pass.
    /// </summary>
    [Fact]
    public void ReadFilter_RestoresEverySlot()
    {
        using Fixture fixture = new Fixture();

        IntPtr pointer = fixture.AddFilterPointer(
            ResourceType.TransactionalId, "txn-1", PatternType.Prefixed,
            "User:alice", "10.0.0.1", AclOperation.DescribeConfigs, AclPermissionType.Deny);

        AclBindingFilter filter = AclRowMarshal.ReadFilter(pointer, fixture.FilterAccessors);

        Assert.Equal(ResourceType.TransactionalId, filter.PatternFilter.ResourceType);
        Assert.Equal("txn-1", filter.PatternFilter.Name);
        Assert.Equal(PatternType.Prefixed, filter.PatternFilter.PatternType);
        Assert.Equal("User:alice", filter.EntryFilter.Principal);
        Assert.Equal("10.0.0.1", filter.EntryFilter.Host);
        Assert.Equal(AclOperation.DescribeConfigs, filter.EntryFilter.Operation);
        Assert.Equal(AclPermissionType.Deny, filter.EntryFilter.PermissionType);
    }

    /// <summary>
    /// A null filter pointer inside the result's own count is rejected rather than turned
    /// into a filter nobody asked for.
    /// </summary>
    [Fact]
    public void ReadFilter_NullPointer_Throws()
    {
        using Fixture fixture = new Fixture();

        KafkaException failure = Assert.Throws<KafkaException>(
            () => AclRowMarshal.ReadFilter(IntPtr.Zero, fixture.FilterAccessors));
        Assert.Equal(
            "The admin result produced no ACL filter for an index within its own count.",
            failure.Message);
    }

    // ------------------------------------------------------------------------------------
    // The inner (i, j) axis — binding XOR error, as a VALUE.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// An inner <c>get_result_error</c> becomes <see cref="DeleteAclsResult.FilterResult.Error"/>
    /// with no binding; an inner <c>get_binding</c> becomes the binding with no error. Both
    /// appear inside a filter whose own <see cref="Task"/> <b>completed</b>.
    /// </summary>
    [Fact]
    public async Task InnerEntries_AreBindingXorError_OnACompletedFilter()
    {
        using Fixture fixture = new Fixture();

        AclBindingFilter key = fixture.AddFilter("mixed");
        fixture.AddBinding(key, Binding("deleted-a"));
        fixture.AddInnerError(key, 42, "could not delete");
        fixture.AddBinding(key, Binding("deleted-b"));

        DeleteAclsResult result = fixture.Walk();

        DeleteAclsResult.FilterResults results =
            await TestTimeout.Run(() => result.Values[key], s_deadline);

        Assert.Equal(3, results.Values.Count);

        Assert.Equal(Binding("deleted-a"), results.Values[0].Binding);
        Assert.Null(results.Values[0].Error);

        Assert.Null(results.Values[1].Binding);
        Assert.Equal("could not delete", results.Values[1].Error!.Message);
        Assert.Equal(42, results.Values[1].Error!.Code);

        // Order is the ABI's own, so an implementation collecting errors separately fails here.
        Assert.Equal(Binding("deleted-b"), results.Values[2].Binding);
        Assert.Null(results.Values[2].Error);
    }

    /// <summary>
    /// A filter that matched nothing resolves to an <b>empty</b> list, not a fault — Java's
    /// <c>DeleteAclsResult.java:97-98</c>.
    /// </summary>
    [Fact]
    public async Task ZeroMatches_IsAnEmptyList_NotAFault()
    {
        using Fixture fixture = new Fixture();

        AclBindingFilter key = fixture.AddFilter("no-match");

        DeleteAclsResult result = fixture.Walk();

        Assert.Empty((await TestTimeout.Run(() => result.Values[key], s_deadline)).Values);
    }

    // ------------------------------------------------------------------------------------
    // The two channels — the discriminating tests.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ <b>T-N2, THE test for this slice.</b> Filter-level success carrying an inner
    /// error: <c>Values[filter]</c> <b>completes</b> and holds the error as a value, while
    /// <c>All()</c> <b>faults</b> with that exact error.
    /// </summary>
    [Fact]
    public async Task InnerError_CompletesItsFilter_ButFaultsAll()
    {
        using Fixture fixture = new Fixture();

        AclBindingFilter key = fixture.AddFilter("inner-only");
        fixture.AddInnerError(key, 58, "ACL deletion failed");

        DeleteAclsResult result = fixture.Walk();

        // Channel 1: the filter's own Task completed, carrying the error as a VALUE.
        DeleteAclsResult.FilterResults results =
            await TestTimeout.Run(() => result.Values[key], s_deadline);
        Assert.Equal("ACL deletion failed", Assert.Single(results.Values).Error!.Message);

        // Channel 2: All() throws the first inner error it meets.
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(result.All, s_deadline));
        Assert.Equal("ACL deletion failed", failure.Message);
        Assert.Equal(58, failure.Code);
    }

    /// <summary>
    /// The mirror: a <b>filter-level</b> error faults that filter's <see cref="Task"/>
    /// <em>and</em> <c>All()</c>, and the filter contributes no bindings.
    /// </summary>
    [Fact]
    public async Task FilterLevelError_FaultsBothChannels()
    {
        using Fixture fixture = new Fixture();

        AclBindingFilter failed = fixture.AddFilter("filter-failed");
        fixture.FailFilter(failed, 31, "filter rejected");

        AclBindingFilter ok = fixture.AddFilter("filter-ok");
        fixture.AddBinding(ok, Binding("survivor"));

        DeleteAclsResult result = fixture.Walk();

        KafkaException perFilter = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values[failed], s_deadline));
        Assert.Equal("filter rejected", perFilter.Message);
        Assert.Equal(31, perFilter.Code);

        // The other filter is unaffected and awaitable on its own.
        Assert.Equal(
            Binding("survivor"),
            Assert.Single((await TestTimeout.Run(() => result.Values[ok], s_deadline)).Values).Binding);

        KafkaException all = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(result.All, s_deadline));
        Assert.Equal("filter rejected", all.Message);
    }

    /// <summary>
    /// ⚠ <b>T-N7.</b> <c>All()</c> over filters that matched nothing is a <b>successful
    /// empty collection</b>, never a fault.
    /// </summary>
    [Fact]
    public async Task All_OverANoMatchResult_IsASuccessfulEmptyCollection()
    {
        using Fixture fixture = new Fixture();

        fixture.AddFilter("empty-a");
        fixture.AddFilter("empty-b");

        DeleteAclsResult result = fixture.Walk();

        Assert.Empty(await TestTimeout.Run(result.All, s_deadline));
    }

    /// <summary>
    /// <c>All()</c> accumulates every deleted binding, across filters, in filter order.
    /// </summary>
    [Fact]
    public async Task All_AccumulatesEveryBinding_InFilterOrder()
    {
        using Fixture fixture = new Fixture();

        AclBindingFilter first = fixture.AddFilter("acc-first");
        fixture.AddBinding(first, Binding("one"));
        fixture.AddBinding(first, Binding("two"));

        AclBindingFilter second = fixture.AddFilter("acc-second");
        fixture.AddBinding(second, Binding("three"));

        DeleteAclsResult result = fixture.Walk();

        Assert.Equal(
            new[] { Binding("one"), Binding("two"), Binding("three") },
            await TestTimeout.Run(result.All, s_deadline));
    }

    /// <summary>
    /// A top-level submit failure faults <b>every</b> per-filter awaitable, leaving none
    /// hanging — the callback's own owned <c>error</c> channel.
    /// </summary>
    [Fact]
    public async Task TopLevelFailure_FaultsEveryFilter()
    {
        AclBindingFilter alpha = Filter("top-alpha");
        AclBindingFilter beta = Filter("top-beta");

        KeyedAdminOperation<AclBindingFilter, DeleteAclsResult.FilterResults> operation =
            new KeyedAdminOperation<AclBindingFilter, DeleteAclsResult.FilterResults>(
                "deleteAcls",
                new[] { alpha, beta },
                EqualityComparer<AclBindingFilter>.Default);

        operation.FailAll(new KafkaException("submit failed"));
        operation.FailUncompleted();

        DeleteAclsResult result = new DeleteAclsResult(operation.Tasks);

        Assert.Equal(
            "submit failed",
            (await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => result.Values[alpha], s_deadline))).Message);
        Assert.Equal(
            "submit failed",
            (await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => result.Values[beta], s_deadline))).Message);
    }

    /// <summary>
    /// The result's keys are the <em>decoded</em> filters, so a freshly built, value-equal
    /// filter finds its own awaitable (PLAN D39).
    /// </summary>
    [Fact]
    public async Task Values_AreKeyedByValue_NotByReference()
    {
        using Fixture fixture = new Fixture();

        fixture.AddFilter("keyed-by-value");

        DeleteAclsResult result = fixture.Walk();

        AclBindingFilter lookup = Filter("keyed-by-value");
        Assert.NotSame(lookup, Assert.Single(result.Values).Key);
        Assert.Empty((await TestTimeout.Run(() => result.Values[lookup], s_deadline)).Values);
    }

    // ------------------------------------------------------------------------------------
    // Harness.
    // ------------------------------------------------------------------------------------

    // ------------------------------------------------------------------------------------
    // describeAcls — sub-shape 3b: ONE awaiter over an ordered collection, no error channel.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// The sub-shape-3b walk builds an ordered collection out of real borrowed
    /// <c>kafka_common_AclBinding_t</c> handles and copies every element out <b>before</b> the
    /// root dies.
    /// </summary>
    /// <remarks>
    /// ⚠ <b>The bindings are real native ones, taken from a <c>create_acls</c> root.</b> The
    /// mock fails <c>describeAcls</c> as a whole (<c>MockAdminClient.java:811-813</c>), so its
    /// own result root never materialises — the callback gets the owned error instead. What
    /// this needs is a live <c>AclBinding_t</c> to decode, and <c>create_acls</c>' result echoes
    /// the submitted bindings back, so the element reader here is production's own
    /// <see cref="AclRowMarshal.BindingReader"/> over production's <see cref="AclRowMarshal.ReadBinding"/>
    /// — only the indexed accessor is substituted. That <em>describeAcls</em> wires its own
    /// <c>get_binding</c> is <c>AdminP4ReaderWiringTests</c>' job.
    /// </remarks>
    [Fact]
    public async Task DescribeAclsWalk_BuildsAnOrderedCollection_CopiedOutBeforeTheRootDies()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        AclBinding[] submitted = { Binding("alpha"), Binding("beta"), Binding("gamma") };
        IntPtr root = SubmitAndCaptureCreateAcls(admin, submitted);

        SingleAdminOperation<IReadOnlyCollection<AclBinding>> operation =
            new SingleAdminOperation<IReadOnlyCollection<AclBinding>>("describeAcls");
        try
        {
            KeyedResultMarshal.CompleteList(
                root,
                NativeMethods.CreateAclsResultCount,
                operation,
                AclRowMarshal.BindingReader(NativeMethods.CreateAclsResultGetBinding));
        }
        finally
        {
            NativeMethods.CreateAclsResultDestroy(root);
        }

        // The root is gone; everything below reads owned managed state.
        IReadOnlyCollection<AclBinding> bindings =
            await TestTimeout.Run(() => operation.Task, s_deadline);

        // Order is the ABI's own (h:7696-7697) — neither shuffled nor re-sorted.
        Assert.Equal(submitted, bindings);
    }

    /// <summary>
    /// A whole-call failure faults the <b>single</b> awaiter — there is no per-key error
    /// channel for it to land in, and no <c>All()</c> to aggregate.
    /// </summary>
    [Fact]
    public async Task DescribeAclsTopLevelFailure_FaultsTheOneTask()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        DescribeAclsResult result = admin.DescribeAcls(
            Filter("top-level"),
            options: null,
            (handle, resourceType, resourceName, patternType, principal, host,
             operation, permissionType, timeoutMs, callback, userData) =>
                callback(IntPtr.Zero, NewOwnedError(7, "describe failed"), userData));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(result.Values, s_deadline));
        Assert.Equal("describe failed", failure.Message);
        Assert.Equal(7, failure.Code);
    }

    /// <summary>
    /// Submits <c>create_acls</c> straight at the ABI and hands back its <b>owned</b> result
    /// root, so the walk above has real borrowed bindings to decode.
    /// </summary>
    private static IntPtr SubmitAndCaptureCreateAcls(NativeAdminClient admin, AclBinding[] acls)
    {
        using AclRowMarshal.Rows rows = AclRowMarshal.Pin(acls);

        // ⚠ The SYNCHRONOUS entry point, since M15/P9 CP6 moved the async one onto per-key
        // callbacks with no result root at all.
        IntPtr error = NativeMethods.AdminClientCreateAcls(
            admin.Handle.DangerousGetHandle(),
            rows.ResourceTypes,
            rows.ResourceNames,
            rows.PatternTypes,
            rows.Principals,
            rows.Hosts,
            rows.Operations,
            rows.PermissionTypes,
            rows.Count,
            -1,
            out IntPtr result);

        KafkaException? submitFailure = KafkaException.FromHandle(error);
        if (submitFailure is not null)
        {
            throw submitFailure;
        }

        Assert.NotEqual(IntPtr.Zero, result);
        return result;
    }

    /// <summary>An <b>owned</b> error for a trampoline to consume, as native would hand it.</summary>
    private static IntPtr NewOwnedError(int code, string message)
    {
        using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
        IntPtr error = NativeMethods.KafkaErrorNew(code, pinned.Pointer);
        Assert.NotEqual(IntPtr.Zero, error);
        return error;
    }

    // ------------------------------------------------------------------------------------
    // describeClientQuotas — shape 3: ONE awaiter over a map with a nested 2-level value.
    // ------------------------------------------------------------------------------------

    /// <summary>
    /// ⚠⚠ <b>T-N5 on the read path.</b> Inside <c>entry_count</c> a null entity name is Java's
    /// <b>null map value</b> — the built-in default entity — and is distinct from both
    /// <c>""</c> and a real name. Collapsing any pair here makes two different entities
    /// compare equal and silently merges their quotas.
    /// </summary>
    [Fact]
    public void ReadEntity_KeepsNullApartFromEmptyAndFromAName()
    {
        using QuotaFixture fixture = new QuotaFixture();

        IntPtr nullName = fixture.AddEntity(("user", null));
        IntPtr emptyName = fixture.AddEntity(("user", string.Empty));
        IntPtr realName = fixture.AddEntity(("user", "x"));

        ClientQuotaEntity fromNull = ClientQuotaMarshal.ReadEntity(nullName, fixture.EntityAccessors);
        ClientQuotaEntity fromEmpty = ClientQuotaMarshal.ReadEntity(emptyName, fixture.EntityAccessors);
        ClientQuotaEntity fromReal = ClientQuotaMarshal.ReadEntity(realName, fixture.EntityAccessors);

        // The entry is PRESENT in all three; only its value differs.
        Assert.Null(Assert.Single(fromNull.Entries).Value);
        Assert.Equal(string.Empty, Assert.Single(fromEmpty.Entries).Value);
        Assert.Equal("x", Assert.Single(fromReal.Entries).Value);

        // …and the three are three distinct dictionary keys (PLAN D39).
        Assert.Equal(
            3,
            new HashSet<ClientQuotaEntity> { fromNull, fromEmpty, fromReal }.Count);
    }

    /// <summary>A multi-entry entity restores every <c>(type, name)</c> pair.</summary>
    [Fact]
    public void ReadEntity_RestoresEveryEntry()
    {
        using QuotaFixture fixture = new QuotaFixture();

        ClientQuotaEntity entity = ClientQuotaMarshal.ReadEntity(
            fixture.AddEntity(("client-id", "svc"), ("user", null)),
            fixture.EntityAccessors);

        Assert.Equal(2, entity.Entries.Count);
        Assert.Equal("svc", entity.Entries["client-id"]);
        Assert.Null(entity.Entries["user"]);
    }

    /// <summary>A null entity pointer inside the count is rejected, not silently skipped.</summary>
    [Fact]
    public void ReadEntity_NullPointer_Throws()
    {
        using QuotaFixture fixture = new QuotaFixture();

        KafkaException rejected = Assert.Throws<KafkaException>(
            () => ClientQuotaMarshal.ReadEntity(IntPtr.Zero, fixture.EntityAccessors));
        Assert.Equal(
            "The admin result produced no client-quota entity for an index within its own count.",
            rejected.Message);
    }

    /// <summary>
    /// ⚠⚠ <b>Presence comes from the returned <c>bool</c>, never from a sentinel.</b> A quota
    /// of <c>0.0</c> and a negative one are both legal values and must read back as
    /// <b>present</b> (<c>confluent_kafka.h:7868-7876</c>).
    /// </summary>
    [Fact]
    public async Task QuotaWalk_ZeroAndNegativeValues_ArePresentNotMissing()
    {
        using QuotaFixture fixture = new QuotaFixture();

        ClientQuotaEntity key = fixture.AddRow(
            new[] { ("user", (string?)"alice") },
            ("producer_byte_rate", 0.0),
            ("consumer_byte_rate", -1.0),
            ("request_percentage", double.MaxValue));

        IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>> entities =
            await TestTimeout.Run(fixture.Walk().Entities, s_deadline);

        IReadOnlyDictionary<string, double> quotas = entities[key];
        Assert.Equal(3, quotas.Count);
        Assert.Equal(0.0, quotas["producer_byte_rate"]);
        Assert.Equal(-1.0, quotas["consumer_byte_rate"]);
        Assert.Equal(double.MaxValue, quotas["request_percentage"]);
    }

    /// <summary>
    /// An entity with no quota values resolves to an <b>empty</b> inner map, not a fault —
    /// "a quota type the entity has no value for is simply absent" (<c>h:7841</c>).
    /// </summary>
    [Fact]
    public async Task QuotaWalk_AnEntityWithNoQuotas_IsAnEmptyMap()
    {
        using QuotaFixture fixture = new QuotaFixture();

        ClientQuotaEntity key = fixture.AddRow(new[] { ("ip", (string?)"10.0.0.1") });

        IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>> entities =
            await TestTimeout.Run(fixture.Walk().Entities, s_deadline);

        Assert.Empty(entities[key]);
    }

    /// <summary>
    /// ⚠ <b>T-N6 for the quota family.</b> An entity built fresh, equal by value to one the
    /// result returned, finds its own quota map — reference equality would miss silently.
    /// </summary>
    [Fact]
    public async Task QuotaWalk_EntitiesAreKeyedByValue_NotByReference()
    {
        using QuotaFixture fixture = new QuotaFixture();

        ClientQuotaEntity returned = fixture.AddRow(
            new[] { ("user", (string?)null), ("client-id", "svc") },
            ("producer_byte_rate", 1024.0));

        IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>> entities =
            await TestTimeout.Run(fixture.Walk().Entities, s_deadline);

        ClientQuotaEntity lookup = new ClientQuotaEntity(
            new Dictionary<string, string?> { ["user"] = null, ["client-id"] = "svc" });

        Assert.NotSame(lookup, returned);
        Assert.True(entities.ContainsKey(lookup));
        Assert.Equal(1024.0, entities[lookup]["producer_byte_rate"]);
    }

    /// <summary>
    /// The whole map is copied out, one inner map per entity, with the entities kept apart.
    /// </summary>
    [Fact]
    public async Task QuotaWalk_BuildsOneInnerMapPerEntity()
    {
        using QuotaFixture fixture = new QuotaFixture();

        ClientQuotaEntity alice = fixture.AddRow(
            new[] { ("user", (string?)"alice") }, ("producer_byte_rate", 1.0));
        ClientQuotaEntity theDefault = fixture.AddRow(
            new[] { ("user", (string?)null) }, ("producer_byte_rate", 2.0));

        IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>> entities =
            await TestTimeout.Run(fixture.Walk().Entities, s_deadline);

        Assert.Equal(2, entities.Count);
        Assert.Equal(1.0, entities[alice]["producer_byte_rate"]);
        Assert.Equal(2.0, entities[theDefault]["producer_byte_rate"]);
    }

    /// <summary>
    /// A whole-call failure faults the <b>single</b> awaiter — there is no per-entity error
    /// channel and no <c>All()</c>.
    /// </summary>
    [Fact]
    public async Task QuotaTopLevelFailure_FaultsTheOneTask()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        DescribeClientQuotasResult result = admin.DescribeClientQuotas(
            ClientQuotaFilter.All(),
            options: null,
            (handle, entityTypes, matchTypes, matchNames, count, strict, timeoutMs, callback, userData) =>
                callback(IntPtr.Zero, NewOwnedError(11, "describe quotas failed"), userData));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(result.Entities, s_deadline));
        Assert.Equal("describe quotas failed", failure.Message);
        Assert.Equal(11, failure.Code);
    }

    /// <summary>
    /// A synthetic <c>describe_client_quotas</c> result table, plus the accessor sets that
    /// read it. Owns every pinned string it hands the walk.
    /// </summary>
    private sealed class QuotaFixture : IDisposable
    {
        private const int EntityPointerBase = 0x9000;

        private readonly List<Utf8Marshal.PinnedUtf8String> _pins =
            new List<Utf8Marshal.PinnedUtf8String>();

        private readonly List<List<(IntPtr Type, IntPtr Name)>> _entities =
            new List<List<(IntPtr, IntPtr)>>();

        private readonly List<QuotaRow> _rows = new List<QuotaRow>();

        /// <summary>The three flat entity accessors over this fixture's entities.</summary>
        internal ClientQuotaMarshal.EntityAccessors EntityAccessors =>
            new ClientQuotaMarshal.EntityAccessors(
                pointer => EntityAt(pointer).Count,
                (pointer, index) => EntityAt(pointer)[index].Type,
                (pointer, index) => EntityAt(pointer)[index].Name);

        /// <summary>Registers a standalone entity and returns its stand-in pointer.</summary>
        internal IntPtr AddEntity(params (string Type, string? Name)[] entries)
        {
            _entities.Add(entries.Select(entry => (Pin(entry.Type), Pin(entry.Name))).ToList());
            return new IntPtr(EntityPointerBase + _entities.Count - 1);
        }

        /// <summary>
        /// Registers one result row — an entity plus its quota map — and returns the managed
        /// entity the walk is expected to produce for it.
        /// </summary>
        internal ClientQuotaEntity AddRow(
            (string Type, string? Name)[] entries, params (string Key, double Value)[] quotas)
        {
            IntPtr entity = AddEntity(entries);
            _rows.Add(
                new QuotaRow(
                    entity,
                    quotas.Select(quota => (Pin(quota.Key), quota.Value)).ToList()));

            return new ClientQuotaEntity(
                entries.ToDictionary(entry => entry.Type, entry => entry.Name, StringComparer.Ordinal));
        }

        /// <summary>Runs production's shape-3 walk over this table and wraps the outcome.</summary>
        internal DescribeClientQuotasResult Walk()
        {
            SingleAdminOperation<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> operation =
                new SingleAdminOperation<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>(
                    "describeClientQuotas");

            ClientQuotaMarshal.EntityAccessors accessors = EntityAccessors;

            KeyedResultMarshal.CompleteAggregate(
                s_root,
                root =>
                {
                    Assert.Equal(s_root, root);
                    return _rows.Count;
                },
                operation,
                (root, index) =>
                {
                    Assert.Equal(s_root, root);
                    return ClientQuotaMarshal.ReadEntity(_rows[index].Entity, accessors);
                },
                ClientQuotaMarshal.QuotaMapReader(
                    (root, index) =>
                    {
                        Assert.Equal(s_root, root);
                        return _rows[index].Quotas.Count;
                    },
                    (root, index, quotaIndex) =>
                    {
                        Assert.Equal(s_root, root);
                        return _rows[index].Quotas[quotaIndex].Key;
                    },
                    (IntPtr root, int index, int quotaIndex, out double value) =>
                    {
                        Assert.Equal(s_root, root);
                        if (index >= _rows.Count || quotaIndex >= _rows[index].Quotas.Count)
                        {
                            value = 0;
                            return false;
                        }

                        value = _rows[index].Quotas[quotaIndex].Value;
                        return true;
                    }),
                EqualityComparer<ClientQuotaEntity>.Default);

            return new DescribeClientQuotasResult(operation.Task);
        }

        /// <summary>Releases every pin.</summary>
        public void Dispose()
        {
            foreach (Utf8Marshal.PinnedUtf8String pin in _pins)
            {
                pin.Dispose();
            }

            _pins.Clear();
        }

        private List<(IntPtr Type, IntPtr Name)> EntityAt(IntPtr pointer) =>
            _entities[(int)pointer - EntityPointerBase];

        private IntPtr Pin(string? value)
        {
            if (value is null)
            {
                return IntPtr.Zero;
            }

            Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(value);
            _pins.Add(pinned);
            return pinned.Pointer;
        }

        /// <summary>One result row: an entity stand-in plus its pinned quota entries.</summary>
        private sealed class QuotaRow
        {
            internal QuotaRow(IntPtr entity, List<(IntPtr Key, double Value)> quotas)
            {
                Entity = entity;
                Quotas = quotas;
            }

            internal IntPtr Entity { get; }

            internal List<(IntPtr Key, double Value)> Quotas { get; }
        }
    }

    private static AclBindingFilter Filter(string name) =>
        new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, name, PatternType.Literal),
            new AccessControlEntryFilter("User:alice", "*", AclOperation.Read, AclPermissionType.Allow));

    private static AclBinding Binding(string name) =>
        new AclBinding(
            new ResourcePattern(ResourceType.Topic, name, PatternType.Literal),
            new AccessControlEntry("User:alice", "*", AclOperation.Read, AclPermissionType.Allow));

    /// <summary>
    /// A synthetic <c>delete_acls</c> result table, plus the accessor sets that read it.
    /// Owns every pinned string and every <c>KafkaError</c> handle it hands the walk.
    /// </summary>
    private sealed class Fixture : IDisposable
    {
        private const int FilterPointerBase = 0x7000;
        private const int BindingPointerBase = 0x8000;

        private readonly List<Utf8Marshal.PinnedUtf8String> _pins =
            new List<Utf8Marshal.PinnedUtf8String>();

        private readonly List<IntPtr> _errors = new List<IntPtr>();
        private readonly List<Row> _rows = new List<Row>();
        private readonly List<AclBinding> _bindings = new List<AclBinding>();

        /// <summary>The seven flat filter accessors over this fixture's rows.</summary>
        internal AclRowMarshal.FilterAccessors FilterAccessors =>
            new AclRowMarshal.FilterAccessors(
                pointer => (int)RowAt(pointer).ResourceType,
                pointer => RowAt(pointer).Name,
                pointer => (int)RowAt(pointer).PatternType,
                pointer => RowAt(pointer).Principal,
                pointer => RowAt(pointer).Host,
                pointer => (int)RowAt(pointer).Operation,
                pointer => (int)RowAt(pointer).PermissionType);

        /// <summary>Registers a filter row whose decoded key is <see cref="Filter"/>.</summary>
        internal AclBindingFilter AddFilter(string name)
        {
            AddFilterPointer(
                ResourceType.Topic, name, PatternType.Literal,
                "User:alice", "*", AclOperation.Read, AclPermissionType.Allow);
            return Filter(name);
        }

        /// <summary>Registers a filter row column by column and returns its stand-in pointer.</summary>
        internal IntPtr AddFilterPointer(
            ResourceType resourceType,
            string? name,
            PatternType patternType,
            string? principal,
            string? host,
            AclOperation operation,
            AclPermissionType permissionType)
        {
            _rows.Add(
                new Row(
                    resourceType,
                    Pin(name),
                    patternType,
                    Pin(principal),
                    Pin(host),
                    operation,
                    permissionType));

            return new IntPtr(FilterPointerBase + _rows.Count - 1);
        }

        /// <summary>Faults the whole filter — the <c>get_error(i)</c> channel.</summary>
        internal void FailFilter(AclBindingFilter key, int code, string message) =>
            RowFor(key).FilterError = NewError(code, message);

        /// <summary>Adds a deleted binding to a filter — the <c>get_binding(i, j)</c> slot.</summary>
        internal void AddBinding(AclBindingFilter key, AclBinding binding)
        {
            _bindings.Add(binding);
            RowFor(key).Entries.Add(
                new Entry(new IntPtr(BindingPointerBase + _bindings.Count - 1), IntPtr.Zero));
        }

        /// <summary>
        /// Adds a failed deletion to a filter — the <c>get_result_error(i, j)</c> slot, which
        /// is a <b>value</b>, not a fault.
        /// </summary>
        internal void AddInnerError(AclBindingFilter key, int code, string message) =>
            RowFor(key).Entries.Add(new Entry(IntPtr.Zero, NewError(code, message)));

        /// <summary>Runs production's walk over this table and wraps the outcome.</summary>
        internal DeleteAclsResult Walk()
        {
            List<AclBindingFilter> keys = Enumerable
                .Range(0, _rows.Count)
                .Select(index => AclRowMarshal.ReadFilter(
                    new IntPtr(FilterPointerBase + index), FilterAccessors))
                .ToList();

            KeyedAdminOperation<AclBindingFilter, DeleteAclsResult.FilterResults> operation =
                new KeyedAdminOperation<AclBindingFilter, DeleteAclsResult.FilterResults>(
                    "deleteAcls", keys, EqualityComparer<AclBindingFilter>.Default);

            Func<IntPtr, int, DeleteAclsResult.FilterResults> readValue =
                DeleteAclsResultMarshal.FilterResultsReader(
                    (root, index) =>
                    {
                        Assert.Equal(s_root, root);
                        return _rows[index].Entries.Count;
                    },
                    (root, index, resultIndex) =>
                    {
                        Assert.Equal(s_root, root);
                        return _rows[index].Entries[resultIndex].Binding;
                    },
                    (root, index, resultIndex) =>
                    {
                        Assert.Equal(s_root, root);
                        return _rows[index].Entries[resultIndex].Error;
                    },
                    pointer => _bindings[(int)pointer - BindingPointerBase]);

            SyntheticPerKeyWalk.Run(
                operation,
                _rows.Count,
                index => AclRowMarshal.ReadFilter(
                    new IntPtr(FilterPointerBase + index), FilterAccessors),
                index => _rows[index].FilterError,
                index => readValue(s_root, index));

            // The trampoline's rescue, mirrored: an unnamed key would hang forever.
            operation.FailUncompleted();
            return new DeleteAclsResult(operation.Tasks);
        }

        /// <summary>
        /// Releases every pin and destroys every error handle. ⚠ A walk that destroyed a
        /// borrowed error would make this a double free, aborting the process.
        /// </summary>
        public void Dispose()
        {
            foreach (Utf8Marshal.PinnedUtf8String pin in _pins)
            {
                pin.Dispose();
            }

            foreach (IntPtr error in _errors)
            {
                NativeMethods.ErrorDestroy(error);
            }

            _pins.Clear();
            _errors.Clear();
        }

        private IntPtr Pin(string? value)
        {
            if (value is null)
            {
                return IntPtr.Zero;
            }

            Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(value);
            _pins.Add(pinned);
            return pinned.Pointer;
        }

        private IntPtr NewError(int code, string message)
        {
            using Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message);
            IntPtr error = NativeMethods.KafkaErrorNew(code, pinned.Pointer);
            Assert.NotEqual(IntPtr.Zero, error);
            _errors.Add(error);
            return error;
        }

        private Row RowAt(IntPtr pointer) => _rows[(int)pointer - FilterPointerBase];

        private Row RowFor(AclBindingFilter key)
        {
            for (int index = 0; index < _rows.Count; index++)
            {
                if (AclRowMarshal.ReadFilter(
                        new IntPtr(FilterPointerBase + index), FilterAccessors).Equals(key))
                {
                    return _rows[index];
                }
            }

            throw new Xunit.Sdk.XunitException("the fixture holds no row for that filter");
        }

        /// <summary>One outer row: the seven decoded columns, its fault, and its entries.</summary>
        private sealed class Row
        {
            internal Row(
                ResourceType resourceType,
                IntPtr name,
                PatternType patternType,
                IntPtr principal,
                IntPtr host,
                AclOperation operation,
                AclPermissionType permissionType)
            {
                ResourceType = resourceType;
                Name = name;
                PatternType = patternType;
                Principal = principal;
                Host = host;
                Operation = operation;
                PermissionType = permissionType;
            }

            internal ResourceType ResourceType { get; }

            internal IntPtr Name { get; }

            internal PatternType PatternType { get; }

            internal IntPtr Principal { get; }

            internal IntPtr Host { get; }

            internal AclOperation Operation { get; }

            internal AclPermissionType PermissionType { get; }

            internal IntPtr FilterError { get; set; }

            internal List<Entry> Entries { get; } = new List<Entry>();
        }

        /// <summary>One inner entry: a binding pointer XOR a borrowed error handle.</summary>
        private sealed class Entry
        {
            internal Entry(IntPtr binding, IntPtr error)
            {
                Binding = binding;
                Error = error;
            }

            internal IntPtr Binding { get; }

            internal IntPtr Error { get; }
        }
    }
}
