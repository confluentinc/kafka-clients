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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M15/P13.3 (c), widened by M15/P13.4 — the shared admin-string precondition,
/// <see cref="AdminStrings"/>: its exact accept/reject boundary, and that a rejection happens
/// <b>before</b> the native submit (ffi §B5), leaving nothing held that would defer the
/// client's release and leaving the client usable.
/// </summary>
/// <remarks>
/// The ordering tests cover one site per <b>shape</b> of guarded string, since each shape
/// places its guard differently: a per-key key inline (<c>createTopics</c>) and in a de-dup
/// helper (<c>deleteTopics</c>); a direct request field (an IAC config value); an
/// <c>*Options</c> string (the ListTransactions pattern); a marshaller-fed row (a quota filter's
/// match name, checked over the caller's filter); a principal row (a delegation-token owner);
/// a result lookup (<c>DescribeUserScramCredentialsResult.Description</c>); and a mock seeding
/// method.
/// </remarks>
public sealed class AdminStringsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_releaseBound = TimeSpan.FromSeconds(5);

    private const string InvalidStringMessage =
        "An admin request string must not contain a NUL character or an unpaired UTF-16 " +
        "surrogate: such a string cannot be passed to the native client unchanged.";

    /// <summary>
    /// The strings under test, by label. ⚠ <b>Looked up by label, never passed as theory
    /// data</b>: xUnit serializes <c>InlineData</c> strings to hand them to the runner, and that
    /// round trip replaces a lone surrogate with U+FFFD — so a surrogate case passed inline
    /// silently tests a valid string instead (observed: every lone-surrogate row went green
    /// against a guard that was never reached).
    /// </summary>
    private static readonly IReadOnlyDictionary<string, string?> s_accepted =
        new Dictionary<string, string?>(StringComparer.Ordinal)
        {
            ["null"] = null,
            ["empty"] = "",
            ["ascii"] = "plain",
            ["latin"] = "délété",
            ["pair"] = "\uD83C\uDF88",
            ["pair-inside"] = "a\uD83C\uDF88b",
            ["two-pairs"] = "\uD83C\uDF88\uD83D\uDE00",
            ["replacement-char"] = "\uFFFD",
        };

    /// <inheritdoc cref="s_accepted"/>
    private static readonly IReadOnlyDictionary<string, string> s_rejected =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["nul-only"] = "\0",
            ["nul-last"] = "a\0",
            ["nul-first"] = "\0a",
            ["nul-inside"] = "a\0b",
            ["lone-high"] = "\uD800",
            ["lone-low"] = "\uDC00",
            ["high-at-end"] = "a\uD83C",
            ["high-then-ascii"] = "\uD83Ca",
            ["reversed-pair"] = "\uDF88\uD83C",
            ["pair-then-low"] = "\uD83C\uDF88\uDF88",
            ["high-then-pair"] = "\uD83C\uD83C\uDF88",
        };

    /// <summary>The labels of <see cref="s_accepted"/>.</summary>
    public static IEnumerable<object[]> AcceptedLabels() => s_accepted.Keys.Select(label => new object[] { label });

    /// <summary>The labels of <see cref="s_rejected"/>.</summary>
    public static IEnumerable<object[]> RejectedLabels() => s_rejected.Keys.Select(label => new object[] { label });

    /// <summary>
    /// The label tables really hold what their names say — the guard against the serialization
    /// trap above creeping back through a table edit: each rejected string has a NUL or an
    /// unpaired surrogate, and no accepted one does.
    /// </summary>
    [Fact]
    public void LabelTables_HoldTheStringsTheirNamesSay()
    {
        Assert.Equal(11, s_rejected.Count);
        Assert.Equal(8, s_accepted.Count);
        Assert.Equal(1, s_rejected["lone-high"].Length);
        Assert.True(char.IsHighSurrogate(s_rejected["lone-high"][0]));
        Assert.True(char.IsLowSurrogate(s_rejected["lone-low"][0]));
        Assert.Equal('\0', s_rejected["nul-only"][0]);
        Assert.True(char.IsSurrogatePair(s_accepted["pair"]![0], s_accepted["pair"]![1]));
    }

    /// <summary>
    /// Every string whose UTF-8 form round-trips — including a well-formed surrogate pair at
    /// either end — passes, and so does <see langword="null"/>, which is each site's own
    /// precondition to decide.
    /// </summary>
    [Theory]
    [MemberData(nameof(AcceptedLabels))]
    public void Validate_AcceptsAStringThatCrossesUnchanged(string label)
    {
        AdminStrings.Validate(s_accepted[label], "p");
    }

    /// <summary>
    /// Each way a string can fail to round-trip: a NUL anywhere, a lone low surrogate, a high
    /// surrogate at the end or followed by a non-low char, and a pair in the wrong order.
    /// </summary>
    [Theory]
    [MemberData(nameof(RejectedLabels))]
    public void Validate_RejectsAStringTheAbiWouldChange(string label)
    {
        ArgumentException rejected =
            Assert.Throws<ArgumentException>(() => AdminStrings.Validate(s_rejected[label], "keys"));

        Assert.Equal("keys", rejected.ParamName);
        Assert.StartsWith(InvalidStringMessage, rejected.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>createTopics</c> (an inline loop site): the rejection happens before production's
    /// submit is reached — the stand-in never runs — and nothing was pinned, allocated or
    /// ref-counted, so the client's native release is not deferred.
    /// </summary>
    [Fact]
    public void CreateTopics_RejectsBeforeTheSubmit_AndHoldsNothing()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        bool submitted = false;

        ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.CreateTopics(
            new[] { new NewTopic("ok", 1, 1), new NewTopic("a\0b", 1, 1) },
            options: null,
            (nativeHandle, topics, count, timeoutMs, validateOnly, retry, callback, userData) => submitted = true));

        Assert.Equal("newTopics", rejected.ParamName);
        Assert.StartsWith(InvalidStringMessage, rejected.Message, StringComparison.Ordinal);
        Assert.False(submitted);
        Assert.True(DisposeAndAwaitRelease(admin, handle), "a rejected call must hold no client reference");
    }

    /// <summary>
    /// <c>deleteTopics</c> by name (a de-dup-helper site): the same ordering, and the second of
    /// two keys that would collapse is where the rejection lands — the first is valid.
    /// </summary>
    [Fact]
    public void DeleteTopics_RejectsBeforeTheSubmit_AndHoldsNothing()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        bool submitted = false;

        ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.DeleteTopics(
            TopicCollection.OfTopicNames(new[] { "fine", "x\uDC00" }),
            options: null,
            (nativeHandle, keys, count, timeoutMs, retry, callback, userData) => submitted = true,
            (nativeHandle, keys, count, timeoutMs, retry, callback, userData) => submitted = true));

        Assert.Equal("topics", rejected.ParamName);
        Assert.StartsWith(InvalidStringMessage, rejected.Message, StringComparison.Ordinal);
        Assert.False(submitted);
        Assert.True(DisposeAndAwaitRelease(admin, handle), "a rejected call must hold no client reference");
    }

    /// <summary>
    /// A direct request field — an <c>incrementalAlterConfigs</c> config value: rejected before
    /// the submit, and the same client then alters the same config.
    /// </summary>
    [Fact]
    public async Task IncrementalAlterConfigs_AConfigValue_RejectsBeforeTheSubmit_AndHoldsNothing()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        bool submitted = false;
        ConfigResource topic = new ConfigResource(ConfigResourceType.Topic, "b5-configs");
        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic("b5-configs", 1, 1) }, null).All(), s_deadline);

        ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.IncrementalAlterConfigs(
            SetOp(topic, "retention.ms", "10\u0000tail"),
            options: null,
            (nativeHandle, resourceTypes, resourceNames, configNames, configValues, opTypes, count, timeoutMs,
                validateOnly, callback, userData) => submitted = true));

        Assert.Equal("configs", rejected.ParamName);
        Assert.StartsWith(InvalidStringMessage, rejected.Message, StringComparison.Ordinal);
        Assert.False(submitted);

        await TestTimeout.Run(() => admin.IncrementalAlterConfigs(SetOp(topic, "retention.ms", "1000"), null).All(), s_deadline);
        Config? config = null;
        await TestTimeout.Run(async () => config = await admin.DescribeConfigs(new[] { topic }, null).Values[topic], s_deadline);
        Assert.Equal("1000", config!.Get("retention.ms")!.Value);

        Assert.True(DisposeAndAwaitRelease(admin, handle), "a rejected call must hold no client reference");
    }

    /// <summary>
    /// An <c>*Options</c> string — <c>listTransactions</c>' transactional-id pattern: rejected
    /// before the submit, and the same client then sends a valid pattern, which the mock
    /// answers with Java's own message for this RPC.
    /// </summary>
    [Fact]
    public async Task ListTransactions_ATransactionalIdPattern_RejectsBeforeTheSubmit_AndHoldsNothing()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        bool submitted = false;

        ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.ListTransactions(
            new ListTransactionsOptions { FilteredTransactionalIdPattern = "txn-\uD800" },
            (nativeHandle, states, stateCount, producerIds, producerIdCount, durationMs, pattern, timeoutMs,
                byBrokerIdCallback, callback, userData) => submitted = true));

        Assert.Equal("options", rejected.ParamName);
        Assert.StartsWith(InvalidStringMessage, rejected.Message, StringComparison.Ordinal);
        Assert.False(submitted);

        ListTransactionsResult result =
            admin.ListTransactions(new ListTransactionsOptions { FilteredTransactionalIdPattern = "txn-.*" });
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(result.All, s_deadline));
        Assert.Equal(35, failure.Code);
        Assert.Equal("Not implemented yet", failure.Message);

        Assert.True(DisposeAndAwaitRelease(admin, handle), "a rejected call must hold no client reference");
    }

    /// <summary>
    /// A marshaller-fed row — a <c>describeClientQuotas</c> filter's match name, which the RPC
    /// checks over the caller's filter because the marshaller runs after the operation is
    /// rooted: rejected before the submit, and the same client then sends a valid filter, which
    /// the mock answers with Java's own message (typo included).
    /// </summary>
    [Fact]
    public async Task DescribeClientQuotas_AMatchName_RejectsBeforeTheSubmit_AndHoldsNothing()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        bool submitted = false;

        ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.DescribeClientQuotas(
            UserFilter("ali\u0000ce"),
            options: null,
            (nativeHandle, entityTypes, matchTypes, matchNames, count, strict, timeoutMs, callback, userData) =>
                submitted = true));

        Assert.Equal("filter", rejected.ParamName);
        Assert.StartsWith(InvalidStringMessage, rejected.Message, StringComparison.Ordinal);
        Assert.False(submitted);

        DescribeClientQuotasResult result = admin.DescribeClientQuotas(UserFilter("alice"), null);
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(result.Entities, s_deadline));
        Assert.Equal(35, failure.Code);
        Assert.Equal("Not implement yet", failure.Message);

        Assert.True(DisposeAndAwaitRelease(admin, handle), "a rejected call must hold no client reference");
    }

    /// <summary>
    /// A principal row — a <c>describeDelegationToken</c> owner filter: rejected before the
    /// submit, and the same client then describes with a valid owner, which the mock answers
    /// with no tokens.
    /// </summary>
    [Fact]
    public async Task DescribeDelegationToken_AnOwnerName_RejectsBeforeTheSubmit_AndHoldsNothing()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        bool submitted = false;

        ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.DescribeDelegationToken(
            new DescribeDelegationTokenOptions { Owners = new[] { new KafkaPrincipal("User", "alice\uDC00") } },
            (nativeHandle, hasOwnersFilter, ownerPrincipalTypes, ownerNames, ownerCount, timeoutMs, callback, userData) =>
                submitted = true));

        Assert.Equal("options", rejected.ParamName);
        Assert.StartsWith(InvalidStringMessage, rejected.Message, StringComparison.Ordinal);
        Assert.False(submitted);

        DescribeDelegationTokenResult result = admin.DescribeDelegationToken(
            new DescribeDelegationTokenOptions { Owners = new[] { new KafkaPrincipal("User", "alice") } });
        IReadOnlyList<DelegationToken> tokens = Array.Empty<DelegationToken>();
        await TestTimeout.Run(async () => tokens = await result.DelegationTokens(), s_deadline);
        Assert.Empty(tokens);

        Assert.True(DisposeAndAwaitRelease(admin, handle), "a rejected call must hold no client reference");
    }

    /// <summary>
    /// A result lookup — <see cref="DescribeUserScramCredentialsResult.Description"/>. There is
    /// no submit to precede here: the RPC was submitted when the result was built, and the
    /// lookup takes no reference on the client of its own. So the ordering asserted is the one
    /// this shape has — the rejection is <b>synchronous</b>, ahead of the await on the result,
    /// which the mock faults (a guard behind that await would surface the mock's
    /// <see cref="KafkaException"/> instead). A valid lookup on the same result then settles
    /// with the mock's own message, and the release proves the RPC's reference was returned
    /// and the rejected lookup added none.
    /// </summary>
    [Fact]
    public async Task DescribeUserScramCredentials_ADescriptionLookup_RejectsSynchronously_AndHoldsNothing()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        DescribeUserScramCredentialsResult result = admin.DescribeUserScramCredentials(new[] { "alice" }, null);

        ArgumentException rejected = Assert.Throws<ArgumentException>(() => { _ = result.Description("ali\uD800ce"); });

        Assert.Equal("userName", rejected.ParamName);
        Assert.StartsWith(InvalidStringMessage, rejected.Message, StringComparison.Ordinal);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Description("alice"), s_deadline));
        Assert.Equal(35, failure.Code);
        Assert.Equal("Not implemented yet", failure.Message);

        Assert.True(DisposeAndAwaitRelease(admin, handle), "a rejected lookup must hold no client reference");
    }

    /// <summary>
    /// A mock seeding method — <c>updateBeginningOffsets</c>. There is no submit stand-in
    /// here: the seeding call is synchronous and passes the handle as its P/Invoke parameter,
    /// so "before the submit" is asserted on the outcome instead — the valid entry beside the
    /// rejected one was <b>not</b> seeded, so nothing of the call reached the mock. The same
    /// client then seeds validly.
    /// </summary>
    [Fact]
    public async Task UpdateBeginningOffsets_ATopic_RejectsBeforeTheNativeCall_AndHoldsNothing()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;
        TopicPartition kept = new TopicPartition("b5-seed", 0);

        ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.UpdateBeginningOffsets(
            new Dictionary<TopicPartition, long> { [kept] = 42, [new TopicPartition("b5-\u0000seed", 0)] = 7 }));

        Assert.Equal("offsets", rejected.ParamName);
        Assert.StartsWith(InvalidStringMessage, rejected.Message, StringComparison.Ordinal);
        Assert.Equal(-1, await EarliestOffset(admin, kept));

        admin.UpdateBeginningOffsets(new Dictionary<TopicPartition, long> { [kept] = 42 });
        Assert.Equal(42, await EarliestOffset(admin, kept));

        Assert.True(DisposeAndAwaitRelease(admin, handle), "a rejected call must hold no client reference");
    }

    private static Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> SetOp(
        ConfigResource resource, string name, string value) =>
        new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
        {
            [resource] = new[] { new AlterConfigOp(new ConfigEntry(name, value), AlterConfigOpType.Set) },
        };

    private static ClientQuotaFilter UserFilter(string name) =>
        ClientQuotaFilter.Contains(new[] { ClientQuotaFilterComponent.OfEntity(ClientQuotaEntity.User, name) });

    private static async Task<long> EarliestOffset(NativeAdminClient admin, TopicPartition partition)
    {
        long offset = 0;
        await TestTimeout.Run(
            async () => offset = (await admin
                .ListOffsets(new Dictionary<TopicPartition, OffsetSpec> { [partition] = OffsetSpec.Earliest() }, null)
                .PartitionResult(partition)).Offset,
            s_deadline);
        return offset;
    }

    private static bool DisposeAndAwaitRelease(NativeAdminClient admin, SafeAdminHandle handle)
    {
        TestTimeout.Run(admin.Dispose, s_deadline);
        return SpinWait.SpinUntil(() => handle.IsClosed, s_releaseBound);
    }
}
