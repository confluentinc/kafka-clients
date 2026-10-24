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
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The shared admin-string precondition, <see cref="AdminStrings"/>: its exact accept/reject boundary, and that a
/// rejection happens before the native call and leaves the client usable.
/// </summary>
/// <remarks>
/// The ordering tests cover one site per shape of guarded string, since each shape places its guard differently: a
/// per-key key inline (<c>createTopics</c>) and in a de-dup helper (<c>deleteTopics</c>); a direct request field (an
/// IAC config value); an <c>*Options</c> string (the ListTransactions pattern); a marshaller-fed row (a quota
/// filter's match name); a principal row (a delegation-token owner); a result lookup
/// (<c>DescribeUserScramCredentialsResult.Description</c>); and a mock seeding method.
/// </remarks>
public sealed class AdminStringsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan s_releaseBound = TimeSpan.FromSeconds(5);

    private const string InvalidStringMessage =
        "An admin request string must not contain a NUL character or an unpaired UTF-16 " +
        "surrogate: such a string cannot be passed to the native client unchanged.";

    // Looked up by label, never passed as theory data: the InlineData round trip replaces a lone surrogate with
    // U+FFFD, so the row would test a valid string.
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

    public static IEnumerable<object[]> AcceptedLabels() => s_accepted.Keys.Select(label => new object[] { label });

    public static IEnumerable<object[]> RejectedLabels() => s_rejected.Keys.Select(label => new object[] { label });

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

    [Theory]
    [MemberData(nameof(AcceptedLabels))]
    public void Validate_AcceptsAStringThatCrossesUnchanged(string label)
    {
        AdminStrings.Validate(s_accepted[label], "p");
    }

    [Theory]
    [MemberData(nameof(RejectedLabels))]
    public void Validate_RejectsAStringTheAbiWouldChange(string label)
    {
        ArgumentException rejected =
            Assert.Throws<ArgumentException>(() => AdminStrings.Validate(s_rejected[label], "keys"));

        Assert.Equal("keys", rejected.ParamName);
        Assert.StartsWith(InvalidStringMessage, rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CreateTopics_RejectsBeforeTheCall()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        try
        {
            ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.CreateTopics(
                new[] { new NewTopic("ok", 1, 1), new NewTopic("a\0b", 1, 1) },
                null));

            Assert.Equal("newTopics", rejected.ParamName);
            Assert.StartsWith(InvalidStringMessage, rejected.Message, StringComparison.Ordinal);
            await TestTimeout.Run(
                () => AsyncCreateTopicsResult.OfClient(admin.CreateTopics(new[] { new NewTopic("ok", 1, 1) }, null)).All().Get(),
                s_deadline);
        }
        finally
        {
            TestTimeout.Run(admin.Dispose, s_deadline);
        }
    }

    [Fact]
    public async Task DeleteTopics_RejectsBeforeTheCall()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        try
        {
            await TestTimeout.Run(
                () => AsyncCreateTopicsResult.OfClient(admin.CreateTopics(new[] { new NewTopic("fine", 1, 1) }, null)).All().Get(),
                s_deadline);

            ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.DeleteTopics(
                TopicCollection.OfTopicNames(new[] { "fine", "x\uDC00" }),
                options: null));

            Assert.Equal("topics", rejected.ParamName);
            Assert.StartsWith(InvalidStringMessage, rejected.Message, StringComparison.Ordinal);
            await TestTimeout.Run(
                () => AsyncDeleteTopicsResult.OfClient(
                    admin.DeleteTopics(TopicCollection.OfTopicNames(new[] { "fine" }), options: null)).All().Get(),
                s_deadline);
        }
        finally
        {
            TestTimeout.Run(admin.Dispose, s_deadline);
        }
    }

    [Fact]
    public async Task IncrementalAlterConfigs_AConfigValue_RejectsBeforeTheCall()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        try
        {
            ConfigResource topic = new ConfigResource(ConfigResourceType.Topic, "b5-configs");
            await TestTimeout.Run(
                () => AsyncCreateTopicsResult.OfClient(admin.CreateTopics(new[] { new NewTopic("b5-configs", 1, 1) }, null)).All().Get(),
                s_deadline);

            ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.IncrementalAlterConfigs(
                SetOp(topic, "retention.ms", "10\u0000tail"),
                options: null));

            Assert.Equal("configs", rejected.ParamName);
            Assert.StartsWith(InvalidStringMessage, rejected.Message, StringComparison.Ordinal);
            Config before = await TestTimeout.Run(
                () => AsyncDescribeConfigsResult.OfClient(admin.DescribeConfigs(new[] { topic }, null)).Values[topic].Get(),
                s_deadline);
            Assert.Null(before.Get("retention.ms"));

            await TestTimeout.Run(
                () => AsyncAlterConfigsResult.OfClient(admin.IncrementalAlterConfigs(SetOp(topic, "retention.ms", "1000"), null)).All().Get(),
                s_deadline);
            Config config = await TestTimeout.Run(
                () => AsyncDescribeConfigsResult.OfClient(admin.DescribeConfigs(new[] { topic }, null)).Values[topic].Get(),
                s_deadline);
            Assert.Equal("1000", config.Get("retention.ms")!.Value);
        }
        finally
        {
            TestTimeout.Run(admin.Dispose, s_deadline);
        }
    }

    [Fact]
    public async Task ListTransactions_ATransactionalIdPattern_RejectsBeforeTheCall()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        try
        {
            ArgumentException rejected = Assert.Throws<ArgumentException>(
                () => admin.ListTransactions(new ListTransactionsOptions { FilteredTransactionalIdPattern = "txn-\uD800" }));

            Assert.Equal("options", rejected.ParamName);
            Assert.StartsWith(InvalidStringMessage, rejected.Message, StringComparison.Ordinal);

            AsyncListTransactionsResult result = AsyncListTransactionsResult.OfClient(
                admin.ListTransactions(new ListTransactionsOptions { FilteredTransactionalIdPattern = "txn-.*" }));
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => result.All().Get(), s_deadline));
            Assert.Equal(35, failure.Code);
            Assert.Equal("Not implemented yet", failure.Message);
        }
        finally
        {
            TestTimeout.Run(admin.Dispose, s_deadline);
        }
    }

    [Fact]
    public async Task DescribeClientQuotas_AMatchName_RejectsBeforeTheCall()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        try
        {
            ArgumentException rejected = Assert.Throws<ArgumentException>(
                () => admin.DescribeClientQuotas(UserFilter("ali\u0000ce"), options: null));

            Assert.Equal("filter", rejected.ParamName);
            Assert.StartsWith(InvalidStringMessage, rejected.Message, StringComparison.Ordinal);

            AsyncDescribeClientQuotasResult result =
                AsyncDescribeClientQuotasResult.OfClient(admin.DescribeClientQuotas(UserFilter("alice"), options: null));
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => result.Entities().Get(), s_deadline));
            Assert.Equal(35, failure.Code);
            Assert.Equal("Not implement yet", failure.Message);
        }
        finally
        {
            TestTimeout.Run(admin.Dispose, s_deadline);
        }
    }

    [Fact]
    public async Task DescribeDelegationToken_AnOwnerName_RejectsBeforeTheCall()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        try
        {
            ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.DescribeDelegationToken(
                new DescribeDelegationTokenOptions { Owners = new[] { new KafkaPrincipal("User", "alice\uDC00") } }));

            Assert.Equal("options", rejected.ParamName);
            Assert.StartsWith(InvalidStringMessage, rejected.Message, StringComparison.Ordinal);

            AsyncDescribeDelegationTokenResult result = AsyncDescribeDelegationTokenResult.OfClient(
                admin.DescribeDelegationToken(
                    new DescribeDelegationTokenOptions { Owners = new[] { new KafkaPrincipal("User", "alice") } }));
            IReadOnlyList<DelegationToken> tokens = Array.Empty<DelegationToken>();
            await TestTimeout.Run(async () => tokens = await result.DelegationTokens().Get(), s_deadline);
            Assert.Empty(tokens);
        }
        finally
        {
            TestTimeout.Run(admin.Dispose, s_deadline);
        }
    }

    [Fact]
    public async Task DescribeUserScramCredentials_ADescriptionLookup_RejectsSynchronously()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        try
        {
            AsyncDescribeUserScramCredentialsResult result =
                AsyncDescribeUserScramCredentialsResult.OfClient(admin.DescribeUserScramCredentials(new[] { "alice" }, null));

            ArgumentException rejected = Assert.Throws<ArgumentException>(() => { _ = result.Description("ali\uD800ce"); });

            Assert.Equal("userName", rejected.ParamName);
            Assert.StartsWith(InvalidStringMessage, rejected.Message, StringComparison.Ordinal);

            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => result.Description("alice").Get(), s_deadline));
            Assert.Equal(35, failure.Code);
            Assert.Equal("Not implemented yet", failure.Message);
        }
        finally
        {
            TestTimeout.Run(admin.Dispose, s_deadline);
        }
    }

    [Fact]
    public async Task UpdateBeginningOffsets_ATopic_RejectsBeforeTheNativeCall_AndHoldsNothing()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminClientHandle handle = HandleOf(admin);
        TopicPartition kept = new TopicPartition("b5-seed", 0);

        ArgumentException rejected = Assert.Throws<ArgumentException>(() => admin.UpdateBeginningOffsets(
            new Dictionary<TopicPartition, long> { [kept] = 42, [new TopicPartition("b5-\u0000seed", 0)] = 7 }));

        Assert.Equal("newOffsets", rejected.ParamName);
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

    private static Task<long> EarliestOffset(NativeAdminClient admin, TopicPartition partition) =>
        Task.FromResult(admin
            .ListOffsets(new Dictionary<TopicPartition, OffsetSpec> { [partition] = OffsetSpec.Earliest() }, null)
            .Futures[partition].Get(s_deadline).Offset);

    private static SafeAdminClientHandle HandleOf(NativeAdminClient native) =>
        (SafeAdminClientHandle)typeof(NativeAdminClient)
            .GetField("_handle", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(native)!;

    private static bool DisposeAndAwaitRelease(NativeAdminClient admin, SafeAdminClientHandle handle)
    {
        TestTimeout.Run(admin.Dispose, s_deadline);
        return SpinWait.SpinUntil(() => handle.IsClosed, s_releaseBound);
    }
}
