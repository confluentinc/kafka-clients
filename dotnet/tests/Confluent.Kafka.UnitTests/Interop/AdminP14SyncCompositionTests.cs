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
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Quota;
using Confluent.Kafka.Common.Resource;
using Confluent.Kafka.Common.Security.Auth;
using Confluent.Kafka.Common.Security.Token.Delegation;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Each sync Result and its async twin, built by their constructors over <c>KafkaFutureImpl</c>s that the harness
/// completes identically, give the same outcome on every row: the same value, or the same exception instance.
/// </summary>
/// <remarks>
/// <para>
/// One <see cref="TheoryAttribute"/> per Result pair, each row run on both flavours. The rows: all keys succeed;
/// one fails; two fail (the aggregate rethrows the first in <b>key</b> order, not completion order); all fail; an
/// unknown key; a null key; per-key independence (one key's outcome does not wait for another's, and the aggregate
/// waits for every key); memoization (a projection or gathered aggregate is computed once per future, so its value or
/// failure is the same instance on every <c>Get()</c>); and erasure identity (a key's void future is equal on every
/// read and completes with its key). Each pair runs the rows that apply to its shape; the rows a pair omits, and why,
/// are listed above its row data.
/// </para>
/// <para>
/// Every wait is bounded: a sync <c>Get()</c> runs on a dedicated thread (<see cref="BlockingCall"/>), an async one
/// under <see cref="TestTimeout"/>.
/// </para>
/// </remarks>
public sealed class AdminP14SyncCompositionTests
{
    private const string Sync = "sync";

    private const string Async = "async";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly string[] s_topics = { "alpha", "beta", "gamma" };

    private static readonly string[] s_flavours = { Sync, Async };

    private static readonly string[] s_groups = { "group", "Group", "GROUP" };

    private const string UnknownGroup = "gRoUp";

    private static readonly string[] s_users = { "alice", "Alice", "ALICE" };

    private const string UnknownUser = "aLiCe";

    private static readonly string[] s_features = { "metadata.version", "group.version", "transaction.version" };

    private static readonly string[] s_transactionalIds = { "txn", "Txn", "TXN" };

    private const string UnknownTransactionalId = "tXn";

    public static TheoryData<string, string> CreateTopicsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "memoization",
        "erasure-identity");

    [Theory]
    [MemberData(nameof(CreateTopicsRows))]
    public async Task CreateTopics_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        CreateTopicsHarness harness = CreateTopicsHarness.For(flavour);
        switch (row)
        {
            case "all-succeed":
                {
                    Config gammaConfig = new Config(new[] { new ConfigEntry("cleanup.policy", "compact") });
                    Uuid betaId = new Uuid(0x0102030405060708L, 0x1112131415161718L);
                    harness.SetResult("alpha", Metadata(3, 2));
                    harness.SetResult("beta", new CreateTopicsResult.TopicMetadataAndConfig(betaId, 1, 1, Empty()));
                    harness.SetResult("gamma", new CreateTopicsResult.TopicMetadataAndConfig(Uuid.Zero, 5, 3, gammaConfig));

                    Assert.Equal(s_topics, harness.ValuesKeys());
                    foreach (string topic in s_topics)
                    {
                        FutureProbe created = harness.Values(topic);
                        Assert.True(created.IsDone, topic);
                        AssertReturned(await created.Outcome());
                    }

                    AssertReturned(await harness.All().Outcome());
                    Assert.Equal((object)3, await harness.NumPartitions("alpha").Outcome());
                    Assert.Equal((object)2, await harness.ReplicationFactor("alpha").Outcome());
                    Assert.Equal((object)betaId, await harness.TopicId("beta").Outcome());
                    Assert.Same(gammaConfig, await harness.Config("gamma").Outcome());
                    break;
                }

            case "one-fails":
                {
                    KafkaException beta = new KafkaException(38, "beta failed", isRetriable: false);
                    harness.SetResult("alpha", Metadata(3, 1));
                    harness.SetException("beta", beta);
                    harness.SetResult("gamma", Metadata(5, 1));

                    AssertReturned(await harness.Values("alpha").Outcome());
                    Assert.Same(beta, await harness.Values("beta").Outcome());
                    AssertReturned(await harness.Values("gamma").Outcome());
                    Assert.Same(beta, await harness.All().Outcome());
                    Assert.Same(beta, await harness.NumPartitions("beta").Outcome());
                    Assert.Same(beta, await harness.Config("beta").Outcome());
                    Assert.Equal((object)5, await harness.NumPartitions("gamma").Outcome());
                    break;
                }

            case "two-fail-first-in-key-order":
                {
                    // gamma fails FIRST in time, alpha last: the aggregate reports alpha, the first in key order.
                    KafkaException alpha = new KafkaException(41, "alpha failed", isRetriable: false);
                    KafkaException gamma = new KafkaException(43, "gamma failed", isRetriable: false);
                    harness.SetException("gamma", gamma);
                    harness.SetResult("beta", Metadata(1, 1));
                    harness.SetException("alpha", alpha);

                    Assert.Same(alpha, await harness.All().Outcome());
                    Assert.Same(gamma, await harness.Values("gamma").Outcome());
                    AssertReturned(await harness.Values("beta").Outcome());
                    break;
                }

            case "all-fail":
                {
                    KafkaException[] failures =
                    {
                    new KafkaException(41, "alpha failed", isRetriable: false),
                    new KafkaException(42, "beta failed", isRetriable: true),
                    new KafkaException(43, "gamma failed", isRetriable: false),
                };

                    // Completed in reverse key order, so key order and completion order disagree.
                    for (int index = s_topics.Length - 1; index >= 0; index--)
                    {
                        harness.SetException(s_topics[index], failures[index]);
                    }

                    Assert.Same(failures[0], await harness.All().Outcome());
                    for (int index = 0; index < s_topics.Length; index++)
                    {
                        Assert.Same(failures[index], await harness.Values(s_topics[index]).Outcome());
                        Assert.Same(failures[index], await harness.ReplicationFactor(s_topics[index]).Outcome());
                    }

                    break;
                }

            case "unknown-key":
                {
                    // The guard is synchronous, at the call site, with the exact message — before any future exists.
                    string expected = new ArgumentException(
                        "Topic 'delta' was not part of this createTopics request.", "topic").Message;
                    foreach (string accessor in CreateTopicsHarness.Accessors)
                    {
                        ArgumentException failure = Assert.IsType<ArgumentException>(harness.CallAccessor(accessor, "delta"));
                        Assert.Equal("topic", failure.ParamName);
                        Assert.Equal(expected, failure.Message);
                    }

                    Assert.IsType<KeyNotFoundException>(harness.CallValuesIndexer("delta"));
                    break;
                }

            case "null-key":
                {
                    string expected = new ArgumentNullException("topic").Message;
                    foreach (string accessor in CreateTopicsHarness.Accessors)
                    {
                        ArgumentNullException failure =
                            Assert.IsType<ArgumentNullException>(harness.CallAccessor(accessor, null!));
                        Assert.Equal("topic", failure.ParamName);
                        Assert.Equal(expected, failure.Message);
                    }

                    Assert.IsType<ArgumentNullException>(harness.CallValuesIndexer(null!));
                    break;
                }

            case "per-key-independence":
                {
                    FutureProbe beta = harness.Values("beta");
                    FutureProbe betaPartitions = harness.NumPartitions("beta");
                    FutureProbe all = harness.All();

                    harness.SetResult("alpha", Metadata(3, 1));

                    FutureProbe alpha = harness.Values("alpha");
                    Assert.True(alpha.IsDone);
                    AssertReturned(await alpha.Outcome());
                    Assert.Equal((object)3, await harness.NumPartitions("alpha").Outcome());

                    Assert.False(beta.IsDone);
                    Assert.False(all.IsDone);
                    beta.AssertPending();
                    all.AssertPending();

                    harness.SetResult("beta", Metadata(7, 1));
                    AssertReturned(await beta.Outcome());
                    Assert.Equal((object)7, await betaPartitions.Outcome());

                    // gamma is still outstanding, so the aggregate is too.
                    Assert.False(all.IsDone);

                    harness.SetResult("gamma", Metadata(1, 1));
                    AssertReturned(await all.Outcome());
                    break;
                }

            case "memoization":
                {
                    // Created, but with no metadata: each typed accessor of CreateTopicsResult.TopicMetadataAndConfig
                    // throws a FRESH KafkaException per call, so a projection applied twice would hand out two
                    // instances.
                    KafkaException stored = new KafkaException(7, "Topic metadata unavailable.", isRetriable: true);
                    harness.SetResult("alpha", new CreateTopicsResult.TopicMetadataAndConfig(stored));

                    AssertReturned(await harness.Values("alpha").Outcome());

                    FutureProbe partitions = harness.NumPartitions("alpha");
                    KafkaException first = Assert.IsType<KafkaException>(await partitions.Outcome());
                    Assert.Equal(7, first.Code);
                    Assert.True(first.IsRetriable);
                    Assert.Equal("Topic metadata unavailable.", first.Message);
                    Assert.Same(stored, first.InnerException);
                    Assert.Same(first, await partitions.Outcome());

                    // A new call is a new future, which projects afresh.
                    Assert.NotSame(first, await harness.NumPartitions("alpha").Outcome());
                    break;
                }

            case "erasure-identity":
                {
                    Assert.True(harness.ValuesEqualAcrossReads("alpha"));

                    FutureProbe erased = harness.Values("alpha");
                    FutureProbe typed = harness.NumPartitions("alpha");
                    Assert.False(erased.IsDone);
                    Assert.False(typed.IsDone);

                    harness.SetResult("alpha", Metadata(2, 1));

                    // The erasure is the key's own future, so it is done the moment the key is. (The typed projection
                    // is too on the sync flavour; on the async one it is a continuation, done a moment later.)
                    Assert.True(erased.IsDone);
                    AssertReturned(await erased.Outcome());
                    Assert.Equal((object)2, await typed.Outcome());
                    Assert.True(harness.ValuesEqualAcrossReads("alpha"));

                    // The erased and typed futures of a failed key fail with the same instance.
                    KafkaException beta = new KafkaException(38, "beta failed", isRetriable: false);
                    harness.SetException("beta", beta);
                    Assert.Same(beta, await harness.Values("beta").Outcome());
                    Assert.Same(beta, await harness.TopicId("beta").Outcome());
                    break;
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(row), row, "unknown row");
        }
    }

    // Omitted: memoization (the result has no projection — its only composite is All()).
    public static TheoryData<string, string> DeleteTopicsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "erasure-identity",
        "by-id");

    // Omitted: erasure identity (the result has no void view; its equal-on-every-read half is the key-identity row).
    public static TheoryData<string, string> DescribeTopicsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "memoization",
        "key-identity",
        "by-id");

    // Omitted: one-fails / two-fail / all-fail (one future, so they collapse into the fails row); unknown-key, null-key
    // and per-key independence (no keys); erasure identity (no void view; the identity row pins NamesToListings()).
    public static TheoryData<string, string> ListTopicsRows => Rows(
        "all-succeed",
        "fails",
        "memoization",
        "identity");

    // Omitted: memoization (no projection).
    public static TheoryData<string, string> CreatePartitionsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "erasure-identity");

    // Omitted: null-key (TopicPartition is a value type, so a null key cannot be expressed); memoization (no
    // projection); erasure identity (no per-key void view; All() is covered by the outcome rows, and the per-key
    // futures' equal-on-every-read half is the key-identity row).
    public static TheoryData<string, string> DeleteRecordsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "per-key-independence",
        "key-identity");

    [Theory]
    [MemberData(nameof(DeleteTopicsRows))]
    public async Task DeleteTopics_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        if (row == "by-id")
        {
            Uuid[] ids = { new Uuid(1L, 1L), new Uuid(1L, 2L), new Uuid(1L, 3L) };
            VoidHarness<Uuid> byId = VoidHarness<Uuid>.DeleteTopicsById(flavour, ids);
            Assert.True(byId.OtherSideIsNull, "a by-id result's TopicNameValues must be null");
            Assert.True(
                VoidHarness<string>.DeleteTopicsByName(flavour, s_topics).OtherSideIsNull,
                "a by-name result's TopicIdValues must be null");

            // The third id fails first in time; the aggregate reports the second, the first failure in key order.
            KafkaException second = new KafkaException(3, "id two failed", isRetriable: true);
            KafkaException third = new KafkaException(3, "id three failed", isRetriable: true);
            byId.Fail(ids[2], third);
            byId.Succeed(ids[0]);
            byId.Fail(ids[1], second);

            Assert.Equal(ids, byId.ValuesKeys());
            AssertReturned(await byId.Value(ids[0]).Outcome());
            Assert.Same(second, await byId.Value(ids[1]).Outcome());
            Assert.Same(third, await byId.Value(ids[2]).Outcome());
            Assert.Same(second, await byId.All().Outcome());
            return;
        }

        await RunKeyedRow(row, VoidHarness<string>.DeleteTopicsByName(flavour, s_topics), "delta");
    }

    [Theory]
    [MemberData(nameof(DescribeTopicsRows))]
    public async Task DescribeTopics_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        switch (row)
        {
            case "by-id":
                {
                    Uuid[] ids = { new Uuid(2L, 1L), new Uuid(2L, 2L), new Uuid(2L, 3L) };
                    ValuedHarness<Uuid, TopicDescription> byId = ValuedHarness<Uuid, TopicDescription>.DescribeTopicsById(flavour, ids);
                    Assert.True(byId.OtherSideIsNull, "a by-id result's TopicNameValues and AllTopicNames() must be null");
                    Assert.True(
                        ValuedHarness<string, TopicDescription>.DescribeTopicsByName(flavour, s_topics).OtherSideIsNull,
                        "a by-name result's TopicIdValues and AllTopicIds() must be null");

                    foreach (Uuid id in ids)
                    {
                        byId.Succeed(id);
                    }

                    Assert.Equal(ids, byId.ValuesKeys());
                    byId.AssertAllSucceeded(await byId.All().Outcome());
                    Assert.Same(byId.Expected(ids[1]), await byId.Value(ids[1]).Outcome());
                    break;
                }

            default:
                await RunKeyedRow(row, ValuedHarness<string, TopicDescription>.DescribeTopicsByName(flavour, s_topics), "delta");
                break;
        }
    }

    [Theory]
    [MemberData(nameof(ListTopicsRows))]
    public async Task ListTopics_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        ListTopicsHarness harness = ListTopicsHarness.For(flavour);
        Dictionary<string, TopicListing> listings = new Dictionary<string, TopicListing>(StringComparer.Ordinal)
        {
            ["alpha"] = new TopicListing("alpha", new Uuid(3L, 1L), isInternal: false),
            ["beta"] = new TopicListing("beta", new Uuid(3L, 2L), isInternal: true),
        };

        switch (row)
        {
            case "all-succeed":
                {
                    FutureProbe map = harness.NamesToListings();
                    FutureProbe values = harness.Listings();
                    FutureProbe names = harness.Names();
                    map.AssertPending();
                    values.AssertPending();
                    names.AssertPending();

                    harness.Impl.Complete(listings);

                    Assert.Same(listings, await map.Outcome());
                    IReadOnlyCollection<TopicListing> listed =
                        Assert.IsAssignableFrom<IReadOnlyCollection<TopicListing>>(await values.Outcome());
                    Assert.Equal(listings.Values, listed);
                    IReadOnlyCollection<string> named =
                        Assert.IsAssignableFrom<IReadOnlyCollection<string>>(await names.Outcome());
                    Assert.Equal(new[] { "alpha", "beta" }, named);
                    break;
                }

            case "fails":
                {
                    KafkaException failure = new KafkaException(7, "listTopics failed", isRetriable: true);
                    harness.Impl.CompleteWithError(failure);

                    Assert.Same(failure, await harness.NamesToListings().Outcome());
                    Assert.Same(failure, await harness.Listings().Outcome());
                    Assert.Same(failure, await harness.Names().Outcome());
                    break;
                }

            case "memoization":
                {
                    harness.Impl.Complete(listings);

                    // A projection is applied once per future, so every Get() of it hands out the same collection ...
                    FutureProbe values = harness.Listings();
                    FutureProbe names = harness.Names();
                    object? listed = await values.Outcome();
                    object? named = await names.Outcome();
                    Assert.Same(listed, await values.Outcome());
                    Assert.Same(named, await names.Outcome());

                    // ... and a new call is a new projection.
                    Assert.NotSame(listed, await harness.Listings().Outcome());
                    Assert.NotSame(named, await harness.Names().Outcome());
                    break;
                }

            case "identity":
                {
                    // NamesToListings() is the operation's own future: equal on every call, done the moment it is.
                    Assert.True(harness.NamesToListingsEqualAcrossCalls());
                    FutureProbe map = harness.NamesToListings();
                    Assert.False(map.IsDone);

                    harness.Impl.Complete(listings);

                    Assert.True(map.IsDone);
                    Assert.True(harness.NamesToListings().IsDone);
                    Assert.True(harness.NamesToListingsEqualAcrossCalls());
                    Assert.Same(listings, await map.Outcome());
                    break;
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(row), row, "unknown row");
        }
    }

    [Theory]
    [MemberData(nameof(CreatePartitionsRows))]
    public Task CreatePartitions_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(row, VoidHarness<string>.CreatePartitions(flavour, s_topics), "delta");

    [Theory]
    [MemberData(nameof(DeleteRecordsRows))]
    public Task DeleteRecords_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        TopicPartition[] partitions = { new TopicPartition("alpha", 0), new TopicPartition("alpha", 1), new TopicPartition("beta", 0) };
        return RunKeyedRow(
            row,
            ValuedHarness<TopicPartition, DeletedRecords>.DeleteRecords(flavour, partitions),
            new TopicPartition("gamma", 0));
    }

    // Omitted: one-fails / two-fail (the core fails the four together, so they collapse into the fails row);
    // unknown-key, null-key and per-key independence (no keys); erasure identity (no void view). The memoization row
    // carries the identity half: each accessor returns its own future, so it is equal on every call.
    public static TheoryData<string, string> DescribeClusterRows => Rows(
        "all-succeed",
        "all-succeed-absent",
        "fails",
        "memoization");

    // Omitted: one-fails / two-fail / all-fail (one future, so they collapse into the fails row); unknown-key, null-key
    // and per-key independence (no keys); memoization (no projection or aggregate); erasure identity (no void view).
    // "fresh-per-call" pins that each All() is a new future.
    public static TheoryData<string, string> ListConfigResourcesRows => Rows(
        "all-succeed",
        "fails",
        "fresh-per-call");

    // Omitted: erasure identity (no void view; its equal-on-every-read half is the key-identity row).
    public static TheoryData<string, string> DescribeConfigsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "memoization",
        "key-identity");

    // Omitted: memoization (no projection — its only composite is All()).
    public static TheoryData<string, string> AlterConfigsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "erasure-identity");

    // Omitted: null-key (a broker id is an int, so a null key cannot be expressed); erasure identity (no void view; its
    // equal-on-every-read half is the key-identity row).
    public static TheoryData<string, string> DescribeLogDirsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "per-key-independence",
        "memoization",
        "key-identity");

    // Omitted: memoization (no projection — its only composite is All()).
    public static TheoryData<string, string> AlterReplicaLogDirsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "erasure-identity");

    // Omitted: erasure identity (no void view; its equal-on-every-read half is the key-identity row).
    public static TheoryData<string, string> DescribeReplicaLogDirsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "memoization",
        "key-identity");

    [Theory]
    [MemberData(nameof(DescribeClusterRows))]
    public async Task DescribeCluster_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        DescribeClusterHarness harness = DescribeClusterHarness.For(flavour);
        Node broker = new Node(1, "broker-1", 9092, "rack-a");
        Node controller = new Node(2, "broker-2", 9093, null);
        IReadOnlyCollection<Node> nodes = new[] { broker, controller };
        IReadOnlyCollection<AclOperation> operations = new[] { AclOperation.Read, AclOperation.Write };

        switch (row)
        {
            case "all-succeed":
                {
                    FutureProbe[] probes = harness.Accessors();
                    foreach (FutureProbe probe in probes)
                    {
                        probe.AssertPending();
                    }

                    harness.SetResult(nodes, controller, "cluster-1", operations);

                    Assert.Same(nodes, await probes[0].Outcome());
                    Assert.Same(controller, await probes[1].Outcome());
                    Assert.Equal("cluster-1", await probes[2].Outcome());
                    Assert.Same(operations, await probes[3].Outcome());
                    break;
                }

            case "all-succeed-absent":
                {
                    // No controller and no authorized operations reported: each yields null rather than failing.
                    harness.SetResult(nodes, null, string.Empty, null);

                    FutureProbe[] probes = harness.Accessors();
                    Assert.Same(nodes, await probes[0].Outcome());
                    Assert.Null(await probes[1].Outcome());
                    Assert.Equal(string.Empty, await probes[2].Outcome());
                    Assert.Null(await probes[3].Outcome());
                    break;
                }

            case "fails":
                {
                    // Every future rethrows the failure's own exception instance, wrapped in nothing.
                    KafkaException failure = new KafkaException(7, "describeCluster failed", isRetriable: true);
                    harness.SetException(failure);

                    foreach (FutureProbe probe in harness.Accessors())
                    {
                        Assert.Same(failure, await probe.Outcome());
                    }

                    break;
                }

            case "memoization":
                {
                    // Each accessor returns its own future: equal on every call, before and after completion ...
                    Assert.True(harness.AccessorsEqualAcrossCalls());
                    FutureProbe[] first = harness.Accessors();

                    harness.SetResult(nodes, controller, "cluster-1", operations);

                    Assert.True(harness.AccessorsEqualAcrossCalls());
                    FutureProbe[] second = harness.Accessors();
                    for (int index = 0; index < first.Length; index++)
                    {
                        // ... so a value read through an earlier call is the very instance a later call yields.
                        object? value = await first[index].Outcome();
                        Assert.Same(value, await first[index].Outcome());
                        Assert.Same(value, await second[index].Outcome());
                    }

                    break;
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(row), row, "unknown row");
        }
    }

    [Theory]
    [MemberData(nameof(ListConfigResourcesRows))]
    public async Task ListConfigResources_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        ListConfigResourcesHarness harness = ListConfigResourcesHarness.For(flavour);
        IReadOnlyCollection<ConfigResource> resources = new[]
        {
            new ConfigResource(ConfigResourceType.Topic, "alpha"),
            new ConfigResource(ConfigResourceType.Broker, "1"),
        };

        switch (row)
        {
            case "all-succeed":
                {
                    FutureProbe all = harness.All();
                    all.AssertPending();

                    harness.Impl.Complete(resources);

                    Assert.Same(resources, await all.Outcome());
                    break;
                }

            case "fails":
                {
                    KafkaException failure = new KafkaException(7, "listConfigResources failed", isRetriable: true);
                    harness.Impl.CompleteWithError(failure);

                    Assert.Same(failure, await harness.All().Outcome());
                    break;
                }

            case "fresh-per-call":
                {
                    (bool fresh, FutureProbe first, FutureProbe second) = harness.TwoAlls();
                    Assert.True(fresh, "two All() calls returned an equal future");
                    first.AssertPending();
                    second.AssertPending();

                    harness.Impl.Complete(resources);

                    Assert.Same(resources, await first.Outcome());
                    Assert.Same(resources, await second.Outcome());
                    break;
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(row), row, "unknown row");
        }
    }

    [Theory]
    [MemberData(nameof(DescribeConfigsRows))]
    public Task DescribeConfigs_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(
            row,
            ValuedHarness<ConfigResource, Config>.DescribeConfigs(flavour, Resources()),
            new ConfigResource(ConfigResourceType.Topic, "delta"));

    [Theory]
    [MemberData(nameof(AlterConfigsRows))]
    public Task AlterConfigs_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(
            row,
            VoidHarness<ConfigResource>.IncrementalAlterConfigs(flavour, Resources()),
            new ConfigResource(ConfigResourceType.Topic, "delta"));

    [Theory]
    [MemberData(nameof(DescribeLogDirsRows))]
    public Task DescribeLogDirs_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(
            row,
            ValuedHarness<int, IReadOnlyDictionary<string, LogDirDescription>>.DescribeLogDirs(flavour, new[] { 1, 2, 3 }),
            9);

    [Theory]
    [MemberData(nameof(AlterReplicaLogDirsRows))]
    public Task AlterReplicaLogDirs_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(
            row,
            VoidHarness<TopicPartitionReplica>.AlterReplicaLogDirs(flavour, Replicas()),
            new TopicPartitionReplica("gamma", 0, 1));

    [Theory]
    [MemberData(nameof(DescribeReplicaLogDirsRows))]
    public Task DescribeReplicaLogDirs_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(
            row,
            ValuedHarness<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo>.DescribeReplicaLogDirs(
                flavour, Replicas()),
            new TopicPartitionReplica("gamma", 0, 1));

    // Omitted: unknown-key, null-key and per-key independence (one future and no per-key member); erasure identity (no
    // void view; the identity row pins Partitions()).
    public static TheoryData<string, string> ElectLeadersRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "fails",
        "memoization",
        "identity");

    // Omitted: null-key (TopicPartition is a value type, so a null key cannot be expressed); memoization (no projection
    // — its only composite is All()); two-fail and all-fail (All() reports the first failure to complete, not the
    // first in key order).
    public static TheoryData<string, string> AlterPartitionReassignmentsRows => Rows(
        "all-succeed",
        "one-fails",
        "unknown-key",
        "per-key-independence",
        "erasure-identity");

    // Omitted: one-fails / two-fail / all-fail (one future, and the ABI declares no per-partition error, so they
    // collapse into the fails row); unknown-key, null-key and per-key independence (no keys); memoization (no
    // projection or aggregate); erasure identity (no void view; the identity row pins Reassignments()).
    public static TheoryData<string, string> ListPartitionReassignmentsRows => Rows(
        "all-succeed",
        "fails",
        "identity");

    // Omitted: null-key (TopicPartition is a value type, so a null key cannot be expressed); erasure identity (no void
    // view; its equal-on-every-read half is the key-identity row).
    public static TheoryData<string, string> ListOffsetsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "per-key-independence",
        "memoization",
        "key-identity");

    [Theory]
    [MemberData(nameof(ElectLeadersRows))]
    public async Task ElectLeaders_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        ElectLeadersHarness harness = ElectLeadersHarness.For(flavour);
        TopicPartition[] partitions = TopicPartitions();

        switch (row)
        {
            case "all-succeed":
                {
                    FutureProbe map = harness.Partitions();
                    FutureProbe all = harness.All();
                    map.AssertPending();
                    all.AssertPending();

                    Dictionary<TopicPartition, KafkaException?> outcomes = Outcomes(partitions, null, null, null);
                    harness.Impl.Complete(outcomes);

                    Assert.Same(outcomes, await map.Outcome());
                    AssertReturned(await all.Outcome());
                    break;
                }

            case "one-fails":
                {
                    // A partition's failure is a VALUE: the map's own future succeeds, and only All() walks it into one.
                    KafkaException failure = new KafkaException(84, "the second election failed", isRetriable: false);
                    Dictionary<TopicPartition, KafkaException?> outcomes = Outcomes(partitions, null, failure, null);
                    harness.Impl.Complete(outcomes);

                    Assert.Same(outcomes, await harness.Partitions().Outcome());
                    Assert.Same(failure, await harness.All().Outcome());
                    break;
                }

            case "two-fail-first-in-key-order":
                {
                    // The walk reports the first failure in the map's order (the ABI's sorted order), not the last.
                    KafkaException first = new KafkaException(84, "the second election failed", isRetriable: false);
                    KafkaException last = new KafkaException(85, "the third election failed", isRetriable: true);
                    Dictionary<TopicPartition, KafkaException?> outcomes = Outcomes(partitions, null, first, last);
                    harness.Impl.Complete(outcomes);

                    Assert.Same(first, await harness.All().Outcome());
                    Assert.Same(outcomes, await harness.Partitions().Outcome());
                    break;
                }

            case "all-fail":
                {
                    KafkaException[] failures =
                    {
                        new KafkaException(84, "the first election failed", isRetriable: false),
                        new KafkaException(85, "the second election failed", isRetriable: true),
                        new KafkaException(86, "the third election failed", isRetriable: false),
                    };
                    Dictionary<TopicPartition, KafkaException?> outcomes = Outcomes(partitions, failures);
                    harness.Impl.Complete(outcomes);

                    Assert.Same(failures[0], await harness.All().Outcome());
                    IReadOnlyDictionary<TopicPartition, KafkaException?> map =
                        Assert.IsAssignableFrom<IReadOnlyDictionary<TopicPartition, KafkaException?>>(
                            await harness.Partitions().Outcome());
                    for (int index = 0; index < partitions.Length; index++)
                    {
                        Assert.Same(failures[index], map[partitions[index]]);
                    }

                    break;
                }

            case "fails":
                {
                    // The call-level failure fails the map's future, and All() forwards it (Java's `throwable != null`).
                    KafkaException failure = new KafkaException(7, "electLeaders failed", isRetriable: true);
                    harness.Impl.CompleteWithError(failure);

                    Assert.Same(failure, await harness.Partitions().Outcome());
                    Assert.Same(failure, await harness.All().Outcome());
                    break;
                }

            case "memoization":
                {
                    KafkaException failure = new KafkaException(84, "the second election failed", isRetriable: false);
                    harness.Impl.Complete(Outcomes(partitions, null, failure, null));

                    // One All() future walks once, so every Get() of it rethrows the same instance ...
                    FutureProbe all = harness.All();
                    Assert.Same(failure, await all.Outcome());
                    Assert.Same(failure, await all.Outcome());

                    // ... and a new call is a new future, as Java allocates a fresh KafkaFutureImpl per all() call.
                    (bool fresh, FutureProbe first, FutureProbe second) = harness.TwoAlls();
                    Assert.True(fresh, "two All() calls returned an equal future");
                    Assert.Same(failure, await first.Outcome());
                    Assert.Same(failure, await second.Outcome());
                    break;
                }

            case "identity":
                {
                    // Partitions() is the operation's own future: equal on every call, done the moment it is.
                    Assert.True(harness.PartitionsEqualAcrossCalls());
                    FutureProbe map = harness.Partitions();
                    Assert.False(map.IsDone);

                    Dictionary<TopicPartition, KafkaException?> outcomes = Outcomes(partitions, null, null, null);
                    harness.Impl.Complete(outcomes);

                    Assert.True(map.IsDone);
                    Assert.True(harness.Partitions().IsDone);
                    Assert.True(harness.PartitionsEqualAcrossCalls());
                    Assert.Same(outcomes, await map.Outcome());
                    break;
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(row), row, "unknown row");
        }
    }

    [Theory]
    [MemberData(nameof(AlterPartitionReassignmentsRows))]
    public Task AlterPartitionReassignments_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(
            row,
            VoidHarness<TopicPartition>.AlterPartitionReassignments(flavour, TopicPartitions()),
            new TopicPartition("gamma", 0));

    [Theory]
    [MemberData(nameof(ListPartitionReassignmentsRows))]
    public async Task ListPartitionReassignments_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        ListPartitionReassignmentsHarness harness = ListPartitionReassignmentsHarness.For(flavour);
        Dictionary<TopicPartition, PartitionReassignment> reassignments = new Dictionary<TopicPartition, PartitionReassignment>
        {
            [new TopicPartition("alpha", 0)] = new PartitionReassignment(new[] { 1, 2, 3 }, new[] { 3 }, Array.Empty<int>()),
            [new TopicPartition("beta", 0)] = new PartitionReassignment(new[] { 2, 3 }, Array.Empty<int>(), new[] { 1 }),
        };

        switch (row)
        {
            case "all-succeed":
                {
                    FutureProbe map = harness.Reassignments();
                    map.AssertPending();

                    harness.Impl.Complete(reassignments);

                    Assert.Same(reassignments, await map.Outcome());
                    break;
                }

            case "fails":
                {
                    KafkaException failure = new KafkaException(7, "listPartitionReassignments failed", isRetriable: true);
                    harness.Impl.CompleteWithError(failure);

                    Assert.Same(failure, await harness.Reassignments().Outcome());
                    break;
                }

            case "identity":
                {
                    // Reassignments() is the operation's own future: equal on every call, done the moment it is.
                    Assert.True(harness.ReassignmentsEqualAcrossCalls());
                    FutureProbe map = harness.Reassignments();
                    Assert.False(map.IsDone);

                    harness.Impl.Complete(reassignments);

                    Assert.True(map.IsDone);
                    Assert.True(harness.Reassignments().IsDone);
                    Assert.True(harness.ReassignmentsEqualAcrossCalls());
                    Assert.Same(reassignments, await map.Outcome());
                    break;
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(row), row, "unknown row");
        }
    }

    [Theory]
    [MemberData(nameof(ListOffsetsRows))]
    public async Task ListOffsets_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        TopicPartition[] partitions = TopicPartitions();
        ListOffsetsHarness harness = ListOffsetsHarness.For(flavour, partitions);
        TopicPartition unknown = new TopicPartition("gamma", 0);
        if (row != "unknown-key")
        {
            await RunKeyedRow(row, harness, unknown);
            return;
        }

        // PartitionResult is the only per-key member, and its guard is Java's:
        // synchronous, at the call, an ArgumentException with the exact message — not a dictionary's KeyNotFoundException.
        string expected = new ArgumentException("List Offsets for partition \"gamma-0\" was not attempted", "partition").Message;
        ArgumentException failure = Assert.IsType<ArgumentException>(harness.CallIndexer(unknown));
        Assert.Equal("partition", failure.ParamName);
        Assert.Equal(expected, failure.Message);
        Assert.False(harness.ContainsKey(unknown));
        Assert.True(harness.ContainsKey(partitions[0]));
    }

    public static TheoryData<string, string> ListGroupsRows => Rows(
        "all-succeed",
        "fails",
        "views-are-independent",
        "same-future-per-call");

    // Omitted: erasure identity (no void view; its equal-on-every-read half is the key-identity row).
    public static TheoryData<string, string> DescribeConsumerGroupsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "memoization",
        "key-identity");

    public static TheoryData<string, string> DescribeClassicGroupsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "memoization",
        "key-identity");

    // Omitted: erasure identity (no void view; its equal-on-every-read half is the key-identity row).
    public static TheoryData<string, string> ListConsumerGroupOffsetsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "memoization",
        "key-identity",
        "no-arg-guard");

    // Omitted: memoization (no projection — its only composite is All()).
    public static TheoryData<string, string> DeleteConsumerGroupsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "erasure-identity");

    [Theory]
    [MemberData(nameof(ListGroupsRows))]
    public async Task ListGroups_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        ListGroupsHarness harness = ListGroupsHarness.For(flavour);
        IReadOnlyCollection<GroupListing> valid = new[] { Listing("group"), Listing("Group") };
        IReadOnlyCollection<KafkaException> none = Array.Empty<KafkaException>();

        switch (row)
        {
            case "all-succeed":
                {
                    FutureProbe all = harness.All();
                    FutureProbe listed = harness.Valid();
                    FutureProbe errors = harness.Errors();
                    all.AssertPending();
                    listed.AssertPending();
                    errors.AssertPending();

                    harness.AllFuture.Complete(valid);
                    harness.ValidFuture.Complete(valid);
                    harness.ErrorsFuture.Complete(none);

                    Assert.Same(valid, await all.Outcome());
                    Assert.Same(valid, await listed.Outcome());
                    Assert.Same(none, await errors.Outcome());
                    break;
                }

            case "fails":
                {
                    KafkaException failure = new KafkaException(7, "listGroups failed", isRetriable: true);
                    harness.AllFuture.CompleteWithError(failure);
                    harness.ValidFuture.CompleteWithError(failure);
                    harness.ErrorsFuture.CompleteWithError(failure);

                    Assert.Same(failure, await harness.All().Outcome());
                    Assert.Same(failure, await harness.Valid().Outcome());
                    Assert.Same(failure, await harness.Errors().Outcome());
                    break;
                }

            case "views-are-independent":
                {
                    // A broker's error is a value of Errors(): Valid() and Errors() succeed while All() fails or waits, as
                    // its own future says.
                    KafkaException failure = new KafkaException(15, "the second coordinator is not available", isRetriable: true);
                    IReadOnlyCollection<KafkaException> errors = new[] { failure };
                    harness.ValidFuture.Complete(valid);
                    harness.ErrorsFuture.Complete(errors);

                    FutureProbe all = harness.All();
                    all.AssertPending();
                    Assert.Same(valid, await harness.Valid().Outcome());
                    Assert.Same(errors, await harness.Errors().Outcome());

                    harness.AllFuture.CompleteWithError(failure);
                    Assert.Same(failure, await all.Outcome());
                    break;
                }

            case "same-future-per-call":
                {
                    harness.AllFuture.Complete(valid);
                    harness.ValidFuture.Complete(valid);
                    harness.ErrorsFuture.Complete(none);

                    Assert.True(harness.SameFuturePerCall(), "an accessor handed out a different future on its second call");
                    Assert.Same(valid, await harness.All().Outcome());
                    Assert.Same(valid, await harness.All().Outcome());
                    break;
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(row), row, "unknown row");
        }
    }

    [Theory]
    [MemberData(nameof(DescribeConsumerGroupsRows))]
    public Task DescribeConsumerGroups_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(row, ValuedHarness<string, ConsumerGroupDescription>.DescribeConsumerGroups(flavour, s_groups), UnknownGroup);

    [Theory]
    [MemberData(nameof(DescribeClassicGroupsRows))]
    public Task DescribeClassicGroups_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(row, ValuedHarness<string, ClassicGroupDescription>.DescribeClassicGroups(flavour, s_groups), UnknownGroup);

    [Theory]
    [MemberData(nameof(ListConsumerGroupOffsetsRows))]
    public async Task ListConsumerGroupOffsets_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        ListConsumerGroupOffsetsHarness harness = ListConsumerGroupOffsetsHarness.For(flavour, s_groups);
        switch (row)
        {
            case "unknown-key":
                {
                    // Java's guard: at the call, before completion and after it,
                    // an ArgumentException naming the parameter — not a dictionary's KeyNotFoundException.
                    string expected = new ArgumentException(
                        "Offsets for consumer group '" + UnknownGroup + "' were not requested.", "groupId").Message;
                    AssertArgument(harness.CallIndexer(UnknownGroup), "groupId", expected);
                    Assert.False(harness.ContainsKey(UnknownGroup));
                    Assert.True(harness.ContainsKey(s_groups[0]));

                    SucceedEvery(harness);
                    AssertArgument(harness.CallIndexer(UnknownGroup), "groupId", expected);
                    break;
                }

            case "null-key":
                {
                    AssertNullArgument(harness.CallIndexer(null!), "groupId");
                    SucceedEvery(harness);
                    AssertNullArgument(harness.CallIndexer(null!), "groupId");
                    break;
                }

            case "no-arg-guard":
                {
                    // Java's guard is `futures.size() != 1`: at the call, before completion and after it, for three
                    // groups ...
                    const string Expected =
                        "Offsets from multiple consumer groups were requested. "
                        + "Use partitionsToOffsetAndMetadata(groupId) instead to get future for a specific group.";
                    Assert.Equal(Expected, Assert.IsType<InvalidOperationException>(harness.CallNoArgument()).Message);
                    SucceedEvery(harness);
                    Assert.Equal(Expected, Assert.IsType<InvalidOperationException>(harness.CallNoArgument()).Message);

                    // ... and for none.
                    ListConsumerGroupOffsetsHarness empty = ListConsumerGroupOffsetsHarness.For(flavour, Array.Empty<string>());
                    Assert.Equal(Expected, Assert.IsType<InvalidOperationException>(empty.CallNoArgument()).Message);

                    // With exactly one group it is that group's own future: equal to the by-id one, done when it is.
                    ListConsumerGroupOffsetsHarness single = ListConsumerGroupOffsetsHarness.For(flavour, new[] { s_groups[0] });
                    Assert.True(single.NoArgumentIsTheGroupsOwn(s_groups[0]));
                    FutureProbe only = single.NoArgument();
                    Assert.False(only.IsDone);
                    single.Succeed(s_groups[0]);
                    Assert.True(only.IsDone);
                    Assert.Same(single.Expected(s_groups[0]), await only.Outcome());
                    break;
                }

            default:
                await RunKeyedRow(row, harness, UnknownGroup);
                break;
        }
    }

    [Theory]
    [MemberData(nameof(DeleteConsumerGroupsRows))]
    public Task DeleteConsumerGroups_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(row, VoidHarness<string>.DeleteConsumerGroups(flavour, s_groups), UnknownGroup);

    // Omitted: memoization (no projection — its only composite is All()); two-fail and all-fail (All() reports the
    // first failure to complete, not the first in key order).
    public static TheoryData<string, string> CreateAclsRows => Rows(
        "all-succeed",
        "one-fails",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "erasure-identity");

    // Omitted: erasure identity (no void view; its equal-on-every-read half is the key-identity row); two-fail and
    // all-fail (All() reports the first filter failure to complete, not the first in filter order).
    public static TheoryData<string, string> DeleteAclsRows => Rows(
        "all-succeed",
        "one-fails",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "memoization",
        "key-identity",
        "inner-failure-at-get",
        "outer-failure-before-inner",
        "empty-filter-results");

    // Omitted: one-fails / two-fail / all-fail (one future, so they collapse into the fails row); unknown-key, null-key
    // and per-key independence (no keys); memoization (no projection or aggregate: Values() is the operation's own
    // future); erasure identity (no void view; the identity row pins Values()).
    public static TheoryData<string, string> DescribeAclsRows => Rows(
        "all-succeed",
        "fails",
        "identity");

    public static TheoryData<string, string> DescribeClientQuotasRows => Rows(
        "all-succeed",
        "fails",
        "identity",
        "constructor-default-guard");

    // Omitted: memoization (no projection — its only composite is All()); two-fail and all-fail (All() reports the
    // first failure to complete, not the first in key order).
    public static TheoryData<string, string> AlterClientQuotasRows => Rows(
        "all-succeed",
        "one-fails",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "erasure-identity",
        "constructor-null-guard",
        "constructor-by-reference");

    [Theory]
    [MemberData(nameof(CreateAclsRows))]
    public Task CreateAcls_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(
            row,
            VoidHarness<AclBinding>.CreateAcls(flavour, Bindings(), Bindings()),
            Binding(ResourceType.Topic, "beta", "User:alice"));

    [Theory]
    [MemberData(nameof(DeleteAclsRows))]
    public async Task DeleteAcls_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        AclBindingFilter[] filters = Filters();
        DeleteAclsHarness harness = DeleteAclsHarness.For(flavour, Filters(), filters);
        switch (row)
        {
            case "inner-failure-at-get":
                {
                    // Java's guard is inside all()'s thenApply: the call succeeds, before
                    // completion and after it, each filter's own future succeeds with its results, and Get() throws the
                    // first FilterResult exception in filter order. The filters complete in the reverse order.
                    KafkaException first = new KafkaException(31, "the first filter's second delete failed", isRetriable: false);
                    KafkaException second = new KafkaException(32, "the second filter's delete failed", isRetriable: true);
                    DeleteAclsResult.FilterResults firstResults = Matched(
                        Deleted(Binding(ResourceType.Topic, "alpha", "User:alice")),
                        Failed(Binding(ResourceType.Topic, "alpha", "User:bob"), first));
                    DeleteAclsResult.FilterResults secondResults = Matched(
                        Failed(Binding(ResourceType.Topic, "beta", "User:alice"), second));
                    FutureProbe? before = null;
                    Assert.Null(Thrown(() => before = harness.All()));
                    before!.AssertPending();

                    harness.Succeed(filters[2]);
                    harness.Succeed(filters[1], secondResults);
                    harness.Succeed(filters[0], firstResults);

                    Assert.Same(firstResults, await harness.Value(filters[0]).Outcome());
                    Assert.Same(secondResults, await harness.Value(filters[1]).Outcome());
                    FutureProbe? after = null;
                    Assert.Null(Thrown(() => after = harness.All()));

                    // One future walks once, so every Get() of it rethrows the same instance.
                    Assert.Same(first, await before.Outcome());
                    Assert.Same(first, await before.Outcome());
                    Assert.Same(first, await after!.Outcome());
                    break;
                }

            case "outer-failure-before-inner":
                {
                    // The first filter carries a FilterResult exception, the second fails outright: allOf fails first
                    // so the projection never runs and the outer failure wins although it is later in filter order.
                    KafkaException inner = new KafkaException(31, "the first filter's delete failed", isRetriable: false);
                    KafkaException outer = new KafkaException(7, "the second filter failed", isRetriable: true);
                    harness.Succeed(filters[0], Matched(Failed(Binding(ResourceType.Topic, "alpha", "User:alice"), inner)));
                    harness.Fail(filters[1], outer);
                    harness.Succeed(filters[2]);

                    Assert.Same(outer, await harness.All().Outcome());
                    break;
                }

            case "empty-filter-results":
                {
                    // Filters matching nothing are not an error: All() succeeds, with what the others matched ...
                    harness.Succeed(filters[0], Matched());
                    harness.Succeed(filters[1]);
                    harness.Succeed(filters[2], Matched());
                    AclBinding[] matched = harness.Bindings(filters[1]);
                    IReadOnlyCollection<AclBinding> some =
                        Assert.IsAssignableFrom<IReadOnlyCollection<AclBinding>>(await harness.All().Outcome());
                    Assert.Equal(matched.Length, some.Count);
                    Assert.Same(matched[0], some.ElementAt(0));
                    Assert.Same(matched[1], some.ElementAt(1));

                    // ... and with an empty collection when none matched anything.
                    DeleteAclsHarness none = DeleteAclsHarness.For(flavour, Filters(), filters);
                    foreach (AclBindingFilter filter in filters)
                    {
                        none.Succeed(filter, Matched());
                    }

                    Assert.Empty(Assert.IsAssignableFrom<IReadOnlyCollection<AclBinding>>(await none.All().Outcome()));
                    break;
                }

            default:
                await RunKeyedRow(row, harness, Filter("delta"));
                break;
        }
    }

    [Theory]
    [MemberData(nameof(DescribeAclsRows))]
    public Task DescribeAcls_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        IReadOnlyCollection<AclBinding> bindings = Bindings();
        return RunSingleRow(row, SingleHarness<IReadOnlyCollection<AclBinding>>.DescribeAcls(flavour), bindings);
    }

    [Theory]
    [MemberData(nameof(DescribeClientQuotasRows))]
    public Task DescribeClientQuotas_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        if (row == "constructor-default-guard")
        {
            // A default future, bound to no source, is this constructor's null: rejected at construction, before any
            // accessor, with the same type, parameter name and message on both flavours.
            AssertNullArgument(
                FlavourOf(flavour) == AdminFlavour.Sync
                    ? Thrown(() => _ = new DescribeClientQuotasResult(default))
                    : Thrown(() => _ = new AsyncDescribeClientQuotasResult(default)),
                "entities");
            return Task.CompletedTask;
        }

        IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>> quotas =
            new Dictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>
            {
                [Entity(ClientQuotaEntity.User, "alice")] =
                    new Dictionary<string, double>(StringComparer.Ordinal) { ["producer_byte_rate"] = 1024d },
            };
        return RunSingleRow(
            row,
            SingleHarness<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>.DescribeClientQuotas(flavour),
            quotas);
    }

    [Theory]
    [MemberData(nameof(AlterClientQuotasRows))]
    public async Task AlterClientQuotas_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        switch (row)
        {
            case "constructor-null-guard":
                {
                    // Rejected at construction, where Java stores the null and fails on first use: the same type,
                    // parameter name and message on both flavours.
                    AssertNullArgument(
                        FlavourOf(flavour) == AdminFlavour.Sync
                            ? Thrown(() => _ = new AlterClientQuotasResult(null!))
                            : Thrown(() => _ = new AsyncAlterClientQuotasResult(null!)),
                        "futures");
                    break;
                }

            case "constructor-by-reference":
                await AlterClientQuotasByReference(flavour);
                break;

            default:
                await RunKeyedRow(
                    row,
                    VoidHarness<ClientQuotaEntity>.AlterClientQuotas(flavour, Entities(), Entities()),
                    Entity(ClientQuotaEntity.User, "bob"));
                break;
        }
    }

    private static async Task AlterClientQuotasByReference(string flavour)
    {
        ClientQuotaEntity[] entities = Entities();
        AdminFlavour adminFlavour = FlavourOf(flavour);
        Dictionary<ClientQuotaEntity, KafkaFutureImpl<object?>> impls =
            Impls<ClientQuotaEntity, object?>(entities, EqualityComparer<ClientQuotaEntity>.Default);
        KafkaException failure = new KafkaException(38, "the second entity failed", isRetriable: false);
        if (adminFlavour == AdminFlavour.Sync)
        {
            Dictionary<ClientQuotaEntity, KafkaFuture> map =
                new Dictionary<ClientQuotaEntity, KafkaFuture> { [entities[0]] = new KafkaFuture(impls[entities[0]]) };
            AlterClientQuotasResult result = new AlterClientQuotasResult(map);
            Assert.Same(map, result.Values);

            impls[entities[0]].Complete(null);
            AssertReturned(await FutureProbe.Of(result.All()).Outcome());

            map.Add(entities[1], new KafkaFuture(impls[entities[1]]));
            impls[entities[1]].CompleteWithError(failure);
            Assert.Same(map, result.Values);
            Assert.Same(failure, await FutureProbe.Of(result.All()).Outcome());
            return;
        }

        Dictionary<ClientQuotaEntity, AsyncKafkaFuture> asyncMap =
            new Dictionary<ClientQuotaEntity, AsyncKafkaFuture> { [entities[0]] = new AsyncKafkaFuture(impls[entities[0]]) };
        AsyncAlterClientQuotasResult asyncResult = new AsyncAlterClientQuotasResult(asyncMap);
        Assert.Same(asyncMap, asyncResult.Values);

        impls[entities[0]].Complete(null);
        AssertReturned(await FutureProbe.Of(asyncResult.All()).Outcome());

        asyncMap.Add(entities[1], new AsyncKafkaFuture(impls[entities[1]]));
        impls[entities[1]].CompleteWithError(failure);
        Assert.Same(asyncMap, asyncResult.Values);
        Assert.Same(failure, await FutureProbe.Of(asyncResult.All()).Outcome());
    }

    // Omitted: every row that needs a pending or a populated Result (no result root can be built in a unit test: the
    // ABI exports no constructor for one and the mock fails this RPC); per-key rows (no per-key futures); memoization
    // and identity (each accessor call is a new future).
    public static TheoryData<string, string> DescribeUserScramCredentialsRows => Rows(
        "fails",
        "description-null-user",
        "description-invalid-user");

    // Omitted: memoization (no projection — its only composite is All()); two-fail and all-fail (All() reports the
    // first failure to complete, not the first in key order).
    public static TheoryData<string, string> AlterUserScramCredentialsRows => Rows(
        "all-succeed",
        "one-fails",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "erasure-identity",
        "constructor-null-guard",
        "constructor-by-reference");

    // Omitted: one-fails / two-fail / all-fail (one future, so they collapse into the fails row); unknown-key, null-key
    // and per-key independence (no keys); memoization (no projection or aggregate: DelegationToken() is the operation's
    // own future); erasure identity (no void view; the identity row pins DelegationToken()).
    public static TheoryData<string, string> CreateDelegationTokenRows => Rows(
        "all-succeed",
        "fails",
        "identity");

    public static TheoryData<string, string> RenewDelegationTokenRows => Rows(
        "all-succeed",
        "fails",
        "identity");

    public static TheoryData<string, string> ExpireDelegationTokenRows => Rows(
        "all-succeed",
        "fails",
        "identity");

    public static TheoryData<string, string> DescribeDelegationTokenRows => Rows(
        "all-succeed",
        "fails",
        "identity");

    public static TheoryData<string, string> DescribeFeaturesRows => Rows(
        "all-succeed",
        "fails",
        "identity");

    // Omitted: memoization (no projection — its only composite is All()); the constructor rows (its constructor is
    // Java's package-private one, with no guard on either flavour).
    public static TheoryData<string, string> UpdateFeaturesRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "erasure-identity");

    [Theory]
    [MemberData(nameof(DescribeUserScramCredentialsRows))]
    public async Task DescribeUserScramCredentials_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        using DescribeUserScramCredentialsHarness harness =
            DescribeUserScramCredentialsHarness.Create(flavour, new[] { "alice", "bob" });
        switch (row)
        {
            case "fails":
                {
                    AssertNotImplemented(await harness.All().Outcome());
                    AssertNotImplemented(await harness.Users().Outcome());
                    AssertNotImplemented(await harness.Description("alice").Outcome());
                    AssertNotImplemented(await harness.Description("carol").Outcome());
                    AssertNotImplemented(await harness.All().Outcome());
                    break;
                }

            case "description-null-user":
                AssertNullArgument(Thrown(() => harness.Description(null!)), "userName");
                break;

            case "description-invalid-user":
                {
                    string expected = new ArgumentException(AdminStrings.InvalidStringMessage, "userName").Message;
                    AssertArgument(Thrown(() => harness.Description("al\0ice")), "userName", expected);
                    break;
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(row), row, "unknown row");
        }
    }

    private static void AssertNotImplemented(object? outcome)
    {
        NotSupportedException failure = Assert.IsType<NotSupportedException>(outcome);
        Assert.Equal("Not implemented yet", failure.Message);
    }

    [Theory]
    [MemberData(nameof(AlterUserScramCredentialsRows))]
    public async Task AlterUserScramCredentials_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        switch (row)
        {
            case "constructor-null-guard":
                {
                    // Rejected at construction, as Java's Objects.requireNonNull rejects it: the same type,
                    // parameter name and message on both flavours.
                    AssertNullArgument(
                        FlavourOf(flavour) == AdminFlavour.Sync
                            ? Thrown(() => _ = new AlterUserScramCredentialsResult(null!))
                            : Thrown(() => _ = new AsyncAlterUserScramCredentialsResult(null!)),
                        "futures");
                    break;
                }

            case "constructor-by-reference":
                await AlterUserScramCredentialsByReference(flavour);
                break;

            default:
                await RunKeyedRow(row, VoidHarness<string>.AlterUserScramCredentials(flavour, s_users), UnknownUser);
                break;
        }
    }

    private static async Task AlterUserScramCredentialsByReference(string flavour)
    {
        AdminFlavour adminFlavour = FlavourOf(flavour);
        Dictionary<string, KafkaFutureImpl<object?>> impls = Impls<string, object?>(s_users, StringComparer.Ordinal);
        KafkaException failure = new KafkaException(58, "the second user failed", isRetriable: false);
        if (adminFlavour == AdminFlavour.Sync)
        {
            Dictionary<string, KafkaFuture> map =
                new Dictionary<string, KafkaFuture>(StringComparer.Ordinal) { [s_users[0]] = new KafkaFuture(impls[s_users[0]]) };
            AlterUserScramCredentialsResult result = new AlterUserScramCredentialsResult(map);
            Assert.Same(map, result.Values);

            impls[s_users[0]].Complete(null);
            AssertReturned(await FutureProbe.Of(result.All()).Outcome());

            map.Add(s_users[1], new KafkaFuture(impls[s_users[1]]));
            impls[s_users[1]].CompleteWithError(failure);
            Assert.Equal(2, result.Values.Count);
            Assert.Same(failure, await FutureProbe.Of(result.All()).Outcome());
            return;
        }

        Dictionary<string, AsyncKafkaFuture> asyncMap = new Dictionary<string, AsyncKafkaFuture>(StringComparer.Ordinal)
        {
            [s_users[0]] = new AsyncKafkaFuture(impls[s_users[0]]),
        };
        AsyncAlterUserScramCredentialsResult asyncResult = new AsyncAlterUserScramCredentialsResult(asyncMap);
        Assert.Same(asyncMap, asyncResult.Values);

        impls[s_users[0]].Complete(null);
        AssertReturned(await FutureProbe.Of(asyncResult.All()).Outcome());

        asyncMap.Add(s_users[1], new AsyncKafkaFuture(impls[s_users[1]]));
        impls[s_users[1]].CompleteWithError(failure);
        Assert.Equal(2, asyncResult.Values.Count);
        Assert.Same(failure, await FutureProbe.Of(asyncResult.All()).Outcome());
    }

    [Theory]
    [MemberData(nameof(CreateDelegationTokenRows))]
    public Task CreateDelegationToken_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunSingleRow(row, SingleHarness<DelegationToken>.CreateDelegationToken(flavour), Token("token-1"));

    [Theory]
    [MemberData(nameof(RenewDelegationTokenRows))]
    public Task RenewDelegationToken_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunSingleRow(row, SingleHarness<long>.RenewDelegationToken(flavour), 1_700_000_000_123L);

    [Theory]
    [MemberData(nameof(ExpireDelegationTokenRows))]
    public Task ExpireDelegationToken_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunSingleRow(row, SingleHarness<long>.ExpireDelegationToken(flavour), 1_700_000_000_456L);

    [Theory]
    [MemberData(nameof(DescribeDelegationTokenRows))]
    public async Task DescribeDelegationToken_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        SingleHarness<IReadOnlyList<DelegationToken>> harness =
            SingleHarness<IReadOnlyList<DelegationToken>>.DescribeDelegationToken(flavour);
        List<DelegationToken> tokens = new List<DelegationToken> { Token("token-1"), Token("token-2") };
        switch (row)
        {
            case "identity":
                {
                    // Every call returns an equal future, before and after it completed.
                    Assert.True(harness.EqualAcrossCalls());
                    FutureProbe future = harness.Future();
                    future.AssertPending();

                    harness.SetResult(tokens);

                    Assert.Same(tokens, await future.Outcome());
                    Assert.True(harness.EqualAcrossCalls());
                    break;
                }

            default:
                await RunSingleRow(row, harness, tokens);
                break;
        }
    }

    [Theory]
    [MemberData(nameof(DescribeFeaturesRows))]
    public Task DescribeFeatures_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        FeatureMetadata metadata = new FeatureMetadata(
            new Dictionary<string, FinalizedVersionRange>(StringComparer.Ordinal)
            {
                ["metadata.version"] = new FinalizedVersionRange(1, 20),
            },
            7L,
            new Dictionary<string, SupportedVersionRange>(StringComparer.Ordinal)
            {
                ["metadata.version"] = new SupportedVersionRange(1, 21),
            });
        return RunSingleRow(row, SingleHarness<FeatureMetadata>.DescribeFeatures(flavour), metadata);
    }

    [Theory]
    [MemberData(nameof(UpdateFeaturesRows))]
    public Task UpdateFeatures_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(row, VoidHarness<string>.UpdateFeatures(flavour, s_features), "kraft.version");

    // Omitted: the gathering-aggregate memoization row (All() is a void all-of and gathers nothing).
    public static TheoryData<string, string> FenceProducersRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "fresh-per-access",
        "projections",
        "memoization");

    // Omitted: erasure identity (no void view; its equal-on-every-read half is the key-identity row).
    public static TheoryData<string, string> DescribeTransactionsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "memoization",
        "key-identity");

    // Omitted: null-key (TopicPartition is a value type); erasure identity (no void view; its equal-on-every-read half
    // is the key-identity row).
    public static TheoryData<string, string> DescribeProducersRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "per-key-independence",
        "memoization",
        "key-identity");

    // Omitted: one-fails / two-fail / all-fail (one future, so they collapse into the fails row); unknown-key, null-key
    // and per-key independence (no keys: Java keeps a one-entry map but publishes no per-partition view).
    public static TheoryData<string, string> AbortTransactionRows => Rows(
        "all-succeed",
        "fails",
        "composed");

    // Omitted: the keyed rows, as for AbortTransactionRows.
    public static TheoryData<string, string> ForceTerminateTransactionRows => Rows(
        "all-succeed",
        "fails",
        "identity");

    public static TheoryData<string, string> UnregisterControllerRows => Rows(
        "all-succeed",
        "fails",
        "identity");


    // Omitted: unknown-key and null-key (no key-taking member: the per-broker map is a plain dictionary over the
    // discovered brokers); erasure identity (no void view); two-fail-first-in-key-order (neither aggregate reports by
    // key order alone: the rule above governs, and the four rows ending this list pin it).
    public static TheoryData<string, string> ListTransactionsRows => Rows(
        "fail-fast",
        "outer-fails",
        "all-succeed",
        "zero-brokers",
        "two-fail-first-in-time",
        "memoization",
        "per-broker-independence",
        "already-failed-first-in-discovery-order",
        "registered-then-two-fail-first-in-time",
        "registered-before-discovery-first-in-time",
        "already-failed-one-still-pending");

    [Theory]
    [MemberData(nameof(FenceProducersRows))]
    public async Task FenceProducers_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        FenceProducersHarness harness = FenceProducersHarness.For(flavour, s_transactionalIds);
        IReadOnlyList<string> keys = harness.Keys;
        switch (row)
        {
            case "unknown-key":
                {
                    // The dictionary fails as a dictionary does; the projections throw Java's precondition at the call.
                    await RunKeyedRow(row, harness, UnknownTransactionalId);
                    string expected = new ArgumentException(
                        "TransactionalId `" + UnknownTransactionalId + "` was not included in the request",
                        "transactionalId").Message;
                    AssertArgument(harness.CallProducerId(UnknownTransactionalId), "transactionalId", expected);
                    AssertArgument(harness.CallEpochId(UnknownTransactionalId), "transactionalId", expected);

                    SucceedEvery(harness);
                    AssertArgument(harness.CallProducerId(UnknownTransactionalId), "transactionalId", expected);
                    AssertArgument(harness.CallEpochId(UnknownTransactionalId), "transactionalId", expected);
                    break;
                }

            case "null-key":
                {
                    await RunKeyedRow(row, harness, UnknownTransactionalId);
                    AssertNullArgument(harness.CallProducerId(null!), "transactionalId");
                    AssertNullArgument(harness.CallEpochId(null!), "transactionalId");

                    SucceedEvery(harness);
                    AssertNullArgument(harness.CallProducerId(null!), "transactionalId");
                    AssertNullArgument(harness.CallEpochId(null!), "transactionalId");
                    break;
                }

            case "projections":
                {
                    FutureProbe firstId = harness.ProducerId(keys[0]);
                    FutureProbe secondEpoch = harness.EpochId(keys[1]);
                    firstId.AssertPending();

                    harness.Succeed(keys[0]);

                    AssertValue(harness.ExpectedProducerId(keys[0]), await firstId.Outcome());
                    AssertValue(harness.ExpectedEpoch(keys[0]), await harness.EpochId(keys[0]).Outcome());

                    // The second id is still outstanding, so its projections are too.
                    secondEpoch.AssertPending();
                    KafkaException failure = new KafkaException(47, "the second producer could not be fenced", isRetriable: false);
                    harness.Fail(keys[1], failure);

                    Assert.Same(failure, await secondEpoch.Outcome());
                    Assert.Same(failure, await harness.ProducerId(keys[1]).Outcome());
                    break;
                }

            case "memoization":
                {
                    KafkaException failure = new KafkaException(47, "the first producer could not be fenced", isRetriable: false);
                    harness.Fail(keys[0], failure);
                    harness.Succeed(keys[1]);

                    // One projection future checks once, so every Get() of it rethrows the same instance ...
                    (bool fresh, FutureProbe first, FutureProbe second) = harness.TwoProducerIds(keys[0]);
                    Assert.Same(failure, await first.Outcome());
                    Assert.Same(failure, await first.Outcome());

                    // ... and each call is a new projection over the same id, on both flavours.
                    Assert.True(fresh, "two ProducerId() calls returned an equal future");
                    Assert.Same(failure, await second.Outcome());

                    FutureProbe epoch = harness.EpochId(keys[1]);
                    AssertValue(harness.ExpectedEpoch(keys[1]), await epoch.Outcome());
                    AssertValue(harness.ExpectedEpoch(keys[1]), await epoch.Outcome());
                    break;
                }

            case "fresh-per-access":
                {
                    Assert.False(harness.MapSameAcrossReads(), "two FencedProducers reads returned the same map");
                    Assert.False(harness.ValueEqualAcrossReads(keys[0]), "two FencedProducers reads returned an equal future");

                    FutureProbe before = harness.Value(keys[0]);
                    before.AssertPending();
                    harness.Succeed(keys[0]);

                    // A future read before its id completed follows it, and one read after is done at once.
                    Assert.True(before.IsDone);
                    Assert.True(harness.Value(keys[0]).IsDone);
                    Assert.Same(harness.Expected(keys[0]), await before.Outcome());
                    Assert.Same(harness.Expected(keys[0]), await harness.Value(keys[0]).Outcome());

                    KafkaException failure = new KafkaException(38, "the second producer could not be fenced", isRetriable: false);
                    harness.Fail(keys[1], failure);
                    Assert.Same(failure, await harness.Value(keys[1]).Outcome());
                    Assert.Same(failure, await harness.Value(keys[1]).Outcome());
                    break;
                }

            default:
                await RunKeyedRow(row, harness, UnknownTransactionalId);
                break;
        }
    }

    [Theory]
    [MemberData(nameof(DescribeTransactionsRows))]
    public async Task DescribeTransactions_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        LookupHarness<string, TransactionDescription> harness =
            LookupHarness<string, TransactionDescription>.DescribeTransactions(flavour, s_transactionalIds);
        switch (row)
        {
            case "unknown-key":
                {
                    // Java's guard: at the call, before completion and after it.
                    string expected = new ArgumentException(
                        "TransactionalId `" + UnknownTransactionalId + "` was not included in the request",
                        "transactionalId").Message;
                    AssertArgument(harness.CallIndexer(UnknownTransactionalId), "transactionalId", expected);
                    Assert.False(harness.ContainsKey(UnknownTransactionalId));
                    Assert.True(harness.ContainsKey(s_transactionalIds[0]));

                    SucceedEvery(harness);
                    AssertArgument(harness.CallIndexer(UnknownTransactionalId), "transactionalId", expected);
                    break;
                }

            case "null-key":
                {
                    AssertNullArgument(harness.CallIndexer(null!), "transactionalId");
                    SucceedEvery(harness);
                    AssertNullArgument(harness.CallIndexer(null!), "transactionalId");
                    break;
                }

            default:
                await RunKeyedRow(row, harness, UnknownTransactionalId);
                break;
        }
    }

    [Theory]
    [MemberData(nameof(DescribeProducersRows))]
    public async Task DescribeProducers_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        LookupHarness<TopicPartition, DescribeProducersResult.PartitionProducerState> harness =
            LookupHarness<TopicPartition, DescribeProducersResult.PartitionProducerState>.DescribeProducers(
                flavour, TopicPartitions());
        TopicPartition unknown = new TopicPartition("gamma", 0);
        switch (row)
        {
            case "unknown-key":
                {
                    // Java's guard: at the call, before completion and after it.
                    string expected = new ArgumentException(
                        "Topic partition " + unknown + " was not included in the request", "partition").Message;
                    AssertArgument(harness.CallIndexer(unknown), "partition", expected);
                    Assert.False(harness.ContainsKey(unknown));
                    Assert.True(harness.ContainsKey(harness.Keys[0]));

                    SucceedEvery(harness);
                    AssertArgument(harness.CallIndexer(unknown), "partition", expected);
                    break;
                }

            default:
                await RunKeyedRow(row, harness, unknown);
                break;
        }
    }

    [Theory]
    [MemberData(nameof(AbortTransactionRows))]
    public Task AbortTransaction_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunSingleRow(row, SingleHarness<object?>.AbortTransaction(flavour), null);

    [Theory]
    [MemberData(nameof(ForceTerminateTransactionRows))]
    public Task ForceTerminateTransaction_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunSingleRow(row, SingleHarness<object?>.ForceTerminateTransaction(flavour), null);

    [Theory]
    [MemberData(nameof(UnregisterControllerRows))]
    public Task UnregisterController_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunSingleRow(row, SingleHarness<object?>.UnregisterController(flavour), null);


    [Theory]
    [MemberData(nameof(ListTransactionsRows))]
    public async Task ListTransactions_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        ListTransactionsHarness harness = ListTransactionsHarness.For(FlavourOf(flavour));
        switch (row)
        {
            case "fail-fast":
                {
                    // Every view requested while the discovery is still running ...
                    FutureProbe byBroker = harness.ByBrokerId();
                    FutureProbe all = harness.AllByBrokerId();
                    FutureProbe flattened = harness.All();
                    byBroker.AssertPending();
                    all.AssertPending();

                    // ... then the brokers are known, and both are still running: so are the aggregates.
                    harness.PublishBrokers(new[] { 1, 2 });
                    object? brokers = await byBroker.Outcome();
                    FutureProbe first = harness.BrokerOf(brokers, 1);
                    all.AssertPending();
                    flattened.AssertPending();
                    first.AssertPending();

                    // Broker 2 fails while broker 1 is still waiting: the aggregates fail at once, with broker 2's own
                    // instance, and broker 1 is still waiting after they have.
                    KafkaException failure = new KafkaException(51, "broker 2 could not list its transactions", isRetriable: true);
                    harness.SetBrokerException(2, failure);

                    Assert.Same(failure, await all.Outcome());
                    Assert.Same(failure, await flattened.Outcome());
                    Assert.Same(failure, await harness.BrokerOf(brokers, 2).Outcome());
                    Assert.True(all.IsDone);
                    first.AssertPending();

                    // A view requested now fails the same way, broker 1 still waiting.
                    Assert.Same(failure, await harness.AllByBrokerId().Outcome());
                    first.AssertPending();

                    // Broker 1 completing later changes none of that; its own future has its own outcome.
                    IReadOnlyCollection<TransactionListing> listings = Listings(1);
                    harness.SetBrokerResult(1, listings);
                    Assert.Same(listings, await first.Outcome());
                    Assert.Same(failure, await all.Outcome());
                    Assert.Same(failure, await harness.AllByBrokerId().Outcome());
                    break;
                }

            case "outer-fails":
                {
                    FutureProbe byBroker = harness.ByBrokerId();
                    FutureProbe all = harness.AllByBrokerId();
                    FutureProbe flattened = harness.All();
                    all.AssertPending();

                    KafkaException failure = new KafkaException(35, "the brokers could not be discovered", isRetriable: false);
                    harness.FailDiscovery(failure);

                    // The discovery's own instance, on every view, requested before the failure and after it.
                    Assert.Same(failure, await byBroker.Outcome());
                    Assert.Same(failure, await all.Outcome());
                    Assert.Same(failure, await flattened.Outcome());
                    Assert.Same(failure, await harness.ByBrokerId().Outcome());
                    Assert.Same(failure, await harness.AllByBrokerId().Outcome());
                    Assert.Same(failure, await harness.All().Outcome());
                    break;
                }

            case "all-succeed":
                {
                    // Discovered out of numeric order: every map keeps the discovery order.
                    int[] discovered = { 3, 1, 2 };
                    Dictionary<int, IReadOnlyCollection<TransactionListing>> expected =
                        discovered.ToDictionary(broker => broker, Listings);
                    FutureProbe all = harness.AllByBrokerId();
                    FutureProbe flattened = harness.All();

                    harness.PublishBrokers(discovered);
                    foreach (int broker in discovered)
                    {
                        harness.SetBrokerResult(broker, expected[broker]);
                    }

                    object? brokers = await harness.ByBrokerId().Outcome();
                    Assert.Equal(discovered, harness.BrokerIdsOf(brokers));
                    foreach (int broker in discovered)
                    {
                        Assert.Same(expected[broker], await harness.BrokerOf(brokers, broker).Outcome());
                    }

                    AssertGathered(discovered, expected, await all.Outcome());
                    Assert.Equal(
                        discovered.SelectMany(broker => expected[broker]).ToList(),
                        Assert.IsAssignableFrom<IReadOnlyCollection<TransactionListing>>(await flattened.Outcome()));
                    break;
                }

            case "zero-brokers":
                {
                    FutureProbe all = harness.AllByBrokerId();
                    FutureProbe flattened = harness.All();
                    harness.PublishBrokers(Array.Empty<int>());

                    // ByBrokerId() is an empty map; the aggregates never complete, as Java's allFuture is completed
                    // only from a per-broker callback.
                    Assert.Empty(harness.BrokerIdsOf(await harness.ByBrokerId().Outcome()));
                    all.AssertPending();
                    flattened.AssertPending();
                    harness.AllByBrokerId().AssertPending();
                    harness.All().AssertPending();
                    break;
                }

            case "two-fail-first-in-time":
                {
                    // The last broker in discovery order fails first, the first last: the aggregate reports the first in time.
                    harness.PublishBrokers(new[] { 1, 2, 3 });
                    FutureProbe all = harness.AllByBrokerId();
                    FutureProbe flattened = harness.All();
                    all.AssertPending();

                    KafkaException earlier = new KafkaException(51, "broker 3 failed first", isRetriable: true);
                    KafkaException later = new KafkaException(52, "broker 1 failed last", isRetriable: false);
                    harness.SetBrokerException(3, earlier);
                    Assert.Same(earlier, await all.Outcome());
                    Assert.Same(earlier, await flattened.Outcome());

                    harness.SetBrokerException(1, later);
                    harness.SetBrokerResult(2, Listings(2));
                    Assert.Same(earlier, await all.Outcome());
                    Assert.Same(earlier, await flattened.Outcome());

                    object? brokers = await harness.ByBrokerId().Outcome();
                    Assert.Same(later, await harness.BrokerOf(brokers, 1).Outcome());
                    Assert.Same(earlier, await harness.BrokerOf(brokers, 3).Outcome());
                    break;
                }

            case "memoization":
                {
                    int[] discovered = { 1, 2 };
                    Dictionary<int, IReadOnlyCollection<TransactionListing>> expected =
                        discovered.ToDictionary(broker => broker, Listings);
                    harness.PublishBrokers(discovered);
                    foreach (int broker in discovered)
                    {
                        harness.SetBrokerResult(broker, expected[broker]);
                    }

                    // One aggregate gathers once, so every Get() of it hands out the same map ...
                    FutureProbe all = harness.AllByBrokerId();
                    object? gathered = await all.Outcome();
                    AssertGathered(discovered, expected, gathered);
                    Assert.Same(gathered, await all.Outcome());
                    FutureProbe flattened = harness.All();
                    object? listings = await flattened.Outcome();
                    Assert.Same(listings, await flattened.Outcome());

                    // ... a new call is a new future, which gathers afresh ...
                    Assert.True(harness.ViewsFreshPerCall(), "two calls of one view returned an equal future");
                    Assert.NotSame(gathered, await harness.AllByBrokerId().Outcome());
                    Assert.NotSame(listings, await harness.All().Outcome());

                    // ... and so is ByBrokerId(): each call builds a new map over the same per-broker futures.
                    FutureProbe byBroker = harness.ByBrokerId();
                    object? brokers = await byBroker.Outcome();
                    Assert.Same(brokers, await byBroker.Outcome());
                    object? again = await harness.ByBrokerId().Outcome();
                    Assert.NotSame(brokers, again);
                    foreach (int broker in discovered)
                    {
                        Assert.True(harness.SameBrokerFuture(brokers, again, broker), $"broker {broker}'s future differs across calls");
                    }

                    break;
                }

            case "per-broker-independence":
                {
                    harness.PublishBrokers(new[] { 1, 2 });
                    object? brokers = await harness.ByBrokerId().Outcome();
                    FutureProbe first = harness.BrokerOf(brokers, 1);
                    FutureProbe second = harness.BrokerOf(brokers, 2);
                    FutureProbe all = harness.AllByBrokerId();
                    FutureProbe flattened = harness.All();

                    IReadOnlyCollection<TransactionListing> firstListings = Listings(1);
                    harness.SetBrokerResult(1, firstListings);
                    Assert.Same(firstListings, await first.Outcome());

                    // The second broker is still outstanding, so the aggregates are too.
                    second.AssertPending();
                    all.AssertPending();
                    flattened.AssertPending();

                    IReadOnlyCollection<TransactionListing> secondListings = Listings(2);
                    harness.SetBrokerResult(2, secondListings);
                    Assert.Same(secondListings, await second.Outcome());
                    AssertGathered(
                        new[] { 1, 2 },
                        new Dictionary<int, IReadOnlyCollection<TransactionListing>> { [1] = firstListings, [2] = secondListings },
                        await all.Outcome());
                    Assert.Equal(
                        firstListings.Concat(secondListings).ToList(),
                        Assert.IsAssignableFrom<IReadOnlyCollection<TransactionListing>>(await flattened.Outcome()));
                    break;
                }

            case "already-failed-first-in-discovery-order":
                {
                    // Broker 3 fails, then broker 2, all before the views are requested.
                    harness.PublishBrokers(new[] { 1, 2, 3 });
                    object? brokers = await harness.ByBrokerId().Outcome();
                    FutureProbe first = harness.BrokerOf(brokers, 1);
                    KafkaException third = new KafkaException(53, "broker 3 failed first", isRetriable: true);
                    KafkaException second = new KafkaException(52, "broker 2 failed second", isRetriable: false);
                    harness.SetBrokerException(3, third);
                    harness.SetBrokerException(2, second);
                    Assert.Same(third, await harness.BrokerOf(brokers, 3).Outcome());
                    Assert.Same(second, await harness.BrokerOf(brokers, 2).Outcome());

                    // Both were complete at the request, so the first faulted in discovery order wins: broker 2, though
                    // broker 3 failed first. Done at the request, while broker 1 is still waiting.
                    FutureProbe all = harness.AllByBrokerId();
                    FutureProbe flattened = harness.All();
                    Assert.True(all.IsDone, "AllByBrokerId() was not done at the request");
                    Assert.True(flattened.IsDone, "All() was not done at the request");
                    Assert.Same(second, await all.Outcome());
                    Assert.Same(second, await flattened.Outcome());
                    first.AssertPending();

                    // Broker 1 completing changes neither, nor a view requested after it.
                    IReadOnlyCollection<TransactionListing> listings = Listings(1);
                    harness.SetBrokerResult(1, listings);
                    Assert.Same(listings, await first.Outcome());
                    Assert.Same(second, await all.Outcome());
                    Assert.Same(second, await flattened.Outcome());
                    Assert.Same(second, await harness.AllByBrokerId().Outcome());
                    Assert.Same(second, await harness.All().Outcome());
                    break;
                }

            case "registered-then-two-fail-first-in-time":
                {
                    // The views are requested with the brokers known and every one still running.
                    harness.PublishBrokers(new[] { 1, 2, 3 });
                    object? brokers = await harness.ByBrokerId().Outcome();
                    FutureProbe all = harness.AllByBrokerId();
                    FutureProbe flattened = harness.All();
                    Assert.False(all.IsDone, "AllByBrokerId() was done with every broker running");
                    Assert.False(flattened.IsDone, "All() was done with every broker running");

                    // Broker 3 fails first, and both views are done; only then does broker 2 fail.
                    KafkaException third = new KafkaException(53, "broker 3 failed first", isRetriable: true);
                    KafkaException second = new KafkaException(52, "broker 2 failed second", isRetriable: false);
                    harness.SetBrokerException(3, third);
                    AssertBecomesDone(all);
                    AssertBecomesDone(flattened);
                    harness.SetBrokerException(2, second);

                    // Neither had completed at the request, so the first in time wins: broker 3.
                    Assert.Same(third, await all.Outcome());
                    Assert.Same(third, await flattened.Outcome());
                    await AssertBrokerOneStillWaits(harness, brokers, all, third, laterView: second);
                    break;
                }

            case "registered-before-discovery-first-in-time":
                {
                    // The views are requested while the discovery is still running: they register when the brokers are
                    // published, before any can complete.
                    FutureProbe all = harness.AllByBrokerId();
                    FutureProbe flattened = harness.All();
                    Assert.False(all.IsDone, "AllByBrokerId() was done before the discovery");
                    Assert.False(flattened.IsDone, "All() was done before the discovery");

                    harness.PublishBrokers(new[] { 1, 2, 3 });
                    object? brokers = await harness.ByBrokerId().Outcome();
                    Assert.False(all.IsDone, "AllByBrokerId() was done with every broker running");
                    Assert.False(flattened.IsDone, "All() was done with every broker running");

                    KafkaException third = new KafkaException(53, "broker 3 failed first", isRetriable: true);
                    KafkaException second = new KafkaException(52, "broker 2 failed second", isRetriable: false);
                    harness.SetBrokerException(3, third);
                    AssertBecomesDone(all);
                    AssertBecomesDone(flattened);
                    harness.SetBrokerException(2, second);

                    Assert.Same(third, await all.Outcome());
                    Assert.Same(third, await flattened.Outcome());
                    await AssertBrokerOneStillWaits(harness, brokers, all, third, laterView: second);
                    break;
                }

            case "already-failed-one-still-pending":
                {
                    // Broker 1 succeeds and broker 3 fails before the request; broker 2 is still running.
                    harness.PublishBrokers(new[] { 1, 2, 3 });
                    object? brokers = await harness.ByBrokerId().Outcome();
                    FutureProbe second = harness.BrokerOf(brokers, 2);
                    KafkaException third = new KafkaException(53, "broker 3 could not list its transactions", isRetriable: true);
                    harness.SetBrokerResult(1, Listings(1));
                    harness.SetBrokerException(3, third);

                    // The views fail at once with broker 3's instance, without waiting for broker 2.
                    FutureProbe all = harness.AllByBrokerId();
                    FutureProbe flattened = harness.All();
                    Assert.True(all.IsDone, "AllByBrokerId() was not done at the request");
                    Assert.True(flattened.IsDone, "All() was not done at the request");
                    Assert.Same(third, await all.Outcome());
                    Assert.Same(third, await flattened.Outcome());
                    second.AssertPending();

                    // Broker 2 completing later changes none of that; its own future has its own outcome.
                    IReadOnlyCollection<TransactionListing> listings = Listings(2);
                    harness.SetBrokerResult(2, listings);
                    Assert.Same(listings, await second.Outcome());
                    Assert.Same(third, await all.Outcome());
                    Assert.Same(third, await harness.AllByBrokerId().Outcome());
                    break;
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(row), row, "unknown row");
        }
    }

    private static async Task AssertBrokerOneStillWaits(
        ListTransactionsHarness harness, object? brokers, FutureProbe all, KafkaException failure, KafkaException laterView)
    {
        FutureProbe first = harness.BrokerOf(brokers, 1);
        first.AssertPending();

        IReadOnlyCollection<TransactionListing> listings = Listings(1);
        harness.SetBrokerResult(1, listings);
        Assert.Same(listings, await first.Outcome());
        Assert.Same(failure, await all.Outcome());
        Assert.Same(laterView, await harness.AllByBrokerId().Outcome());
        Assert.Same(laterView, await harness.All().Outcome());
    }

    private static void AssertBecomesDone(FutureProbe probe) =>
        Assert.True(
            SpinWait.SpinUntil(() => probe.IsDone, BlockingCall.Deadline),
            $"The future was not done within {BlockingCall.Deadline}.");

    private static IReadOnlyCollection<TransactionListing> Listings(int broker) =>
        new[] { new TransactionListing("txn-" + broker, 100L + broker, TransactionState.Ongoing) };

    private static void AssertGathered(
        IReadOnlyList<int> brokers,
        IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>> expected,
        object? outcome)
    {
        IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>> gathered =
            Assert.IsAssignableFrom<IReadOnlyDictionary<int, IReadOnlyCollection<TransactionListing>>>(outcome);
        Assert.Equal(brokers, gathered.Keys);
        foreach (int broker in brokers)
        {
            Assert.Same(expected[broker], gathered[broker]);
        }
    }

    private static async Task RunSingleRow<T>(string row, SingleHarness<T> harness, T value)
    {
        switch (row)
        {
            case "all-succeed":
                {
                    FutureProbe future = harness.Future();
                    future.AssertPending();

                    harness.SetResult(value);

                    AssertSingle(harness, value, await future.Outcome());
                    break;
                }

            case "fails":
                {
                    // The call-level failure is the one future's own, the same instance on every Get().
                    KafkaException failure = new KafkaException(7, "the request failed", isRetriable: true);
                    harness.SetException(failure);

                    FutureProbe future = harness.Future();
                    Assert.Same(failure, await future.Outcome());
                    Assert.Same(failure, await future.Outcome());
                    break;
                }

            case "identity":
                {
                    // The accessor hands out the operation's own future: equal on every call, done the moment it is.
                    Assert.True(harness.EqualAcrossCalls());
                    FutureProbe future = harness.Future();
                    Assert.False(future.IsDone);

                    harness.SetResult(value);

                    Assert.True(future.IsDone);
                    Assert.True(harness.Future().IsDone);
                    Assert.True(harness.EqualAcrossCalls());
                    AssertSingle(harness, value, await future.Outcome());
                    break;
                }

            case "composed":
                {
                    // The accessor composes a new future on every call, each done once the call's future is.
                    Assert.False(harness.EqualAcrossCalls());
                    FutureProbe future = harness.Future();
                    Assert.False(future.IsDone);

                    harness.SetResult(value);

                    Assert.True(future.IsDone);
                    Assert.True(harness.Future().IsDone);
                    Assert.False(harness.EqualAcrossCalls());
                    AssertSingle(harness, value, await future.Outcome());
                    break;
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(row), row, "unknown row");
        }
    }

    private static void AssertSingle<T>(SingleHarness<T> harness, T value, object? outcome)
    {
        if (harness.IsVoid)
        {
            AssertReturned(outcome);
        }
        else
        {
            AssertValue(value, outcome);
        }
    }

    private static void SucceedEvery<TKey>(KeyedHarness<TKey> harness)
        where TKey : notnull
    {
        foreach (TKey key in harness.Keys)
        {
            harness.Succeed(key);
        }
    }

    private static void AssertArgument(object? thrown, string? parameterName, string message)
    {
        ArgumentException failure = Assert.IsType<ArgumentException>(thrown);
        Assert.Equal(parameterName, failure.ParamName);
        Assert.Equal(message, failure.Message);
    }

    private static void AssertNullArgument(object? thrown, string parameterName)
    {
        ArgumentNullException failure = Assert.IsType<ArgumentNullException>(thrown);
        Assert.Equal(parameterName, failure.ParamName);
        Assert.Equal(new ArgumentNullException(parameterName).Message, failure.Message);
    }

    private static async Task RunKeyedRow<TKey>(string row, KeyedHarness<TKey> harness, TKey unknown)
        where TKey : notnull
    {
        IReadOnlyList<TKey> keys = harness.Keys;
        switch (row)
        {
            case "all-succeed":
                {
                    foreach (TKey key in keys)
                    {
                        harness.Succeed(key);
                    }

                    Assert.Equal(keys, harness.ValuesKeys());
                    foreach (TKey key in keys)
                    {
                        FutureProbe value = harness.Value(key);
                        Assert.True(value.IsDone, key.ToString());
                        Assert.Same(harness.Expected(key), await value.Outcome());
                    }

                    harness.AssertAllSucceeded(await harness.All().Outcome());
                    break;
                }

            case "one-fails":
                {
                    KafkaException failure = new KafkaException(38, "the second key failed", isRetriable: false);
                    harness.Succeed(keys[0]);
                    harness.Fail(keys[1], failure);
                    harness.Succeed(keys[2]);

                    Assert.Same(harness.Expected(keys[0]), await harness.Value(keys[0]).Outcome());
                    Assert.Same(failure, await harness.Value(keys[1]).Outcome());
                    Assert.Same(harness.Expected(keys[2]), await harness.Value(keys[2]).Outcome());
                    Assert.Same(failure, await harness.All().Outcome());
                    break;
                }

            case "two-fail-first-in-key-order":
                {
                    // The last key fails FIRST in time, the first key last: the aggregate reports the first in key order.
                    KafkaException first = new KafkaException(41, "the first key failed", isRetriable: false);
                    KafkaException last = new KafkaException(43, "the last key failed", isRetriable: false);
                    harness.Fail(keys[2], last);
                    harness.Succeed(keys[1]);
                    harness.Fail(keys[0], first);

                    Assert.Same(first, await harness.All().Outcome());
                    Assert.Same(last, await harness.Value(keys[2]).Outcome());
                    Assert.Same(harness.Expected(keys[1]), await harness.Value(keys[1]).Outcome());
                    break;
                }

            case "all-fail":
                {
                    KafkaException[] failures =
                    {
                        new KafkaException(41, "the first key failed", isRetriable: false),
                        new KafkaException(42, "the second key failed", isRetriable: true),
                        new KafkaException(43, "the third key failed", isRetriable: false),
                    };

                    // Completed in reverse key order, so key order and completion order disagree.
                    for (int index = keys.Count - 1; index >= 0; index--)
                    {
                        harness.Fail(keys[index], failures[index]);
                    }

                    Assert.Same(failures[0], await harness.All().Outcome());
                    for (int index = 0; index < keys.Count; index++)
                    {
                        Assert.Same(failures[index], await harness.Value(keys[index]).Outcome());
                    }

                    break;
                }

            case "unknown-key":
                {
                    // The per-key dictionary is the only key-taking member: it fails as a dictionary does, at the call.
                    KeyNotFoundException failure = Assert.IsType<KeyNotFoundException>(harness.CallIndexer(unknown));
                    Assert.Equal($"The given key '{unknown}' was not present in the dictionary.", failure.Message);
                    Assert.False(harness.TryGetValue(unknown));
                    Assert.False(harness.ContainsKey(unknown));
                    break;
                }

            case "null-key":
                {
                    ArgumentNullException failure =
                        Assert.IsType<ArgumentNullException>(harness.CallIndexer(default!));
                    Assert.Equal("key", failure.ParamName);
                    Assert.Equal(new ArgumentNullException("key").Message, failure.Message);
                    break;
                }

            case "per-key-independence":
                {
                    FutureProbe second = harness.Value(keys[1]);
                    FutureProbe all = harness.All();
                    harness.Succeed(keys[0]);

                    FutureProbe first = harness.Value(keys[0]);
                    Assert.True(first.IsDone);
                    Assert.Same(harness.Expected(keys[0]), await first.Outcome());

                    Assert.False(second.IsDone);
                    Assert.False(all.IsDone);
                    second.AssertPending();
                    all.AssertPending();

                    harness.Succeed(keys[1]);
                    Assert.Same(harness.Expected(keys[1]), await second.Outcome());

                    // The third key is still outstanding, so the aggregate is too.
                    Assert.False(all.IsDone);

                    harness.Succeed(keys[2]);
                    harness.AssertAllSucceeded(await all.Outcome());
                    break;
                }

            case "memoization":
                {
                    // A gathering aggregate (the rows list it only for a Result whose All() gathers a map).
                    foreach (TKey key in keys)
                    {
                        harness.Succeed(key);
                    }

                    // One aggregate gathers once, so every Get() of it hands out the same map ...
                    FutureProbe aggregate = harness.All();
                    object? gathered = await aggregate.Outcome();
                    harness.AssertAllSucceeded(gathered);
                    Assert.Same(gathered, await aggregate.Outcome());

                    // ... and a new call is a new aggregate, which gathers afresh.
                    Assert.NotSame(gathered, await harness.All().Outcome());
                    break;
                }

            case "erasure-identity":
            case "key-identity":
                {
                    // A key's future is its key's own KafkaFutureImpl: equal on every read, done the moment the key is.
                    Assert.True(harness.ValueEqualAcrossReads(keys[0]));
                    FutureProbe value = harness.Value(keys[0]);
                    Assert.False(value.IsDone);

                    harness.Succeed(keys[0]);

                    Assert.True(value.IsDone);
                    Assert.True(harness.Value(keys[0]).IsDone);
                    Assert.True(harness.ValueEqualAcrossReads(keys[0]));
                    Assert.Same(harness.Expected(keys[0]), await value.Outcome());

                    KafkaException failure = new KafkaException(38, "the second key failed", isRetriable: false);
                    harness.Fail(keys[1], failure);
                    Assert.Same(failure, await harness.Value(keys[1]).Outcome());
                    Assert.Same(failure, await harness.Value(keys[1]).Outcome());
                    break;
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(row), row, "unknown row");
        }
    }

    private static TheoryData<string, string> Rows(params string[] rows)
    {
        TheoryData<string, string> data = new TheoryData<string, string>();
        foreach (string row in rows)
        {
            foreach (string flavour in s_flavours)
            {
                data.Add(row, flavour);
            }
        }

        return data;
    }

    private static Dictionary<TKey, KafkaFutureImpl<TValue>> Impls<TKey, TValue>(
        IReadOnlyList<TKey> keys, IEqualityComparer<TKey> comparer)
        where TKey : notnull
    {
        Dictionary<TKey, KafkaFutureImpl<TValue>> futures = new Dictionary<TKey, KafkaFutureImpl<TValue>>(keys.Count, comparer);
        foreach (TKey key in keys)
        {
            futures.Add(key, new KafkaFutureImpl<TValue>());
        }

        return futures;
    }

    private static SafeAdminResultHandle NoResultHandle() => new SafeAdminResultHandle(IntPtr.Zero, NoDestroy);

    private static void NoDestroy(IntPtr result, ref IntPtr outErr)
    {
    }

    private static AdminFlavour FlavourOf(string flavour) => flavour switch
    {
        Sync => AdminFlavour.Sync,
        Async => AdminFlavour.Async,
        _ => throw new ArgumentOutOfRangeException(nameof(flavour), flavour, "unknown flavour"),
    };

    private static Config Empty() => new Config(Array.Empty<ConfigEntry>());

    private static ConfigResource[] Resources() => new[]
    {
        new ConfigResource(ConfigResourceType.Topic, "alpha"),
        new ConfigResource(ConfigResourceType.Broker, "1"),
        new ConfigResource(ConfigResourceType.Topic, "gamma"),
    };

    private static TopicPartitionReplica[] Replicas() => new[]
    {
        new TopicPartitionReplica("alpha", 0, 1),
        new TopicPartitionReplica("alpha", 1, 2),
        new TopicPartitionReplica("beta", 0, 1),
    };

    private static TopicPartition[] TopicPartitions() => new[]
    {
        new TopicPartition("alpha", 0),
        new TopicPartition("alpha", 1),
        new TopicPartition("beta", 0),
    };

    private static AclBinding[] Bindings() => new[]
    {
        Binding(ResourceType.Topic, "alpha", "User:alice"),
        Binding(ResourceType.Topic, "alpha", "User:bob"),
        Binding(ResourceType.Group, "alpha", "User:alice"),
    };

    private static AclBinding Binding(ResourceType resourceType, string name, string principal) =>
        new AclBinding(
            new ResourcePattern(resourceType, name, PatternType.Literal),
            new AccessControlEntry(principal, "*", AclOperation.Read, AclPermissionType.Allow));

    private static AclBindingFilter[] Filters() => new[] { Filter("alpha"), Filter("beta"), Filter("gamma") };

    private static AclBindingFilter Filter(string name) =>
        new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, name, PatternType.Literal),
            new AccessControlEntryFilter("User:alice", "*", AclOperation.Read, AclPermissionType.Allow));

    private static ClientQuotaEntity[] Entities() => new[]
    {
        Entity(ClientQuotaEntity.User, "alice"),
        Entity(ClientQuotaEntity.User, null),
        Entity(ClientQuotaEntity.ClientId, "alice"),
    };

    private static ClientQuotaEntity Entity(string entityType, string? name) =>
        new ClientQuotaEntity(new Dictionary<string, string?>(StringComparer.Ordinal) { [entityType] = name });

    private static DeleteAclsResult.FilterResults Matched(params DeleteAclsResult.FilterResult[] results) =>
        new DeleteAclsResult.FilterResults(results);

    private static DeleteAclsResult.FilterResult Deleted(AclBinding binding) => new DeleteAclsResult.FilterResult(binding, null);

    private static DeleteAclsResult.FilterResult Failed(AclBinding binding, KafkaException failure) =>
        new DeleteAclsResult.FilterResult(binding, failure);

    private static GroupListing Listing(string groupId) =>
        new GroupListing(groupId, GroupType.Consumer, "consumer", GroupState.Stable);

    private static Dictionary<TopicPartition, KafkaException?> Outcomes(
        IReadOnlyList<TopicPartition> partitions, params KafkaException?[] outcomes)
    {
        Dictionary<TopicPartition, KafkaException?> map = new Dictionary<TopicPartition, KafkaException?>();
        for (int index = 0; index < partitions.Count; index++)
        {
            map.Add(partitions[index], outcomes[index]);
        }

        return map;
    }

    private static CreateTopicsResult.TopicMetadataAndConfig Metadata(int numPartitions, int replicationFactor) =>
        new CreateTopicsResult.TopicMetadataAndConfig(Uuid.Zero, numPartitions, replicationFactor, Empty());

    private static DelegationToken Token(string tokenId) =>
        new DelegationToken(
            new TokenInformation(
                tokenId,
                new KafkaPrincipal("User", "alice"),
                new[] { new KafkaPrincipal("User", "bob") },
                1_000L,
                3_000L,
                2_000L),
            new byte[] { 1, 2, 3 });

    private static void AssertValue<T>(T value, object? outcome)
    {
        object? expected = value;
        if (typeof(T).IsValueType)
        {
            Assert.Equal(expected, outcome);
        }
        else
        {
            Assert.Same(expected, outcome);
        }
    }

    private static void AssertReturned(object? outcome) => Assert.Same(BlockingCall.Returned, outcome);

    private static Exception? Thrown(Action call)
    {
        try
        {
            call();
            return null;
        }
        catch (Exception exception)
        {
            return exception;
        }
    }

    /// <summary>
    /// <see cref="AdminP14SyncCompositionTests.CreateTopics_SyncAndAsyncResults_ComposeAlike"/>'s flavour adapter: the
    /// same operations over a <see cref="CreateTopicsResult"/> or an <see cref="AsyncCreateTopicsResult"/>.
    /// </summary>
    private sealed class CreateTopicsHarness
    {
        internal static readonly string[] Accessors =
        {
            nameof(CreateTopicsResult.Config),
            nameof(CreateTopicsResult.TopicId),
            nameof(CreateTopicsResult.NumPartitions),
            nameof(CreateTopicsResult.ReplicationFactor),
        };

        private readonly Dictionary<string, Action<CreateTopicsResult.TopicMetadataAndConfig>> _complete;
        private readonly Dictionary<string, Action<Exception>> _fail;
        private readonly CreateTopicsResult? _sync;
        private readonly AsyncCreateTopicsResult? _async;

        private CreateTopicsHarness(
            Dictionary<string, Action<CreateTopicsResult.TopicMetadataAndConfig>> complete,
            Dictionary<string, Action<Exception>> fail,
            CreateTopicsResult? sync,
            AsyncCreateTopicsResult? async)
        {
            _complete = complete;
            _fail = fail;
            _sync = sync;
            _async = async;
        }

        internal static CreateTopicsHarness For(string flavour)
        {
            Dictionary<string, Action<CreateTopicsResult.TopicMetadataAndConfig>> complete =
                new Dictionary<string, Action<CreateTopicsResult.TopicMetadataAndConfig>>(StringComparer.Ordinal);
            Dictionary<string, Action<Exception>> fail = new Dictionary<string, Action<Exception>>(StringComparer.Ordinal);

            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                Dictionary<string, KafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>> futures =
                    new Dictionary<string, KafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>>(StringComparer.Ordinal);
                foreach (string topic in s_topics)
                {
                    KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> impl = new KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig>();
                    complete.Add(topic, value => impl.Complete(value));
                    fail.Add(topic, failure => impl.CompleteWithError(failure));
                    futures.Add(topic, new KafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>(impl));
                }

                return new CreateTopicsHarness(complete, fail, new CreateTopicsResult(futures), null);
            }

            Dictionary<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>> asyncFutures =
                new Dictionary<string, AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>>(StringComparer.Ordinal);
            foreach (string topic in s_topics)
            {
                KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig> impl = new KafkaFutureImpl<CreateTopicsResult.TopicMetadataAndConfig>();
                complete.Add(topic, value => impl.Complete(value));
                fail.Add(topic, failure => impl.CompleteWithError(failure));
                asyncFutures.Add(topic, new AsyncKafkaFuture<CreateTopicsResult.TopicMetadataAndConfig>(impl));
            }

            return new CreateTopicsHarness(complete, fail, null, new AsyncCreateTopicsResult(asyncFutures));
        }

        internal void SetResult(string topic, CreateTopicsResult.TopicMetadataAndConfig value) => _complete[topic](value);

        internal void SetException(string topic, Exception failure) => _fail[topic](failure);

        internal IEnumerable<string> ValuesKeys() =>
            _sync is not null ? _sync.Values.Keys.ToList() : _async!.Values.Keys.ToList();

        internal FutureProbe Values(string topic) =>
            _sync is not null ? FutureProbe.Of(_sync.Values[topic]) : FutureProbe.Of(_async!.Values[topic]);

        internal FutureProbe All() => _sync is not null ? FutureProbe.Of(_sync.All()) : FutureProbe.Of(_async!.All());

        internal FutureProbe Config(string topic) =>
            _sync is not null ? FutureProbe.Of(_sync.Config(topic)) : FutureProbe.Of(_async!.Config(topic));

        internal FutureProbe TopicId(string topic) =>
            _sync is not null ? FutureProbe.Of(_sync.TopicId(topic)) : FutureProbe.Of(_async!.TopicId(topic));

        internal FutureProbe NumPartitions(string topic) =>
            _sync is not null ? FutureProbe.Of(_sync.NumPartitions(topic)) : FutureProbe.Of(_async!.NumPartitions(topic));

        internal FutureProbe ReplicationFactor(string topic) =>
            _sync is not null
                ? FutureProbe.Of(_sync.ReplicationFactor(topic))
                : FutureProbe.Of(_async!.ReplicationFactor(topic));

        internal bool ValuesEqualAcrossReads(string topic) =>
            _sync is not null
                ? _sync.Values[topic] == _sync.Values[topic]
                : _async!.Values[topic] == _async.Values[topic];

        internal Exception? CallAccessor(string accessor, string topic) => Thrown(accessor switch
        {
            nameof(CreateTopicsResult.Config) => () => _ = Config(topic),
            nameof(CreateTopicsResult.TopicId) => () => _ = TopicId(topic),
            nameof(CreateTopicsResult.NumPartitions) => () => _ = NumPartitions(topic),
            nameof(CreateTopicsResult.ReplicationFactor) => () => _ = ReplicationFactor(topic),
            _ => throw new ArgumentOutOfRangeException(nameof(accessor), accessor, "unknown accessor"),
        });

        internal Exception? CallValuesIndexer(string topic) => Thrown(() => _ = Values(topic));
    }

    /// <summary>
    /// A keyed Result of either flavour, driven through <c>Succeed</c> / <c>Fail</c>:
    /// the per-key dictionary, its aggregate and the outcome a succeeded key gives.
    /// </summary>
    private abstract class KeyedHarness<TKey>
        where TKey : notnull
    {
        protected KeyedHarness(IReadOnlyList<TKey> keys, bool otherSideIsNull)
        {
            Keys = keys;
            OtherSideIsNull = otherSideIsNull;
        }

        internal IReadOnlyList<TKey> Keys { get; }

        internal bool OtherSideIsNull { get; }

        internal abstract void Succeed(TKey key);

        internal abstract void Fail(TKey key, Exception failure);

        internal abstract object? Expected(TKey key);

        internal abstract IEnumerable<TKey> ValuesKeys();

        internal abstract FutureProbe Value(TKey key);

        internal abstract FutureProbe All();

        internal abstract void AssertAllSucceeded(object? outcome);

        internal abstract bool ValueEqualAcrossReads(TKey key);

        internal abstract bool TryGetValue(TKey key);

        internal abstract bool ContainsKey(TKey key);

        internal Exception? CallIndexer(TKey key) => Thrown(() => _ = Value(key));
    }

    /// <summary>A Result whose per-key futures are void, over one <see cref="KafkaFutureImpl{T}"/> per key.</summary>
    private sealed class VoidHarness<TKey> : KeyedHarness<TKey>
        where TKey : notnull
    {
        private readonly Action<TKey> _succeed;
        private readonly Action<TKey, Exception> _fail;
        private readonly Func<IReadOnlyDictionary<TKey, KafkaFuture>>? _syncValues;
        private readonly Func<KafkaFuture>? _syncAll;
        private readonly Func<IReadOnlyDictionary<TKey, AsyncKafkaFuture>>? _asyncValues;
        private readonly Func<AsyncKafkaFuture>? _asyncAll;

        private VoidHarness(
            IReadOnlyDictionary<TKey, KafkaFutureImpl<object?>> futures,
            IReadOnlyList<TKey> keys,
            bool otherSideIsNull,
            Func<IReadOnlyDictionary<TKey, KafkaFuture>>? syncValues,
            Func<KafkaFuture>? syncAll,
            Func<IReadOnlyDictionary<TKey, AsyncKafkaFuture>>? asyncValues,
            Func<AsyncKafkaFuture>? asyncAll)
            : this(
                key => futures[key].Complete(null),
                (key, failure) => futures[key].CompleteWithError(failure),
                keys,
                otherSideIsNull,
                syncValues,
                syncAll,
                asyncValues,
                asyncAll)
        {
        }

        private VoidHarness(
            Action<TKey> succeed,
            Action<TKey, Exception> fail,
            IReadOnlyList<TKey> keys,
            bool otherSideIsNull,
            Func<IReadOnlyDictionary<TKey, KafkaFuture>>? syncValues,
            Func<KafkaFuture>? syncAll,
            Func<IReadOnlyDictionary<TKey, AsyncKafkaFuture>>? asyncValues,
            Func<AsyncKafkaFuture>? asyncAll)
            : base(keys, otherSideIsNull)
        {
            _succeed = succeed;
            _fail = fail;
            _syncValues = syncValues;
            _syncAll = syncAll;
            _asyncValues = asyncValues;
            _asyncAll = asyncAll;
        }

        internal static VoidHarness<string> DeleteTopicsByName(string flavour, IReadOnlyList<string> topics)
        {
            Dictionary<string, KafkaFutureImpl<object?>> futures =
                Impls<string, object?>(topics, StringComparer.Ordinal);
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                DeleteTopicsResult result = DeleteTopicsResult.OfClient((futures, null, NoResultHandle()));
                return new VoidHarness<string>(
                    futures, topics, result.TopicIdValues is null, () => result.TopicNameValues!, result.All, null, null);
            }

            AsyncDeleteTopicsResult asyncResult = AsyncDeleteTopicsResult.OfClient((futures, null, NoResultHandle()));
            return new VoidHarness<string>(
                futures,
                topics,
                asyncResult.TopicIdValues is null,
                null,
                null,
                () => asyncResult.TopicNameValues!,
                asyncResult.All);
        }

        internal static VoidHarness<Uuid> DeleteTopicsById(string flavour, IReadOnlyList<Uuid> ids)
        {
            Dictionary<Uuid, KafkaFutureImpl<object?>> futures =
                Impls<Uuid, object?>(ids, EqualityComparer<Uuid>.Default);
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                DeleteTopicsResult result = DeleteTopicsResult.OfClient((null, futures, NoResultHandle()));
                return new VoidHarness<Uuid>(
                    futures, ids, result.TopicNameValues is null, () => result.TopicIdValues!, result.All, null, null);
            }

            AsyncDeleteTopicsResult asyncResult = AsyncDeleteTopicsResult.OfClient((null, futures, NoResultHandle()));
            return new VoidHarness<Uuid>(
                futures,
                ids,
                asyncResult.TopicNameValues is null,
                null,
                null,
                () => asyncResult.TopicIdValues!,
                asyncResult.All);
        }

        internal static VoidHarness<string> CreatePartitions(string flavour, IReadOnlyList<string> topics)
        {
            Dictionary<string, KafkaFutureImpl<object?>> futures =
                Impls<string, object?>(topics, StringComparer.Ordinal);
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                CreatePartitionsResult result = CreatePartitionsResult.OfClient((futures, NoResultHandle()));
                return new VoidHarness<string>(futures, topics, true, () => result.Values, result.All, null, null);
            }

            AsyncCreatePartitionsResult asyncResult = AsyncCreatePartitionsResult.OfClient((futures, NoResultHandle()));
            return new VoidHarness<string>(futures, topics, true, null, null, () => asyncResult.Values, asyncResult.All);
        }

        internal static VoidHarness<ConfigResource> IncrementalAlterConfigs(
            string flavour, IReadOnlyList<ConfigResource> resources)
        {
            Dictionary<ConfigResource, KafkaFutureImpl<object?>> futures =
                Impls<ConfigResource, object?>(resources, EqualityComparer<ConfigResource>.Default);
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                AlterConfigsResult result = AlterConfigsResult.OfClient((futures, NoResultHandle()));
                return new VoidHarness<ConfigResource>(futures, resources, true, () => result.Values, result.All, null, null);
            }

            AsyncAlterConfigsResult asyncResult = AsyncAlterConfigsResult.OfClient((futures, NoResultHandle()));
            return new VoidHarness<ConfigResource>(
                futures, resources, true, null, null, () => asyncResult.Values, asyncResult.All);
        }

        internal static VoidHarness<TopicPartitionReplica> AlterReplicaLogDirs(
            string flavour, IReadOnlyList<TopicPartitionReplica> replicas)
        {
            Dictionary<TopicPartitionReplica, KafkaFutureImpl<object?>> futures =
                Impls<TopicPartitionReplica, object?>(replicas, EqualityComparer<TopicPartitionReplica>.Default);
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                AlterReplicaLogDirsResult result = AlterReplicaLogDirsResult.OfClient((futures, NoResultHandle()));
                return new VoidHarness<TopicPartitionReplica>(
                    futures, replicas, true, () => result.Values, result.All, null, null);
            }

            AsyncAlterReplicaLogDirsResult asyncResult = AsyncAlterReplicaLogDirsResult.OfClient((futures, NoResultHandle()));
            return new VoidHarness<TopicPartitionReplica>(
                futures, replicas, true, null, null, () => asyncResult.Values, asyncResult.All);
        }

        internal static VoidHarness<TopicPartition> AlterPartitionReassignments(
            string flavour, IReadOnlyList<TopicPartition> partitions)
        {
            Dictionary<TopicPartition, KafkaFutureImpl<object?>> futures =
                new Dictionary<TopicPartition, KafkaFutureImpl<object?>>(partitions.Count);
            foreach (TopicPartition partition in partitions)
            {
                futures.Add(partition, new KafkaFutureImpl<object?>());
            }

            Action<TopicPartition> succeed = partition => futures[partition].Complete(null);
            Action<TopicPartition, Exception> fail = (partition, failure) => futures[partition].CompleteWithError(failure);
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                AlterPartitionReassignmentsResult result =
                    AlterPartitionReassignmentsResult.OfClient((futures, NoResultHandle()));
                return new VoidHarness<TopicPartition>(
                    succeed, fail, partitions, true, () => result.Values, result.All, null, null);
            }

            AsyncAlterPartitionReassignmentsResult asyncResult =
                AsyncAlterPartitionReassignmentsResult.OfClient((futures, NoResultHandle()));
            return new VoidHarness<TopicPartition>(
                succeed, fail, partitions, true, null, null, () => asyncResult.Values, asyncResult.All);
        }

        internal static VoidHarness<string> DeleteConsumerGroups(string flavour, IReadOnlyList<string> groups)
        {
            Dictionary<string, KafkaFutureImpl<object?>> futures =
                new Dictionary<string, KafkaFutureImpl<object?>>(groups.Count, StringComparer.Ordinal);
            foreach (string group in groups)
            {
                futures.Add(group, new KafkaFutureImpl<object?>());
            }

            Action<string> succeed = group => futures[group].Complete(null);
            Action<string, Exception> fail = (group, failure) => futures[group].CompleteWithError(failure);
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                DeleteConsumerGroupsResult result = DeleteConsumerGroupsResult.OfClient((futures, NoResultHandle()));
                return new VoidHarness<string>(succeed, fail, groups, true, () => result.DeletedGroups, result.All, null, null);
            }

            AsyncDeleteConsumerGroupsResult asyncResult = AsyncDeleteConsumerGroupsResult.OfClient((futures, NoResultHandle()));
            return new VoidHarness<string>(
                succeed, fail, groups, true, null, null, () => asyncResult.DeletedGroups, asyncResult.All);
        }

        internal static VoidHarness<AclBinding> CreateAcls(
            string flavour, IReadOnlyList<AclBinding> requested, IReadOnlyList<AclBinding> keys)
        {
            Dictionary<AclBinding, KafkaFutureImpl<object?>> futures =
                new Dictionary<AclBinding, KafkaFutureImpl<object?>>(requested.Count);
            foreach (AclBinding binding in requested)
            {
                futures.Add(binding, new KafkaFutureImpl<object?>());
            }

            Action<AclBinding> succeed = binding => futures[binding].Complete(null);
            Action<AclBinding, Exception> fail = (binding, failure) => futures[binding].CompleteWithError(failure);
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                CreateAclsResult result = CreateAclsResult.OfClient((futures, NoResultHandle()));
                return new VoidHarness<AclBinding>(succeed, fail, keys, true, () => result.Values, result.All, null, null);
            }

            AsyncCreateAclsResult asyncResult = AsyncCreateAclsResult.OfClient((futures, NoResultHandle()));
            return new VoidHarness<AclBinding>(
                succeed, fail, keys, true, null, null, () => asyncResult.Values, asyncResult.All);
        }

        internal static VoidHarness<ClientQuotaEntity> AlterClientQuotas(
            string flavour, IReadOnlyList<ClientQuotaEntity> requested, IReadOnlyList<ClientQuotaEntity> keys)
        {
            Dictionary<ClientQuotaEntity, KafkaFutureImpl<object?>> futures =
                new Dictionary<ClientQuotaEntity, KafkaFutureImpl<object?>>(requested.Count);
            foreach (ClientQuotaEntity entity in requested)
            {
                futures.Add(entity, new KafkaFutureImpl<object?>());
            }

            Action<ClientQuotaEntity> succeed = entity => futures[entity].Complete(null);
            Action<ClientQuotaEntity, Exception> fail = (entity, failure) => futures[entity].CompleteWithError(failure);
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                AlterClientQuotasResult result = AlterClientQuotasResult.OfClient((futures, NoResultHandle()));
                return new VoidHarness<ClientQuotaEntity>(succeed, fail, keys, true, () => result.Values, result.All, null, null);
            }

            AsyncAlterClientQuotasResult asyncResult = AsyncAlterClientQuotasResult.OfClient((futures, NoResultHandle()));
            return new VoidHarness<ClientQuotaEntity>(
                succeed, fail, keys, true, null, null, () => asyncResult.Values, asyncResult.All);
        }

        internal static VoidHarness<string> AlterUserScramCredentials(string flavour, IReadOnlyList<string> users)
        {
            Dictionary<string, KafkaFutureImpl<object?>> futures =
                new Dictionary<string, KafkaFutureImpl<object?>>(users.Count, StringComparer.Ordinal);
            foreach (string user in users)
            {
                futures.Add(user, new KafkaFutureImpl<object?>());
            }

            Action<string> succeed = user => futures[user].Complete(null);
            Action<string, Exception> fail = (user, failure) => futures[user].CompleteWithError(failure);
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                AlterUserScramCredentialsResult result = AlterUserScramCredentialsResult.OfClient((futures, NoResultHandle()));
                return new VoidHarness<string>(succeed, fail, users, true, () => result.Values, result.All, null, null);
            }

            AsyncAlterUserScramCredentialsResult asyncResult = AsyncAlterUserScramCredentialsResult.OfClient((futures, NoResultHandle()));
            return new VoidHarness<string>(succeed, fail, users, true, null, null, () => asyncResult.Values, asyncResult.All);
        }

        internal static VoidHarness<string> UpdateFeatures(string flavour, IReadOnlyList<string> features)
        {
            Dictionary<string, KafkaFutureImpl<object?>> futures =
                new Dictionary<string, KafkaFutureImpl<object?>>(features.Count, StringComparer.Ordinal);
            foreach (string feature in features)
            {
                futures.Add(feature, new KafkaFutureImpl<object?>());
            }

            Action<string> succeed = feature => futures[feature].Complete(null);
            Action<string, Exception> fail = (feature, failure) => futures[feature].CompleteWithError(failure);
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                UpdateFeaturesResult result = UpdateFeaturesResult.OfClient((futures, NoResultHandle()));
                return new VoidHarness<string>(succeed, fail, features, true, () => result.Values, result.All, null, null);
            }

            AsyncUpdateFeaturesResult asyncResult = AsyncUpdateFeaturesResult.OfClient((futures, NoResultHandle()));
            return new VoidHarness<string>(succeed, fail, features, true, null, null, () => asyncResult.Values, asyncResult.All);
        }

        internal override void Succeed(TKey key) => _succeed(key);

        internal override void Fail(TKey key, Exception failure) => _fail(key, failure);

        internal override object? Expected(TKey key) => BlockingCall.Returned;

        internal override IEnumerable<TKey> ValuesKeys() =>
            _syncValues is not null ? _syncValues().Keys.ToList() : _asyncValues!().Keys.ToList();

        internal override FutureProbe Value(TKey key) =>
            _syncValues is not null ? FutureProbe.Of(_syncValues()[key]) : FutureProbe.Of(_asyncValues!()[key]);

        internal override FutureProbe All() =>
            _syncAll is not null ? FutureProbe.Of(_syncAll()) : FutureProbe.Of(_asyncAll!());

        internal override void AssertAllSucceeded(object? outcome) => AssertReturned(outcome);

        internal override bool ValueEqualAcrossReads(TKey key) =>
            _syncValues is not null ? _syncValues()[key] == _syncValues()[key] : _asyncValues!()[key] == _asyncValues()[key];

        internal override bool TryGetValue(TKey key) =>
            _syncValues is not null ? _syncValues().TryGetValue(key, out _) : _asyncValues!().TryGetValue(key, out _);

        internal override bool ContainsKey(TKey key) =>
            _syncValues is not null ? _syncValues().ContainsKey(key) : _asyncValues!().ContainsKey(key);
    }

    /// <summary>A Result whose per-key futures carry a value, over one <see cref="KafkaFutureImpl{T}"/> per key.</summary>
    private sealed class ValuedHarness<TKey, TValue> : KeyedHarness<TKey>
        where TKey : notnull
        where TValue : class
    {
        private readonly Action<TKey, TValue> _succeed;
        private readonly Action<TKey, Exception> _fail;
        private readonly Dictionary<TKey, TValue> _values;
        private readonly Func<IReadOnlyDictionary<TKey, KafkaFuture<TValue>>>? _syncValues;
        private readonly Func<IReadOnlyDictionary<TKey, AsyncKafkaFuture<TValue>>>? _asyncValues;
        private readonly Func<FutureProbe> _all;
        private readonly bool _allGathers;

        private ValuedHarness(
            IReadOnlyDictionary<TKey, KafkaFutureImpl<TValue>> futures,
            IReadOnlyList<TKey> keys,
            Func<TKey, TValue> value,
            bool otherSideIsNull,
            Func<IReadOnlyDictionary<TKey, KafkaFuture<TValue>>>? syncValues,
            Func<IReadOnlyDictionary<TKey, AsyncKafkaFuture<TValue>>>? asyncValues,
            Func<FutureProbe> all,
            bool allGathers)
            : this(
                (key, result) => futures[key].Complete(result),
                (key, failure) => futures[key].CompleteWithError(failure),
                keys,
                value,
                otherSideIsNull,
                syncValues,
                asyncValues,
                all,
                allGathers)
        {
        }

        private ValuedHarness(
            Action<TKey, TValue> succeed,
            Action<TKey, Exception> fail,
            IReadOnlyList<TKey> keys,
            Func<TKey, TValue> value,
            bool otherSideIsNull,
            Func<IReadOnlyDictionary<TKey, KafkaFuture<TValue>>>? syncValues,
            Func<IReadOnlyDictionary<TKey, AsyncKafkaFuture<TValue>>>? asyncValues,
            Func<FutureProbe> all,
            bool allGathers)
            : base(keys, otherSideIsNull)
        {
            _succeed = succeed;
            _fail = fail;
            _values = keys.ToDictionary(key => key, value);
            _syncValues = syncValues;
            _asyncValues = asyncValues;
            _all = all;
            _allGathers = allGathers;
        }

        internal static ValuedHarness<string, TopicDescription> DescribeTopicsByName(string flavour, IReadOnlyList<string> topics)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            Dictionary<string, KafkaFutureImpl<TopicDescription>> futures =
                Impls<string, TopicDescription>(topics, StringComparer.Ordinal);
            if (adminFlavour == AdminFlavour.Sync)
            {
                DescribeTopicsResult result = DescribeTopicsResult.OfClient((futures, null, NoResultHandle()));
                return new ValuedHarness<string, TopicDescription>(
                    futures,
                    topics,
                    Description,
                    result.TopicIdValues is null && result.AllTopicIds() is null,
                    () => result.TopicNameValues!,
                    null,
                    () => FutureProbe.Of(result.AllTopicNames()!.Value),
                    allGathers: true);
            }

            AsyncDescribeTopicsResult asyncResult = AsyncDescribeTopicsResult.OfClient((futures, null, NoResultHandle()));
            return new ValuedHarness<string, TopicDescription>(
                futures,
                topics,
                Description,
                asyncResult.TopicIdValues is null && asyncResult.AllTopicIds() is null,
                null,
                () => asyncResult.TopicNameValues!,
                () => FutureProbe.Of(asyncResult.AllTopicNames()!.Value),
                allGathers: true);
        }

        internal static ValuedHarness<Uuid, TopicDescription> DescribeTopicsById(string flavour, IReadOnlyList<Uuid> ids)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            Dictionary<Uuid, KafkaFutureImpl<TopicDescription>> futures =
                Impls<Uuid, TopicDescription>(ids, EqualityComparer<Uuid>.Default);
            if (adminFlavour == AdminFlavour.Sync)
            {
                DescribeTopicsResult result = DescribeTopicsResult.OfClient((null, futures, NoResultHandle()));
                return new ValuedHarness<Uuid, TopicDescription>(
                    futures,
                    ids,
                    id => Description(id.ToString()),
                    result.TopicNameValues is null && result.AllTopicNames() is null,
                    () => result.TopicIdValues!,
                    null,
                    () => FutureProbe.Of(result.AllTopicIds()!.Value),
                    allGathers: true);
            }

            AsyncDescribeTopicsResult asyncResult = AsyncDescribeTopicsResult.OfClient((null, futures, NoResultHandle()));
            return new ValuedHarness<Uuid, TopicDescription>(
                futures,
                ids,
                id => Description(id.ToString()),
                asyncResult.TopicNameValues is null && asyncResult.AllTopicNames() is null,
                null,
                () => asyncResult.TopicIdValues!,
                () => FutureProbe.Of(asyncResult.AllTopicIds()!.Value),
                allGathers: true);
        }

        internal static ValuedHarness<TopicPartition, DeletedRecords> DeleteRecords(
            string flavour, IReadOnlyList<TopicPartition> partitions)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            Dictionary<TopicPartition, KafkaFutureImpl<DeletedRecords>> futures =
                Impls<TopicPartition, DeletedRecords>(partitions, EqualityComparer<TopicPartition>.Default);
            if (adminFlavour == AdminFlavour.Sync)
            {
                DeleteRecordsResult result = DeleteRecordsResult.OfClient((futures, NoResultHandle()));
                return new ValuedHarness<TopicPartition, DeletedRecords>(
                    futures,
                    partitions,
                    partition => new DeletedRecords(100L + partition.Partition),
                    true,
                    () => result.LowWatermarks,
                    null,
                    () => FutureProbe.Of(result.All()),
                    allGathers: false);
            }

            AsyncDeleteRecordsResult asyncResult = AsyncDeleteRecordsResult.OfClient((futures, NoResultHandle()));
            return new ValuedHarness<TopicPartition, DeletedRecords>(
                futures,
                partitions,
                partition => new DeletedRecords(100L + partition.Partition),
                true,
                null,
                () => asyncResult.LowWatermarks,
                () => FutureProbe.Of(asyncResult.All()),
                allGathers: false);
        }

        internal static ValuedHarness<ConfigResource, Config> DescribeConfigs(
            string flavour, IReadOnlyList<ConfigResource> resources)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            Dictionary<ConfigResource, KafkaFutureImpl<Config>> futures =
                Impls<ConfigResource, Config>(resources, EqualityComparer<ConfigResource>.Default);
            if (adminFlavour == AdminFlavour.Sync)
            {
                DescribeConfigsResult result = DescribeConfigsResult.OfClient((futures, NoResultHandle()));
                return new ValuedHarness<ConfigResource, Config>(
                    futures, resources, _ => Empty(), true, () => result.Values, null, () => FutureProbe.Of(result.All()), allGathers: true);
            }

            AsyncDescribeConfigsResult asyncResult = AsyncDescribeConfigsResult.OfClient((futures, NoResultHandle()));
            return new ValuedHarness<ConfigResource, Config>(
                futures,
                resources,
                _ => Empty(),
                true,
                null,
                () => asyncResult.Values,
                () => FutureProbe.Of(asyncResult.All()),
                allGathers: true);
        }

        internal static ValuedHarness<int, IReadOnlyDictionary<string, LogDirDescription>> DescribeLogDirs(
            string flavour, IReadOnlyList<int> brokers)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            Dictionary<int, KafkaFutureImpl<IReadOnlyDictionary<string, LogDirDescription>>> futures =
                Impls<int, IReadOnlyDictionary<string, LogDirDescription>>(brokers, EqualityComparer<int>.Default);
            if (adminFlavour == AdminFlavour.Sync)
            {
                DescribeLogDirsResult result = DescribeLogDirsResult.OfClient((futures, NoResultHandle()));
                return new ValuedHarness<int, IReadOnlyDictionary<string, LogDirDescription>>(
                    futures,
                    brokers,
                    LogDirs,
                    true,
                    () => result.Descriptions,
                    null,
                    () => FutureProbe.Of(result.AllDescriptions()),
                    allGathers: true);
            }

            AsyncDescribeLogDirsResult asyncResult = AsyncDescribeLogDirsResult.OfClient((futures, NoResultHandle()));
            return new ValuedHarness<int, IReadOnlyDictionary<string, LogDirDescription>>(
                futures,
                brokers,
                LogDirs,
                true,
                null,
                () => asyncResult.Descriptions,
                () => FutureProbe.Of(asyncResult.AllDescriptions()),
                allGathers: true);
        }

        internal static ValuedHarness<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo> DescribeReplicaLogDirs(
            string flavour, IReadOnlyList<TopicPartitionReplica> replicas)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            Dictionary<TopicPartitionReplica, KafkaFutureImpl<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> futures =
                Impls<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo>(replicas, EqualityComparer<TopicPartitionReplica>.Default);
            if (adminFlavour == AdminFlavour.Sync)
            {
                DescribeReplicaLogDirsResult result =
                    DescribeReplicaLogDirsResult.OfClient((futures, NoResultHandle()));
                return new ValuedHarness<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo>(
                    futures,
                    replicas,
                    ReplicaLogDir,
                    true,
                    () => result.Values,
                    null,
                    () => FutureProbe.Of(result.All()),
                    allGathers: true);
            }

            AsyncDescribeReplicaLogDirsResult asyncResult =
                AsyncDescribeReplicaLogDirsResult.OfClient((futures, NoResultHandle()));
            return new ValuedHarness<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo>(
                futures,
                replicas,
                ReplicaLogDir,
                true,
                null,
                () => asyncResult.Values,
                () => FutureProbe.Of(asyncResult.All()),
                allGathers: true);
        }

        internal static ValuedHarness<string, ConsumerGroupDescription> DescribeConsumerGroups(
            string flavour, IReadOnlyList<string> groups)
        {
            Dictionary<string, KafkaFutureImpl<ConsumerGroupDescription>> futures =
                new Dictionary<string, KafkaFutureImpl<ConsumerGroupDescription>>(groups.Count, StringComparer.Ordinal);
            foreach (string group in groups)
            {
                futures.Add(group, new KafkaFutureImpl<ConsumerGroupDescription>());
            }

            Action<string, ConsumerGroupDescription> succeed = (group, value) => futures[group].Complete(value);
            Action<string, Exception> fail = (group, failure) => futures[group].CompleteWithError(failure);
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                DescribeConsumerGroupsResult result = DescribeConsumerGroupsResult.OfClient((futures, NoResultHandle()));
                return new ValuedHarness<string, ConsumerGroupDescription>(
                    succeed,
                    fail,
                    groups,
                    ConsumerGroup,
                    true,
                    () => result.DescribedGroups,
                    null,
                    () => FutureProbe.Of(result.All()),
                    allGathers: true);
            }

            AsyncDescribeConsumerGroupsResult asyncResult =
                AsyncDescribeConsumerGroupsResult.OfClient((futures, NoResultHandle()));
            return new ValuedHarness<string, ConsumerGroupDescription>(
                succeed,
                fail,
                groups,
                ConsumerGroup,
                true,
                null,
                () => asyncResult.DescribedGroups,
                () => FutureProbe.Of(asyncResult.All()),
                allGathers: true);
        }

        internal static ValuedHarness<string, ClassicGroupDescription> DescribeClassicGroups(
            string flavour, IReadOnlyList<string> groups)
        {
            Dictionary<string, KafkaFutureImpl<ClassicGroupDescription>> futures =
                new Dictionary<string, KafkaFutureImpl<ClassicGroupDescription>>(groups.Count, StringComparer.Ordinal);
            foreach (string group in groups)
            {
                futures.Add(group, new KafkaFutureImpl<ClassicGroupDescription>());
            }

            Action<string, ClassicGroupDescription> succeed = (group, value) => futures[group].Complete(value);
            Action<string, Exception> fail = (group, failure) => futures[group].CompleteWithError(failure);
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                DescribeClassicGroupsResult result = DescribeClassicGroupsResult.OfClient((futures, NoResultHandle()));
                return new ValuedHarness<string, ClassicGroupDescription>(
                    succeed,
                    fail,
                    groups,
                    ClassicGroup,
                    true,
                    () => result.DescribedGroups,
                    null,
                    () => FutureProbe.Of(result.All()),
                    allGathers: true);
            }

            AsyncDescribeClassicGroupsResult asyncResult =
                AsyncDescribeClassicGroupsResult.OfClient((futures, NoResultHandle()));
            return new ValuedHarness<string, ClassicGroupDescription>(
                succeed,
                fail,
                groups,
                ClassicGroup,
                true,
                null,
                () => asyncResult.DescribedGroups,
                () => FutureProbe.Of(asyncResult.All()),
                allGathers: true);
        }

        internal override void Succeed(TKey key) => _succeed(key, _values[key]);

        internal override void Fail(TKey key, Exception failure) => _fail(key, failure);

        internal override object? Expected(TKey key) => _values[key];

        internal override IEnumerable<TKey> ValuesKeys() =>
            _syncValues is not null ? _syncValues().Keys.ToList() : _asyncValues!().Keys.ToList();

        internal override FutureProbe Value(TKey key) =>
            _syncValues is not null ? FutureProbe.Of(_syncValues()[key]) : FutureProbe.Of(_asyncValues!()[key]);

        internal override FutureProbe All() => _all();

        internal override void AssertAllSucceeded(object? outcome)
        {
            if (!_allGathers)
            {
                AssertReturned(outcome);
                return;
            }

            // The gathered map: every key, in key order, each value the very instance its key completed with.
            IReadOnlyDictionary<TKey, TValue> gathered = Assert.IsAssignableFrom<IReadOnlyDictionary<TKey, TValue>>(outcome);
            Assert.Equal(Keys, gathered.Keys);
            foreach (TKey key in Keys)
            {
                Assert.Same(_values[key], gathered[key]);
            }
        }

        internal override bool ValueEqualAcrossReads(TKey key) =>
            _syncValues is not null ? _syncValues()[key] == _syncValues()[key] : _asyncValues!()[key] == _asyncValues()[key];

        internal override bool TryGetValue(TKey key) =>
            _syncValues is not null ? _syncValues().TryGetValue(key, out _) : _asyncValues!().TryGetValue(key, out _);

        internal override bool ContainsKey(TKey key) =>
            _syncValues is not null ? _syncValues().ContainsKey(key) : _asyncValues!().ContainsKey(key);

        private static TopicDescription Description(string name) =>
            new TopicDescription(name, false, Array.Empty<TopicPartitionInfo>());

        private static IReadOnlyDictionary<string, LogDirDescription> LogDirs(int broker) =>
            new Dictionary<string, LogDirDescription>(StringComparer.Ordinal)
            {
                ["/data/broker-" + broker] = new LogDirDescription(null, new Dictionary<TopicPartition, ReplicaInfo>()),
            };

        private static DescribeReplicaLogDirsResult.ReplicaLogDirInfo ReplicaLogDir(TopicPartitionReplica replica) =>
            new DescribeReplicaLogDirsResult.ReplicaLogDirInfo("/data/broker-" + replica.BrokerId, 0L, null, -1L);

        private static ConsumerGroupDescription ConsumerGroup(string groupId) =>
            new ConsumerGroupDescription(
                groupId, false, null, "range", GroupType.Consumer, GroupState.Stable, null, null, null, null);

        private static ClassicGroupDescription ClassicGroup(string groupId) =>
            new ClassicGroupDescription(groupId, "consumer", "range", null, ClassicGroupState.Stable, null);
    }

    /// <summary>
    /// <see cref="AdminP14SyncCompositionTests.FenceProducers_SyncAndAsyncResults_ComposeAlike"/>'s flavour adapter: the
    /// <c>FencedProducers</c> dictionary as the per-key view, the void all-of <c>All()</c>, and the <c>ProducerId</c> /
    /// <c>EpochId</c> projections of each id's own value.
    /// </summary>
    private sealed class FenceProducersHarness : KeyedHarness<string>
    {
        private readonly Dictionary<string, KafkaFutureImpl<ProducerIdAndEpoch>> _futures;
        private readonly Dictionary<string, ProducerIdAndEpoch> _values;
        private readonly FenceProducersResult? _sync;
        private readonly AsyncFenceProducersResult? _async;

        private FenceProducersHarness(
            Dictionary<string, KafkaFutureImpl<ProducerIdAndEpoch>> futures,
            IReadOnlyList<string> ids,
            FenceProducersResult? sync,
            AsyncFenceProducersResult? async)
            : base(ids, true)
        {
            _futures = futures;

            // A distinct producer id and epoch per transactional id.
            _values = new Dictionary<string, ProducerIdAndEpoch>(StringComparer.Ordinal);
            for (int index = 0; index < ids.Count; index++)
            {
                _values.Add(ids[index], new ProducerIdAndEpoch(5_000L + index, (short)(index + 2)));
            }

            _sync = sync;
            _async = async;
        }

        internal static FenceProducersHarness For(string flavour, IReadOnlyList<string> ids)
        {
            Dictionary<string, KafkaFutureImpl<ProducerIdAndEpoch>> futures =
                new Dictionary<string, KafkaFutureImpl<ProducerIdAndEpoch>>(ids.Count, StringComparer.Ordinal);
            foreach (string id in ids)
            {
                futures.Add(id, new KafkaFutureImpl<ProducerIdAndEpoch>());
            }

            return FlavourOf(flavour) == AdminFlavour.Sync
                ? new FenceProducersHarness(futures, ids, FenceProducersResult.OfClient((futures, NoResultHandle())), null)
                : new FenceProducersHarness(futures, ids, null, AsyncFenceProducersResult.OfClient((futures, NoResultHandle())));
        }

        internal override void Succeed(string key) => _futures[key].Complete(_values[key]);

        internal override void Fail(string key, Exception failure) => _futures[key].CompleteWithError(failure);

        internal override object? Expected(string key) => BlockingCall.Returned;

        internal override IEnumerable<string> ValuesKeys() =>
            _sync is not null ? _sync.FencedProducers.Keys.ToList() : _async!.FencedProducers.Keys.ToList();

        internal override FutureProbe Value(string key) =>
            _sync is not null ? FutureProbe.Of(_sync.FencedProducers[key]) : FutureProbe.Of(_async!.FencedProducers[key]);

        internal override FutureProbe All() => _sync is not null ? FutureProbe.Of(_sync.All()) : FutureProbe.Of(_async!.All());

        internal override void AssertAllSucceeded(object? outcome) => AssertReturned(outcome);

        internal override bool ValueEqualAcrossReads(string key) =>
            _sync is not null
                ? _sync.FencedProducers[key] == _sync.FencedProducers[key]
                : _async!.FencedProducers[key] == _async.FencedProducers[key];

        internal bool MapSameAcrossReads() =>
            _sync is not null
                ? ReferenceEquals(_sync.FencedProducers, _sync.FencedProducers)
                : ReferenceEquals(_async!.FencedProducers, _async.FencedProducers);

        internal override bool TryGetValue(string key) =>
            _sync is not null ? _sync.FencedProducers.TryGetValue(key, out _) : _async!.FencedProducers.TryGetValue(key, out _);

        internal override bool ContainsKey(string key) =>
            _sync is not null ? _sync.FencedProducers.ContainsKey(key) : _async!.FencedProducers.ContainsKey(key);

        internal long ExpectedProducerId(string key) => _values[key].ProducerId;

        internal short ExpectedEpoch(string key) => _values[key].Epoch;

        internal FutureProbe ProducerId(string key) =>
            _sync is not null ? FutureProbe.Of(_sync.ProducerId(key)) : FutureProbe.Of(_async!.ProducerId(key));

        internal FutureProbe EpochId(string key) =>
            _sync is not null ? FutureProbe.Of(_sync.EpochId(key)) : FutureProbe.Of(_async!.EpochId(key));

        internal Exception? CallProducerId(string key) => Thrown(() => _ = ProducerId(key));

        internal Exception? CallEpochId(string key) => Thrown(() => _ = EpochId(key));

        internal (bool Fresh, FutureProbe First, FutureProbe Second) TwoProducerIds(string key)
        {
            if (_sync is not null)
            {
                KafkaFuture<long> first = _sync.ProducerId(key);
                KafkaFuture<long> second = _sync.ProducerId(key);
                return (first != second, FutureProbe.Of(first), FutureProbe.Of(second));
            }

            AsyncKafkaFuture<long> asyncFirst = _async!.ProducerId(key);
            AsyncKafkaFuture<long> asyncSecond = _async.ProducerId(key);
            return (asyncFirst != asyncSecond, FutureProbe.Of(asyncFirst), FutureProbe.Of(asyncSecond));
        }
    }

    /// <summary>
    /// A keyed Result with no per-key dictionary — <see cref="DescribeTransactionsResult"/>'s
    /// <c>Description(transactionalId)</c> or <see cref="DescribeProducersResult"/>'s <c>PartitionResult(partition)</c>
    /// is its per-key member, and the keys it answers for are those that member does not reject — with a gathering
    /// <c>All()</c>.
    /// </summary>
    private sealed class LookupHarness<TKey, TValue> : KeyedHarness<TKey>
        where TKey : notnull
        where TValue : class
    {
        private readonly Action<TKey, TValue> _succeed;
        private readonly Action<TKey, Exception> _fail;
        private readonly Dictionary<TKey, TValue> _values;
        private readonly Func<TKey, KafkaFuture<TValue>>? _syncLookup;
        private readonly Func<TKey, AsyncKafkaFuture<TValue>>? _asyncLookup;
        private readonly Func<FutureProbe> _all;

        private LookupHarness(
            Action<TKey, TValue> succeed,
            Action<TKey, Exception> fail,
            IReadOnlyList<TKey> keys,
            Func<TKey, TValue> value,
            Func<TKey, KafkaFuture<TValue>>? syncLookup,
            Func<TKey, AsyncKafkaFuture<TValue>>? asyncLookup,
            Func<FutureProbe> all)
            : base(keys, true)
        {
            _succeed = succeed;
            _fail = fail;
            _values = keys.ToDictionary(key => key, value);
            _syncLookup = syncLookup;
            _asyncLookup = asyncLookup;
            _all = all;
        }

        internal static LookupHarness<string, TransactionDescription> DescribeTransactions(
            string flavour, IReadOnlyList<string> ids)
        {
            Dictionary<string, KafkaFutureImpl<TransactionDescription>> futures =
                new Dictionary<string, KafkaFutureImpl<TransactionDescription>>(ids.Count, StringComparer.Ordinal);
            foreach (string id in ids)
            {
                futures.Add(id, new KafkaFutureImpl<TransactionDescription>());
            }

            Action<string, TransactionDescription> succeed = (id, value) => futures[id].Complete(value);
            Action<string, Exception> fail = (id, failure) => futures[id].CompleteWithError(failure);
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                DescribeTransactionsResult result = DescribeTransactionsResult.OfClient((futures, NoResultHandle()));
                return new LookupHarness<string, TransactionDescription>(
                    succeed, fail, ids, Transaction, result.Description, null, () => FutureProbe.Of(result.All()));
            }

            AsyncDescribeTransactionsResult asyncResult = AsyncDescribeTransactionsResult.OfClient((futures, NoResultHandle()));
            return new LookupHarness<string, TransactionDescription>(
                succeed, fail, ids, Transaction, null, asyncResult.Description, () => FutureProbe.Of(asyncResult.All()));
        }

        internal static LookupHarness<TopicPartition, DescribeProducersResult.PartitionProducerState> DescribeProducers(
            string flavour, IReadOnlyList<TopicPartition> partitions)
        {
            Dictionary<TopicPartition, KafkaFutureImpl<DescribeProducersResult.PartitionProducerState>> futures =
                new Dictionary<TopicPartition, KafkaFutureImpl<DescribeProducersResult.PartitionProducerState>>(partitions.Count);
            foreach (TopicPartition partition in partitions)
            {
                futures.Add(partition, new KafkaFutureImpl<DescribeProducersResult.PartitionProducerState>());
            }

            Action<TopicPartition, DescribeProducersResult.PartitionProducerState> succeed =
                (partition, value) => futures[partition].Complete(value);
            Action<TopicPartition, Exception> fail = (partition, failure) => futures[partition].CompleteWithError(failure);
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                DescribeProducersResult result = DescribeProducersResult.OfClient((futures, NoResultHandle()));
                return new LookupHarness<TopicPartition, DescribeProducersResult.PartitionProducerState>(
                    succeed, fail, partitions, Producers, result.PartitionResult, null, () => FutureProbe.Of(result.All()));
            }

            AsyncDescribeProducersResult asyncResult = AsyncDescribeProducersResult.OfClient((futures, NoResultHandle()));
            return new LookupHarness<TopicPartition, DescribeProducersResult.PartitionProducerState>(
                succeed, fail, partitions, Producers, null, asyncResult.PartitionResult, () => FutureProbe.Of(asyncResult.All()));
        }

        internal override void Succeed(TKey key) => _succeed(key, _values[key]);

        internal override void Fail(TKey key, Exception failure) => _fail(key, failure);

        internal override object? Expected(TKey key) => _values[key];

        internal override IEnumerable<TKey> ValuesKeys() => Keys.Where(Answers).ToList();

        internal override FutureProbe Value(TKey key) =>
            _syncLookup is not null ? FutureProbe.Of(_syncLookup(key)) : FutureProbe.Of(_asyncLookup!(key));

        internal override FutureProbe All() => _all();

        internal override void AssertAllSucceeded(object? outcome)
        {
            // The gathered map: every key, in key order, each value the very instance its key completed with.
            IReadOnlyDictionary<TKey, TValue> gathered = Assert.IsAssignableFrom<IReadOnlyDictionary<TKey, TValue>>(outcome);
            Assert.Equal(Keys, gathered.Keys);
            foreach (TKey key in Keys)
            {
                Assert.Same(_values[key], gathered[key]);
            }
        }

        internal override bool ValueEqualAcrossReads(TKey key) =>
            _syncLookup is not null ? _syncLookup(key) == _syncLookup(key) : _asyncLookup!(key) == _asyncLookup(key);

        internal override bool TryGetValue(TKey key) => Answers(key);

        internal override bool ContainsKey(TKey key) => Answers(key);

        private static TransactionDescription Transaction(string transactionalId) =>
            new TransactionDescription(
                1,
                TransactionState.Ongoing,
                1_000L + transactionalId.Sum(character => (long)character),
                3,
                60_000L,
                1_700_000_000_000L,
                new[] { new TopicPartition("alpha", 0) });

        private static DescribeProducersResult.PartitionProducerState Producers(TopicPartition partition) =>
            new DescribeProducersResult.PartitionProducerState(
                new[] { new ProducerState(2_000L + partition.Partition, 2, 7, 1_700_000_000_000L, null, null) });

        private bool Answers(TKey key) => CallIndexer(key) is null;
    }

    /// <summary>
    /// <see cref="AdminP14SyncCompositionTests.ListTopics_SyncAndAsyncResults_ComposeAlike"/>'s flavour adapter.
    /// </summary>
    private sealed class ListTopicsHarness
    {
        private readonly ListTopicsResult? _sync;
        private readonly AsyncListTopicsResult? _async;

        private ListTopicsHarness(
            KafkaFutureImpl<IReadOnlyDictionary<string, TopicListing>> impl,
            ListTopicsResult? sync,
            AsyncListTopicsResult? async)
        {
            Impl = impl;
            _sync = sync;
            _async = async;
        }

        internal KafkaFutureImpl<IReadOnlyDictionary<string, TopicListing>> Impl { get; }

        internal static ListTopicsHarness For(string flavour)
        {
            KafkaFutureImpl<IReadOnlyDictionary<string, TopicListing>> impl =
                new KafkaFutureImpl<IReadOnlyDictionary<string, TopicListing>>();
            return FlavourOf(flavour) == AdminFlavour.Sync
                ? new ListTopicsHarness(
                    impl, new ListTopicsResult(new KafkaFuture<IReadOnlyDictionary<string, TopicListing>>(impl)), null)
                : new ListTopicsHarness(
                    impl, null, new AsyncListTopicsResult(new AsyncKafkaFuture<IReadOnlyDictionary<string, TopicListing>>(impl)));
        }

        internal FutureProbe NamesToListings() =>
            _sync is not null ? FutureProbe.Of(_sync.NamesToListings()) : FutureProbe.Of(_async!.NamesToListings());

        internal FutureProbe Listings() =>
            _sync is not null ? FutureProbe.Of(_sync.Listings()) : FutureProbe.Of(_async!.Listings());

        internal FutureProbe Names() => _sync is not null ? FutureProbe.Of(_sync.Names()) : FutureProbe.Of(_async!.Names());

        internal bool NamesToListingsEqualAcrossCalls() =>
            _sync is not null
                ? _sync.NamesToListings() == _sync.NamesToListings()
                : _async!.NamesToListings() == _async.NamesToListings();
    }

    /// <summary>
    /// <see cref="AdminP14SyncCompositionTests.ListTransactions_SyncAndAsyncResults_ComposeAlike"/>'s flavour adapter:
    /// the broker discovery and one future per discovered broker, completed by hand.
    /// </summary>
    private sealed class ListTransactionsHarness
    {
        private readonly KafkaFutureImpl<IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>>> _discovery =
            new KafkaFutureImpl<IReadOnlyDictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>>>();
        private readonly Dictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>> _brokers =
            new Dictionary<int, KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>>();
        private readonly ListTransactionsResult? _sync;
        private readonly AsyncListTransactionsResult? _async;

        private ListTransactionsHarness(AdminFlavour flavour)
        {
            if (flavour == AdminFlavour.Sync)
            {
                _sync = ListTransactionsResult.OfClient((_discovery, NoResultHandle()));
            }
            else
            {
                _async = AsyncListTransactionsResult.OfClient((_discovery, NoResultHandle()));
            }
        }

        internal static ListTransactionsHarness For(AdminFlavour flavour) => new ListTransactionsHarness(flavour);

        internal void PublishBrokers(IReadOnlyList<int> brokers)
        {
            foreach (int broker in brokers)
            {
                _brokers.Add(broker, new KafkaFutureImpl<IReadOnlyCollection<TransactionListing>>());
            }

            Assert.True(_discovery.Complete(_brokers));
        }

        internal void FailDiscovery(Exception failure) => Assert.True(_discovery.CompleteWithError(failure));

        internal void SetBrokerResult(int broker, IReadOnlyCollection<TransactionListing> listings) =>
            Assert.True(_brokers[broker].Complete(listings));

        internal void SetBrokerException(int broker, Exception failure) =>
            Assert.True(_brokers[broker].CompleteWithError(failure));

        internal FutureProbe ByBrokerId() =>
            _sync is not null ? FutureProbe.Of(_sync.ByBrokerId()) : FutureProbe.Of(_async!.ByBrokerId());

        internal FutureProbe AllByBrokerId() =>
            _sync is not null ? FutureProbe.Of(_sync.AllByBrokerId()) : FutureProbe.Of(_async!.AllByBrokerId());

        internal FutureProbe All() => _sync is not null ? FutureProbe.Of(_sync.All()) : FutureProbe.Of(_async!.All());

        internal IReadOnlyList<int> BrokerIdsOf(object? byBrokerId) =>
            _sync is not null ? SyncMap(byBrokerId).Keys.ToList() : AsyncMap(byBrokerId).Keys.ToList();

        internal FutureProbe BrokerOf(object? byBrokerId, int brokerId) =>
            _sync is not null
                ? FutureProbe.Of(SyncMap(byBrokerId)[brokerId])
                : FutureProbe.Of(AsyncMap(byBrokerId)[brokerId]);

        internal bool SameBrokerFuture(object? first, object? second, int brokerId) =>
            _sync is not null
                ? SyncMap(first)[brokerId] == SyncMap(second)[brokerId]
                : AsyncMap(first)[brokerId] == AsyncMap(second)[brokerId];

        internal bool ViewsFreshPerCall() =>
            _sync is not null
                ? _sync.ByBrokerId() != _sync.ByBrokerId()
                    && _sync.AllByBrokerId() != _sync.AllByBrokerId()
                    && _sync.All() != _sync.All()
                : _async!.ByBrokerId() != _async.ByBrokerId()
                    && _async.AllByBrokerId() != _async.AllByBrokerId()
                    && _async.All() != _async.All();

        private static IReadOnlyDictionary<int, KafkaFuture<IReadOnlyCollection<TransactionListing>>> SyncMap(object? map) =>
            Assert.IsAssignableFrom<IReadOnlyDictionary<int, KafkaFuture<IReadOnlyCollection<TransactionListing>>>>(map);

        private static IReadOnlyDictionary<int, AsyncKafkaFuture<IReadOnlyCollection<TransactionListing>>> AsyncMap(object? map) =>
            Assert.IsAssignableFrom<IReadOnlyDictionary<int, AsyncKafkaFuture<IReadOnlyCollection<TransactionListing>>>>(map);
    }

    /// <summary>
    /// <see cref="AdminP14SyncCompositionTests.DescribeCluster_SyncAndAsyncResults_ComposeAlike"/>'s flavour adapter.
    /// </summary>
    private sealed class DescribeClusterHarness
    {
        private readonly KafkaFutureImpl<IReadOnlyCollection<Node>> _nodes = new KafkaFutureImpl<IReadOnlyCollection<Node>>();
        private readonly KafkaFutureImpl<Node?> _controller = new KafkaFutureImpl<Node?>();
        private readonly KafkaFutureImpl<string> _clusterId = new KafkaFutureImpl<string>();
        private readonly KafkaFutureImpl<IReadOnlyCollection<AclOperation>?> _authorizedOperations =
            new KafkaFutureImpl<IReadOnlyCollection<AclOperation>?>();

        private readonly DescribeClusterResult? _sync;
        private readonly AsyncDescribeClusterResult? _async;

        private DescribeClusterHarness(AdminFlavour flavour)
        {
            if (flavour == AdminFlavour.Sync)
            {
                _sync = new DescribeClusterResult(
                    new KafkaFuture<IReadOnlyCollection<Node>>(_nodes),
                    new KafkaFuture<Node?>(_controller),
                    new KafkaFuture<string>(_clusterId),
                    new KafkaFuture<IReadOnlyCollection<AclOperation>?>(_authorizedOperations));
            }
            else
            {
                _async = new AsyncDescribeClusterResult(
                    new AsyncKafkaFuture<IReadOnlyCollection<Node>>(_nodes),
                    new AsyncKafkaFuture<Node?>(_controller),
                    new AsyncKafkaFuture<string>(_clusterId),
                    new AsyncKafkaFuture<IReadOnlyCollection<AclOperation>?>(_authorizedOperations));
            }
        }

        internal static DescribeClusterHarness For(string flavour) => new DescribeClusterHarness(FlavourOf(flavour));

        internal void SetResult(
            IReadOnlyCollection<Node> nodes,
            Node? controller,
            string clusterId,
            IReadOnlyCollection<AclOperation>? authorizedOperations)
        {
            _nodes.Complete(nodes);
            _controller.Complete(controller);
            _clusterId.Complete(clusterId);
            _authorizedOperations.Complete(authorizedOperations);
        }

        internal void SetException(Exception failure)
        {
            _nodes.CompleteWithError(failure);
            _controller.CompleteWithError(failure);
            _clusterId.CompleteWithError(failure);
            _authorizedOperations.CompleteWithError(failure);
        }

        internal FutureProbe[] Accessors() =>
            _sync is not null
                ? new[]
                {
                    FutureProbe.Of(_sync.Nodes()),
                    FutureProbe.Of(_sync.Controller()),
                    FutureProbe.Of(_sync.ClusterId()),
                    FutureProbe.Of(_sync.AuthorizedOperations()),
                }
                : new[]
                {
                    FutureProbe.Of(_async!.Nodes()),
                    FutureProbe.Of(_async.Controller()),
                    FutureProbe.Of(_async.ClusterId()),
                    FutureProbe.Of(_async.AuthorizedOperations()),
                };

        internal bool AccessorsEqualAcrossCalls() =>
            _sync is not null
                ? _sync.Nodes() == _sync.Nodes()
                    && _sync.Controller() == _sync.Controller()
                    && _sync.ClusterId() == _sync.ClusterId()
                    && _sync.AuthorizedOperations() == _sync.AuthorizedOperations()
                : _async!.Nodes() == _async.Nodes()
                    && _async.Controller() == _async.Controller()
                    && _async.ClusterId() == _async.ClusterId()
                    && _async.AuthorizedOperations() == _async.AuthorizedOperations();
    }

    /// <summary>
    /// <see cref="AdminP14SyncCompositionTests.ListConfigResources_SyncAndAsyncResults_ComposeAlike"/>'s flavour adapter.
    /// </summary>
    private sealed class ListConfigResourcesHarness
    {
        private readonly ListConfigResourcesResult? _sync;
        private readonly AsyncListConfigResourcesResult? _async;

        private ListConfigResourcesHarness(
            KafkaFutureImpl<IReadOnlyCollection<ConfigResource>> impl,
            ListConfigResourcesResult? sync,
            AsyncListConfigResourcesResult? async)
        {
            Impl = impl;
            _sync = sync;
            _async = async;
        }

        internal KafkaFutureImpl<IReadOnlyCollection<ConfigResource>> Impl { get; }

        internal static ListConfigResourcesHarness For(string flavour)
        {
            KafkaFutureImpl<IReadOnlyCollection<ConfigResource>> impl = new KafkaFutureImpl<IReadOnlyCollection<ConfigResource>>();
            return FlavourOf(flavour) == AdminFlavour.Sync
                ? new ListConfigResourcesHarness(impl, ListConfigResourcesResult.OfClient((impl, NoResultHandle())), null)
                : new ListConfigResourcesHarness(impl, null, AsyncListConfigResourcesResult.OfClient((impl, NoResultHandle())));
        }

        internal FutureProbe All() => _sync is not null ? FutureProbe.Of(_sync.All()) : FutureProbe.Of(_async!.All());

        internal (bool Fresh, FutureProbe First, FutureProbe Second) TwoAlls()
        {
            if (_sync is not null)
            {
                KafkaFuture<IReadOnlyCollection<ConfigResource>> first = _sync.All();
                KafkaFuture<IReadOnlyCollection<ConfigResource>> second = _sync.All();
                return (first != second, FutureProbe.Of(first), FutureProbe.Of(second));
            }

            AsyncKafkaFuture<IReadOnlyCollection<ConfigResource>> asyncFirst = _async!.All();
            AsyncKafkaFuture<IReadOnlyCollection<ConfigResource>> asyncSecond = _async.All();
            return (asyncFirst != asyncSecond, FutureProbe.Of(asyncFirst), FutureProbe.Of(asyncSecond));
        }
    }

    /// <summary>
    /// <see cref="AdminP14SyncCompositionTests.ElectLeaders_SyncAndAsyncResults_ComposeAlike"/>'s flavour adapter.
    /// </summary>
    private sealed class ElectLeadersHarness
    {
        private readonly ElectLeadersResult? _sync;
        private readonly AsyncElectLeadersResult? _async;

        private ElectLeadersHarness(
            KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, KafkaException?>> impl,
            ElectLeadersResult? sync,
            AsyncElectLeadersResult? async)
        {
            Impl = impl;
            _sync = sync;
            _async = async;
        }

        internal KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, KafkaException?>> Impl { get; }

        internal static ElectLeadersHarness For(string flavour)
        {
            KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, KafkaException?>> impl =
                new KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, KafkaException?>>();
            return FlavourOf(flavour) == AdminFlavour.Sync
                ? new ElectLeadersHarness(impl, ElectLeadersResult.OfClient((impl, NoResultHandle())), null)
                : new ElectLeadersHarness(impl, null, AsyncElectLeadersResult.OfClient((impl, NoResultHandle())));
        }

        internal FutureProbe Partitions() =>
            _sync is not null ? FutureProbe.Of(_sync.Partitions()) : FutureProbe.Of(_async!.Partitions());

        internal FutureProbe All() => _sync is not null ? FutureProbe.Of(_sync.All()) : FutureProbe.Of(_async!.All());

        internal bool PartitionsEqualAcrossCalls() =>
            _sync is not null ? _sync.Partitions() == _sync.Partitions() : _async!.Partitions() == _async.Partitions();

        internal (bool Fresh, FutureProbe First, FutureProbe Second) TwoAlls()
        {
            if (_sync is not null)
            {
                KafkaFuture first = _sync.All();
                KafkaFuture second = _sync.All();
                return (first != second, FutureProbe.Of(first), FutureProbe.Of(second));
            }

            AsyncKafkaFuture asyncFirst = _async!.All();
            AsyncKafkaFuture asyncSecond = _async.All();
            return (asyncFirst != asyncSecond, FutureProbe.Of(asyncFirst), FutureProbe.Of(asyncSecond));
        }
    }

    /// <summary>
    /// <see cref="AdminP14SyncCompositionTests.ListPartitionReassignments_SyncAndAsyncResults_ComposeAlike"/>'s flavour
    /// adapter.
    /// </summary>
    private sealed class ListPartitionReassignmentsHarness
    {
        private readonly ListPartitionReassignmentsResult? _sync;
        private readonly AsyncListPartitionReassignmentsResult? _async;

        private ListPartitionReassignmentsHarness(
            KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, PartitionReassignment>> impl,
            ListPartitionReassignmentsResult? sync,
            AsyncListPartitionReassignmentsResult? async)
        {
            Impl = impl;
            _sync = sync;
            _async = async;
        }

        internal KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, PartitionReassignment>> Impl { get; }

        internal static ListPartitionReassignmentsHarness For(string flavour)
        {
            KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, PartitionReassignment>> impl =
                new KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, PartitionReassignment>>();
            return FlavourOf(flavour) == AdminFlavour.Sync
                ? new ListPartitionReassignmentsHarness(impl, ListPartitionReassignmentsResult.OfClient((impl, NoResultHandle())), null)
                : new ListPartitionReassignmentsHarness(
                    impl, null, AsyncListPartitionReassignmentsResult.OfClient((impl, NoResultHandle())));
        }

        internal FutureProbe Reassignments() =>
            _sync is not null ? FutureProbe.Of(_sync.Reassignments()) : FutureProbe.Of(_async!.Reassignments());

        internal bool ReassignmentsEqualAcrossCalls() =>
            _sync is not null
                ? _sync.Reassignments() == _sync.Reassignments()
                : _async!.Reassignments() == _async.Reassignments();
    }

    /// <summary>
    /// <see cref="AdminP14SyncCompositionTests.ListOffsets_SyncAndAsyncResults_ComposeAlike"/>'s flavour adapter: a
    /// keyed Result with no per-key dictionary, so its per-key member is <c>PartitionResult</c>, and the keys it answers
    /// for are those <c>PartitionResult</c> does not reject.
    /// </summary>
    private sealed class ListOffsetsHarness : KeyedHarness<TopicPartition>
    {
        private readonly IReadOnlyDictionary<TopicPartition, KafkaFutureImpl<ListOffsetsResult.ListOffsetsResultInfo>> _futures;
        private readonly Dictionary<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> _values;
        private readonly ListOffsetsResult? _sync;
        private readonly AsyncListOffsetsResult? _async;

        private ListOffsetsHarness(
            IReadOnlyDictionary<TopicPartition, KafkaFutureImpl<ListOffsetsResult.ListOffsetsResultInfo>> futures,
            IReadOnlyList<TopicPartition> partitions,
            ListOffsetsResult? sync,
            AsyncListOffsetsResult? async)
            : base(partitions, true)
        {
            _futures = futures;
            _values = partitions.ToDictionary(
                partition => partition,
                partition => new ListOffsetsResult.ListOffsetsResultInfo(100L + partition.Partition, -1L, null));
            _sync = sync;
            _async = async;
        }

        internal static ListOffsetsHarness For(string flavour, IReadOnlyList<TopicPartition> partitions)
        {
            Dictionary<TopicPartition, KafkaFutureImpl<ListOffsetsResult.ListOffsetsResultInfo>> futures =
                Impls<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>(partitions, EqualityComparer<TopicPartition>.Default);
            return FlavourOf(flavour) == AdminFlavour.Sync
                ? new ListOffsetsHarness(futures, partitions, ListOffsetsResult.OfClient((futures, NoResultHandle())), null)
                : new ListOffsetsHarness(futures, partitions, null, AsyncListOffsetsResult.OfClient((futures, NoResultHandle())));
        }

        internal override void Succeed(TopicPartition key) => _futures[key].Complete(_values[key]);

        internal override void Fail(TopicPartition key, Exception failure) => _futures[key].CompleteWithError(failure);

        internal override object? Expected(TopicPartition key) => _values[key];

        internal override IEnumerable<TopicPartition> ValuesKeys() => Keys.Where(Answers).ToList();

        internal override FutureProbe Value(TopicPartition key) =>
            _sync is not null ? FutureProbe.Of(_sync.PartitionResult(key)) : FutureProbe.Of(_async!.PartitionResult(key));

        internal override FutureProbe All() => _sync is not null ? FutureProbe.Of(_sync.All()) : FutureProbe.Of(_async!.All());

        internal override void AssertAllSucceeded(object? outcome)
        {
            // The gathered map: every key, in key order, each value the very instance its key completed with.
            IReadOnlyDictionary<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> gathered =
                Assert.IsAssignableFrom<IReadOnlyDictionary<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>>(outcome);
            Assert.Equal(Keys, gathered.Keys);
            foreach (TopicPartition key in Keys)
            {
                Assert.Same(_values[key], gathered[key]);
            }
        }

        internal override bool ValueEqualAcrossReads(TopicPartition key) =>
            _sync is not null
                ? _sync.PartitionResult(key) == _sync.PartitionResult(key)
                : _async!.PartitionResult(key) == _async.PartitionResult(key);

        internal override bool TryGetValue(TopicPartition key) => Answers(key);

        internal override bool ContainsKey(TopicPartition key) => Answers(key);

        private bool Answers(TopicPartition key) => CallIndexer(key) is null;
    }

    /// <summary>
    /// <see cref="AdminP14SyncCompositionTests.ListGroups_SyncAndAsyncResults_ComposeAlike"/>'s flavour adapter.
    /// </summary>
    private sealed class ListGroupsHarness
    {
        private readonly ListGroupsResult? _sync;
        private readonly AsyncListGroupsResult? _async;

        private ListGroupsHarness(
            KafkaFutureImpl<IReadOnlyCollection<GroupListing>> all,
            KafkaFutureImpl<IReadOnlyCollection<GroupListing>> valid,
            KafkaFutureImpl<IReadOnlyCollection<KafkaException>> errors,
            ListGroupsResult? sync,
            AsyncListGroupsResult? async)
        {
            AllFuture = all;
            ValidFuture = valid;
            ErrorsFuture = errors;
            _sync = sync;
            _async = async;
        }

        internal KafkaFutureImpl<IReadOnlyCollection<GroupListing>> AllFuture { get; }

        internal KafkaFutureImpl<IReadOnlyCollection<GroupListing>> ValidFuture { get; }

        internal KafkaFutureImpl<IReadOnlyCollection<KafkaException>> ErrorsFuture { get; }

        internal static ListGroupsHarness For(string flavour)
        {
            KafkaFutureImpl<IReadOnlyCollection<GroupListing>> all = new KafkaFutureImpl<IReadOnlyCollection<GroupListing>>();
            KafkaFutureImpl<IReadOnlyCollection<GroupListing>> valid = new KafkaFutureImpl<IReadOnlyCollection<GroupListing>>();
            KafkaFutureImpl<IReadOnlyCollection<KafkaException>> errors = new KafkaFutureImpl<IReadOnlyCollection<KafkaException>>();
            return FlavourOf(flavour) == AdminFlavour.Sync
                ? new ListGroupsHarness(all, valid, errors, ListGroupsResult.OfClient((all, valid, errors, NoResultHandle())), null)
                : new ListGroupsHarness(all, valid, errors, null, AsyncListGroupsResult.OfClient((all, valid, errors, NoResultHandle())));
        }

        internal FutureProbe All() => _sync is not null ? FutureProbe.Of(_sync.All()) : FutureProbe.Of(_async!.All());

        internal FutureProbe Valid() => _sync is not null ? FutureProbe.Of(_sync.Valid()) : FutureProbe.Of(_async!.Valid());

        internal FutureProbe Errors() => _sync is not null ? FutureProbe.Of(_sync.Errors()) : FutureProbe.Of(_async!.Errors());

        internal bool SameFuturePerCall() =>
            _sync is not null
                ? _sync.All() == _sync.All() && _sync.Valid() == _sync.Valid() && _sync.Errors() == _sync.Errors()
                : _async!.All() == _async.All() && _async.Valid() == _async.Valid() && _async.Errors() == _async.Errors();
    }

    /// <summary>
    /// <see cref="AdminP14SyncCompositionTests.ListConsumerGroupOffsets_SyncAndAsyncResults_ComposeAlike"/>'s flavour
    /// adapter: a keyed Result with no per-key dictionary, so its per-key member is
    /// <c>PartitionsToOffsetAndMetadata(groupId)</c>, and the keys it answers for are those that member does not reject.
    /// </summary>
    private sealed class ListConsumerGroupOffsetsHarness : KeyedHarness<string>
    {
        private readonly Dictionary<string, KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> _futures;
        private readonly Dictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> _values;
        private readonly ListConsumerGroupOffsetsResult? _sync;
        private readonly AsyncListConsumerGroupOffsetsResult? _async;

        private ListConsumerGroupOffsetsHarness(
            Dictionary<string, KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> futures,
            IReadOnlyList<string> groups,
            ListConsumerGroupOffsetsResult? sync,
            AsyncListConsumerGroupOffsetsResult? async)
            : base(groups, true)
        {
            _futures = futures;

            // A distinct map per group, each with an uncommitted (null) partition alongside a committed one.
            _values = groups.ToDictionary(
                group => group,
                group => (IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>)new Dictionary<TopicPartition, OffsetAndMetadata?>
                {
                    [new TopicPartition("alpha", 0)] = new OffsetAndMetadata(7L, group),
                    [new TopicPartition("alpha", 1)] = null,
                },
                StringComparer.Ordinal);
            _sync = sync;
            _async = async;
        }

        internal static ListConsumerGroupOffsetsHarness For(string flavour, IReadOnlyList<string> groups)
        {
            Dictionary<string, KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>> futures =
                new Dictionary<string, KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>>(groups.Count, StringComparer.Ordinal);
            foreach (string group in groups)
            {
                futures[group] = new KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>();
            }

            return FlavourOf(flavour) == AdminFlavour.Sync
                ? new ListConsumerGroupOffsetsHarness(futures, groups, ListConsumerGroupOffsetsResult.OfClient((futures, NoResultHandle())), null)
                : new ListConsumerGroupOffsetsHarness(futures, groups, null, AsyncListConsumerGroupOffsetsResult.OfClient((futures, NoResultHandle())));
        }

        internal override void Succeed(string key) => _futures[key].Complete(_values[key]);

        internal override void Fail(string key, Exception failure) => _futures[key].CompleteWithError(failure);

        internal override object? Expected(string key) => _values[key];

        internal override IEnumerable<string> ValuesKeys() => Keys.Where(Answers).ToList();

        internal override FutureProbe Value(string key) =>
            _sync is not null
                ? FutureProbe.Of(_sync.PartitionsToOffsetAndMetadata(key))
                : FutureProbe.Of(_async!.PartitionsToOffsetAndMetadata(key));

        internal override FutureProbe All() => _sync is not null ? FutureProbe.Of(_sync.All()) : FutureProbe.Of(_async!.All());

        internal override void AssertAllSucceeded(object? outcome)
        {
            // The gathered map: every group, in key order, each value the very instance its group completed with.
            IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> gathered =
                Assert.IsAssignableFrom<IReadOnlyDictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>>(outcome);
            Assert.Equal(Keys, gathered.Keys);
            foreach (string key in Keys)
            {
                Assert.Same(_values[key], gathered[key]);
            }
        }

        internal override bool ValueEqualAcrossReads(string key) =>
            _sync is not null
                ? _sync.PartitionsToOffsetAndMetadata(key) == _sync.PartitionsToOffsetAndMetadata(key)
                : _async!.PartitionsToOffsetAndMetadata(key) == _async.PartitionsToOffsetAndMetadata(key);

        internal override bool TryGetValue(string key) => Answers(key);

        internal override bool ContainsKey(string key) => Answers(key);

        internal FutureProbe NoArgument() =>
            _sync is not null
                ? FutureProbe.Of(_sync.PartitionsToOffsetAndMetadata())
                : FutureProbe.Of(_async!.PartitionsToOffsetAndMetadata());

        internal Exception? CallNoArgument() => Thrown(() => _ = NoArgument());

        internal bool NoArgumentIsTheGroupsOwn(string key) =>
            _sync is not null
                ? _sync.PartitionsToOffsetAndMetadata() == _sync.PartitionsToOffsetAndMetadata(key)
                : _async!.PartitionsToOffsetAndMetadata() == _async.PartitionsToOffsetAndMetadata(key);

        private bool Answers(string key) => CallIndexer(key) is null;
    }

    /// <summary>
    /// <see cref="AdminP14SyncCompositionTests.DeleteAcls_SyncAndAsyncResults_ComposeAlike"/>'s flavour adapter
    /// the per-filter futures, and an <c>All()</c> that flattens every filter's matched bindings in
    /// filter order. A succeeded filter matched two bindings unless the row hands it its own results.
    /// </summary>
    private sealed class DeleteAclsHarness : KeyedHarness<AclBindingFilter>
    {
        private readonly Dictionary<AclBindingFilter, KafkaFutureImpl<DeleteAclsResult.FilterResults>> _futures;
        private readonly Dictionary<AclBindingFilter, DeleteAclsResult.FilterResults> _values;
        private readonly DeleteAclsResult? _sync;
        private readonly AsyncDeleteAclsResult? _async;

        private DeleteAclsHarness(
            Dictionary<AclBindingFilter, KafkaFutureImpl<DeleteAclsResult.FilterResults>> futures,
            IReadOnlyList<AclBindingFilter> keys,
            DeleteAclsResult? sync,
            AsyncDeleteAclsResult? async)
            : base(keys, otherSideIsNull: true)
        {
            _futures = futures;
            _values = new Dictionary<AclBindingFilter, DeleteAclsResult.FilterResults>();
            for (int index = 0; index < keys.Count; index++)
            {
                _values.Add(
                    keys[index],
                    Matched(
                        Deleted(Binding(ResourceType.Topic, s_topics[index], "User:alice")),
                        Deleted(Binding(ResourceType.Topic, s_topics[index], "User:bob"))));
            }

            _sync = sync;
            _async = async;
        }

        internal static DeleteAclsHarness For(
            string flavour, IReadOnlyList<AclBindingFilter> requested, IReadOnlyList<AclBindingFilter> keys)
        {
            Dictionary<AclBindingFilter, KafkaFutureImpl<DeleteAclsResult.FilterResults>> futures =
                new Dictionary<AclBindingFilter, KafkaFutureImpl<DeleteAclsResult.FilterResults>>(requested.Count);
            foreach (AclBindingFilter filter in requested)
            {
                futures.Add(filter, new KafkaFutureImpl<DeleteAclsResult.FilterResults>());
            }

            return FlavourOf(flavour) == AdminFlavour.Sync
                ? new DeleteAclsHarness(futures, keys, DeleteAclsResult.OfClient((futures, NoResultHandle())), null)
                : new DeleteAclsHarness(futures, keys, null, AsyncDeleteAclsResult.OfClient((futures, NoResultHandle())));
        }

        internal AclBinding[] Bindings(AclBindingFilter key) => _values[key].Values.Select(result => result.Binding!).ToArray();

        internal void Succeed(AclBindingFilter key, DeleteAclsResult.FilterResults results) => _futures[key].Complete(results);

        internal override void Succeed(AclBindingFilter key) => _futures[key].Complete(_values[key]);

        internal override void Fail(AclBindingFilter key, Exception failure) => _futures[key].CompleteWithError(failure);

        internal override object? Expected(AclBindingFilter key) => _values[key];

        internal override IEnumerable<AclBindingFilter> ValuesKeys() =>
            _sync is not null ? _sync.Values.Keys.ToList() : _async!.Values.Keys.ToList();

        internal override FutureProbe Value(AclBindingFilter key) =>
            _sync is not null ? FutureProbe.Of(_sync.Values[key]) : FutureProbe.Of(_async!.Values[key]);

        internal override FutureProbe All() => _sync is not null ? FutureProbe.Of(_sync.All()) : FutureProbe.Of(_async!.All());

        internal override void AssertAllSucceeded(object? outcome)
        {
            // Every filter's matched bindings, flattened in filter order, each the very instance its filter completed with.
            AclBinding[] expected = Keys.SelectMany(Bindings).ToArray();
            AclBinding[] flattened = Assert.IsAssignableFrom<IReadOnlyCollection<AclBinding>>(outcome).ToArray();
            Assert.Equal(expected.Length, flattened.Length);
            for (int index = 0; index < expected.Length; index++)
            {
                Assert.Same(expected[index], flattened[index]);
            }
        }

        internal override bool ValueEqualAcrossReads(AclBindingFilter key) =>
            _sync is not null ? _sync.Values[key] == _sync.Values[key] : _async!.Values[key] == _async.Values[key];

        internal override bool TryGetValue(AclBindingFilter key) =>
            _sync is not null ? _sync.Values.TryGetValue(key, out _) : _async!.Values.TryGetValue(key, out _);

        internal override bool ContainsKey(AclBindingFilter key) =>
            _sync is not null ? _sync.Values.ContainsKey(key) : _async!.Values.ContainsKey(key);
    }

    /// <summary>
    /// A Result over one future and no keys, of either flavour — <see cref="DescribeAclsResult"/>'s
    /// <c>Values()</c>, <see cref="DescribeClientQuotasResult"/>'s <c>Entities()</c>, or the single accessor of a
    /// delegation-token or <c>DescribeFeatures</c> Result — driven through its <c>KafkaFutureImpl</c>.
    /// <typeparamref name="T"/> may be a value type (the expiry timestamps). A void Result (<c>AbortTransaction</c>,
    /// <c>ForceTerminateTransaction</c>, <c>UnregisterController</c>) is a <see cref="SingleHarness{T}"/> of
    /// <see langword="object"/> with <see cref="IsVoid"/> set.
    /// </summary>
    private sealed class SingleHarness<T>
    {
        private readonly Action<T> _setResult;
        private readonly Action<Exception> _setException;
        private readonly Func<FutureProbe> _future;
        private readonly Func<bool> _equalAcrossCalls;

        private SingleHarness(
            KafkaFutureImpl<T> impl, Func<FutureProbe> future, Func<bool> equalAcrossCalls, bool isVoid = false)
            : this(value => impl.Complete(value), failure => impl.CompleteWithError(failure), future, equalAcrossCalls, isVoid)
        {
        }

        private SingleHarness(
            Action<T> setResult, Action<Exception> setException, Func<FutureProbe> future, Func<bool> equalAcrossCalls, bool isVoid)
        {
            _setResult = setResult;
            _setException = setException;
            _future = future;
            _equalAcrossCalls = equalAcrossCalls;
            IsVoid = isVoid;
        }

        internal bool IsVoid { get; }

        internal static SingleHarness<object?> AbortTransaction(string flavour)
        {
            KafkaFutureImpl<object?> impl = new KafkaFutureImpl<object?>();
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                AbortTransactionResult result = AbortTransactionResult.OfClient((impl, NoResultHandle()));
                return new SingleHarness<object?>(
                    impl, () => FutureProbe.Of(result.All()), () => result.All() == result.All(), isVoid: true);
            }

            AsyncAbortTransactionResult asyncResult = AsyncAbortTransactionResult.OfClient((impl, NoResultHandle()));
            return new SingleHarness<object?>(
                impl, () => FutureProbe.Of(asyncResult.All()), () => asyncResult.All() == asyncResult.All(), isVoid: true);
        }

        internal static SingleHarness<object?> ForceTerminateTransaction(string flavour)
        {
            KafkaFutureImpl<object?> impl = new KafkaFutureImpl<object?>();
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                TerminateTransactionResult result = TerminateTransactionResult.OfClient((impl, NoResultHandle()));
                return new SingleHarness<object?>(
                    impl, () => FutureProbe.Of(result.Result()), () => result.Result() == result.Result(), isVoid: true);
            }

            AsyncTerminateTransactionResult asyncResult = AsyncTerminateTransactionResult.OfClient((impl, NoResultHandle()));
            return new SingleHarness<object?>(
                impl, () => FutureProbe.Of(asyncResult.Result()), () => asyncResult.Result() == asyncResult.Result(), isVoid: true);
        }

        internal static SingleHarness<IReadOnlyCollection<AclBinding>> DescribeAcls(string flavour)
        {
            KafkaFutureImpl<IReadOnlyCollection<AclBinding>> impl = new KafkaFutureImpl<IReadOnlyCollection<AclBinding>>();
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                DescribeAclsResult result = DescribeAclsResult.OfClient((impl, NoResultHandle()));
                return new SingleHarness<IReadOnlyCollection<AclBinding>>(
                    impl, () => FutureProbe.Of(result.Values()), () => result.Values() == result.Values());
            }

            AsyncDescribeAclsResult asyncResult = AsyncDescribeAclsResult.OfClient((impl, NoResultHandle()));
            return new SingleHarness<IReadOnlyCollection<AclBinding>>(
                impl, () => FutureProbe.Of(asyncResult.Values()), () => asyncResult.Values() == asyncResult.Values());
        }

        internal static SingleHarness<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>
            DescribeClientQuotas(string flavour)
        {
            KafkaFutureImpl<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> impl =
                new KafkaFutureImpl<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>();
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                DescribeClientQuotasResult result = DescribeClientQuotasResult.OfClient((impl, NoResultHandle()));
                return new SingleHarness<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>(
                    impl, () => FutureProbe.Of(result.Entities()), () => result.Entities() == result.Entities());
            }

            AsyncDescribeClientQuotasResult asyncResult = AsyncDescribeClientQuotasResult.OfClient((impl, NoResultHandle()));
            return new SingleHarness<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>(
                impl,
                () => FutureProbe.Of(asyncResult.Entities()),
                () => asyncResult.Entities() == asyncResult.Entities());
        }

        internal static SingleHarness<DelegationToken> CreateDelegationToken(string flavour)
        {
            KafkaFutureImpl<DelegationToken> impl = new KafkaFutureImpl<DelegationToken>();
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                CreateDelegationTokenResult result = CreateDelegationTokenResult.OfClient((impl, NoResultHandle()));
                return new SingleHarness<DelegationToken>(
                    impl, () => FutureProbe.Of(result.DelegationToken()), () => result.DelegationToken() == result.DelegationToken());
            }

            AsyncCreateDelegationTokenResult asyncResult = AsyncCreateDelegationTokenResult.OfClient((impl, NoResultHandle()));
            return new SingleHarness<DelegationToken>(
                impl,
                () => FutureProbe.Of(asyncResult.DelegationToken()),
                () => asyncResult.DelegationToken() == asyncResult.DelegationToken());
        }

        internal static SingleHarness<long> RenewDelegationToken(string flavour)
        {
            KafkaFutureImpl<long> impl = new KafkaFutureImpl<long>();
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                RenewDelegationTokenResult result = RenewDelegationTokenResult.OfClient((impl, NoResultHandle()));
                return new SingleHarness<long>(
                    impl, () => FutureProbe.Of(result.ExpiryTimestamp()), () => result.ExpiryTimestamp() == result.ExpiryTimestamp());
            }

            AsyncRenewDelegationTokenResult asyncResult = AsyncRenewDelegationTokenResult.OfClient((impl, NoResultHandle()));
            return new SingleHarness<long>(
                impl,
                () => FutureProbe.Of(asyncResult.ExpiryTimestamp()),
                () => asyncResult.ExpiryTimestamp() == asyncResult.ExpiryTimestamp());
        }

        internal static SingleHarness<long> ExpireDelegationToken(string flavour)
        {
            KafkaFutureImpl<long> impl = new KafkaFutureImpl<long>();
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                ExpireDelegationTokenResult result = ExpireDelegationTokenResult.OfClient((impl, NoResultHandle()));
                return new SingleHarness<long>(
                    impl, () => FutureProbe.Of(result.ExpiryTimestamp()), () => result.ExpiryTimestamp() == result.ExpiryTimestamp());
            }

            AsyncExpireDelegationTokenResult asyncResult = AsyncExpireDelegationTokenResult.OfClient((impl, NoResultHandle()));
            return new SingleHarness<long>(
                impl,
                () => FutureProbe.Of(asyncResult.ExpiryTimestamp()),
                () => asyncResult.ExpiryTimestamp() == asyncResult.ExpiryTimestamp());
        }

        internal static SingleHarness<IReadOnlyList<DelegationToken>> DescribeDelegationToken(string flavour)
        {
            KafkaFutureImpl<IReadOnlyList<DelegationToken>> impl = new KafkaFutureImpl<IReadOnlyList<DelegationToken>>();
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                DescribeDelegationTokenResult result = DescribeDelegationTokenResult.OfClient((impl, NoResultHandle()));
                return new SingleHarness<IReadOnlyList<DelegationToken>>(
                    impl,
                    () => FutureProbe.Of(result.DelegationTokens()),
                    () => result.DelegationTokens() == result.DelegationTokens());
            }

            AsyncDescribeDelegationTokenResult asyncResult = AsyncDescribeDelegationTokenResult.OfClient((impl, NoResultHandle()));
            return new SingleHarness<IReadOnlyList<DelegationToken>>(
                impl,
                () => FutureProbe.Of(asyncResult.DelegationTokens()),
                () => asyncResult.DelegationTokens() == asyncResult.DelegationTokens());
        }

        internal static SingleHarness<FeatureMetadata> DescribeFeatures(string flavour)
        {
            KafkaFutureImpl<FeatureMetadata> impl = new KafkaFutureImpl<FeatureMetadata>();
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                DescribeFeaturesResult result = DescribeFeaturesResult.OfClient((impl, NoResultHandle()));
                return new SingleHarness<FeatureMetadata>(
                    impl, () => FutureProbe.Of(result.FeatureMetadata()), () => result.FeatureMetadata() == result.FeatureMetadata());
            }

            AsyncDescribeFeaturesResult asyncResult = AsyncDescribeFeaturesResult.OfClient((impl, NoResultHandle()));
            return new SingleHarness<FeatureMetadata>(
                impl,
                () => FutureProbe.Of(asyncResult.FeatureMetadata()),
                () => asyncResult.FeatureMetadata() == asyncResult.FeatureMetadata());
        }

        internal static SingleHarness<object?> UnregisterController(string flavour)
        {
            KafkaFutureImpl<object?> impl = new KafkaFutureImpl<object?>();
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                UnregisterControllerResult result = UnregisterControllerResult.OfClient((impl, NoResultHandle()));
                return new SingleHarness<object?>(
                    impl, () => FutureProbe.Of(result.All()), () => result.All() == result.All(), isVoid: true);
            }

            AsyncUnregisterControllerResult asyncResult = AsyncUnregisterControllerResult.OfClient((impl, NoResultHandle()));
            return new SingleHarness<object?>(
                impl, () => FutureProbe.Of(asyncResult.All()), () => asyncResult.All() == asyncResult.All(), isVoid: true);
        }


        internal void SetResult(T value) => _setResult(value);

        internal void SetException(Exception failure) => _setException(failure);

        internal FutureProbe Future() => _future();

        internal bool EqualAcrossCalls() => _equalAcrossCalls();
    }

    /// <summary>
    /// A <c>DescribeUserScramCredentials</c> Result of either flavour from a mock admin client, which it disposes —
    /// <c>All()</c>, <c>Users()</c> and <c>Description</c>.
    /// </summary>
    private sealed class DescribeUserScramCredentialsHarness : IDisposable
    {
        private readonly IDisposable _admin;
        private readonly Func<FutureProbe> _all;
        private readonly Func<FutureProbe> _users;
        private readonly Func<string, FutureProbe> _description;

        private DescribeUserScramCredentialsHarness(
            IDisposable admin,
            Func<FutureProbe> all,
            Func<FutureProbe> users,
            Func<string, FutureProbe> description)
        {
            _admin = admin;
            _all = all;
            _users = users;
            _description = description;
        }

        internal static DescribeUserScramCredentialsHarness Create(string flavour, IReadOnlyCollection<string> users)
        {
            if (FlavourOf(flavour) == AdminFlavour.Sync)
            {
                MockAdminClient admin = MockAdminClient.Create().Build();
                DescribeUserScramCredentialsResult result = admin.DescribeUserScramCredentials(users);
                return new DescribeUserScramCredentialsHarness(
                    admin,
                    () => FutureProbe.Of(result.All()),
                    () => FutureProbe.Of(result.Users()),
                    user => FutureProbe.Of(result.Description(user)));
            }

            AsyncMockAdminClient asyncAdmin = AsyncMockAdminClient.Create().Build();
            AsyncDescribeUserScramCredentialsResult asyncResult = asyncAdmin.DescribeUserScramCredentials(users);
            return new DescribeUserScramCredentialsHarness(
                asyncAdmin,
                () => FutureProbe.Of(asyncResult.All()),
                () => FutureProbe.Of(asyncResult.Users()),
                user => FutureProbe.Of(asyncResult.Description(user)));
        }

        public void Dispose() => _admin.Dispose();

        internal FutureProbe All() => _all();

        internal FutureProbe Users() => _users();

        internal FutureProbe Description(string userName) => _description(userName);
    }
}
