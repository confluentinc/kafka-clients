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
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The both-flavour composition theories of M15/P14 S4: each sync Result and its async twin, built by their
/// <b>production</b> <c>From</c> over an operation of their own flavour, driven identically through the
/// operation's own <c>SetResult</c> / <c>SetException</c> (the calls the production trampolines make), give the
/// same outcome on every row — the same value, or the same exception instance.
/// </summary>
/// <remarks>
/// <para>
/// One <see cref="TheoryAttribute"/> per Result pair, each row run on both flavours. The rows: all keys succeed;
/// one fails; two fail (the aggregate rethrows the first in <b>key</b> order, not completion order); all fail; an
/// unknown key; a null key; per-key independence (one key's outcome does not wait for another's, and the aggregate
/// waits for every key); memoization (a projection or gathered aggregate is computed once per future, so its value or
/// failure is the same instance on every <c>Get()</c>); and erasure identity (a key's void future is equal on every
/// read and completes with its key). Each pair runs the rows that apply to its shape; the rows a pair omits, and why,
/// are listed on its row data.
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

    /// <summary>
    /// The group ids of the S5c group Results: distinct under the ordinal comparer every one of them keys by, and equal
    /// under a case-insensitive one, so an aggregate gathered with any other comparer loses keys and fails the
    /// all-succeed rows.
    /// </summary>
    private static readonly string[] s_groups = { "group", "Group", "GROUP" };

    /// <summary>A group id none of <see cref="s_groups"/> equals ordinally.</summary>
    private const string UnknownGroup = "gRoUp";

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

    /// <summary>
    /// <see cref="CreateTopicsResult"/> and <see cref="AsyncCreateTopicsResult"/>: <c>Values</c>, <c>All()</c> and
    /// the four typed accessors compose alike on every row.
    /// </summary>
    [Theory]
    [MemberData(nameof(CreateTopicsRows))]
    public async Task CreateTopics_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        CreateTopicsHarness harness = CreateTopicsHarness.For(flavour);
        KeyedAdminOperation<string, TopicMetadataAndConfig> operation = harness.Operation;

        switch (row)
        {
            case "all-succeed":
                {
                    Config gammaConfig = new Config(new[] { new ConfigEntry("cleanup.policy", "compact") });
                    Uuid betaId = new Uuid(0x0102030405060708L, 0x1112131415161718L);
                    operation.SetResult("alpha", Metadata(3, 2));
                    operation.SetResult("beta", new TopicMetadataAndConfig(betaId, 1, 1, Empty()));
                    operation.SetResult("gamma", new TopicMetadataAndConfig(Uuid.Zero, 5, 3, gammaConfig));

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
                    operation.SetResult("alpha", Metadata(3, 1));
                    operation.SetException("beta", beta);
                    operation.SetResult("gamma", Metadata(5, 1));

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
                    operation.SetException("gamma", gamma);
                    operation.SetResult("beta", Metadata(1, 1));
                    operation.SetException("alpha", alpha);

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
                        operation.SetException(s_topics[index], failures[index]);
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

                    operation.SetResult("alpha", Metadata(3, 1));

                    FutureProbe alpha = harness.Values("alpha");
                    Assert.True(alpha.IsDone);
                    AssertReturned(await alpha.Outcome());
                    Assert.Equal((object)3, await harness.NumPartitions("alpha").Outcome());

                    Assert.False(beta.IsDone);
                    Assert.False(all.IsDone);
                    beta.AssertPending();
                    all.AssertPending();

                    operation.SetResult("beta", Metadata(7, 1));
                    AssertReturned(await beta.Outcome());
                    Assert.Equal((object)7, await betaPartitions.Outcome());

                    // gamma is still outstanding, so the aggregate is too.
                    Assert.False(all.IsDone);

                    operation.SetResult("gamma", Metadata(1, 1));
                    AssertReturned(await all.Outcome());
                    break;
                }

            case "memoization":
                {
                    // Created, but with no metadata: each typed accessor of TopicMetadataAndConfig throws a FRESH
                    // KafkaException per call, so a projection applied twice would hand out two instances.
                    KafkaException stored = new KafkaException(7, "Topic metadata unavailable.", isRetriable: true);
                    operation.SetResult("alpha", new TopicMetadataAndConfig(stored));

                    AssertReturned(await harness.Values("alpha").Outcome());

                    FutureProbe partitions = harness.NumPartitions("alpha");
                    KafkaException first = Assert.IsType<KafkaException>(await partitions.Outcome());
                    Assert.Equal(7, first.Code);
                    Assert.True(first.IsRetriable);
                    Assert.Equal("Topic metadata unavailable.", first.Message);
                    Assert.Same(stored, first.InnerException);
                    Assert.Same(first, await partitions.Outcome());

                    // A new call is a new future, as on both flavours today: it projects afresh.
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

                    operation.SetResult("alpha", Metadata(2, 1));

                    // The erasure is the key's own future, so it is done the moment the key is. (The typed projection
                    // is too on the sync flavour; on the async one it is a continuation, done a moment later.)
                    Assert.True(erased.IsDone);
                    AssertReturned(await erased.Outcome());
                    Assert.Equal((object)2, await typed.Outcome());
                    Assert.True(harness.ValuesEqualAcrossReads("alpha"));

                    // The erased and typed futures of a failed key fail with the same instance.
                    KafkaException beta = new KafkaException(38, "beta failed", isRetriable: false);
                    operation.SetException("beta", beta);
                    Assert.Same(beta, await harness.Values("beta").Outcome());
                    Assert.Same(beta, await harness.TopicId("beta").Outcome());
                    break;
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(row), row, "unknown row");
        }
    }

    /// <summary>
    /// <see cref="DeleteTopicsResult"/> / <see cref="AsyncDeleteTopicsResult"/>. Omitted: memoization (the result has
    /// no projection — its only composite is <c>All()</c>).
    /// </summary>
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

    /// <summary>
    /// <see cref="DescribeTopicsResult"/> / <see cref="AsyncDescribeTopicsResult"/>. Omitted: erasure identity (the
    /// result has no void view; its equal-on-every-read half is the key-identity row).
    /// </summary>
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

    /// <summary>
    /// <see cref="ListTopicsResult"/> / <see cref="AsyncListTopicsResult"/>: one future and two projections, no keys.
    /// Omitted: one-fails / two-fail / all-fail (one future, so they collapse into the fails row); unknown-key,
    /// null-key and per-key independence (no keys); erasure identity (no void view; the identity row pins
    /// <c>NamesToListings()</c>).
    /// </summary>
    public static TheoryData<string, string> ListTopicsRows => Rows(
        "all-succeed",
        "fails",
        "memoization",
        "identity");

    /// <summary>
    /// <see cref="CreatePartitionsResult"/> / <see cref="AsyncCreatePartitionsResult"/>. Omitted: memoization (no
    /// projection).
    /// </summary>
    public static TheoryData<string, string> CreatePartitionsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "erasure-identity");

    /// <summary>
    /// <see cref="DeleteRecordsResult"/> / <see cref="AsyncDeleteRecordsResult"/>. Omitted: null-key
    /// (<see cref="TopicPartition"/> is a value type, so a null key cannot be expressed); memoization (no projection);
    /// erasure identity (no per-key void view; <c>All()</c> is covered by the outcome rows, and the per-key futures'
    /// equal-on-every-read half is the key-identity row).
    /// </summary>
    public static TheoryData<string, string> DeleteRecordsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "per-key-independence",
        "key-identity");

    /// <summary>
    /// <see cref="DeleteTopicsResult"/> and <see cref="AsyncDeleteTopicsResult"/>: the per-key void futures and
    /// <c>All()</c>, by name, and by id on the by-id row, compose alike.
    /// </summary>
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

    /// <summary>
    /// <see cref="DescribeTopicsResult"/> and <see cref="AsyncDescribeTopicsResult"/>: the per-key futures and the
    /// gathered <c>AllTopicNames()</c> / <c>AllTopicIds()</c> compose alike, the mismatched side being null on both.
    /// </summary>
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

    /// <summary>
    /// <see cref="ListTopicsResult"/> and <see cref="AsyncListTopicsResult"/>: <c>NamesToListings()</c> and the two
    /// projections over it compose alike.
    /// </summary>
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

                    harness.Operation.SetResult(listings);

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
                    harness.Operation.SetException(failure);

                    Assert.Same(failure, await harness.NamesToListings().Outcome());
                    Assert.Same(failure, await harness.Listings().Outcome());
                    Assert.Same(failure, await harness.Names().Outcome());
                    break;
                }

            case "memoization":
                {
                    harness.Operation.SetResult(listings);

                    // A projection is applied once per future, so every Get() of it hands out the same collection ...
                    FutureProbe values = harness.Listings();
                    FutureProbe names = harness.Names();
                    object? listed = await values.Outcome();
                    object? named = await names.Outcome();
                    Assert.Same(listed, await values.Outcome());
                    Assert.Same(named, await names.Outcome());

                    // ... and a new call is a new projection, as on both flavours before S4.
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

                    harness.Operation.SetResult(listings);

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

    /// <summary>
    /// <see cref="CreatePartitionsResult"/> and <see cref="AsyncCreatePartitionsResult"/>: the per-key void futures
    /// and <c>All()</c> compose alike.
    /// </summary>
    [Theory]
    [MemberData(nameof(CreatePartitionsRows))]
    public Task CreatePartitions_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(row, VoidHarness<string>.CreatePartitions(flavour, s_topics), "delta");

    /// <summary>
    /// <see cref="DeleteRecordsResult"/> and <see cref="AsyncDeleteRecordsResult"/>: the per-partition low watermarks
    /// and the void <c>All()</c> compose alike.
    /// </summary>
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

    /// <summary>
    /// <see cref="DescribeClusterResult"/> / <see cref="AsyncDescribeClusterResult"/> (M15/P14 S5a): one completion
    /// and four projections over it, no keys. Omitted: one-fails / two-fail / all-fail (one completion, so they collapse
    /// into the fails row); unknown-key, null-key and per-key independence (no keys); erasure identity (no void view).
    /// The memoization row carries the identity half: each accessor is built once, so it is equal on every call.
    /// </summary>
    public static TheoryData<string, string> DescribeClusterRows => Rows(
        "all-succeed",
        "all-succeed-absent",
        "fails",
        "memoization");

    /// <summary>
    /// <see cref="ListConfigResourcesResult"/> / <see cref="AsyncListConfigResourcesResult"/> (M15/P14 S5a): one
    /// future, no projection, no keys. Omitted: one-fails / two-fail / all-fail (one future, so they collapse into the
    /// fails row); unknown-key, null-key and per-key independence (no keys); memoization (no projection or aggregate);
    /// erasure identity (no void view; the identity row pins <c>All()</c>).
    /// </summary>
    public static TheoryData<string, string> ListConfigResourcesRows => Rows(
        "all-succeed",
        "fails",
        "identity");

    /// <summary>
    /// <see cref="DescribeConfigsResult"/> / <see cref="AsyncDescribeConfigsResult"/> (M15/P14 S5a). Omitted: erasure
    /// identity (no void view; its equal-on-every-read half is the key-identity row).
    /// </summary>
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

    /// <summary>
    /// <see cref="AlterConfigsResult"/> / <see cref="AsyncAlterConfigsResult"/> (M15/P14 S5a). Omitted: memoization (no
    /// projection — its only composite is <c>All()</c>).
    /// </summary>
    public static TheoryData<string, string> AlterConfigsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "erasure-identity");

    /// <summary>
    /// <see cref="DescribeLogDirsResult"/> / <see cref="AsyncDescribeLogDirsResult"/> (M15/P14 S5a). Omitted: null-key
    /// (a broker id is an <see cref="int"/>, so a null key cannot be expressed); erasure identity (no void view; its
    /// equal-on-every-read half is the key-identity row).
    /// </summary>
    public static TheoryData<string, string> DescribeLogDirsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "per-key-independence",
        "memoization",
        "key-identity");

    /// <summary>
    /// <see cref="AlterReplicaLogDirsResult"/> / <see cref="AsyncAlterReplicaLogDirsResult"/> (M15/P14 S5a). Omitted:
    /// memoization (no projection — its only composite is <c>All()</c>).
    /// </summary>
    public static TheoryData<string, string> AlterReplicaLogDirsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "erasure-identity");

    /// <summary>
    /// <see cref="DescribeReplicaLogDirsResult"/> / <see cref="AsyncDescribeReplicaLogDirsResult"/> (M15/P14 S5a).
    /// Omitted: erasure identity (no void view; its equal-on-every-read half is the key-identity row).
    /// </summary>
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

    /// <summary>
    /// <see cref="DescribeClusterResult"/> and <see cref="AsyncDescribeClusterResult"/>: the four projections over the
    /// one completion compose alike.
    /// </summary>
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

                    harness.Operation.SetResult(new DescribeClusterSnapshot(nodes, controller, "cluster-1", operations));

                    Assert.Same(nodes, await probes[0].Outcome());
                    Assert.Same(controller, await probes[1].Outcome());
                    Assert.Equal("cluster-1", await probes[2].Outcome());
                    Assert.Same(operations, await probes[3].Outcome());
                    break;
                }

            case "all-succeed-absent":
                {
                    // No controller, no cluster id (the old-broker Metadata fallback), no authorized operations reported:
                    // each projection yields null rather than failing.
                    harness.Operation.SetResult(new DescribeClusterSnapshot(nodes, null, null, null));

                    FutureProbe[] probes = harness.Accessors();
                    Assert.Same(nodes, await probes[0].Outcome());
                    Assert.Null(await probes[1].Outcome());
                    Assert.Null(await probes[2].Outcome());
                    Assert.Null(await probes[3].Outcome());
                    break;
                }

            case "fails":
                {
                    // Every projection rethrows the completion's own exception instance, wrapped in nothing.
                    KafkaException failure = new KafkaException(7, "describeCluster failed", isRetriable: true);
                    harness.Operation.SetException(failure);

                    foreach (FutureProbe probe in harness.Accessors())
                    {
                        Assert.Same(failure, await probe.Outcome());
                    }

                    break;
                }

            case "memoization":
                {
                    // Each projection is built once, with the result: equal on every call, before and after completion ...
                    Assert.True(harness.AccessorsEqualAcrossCalls());
                    FutureProbe[] first = harness.Accessors();

                    harness.Operation.SetResult(new DescribeClusterSnapshot(nodes, controller, "cluster-1", operations));

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

    /// <summary>
    /// <see cref="ListConfigResourcesResult"/> and <see cref="AsyncListConfigResourcesResult"/>: <c>All()</c> composes
    /// alike.
    /// </summary>
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

                    harness.Operation.SetResult(resources);

                    Assert.Same(resources, await all.Outcome());
                    break;
                }

            case "fails":
                {
                    KafkaException failure = new KafkaException(7, "listConfigResources failed", isRetriable: true);
                    harness.Operation.SetException(failure);

                    Assert.Same(failure, await harness.All().Outcome());
                    break;
                }

            case "identity":
                {
                    // All() is the operation's own future: equal on every call, done the moment it is.
                    Assert.True(harness.AllEqualAcrossCalls());
                    FutureProbe all = harness.All();
                    Assert.False(all.IsDone);

                    harness.Operation.SetResult(resources);

                    Assert.True(all.IsDone);
                    Assert.True(harness.All().IsDone);
                    Assert.True(harness.AllEqualAcrossCalls());
                    Assert.Same(resources, await all.Outcome());
                    break;
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(row), row, "unknown row");
        }
    }

    /// <summary>
    /// <see cref="DescribeConfigsResult"/> and <see cref="AsyncDescribeConfigsResult"/>: the per-resource futures and
    /// the gathered <c>All()</c> compose alike.
    /// </summary>
    [Theory]
    [MemberData(nameof(DescribeConfigsRows))]
    public Task DescribeConfigs_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(
            row,
            ValuedHarness<ConfigResource, Config>.DescribeConfigs(flavour, Resources()),
            new ConfigResource(ConfigResourceType.Topic, "delta"));

    /// <summary>
    /// <see cref="AlterConfigsResult"/> and <see cref="AsyncAlterConfigsResult"/>: the per-resource void futures and
    /// <c>All()</c> compose alike.
    /// </summary>
    [Theory]
    [MemberData(nameof(AlterConfigsRows))]
    public Task AlterConfigs_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(
            row,
            VoidHarness<ConfigResource>.IncrementalAlterConfigs(flavour, Resources()),
            new ConfigResource(ConfigResourceType.Topic, "delta"));

    /// <summary>
    /// <see cref="DescribeLogDirsResult"/> and <see cref="AsyncDescribeLogDirsResult"/>: the per-broker futures and
    /// the gathered <c>AllDescriptions()</c> compose alike.
    /// </summary>
    [Theory]
    [MemberData(nameof(DescribeLogDirsRows))]
    public Task DescribeLogDirs_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(
            row,
            ValuedHarness<int, IReadOnlyDictionary<string, LogDirDescription>>.DescribeLogDirs(flavour, new[] { 1, 2, 3 }),
            9);

    /// <summary>
    /// <see cref="AlterReplicaLogDirsResult"/> and <see cref="AsyncAlterReplicaLogDirsResult"/>: the per-replica void
    /// futures and <c>All()</c> compose alike.
    /// </summary>
    [Theory]
    [MemberData(nameof(AlterReplicaLogDirsRows))]
    public Task AlterReplicaLogDirs_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(
            row,
            VoidHarness<TopicPartitionReplica>.AlterReplicaLogDirs(flavour, Replicas()),
            new TopicPartitionReplica("gamma", 0, 1));

    /// <summary>
    /// <see cref="DescribeReplicaLogDirsResult"/> and <see cref="AsyncDescribeReplicaLogDirsResult"/>: the per-replica
    /// futures and the gathered <c>All()</c> compose alike.
    /// </summary>
    [Theory]
    [MemberData(nameof(DescribeReplicaLogDirsRows))]
    public Task DescribeReplicaLogDirs_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(
            row,
            ValuedHarness<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo>.DescribeReplicaLogDirs(
                flavour, Replicas()),
            new TopicPartitionReplica("gamma", 0, 1));

    /// <summary>
    /// <see cref="ElectLeadersResult"/> / <see cref="AsyncElectLeadersResult"/> (M15/P14 S5b): one future over a map
    /// whose values are the per-partition outcomes, and <c>All()</c>'s walk over those values. The one-fails /
    /// two-fail / all-fail rows put the failures in the map, where Java's <c>Optional&lt;Throwable&gt;</c> carries them
    /// (the map's own future succeeds); the fails row is the call-level failure. Omitted: unknown-key, null-key and
    /// per-key independence (one future and no per-key member); erasure identity (no void view; the identity row pins
    /// <c>Partitions()</c>).
    /// </summary>
    public static TheoryData<string, string> ElectLeadersRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "fails",
        "memoization",
        "identity");

    /// <summary>
    /// <see cref="AlterPartitionReassignmentsResult"/> / <see cref="AsyncAlterPartitionReassignmentsResult"/> (M15/P14
    /// S5b). Omitted: null-key (<see cref="TopicPartition"/> is a value type, so a null key cannot be expressed);
    /// memoization (no projection — its only composite is <c>All()</c>).
    /// </summary>
    public static TheoryData<string, string> AlterPartitionReassignmentsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "per-key-independence",
        "erasure-identity");

    /// <summary>
    /// <see cref="ListPartitionReassignmentsResult"/> / <see cref="AsyncListPartitionReassignmentsResult"/> (M15/P14
    /// S5b): one future, no projection, no keys. Omitted: one-fails / two-fail / all-fail (one future, and the ABI
    /// declares no per-partition error, so they collapse into the fails row); unknown-key, null-key and per-key
    /// independence (no keys); memoization (no projection or aggregate); erasure identity (no void view; the identity row
    /// pins <c>Reassignments()</c>).
    /// </summary>
    public static TheoryData<string, string> ListPartitionReassignmentsRows => Rows(
        "all-succeed",
        "fails",
        "identity");

    /// <summary>
    /// <see cref="ListOffsetsResult"/> / <see cref="AsyncListOffsetsResult"/> (M15/P14 S5b). The per-key member is
    /// <c>PartitionResult</c>, so the unknown-key row asserts its own guard rather than a dictionary's. Omitted: null-key
    /// (<see cref="TopicPartition"/> is a value type, so a null key cannot be expressed); erasure identity (no void view;
    /// its equal-on-every-read half is the key-identity row).
    /// </summary>
    public static TheoryData<string, string> ListOffsetsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "per-key-independence",
        "memoization",
        "key-identity");

    /// <summary>
    /// <see cref="ElectLeadersResult"/> and <see cref="AsyncElectLeadersResult"/>: <c>Partitions()</c> and the walk
    /// <c>All()</c> makes over its values compose alike.
    /// </summary>
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
                    harness.Operation.SetResult(outcomes);

                    Assert.Same(outcomes, await map.Outcome());
                    AssertReturned(await all.Outcome());
                    break;
                }

            case "one-fails":
                {
                    // A partition's failure is a VALUE: the map's own future succeeds, and only All() walks it into one.
                    KafkaException failure = new KafkaException(84, "the second election failed", isRetriable: false);
                    Dictionary<TopicPartition, KafkaException?> outcomes = Outcomes(partitions, null, failure, null);
                    harness.Operation.SetResult(outcomes);

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
                    harness.Operation.SetResult(outcomes);

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
                    harness.Operation.SetResult(outcomes);

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
                    harness.Operation.SetException(failure);

                    Assert.Same(failure, await harness.Partitions().Outcome());
                    Assert.Same(failure, await harness.All().Outcome());
                    break;
                }

            case "memoization":
                {
                    KafkaException failure = new KafkaException(84, "the second election failed", isRetriable: false);
                    harness.Operation.SetResult(Outcomes(partitions, null, failure, null));

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
                    harness.Operation.SetResult(outcomes);

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

    /// <summary>
    /// <see cref="AlterPartitionReassignmentsResult"/> and <see cref="AsyncAlterPartitionReassignmentsResult"/>: the
    /// per-partition void futures and <c>All()</c> compose alike.
    /// </summary>
    [Theory]
    [MemberData(nameof(AlterPartitionReassignmentsRows))]
    public Task AlterPartitionReassignments_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(
            row,
            VoidHarness<TopicPartition>.AlterPartitionReassignments(flavour, TopicPartitions()),
            new TopicPartition("gamma", 0));

    /// <summary>
    /// <see cref="ListPartitionReassignmentsResult"/> and <see cref="AsyncListPartitionReassignmentsResult"/>:
    /// <c>Reassignments()</c> composes alike.
    /// </summary>
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

                    harness.Operation.SetResult(reassignments);

                    Assert.Same(reassignments, await map.Outcome());
                    break;
                }

            case "fails":
                {
                    KafkaException failure = new KafkaException(7, "listPartitionReassignments failed", isRetriable: true);
                    harness.Operation.SetException(failure);

                    Assert.Same(failure, await harness.Reassignments().Outcome());
                    break;
                }

            case "identity":
                {
                    // Reassignments() is the operation's own future: equal on every call, done the moment it is.
                    Assert.True(harness.ReassignmentsEqualAcrossCalls());
                    FutureProbe map = harness.Reassignments();
                    Assert.False(map.IsDone);

                    harness.Operation.SetResult(reassignments);

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

    /// <summary>
    /// <see cref="ListOffsetsResult"/> and <see cref="AsyncListOffsetsResult"/>: the per-partition futures
    /// <c>PartitionResult</c> hands out and the gathered <c>All()</c> compose alike.
    /// </summary>
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

        // PartitionResult is the only per-key member, and its guard is Java's (ListOffsetsResult.java:43-46):
        // synchronous, at the call, an ArgumentException with the exact message — not a dictionary's KeyNotFoundException.
        string expected = new ArgumentException("List Offsets for partition \"gamma-0\" was not attempted", "partition").Message;
        ArgumentException failure = Assert.IsType<ArgumentException>(harness.CallIndexer(unknown));
        Assert.Equal("partition", failure.ParamName);
        Assert.Equal(expected, failure.Message);
        Assert.False(harness.ContainsKey(unknown));
        Assert.True(harness.ContainsKey(partitions[0]));
    }

    /// <summary>
    /// <see cref="ListGroupsResult"/> / <see cref="AsyncListGroupsResult"/> (M15/P14 S5c): one completion and three
    /// projections over it, no keys. A per-group error is a <b>value</b> of the completion (Java's <c>errors</c>
    /// collection), so the one-fails / two-fail rows put errors there and only <c>All()</c> turns one into a failure;
    /// "fails" is the call-level failure. "errors-guard-at-get" pins the timing of <c>All()</c>'s error guard: at
    /// <c>Get()</c>, never at the call. Omitted: all-fail (the errors are one collection, so it is the two-fail row);
    /// unknown-key, null-key and per-key independence (no keys); erasure identity (no void view; each accessor is a fresh
    /// future per call, which the memoization row pins).
    /// </summary>
    public static TheoryData<string, string> ListGroupsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-order",
        "fails",
        "errors-guard-at-get",
        "memoization");

    /// <summary>
    /// <see cref="DescribeConsumerGroupsResult"/> / <see cref="AsyncDescribeConsumerGroupsResult"/> (M15/P14 S5c). The
    /// group ids differ only in case (<see cref="s_groups"/>), so every gathered row also pins <c>All()</c>'s ordinal
    /// key comparer. Omitted: erasure identity (no void view; its equal-on-every-read half is the key-identity row).
    /// </summary>
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

    /// <summary>
    /// <see cref="DescribeClassicGroupsResult"/> / <see cref="AsyncDescribeClassicGroupsResult"/> (M15/P14 S5c): the rows
    /// and omissions of <see cref="DescribeConsumerGroupsRows"/>.
    /// </summary>
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

    /// <summary>
    /// <see cref="ListConsumerGroupOffsetsResult"/> / <see cref="AsyncListConsumerGroupOffsetsResult"/> (M15/P14 S5c).
    /// The per-key member is <c>PartitionsToOffsetAndMetadata(groupId)</c>, so the unknown-key and null-key rows assert
    /// its own guards (at the call, before and after completion) rather than a dictionary's; "no-arg-guard" pins the
    /// no-argument overload's count guard at the call, and that with one group it is that group's own future. Omitted:
    /// erasure identity (no void view; its equal-on-every-read half is the key-identity row).
    /// </summary>
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

    /// <summary>
    /// <see cref="DeleteConsumerGroupsResult"/> / <see cref="AsyncDeleteConsumerGroupsResult"/> (M15/P14 S5c). Omitted:
    /// memoization (no projection — its only composite is <c>All()</c>).
    /// </summary>
    public static TheoryData<string, string> DeleteConsumerGroupsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "erasure-identity");

    /// <summary>
    /// <see cref="AlterConsumerGroupOffsetsResult"/> / <see cref="AsyncAlterConsumerGroupOffsetsResult"/> (M15/P14 S5c):
    /// one completion carrying the per-partition outcomes and the core's own aggregate, with <c>PartitionResult</c> and
    /// <c>All()</c> projections over it. "unknown-key-at-get" pins the unknown-partition guard's timing: inside the
    /// future, as Java's is (<c>AlterConsumerGroupOffsetsResult.java:48-50</c>), so the call succeeds and <c>Get()</c>
    /// throws; and a call-level failure wins over it (Java tests <c>throwable</c> first, <c>:46-47</c>). Omitted:
    /// two-fail / all-fail (<c>All()</c> rethrows the core's stored aggregate rather than composing per-partition
    /// failures, so there is no binding-side order to pin — the one-fails row pins that it is the stored one); null-key
    /// (<see cref="TopicPartition"/> is a value type); per-key independence (one completion: every partition completes at
    /// once); erasure identity (no per-key void view: <c>PartitionResult</c> is a fresh future per call, which the
    /// memoization row pins); missing-row (the unknown-key row is it: the map is the request set).
    /// </summary>
    public static TheoryData<string, string> AlterConsumerGroupOffsetsRows => Rows(
        "all-succeed",
        "one-fails",
        "fails",
        "unknown-key-at-get",
        "memoization");

    /// <summary>
    /// <see cref="DeleteConsumerGroupOffsetsResult"/> / <see cref="AsyncDeleteConsumerGroupOffsetsResult"/> (M15/P14 S5c):
    /// the shape of <see cref="AlterConsumerGroupOffsetsRows"/>, but the unknown-partition guard is Java's synchronous
    /// precondition (<c>DeleteConsumerGroupOffsetsResult.java:44-46</c>), pinned at the call by "unknown-key-at-call";
    /// "missing-row-at-get" pins that a requested partition the completion has no row for fails at <c>Get()</c>. Omitted:
    /// two-fail / all-fail, null-key, per-key independence and erasure identity, as for AlterConsumerGroupOffsets.
    /// </summary>
    public static TheoryData<string, string> DeleteConsumerGroupOffsetsRows => Rows(
        "all-succeed",
        "one-fails",
        "fails",
        "unknown-key-at-call",
        "missing-row-at-get",
        "memoization");

    /// <summary>
    /// <see cref="RemoveMembersFromConsumerGroupResult"/> / <see cref="AsyncRemoveMembersFromConsumerGroupResult"/>
    /// (M15/P14 S5c): the shape of <see cref="DeleteConsumerGroupOffsetsRows"/>, keyed by member. Its three guards are
    /// Java's synchronous preconditions (<c>RemoveMembersFromConsumerGroupResult.java:82-87</c>), each pinned at the
    /// call by its own row: a null member, removeAll mode, and an unrequested member. Omitted: two-fail / all-fail
    /// (<c>All()</c> rethrows the core's stored aggregate); per-key independence and erasure identity, as for
    /// AlterConsumerGroupOffsets.
    /// </summary>
    public static TheoryData<string, string> RemoveMembersFromConsumerGroupRows => Rows(
        "all-succeed",
        "one-fails",
        "fails",
        "null-key-at-call",
        "remove-all-at-call",
        "unknown-key-at-call",
        "missing-row-at-get",
        "memoization");

    /// <summary>
    /// <see cref="ListGroupsResult"/> and <see cref="AsyncListGroupsResult"/>: <c>All()</c>, <c>Valid()</c> and
    /// <c>Errors()</c> over the one completion compose alike.
    /// </summary>
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

                    harness.Operation.SetResult((valid, none));

                    // With no error, All() yields exactly what Valid() does.
                    Assert.Same(valid, await all.Outcome());
                    Assert.Same(valid, await listed.Outcome());
                    Assert.Same(none, await errors.Outcome());
                    break;
                }

            case "one-fails":
                {
                    // A group's error is a VALUE: Valid() and Errors() succeed, and only All() turns it into a failure.
                    KafkaException failure = new KafkaException(15, "the second coordinator is not available", isRetriable: true);
                    IReadOnlyCollection<KafkaException> errors = new[] { failure };
                    harness.Operation.SetResult((valid, errors));

                    Assert.Same(failure, await harness.All().Outcome());
                    Assert.Same(valid, await harness.Valid().Outcome());
                    Assert.Same(errors, await harness.Errors().Outcome());
                    break;
                }

            case "two-fail-first-in-order":
                {
                    // All() rethrows the first error in the collection's order (Java's errorsResult.get(0)), not the last.
                    KafkaException first = new KafkaException(15, "the first coordinator is not available", isRetriable: true);
                    KafkaException last = new KafkaException(16, "the last coordinator is loading", isRetriable: true);
                    IReadOnlyCollection<KafkaException> errors = new[] { first, last };
                    harness.Operation.SetResult((valid, errors));

                    Assert.Same(first, await harness.All().Outcome());
                    Assert.Same(errors, await harness.Errors().Outcome());
                    Assert.Same(valid, await harness.Valid().Outcome());
                    break;
                }

            case "fails":
                {
                    // The call-level failure fails all three with the call's own instance (see the type remarks: Java would
                    // leave them incomplete).
                    KafkaException failure = new KafkaException(7, "listGroups failed", isRetriable: true);
                    harness.Operation.SetException(failure);

                    Assert.Same(failure, await harness.All().Outcome());
                    Assert.Same(failure, await harness.Valid().Outcome());
                    Assert.Same(failure, await harness.Errors().Outcome());
                    break;
                }

            case "errors-guard-at-get":
                {
                    // The guard is inside the future: neither the call nor its probe throws, before completion or after it,
                    // and Get() fails with the error.
                    KafkaException failure = new KafkaException(15, "the second coordinator is not available", isRetriable: true);
                    FutureProbe? before = null;
                    Assert.Null(Thrown(() => before = harness.All()));
                    before!.AssertPending();

                    harness.Operation.SetResult((valid, new[] { failure }));

                    FutureProbe? after = null;
                    Assert.Null(Thrown(() => after = harness.All()));
                    Assert.Same(failure, await before.Outcome());
                    Assert.Same(failure, await after!.Outcome());
                    break;
                }

            case "memoization":
                {
                    KafkaException failure = new KafkaException(15, "the second coordinator is not available", isRetriable: true);
                    harness.Operation.SetResult((valid, new[] { failure }));

                    // One All() future checks once, so every Get() of it rethrows the same instance ...
                    FutureProbe all = harness.All();
                    Assert.Same(failure, await all.Outcome());
                    Assert.Same(failure, await all.Outcome());
                    FutureProbe listed = harness.Valid();
                    Assert.Same(valid, await listed.Outcome());
                    Assert.Same(valid, await listed.Outcome());

                    // ... and every accessor is a fresh future per call, on both flavours.
                    (bool fresh, FutureProbe first, FutureProbe second) = harness.TwoAlls();
                    Assert.True(fresh, "two All() calls returned an equal future");
                    Assert.Same(failure, await first.Outcome());
                    Assert.Same(failure, await second.Outcome());
                    Assert.True(harness.ValidAndErrorsFreshPerCall(), "Valid() or Errors() returned an equal future twice");
                    break;
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(row), row, "unknown row");
        }
    }

    /// <summary>
    /// <see cref="DescribeConsumerGroupsResult"/> and <see cref="AsyncDescribeConsumerGroupsResult"/>: the per-group
    /// futures and the gathered <c>All()</c> compose alike.
    /// </summary>
    [Theory]
    [MemberData(nameof(DescribeConsumerGroupsRows))]
    public Task DescribeConsumerGroups_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(row, ValuedHarness<string, ConsumerGroupDescription>.DescribeConsumerGroups(flavour, s_groups), UnknownGroup);

    /// <summary>
    /// <see cref="DescribeClassicGroupsResult"/> and <see cref="AsyncDescribeClassicGroupsResult"/>: the per-group
    /// futures and the gathered <c>All()</c> compose alike.
    /// </summary>
    [Theory]
    [MemberData(nameof(DescribeClassicGroupsRows))]
    public Task DescribeClassicGroups_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(row, ValuedHarness<string, ClassicGroupDescription>.DescribeClassicGroups(flavour, s_groups), UnknownGroup);

    /// <summary>
    /// <see cref="ListConsumerGroupOffsetsResult"/> and <see cref="AsyncListConsumerGroupOffsetsResult"/>: the per-group
    /// futures <c>PartitionsToOffsetAndMetadata</c> hands out, its no-argument form and the gathered <c>All()</c>
    /// compose alike.
    /// </summary>
    [Theory]
    [MemberData(nameof(ListConsumerGroupOffsetsRows))]
    public async Task ListConsumerGroupOffsets_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        ListConsumerGroupOffsetsHarness harness = ListConsumerGroupOffsetsHarness.For(flavour, s_groups);
        switch (row)
        {
            case "unknown-key":
                {
                    // Java's guard (ListConsumerGroupOffsetsResult.java:63-64): at the call, before completion and after it,
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
                    // Java's guard is `futures.size() != 1` (:50-53): at the call, before completion and after it, for three
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

    /// <summary>
    /// <see cref="DeleteConsumerGroupsResult"/> and <see cref="AsyncDeleteConsumerGroupsResult"/>: the per-group void
    /// futures and <c>All()</c> compose alike.
    /// </summary>
    [Theory]
    [MemberData(nameof(DeleteConsumerGroupsRows))]
    public Task DeleteConsumerGroups_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(row, VoidHarness<string>.DeleteConsumerGroups(flavour, s_groups), UnknownGroup);

    /// <summary>
    /// <see cref="AlterConsumerGroupOffsetsResult"/> and <see cref="AsyncAlterConsumerGroupOffsetsResult"/>:
    /// <c>PartitionResult</c> and <c>All()</c> over the one completion compose alike.
    /// </summary>
    [Theory]
    [MemberData(nameof(AlterConsumerGroupOffsetsRows))]
    public async Task AlterConsumerGroupOffsets_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        TopicPartition[] partitions = TopicPartitions();
        OutcomeMapHarness<TopicPartition, TopicPartition> harness =
            OutcomeMapHarness<TopicPartition, TopicPartition>.AlterConsumerGroupOffsets(flavour, partitions);
        if (row != "unknown-key-at-get")
        {
            await RunOutcomeMapRow(row, harness);
            return;
        }

        // Java's guard is inside the future (AlterConsumerGroupOffsetsResult.java:48-50): the call succeeds, before
        // completion and after it, and Get() throws Java's message (no parameter name).
        TopicPartition unknown = new TopicPartition("gamma", 0);
        const string Expected = "Alter offset for partition \"gamma-0\" was not attempted";
        FutureProbe? before = null;
        Assert.Null(Thrown(() => before = harness.Key(unknown)));
        before!.AssertPending();

        harness.SucceedEvery();

        FutureProbe? after = null;
        Assert.Null(Thrown(() => after = harness.Key(unknown)));
        AssertArgument(await before.Outcome(), null, Expected);
        AssertArgument(await after!.Outcome(), null, Expected);
        AssertReturned(await harness.All().Outcome());

        // A call-level failure wins over the guard: Java tests `throwable` before the map (:46-50).
        OutcomeMapHarness<TopicPartition, TopicPartition> failed =
            OutcomeMapHarness<TopicPartition, TopicPartition>.AlterConsumerGroupOffsets(flavour, partitions);
        KafkaException failure = new KafkaException(7, "the request failed", isRetriable: true);
        failed.Operation.SetException(failure);
        Assert.Same(failure, await failed.Key(unknown).Outcome());
    }

    /// <summary>
    /// <see cref="DeleteConsumerGroupOffsetsResult"/> and <see cref="AsyncDeleteConsumerGroupOffsetsResult"/>:
    /// <c>PartitionResult</c> and <c>All()</c> over the one completion compose alike.
    /// </summary>
    [Theory]
    [MemberData(nameof(DeleteConsumerGroupOffsetsRows))]
    public async Task DeleteConsumerGroupOffsets_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        TopicPartition[] partitions = TopicPartitions();
        OutcomeMapHarness<TopicPartition, TopicPartition> harness =
            OutcomeMapHarness<TopicPartition, TopicPartition>.DeleteConsumerGroupOffsets(flavour, partitions);
        if (row != "unknown-key-at-call")
        {
            await RunOutcomeMapRow(row, harness);
            return;
        }

        // Java's precondition (DeleteConsumerGroupOffsetsResult.java:44-46) runs before the future is consulted: at the
        // call, before completion, after it, and after a call-level failure alike, with Java's message.
        TopicPartition unknown = new TopicPartition("gamma", 0);
        const string Expected = "Partition gamma-0 was not included in the original request";
        AssertArgument(harness.CallKey(unknown), null, Expected);
        harness.SucceedEvery();
        AssertArgument(harness.CallKey(unknown), null, Expected);
        AssertReturned(await harness.Key(partitions[0]).Outcome());

        OutcomeMapHarness<TopicPartition, TopicPartition> failed =
            OutcomeMapHarness<TopicPartition, TopicPartition>.DeleteConsumerGroupOffsets(flavour, partitions);
        KafkaException failure = new KafkaException(7, "the request failed", isRetriable: true);
        failed.Operation.SetException(failure);
        AssertArgument(failed.CallKey(unknown), null, Expected);
        Assert.Same(failure, await failed.Key(partitions[0]).Outcome());
    }

    /// <summary>
    /// <see cref="RemoveMembersFromConsumerGroupResult"/> and <see cref="AsyncRemoveMembersFromConsumerGroupResult"/>:
    /// <c>MemberResult</c> and <c>All()</c> over the one completion compose alike.
    /// </summary>
    [Theory]
    [MemberData(nameof(RemoveMembersFromConsumerGroupRows))]
    public async Task RemoveMembersFromConsumerGroup_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        MemberToRemove[] members = Members();
        OutcomeMapHarness<MemberToRemove, string> harness =
            OutcomeMapHarness<MemberToRemove, string>.RemoveMembersFromConsumerGroup(flavour, members);
        switch (row)
        {
            case "null-key-at-call":
                {
                    // Java's preconditions run before the future (RemoveMembersFromConsumerGroupResult.java:82-87): at the
                    // call, before completion and after it.
                    AssertNullArgument(harness.CallKey(null!), "member");
                    harness.SucceedEvery();
                    AssertNullArgument(harness.CallKey(null!), "member");
                    break;
                }

            case "remove-all-at-call":
                {
                    const string Expected = "The method: memberResult is not applicable in 'removeAll' mode";
                    OutcomeMapHarness<MemberToRemove, string> removeAll =
                        OutcomeMapHarness<MemberToRemove, string>.RemoveMembersFromConsumerGroup(
                            flavour, Array.Empty<MemberToRemove>());
                    AssertArgument(removeAll.CallKey(members[0]), null, Expected);

                    // All() still answers in removeAll mode, with the core's own aggregate (see the type remarks).
                    KafkaException aggregate = new KafkaException(25, "the second member is unknown", isRetriable: false);
                    removeAll.Settle(Array.Empty<KafkaException?>(), aggregate);
                    AssertArgument(removeAll.CallKey(members[0]), null, Expected);
                    Assert.Same(aggregate, await removeAll.All().Outcome());
                    break;
                }

            case "unknown-key-at-call":
                {
                    // GroupInstanceId stands in for Java's identity-hash toString (see the type remarks).
                    MemberToRemove unknown = new MemberToRemove("instance-9");
                    const string Expected = "Member instance-9 was not included in the original request";
                    AssertArgument(harness.CallKey(unknown), null, Expected);

                    // An equal member is accepted: the request set compares by group instance id, not by reference.
                    FutureProbe equal = harness.Key(new MemberToRemove(members[0].GroupInstanceId));
                    harness.SucceedEvery();
                    AssertArgument(harness.CallKey(unknown), null, Expected);
                    AssertReturned(await equal.Outcome());
                    break;
                }

            default:
                await RunOutcomeMapRow(row, harness);
                break;
        }
    }

    /// <summary>
    /// <see cref="CreateAclsResult"/> / <see cref="AsyncCreateAclsResult"/> (M15/P14 S5d), keyed by binding. Each Result
    /// is built over one <see cref="Bindings"/> call and read with another's, so every keyed row also pins that its
    /// dictionary looks bindings up by value. Omitted: memoization (no projection — its only composite is
    /// <c>All()</c>).
    /// </summary>
    public static TheoryData<string, string> CreateAclsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "erasure-identity");

    /// <summary>
    /// <see cref="DeleteAclsResult"/> / <see cref="AsyncDeleteAclsResult"/> (M15/P14 S5d), keyed by filter and read
    /// by value as for <see cref="CreateAclsRows"/>. <c>All()</c> is Java's
    /// <c>allOf(...).thenApply(getAclBindings)</c> (<c>DeleteAclsResult.java:99-121</c>), every filter's matched
    /// bindings flattened in filter order. "inner-failure-at-get" pins the timing of its
    /// <see cref="DeleteAclsResult.FilterResult.Exception"/> guard (<c>:115-116</c>): at <c>Get()</c>, never at the
    /// call, and the first in filter order, not completion order. "outer-failure-before-inner" pins that a filter's own
    /// failure wins over an earlier filter's <c>FilterResult</c> exception, since <c>allOf</c> fails before the
    /// projection runs; "empty-filter-results" that filters matching nothing are not an error (<c>:96-97</c>).
    /// Omitted: erasure identity (no void view; its equal-on-every-read half is the key-identity row).
    /// </summary>
    public static TheoryData<string, string> DeleteAclsRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "memoization",
        "key-identity",
        "inner-failure-at-get",
        "outer-failure-before-inner",
        "empty-filter-results");

    /// <summary>
    /// <see cref="DescribeAclsResult"/> / <see cref="AsyncDescribeAclsResult"/> (M15/P14 S5d): one future,
    /// <c>Values()</c>, and no keys. Omitted: one-fails / two-fail / all-fail (one future, so they collapse into the
    /// fails row); unknown-key, null-key and per-key independence (no keys); memoization (no projection or aggregate:
    /// <c>Values()</c> is the operation's own future); erasure identity (no void view; the identity row pins
    /// <c>Values()</c>).
    /// </summary>
    public static TheoryData<string, string> DescribeAclsRows => Rows(
        "all-succeed",
        "fails",
        "identity");

    /// <summary>
    /// <see cref="DescribeClientQuotasResult"/> / <see cref="AsyncDescribeClientQuotasResult"/> (M15/P14 S5d): the rows
    /// and omissions of <see cref="DescribeAclsRows"/>, over <c>Entities()</c>, plus "constructor-default-guard": the
    /// internal constructor's guard (FU-5) throws at construction, alike on both flavours.
    /// </summary>
    public static TheoryData<string, string> DescribeClientQuotasRows => Rows(
        "all-succeed",
        "fails",
        "identity",
        "constructor-default-guard");

    /// <summary>
    /// <see cref="AlterClientQuotasResult"/> / <see cref="AsyncAlterClientQuotasResult"/> (M15/P14 S5d), keyed by entity
    /// (one of them the default user, a null name) and read by value as for <see cref="CreateAclsRows"/>. Two rows pin
    /// the internal constructor (FU-5): "constructor-null-guard", its guard at construction, alike on both flavours;
    /// and "constructor-by-reference", that it holds the map it is given as Java's does
    /// (<c>AlterClientQuotasResult.java:38-40</c>). Omitted: memoization (no projection — its only composite is
    /// <c>All()</c>).
    /// </summary>
    public static TheoryData<string, string> AlterClientQuotasRows => Rows(
        "all-succeed",
        "one-fails",
        "two-fail-first-in-key-order",
        "all-fail",
        "unknown-key",
        "null-key",
        "per-key-independence",
        "erasure-identity",
        "constructor-null-guard",
        "constructor-by-reference");

    /// <summary>
    /// <see cref="CreateAclsResult"/> and <see cref="AsyncCreateAclsResult"/>: the per-binding void futures and
    /// <c>All()</c> compose alike.
    /// </summary>
    [Theory]
    [MemberData(nameof(CreateAclsRows))]
    public Task CreateAcls_SyncAndAsyncResults_ComposeAlike(string row, string flavour) =>
        RunKeyedRow(
            row,
            VoidHarness<AclBinding>.CreateAcls(flavour, Bindings(), Bindings()),
            Binding(ResourceType.Topic, "beta", "User:alice"));

    /// <summary>
    /// <see cref="DeleteAclsResult"/> and <see cref="AsyncDeleteAclsResult"/>: the per-filter futures and the
    /// flattening <c>All()</c> compose alike.
    /// </summary>
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
                    // Java's guard is inside all()'s thenApply (DeleteAclsResult.java:115-116): the call succeeds, before
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
                    // (:100), so the projection never runs and the outer failure wins although it is later in filter order.
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
                    // Filters matching nothing are not an error (:96-97): All() succeeds, with what the others matched ...
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

    /// <summary>
    /// <see cref="DescribeAclsResult"/> and <see cref="AsyncDescribeAclsResult"/>: <c>Values()</c> composes alike.
    /// </summary>
    [Theory]
    [MemberData(nameof(DescribeAclsRows))]
    public Task DescribeAcls_SyncAndAsyncResults_ComposeAlike(string row, string flavour)
    {
        IReadOnlyCollection<AclBinding> bindings = Bindings();
        return RunSingleRow(row, SingleHarness<IReadOnlyCollection<AclBinding>>.DescribeAcls(flavour), bindings);
    }

    /// <summary>
    /// <see cref="DescribeClientQuotasResult"/> and <see cref="AsyncDescribeClientQuotasResult"/>: <c>Entities()</c>
    /// and the internal constructor compose alike.
    /// </summary>
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

    /// <summary>
    /// <see cref="AlterClientQuotasResult"/> and <see cref="AsyncAlterClientQuotasResult"/>: the per-entity void
    /// futures, <c>All()</c> and the internal constructor compose alike.
    /// </summary>
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

    /// <summary>
    /// <see cref="AlterClientQuotas_SyncAndAsyncResults_ComposeAlike"/>'s "constructor-by-reference" row: the
    /// constructor holds the map it is given (<c>AlterClientQuotasResult.java:38-40</c>), so <c>Values</c> is that very
    /// map and <c>All()</c> reads whatever it holds when called. One entity's future is the map's only entry at first; a
    /// second, failed one is added after construction.
    /// </summary>
    private static async Task AlterClientQuotasByReference(string flavour)
    {
        ClientQuotaEntity[] entities = Entities();
        AdminFlavour adminFlavour = FlavourOf(flavour);
        VoidKeyedAdminOperation<ClientQuotaEntity> operation = new VoidKeyedAdminOperation<ClientQuotaEntity>(
            "alterClientQuotas", entities, EqualityComparer<ClientQuotaEntity>.Default, adminFlavour);
        KafkaException failure = new KafkaException(38, "the second entity failed", isRetriable: false);
        if (adminFlavour == AdminFlavour.Sync)
        {
            IReadOnlyDictionary<ClientQuotaEntity, KafkaFuture> erased =
                SyncAdminFutures.Erase(operation.SyncSources, operation.KeyComparer);
            Dictionary<ClientQuotaEntity, KafkaFuture> map =
                new Dictionary<ClientQuotaEntity, KafkaFuture> { [entities[0]] = erased[entities[0]] };
            AlterClientQuotasResult result = new AlterClientQuotasResult(map);
            Assert.Same(map, result.Values);

            operation.SetResult(entities[0], true);
            AssertReturned(await FutureProbe.Of(result.All()).Outcome());

            map.Add(entities[1], erased[entities[1]]);
            operation.SetException(entities[1], failure);
            Assert.Same(map, result.Values);
            Assert.Same(failure, await FutureProbe.Of(result.All()).Outcome());
            return;
        }

        Dictionary<ClientQuotaEntity, AsyncKafkaFuture> asyncMap =
            new Dictionary<ClientQuotaEntity, AsyncKafkaFuture> { [entities[0]] = new AsyncKafkaFuture(operation.Tasks[entities[0]]) };
        AsyncAlterClientQuotasResult asyncResult = new AsyncAlterClientQuotasResult(asyncMap);
        Assert.Same(asyncMap, asyncResult.Values);

        operation.SetResult(entities[0], true);
        AssertReturned(await FutureProbe.Of(asyncResult.All()).Outcome());

        asyncMap.Add(entities[1], new AsyncKafkaFuture(operation.Tasks[entities[1]]));
        operation.SetException(entities[1], failure);
        Assert.Same(asyncMap, asyncResult.Values);
        Assert.Same(failure, await FutureProbe.Of(asyncResult.All()).Outcome());
    }

    /// <summary>The rows every Result over one future and no keys shares.</summary>
    private static async Task RunSingleRow<T>(string row, SingleHarness<T> harness, T value)
        where T : class
    {
        switch (row)
        {
            case "all-succeed":
                {
                    FutureProbe future = harness.Future();
                    future.AssertPending();

                    harness.Operation.SetResult(value);

                    Assert.Same(value, await future.Outcome());
                    break;
                }

            case "fails":
                {
                    // The call-level failure is the one future's own, the same instance on every Get().
                    KafkaException failure = new KafkaException(7, "the request failed", isRetriable: true);
                    harness.Operation.SetException(failure);

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

                    harness.Operation.SetResult(value);

                    Assert.True(future.IsDone);
                    Assert.True(harness.Future().IsDone);
                    Assert.True(harness.EqualAcrossCalls());
                    Assert.Same(value, await future.Outcome());
                    break;
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(row), row, "unknown row");
        }
    }

    /// <summary>
    /// The rows every Result over one per-key outcome map shares, over <paramref name="harness"/>'s three keys.
    /// </summary>
    private static async Task RunOutcomeMapRow<TKey, TRow>(string row, OutcomeMapHarness<TKey, TRow> harness)
        where TRow : notnull
    {
        IReadOnlyList<TKey> keys = harness.Keys;
        switch (row)
        {
            case "all-succeed":
                {
                    FutureProbe first = harness.Key(keys[0]);
                    FutureProbe all = harness.All();
                    first.AssertPending();
                    all.AssertPending();

                    harness.SucceedEvery();

                    AssertReturned(await first.Outcome());
                    foreach (TKey key in keys)
                    {
                        AssertReturned(await harness.Key(key).Outcome());
                    }

                    AssertReturned(await all.Outcome());
                    break;
                }

            case "one-fails":
                {
                    // A key's failure is a VALUE of the one completion, and All() rethrows the core's stored aggregate — not
                    // the per-key error, which the binding does not compose.
                    KafkaException failure = new KafkaException(25, "the second key failed", isRetriable: false);
                    KafkaException aggregate = new KafkaException(25, "the request had a failed key", isRetriable: false);
                    harness.Settle(new KafkaException?[] { null, failure, null }, aggregate);

                    AssertReturned(await harness.Key(keys[0]).Outcome());
                    Assert.Same(failure, await harness.Key(keys[1]).Outcome());
                    AssertReturned(await harness.Key(keys[2]).Outcome());
                    Assert.Same(aggregate, await harness.All().Outcome());
                    break;
                }

            case "fails":
                {
                    // A call-level failure fails every member alike, with the call's own instance.
                    KafkaException failure = new KafkaException(7, "the request failed", isRetriable: true);
                    harness.Operation.SetException(failure);

                    foreach (TKey key in keys)
                    {
                        Assert.Same(failure, await harness.Key(key).Outcome());
                    }

                    Assert.Same(failure, await harness.All().Outcome());
                    break;
                }

            case "missing-row-at-get":
                {
                    // A requested key the completion has no row for — a core contract violation — fails at Get() with the
                    // dictionary's own exception rather than succeeding: the call itself succeeds, before completion.
                    FutureProbe? missing = null;
                    Assert.Null(Thrown(() => missing = harness.Key(keys[2])));
                    harness.Settle(new KafkaException?[] { null, null }, null);

                    KeyNotFoundException failure = Assert.IsType<KeyNotFoundException>(await missing!.Outcome());
                    Assert.Equal($"The given key '{harness.RowOf(keys[2])}' was not present in the dictionary.", failure.Message);
                    AssertReturned(await harness.Key(keys[0]).Outcome());
                    AssertReturned(await harness.All().Outcome());
                    break;
                }

            case "memoization":
                {
                    KafkaException failure = new KafkaException(25, "the second key failed", isRetriable: false);
                    KafkaException aggregate = new KafkaException(25, "the request had a failed key", isRetriable: false);
                    harness.Settle(new KafkaException?[] { null, failure, null }, aggregate);

                    // One future projects once, so every Get() of it rethrows the same instance ...
                    FutureProbe second = harness.Key(keys[1]);
                    Assert.Same(failure, await second.Outcome());
                    Assert.Same(failure, await second.Outcome());
                    FutureProbe all = harness.All();
                    Assert.Same(aggregate, await all.Outcome());
                    Assert.Same(aggregate, await all.Outcome());

                    // ... and each call is a new future, as Java allocates a fresh KafkaFutureImpl per call.
                    (bool fresh, FutureProbe first, FutureProbe again) = harness.TwoKeys(keys[1]);
                    Assert.True(fresh, "two per-key calls returned an equal future");
                    Assert.Same(failure, await first.Outcome());
                    Assert.Same(failure, await again.Outcome());
                    (bool freshAll, FutureProbe firstAll, FutureProbe secondAll) = harness.TwoAlls();
                    Assert.True(freshAll, "two All() calls returned an equal future");
                    Assert.Same(aggregate, await firstAll.Outcome());
                    Assert.Same(aggregate, await secondAll.Outcome());
                    break;
                }

            default:
                throw new ArgumentOutOfRangeException(nameof(row), row, "unknown row");
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

    /// <summary>
    /// The rows every keyed Result shares, over <paramref name="harness"/>'s three keys in their key order.
    /// </summary>
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

                    // ... and a new call is a new aggregate, as on both flavours before S4: it gathers afresh.
                    Assert.NotSame(gathered, await harness.All().Outcome());
                    break;
                }

            case "erasure-identity":
            case "key-identity":
                {
                    // A key's future is its key's own latch (or task): equal on every read, done the moment the key is.
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

    private static MemberToRemove[] Members() => new[]
    {
        new MemberToRemove("instance-1"),
        new MemberToRemove("instance-2"),
        new MemberToRemove("instance-3"),
    };

    /// <summary>
    /// Three ACL bindings, each differing from another in one field (M15/P14 S5d). Every call builds new instances, so a
    /// Result built over one call and read with another's looks its keys up by value.
    /// </summary>
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

    /// <summary>Three ACL filters, new instances on every call (see <see cref="Bindings"/>).</summary>
    private static AclBindingFilter[] Filters() => new[] { Filter("alpha"), Filter("beta"), Filter("gamma") };

    private static AclBindingFilter Filter(string name) =>
        new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.Topic, name, PatternType.Literal),
            new AccessControlEntryFilter("User:alice", "*", AclOperation.Read, AclPermissionType.Allow));

    /// <summary>
    /// Three quota entities — a named user, the default user (a null name) and a client id — new instances on every
    /// call (see <see cref="Bindings"/>).
    /// </summary>
    private static ClientQuotaEntity[] Entities() => new[]
    {
        Entity(ClientQuotaEntity.User, "alice"),
        Entity(ClientQuotaEntity.User, null),
        Entity(ClientQuotaEntity.ClientId, "alice"),
    };

    private static ClientQuotaEntity Entity(string entityType, string? name) =>
        new ClientQuotaEntity(new Dictionary<string, string?>(StringComparer.Ordinal) { [entityType] = name });

    /// <summary>What one filter matched (<see cref="DeleteAclsResult.FilterResults"/>), in the given order.</summary>
    private static DeleteAclsResult.FilterResults Matched(params DeleteAclsResult.FilterResult[] results) =>
        new DeleteAclsResult.FilterResults(results);

    private static DeleteAclsResult.FilterResult Deleted(AclBinding binding) => new DeleteAclsResult.FilterResult(binding, null);

    /// <summary>A matched binding whose delete failed: Java stores both (see <see cref="DeleteAclsResult.FilterResult"/>).</summary>
    private static DeleteAclsResult.FilterResult Failed(AclBinding binding, KafkaException failure) =>
        new DeleteAclsResult.FilterResult(binding, failure);

    private static GroupListing Listing(string groupId) =>
        new GroupListing(groupId, GroupType.Consumer, "consumer", GroupState.Stable);

    /// <summary>An election map in <paramref name="partitions"/>' order, each partition mapped to its outcome.</summary>
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

    private static TopicMetadataAndConfig Metadata(int numPartitions, int replicationFactor) =>
        new TopicMetadataAndConfig(Uuid.Zero, numPartitions, replicationFactor, Empty());

    private static void AssertReturned(object? outcome) => Assert.Same(BlockingCall.Returned, outcome);

    /// <summary>Runs a call-site action and returns what it threw, or <see langword="null"/>.</summary>
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

        private readonly CreateTopicsResult? _sync;
        private readonly AsyncCreateTopicsResult? _async;

        private CreateTopicsHarness(
            KeyedAdminOperation<string, TopicMetadataAndConfig> operation,
            CreateTopicsResult? sync,
            AsyncCreateTopicsResult? async)
        {
            Operation = operation;
            _sync = sync;
            _async = async;
        }

        internal KeyedAdminOperation<string, TopicMetadataAndConfig> Operation { get; }

        internal static CreateTopicsHarness For(string flavour)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            KeyedAdminOperation<string, TopicMetadataAndConfig> operation =
                new KeyedAdminOperation<string, TopicMetadataAndConfig>(
                    "createTopics", s_topics, StringComparer.Ordinal, adminFlavour);
            return adminFlavour == AdminFlavour.Sync
                ? new CreateTopicsHarness(operation, CreateTopicsResult.From(operation), null)
                : new CreateTopicsHarness(operation, null, AsyncCreateTopicsResult.From(operation));
        }

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
    /// A keyed Result of either flavour, driven through its operation's own <c>SetResult</c> / <c>SetException</c>:
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

        /// <summary>Whether the side of an exactly-one Result not requested is null (true for a one-sided Result).</summary>
        internal bool OtherSideIsNull { get; }

        internal abstract void Succeed(TKey key);

        internal abstract void Fail(TKey key, Exception failure);

        /// <summary>The outcome a succeeded key's future gives: its value, or <see cref="BlockingCall.Returned"/>.</summary>
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

    /// <summary>A Result whose per-key futures are void, over a <see cref="VoidKeyedAdminOperation{TKey}"/>.</summary>
    private sealed class VoidHarness<TKey> : KeyedHarness<TKey>
        where TKey : notnull
    {
        private readonly VoidKeyedAdminOperation<TKey> _operation;
        private readonly Func<IReadOnlyDictionary<TKey, KafkaFuture>>? _syncValues;
        private readonly Func<KafkaFuture>? _syncAll;
        private readonly Func<IReadOnlyDictionary<TKey, AsyncKafkaFuture>>? _asyncValues;
        private readonly Func<AsyncKafkaFuture>? _asyncAll;

        private VoidHarness(
            VoidKeyedAdminOperation<TKey> operation,
            IReadOnlyList<TKey> keys,
            bool otherSideIsNull,
            Func<IReadOnlyDictionary<TKey, KafkaFuture>>? syncValues,
            Func<KafkaFuture>? syncAll,
            Func<IReadOnlyDictionary<TKey, AsyncKafkaFuture>>? asyncValues,
            Func<AsyncKafkaFuture>? asyncAll)
            : base(keys, otherSideIsNull)
        {
            _operation = operation;
            _syncValues = syncValues;
            _syncAll = syncAll;
            _asyncValues = asyncValues;
            _asyncAll = asyncAll;
        }

        internal static VoidHarness<string> DeleteTopicsByName(string flavour, IReadOnlyList<string> topics)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            VoidKeyedAdminOperation<string> operation =
                new VoidKeyedAdminOperation<string>("deleteTopics", topics, StringComparer.Ordinal, adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                DeleteTopicsResult result = DeleteTopicsResult.From((operation, null));
                return new VoidHarness<string>(
                    operation, topics, result.TopicIdValues is null, () => result.TopicNameValues!, result.All, null, null);
            }

            AsyncDeleteTopicsResult asyncResult = AsyncDeleteTopicsResult.From((operation, null));
            return new VoidHarness<string>(
                operation,
                topics,
                asyncResult.TopicIdValues is null,
                null,
                null,
                () => asyncResult.TopicNameValues!,
                asyncResult.All);
        }

        internal static VoidHarness<Uuid> DeleteTopicsById(string flavour, IReadOnlyList<Uuid> ids)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            VoidKeyedAdminOperation<Uuid> operation =
                new VoidKeyedAdminOperation<Uuid>("deleteTopics", ids, EqualityComparer<Uuid>.Default, adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                DeleteTopicsResult result = DeleteTopicsResult.From((null, operation));
                return new VoidHarness<Uuid>(
                    operation, ids, result.TopicNameValues is null, () => result.TopicIdValues!, result.All, null, null);
            }

            AsyncDeleteTopicsResult asyncResult = AsyncDeleteTopicsResult.From((null, operation));
            return new VoidHarness<Uuid>(
                operation,
                ids,
                asyncResult.TopicNameValues is null,
                null,
                null,
                () => asyncResult.TopicIdValues!,
                asyncResult.All);
        }

        internal static VoidHarness<string> CreatePartitions(string flavour, IReadOnlyList<string> topics)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            VoidKeyedAdminOperation<string> operation =
                new VoidKeyedAdminOperation<string>("createPartitions", topics, StringComparer.Ordinal, adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                CreatePartitionsResult result = CreatePartitionsResult.From(operation);
                return new VoidHarness<string>(operation, topics, true, () => result.Values, result.All, null, null);
            }

            AsyncCreatePartitionsResult asyncResult = AsyncCreatePartitionsResult.From(operation);
            return new VoidHarness<string>(operation, topics, true, null, null, () => asyncResult.Values, asyncResult.All);
        }

        internal static VoidHarness<ConfigResource> IncrementalAlterConfigs(
            string flavour, IReadOnlyList<ConfigResource> resources)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            VoidKeyedAdminOperation<ConfigResource> operation = new VoidKeyedAdminOperation<ConfigResource>(
                "incrementalAlterConfigs", resources, EqualityComparer<ConfigResource>.Default, adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                AlterConfigsResult result = AlterConfigsResult.From(operation);
                return new VoidHarness<ConfigResource>(operation, resources, true, () => result.Values, result.All, null, null);
            }

            AsyncAlterConfigsResult asyncResult = AsyncAlterConfigsResult.From(operation);
            return new VoidHarness<ConfigResource>(
                operation, resources, true, null, null, () => asyncResult.Values, asyncResult.All);
        }

        internal static VoidHarness<TopicPartitionReplica> AlterReplicaLogDirs(
            string flavour, IReadOnlyList<TopicPartitionReplica> replicas)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            VoidKeyedAdminOperation<TopicPartitionReplica> operation = new VoidKeyedAdminOperation<TopicPartitionReplica>(
                "alterReplicaLogDirs", replicas, EqualityComparer<TopicPartitionReplica>.Default, adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                AlterReplicaLogDirsResult result = AlterReplicaLogDirsResult.From(operation);
                return new VoidHarness<TopicPartitionReplica>(
                    operation, replicas, true, () => result.Values, result.All, null, null);
            }

            AsyncAlterReplicaLogDirsResult asyncResult = AsyncAlterReplicaLogDirsResult.From(operation);
            return new VoidHarness<TopicPartitionReplica>(
                operation, replicas, true, null, null, () => asyncResult.Values, asyncResult.All);
        }

        internal static VoidHarness<TopicPartition> AlterPartitionReassignments(
            string flavour, IReadOnlyList<TopicPartition> partitions)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            VoidKeyedAdminOperation<TopicPartition> operation = new VoidKeyedAdminOperation<TopicPartition>(
                "alterPartitionReassignments", partitions, EqualityComparer<TopicPartition>.Default, adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                AlterPartitionReassignmentsResult result = AlterPartitionReassignmentsResult.From(operation);
                return new VoidHarness<TopicPartition>(
                    operation, partitions, true, () => result.Values, result.All, null, null);
            }

            AsyncAlterPartitionReassignmentsResult asyncResult = AsyncAlterPartitionReassignmentsResult.From(operation);
            return new VoidHarness<TopicPartition>(
                operation, partitions, true, null, null, () => asyncResult.Values, asyncResult.All);
        }

        internal static VoidHarness<string> DeleteConsumerGroups(string flavour, IReadOnlyList<string> groups)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            VoidKeyedAdminOperation<string> operation =
                new VoidKeyedAdminOperation<string>("deleteConsumerGroups", groups, StringComparer.Ordinal, adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                DeleteConsumerGroupsResult result = DeleteConsumerGroupsResult.From(operation);
                return new VoidHarness<string>(operation, groups, true, () => result.DeletedGroups, result.All, null, null);
            }

            AsyncDeleteConsumerGroupsResult asyncResult = AsyncDeleteConsumerGroupsResult.From(operation);
            return new VoidHarness<string>(
                operation, groups, true, null, null, () => asyncResult.DeletedGroups, asyncResult.All);
        }

        /// <summary>
        /// <c>CreateAcls</c> (M15/P14 S5d): the operation is built over <paramref name="requested"/>, and the rows read
        /// it with the value-equal <paramref name="keys"/>.
        /// </summary>
        internal static VoidHarness<AclBinding> CreateAcls(
            string flavour, IReadOnlyList<AclBinding> requested, IReadOnlyList<AclBinding> keys)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            VoidKeyedAdminOperation<AclBinding> operation = new VoidKeyedAdminOperation<AclBinding>(
                "createAcls", requested, EqualityComparer<AclBinding>.Default, adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                CreateAclsResult result = CreateAclsResult.From(operation);
                return new VoidHarness<AclBinding>(operation, keys, true, () => result.Values, result.All, null, null);
            }

            AsyncCreateAclsResult asyncResult = AsyncCreateAclsResult.From(operation);
            return new VoidHarness<AclBinding>(
                operation, keys, true, null, null, () => asyncResult.Values, asyncResult.All);
        }

        /// <summary>
        /// <c>AlterClientQuotas</c> (M15/P14 S5d): the operation is built over <paramref name="requested"/>, and the rows
        /// read it with the value-equal <paramref name="keys"/>.
        /// </summary>
        internal static VoidHarness<ClientQuotaEntity> AlterClientQuotas(
            string flavour, IReadOnlyList<ClientQuotaEntity> requested, IReadOnlyList<ClientQuotaEntity> keys)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            VoidKeyedAdminOperation<ClientQuotaEntity> operation = new VoidKeyedAdminOperation<ClientQuotaEntity>(
                "alterClientQuotas", requested, EqualityComparer<ClientQuotaEntity>.Default, adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                AlterClientQuotasResult result = AlterClientQuotasResult.From(operation);
                return new VoidHarness<ClientQuotaEntity>(operation, keys, true, () => result.Values, result.All, null, null);
            }

            AsyncAlterClientQuotasResult asyncResult = AsyncAlterClientQuotasResult.From(operation);
            return new VoidHarness<ClientQuotaEntity>(
                operation, keys, true, null, null, () => asyncResult.Values, asyncResult.All);
        }

        internal override void Succeed(TKey key) => _operation.SetResult(key, true);

        internal override void Fail(TKey key, Exception failure) => _operation.SetException(key, failure);

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

    /// <summary>A Result whose per-key futures carry a value, over a <see cref="KeyedAdminOperation{TKey, TValue}"/>.</summary>
    private sealed class ValuedHarness<TKey, TValue> : KeyedHarness<TKey>
        where TKey : notnull
        where TValue : class
    {
        private readonly KeyedAdminOperation<TKey, TValue> _operation;
        private readonly Dictionary<TKey, TValue> _values;
        private readonly Func<IReadOnlyDictionary<TKey, KafkaFuture<TValue>>>? _syncValues;
        private readonly Func<IReadOnlyDictionary<TKey, AsyncKafkaFuture<TValue>>>? _asyncValues;
        private readonly Func<FutureProbe> _all;
        private readonly bool _allGathers;

        private ValuedHarness(
            KeyedAdminOperation<TKey, TValue> operation,
            IReadOnlyList<TKey> keys,
            Func<TKey, TValue> value,
            bool otherSideIsNull,
            Func<IReadOnlyDictionary<TKey, KafkaFuture<TValue>>>? syncValues,
            Func<IReadOnlyDictionary<TKey, AsyncKafkaFuture<TValue>>>? asyncValues,
            Func<FutureProbe> all,
            bool allGathers)
            : base(keys, otherSideIsNull)
        {
            _operation = operation;
            _values = keys.ToDictionary(key => key, value);
            _syncValues = syncValues;
            _asyncValues = asyncValues;
            _all = all;
            _allGathers = allGathers;
        }

        internal static ValuedHarness<string, TopicDescription> DescribeTopicsByName(string flavour, IReadOnlyList<string> topics)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            KeyedAdminOperation<string, TopicDescription> operation =
                new KeyedAdminOperation<string, TopicDescription>("describeTopics", topics, StringComparer.Ordinal, adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                DescribeTopicsResult result = DescribeTopicsResult.From((operation, null));
                return new ValuedHarness<string, TopicDescription>(
                    operation,
                    topics,
                    Description,
                    result.TopicIdValues is null && result.AllTopicIds() is null,
                    () => result.TopicNameValues!,
                    null,
                    () => FutureProbe.Of(result.AllTopicNames()!.Value),
                    allGathers: true);
            }

            AsyncDescribeTopicsResult asyncResult = AsyncDescribeTopicsResult.From((operation, null));
            return new ValuedHarness<string, TopicDescription>(
                operation,
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
            KeyedAdminOperation<Uuid, TopicDescription> operation =
                new KeyedAdminOperation<Uuid, TopicDescription>("describeTopics", ids, EqualityComparer<Uuid>.Default, adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                DescribeTopicsResult result = DescribeTopicsResult.From((null, operation));
                return new ValuedHarness<Uuid, TopicDescription>(
                    operation,
                    ids,
                    id => Description(id.ToString()),
                    result.TopicNameValues is null && result.AllTopicNames() is null,
                    () => result.TopicIdValues!,
                    null,
                    () => FutureProbe.Of(result.AllTopicIds()!.Value),
                    allGathers: true);
            }

            AsyncDescribeTopicsResult asyncResult = AsyncDescribeTopicsResult.From((null, operation));
            return new ValuedHarness<Uuid, TopicDescription>(
                operation,
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
            KeyedAdminOperation<TopicPartition, DeletedRecords> operation =
                new KeyedAdminOperation<TopicPartition, DeletedRecords>(
                    "deleteRecords", partitions, EqualityComparer<TopicPartition>.Default, adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                DeleteRecordsResult result = DeleteRecordsResult.From(operation);
                return new ValuedHarness<TopicPartition, DeletedRecords>(
                    operation,
                    partitions,
                    partition => new DeletedRecords(100L + partition.Partition),
                    true,
                    () => result.LowWatermarks,
                    null,
                    () => FutureProbe.Of(result.All()),
                    allGathers: false);
            }

            AsyncDeleteRecordsResult asyncResult = AsyncDeleteRecordsResult.From(operation);
            return new ValuedHarness<TopicPartition, DeletedRecords>(
                operation,
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
            KeyedAdminOperation<ConfigResource, Config> operation = new KeyedAdminOperation<ConfigResource, Config>(
                "describeConfigs", resources, EqualityComparer<ConfigResource>.Default, adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                DescribeConfigsResult result = DescribeConfigsResult.From(operation);
                return new ValuedHarness<ConfigResource, Config>(
                    operation, resources, _ => Empty(), true, () => result.Values, null, () => FutureProbe.Of(result.All()), allGathers: true);
            }

            AsyncDescribeConfigsResult asyncResult = AsyncDescribeConfigsResult.From(operation);
            return new ValuedHarness<ConfigResource, Config>(
                operation,
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
            KeyedAdminOperation<int, IReadOnlyDictionary<string, LogDirDescription>> operation =
                new KeyedAdminOperation<int, IReadOnlyDictionary<string, LogDirDescription>>(
                    "describeLogDirs", brokers, EqualityComparer<int>.Default, adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                DescribeLogDirsResult result = DescribeLogDirsResult.From(operation);
                return new ValuedHarness<int, IReadOnlyDictionary<string, LogDirDescription>>(
                    operation,
                    brokers,
                    LogDirs,
                    true,
                    () => result.Descriptions,
                    null,
                    () => FutureProbe.Of(result.AllDescriptions()),
                    allGathers: true);
            }

            AsyncDescribeLogDirsResult asyncResult = AsyncDescribeLogDirsResult.From(operation);
            return new ValuedHarness<int, IReadOnlyDictionary<string, LogDirDescription>>(
                operation,
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
            KeyedAdminOperation<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo> operation =
                new KeyedAdminOperation<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo>(
                    "describeReplicaLogDirs", replicas, EqualityComparer<TopicPartitionReplica>.Default, adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                DescribeReplicaLogDirsResult result = DescribeReplicaLogDirsResult.From(operation);
                return new ValuedHarness<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo>(
                    operation,
                    replicas,
                    ReplicaLogDir,
                    true,
                    () => result.Values,
                    null,
                    () => FutureProbe.Of(result.All()),
                    allGathers: true);
            }

            AsyncDescribeReplicaLogDirsResult asyncResult = AsyncDescribeReplicaLogDirsResult.From(operation);
            return new ValuedHarness<TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo>(
                operation,
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
            AdminFlavour adminFlavour = FlavourOf(flavour);
            KeyedAdminOperation<string, ConsumerGroupDescription> operation =
                new KeyedAdminOperation<string, ConsumerGroupDescription>(
                    "describeConsumerGroups", groups, StringComparer.Ordinal, adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                DescribeConsumerGroupsResult result = DescribeConsumerGroupsResult.From(operation);
                return new ValuedHarness<string, ConsumerGroupDescription>(
                    operation,
                    groups,
                    ConsumerGroup,
                    true,
                    () => result.DescribedGroups,
                    null,
                    () => FutureProbe.Of(result.All()),
                    allGathers: true);
            }

            AsyncDescribeConsumerGroupsResult asyncResult = AsyncDescribeConsumerGroupsResult.From(operation);
            return new ValuedHarness<string, ConsumerGroupDescription>(
                operation,
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
            AdminFlavour adminFlavour = FlavourOf(flavour);
            KeyedAdminOperation<string, ClassicGroupDescription> operation =
                new KeyedAdminOperation<string, ClassicGroupDescription>(
                    "describeClassicGroups", groups, StringComparer.Ordinal, adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                DescribeClassicGroupsResult result = DescribeClassicGroupsResult.From(operation);
                return new ValuedHarness<string, ClassicGroupDescription>(
                    operation,
                    groups,
                    ClassicGroup,
                    true,
                    () => result.DescribedGroups,
                    null,
                    () => FutureProbe.Of(result.All()),
                    allGathers: true);
            }

            AsyncDescribeClassicGroupsResult asyncResult = AsyncDescribeClassicGroupsResult.From(operation);
            return new ValuedHarness<string, ClassicGroupDescription>(
                operation,
                groups,
                ClassicGroup,
                true,
                null,
                () => asyncResult.DescribedGroups,
                () => FutureProbe.Of(asyncResult.All()),
                allGathers: true);
        }

        internal override void Succeed(TKey key) => _operation.SetResult(key, _values[key]);

        internal override void Fail(TKey key, Exception failure) => _operation.SetException(key, failure);

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
    /// <see cref="AdminP14SyncCompositionTests.ListTopics_SyncAndAsyncResults_ComposeAlike"/>'s flavour adapter.
    /// </summary>
    private sealed class ListTopicsHarness
    {
        private readonly ListTopicsResult? _sync;
        private readonly AsyncListTopicsResult? _async;

        private ListTopicsHarness(
            SingleAdminOperation<IReadOnlyDictionary<string, TopicListing>> operation,
            ListTopicsResult? sync,
            AsyncListTopicsResult? async)
        {
            Operation = operation;
            _sync = sync;
            _async = async;
        }

        internal SingleAdminOperation<IReadOnlyDictionary<string, TopicListing>> Operation { get; }

        internal static ListTopicsHarness For(string flavour)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            SingleAdminOperation<IReadOnlyDictionary<string, TopicListing>> operation =
                new SingleAdminOperation<IReadOnlyDictionary<string, TopicListing>>("listTopics", adminFlavour);
            return adminFlavour == AdminFlavour.Sync
                ? new ListTopicsHarness(operation, ListTopicsResult.From(operation), null)
                : new ListTopicsHarness(operation, null, AsyncListTopicsResult.From(operation));
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
    /// <see cref="AdminP14SyncCompositionTests.DescribeCluster_SyncAndAsyncResults_ComposeAlike"/>'s flavour adapter.
    /// </summary>
    private sealed class DescribeClusterHarness
    {
        private readonly DescribeClusterResult? _sync;
        private readonly AsyncDescribeClusterResult? _async;

        private DescribeClusterHarness(
            SingleAdminOperation<DescribeClusterSnapshot> operation,
            DescribeClusterResult? sync,
            AsyncDescribeClusterResult? async)
        {
            Operation = operation;
            _sync = sync;
            _async = async;
        }

        internal SingleAdminOperation<DescribeClusterSnapshot> Operation { get; }

        internal static DescribeClusterHarness For(string flavour)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            SingleAdminOperation<DescribeClusterSnapshot> operation =
                new SingleAdminOperation<DescribeClusterSnapshot>("describeCluster", adminFlavour);
            return adminFlavour == AdminFlavour.Sync
                ? new DescribeClusterHarness(operation, DescribeClusterResult.From(operation), null)
                : new DescribeClusterHarness(operation, null, AsyncDescribeClusterResult.From(operation));
        }

        /// <summary><c>Nodes()</c>, <c>Controller()</c>, <c>ClusterId()</c> and <c>AuthorizedOperations()</c>, in that order.</summary>
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
            SingleAdminOperation<IReadOnlyCollection<ConfigResource>> operation,
            ListConfigResourcesResult? sync,
            AsyncListConfigResourcesResult? async)
        {
            Operation = operation;
            _sync = sync;
            _async = async;
        }

        internal SingleAdminOperation<IReadOnlyCollection<ConfigResource>> Operation { get; }

        internal static ListConfigResourcesHarness For(string flavour)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            SingleAdminOperation<IReadOnlyCollection<ConfigResource>> operation =
                new SingleAdminOperation<IReadOnlyCollection<ConfigResource>>("listConfigResources", adminFlavour);
            return adminFlavour == AdminFlavour.Sync
                ? new ListConfigResourcesHarness(operation, ListConfigResourcesResult.From(operation), null)
                : new ListConfigResourcesHarness(operation, null, AsyncListConfigResourcesResult.From(operation));
        }

        internal FutureProbe All() => _sync is not null ? FutureProbe.Of(_sync.All()) : FutureProbe.Of(_async!.All());

        internal bool AllEqualAcrossCalls() =>
            _sync is not null ? _sync.All() == _sync.All() : _async!.All() == _async.All();
    }

    /// <summary>
    /// <see cref="AdminP14SyncCompositionTests.ElectLeaders_SyncAndAsyncResults_ComposeAlike"/>'s flavour adapter.
    /// </summary>
    private sealed class ElectLeadersHarness
    {
        private readonly ElectLeadersResult? _sync;
        private readonly AsyncElectLeadersResult? _async;

        private ElectLeadersHarness(
            SingleAdminOperation<IReadOnlyDictionary<TopicPartition, KafkaException?>> operation,
            ElectLeadersResult? sync,
            AsyncElectLeadersResult? async)
        {
            Operation = operation;
            _sync = sync;
            _async = async;
        }

        internal SingleAdminOperation<IReadOnlyDictionary<TopicPartition, KafkaException?>> Operation { get; }

        internal static ElectLeadersHarness For(string flavour)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            SingleAdminOperation<IReadOnlyDictionary<TopicPartition, KafkaException?>> operation =
                new SingleAdminOperation<IReadOnlyDictionary<TopicPartition, KafkaException?>>("electLeaders", adminFlavour);
            return adminFlavour == AdminFlavour.Sync
                ? new ElectLeadersHarness(operation, ElectLeadersResult.From(operation), null)
                : new ElectLeadersHarness(operation, null, AsyncElectLeadersResult.From(operation));
        }

        internal FutureProbe Partitions() =>
            _sync is not null ? FutureProbe.Of(_sync.Partitions()) : FutureProbe.Of(_async!.Partitions());

        internal FutureProbe All() => _sync is not null ? FutureProbe.Of(_sync.All()) : FutureProbe.Of(_async!.All());

        internal bool PartitionsEqualAcrossCalls() =>
            _sync is not null ? _sync.Partitions() == _sync.Partitions() : _async!.Partitions() == _async.Partitions();

        /// <summary>
        /// Two <c>All()</c> calls: whether they are different futures, and both, so the caller observes each outcome
        /// (an async one left unobserved would fault on the finalizer).
        /// </summary>
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
            SingleAdminOperation<IReadOnlyDictionary<TopicPartition, PartitionReassignment>> operation,
            ListPartitionReassignmentsResult? sync,
            AsyncListPartitionReassignmentsResult? async)
        {
            Operation = operation;
            _sync = sync;
            _async = async;
        }

        internal SingleAdminOperation<IReadOnlyDictionary<TopicPartition, PartitionReassignment>> Operation { get; }

        internal static ListPartitionReassignmentsHarness For(string flavour)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            SingleAdminOperation<IReadOnlyDictionary<TopicPartition, PartitionReassignment>> operation =
                new SingleAdminOperation<IReadOnlyDictionary<TopicPartition, PartitionReassignment>>(
                    "listPartitionReassignments", adminFlavour);
            return adminFlavour == AdminFlavour.Sync
                ? new ListPartitionReassignmentsHarness(operation, ListPartitionReassignmentsResult.From(operation), null)
                : new ListPartitionReassignmentsHarness(operation, null, AsyncListPartitionReassignmentsResult.From(operation));
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
        private readonly KeyedAdminOperation<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> _operation;
        private readonly Dictionary<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> _values;
        private readonly ListOffsetsResult? _sync;
        private readonly AsyncListOffsetsResult? _async;

        private ListOffsetsHarness(
            KeyedAdminOperation<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> operation,
            IReadOnlyList<TopicPartition> partitions,
            ListOffsetsResult? sync,
            AsyncListOffsetsResult? async)
            : base(partitions, true)
        {
            _operation = operation;
            _values = partitions.ToDictionary(
                partition => partition,
                partition => new ListOffsetsResult.ListOffsetsResultInfo(100L + partition.Partition, -1L, null));
            _sync = sync;
            _async = async;
        }

        internal static ListOffsetsHarness For(string flavour, IReadOnlyList<TopicPartition> partitions)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            KeyedAdminOperation<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> operation =
                new KeyedAdminOperation<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo>(
                    "listOffsets", partitions, EqualityComparer<TopicPartition>.Default, adminFlavour);
            return adminFlavour == AdminFlavour.Sync
                ? new ListOffsetsHarness(operation, partitions, ListOffsetsResult.From(operation), null)
                : new ListOffsetsHarness(operation, partitions, null, AsyncListOffsetsResult.From(operation));
        }

        internal override void Succeed(TopicPartition key) => _operation.SetResult(key, _values[key]);

        internal override void Fail(TopicPartition key, Exception failure) => _operation.SetException(key, failure);

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
            SingleAdminOperation<(IReadOnlyCollection<GroupListing> Valid, IReadOnlyCollection<KafkaException> Errors)> operation,
            ListGroupsResult? sync,
            AsyncListGroupsResult? async)
        {
            Operation = operation;
            _sync = sync;
            _async = async;
        }

        internal SingleAdminOperation<(IReadOnlyCollection<GroupListing> Valid, IReadOnlyCollection<KafkaException> Errors)> Operation
        {
            get;
        }

        internal static ListGroupsHarness For(string flavour)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            SingleAdminOperation<(IReadOnlyCollection<GroupListing> Valid, IReadOnlyCollection<KafkaException> Errors)> operation =
                new SingleAdminOperation<(IReadOnlyCollection<GroupListing> Valid, IReadOnlyCollection<KafkaException> Errors)>(
                    "listGroups", adminFlavour);
            return adminFlavour == AdminFlavour.Sync
                ? new ListGroupsHarness(operation, ListGroupsResult.From(operation), null)
                : new ListGroupsHarness(operation, null, AsyncListGroupsResult.From(operation));
        }

        internal FutureProbe All() => _sync is not null ? FutureProbe.Of(_sync.All()) : FutureProbe.Of(_async!.All());

        internal FutureProbe Valid() => _sync is not null ? FutureProbe.Of(_sync.Valid()) : FutureProbe.Of(_async!.Valid());

        internal FutureProbe Errors() => _sync is not null ? FutureProbe.Of(_sync.Errors()) : FutureProbe.Of(_async!.Errors());

        /// <summary>
        /// Two <c>All()</c> calls: whether they are different futures, and both, so the caller observes each outcome
        /// (an async one left unobserved would fault on the finalizer).
        /// </summary>
        internal (bool Fresh, FutureProbe First, FutureProbe Second) TwoAlls()
        {
            if (_sync is not null)
            {
                KafkaFuture<IReadOnlyCollection<GroupListing>> first = _sync.All();
                KafkaFuture<IReadOnlyCollection<GroupListing>> second = _sync.All();
                return (first != second, FutureProbe.Of(first), FutureProbe.Of(second));
            }

            AsyncKafkaFuture<IReadOnlyCollection<GroupListing>> asyncFirst = _async!.All();
            AsyncKafkaFuture<IReadOnlyCollection<GroupListing>> asyncSecond = _async.All();
            return (asyncFirst != asyncSecond, FutureProbe.Of(asyncFirst), FutureProbe.Of(asyncSecond));
        }

        /// <summary>Whether <c>Valid()</c> and <c>Errors()</c> each hand out a new future per call (neither fails).</summary>
        internal bool ValidAndErrorsFreshPerCall() =>
            _sync is not null
                ? _sync.Valid() != _sync.Valid() && _sync.Errors() != _sync.Errors()
                : _async!.Valid() != _async.Valid() && _async.Errors() != _async.Errors();
    }

    /// <summary>
    /// <see cref="AdminP14SyncCompositionTests.ListConsumerGroupOffsets_SyncAndAsyncResults_ComposeAlike"/>'s flavour
    /// adapter: a keyed Result with no per-key dictionary, so its per-key member is
    /// <c>PartitionsToOffsetAndMetadata(groupId)</c>, and the keys it answers for are those that member does not reject.
    /// </summary>
    private sealed class ListConsumerGroupOffsetsHarness : KeyedHarness<string>
    {
        private readonly KeyedAdminOperation<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> _operation;
        private readonly Dictionary<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> _values;
        private readonly ListConsumerGroupOffsetsResult? _sync;
        private readonly AsyncListConsumerGroupOffsetsResult? _async;

        private ListConsumerGroupOffsetsHarness(
            KeyedAdminOperation<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> operation,
            IReadOnlyList<string> groups,
            ListConsumerGroupOffsetsResult? sync,
            AsyncListConsumerGroupOffsetsResult? async)
            : base(groups, true)
        {
            _operation = operation;

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
            AdminFlavour adminFlavour = FlavourOf(flavour);
            KeyedAdminOperation<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> operation =
                new KeyedAdminOperation<string, IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>>(
                    "listConsumerGroupOffsets", groups, StringComparer.Ordinal, adminFlavour);
            return adminFlavour == AdminFlavour.Sync
                ? new ListConsumerGroupOffsetsHarness(operation, groups, ListConsumerGroupOffsetsResult.From(operation), null)
                : new ListConsumerGroupOffsetsHarness(operation, groups, null, AsyncListConsumerGroupOffsetsResult.From(operation));
        }

        internal override void Succeed(string key) => _operation.SetResult(key, _values[key]);

        internal override void Fail(string key, Exception failure) => _operation.SetException(key, failure);

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

        /// <summary>The no-argument <c>PartitionsToOffsetAndMetadata()</c>.</summary>
        internal FutureProbe NoArgument() =>
            _sync is not null
                ? FutureProbe.Of(_sync.PartitionsToOffsetAndMetadata())
                : FutureProbe.Of(_async!.PartitionsToOffsetAndMetadata());

        internal Exception? CallNoArgument() => Thrown(() => _ = NoArgument());

        /// <summary>Whether the no-argument form is <paramref name="key"/>'s own future.</summary>
        internal bool NoArgumentIsTheGroupsOwn(string key) =>
            _sync is not null
                ? _sync.PartitionsToOffsetAndMetadata() == _sync.PartitionsToOffsetAndMetadata(key)
                : _async!.PartitionsToOffsetAndMetadata() == _async.PartitionsToOffsetAndMetadata(key);

        private bool Answers(string key) => CallIndexer(key) is null;
    }

    /// <summary>
    /// <see cref="AdminP14SyncCompositionTests.DeleteAcls_SyncAndAsyncResults_ComposeAlike"/>'s flavour adapter
    /// (M15/P14 S5d): the per-filter futures, and an <c>All()</c> that flattens every filter's matched bindings in
    /// filter order. A succeeded filter matched two bindings unless the row hands it its own results.
    /// </summary>
    private sealed class DeleteAclsHarness : KeyedHarness<AclBindingFilter>
    {
        private readonly KeyedAdminOperation<AclBindingFilter, DeleteAclsResult.FilterResults> _operation;
        private readonly Dictionary<AclBindingFilter, DeleteAclsResult.FilterResults> _values;
        private readonly DeleteAclsResult? _sync;
        private readonly AsyncDeleteAclsResult? _async;

        private DeleteAclsHarness(
            KeyedAdminOperation<AclBindingFilter, DeleteAclsResult.FilterResults> operation,
            IReadOnlyList<AclBindingFilter> keys,
            DeleteAclsResult? sync,
            AsyncDeleteAclsResult? async)
            : base(keys, otherSideIsNull: true)
        {
            _operation = operation;
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

        /// <summary>
        /// The operation is built over <paramref name="requested"/>, and the rows read it with the value-equal
        /// <paramref name="keys"/>.
        /// </summary>
        internal static DeleteAclsHarness For(
            string flavour, IReadOnlyList<AclBindingFilter> requested, IReadOnlyList<AclBindingFilter> keys)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            KeyedAdminOperation<AclBindingFilter, DeleteAclsResult.FilterResults> operation =
                new KeyedAdminOperation<AclBindingFilter, DeleteAclsResult.FilterResults>(
                    "deleteAcls", requested, EqualityComparer<AclBindingFilter>.Default, adminFlavour);
            return adminFlavour == AdminFlavour.Sync
                ? new DeleteAclsHarness(operation, keys, DeleteAclsResult.From(operation), null)
                : new DeleteAclsHarness(operation, keys, null, AsyncDeleteAclsResult.From(operation));
        }

        /// <summary>The bindings a filter's default results matched, in their order.</summary>
        internal AclBinding[] Bindings(AclBindingFilter key) => _values[key].Values.Select(result => result.Binding!).ToArray();

        internal void Succeed(AclBindingFilter key, DeleteAclsResult.FilterResults results) =>
            _operation.SetResult(key, results);

        internal override void Succeed(AclBindingFilter key) => _operation.SetResult(key, _values[key]);

        internal override void Fail(AclBindingFilter key, Exception failure) => _operation.SetException(key, failure);

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
    /// A Result over one future and no keys, of either flavour (M15/P14 S5d) — <see cref="DescribeAclsResult"/>'s
    /// <c>Values()</c> or <see cref="DescribeClientQuotasResult"/>'s <c>Entities()</c> — driven through its operation's
    /// own <c>SetResult</c> / <c>SetException</c>.
    /// </summary>
    private sealed class SingleHarness<T>
        where T : class
    {
        private readonly Func<FutureProbe> _future;
        private readonly Func<bool> _equalAcrossCalls;

        private SingleHarness(SingleAdminOperation<T> operation, Func<FutureProbe> future, Func<bool> equalAcrossCalls)
        {
            Operation = operation;
            _future = future;
            _equalAcrossCalls = equalAcrossCalls;
        }

        internal SingleAdminOperation<T> Operation { get; }

        internal static SingleHarness<IReadOnlyCollection<AclBinding>> DescribeAcls(string flavour)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            SingleAdminOperation<IReadOnlyCollection<AclBinding>> operation =
                new SingleAdminOperation<IReadOnlyCollection<AclBinding>>("describeAcls", adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                DescribeAclsResult result = DescribeAclsResult.From(operation);
                return new SingleHarness<IReadOnlyCollection<AclBinding>>(
                    operation, () => FutureProbe.Of(result.Values()), () => result.Values() == result.Values());
            }

            AsyncDescribeAclsResult asyncResult = AsyncDescribeAclsResult.From(operation);
            return new SingleHarness<IReadOnlyCollection<AclBinding>>(
                operation, () => FutureProbe.Of(asyncResult.Values()), () => asyncResult.Values() == asyncResult.Values());
        }

        internal static SingleHarness<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>
            DescribeClientQuotas(string flavour)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            SingleAdminOperation<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>> operation =
                new SingleAdminOperation<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>(
                    "describeClientQuotas", adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                DescribeClientQuotasResult result = DescribeClientQuotasResult.From(operation);
                return new SingleHarness<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>(
                    operation, () => FutureProbe.Of(result.Entities()), () => result.Entities() == result.Entities());
            }

            AsyncDescribeClientQuotasResult asyncResult = AsyncDescribeClientQuotasResult.From(operation);
            return new SingleHarness<IReadOnlyDictionary<ClientQuotaEntity, IReadOnlyDictionary<string, double>>>(
                operation,
                () => FutureProbe.Of(asyncResult.Entities()),
                () => asyncResult.Entities() == asyncResult.Entities());
        }

        internal FutureProbe Future() => _future();

        internal bool EqualAcrossCalls() => _equalAcrossCalls();
    }

    /// <summary>
    /// A Result over one completion carrying a per-key outcome map and the core's own aggregate —
    /// <c>AlterConsumerGroupOffsets</c>, <c>DeleteConsumerGroupOffsets</c> and <c>RemoveMembersFromConsumerGroup</c>
    /// (M15/P14 S5c) — of either flavour: its per-key member (<c>PartitionResult</c> / <c>MemberResult</c>) and
    /// <c>All()</c>, both void projections of that completion.
    /// </summary>
    /// <typeparam name="TKey">What the per-key member takes.</typeparam>
    /// <typeparam name="TRow">What the completion's outcome map is keyed by.</typeparam>
    private sealed class OutcomeMapHarness<TKey, TRow>
        where TRow : notnull
    {
        private readonly Func<TKey, TRow> _rowOf;
        private readonly Func<TKey, KafkaFuture>? _syncKey;
        private readonly Func<KafkaFuture>? _syncAll;
        private readonly Func<TKey, AsyncKafkaFuture>? _asyncKey;
        private readonly Func<AsyncKafkaFuture>? _asyncAll;

        private OutcomeMapHarness(
            SingleAdminOperation<(IReadOnlyDictionary<TRow, KafkaException?> PerKey, KafkaException? All)> operation,
            IReadOnlyList<TKey> keys,
            Func<TKey, TRow> rowOf,
            Func<TKey, KafkaFuture>? syncKey,
            Func<KafkaFuture>? syncAll,
            Func<TKey, AsyncKafkaFuture>? asyncKey,
            Func<AsyncKafkaFuture>? asyncAll)
        {
            Operation = operation;
            Keys = keys;
            _rowOf = rowOf;
            _syncKey = syncKey;
            _syncAll = syncAll;
            _asyncKey = asyncKey;
            _asyncAll = asyncAll;
        }

        internal SingleAdminOperation<(IReadOnlyDictionary<TRow, KafkaException?> PerKey, KafkaException? All)> Operation { get; }

        internal IReadOnlyList<TKey> Keys { get; }

        internal static OutcomeMapHarness<TopicPartition, TopicPartition> AlterConsumerGroupOffsets(
            string flavour, IReadOnlyList<TopicPartition> partitions)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            SingleAdminOperation<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)> operation =
                new SingleAdminOperation<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)>(
                    "alterConsumerGroupOffsets", adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                AlterConsumerGroupOffsetsResult result = AlterConsumerGroupOffsetsResult.From(operation);
                return new OutcomeMapHarness<TopicPartition, TopicPartition>(
                    operation, partitions, static partition => partition, result.PartitionResult, result.All, null, null);
            }

            AsyncAlterConsumerGroupOffsetsResult asyncResult = AsyncAlterConsumerGroupOffsetsResult.From(operation);
            return new OutcomeMapHarness<TopicPartition, TopicPartition>(
                operation, partitions, static partition => partition, null, null, asyncResult.PartitionResult, asyncResult.All);
        }

        internal static OutcomeMapHarness<TopicPartition, TopicPartition> DeleteConsumerGroupOffsets(
            string flavour, IReadOnlyList<TopicPartition> partitions)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            SingleAdminOperation<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)> operation =
                new SingleAdminOperation<(IReadOnlyDictionary<TopicPartition, KafkaException?> PerKey, KafkaException? All)>(
                    "deleteConsumerGroupOffsets", adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                DeleteConsumerGroupOffsetsResult result = DeleteConsumerGroupOffsetsResult.From((operation, partitions));
                return new OutcomeMapHarness<TopicPartition, TopicPartition>(
                    operation, partitions, static partition => partition, result.PartitionResult, result.All, null, null);
            }

            AsyncDeleteConsumerGroupOffsetsResult asyncResult = AsyncDeleteConsumerGroupOffsetsResult.From((operation, partitions));
            return new OutcomeMapHarness<TopicPartition, TopicPartition>(
                operation, partitions, static partition => partition, null, null, asyncResult.PartitionResult, asyncResult.All);
        }

        internal static OutcomeMapHarness<MemberToRemove, string> RemoveMembersFromConsumerGroup(
            string flavour, IReadOnlyList<MemberToRemove> members)
        {
            AdminFlavour adminFlavour = FlavourOf(flavour);
            SingleAdminOperation<(IReadOnlyDictionary<string, KafkaException?> PerKey, KafkaException? All)> operation =
                new SingleAdminOperation<(IReadOnlyDictionary<string, KafkaException?> PerKey, KafkaException? All)>(
                    "removeMembersFromConsumerGroup", adminFlavour);
            if (adminFlavour == AdminFlavour.Sync)
            {
                RemoveMembersFromConsumerGroupResult result = RemoveMembersFromConsumerGroupResult.From((operation, members));
                return new OutcomeMapHarness<MemberToRemove, string>(
                    operation, members, static member => member.GroupInstanceId, result.MemberResult, result.All, null, null);
            }

            AsyncRemoveMembersFromConsumerGroupResult asyncResult =
                AsyncRemoveMembersFromConsumerGroupResult.From((operation, members));
            return new OutcomeMapHarness<MemberToRemove, string>(
                operation, members, static member => member.GroupInstanceId, null, null, asyncResult.MemberResult, asyncResult.All);
        }

        internal TRow RowOf(TKey key) => _rowOf(key);

        /// <summary>
        /// Completes the operation with the first <paramref name="outcomes"/>.Length keys, in order, mapped to their
        /// outcomes (a shorter array leaves the later keys without a row), and <paramref name="all"/> as the core's
        /// aggregate.
        /// </summary>
        internal void Settle(KafkaException?[] outcomes, KafkaException? all)
        {
            Dictionary<TRow, KafkaException?> perKey = new Dictionary<TRow, KafkaException?>();
            for (int index = 0; index < outcomes.Length; index++)
            {
                perKey.Add(_rowOf(Keys[index]), outcomes[index]);
            }

            Operation.SetResult((perKey, all));
        }

        /// <summary>Completes the operation with every key succeeded and no aggregate failure.</summary>
        internal void SucceedEvery() => Settle(new KafkaException?[Keys.Count], null);

        internal FutureProbe Key(TKey key) => _syncKey is not null ? FutureProbe.Of(_syncKey(key)) : FutureProbe.Of(_asyncKey!(key));

        internal Exception? CallKey(TKey key) => Thrown(() => _ = Key(key));

        internal FutureProbe All() => _syncAll is not null ? FutureProbe.Of(_syncAll()) : FutureProbe.Of(_asyncAll!());

        /// <summary>
        /// Two per-key calls for <paramref name="key"/>: whether they are different futures, and both, so the caller
        /// observes each outcome.
        /// </summary>
        internal (bool Fresh, FutureProbe First, FutureProbe Second) TwoKeys(TKey key)
        {
            if (_syncKey is not null)
            {
                KafkaFuture first = _syncKey(key);
                KafkaFuture second = _syncKey(key);
                return (first != second, FutureProbe.Of(first), FutureProbe.Of(second));
            }

            AsyncKafkaFuture asyncFirst = _asyncKey!(key);
            AsyncKafkaFuture asyncSecond = _asyncKey(key);
            return (asyncFirst != asyncSecond, FutureProbe.Of(asyncFirst), FutureProbe.Of(asyncSecond));
        }

        /// <summary>Two <c>All()</c> calls: whether they are different futures, and both.</summary>
        internal (bool Fresh, FutureProbe First, FutureProbe Second) TwoAlls()
        {
            if (_syncAll is not null)
            {
                KafkaFuture first = _syncAll();
                KafkaFuture second = _syncAll();
                return (first != second, FutureProbe.Of(first), FutureProbe.Of(second));
            }

            AsyncKafkaFuture asyncFirst = _asyncAll!();
            AsyncKafkaFuture asyncSecond = _asyncAll();
            return (asyncFirst != asyncSecond, FutureProbe.Of(asyncFirst), FutureProbe.Of(asyncSecond));
        }
    }
}
