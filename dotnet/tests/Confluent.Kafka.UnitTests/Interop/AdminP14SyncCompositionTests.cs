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

            case "memoization":
                {
                    ValuedHarness<string, TopicDescription> harness =
                        ValuedHarness<string, TopicDescription>.DescribeTopicsByName(flavour, s_topics);
                    foreach (string topic in s_topics)
                    {
                        harness.Succeed(topic);
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
}
