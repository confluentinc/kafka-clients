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
/// waits for every key); memoization (a projection is applied once per future, so its failure is the same instance
/// on every <c>Get()</c>); and erasure identity (a key's void future is equal on every read and completes with its
/// typed one).
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
}
