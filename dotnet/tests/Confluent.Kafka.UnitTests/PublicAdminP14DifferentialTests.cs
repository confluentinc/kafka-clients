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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The differential rows of M15/P14 S4: each converted RPC, called with the same arguments on a
/// <see cref="MockAdminClient"/> and an <see cref="AsyncMockAdminClient"/> seeded with the same mock state, gives the
/// same outcome through every member of its Result — equal values, or an exception of the same type with the same
/// message (and, for a <see cref="KafkaException"/>, the same code). Each row also pins the outcome itself, so two
/// flavours that agreed on a wrong answer would still fail.
/// </summary>
/// <remarks>
/// Every wait is bounded (<see cref="FutureProbe"/>): a sync <c>Get()</c> runs on a dedicated thread, an async one
/// under <see cref="TestTimeout"/>.
/// </remarks>
public sealed class PublicAdminP14DifferentialTests
{
    // Errors.INVALID_REPLICATION_FACTOR and Errors.TOPIC_ALREADY_EXISTS.
    private const int InvalidReplicationFactorCode = 38;
    private const int TopicAlreadyExistsCode = 36;

    /// <summary>
    /// <c>CreateTopics</c>: a creatable topic and one whose replication factor exceeds the broker count, then the
    /// creatable one again (now existing). Values, <c>All()</c> and the four typed accessors agree on both mocks. The
    /// topic id is minted by each mock independently, so only its presence is compared.
    /// </summary>
    [Fact]
    public async Task CreateTopics_OnBothMocks_GivesEqualOutcomes()
    {
        using MockAdminClient sync = new MockAdminClient(1);
        using AsyncMockAdminClient async = new AsyncMockAdminClient(1);
        NewTopic[] request = { new NewTopic("diff-ok", 3, 1), new NewTopic("diff-bad", 1, 5) };

        CreateTopicsResult syncResult = sync.CreateTopics(request);
        AsyncCreateTopicsResult asyncResult = async.CreateTopics(request);

        Assert.Equal(syncResult.Values.Keys, asyncResult.Values.Keys);

        Assert.Same(
            BlockingCall.Returned,
            await Agree(FutureProbe.Of(syncResult.Values["diff-ok"]), FutureProbe.Of(asyncResult.Values["diff-ok"])));
        Assert.Equal(
            (object)3,
            await Agree(FutureProbe.Of(syncResult.NumPartitions("diff-ok")), FutureProbe.Of(asyncResult.NumPartitions("diff-ok"))));
        Assert.Equal(
            (object)1,
            await Agree(
                FutureProbe.Of(syncResult.ReplicationFactor("diff-ok")), FutureProbe.Of(asyncResult.ReplicationFactor("diff-ok"))));
        Config syncConfig = Assert.IsType<Config>(await FutureProbe.Of(syncResult.Config("diff-ok")).Outcome());
        Config asyncConfig = Assert.IsType<Config>(await FutureProbe.Of(asyncResult.Config("diff-ok")).Outcome());
        Assert.Equal(Entries(syncConfig), Entries(asyncConfig));
        Assert.NotEqual((object)Uuid.Zero, await FutureProbe.Of(syncResult.TopicId("diff-ok")).Outcome());
        Assert.NotEqual((object)Uuid.Zero, await FutureProbe.Of(asyncResult.TopicId("diff-ok")).Outcome());

        const string TooLarge = "Replication factor: 5 is larger than brokers: 1";
        AssertKafka(
            InvalidReplicationFactorCode,
            TooLarge,
            await Agree(FutureProbe.Of(syncResult.Values["diff-bad"]), FutureProbe.Of(asyncResult.Values["diff-bad"])));
        AssertKafka(
            InvalidReplicationFactorCode,
            TooLarge,
            await Agree(FutureProbe.Of(syncResult.NumPartitions("diff-bad")), FutureProbe.Of(asyncResult.NumPartitions("diff-bad"))));
        AssertKafka(
            InvalidReplicationFactorCode,
            TooLarge,
            await Agree(FutureProbe.Of(syncResult.All()), FutureProbe.Of(asyncResult.All())));

        CreateTopicsResult syncAgain = sync.CreateTopics(new[] { new NewTopic("diff-ok", 3, 1) });
        AsyncCreateTopicsResult asyncAgain = async.CreateTopics(new[] { new NewTopic("diff-ok", 3, 1) });
        AssertKafka(
            TopicAlreadyExistsCode,
            "Topic diff-ok exists already.",
            await Agree(FutureProbe.Of(syncAgain.All()), FutureProbe.Of(asyncAgain.All())));
    }

    /// <summary>
    /// Runs both probes to their outcomes and asserts they agree: equal values, or exceptions of the same type with
    /// the same message (and code). Returns the sync outcome for the row's own pin.
    /// </summary>
    private static async Task<object?> Agree(FutureProbe sync, FutureProbe async)
    {
        object? syncOutcome = await sync.Outcome();
        object? asyncOutcome = await async.Outcome();

        if (syncOutcome is Exception syncFailure)
        {
            Exception asyncFailure = Assert.IsAssignableFrom<Exception>(asyncOutcome);
            Assert.Equal(syncFailure.GetType(), asyncFailure.GetType());
            Assert.Equal(syncFailure.Message, asyncFailure.Message);
            if (syncFailure is KafkaException syncKafka)
            {
                KafkaException asyncKafka = (KafkaException)asyncFailure;
                Assert.Equal(syncKafka.Code, asyncKafka.Code);
                Assert.Equal(syncKafka.IsRetriable, asyncKafka.IsRetriable);
            }
        }
        else if (ReferenceEquals(syncOutcome, BlockingCall.Returned))
        {
            Assert.Same(BlockingCall.Returned, asyncOutcome);
        }
        else
        {
            Assert.Equal(syncOutcome, asyncOutcome);
        }

        return syncOutcome;
    }

    private static void AssertKafka(int code, string message, object? outcome)
    {
        KafkaException failure = Assert.IsType<KafkaException>(outcome);
        Assert.Equal(code, failure.Code);
        Assert.Equal(message, failure.Message);
    }

    private static List<string> Entries(Config config) =>
        config.Entries.Select(static entry => entry.Name + "=" + entry.Value).OrderBy(static e => e, StringComparer.Ordinal).ToList();
}
