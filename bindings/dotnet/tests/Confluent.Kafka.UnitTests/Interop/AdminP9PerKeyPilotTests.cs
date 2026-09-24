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
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M15/P9 CP2 — <c>createTopics</c> (shape <b>4a</b>) and <c>deleteTopics</c>-by-name
/// (shape <b>4b</b>) driven end to end over the <em>real</em> per-key ABI, where each key
/// resolves from its own callback, fired as that key's future resolves — possibly
/// concurrently, on a core thread, in any order, with no result root anywhere.
/// </summary>
public sealed class AdminP9PerKeyPilotTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The C code Kafka assigns to <c>INVALID_REPLICATION_FACTOR</c>.</summary>
    private const int InvalidReplicationFactorCode = 38;

    /// <summary>The C code Kafka assigns to <c>UNKNOWN_TOPIC_OR_PARTITION</c>.</summary>
    private const int UnknownTopicOrPartitionCode = 3;

    /// <summary>
    /// Shape 4a, over real per-key callbacks: one key succeeding and another failing must
    /// resolve <b>independently</b>. The discriminator is an implementation that lets the
    /// last callback to arrive decide every key's outcome — which is what a walker reused
    /// against a per-key ABI would do.
    /// </summary>
    [Fact]
    public async Task CreateTopics_PerKeyOutcomes_AreIndependent()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        // Replication factor 5 cannot be satisfied by 1 broker, so the same submit
        // carries one success and one failure.
        CreateTopicsResult result = admin.CreateTopics(
            new[] { new NewTopic("cp2-good", 3, 1), new NewTopic("cp2-bad", 1, 5) });

        await TestTimeout.Run(() => result.Values["cp2-good"], s_deadline);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.Values["cp2-bad"], s_deadline));
        Assert.Equal(InvalidReplicationFactorCode, failure.Code);

        // The copied-out value outlives the owned native payload the callback destroyed.
        Assert.Equal(3, await TestTimeout.Run(() => result.NumPartitions("cp2-good"), s_deadline));
        Assert.Equal(1, await TestTimeout.Run(() => result.ReplicationFactor("cp2-good"), s_deadline));
        Assert.NotEqual(
            Uuid.Zero, await TestTimeout.Run(() => result.TopicId("cp2-good"), s_deadline));

        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(result.All, s_deadline));
    }

    /// <summary>
    /// Shape 4b, over real per-key callbacks: a <b>null</b> per-key error <em>is</em> the
    /// success value, and a key that fails faults only itself.
    /// </summary>
    [Fact]
    public async Task DeleteTopicsByName_PerKeyOutcomes_AreIndependent()
    {
        using MockAdminClient admin = new MockAdminClient(1);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("cp2-present", 1, 1) }).All(),
            s_deadline);

        DeleteTopicsResult result = admin.DeleteTopics(
            TopicCollection.OfTopicNames(new[] { "cp2-present", "cp2-absent" }));

        await TestTimeout.Run(() => result.TopicNameValues!["cp2-present"], s_deadline);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.TopicNameValues!["cp2-absent"], s_deadline));
        Assert.Equal(UnknownTopicOrPartitionCode, failure.Code);
    }

    /// <summary>
    /// The <c>n == 0</c> rule at the RPC level: an empty collection fires <b>no</b>
    /// callback at all, so only the submit's own countdown slot can release the operation.
    /// Without it the <c>GCHandle</c> and the span-the-op reference are held forever and
    /// <c>AdminClient_destroy</c> never runs — a leak the countdown introduces and this
    /// asserts away for both pilot RPCs.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void EmptyCollection_ResolvesAtTheSubmitBoundary_AndReleasesTheClient(bool create)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        if (create)
        {
            Assert.Empty(admin.CreateTopics(Array.Empty<NewTopic>(), options: null).Values);
        }
        else
        {
            Assert.Empty(
                admin.DeleteTopics(TopicCollection.OfTopicNames(Array.Empty<string>()), options: null)
                    .TopicNameValues!);
        }

        TestTimeout.Run(admin.Dispose, s_deadline);

        Assert.True(
            handle.IsClosed,
            "zero callbacks must still release: the submit token is the only thing that can");
    }

    /// <summary>
    /// Many keys per operation, over many operations, leave the reference count
    /// <b>balanced</b> — the countdown's core claim once arrivals are concurrent. An
    /// over-release would have thrown out of the
    /// <see cref="System.Runtime.InteropServices.SafeHandle"/>; an under-release leaves
    /// <c>IsClosed</c> false forever.
    /// </summary>
    [Fact]
    public async Task ManyKeysPerOperation_LeaveTheReferenceCountBalanced()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        for (int round = 0; round < 5; round++)
        {
            List<NewTopic> topics = new List<NewTopic>();
            for (int i = 0; i < 25; i++)
            {
                topics.Add(new NewTopic($"cp2-balance-{round}-{i}", 1, 1));
            }

            CreateTopicsResult created = admin.CreateTopics(topics, options: null);
            Assert.Equal(25, created.Values.Count);
            await TestTimeout.Run(created.All, s_deadline);

            DeleteTopicsResult deleted = admin.DeleteTopics(
                TopicCollection.OfTopicNames(topics.ConvertAll(topic => topic.Name)),
                options: null);
            Assert.Equal(25, deleted.TopicNameValues!.Count);
            await TestTimeout.Run(deleted.All, s_deadline);

            Assert.False(handle.IsClosed, "the client is still alive between operations");
        }

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "250 per-key callbacks must leave the count balanced");
    }
}
