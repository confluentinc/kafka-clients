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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A negative partition now reaches the admin core (M15/P13.2 G3-4): the binding adds no guard
/// of its own, because <see cref="TopicPartition"/> stores one as Java's does
/// (<c>TopicPartition.java:32-35</c>), and the core answers it as Java's client does — per key
/// for <c>alterPartitionReassignments</c>, for the whole request for
/// <c>listPartitionReassignments</c>.
/// </summary>
/// <remarks>
/// <para>
/// The mock test is also the proof that a negative key <b>round-trips the per-key callback
/// echo</b>: the result reader rebuilds each key from the native row
/// (<c>AdminCallbacks.s_topicPartitionKey</c>), so a reader that still rejected a negative
/// partition would throw inside the no-throw callback boundary and leave the key's task
/// pending — which the bounded wait here turns into a failure rather than a hang.
/// </para>
/// <para>
/// The real-client tests need no broker: both answers are client-side in the core, before
/// anything is submitted (<c>kafka_admin_client.rs</c> <c>alter_partition_reassignments</c>
/// "partition &lt; 0" arm and <c>list_partition_reassignments</c>' client-side validation,
/// mirroring <c>KafkaAdminClient.java</c>), so neither outcome depends on the bootstrap address
/// being reachable.
/// </para>
/// </remarks>
public sealed class PublicAdminNegativePartitionTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary><c>Errors.UNKNOWN_TOPIC_OR_PARTITION</c>.</summary>
    private const int UnknownTopicOrPartitionCode = 3;

    /// <summary><c>Errors.INVALID_TOPIC_EXCEPTION</c>.</summary>
    private const int InvalidTopicCode = 17;

    /// <summary>The core's default message for <c>UNKNOWN_TOPIC_OR_PARTITION</c>.</summary>
    private const string UnknownTopicOrPartitionMessage = "This server does not host this topic-partition.";

    /// <summary>The core's client-side message for a negative partition (Java's text).</summary>
    private const string InvalidPartitionMessage = "The given partition index -1 is not valid.";

    [Fact]
    public async Task Mock_AlterPartitionReassignments_ANegativeKey_FaultsOnlyItself_AndRoundTripsTheKey()
    {
        await using MockAdminClient admin = new MockAdminClient(3);
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("neg-apr", 2, 1) }).All(), s_deadline);

        TopicPartition negative = new TopicPartition("neg-apr", -1);
        TopicPartition zero = new TopicPartition("neg-apr", 0);

        // No synchronous throw: the binding sends the negative key to the core.
        AlterPartitionReassignmentsResult result = admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>
            {
                [negative] = new NewPartitionReassignment(new[] { 0 }),
                [zero] = new NewPartitionReassignment(new[] { 0, 1 }),
            });

        Assert.Equal(2, result.Values.Count);
        Assert.Contains(negative, result.Values.Keys);

        await TestTimeout.Run(() => result.Values[zero], s_deadline);

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Values[negative]), s_deadline);
        Assert.Equal(UnknownTopicOrPartitionCode, failure.Code);
        Assert.Equal(UnknownTopicOrPartitionMessage, failure.Message);
    }

    [Fact]
    public async Task RealClient_ListPartitionReassignments_ANegativePartition_FailsTheWholeRequest()
    {
        await using KafkaAdminClient admin = NewUnconnectedClient();

        ListPartitionReassignmentsResult result =
            admin.ListPartitionReassignments(new[] { new TopicPartition("neg-lpr", -1) });

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(result.Reassignments), s_deadline);
        Assert.Equal(InvalidTopicCode, failure.Code);
        Assert.Equal(InvalidPartitionMessage, failure.Message);
    }

    [Fact]
    public async Task RealClient_AlterPartitionReassignments_ANegativePartition_FaultsThatKey()
    {
        await using KafkaAdminClient admin = NewUnconnectedClient();
        TopicPartition negative = new TopicPartition("neg-apr", -1);

        AlterPartitionReassignmentsResult result = admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>
            {
                [negative] = new NewPartitionReassignment(new[] { 0 }),
            });

        Assert.Contains(negative, result.Values.Keys);

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Values[negative]), s_deadline);
        Assert.Equal(InvalidTopicCode, failure.Code);
        Assert.Equal(InvalidPartitionMessage, failure.Message);

        await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(result.All), s_deadline);
    }

    private static KafkaAdminClient NewUnconnectedClient() =>
        new KafkaAdminClient(new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" });
}
