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

using Confluent.Kafka.Common;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A negative partition reaches the admin core with no binding guard, as Java's <see cref="TopicPartition"/> stores
/// one: the mock answers it per key and round-trips the key, and the real client answers it before anything is sent
/// (per key for <c>AlterPartitionReassignments</c>, for the whole request for <c>ListPartitionReassignments</c>), so
/// no broker is needed.
/// </summary>
public sealed class PublicAdminNegativePartitionTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const int UnknownTopicOrPartitionCode = 3;

    private const int InvalidTopicCode = 17;

    private const string UnknownTopicOrPartitionMessage = "This server does not host this topic-partition.";

    private const string InvalidPartitionMessage = "The given partition index -1 is not valid.";

    [Fact]
    public async Task Mock_AlterPartitionReassignments_ANegativeKey_FaultsOnlyItself_AndRoundTripsTheKey()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().NumBrokers(3).Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("neg-apr", 2, 1) }).All().Get(), s_deadline);

        TopicPartition negative = new TopicPartition("neg-apr", -1);
        TopicPartition zero = new TopicPartition("neg-apr", 0);

        // No synchronous throw: the binding sends the negative key to the core.
        AsyncAlterPartitionReassignmentsResult result = admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>
            {
                [negative] = new NewPartitionReassignment(new[] { 0 }),
                [zero] = new NewPartitionReassignment(new[] { 0, 1 }),
            });

        Assert.Equal(2, result.Values.Count);
        Assert.Contains(negative, result.Values.Keys);

        await TestTimeout.Run(() => result.Values[zero].Get(), s_deadline);

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Values[negative].Get()), s_deadline);
        Assert.Equal(UnknownTopicOrPartitionCode, failure.Code);
        Assert.Equal(UnknownTopicOrPartitionMessage, failure.Message);
    }

    [Fact]
    public async Task RealClient_ListPartitionReassignments_ANegativePartition_FailsTheWholeRequest()
    {
        AsyncKafkaAdminClient admin = NewUnconnectedClient();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        AsyncListPartitionReassignmentsResult result =
            admin.ListPartitionReassignments(new[] { new TopicPartition("neg-lpr", -1) });

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Reassignments().Get()), s_deadline);
        Assert.Equal(InvalidTopicCode, failure.Code);
        Assert.Equal(InvalidPartitionMessage, failure.Message);
    }

    [Fact]
    public async Task RealClient_AlterPartitionReassignments_ANegativePartition_FaultsThatKey()
    {
        AsyncKafkaAdminClient admin = NewUnconnectedClient();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        TopicPartition negative = new TopicPartition("neg-apr", -1);

        AsyncAlterPartitionReassignmentsResult result = admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>
            {
                [negative] = new NewPartitionReassignment(new[] { 0 }),
            });

        Assert.Contains(negative, result.Values.Keys);

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Values[negative].Get()), s_deadline);
        Assert.Equal(InvalidTopicCode, failure.Code);
        Assert.Equal(InvalidPartitionMessage, failure.Message);

        KafkaException allFailure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.All().Get()), s_deadline);
        Assert.Equal(InvalidTopicCode, allFailure.Code);
        Assert.Equal(InvalidPartitionMessage, allFailure.Message);
    }

    private static AsyncKafkaAdminClient NewUnconnectedClient() =>
        new AsyncKafkaAdminClient(new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" });
}
