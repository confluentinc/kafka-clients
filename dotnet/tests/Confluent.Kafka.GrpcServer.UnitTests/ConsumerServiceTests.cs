// Copyright 2026 Confluent Inc.
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

using Confluent.Kafka.Consumer;
using Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

using Xunit;

using Proto = Confluent.Kafka.Test;

namespace Confluent.Kafka.GrpcServer.UnitTests;

/// <summary>
/// The consumer servicers, sync and async, over the mock consumer, and their close request's translation
/// (<c>dotnet/CLAUDE.md</c> §7.6).
/// </summary>
public sealed class ConsumerServiceTests
{
    private const string TimerMessage = "Invalid negative timeout -1";
    private const string DeadlineMessage = "Timeout must not be negative";

    [Theory]
    [InlineData("sync", false)]
    [InlineData("async", false)]
    [InlineData("sync", true)]
    [InlineData("async", true)]
    public async Task Subscribe_WithAPattern_IsUnsupportedAndKeepsTheSubscription(string flavour, bool withListener)
    {
        using ConsumerServer server = ConsumerServer.Create(flavour);
        ulong consumerId = await server.CreateMockConsumer();
        Proto.SubscribeRequest topics = new Proto.SubscribeRequest { ConsumerId = consumerId };
        topics.Topics.Add("a");
        Assert.Null((await server.Service.Subscribe(topics, server.Context)).Error);

        Proto.StatusResponse response = await server.Service.Subscribe(
            new Proto.SubscribeRequest { ConsumerId = consumerId, Pattern = "a.*", WithListener = withListener }, server.Context);
        Proto.SubscriptionResponse subscription = await server.Service.Subscription(
            new Proto.ConsumerIdRequest { ConsumerId = consumerId }, server.Context);

        Assert.Equal(-31, response.Error.Code);
        Assert.Equal("pattern subscribe is not supported by the .NET binding yet", response.Error.Message);
        Assert.Equal(new[] { "a" }, subscription.Topics.Values);
    }

    [Theory]
    [InlineData("sync", "CommitSync", TimerMessage)]
    [InlineData("async", "CommitSync", TimerMessage)]
    [InlineData("sync", "CommitSyncOffsets", TimerMessage)]
    [InlineData("async", "CommitSyncOffsets", TimerMessage)]
    [InlineData("sync", "Committed", DeadlineMessage)]
    [InlineData("async", "Committed", DeadlineMessage)]
    [InlineData("sync", "Position", TimerMessage)]
    [InlineData("async", "Position", TimerMessage)]
    [InlineData("sync", "BeginningOffsets", DeadlineMessage)]
    [InlineData("async", "BeginningOffsets", DeadlineMessage)]
    [InlineData("sync", "EndOffsets", DeadlineMessage)]
    [InlineData("async", "EndOffsets", DeadlineMessage)]
    [InlineData("sync", "OffsetsForTimes", DeadlineMessage)]
    [InlineData("async", "OffsetsForTimes", DeadlineMessage)]
    public async Task TimedLookup_WithATimeout_CallsTheTimedOverload(string flavour, string rpc, string message)
    {
        using ConsumerServer server = ConsumerServer.Create(flavour);
        ulong consumerId = await server.CreateMockConsumer();

        Proto.KafkaError? error = await CallWithTimeout(server, consumerId, rpc, timeoutMs: -1);

        Assert.NotNull(error);
        Assert.Equal(-3, error.Code);
        Assert.Equal(message, error.Message);
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task GroupMetadata_StoresEachAnswerUnderTheNextIdAndReturnsItsFields(string flavour)
    {
        using ConsumerServer server = ConsumerServer.Create(flavour);
        ulong consumerId = await server.CreateMockConsumer();

        Proto.GroupMetadataResponse first = await server.Service.GroupMetadata(
            new Proto.ConsumerIdRequest { ConsumerId = consumerId }, server.Context);
        Proto.GroupMetadataResponse second = await server.Service.GroupMetadata(
            new Proto.ConsumerIdRequest { ConsumerId = consumerId }, server.Context);

        Assert.Null(first.Error);
        Assert.Equal(1UL, first.GroupMetadataId);
        Assert.Equal(2UL, second.GroupMetadataId);
        Assert.Equal("dummy.group.id", first.GroupMetadata.GroupId);
        Assert.Equal(1, first.GroupMetadata.GenerationId);
        Assert.Equal("1", first.GroupMetadata.MemberId);
        Assert.False(first.GroupMetadata.HasGroupInstanceId);

        ConsumerGroupMetadata stored = Assert.IsType<ConsumerGroupMetadata>(server.GroupMetadata.Get(first.GroupMetadataId));
        Assert.Equal("dummy.group.id", stored.GroupId);
        Assert.NotNull(server.GroupMetadata.Get(second.GroupMetadataId));
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task GroupMetadata_OnAnUnknownConsumer_IsTheUnknownConsumerError(string flavour)
    {
        using ConsumerServer server = ConsumerServer.Create(flavour);

        Proto.GroupMetadataResponse response = await server.Service.GroupMetadata(
            new Proto.ConsumerIdRequest { ConsumerId = 9 }, server.Context);

        Assert.Equal(0UL, response.GroupMetadataId);
        Assert.Equal(-4, response.Error.Code);
        Assert.Equal("unknown consumer_id 9", response.Error.Message);
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task ReleaseGroupMetadata_DropsTheStoredMetadataAndIgnoresAnUnknownId(string flavour)
    {
        using ConsumerServer server = ConsumerServer.Create(flavour);
        ulong consumerId = await server.CreateMockConsumer();
        Proto.GroupMetadataResponse stored = await server.Service.GroupMetadata(
            new Proto.ConsumerIdRequest { ConsumerId = consumerId }, server.Context);

        Proto.StatusResponse released = await server.Service.ReleaseGroupMetadata(
            new Proto.ReleaseGroupMetadataRequest { GroupMetadataId = stored.GroupMetadataId }, server.Context);
        Proto.StatusResponse unknown = await server.Service.ReleaseGroupMetadata(
            new Proto.ReleaseGroupMetadataRequest { GroupMetadataId = 99 }, server.Context);

        Assert.Null(released.Error);
        Assert.Null(unknown.Error);
        Assert.Null(server.GroupMetadata.Get(stored.GroupMetadataId));
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task CurrentLag_WithAnEndOffset_IsTheMockLag(string flavour)
    {
        using ConsumerServer server = ConsumerServer.Create(flavour);
        ulong consumerId = await server.CreateMockConsumer();
        Proto.TopicPartition partition = await AssignTopicPartition(server, consumerId);
        server.UpdateBeginningOffset(consumerId, "t", 0, 0);
        server.UpdateEndOffset(consumerId, "t", 0, 10);

        Proto.CurrentLagResponse response = await server.Service.CurrentLag(
            new Proto.CurrentLagRequest { ConsumerId = consumerId, Partition = partition }, server.Context);

        Assert.Null(response.Error);
        Assert.True(response.HasLag);
        Assert.Equal(10L, response.Lag);
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task CurrentLag_WithoutAnEndOffset_IsAPresentZero(string flavour)
    {
        using ConsumerServer server = ConsumerServer.Create(flavour);
        ulong consumerId = await server.CreateMockConsumer();
        Proto.TopicPartition partition = await AssignTopicPartition(server, consumerId);

        Proto.CurrentLagResponse response = await server.Service.CurrentLag(
            new Proto.CurrentLagRequest { ConsumerId = consumerId, Partition = partition }, server.Context);

        Assert.Null(response.Error);
        Assert.True(response.HasLag);
        Assert.Equal(0L, response.Lag);
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task CurrentLag_OnAnUnknownConsumer_IsTheUnknownConsumerError(string flavour)
    {
        using ConsumerServer server = ConsumerServer.Create(flavour);

        Proto.CurrentLagResponse response = await server.Service.CurrentLag(
            new Proto.CurrentLagRequest { ConsumerId = 9, Partition = new Proto.TopicPartition { Topic = "t" } },
            server.Context);

        Assert.False(response.HasLag);
        Assert.Equal(-4, response.Error.Code);
        Assert.Equal("unknown consumer_id 9", response.Error.Message);
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task Close_WithAnOperation_CallsCloseWithTheOptions(string flavour)
    {
        using ConsumerServer server = ConsumerServer.Create(flavour);
        ConsumerScript script = new ConsumerScript();
        ulong consumerId = server.RegisterScripted(script);

        Proto.StatusResponse response = await server.Service.Close(
            new Proto.ConsumerCloseRequest { ConsumerId = consumerId, GroupMembershipOperation = "LEAVE_GROUP", CloseTimeoutMs = 500 },
            server.Context);

        Assert.Null(response.Error);
        Assert.Equal(new[] { "Close(option)" }, script.Calls);
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task Close_WithALoneTimeout_CallsTheNoArgumentClose(string flavour)
    {
        using ConsumerServer server = ConsumerServer.Create(flavour);
        ConsumerScript script = new ConsumerScript();
        ulong consumerId = server.RegisterScripted(script);

        Proto.StatusResponse response = await server.Service.Close(
            new Proto.ConsumerCloseRequest { ConsumerId = consumerId, CloseTimeoutMs = 500 }, server.Context);
        Proto.TopicPartitionListResponse after = await server.Service.Assignment(
            new Proto.ConsumerIdRequest { ConsumerId = consumerId }, server.Context);

        Assert.Null(response.Error);
        Assert.Equal(new[] { "Close" }, script.Calls);
        Assert.Equal($"unknown consumer_id {consumerId}", after.Error.Message);
    }

    [Theory]
    [InlineData("sync")]
    [InlineData("async")]
    public async Task Close_WithAnUnknownOperation_EscapesTheHandlerAndLeavesTheConsumerOpen(string flavour)
    {
        using ConsumerServer server = ConsumerServer.Create(flavour);
        ulong consumerId = await server.CreateMockConsumer();

        await Assert.ThrowsAsync<KeyNotFoundException>(() => server.Service.Close(
            new Proto.ConsumerCloseRequest { ConsumerId = consumerId, GroupMembershipOperation = "LEAVE" }, server.Context));
        Proto.TopicPartitionListResponse after = await server.Service.Assignment(
            new Proto.ConsumerIdRequest { ConsumerId = consumerId }, server.Context);

        Assert.Null(after.Error);
    }

    [Theory]
    [InlineData("LEAVE_GROUP", GroupMembershipOperation.LeaveGroup)]
    [InlineData("REMAIN_IN_GROUP", GroupMembershipOperation.RemainInGroup)]
    [InlineData("DEFAULT", GroupMembershipOperation.Default)]
    public void CloseOptionsFromProto_NamesTheOperationAndTakesAPresentTimeout(string name, GroupMembershipOperation operation)
    {
        CloseOptions timed = Assert.IsType<CloseOptions>(Translate.CloseOptionsFromProto(
            new Proto.ConsumerCloseRequest { GroupMembershipOperation = name, CloseTimeoutMs = 500 }));
        CloseOptions untimed = Assert.IsType<CloseOptions>(Translate.CloseOptionsFromProto(
            new Proto.ConsumerCloseRequest { GroupMembershipOperation = name }));

        Assert.Equal(operation, timed.GroupMembershipOperation());
        Assert.Equal(TimeSpan.FromMilliseconds(500), timed.Timeout());
        Assert.Equal(operation, untimed.GroupMembershipOperation());
        Assert.Null(untimed.Timeout());
    }

    [Fact]
    public void CloseOptionsFromProto_WithoutAnOperation_IsNoOptions() =>
        Assert.Null(Translate.CloseOptionsFromProto(new Proto.ConsumerCloseRequest { CloseTimeoutMs = 500 }));

    [Fact]
    public void CloseOptionsFromProto_WithAnUnknownOperation_ThrowsTheLookupFailure()
    {
        KeyNotFoundException error = Assert.Throws<KeyNotFoundException>(() => Translate.CloseOptionsFromProto(
            new Proto.ConsumerCloseRequest { GroupMembershipOperation = "LEAVE" }));

        Assert.Equal("'LEAVE'", error.Message);
    }

    private static async Task<Proto.TopicPartition> AssignTopicPartition(ConsumerServer server, ulong consumerId)
    {
        Proto.TopicPartition partition = new Proto.TopicPartition { Topic = "t", Partition = 0 };
        Proto.AssignRequest assign = new Proto.AssignRequest { ConsumerId = consumerId };
        assign.Partitions.Add(partition);
        Assert.Null((await server.Service.Assign(assign, server.Context)).Error);
        return partition;
    }

    private static async Task<Proto.KafkaError?> CallWithTimeout(ConsumerServer server, ulong consumerId, string rpc, long timeoutMs)
    {
        Proto.TopicPartition partition = new Proto.TopicPartition { Topic = "t", Partition = 0 };
        switch (rpc)
        {
            case "CommitSync":
                return (await server.Service.CommitSync(
                    new Proto.CommitSyncRequest { ConsumerId = consumerId, TimeoutMs = timeoutMs }, server.Context)).Error;
            case "CommitSyncOffsets":
                Proto.CommitSyncRequest commit = new Proto.CommitSyncRequest { ConsumerId = consumerId, TimeoutMs = timeoutMs };
                commit.Offsets.Add(new Proto.OffsetMapEntry { Partition = partition, Offset = new Proto.OffsetAndMetadata { Offset = 5 } });
                return (await server.Service.CommitSync(commit, server.Context)).Error;
            case "Committed":
                Proto.CommittedRequest committed = new Proto.CommittedRequest { ConsumerId = consumerId, TimeoutMs = timeoutMs };
                committed.Partitions.Add(partition);
                return (await server.Service.Committed(committed, server.Context)).Error;
            case "Position":
                return (await server.Service.Position(
                    new Proto.PositionRequest { ConsumerId = consumerId, Partition = partition, TimeoutMs = timeoutMs },
                    server.Context)).Error;
            case "BeginningOffsets":
                return (await server.Service.BeginningOffsets(PartitionList(consumerId, partition, timeoutMs), server.Context)).Error;
            case "EndOffsets":
                return (await server.Service.EndOffsets(PartitionList(consumerId, partition, timeoutMs), server.Context)).Error;
            case "OffsetsForTimes":
                Proto.OffsetsForTimesRequest times = new Proto.OffsetsForTimesRequest { ConsumerId = consumerId, TimeoutMs = timeoutMs };
                times.Timestamps.Add(new Proto.TimestampSpecEntry { Partition = partition, Timestamp = 0 });
                return (await server.Service.OffsetsForTimes(times, server.Context)).Error;
            default:
                throw new ArgumentOutOfRangeException(nameof(rpc), rpc, "a timed lookup");
        }
    }

    private static Proto.TopicPartitionListRequest PartitionList(ulong consumerId, Proto.TopicPartition partition, long timeoutMs)
    {
        Proto.TopicPartitionListRequest request = new Proto.TopicPartitionListRequest { ConsumerId = consumerId, TimeoutMs = timeoutMs };
        request.Partitions.Add(partition);
        return request;
    }
}
