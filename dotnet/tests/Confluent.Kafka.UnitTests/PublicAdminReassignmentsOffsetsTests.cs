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
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>ListOffsets</c> and <c>ListPartitionReassignments</c> end to end against the mocks, and the options forwarding
/// of the latter on a real client.
/// </summary>
/// <remarks>
/// The mock refuses a timestamp query and answers an earliest one, as Java's does, so <c>ForTimestamp(-2)</c> and
/// <c>Earliest()</c> have different outcomes through the C ABI: if the timestamp flag were lost, the timestamp query
/// would succeed.
/// </remarks>
public sealed class PublicAdminReassignmentsOffsetsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const int UnsupportedVersionCode = 35;

    private const string NotImplemented = "Not implement yet";

    [Fact]
    public async Task ListOffsets_ForTimestampAtTheEarliestSentinel_DiffersFromEarliest()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p4s2-collide", 2, 1) }).All().Get(), s_deadline);

        TopicPartition viaTimestamp = new TopicPartition("p4s2-collide", 0);
        TopicPartition viaEarliest = new TopicPartition("p4s2-collide", 1);

        AsyncListOffsetsResult result = admin.ListOffsets(
            new Dictionary<TopicPartition, OffsetSpec>
            {
                // -2 is the earliest sentinel, but this is still a timestamp query.
                [viaTimestamp] = OffsetSpec.ForTimestamp(-2),
                [viaEarliest] = OffsetSpec.Earliest(),
            });

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.PartitionResult(viaTimestamp).Get()), s_deadline);
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal(NotImplemented, failure.Message);

        ListOffsetsResult.ListOffsetsResultInfo earliest =
            await TestTimeout.Run(() => result.PartitionResult(viaEarliest).Get(), s_deadline);
        Assert.NotNull(earliest);
    }

    [Theory]
    [InlineData("latest")]
    [InlineData("earliest")]
    [InlineData("maxTimestamp")]
    [InlineData("earliestLocal")]
    [InlineData("latestTiered")]
    [InlineData("earliestPendingUpload")]
    public async Task ListOffsets_EverySentinelKindIsAccepted(string kind)
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p4s2-kinds", 1, 1) }).All().Get(), s_deadline);

        TopicPartition partition = new TopicPartition("p4s2-kinds", 0);
        OffsetSpec spec = kind switch
        {
            "latest" => OffsetSpec.Latest(),
            "earliest" => OffsetSpec.Earliest(),
            "maxTimestamp" => OffsetSpec.MaxTimestamp(),
            "earliestLocal" => OffsetSpec.EarliestLocal(),
            "latestTiered" => OffsetSpec.LatestTiered(),
            _ => OffsetSpec.EarliestPendingUpload(),
        };

        AsyncListOffsetsResult result = admin.ListOffsets(
            new Dictionary<TopicPartition, OffsetSpec> { [partition] = spec });

        // The mock seeds no offsets, so -1 is expected: what is under test is that the kind is accepted.
        ListOffsetsResult.ListOffsetsResultInfo info =
            await TestTimeout.Run(() => result.PartitionResult(partition).Get(), s_deadline);
        Assert.Equal(-1, info.Offset);
        Assert.Equal(-1, info.Timestamp);

        // The mock builds every info without a leader epoch, so it reads as null.
        Assert.Null(info.LeaderEpoch);
    }

    [Fact]
    public async Task ListOffsets_FaultsOnlyTheFailingPartition()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p4s2-granular", 2, 1) }).All().Get(), s_deadline);

        TopicPartition good = new TopicPartition("p4s2-granular", 0);
        TopicPartition bad = new TopicPartition("p4s2-granular", 1);

        AsyncListOffsetsResult result = admin.ListOffsets(
            new Dictionary<TopicPartition, OffsetSpec>
            {
                [good] = OffsetSpec.Latest(),
                [bad] = OffsetSpec.ForTimestamp(123456789L),
            });

        Assert.NotNull(await TestTimeout.Run(() => result.PartitionResult(good).Get(), s_deadline));
        await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.PartitionResult(bad).Get()), s_deadline);
        await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(() => result.All().Get()), s_deadline);
    }

    [Fact]
    public async Task ListOffsets_PartitionResultThrowsForAnUnrequestedPartition()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p4s2-unrequested", 1, 1) }).All().Get(), s_deadline);

        TopicPartition requested = new TopicPartition("p4s2-unrequested", 0);
        AsyncListOffsetsResult result = admin.ListOffsets(
            new Dictionary<TopicPartition, OffsetSpec> { [requested] = OffsetSpec.Latest() });

        // A statement lambda, so xUnit binds the Action overload: PartitionResult throws synchronously, as Java's
        // does, instead of returning a faulted task.
        ArgumentException rejected = Assert.Throws<ArgumentException>(
            () => { _ = result.PartitionResult(new TopicPartition("p4s2-unrequested", 7)); });
        Assert.Equal("partition", rejected.ParamName);
        Assert.Contains(
            "List Offsets for partition \"p4s2-unrequested-7\" was not attempted",
            rejected.Message,
            StringComparison.Ordinal);

        await TestTimeout.Run(() => result.PartitionResult(requested).Get(), s_deadline);
    }

    [Fact]
    public async Task ListPartitionReassignments_ReturnsTheReassignmentJustCreated()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().NumBrokers(3).Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p4s2-lpr", 1, 2) }).All().Get(), s_deadline);

        TopicPartition partition = new TopicPartition("p4s2-lpr", 0);
        await TestTimeout.Run(
            () => admin.AlterPartitionReassignments(
                new Dictionary<TopicPartition, NewPartitionReassignment?>
                {
                    [partition] = new NewPartitionReassignment(new[] { 0, 2 }),
                }).All().Get(),
            s_deadline);

        IReadOnlyDictionary<TopicPartition, PartitionReassignment> reassignments =
            await TestTimeout.Run(
                () => admin.ListPartitionReassignments(new[] { partition }).Reassignments().Get(), s_deadline);

        PartitionReassignment reassignment = Assert.Contains(partition, reassignments);
        Assert.NotEmpty(reassignment.Replicas);

        Assert.All(reassignment.AddingReplicas, broker => Assert.DoesNotContain(broker, reassignment.Replicas));
        Assert.All(reassignment.RemovingReplicas, broker => Assert.Contains(broker, reassignment.Replicas));
    }

    [Fact]
    public async Task ListPartitionReassignments_AQuietPartitionIsAbsentNotFaulted()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().NumBrokers(2).Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p4s2-quiet", 2, 1) }).All().Get(), s_deadline);

        IReadOnlyDictionary<TopicPartition, PartitionReassignment> reassignments =
            await TestTimeout.Run(
                () => admin.ListPartitionReassignments(
                    new[]
                    {
                        new TopicPartition("p4s2-quiet", 0),
                        new TopicPartition("p4s2-quiet", 1),
                    }).Reassignments().Get(),
                s_deadline);

        Assert.Empty(reassignments);
    }

    [Fact]
    public async Task ListPartitionReassignments_AcceptsNullAndEmptySelections()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        Assert.Empty(await TestTimeout.Run(
            () => admin.ListPartitionReassignments(partitions: null).Reassignments().Get(), s_deadline));
        Assert.Empty(await TestTimeout.Run(
            () => admin.ListPartitionReassignments(Array.Empty<TopicPartition>()).Reassignments().Get(), s_deadline));
    }

    [Fact]
    public async Task ListPartitionReassignmentsResult_ReturnsTheSameTaskInstance()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        AsyncListPartitionReassignmentsResult result = admin.ListPartitionReassignments();

        Assert.Same(result.Reassignments().Get(), result.Reassignments().Get());
        await TestTimeout.Run(() => result.Reassignments().Get(), s_deadline);
    }

    [Fact]
    public async Task BothRpcs_ThrowObjectDisposedAfterClose_AndAreReachableThroughTheInterface()
    {
        AsyncMockAdminClient live = AsyncMockAdminClient.Create().Build();
        await using (TestTimeout.AsyncDisposal(live))
        {
            IAsyncAdmin admin = live;
            await TestTimeout.Run(() => admin.ListPartitionReassignments().Reassignments().Get(), s_deadline);
            await TestTimeout.Run(
                () => admin.ListOffsets(new Dictionary<TopicPartition, OffsetSpec>()).All().Get(), s_deadline);
        }

        AsyncMockAdminClient closed = AsyncMockAdminClient.Create().Build();
        TestTimeout.Dispose(closed);

        Assert.Throws<ObjectDisposedException>(() => closed.ListPartitionReassignments());
        Assert.Throws<ObjectDisposedException>(
            () => closed.ListPartitionReassignments(new ListPartitionReassignmentsOptions()));
        Assert.Throws<ObjectDisposedException>(
            () => closed.ListOffsets(new Dictionary<TopicPartition, OffsetSpec>()));

        Assert.Contains(
            typeof(AsyncKafkaAdminClient).GetMethods(),
            method => method.Name == nameof(IAsyncAdmin.ListPartitionReassignments));
        Assert.NotNull(typeof(AsyncKafkaAdminClient).GetMethod(nameof(IAsyncAdmin.ListOffsets)));
    }

    [Theory]
    [InlineData(typeof(IAdmin))]
    [InlineData(typeof(KafkaAdminClient))]
    [InlineData(typeof(MockAdminClient))]
    public void G3_7_TheZeroArgAndOptionsOnlyForms_AreDeclared(Type type)
    {
        MethodInfo? selection = type.GetMethod(
            nameof(IAdmin.ListPartitionReassignments),
            new[] { typeof(IReadOnlyCollection<TopicPartition>), typeof(ListPartitionReassignmentsOptions) });
        MethodInfo? optionsOnly = type.GetMethod(
            nameof(IAdmin.ListPartitionReassignments), new[] { typeof(ListPartitionReassignmentsOptions) });

        Assert.NotNull(selection);
        ParameterInfo partitions = selection!.GetParameters()[0];
        Assert.Equal("partitions", partitions.Name);
        Assert.True(partitions.HasDefaultValue);
        Assert.Null(partitions.DefaultValue);
        Assert.True(selection.GetParameters()[1].HasDefaultValue);
        Assert.Null(selection.GetParameters()[1].DefaultValue);

        Assert.NotNull(optionsOnly);
        Assert.Equal(typeof(ListPartitionReassignmentsResult), optionsOnly!.ReturnType);
        Assert.False(optionsOnly.GetParameters()[0].IsOptional);

        Assert.Equal(
            2, type.GetMethods().Count(method => method.Name == nameof(IAdmin.ListPartitionReassignments)));
    }

    [Fact]
    public async Task G3_7_TheZeroArgAndOptionsOnlyForms_ListEveryReassignment()
    {
        AsyncMockAdminClient mock = AsyncMockAdminClient.Create().NumBrokers(3).Build();
        await using IAsyncDisposable mockDisposal = TestTimeout.AsyncDisposal(mock);
        IAsyncAdmin admin = mock;
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p135-g37", 1, 2) }).All().Get(), s_deadline);

        TopicPartition partition = new TopicPartition("p135-g37", 0);
        await TestTimeout.Run(
            () => admin.AlterPartitionReassignments(
                new Dictionary<TopicPartition, NewPartitionReassignment?>
                {
                    [partition] = new NewPartitionReassignment(new[] { 0, 2 }),
                }).All().Get(),
            s_deadline);

        ListPartitionReassignmentsOptions options = new ListPartitionReassignmentsOptions();
        IReadOnlyDictionary<TopicPartition, PartitionReassignment> explicitNull = await TestTimeout.Run(
            () => admin.ListPartitionReassignments(null, options).Reassignments().Get(), s_deadline);
        IReadOnlyDictionary<TopicPartition, PartitionReassignment> zeroArg = await TestTimeout.Run(
            () => admin.ListPartitionReassignments().Reassignments().Get(), s_deadline);
        IReadOnlyDictionary<TopicPartition, PartitionReassignment> optionsOnly = await TestTimeout.Run(
            () => admin.ListPartitionReassignments(options).Reassignments().Get(), s_deadline);

        Assert.Equal(new[] { partition }, explicitNull.Keys);
        Assert.Equal(new[] { partition }, zeroArg.Keys);
        Assert.Equal(new[] { partition }, optionsOnly.Keys);
        Assert.Equal(explicitNull[partition].Replicas, optionsOnly[partition].Replicas);

        // The control: an empty selection is a different request, and lists nothing.
        Assert.Empty(await TestTimeout.Run(
            () => admin.ListPartitionReassignments(Array.Empty<TopicPartition>(), options).Reassignments().Get(),
            s_deadline));
    }

    [Fact]
    public async Task G3_7_TheOptionsOnlyForm_ForwardsItsOptions()
    {
        AsyncKafkaAdminClient real = new AsyncKafkaAdminClient(
            new Dictionary<string, string> { ["bootstrap.servers"] = "127.0.0.1:1" });
        using IDisposable realDisposal = TestTimeout.Disposal(real);
        ListPartitionReassignmentsOptions options = new ListPartitionReassignmentsOptions { TimeoutMs = -1 };

        KafkaException explicitNull = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => real.ListPartitionReassignments(null, options).Reassignments().Get()),
            s_deadline);
        KafkaException optionsOnly = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => real.ListPartitionReassignments(options).Reassignments().Get()),
            s_deadline);

        // 7 is REQUEST_TIMED_OUT, the same on both forms: a form that dropped its options would wait out the client
        // default and fail the bound. The mock ignores timeouts, hence the real client.
        Assert.Equal(7, explicitNull.Code);
        Assert.Equal(explicitNull.Code, optionsOnly.Code);
        Assert.Equal(explicitNull.Message, optionsOnly.Message);
    }
}
