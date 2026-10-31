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
using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <c>ElectLeaders</c> and <c>AlterPartitionReassignments</c> end to end against the mocks. Java's mock refuses
/// <c>electLeaders</c>, so the tests assert the mock's own answer and build <see cref="ElectLeadersResult"/>'s
/// per-partition values on the result directly; the reassignments, cancellation included, run in the mock.
/// </summary>
public sealed class PublicAdminElectionsReassignmentsTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const int UnknownTopicOrPartitionCode = 3;

    private const string NotImplemented = "Not implemented yet";

    [Fact]
    public async Task ElectLeaders_SurfacesTheMocksDocumentedRefusal_ForEverySelectionShape()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        IReadOnlyCollection<TopicPartition>?[] selections =
        {
            new[] { new TopicPartition("p4-elect", 0) },
            Array.Empty<TopicPartition>(),
            null,
        };

        foreach (IReadOnlyCollection<TopicPartition>? selection in selections)
        {
            AsyncElectLeadersResult result = admin.ElectLeaders(ElectionType.Preferred, selection);

            NotSupportedException failure = await TestTimeout.Run(
                () => Assert.ThrowsAsync<NotSupportedException>(() => result.Partitions().Get()), s_deadline);
            Assert.Equal(NotImplemented, failure.Message);

            NotSupportedException fromAll = await TestTimeout.Run(
                () => Assert.ThrowsAsync<NotSupportedException>(() => result.All().Get()), s_deadline);
            Assert.Equal(NotImplemented, fromAll.Message);
        }
    }

    [Theory]
    [InlineData(ElectionType.Preferred)]
    [InlineData(ElectionType.Unclean)]
    public async Task ElectLeaders_AcceptsBothElectionTypes(ElectionType type)
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        NotSupportedException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<NotSupportedException>(
                () => admin.ElectLeaders(type, new[] { new TopicPartition("p4-type", 0) }).Partitions().Get()),
            s_deadline);

        Assert.Equal(NotImplemented, failure.Message);
    }

    [Fact]
    public async Task ElectLeadersResult_PartitionsReturnsTheSameTaskInstance()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        AsyncElectLeadersResult result = admin.ElectLeaders(ElectionType.Preferred, null);

        Assert.Same(result.Partitions().Get(), result.Partitions().Get());

        await TestTimeout.Run(() => Assert.ThrowsAsync<NotSupportedException>(() => result.Partitions().Get()), s_deadline);
    }

    [Fact]
    public async Task ElectLeadersResult_APerPartitionFailureIsAValue_AndAllReportsTheFirst()
    {
        TopicPartition good = new TopicPartition("p4-all", 0);
        TopicPartition bad = new TopicPartition("p4-all", 1);
        TopicPartition worse = new TopicPartition("p4-all", 2);

        KafkaException first = new KafkaException(11, "first failure", isRetriable: false);
        KafkaException second = new KafkaException(12, "second failure", isRetriable: false);

        Dictionary<TopicPartition, KafkaException?> outcomes = new Dictionary<TopicPartition, KafkaException?>
        {
            [good] = null,
            [bad] = first,
            [worse] = second,
        };

        AsyncElectLeadersResult mixed = ElectionOf(outcomes);

        // Partitions succeeds: the per-partition failures are values, not faults.
        IReadOnlyDictionary<TopicPartition, KafkaException?> map =
            await TestTimeout.Run(() => mixed.Partitions().Get(), s_deadline);
        Assert.Null(map[good]);
        Assert.Same(first, map[bad]);
        Assert.Same(second, map[worse]);

        // All fails with the first present failure and does not inspect the rest, as Java's all() does.
        KafkaException thrown =
            await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(() => mixed.All().Get()), s_deadline);
        Assert.Same(map.Values.First(value => value is not null), thrown);

        AsyncElectLeadersResult clean = ElectionOf(new Dictionary<TopicPartition, KafkaException?> { [good] = null });
        await TestTimeout.Run(() => clean.All().Get(), s_deadline);
    }

    [Fact]
    public async Task AlterPartitionReassignments_FaultsOnlyTheFailingPartition()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().NumBrokers(3).Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        await TestTimeout.Run(() => admin.CreateTopics(new[] { new NewTopic("p4-apr", 2, 1) }).All().Get(), s_deadline);

        TopicPartition zero = new TopicPartition("p4-apr", 0);
        TopicPartition one = new TopicPartition("p4-apr", 1);
        TopicPartition missing = new TopicPartition("p4-apr-missing", 0);

        AsyncAlterPartitionReassignmentsResult result = admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>
            {
                [zero] = new NewPartitionReassignment(new[] { 0, 1 }),
                [one] = new NewPartitionReassignment(new[] { 1, 2 }),
                [missing] = new NewPartitionReassignment(new[] { 0 }),
            });

        Assert.Equal(3, result.Values.Count);

        await TestTimeout.Run(() => result.Values[zero].Get(), s_deadline);
        await TestTimeout.Run(() => result.Values[one].Get(), s_deadline);

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Values[missing].Get()), s_deadline);
        Assert.Equal(UnknownTopicOrPartitionCode, failure.Code);

        await TestTimeout.Run(() => Assert.ThrowsAsync<KafkaException>(() => result.All().Get()), s_deadline);
    }

    [Fact]
    public async Task AlterPartitionReassignments_CancelSucceeds_AlongsideAReassignment()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().NumBrokers(3).Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p4-apr-cancel", 2, 1) }).All().Get(), s_deadline);

        TopicPartition reassigned = new TopicPartition("p4-apr-cancel", 0);
        TopicPartition cancelled = new TopicPartition("p4-apr-cancel", 1);

        AsyncAlterPartitionReassignmentsResult result = admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>
            {
                [reassigned] = new NewPartitionReassignment(new[] { 0, 2 }),
                [cancelled] = null,
            });

        await TestTimeout.Run(() => result.All().Get(), s_deadline);
        Assert.Equal(2, result.Values.Count);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AlterPartitionReassignments_AcceptsBothReplicationFactorSettings(bool allow)
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().NumBrokers(2).Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);
        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("p4-apr-rf", 1, 1) }).All().Get(), s_deadline);

        AsyncAlterPartitionReassignmentsResult result = admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>
            {
                [new TopicPartition("p4-apr-rf", 0)] = new NewPartitionReassignment(new[] { 0, 1 }),
            },
            new AlterPartitionReassignmentsOptions { AllowReplicationFactorChange = allow });

        await TestTimeout.Run(() => result.All().Get(), s_deadline);
    }

    [Fact]
    public async Task AlterPartitionReassignments_AnEmptyMapCompletes()
    {
        AsyncMockAdminClient admin = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable adminDisposal = TestTimeout.AsyncDisposal(admin);

        AsyncAlterPartitionReassignmentsResult result = admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>());

        Assert.Empty(result.Values);
        await TestTimeout.Run(() => result.All().Get(), s_deadline);
    }

    [Fact]
    public void BothRpcs_ThrowObjectDisposedAfterClose()
    {
        MockAdminClient admin = MockAdminClient.Create().Build();
        TestTimeout.Dispose(admin);

        Assert.Throws<ObjectDisposedException>(() => admin.ElectLeaders(ElectionType.Preferred, null));
        Assert.Throws<ObjectDisposedException>(() => admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>()));
    }

    [Fact]
    public async Task BothRpcs_AreReachableThroughTheInterface()
    {
        AsyncMockAdminClient mock = AsyncMockAdminClient.Create().Build();
        await using IAsyncDisposable mockDisposal = TestTimeout.AsyncDisposal(mock);
        IAsyncAdmin admin = mock;

        await TestTimeout.Run(
            () => Assert.ThrowsAsync<NotSupportedException>(
                () => admin.ElectLeaders(ElectionType.Preferred, null).Partitions().Get()),
            s_deadline);

        AsyncAlterPartitionReassignmentsResult result = admin.AlterPartitionReassignments(
            new Dictionary<TopicPartition, NewPartitionReassignment?>());
        await TestTimeout.Run(() => result.All().Get(), s_deadline);

        Assert.NotNull(typeof(AsyncKafkaAdminClient).GetMethod(nameof(IAsyncAdmin.ElectLeaders)));
        Assert.NotNull(typeof(AsyncKafkaAdminClient).GetMethod(nameof(IAsyncAdmin.AlterPartitionReassignments)));
    }

    private static AsyncElectLeadersResult ElectionOf(IReadOnlyDictionary<TopicPartition, KafkaException?> outcomes)
    {
        KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, KafkaException?>> partitions =
            new KafkaFutureImpl<IReadOnlyDictionary<TopicPartition, KafkaException?>>();
        partitions.Complete(outcomes);
        return AsyncElectLeadersResult.OfClient((partitions, new SafeAdminResultHandle(IntPtr.Zero, NoDestroy)));
    }

    private static void NoDestroy(IntPtr result, ref IntPtr outErr)
    {
    }
}
