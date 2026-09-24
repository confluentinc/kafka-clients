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
/// M15/P9 CP4 — the four RPCs whose per-key callback decomposes a composite key into
/// scalars (shape <b>4a</b>): a <c>(topic, partition)</c> pair for <c>deleteRecords</c> /
/// <c>listOffsets</c> / <c>describeProducers</c>, and a <c>(topic, partition, broker)</c>
/// triple for <c>describeReplicaLogDirs</c>.
/// </summary>
/// <remarks>
/// A key that arrives in the wrong slot resolves the wrong entry — or none — so every test
/// below varies the components independently and requires <em>every</em> key to settle.
/// </remarks>
public sealed class AdminP9PerKeyStage3Tests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    /// <summary>The code the mock reports for every unimplemented RPC.</summary>
    private const int UnsupportedVersionCode = 35;

    private const string NotImplemented = "Not implemented yet";

    private const string DefaultLogDir = "/tmp/kafka-logs";

    /// <summary>
    /// ⚠ <b>Both halves of the pair are load-bearing.</b> Two topics at the same partition
    /// index plus two partitions within one topic make a callback that dropped either half
    /// resolve the wrong key, which shows up as a key that never settles.
    /// </summary>
    [Fact]
    public async Task DeleteRecords_ResolvesEveryHalfOfThePairIndependently()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        TopicPartition[] keys =
        {
            new TopicPartition("cp4-del-a", 0),
            new TopicPartition("cp4-del-a", 1),
            new TopicPartition("cp4-del-b", 0),
        };

        Dictionary<TopicPartition, RecordsToDelete> request =
            new Dictionary<TopicPartition, RecordsToDelete>();
        foreach (TopicPartition key in keys)
        {
            request[key] = RecordsToDelete.BeforeOffset(10);
        }

        DeleteRecordsResult result = admin.DeleteRecords(request, options: null);

        Assert.Equal(keys.Length, result.LowWatermarks.Count);
        foreach (TopicPartition key in keys)
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => result.LowWatermarks[key], s_deadline));
            Assert.Equal(UnsupportedVersionCode, failure.Code);
            Assert.Equal(NotImplemented, failure.Message);
        }
    }

    /// <summary>
    /// One partition succeeding and another failing resolve <b>independently</b>, and the
    /// copied-out offset survives the owned <c>ListOffsetsResultInfo_t</c> the callback
    /// destroyed.
    /// </summary>
    [Fact]
    public async Task ListOffsets_PerKeyOutcomes_AreIndependent()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("cp4-offsets", 2, 1) }, options: null).All(),
            s_deadline);

        TopicPartition good = new TopicPartition("cp4-offsets", 0);
        TopicPartition bad = new TopicPartition("cp4-offsets", 1);

        ListOffsetsResult result = admin.ListOffsets(
            new Dictionary<TopicPartition, OffsetSpec>
            {
                [good] = OffsetSpec.Latest(),
                [bad] = OffsetSpec.ForTimestamp(1),
            },
            options: null);

        ListOffsetsResult.ListOffsetsResultInfo info =
            await TestTimeout.Run(() => result.PartitionResult(good), s_deadline);
        // The mock reports -1 for an unwritten partition; what matters is that a real owned
        // value crossed and was copied out before the callback destroyed it.
        Assert.Equal(-1L, info.Offset);

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => result.PartitionResult(bad), s_deadline));
        Assert.Equal(UnsupportedVersionCode, failure.Code);
    }

    /// <summary>
    /// ⚠ <b>The three-part key.</b> Four replicas that differ pairwise in partition and in
    /// broker id discriminate a callback that read the wrong scalar slot, and the successful
    /// ones prove the value reader over a real owned <c>ReplicaLogDirInfo_t</c>.
    /// </summary>
    [Fact]
    public async Task DescribeReplicaLogDirs_ResolvesTheThreePartKey()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(2);

        await TestTimeout.Run(
            () => admin.CreateTopics(new[] { new NewTopic("cp4-replicas", 2, 2) }, options: null).All(),
            s_deadline);

        TopicPartitionReplica[] keys =
        {
            new TopicPartitionReplica("cp4-replicas", 0, 0),
            new TopicPartitionReplica("cp4-replicas", 0, 1),
            new TopicPartitionReplica("cp4-replicas", 1, 0),
            new TopicPartitionReplica("cp4-replicas", 1, 1),
        };

        DescribeReplicaLogDirsResult result = admin.DescribeReplicaLogDirs(keys, options: null);

        Assert.Equal(keys.Length, result.Values.Count);
        foreach (TopicPartitionReplica key in keys)
        {
            DescribeReplicaLogDirsResult.ReplicaLogDirInfo info =
                await TestTimeout.Run(() => result.Values[key], s_deadline);
            Assert.Equal(DefaultLogDir, info.GetCurrentReplicaLogDir());
        }
    }

    /// <summary>
    /// <c>describeProducers</c>' per-key value is read out of an index-addressed
    /// <c>DescribeProducersResult_t</c> at index 0, which the mock never produces — so what
    /// is covered here is its key marshalling and per-key error ownership.
    /// </summary>
    [Fact]
    public async Task DescribeProducers_FaultsEveryPartitionIndependently()
    {
        using NativeAdminClient admin = NativeAdminClient.CreateMock(1);

        TopicPartition[] keys =
        {
            new TopicPartition("cp4-prod-a", 0),
            new TopicPartition("cp4-prod-a", 7),
            new TopicPartition("cp4-prod-b", 0),
        };

        DescribeProducersResult result = admin.DescribeProducers(keys, options: null);

        foreach (TopicPartition key in keys)
        {
            KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
                () => TestTimeout.Run(() => result.PartitionResult(key), s_deadline));
            Assert.Equal(UnsupportedVersionCode, failure.Code);
            Assert.Equal(NotImplemented, failure.Message);
        }
    }

    /// <summary>
    /// The <c>n == 0</c> rule at the RPC level: an empty collection fires <b>no</b> callback,
    /// so only the submit's own countdown slot can release the operation — without it the
    /// <c>GCHandle</c> and the span-the-op reference are held forever.
    /// </summary>
    [Theory]
    [InlineData(Rpc.DeleteRecords)]
    [InlineData(Rpc.ListOffsets)]
    [InlineData(Rpc.DescribeProducers)]
    [InlineData(Rpc.DescribeReplicaLogDirs)]
    public void EmptyCollection_ResolvesAtTheSubmitBoundary_AndReleasesTheClient(Rpc rpc)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        Assert.Empty(Submit(admin, rpc, Array.Empty<TopicPartition>()));

        TestTimeout.Run(admin.Dispose, s_deadline);

        Assert.True(
            handle.IsClosed,
            "zero callbacks must still release: the submit token is the only thing that can");
    }

    /// <summary>
    /// Many keys per operation, over many operations and all four RPCs, leave the reference
    /// count <b>balanced</b> — the countdown's core claim once arrivals are concurrent.
    /// </summary>
    [Fact]
    public async Task ManyKeysAcrossEveryRpc_LeaveTheReferenceCountBalanced()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        SafeAdminHandle handle = admin.Handle;

        TopicPartition[] keys = new TopicPartition[10];
        for (int i = 0; i < keys.Length; i++)
        {
            keys[i] = new TopicPartition($"cp4-balance-{i % 2}", i);
        }

        foreach (Rpc rpc in new[]
        {
            Rpc.DeleteRecords,
            Rpc.ListOffsets,
            Rpc.DescribeProducers,
            Rpc.DescribeReplicaLogDirs,
        })
        {
            for (int round = 0; round < 3; round++)
            {
                IReadOnlyList<Func<Task>> awaitables = Submit(admin, rpc, keys);
                Assert.Equal(keys.Length, awaitables.Count);

                foreach (Func<Task> awaitable in awaitables)
                {
                    // Every key resolves one way or the other; only that it SETTLES matters.
                    await TestTimeout.Run(() => Settle(awaitable), s_deadline);
                }

                Assert.False(handle.IsClosed, "the client is still alive between operations");
            }
        }

        TestTimeout.Run(admin.Dispose, s_deadline);
        Assert.True(handle.IsClosed, "120 per-key callbacks must leave the count balanced");
    }

    /// <summary>The four RPCs CP4 migrated, as a test axis.</summary>
    public enum Rpc
    {
        /// <summary>Keyed by the <c>(topic, partition)</c> pair.</summary>
        DeleteRecords,

        /// <summary>Keyed by the <c>(topic, partition)</c> pair.</summary>
        ListOffsets,

        /// <summary>Keyed by the <c>(topic, partition)</c> pair.</summary>
        DescribeProducers,

        /// <summary>Keyed by the <c>(topic, partition, broker)</c> triple.</summary>
        DescribeReplicaLogDirs,
    }

    /// <summary>Awaits one key's outcome, treating a fault as a settlement.</summary>
    private static async Task Settle(Func<Task> awaitable)
    {
        try
        {
            await awaitable().ConfigureAwait(false);
        }
        catch (KafkaException)
        {
        }
    }

    /// <summary>
    /// Submits <paramref name="rpc"/> for <paramref name="keys"/> and hands back one
    /// awaitable factory per key the result exposed — so a key the bridge dropped shows up
    /// as a count mismatch rather than as a passing loop over nothing.
    /// </summary>
    private static IReadOnlyList<Func<Task>> Submit(
        NativeAdminClient admin, Rpc rpc, IReadOnlyCollection<TopicPartition> keys)
    {
        List<Func<Task>> awaitables = new List<Func<Task>>(keys.Count);
        switch (rpc)
        {
            case Rpc.DeleteRecords:
                {
                    Dictionary<TopicPartition, RecordsToDelete> request =
                        new Dictionary<TopicPartition, RecordsToDelete>();
                    foreach (TopicPartition key in keys)
                    {
                        request[key] = RecordsToDelete.BeforeOffset(1);
                    }

                    DeleteRecordsResult result = admin.DeleteRecords(request, options: null);
                    foreach (KeyValuePair<TopicPartition, Task<DeletedRecords>> entry in result.LowWatermarks)
                    {
                        awaitables.Add(() => entry.Value);
                    }

                    break;
                }

            case Rpc.ListOffsets:
                {
                    Dictionary<TopicPartition, OffsetSpec> request =
                        new Dictionary<TopicPartition, OffsetSpec>();
                    foreach (TopicPartition key in keys)
                    {
                        request[key] = OffsetSpec.Latest();
                    }

                    ListOffsetsResult result = admin.ListOffsets(request, options: null);
                    foreach (TopicPartition key in request.Keys)
                    {
                        TopicPartition captured = key;
                        awaitables.Add(() => result.PartitionResult(captured));
                    }

                    break;
                }

            case Rpc.DescribeProducers:
                {
                    DescribeProducersResult result = admin.DescribeProducers(keys, options: null);
                    foreach (TopicPartition key in keys)
                    {
                        TopicPartition captured = key;
                        awaitables.Add(() => result.PartitionResult(captured));
                    }

                    break;
                }

            default:
                {
                    List<TopicPartitionReplica> replicas = new List<TopicPartitionReplica>(keys.Count);
                    foreach (TopicPartition key in keys)
                    {
                        replicas.Add(new TopicPartitionReplica(key.Topic, key.Partition, 0));
                    }

                    DescribeReplicaLogDirsResult result =
                        admin.DescribeReplicaLogDirs(replicas, options: null);
                    foreach (KeyValuePair<TopicPartitionReplica,
                        Task<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> entry in result.Values)
                    {
                        awaitables.Add(() => entry.Value);
                    }

                    break;
                }
        }

        return awaitables;
    }
}
