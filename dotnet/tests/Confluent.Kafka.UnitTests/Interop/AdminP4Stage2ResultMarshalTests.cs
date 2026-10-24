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
using System.Runtime.InteropServices;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;
/// <summary>
/// Reads the native ListOffsets and ListPartitionReassignments values through production's
/// marshallers, and checks that what they copy out survives the native value.
/// </summary>
public sealed class AdminP4Stage2ResultMarshalTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const int UnsupportedVersionCode = 35;

    [Theory]
    [InlineData(42L, 1700000000000L, -1, null)]
    [InlineData(0L, -1L, 0, 0)]
    [InlineData(7L, 123L, 7, 7)]
    public void ListOffsetsResultInfo_LeaderEpoch_MinusOneIsEmpty(
        long offset, long timestamp, int nativeLeaderEpoch, int? expected)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr info = Native.ListOffsetsResultInfoNew(offset, timestamp, nativeLeaderEpoch, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        ListOffsetsResult.ListOffsetsResultInfo copied;
        try
        {
            copied = ListOffsetsResultInfoMarshal.CopyOut(info);
        }
        finally
        {
            Native.ListOffsetsResultInfoDestroy(info, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }

        Assert.Equal(offset, copied.Offset);
        Assert.Equal(timestamp, copied.Timestamp);
        Assert.Equal(expected, copied.LeaderEpoch);
    }

    [Fact]
    public void ListOffsets_OnTheMock_EachKeyCompletesOnItsOwn()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        TopicPartition latest = new TopicPartition("p4s2-lo", 0);
        TopicPartition timestamp = new TopicPartition("p4s2-lo", 1);

        var listed = admin.ListOffsets(
            new Dictionary<TopicPartition, OffsetSpec>
            {
                [latest] = OffsetSpec.Latest(),
                [timestamp] = OffsetSpec.ForTimestamp(1),
            },
            null);

        Assert.Equal(new[] { latest, timestamp }, listed.Futures.Keys);
        ListOffsetsResult.ListOffsetsResultInfo info = listed.Futures[latest].Get(s_deadline);
        Assert.Equal(-1L, info.Offset);
        Assert.Equal(-1L, info.Timestamp);
        Assert.Null(info.LeaderEpoch);

        KafkaException failure = Assert.Throws<KafkaException>(() => listed.Futures[timestamp].Get(s_deadline));
        Assert.Equal(UnsupportedVersionCode, failure.Code);
        Assert.Equal("Not implement yet", failure.Message);
    }

    [Fact]
    public async Task PartitionReassignment_ThreeListsAreCopiedOut_AndSurviveTheDelivery()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(3);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        TopicPartition partition = SeedTopic(admin, "p4s2-lpr-marshal", replicationFactor: 2);

        await TestTimeout.Run(
            () => AsyncAlterPartitionReassignmentsResult.OfClient(admin.AlterPartitionReassignments(
                new Dictionary<TopicPartition, NewPartitionReassignment?>
                {
                    [partition] = new NewPartitionReassignment(new[] { 0, 2 }),
                },
                options: null)).All().Get(),
            s_deadline);

        var listed = admin.ListPartitionReassignments(new[] { partition }, options: null);
        IReadOnlyDictionary<TopicPartition, PartitionReassignment> reassignments =
            new KafkaFuture<IReadOnlyDictionary<TopicPartition, PartitionReassignment>>(listed.Reassignments)
                .Get(s_deadline);
        listed.Result.Dispose();

        PartitionReassignment reassignment = Assert.Single(reassignments).Value;
        Assert.Equal(partition, Assert.Single(reassignments).Key);
        Assert.NotEmpty(reassignment.Replicas);
        Assert.All(reassignment.AddingReplicas, broker => Assert.DoesNotContain(broker, reassignment.Replicas));
        Assert.All(reassignment.RemovingReplicas, broker => Assert.Contains(broker, reassignment.Replicas));

        // The three lists are read through different accessors, so a copy-paste that pointed
        // two of them at the same accessor would make them equal. They are not.
        Assert.NotEqual(reassignment.Replicas, reassignment.AddingReplicas);
    }

    private static TopicPartition SeedTopic(
        NativeAdminClient admin, string name, int partitions = 1, short replicationFactor = 1)
    {
        TestTimeout.Run(
            () => AsyncCreateTopicsResult.OfClient(admin.CreateTopics(
                new[] { new NewTopic(name, partitions, replicationFactor) }, null)).All().Get(),
            s_deadline)
            .GetAwaiter()
            .GetResult();
        return new TopicPartition(name, 0);
    }

    /// <summary>The C functions only these tests call.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ListOffsetsResultInfoNew(long offset, long timestamp, int leaderEpoch, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsResult_ListOffsetsResultInfo_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void ListOffsetsResultInfoDestroy(IntPtr self, ref IntPtr outErr);
    }
}
