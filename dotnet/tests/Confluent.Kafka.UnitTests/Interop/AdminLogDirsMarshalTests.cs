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
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Reads native log-directory values through production's <see cref="LogDirMarshal"/>, and checks that what it
/// copies out survives the native value.
/// </summary>
public sealed class AdminLogDirsMarshalTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "logdir-marshal-topic";

    private const string DefaultLogDir = "/tmp/kafka-logs";

    private const int UnknownServerErrorCode = -1;

    [Fact]
    public void CopyOutDescription_ReadsEveryField_AndSurvivesTheDestroy()
    {
        LogDirDescription copied = CopyOutBuilt(
            IntPtr.Zero,
            new[] { (new TopicPartition(Topic, 0), 100L, 3L, false), (new TopicPartition(Topic, 1), 7L, 0L, true) },
            totalBytes: 1_099_511_627_776L,
            usableBytes: 0L,
            isCordoned: true);

        Assert.Null(copied.Error);
        Assert.Equal(1_099_511_627_776L, copied.TotalBytes);
        Assert.Equal(0L, copied.UsableBytes);
        Assert.True(copied.IsCordoned);
        Assert.Equal(2, copied.ReplicaInfos.Count);
        ReplicaInfo first = Assert.Contains(new TopicPartition(Topic, 0), copied.ReplicaInfos);
        Assert.Equal(100L, first.Size);
        Assert.Equal(3L, first.OffsetLag);
        Assert.False(first.IsFuture);
        ReplicaInfo second = Assert.Contains(new TopicPartition(Topic, 1), copied.ReplicaInfos);
        Assert.Equal(7L, second.Size);
        Assert.Equal(0L, second.OffsetLag);
        Assert.True(second.IsFuture);
    }

    [Theory]
    [InlineData(-1L, null)]
    [InlineData(0L, 0L)]
    [InlineData(-2L, -2L)]
    [InlineData(long.MaxValue, long.MaxValue)]
    public void CopyOutDescription_OnlyMinusOneVolumeBytesAreEmpty(long nativeBytes, long? expected)
    {
        LogDirDescription copied = CopyOutBuilt(
            IntPtr.Zero, Array.Empty<(TopicPartition, long, long, bool)>(), nativeBytes, nativeBytes, isCordoned: false);

        Assert.Equal(expected, copied.TotalBytes);
        Assert.Equal(expected, copied.UsableBytes);
        Assert.False(copied.IsCordoned);
        Assert.Empty(copied.ReplicaInfos);
    }

    [Fact]
    public void CopyOutDescription_ADirectoryKafkaError_IsAFieldOfTheDescription()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr error;
        using (Utf8Marshal.PinnedUtf8String message = Utf8Marshal.Pin("The log directory is offline."))
        {
            error = Native.ErrorKafkaMessage(message.Pointer, ref outErr);
        }

        KafkaException.ThrowIfFfiFailed(ref outErr);

        LogDirDescription copied = CopyOutBuilt(
            error, Array.Empty<(TopicPartition, long, long, bool)>(), -1L, -1L, isCordoned: false);

        KafkaException directoryError = Assert.IsType<KafkaException>(copied.Error);
        Assert.Equal(UnknownServerErrorCode, directoryError.Code);
        Assert.Equal("The log directory is offline.", directoryError.Message);
        Assert.Contains("The log directory is offline.", copied.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void CopyOutDescription_ADirectoryErrorThatIsNotAKafkaError_IsThrown()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr error;
        using (Utf8Marshal.PinnedUtf8String message = Utf8Marshal.Pin("not a Kafka error"))
        {
            error = NativeMethods.ErrorLocalIllegalState(message.Pointer, ref outErr);
        }

        KafkaException.ThrowIfFfiFailed(ref outErr);

        InvalidOperationException thrown = Assert.Throws<InvalidOperationException>(
            () => CopyOutBuilt(error, Array.Empty<(TopicPartition, long, long, bool)>(), -1L, -1L, isCordoned: false));
        Assert.Equal("not a Kafka error", thrown.Message);
    }

    [Fact]
    public async Task DescribeLogDirs_OnTheMock_CopiesOutTheDefaultDirectory()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        await SeedTopic(admin, partitions: 2);

        var described = admin.DescribeLogDirs(new[] { 0 }, null);
        IReadOnlyDictionary<string, LogDirDescription> directories = described.Descriptions[0].Get(s_deadline);
        described.Result.Dispose();

        LogDirDescription description = Assert.Contains(DefaultLogDir, directories);
        Assert.Null(description.Error);
        Assert.Null(description.TotalBytes);
        Assert.Null(description.UsableBytes);
        Assert.Equal(2, description.ReplicaInfos.Count);
        foreach (int partition in new[] { 0, 1 })
        {
            ReplicaInfo replica = Assert.Contains(new TopicPartition(Topic, partition), description.ReplicaInfos);
            Assert.Equal(0, replica.Size);
            Assert.Equal(0, replica.OffsetLag);
            Assert.False(replica.IsFuture);
        }
    }

    [Fact]
    public async Task DescribeReplicaLogDirs_OnTheMock_CopiesOutANullFutureDirectory()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        await SeedTopic(admin, partitions: 1);
        TopicPartitionReplica replica = new TopicPartitionReplica(Topic, 0, 0);

        var described = admin.DescribeReplicaLogDirs(new[] { replica }, null);
        DescribeReplicaLogDirsResult.ReplicaLogDirInfo info = described.Values[replica].Get(s_deadline);
        described.Result.Dispose();

        Assert.Equal(new[] { replica }, described.Values.Keys);
        Assert.Equal(DefaultLogDir, info.GetCurrentReplicaLogDir());
        Assert.Equal(0, info.GetCurrentReplicaOffsetLag());
        Assert.Null(info.GetFutureReplicaLogDir());
    }

    [Fact]
    public void CopyOutReplicaLogDirInfo_ANullValue_Throws()
    {
        KafkaException thrown = Assert.Throws<KafkaException>(() => LogDirMarshal.CopyOutReplicaLogDirInfo(IntPtr.Zero));
        Assert.Equal("The describeReplicaLogDirs future delivered no log-directory information.", thrown.Message);
    }

    [Fact]
    public async Task AlterReplicaLogDirs_OnTheMock_FailsTheReplicasFutureWithTheMocksMessage()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        await SeedTopic(admin, partitions: 1);
        TopicPartitionReplica replica = new TopicPartitionReplica(Topic, 0, 0);

        var altered = admin.AlterReplicaLogDirs(
            new Dictionary<TopicPartitionReplica, string> { [replica] = "/not-a-configured-dir" }, null);

        KafkaException failure = Assert.Throws<KafkaException>(() => altered.Values[replica].Get(s_deadline));
        Assert.Equal("Log directory /not-a-configured-dir is offline", failure.Message);
        altered.Result.Dispose();
    }

    [Fact]
    public void ReplicaKey_IsCopiedOut_AndSurvivesTheDestroy()
    {
        TopicPartitionReplica expected = new TopicPartitionReplica("tpr-été", 3, 7);
        IntPtr native = LogDirMarshal.NewReplica(expected);
        TopicPartitionReplica copied;
        try
        {
            copied = LogDirMarshal.CopyOutReplica(native);
        }
        finally
        {
            LogDirMarshal.DestroyReplica(native);
        }

        Assert.Equal(expected, copied);
    }

    [Fact]
    public void TheCopiedOutTypes_HoldNoNativeState()
    {
        foreach (Type type in new[]
                 {
                     typeof(LogDirDescription),
                     typeof(ReplicaInfo),
                     typeof(DescribeReplicaLogDirsResult.ReplicaLogDirInfo),
                     typeof(TopicPartitionReplica),
                 })
        {
            FieldInfo[] fields = type.GetFields(
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static);

            Assert.DoesNotContain(fields, field => field.FieldType == typeof(IntPtr));
            Assert.DoesNotContain(fields, field => typeof(SafeHandle).IsAssignableFrom(field.FieldType));
            Assert.DoesNotContain(fields, field => field.FieldType == typeof(UIntPtr));
        }
    }

    private static Task SeedTopic(NativeAdminClient admin, int partitions) =>
        TestTimeout.Run(
            () => AsyncCreateTopicsResult.OfClient(
                admin.CreateTopics(new[] { new NewTopic(Topic, partitions, 1) }, null)).All().Get(),
            s_deadline);

    private static LogDirDescription CopyOutBuilt(
        IntPtr error,
        (TopicPartition Partition, long Size, long OffsetLag, bool IsFuture)[] replicas,
        long totalBytes,
        long usableBytes,
        bool isCordoned)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr map = NativeMethods.MapNew(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        List<IntPtr> keys = new List<IntPtr>();
        List<IntPtr> values = new List<IntPtr>();
        IntPtr description = IntPtr.Zero;
        try
        {
            foreach ((TopicPartition partition, long size, long offsetLag, bool isFuture) in replicas)
            {
                using (Utf8Marshal.PinnedUtf8String topic = Utf8Marshal.Pin(partition.Topic))
                {
                    keys.Add(NativeMethods.TopicPartitionNew(topic.Pointer, partition.Partition, ref outErr));
                }

                KafkaException.ThrowIfFfiFailed(ref outErr);
                values.Add(Native.ReplicaInfoNew(size, offsetLag, isFuture ? (sbyte)1 : (sbyte)0, ref outErr));
                KafkaException.ThrowIfFfiFailed(ref outErr);
                NativeMethods.MapPut(map, keys[keys.Count - 1], values[values.Count - 1], ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            description = Native.LogDirDescriptionNew(
                error, map, totalBytes, usableBytes, isCordoned ? (sbyte)1 : (sbyte)0, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return LogDirMarshal.CopyOutDescription(description);
        }
        finally
        {
            Native.LogDirDescriptionDestroy(description, ref outErr);
            NativeMethods.MapDestroy(map, ref outErr);
            foreach (IntPtr key in keys)
            {
                NativeMethods.TopicPartitionDestroy(key, ref outErr);
            }

            foreach (IntPtr value in values)
            {
                Native.ReplicaInfoDestroy(value, ref outErr);
            }

            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
    }

    /// <summary>The C functions only these tests call.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_admin_LogDirDescription_with_total_bytes_usable_bytes_is_cordoned_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr LogDirDescriptionNew(
            IntPtr error, IntPtr replicaInfos, long totalBytes, long usableBytes, sbyte isCordoned, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_LogDirDescription_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void LogDirDescriptionDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ReplicaInfo_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ReplicaInfoNew(long size, long offsetLag, sbyte isFuture, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ReplicaInfo_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void ReplicaInfoDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_Error_kafka_message_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ErrorKafkaMessage(IntPtr message, ref IntPtr outErr);
    }
}
