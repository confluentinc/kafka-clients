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
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Reads the C options the three log-directory RPCs build back through the header's getters, and checks the
/// preconditions their inputs meet before any native call.
/// </summary>
public sealed class AdminLogDirsSubmitArgumentTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const int KafkaStorageErrorCode = 56;

    private static readonly TopicPartitionReplica s_replica = new TopicPartitionReplica("logdir-topic", 3, 7);

    [Fact]
    public void NoSetterRan_TheCOptionsCarryTheCoresDefault()
    {
        Assert.Equal(-1, ReadBack((DescribeLogDirsOptions?)null));
        Assert.Equal(-1, ReadBack(new DescribeLogDirsOptions()));
        Assert.Empty(new DescribeLogDirsOptions().Setters.InOrder);

        Assert.Equal(-1, ReadBack((AlterReplicaLogDirsOptions?)null));
        Assert.Equal(-1, ReadBack(new AlterReplicaLogDirsOptions()));
        Assert.Empty(new AlterReplicaLogDirsOptions().Setters.InOrder);

        Assert.Equal(-1, ReadBack((DescribeReplicaLogDirsOptions?)null));
        Assert.Equal(-1, ReadBack(new DescribeReplicaLogDirsOptions()));
        Assert.Empty(new DescribeReplicaLogDirsOptions().Setters.InOrder);
    }

    [Fact]
    public void TheTimeoutSetter_ReachesTheCOptions_AndItsLastValueWins()
    {
        Assert.Equal(0, ReadBack(new DescribeLogDirsOptions { TimeoutMs = -5 }));
        DescribeLogDirsOptions describe = new DescribeLogDirsOptions { TimeoutMs = 10 };
        describe.TimeoutMs = 20;
        Assert.Equal(20, ReadBack(describe));
        describe.TimeoutMs = null;
        Assert.Equal(-1, ReadBack(describe));

        Assert.Equal(0, ReadBack(new AlterReplicaLogDirsOptions { TimeoutMs = -5 }));
        AlterReplicaLogDirsOptions alter = new AlterReplicaLogDirsOptions { TimeoutMs = 10 };
        alter.TimeoutMs = 30;
        Assert.Equal(30, ReadBack(alter));
        alter.TimeoutMs = null;
        Assert.Equal(-1, ReadBack(alter));

        Assert.Equal(0, ReadBack(new DescribeReplicaLogDirsOptions { TimeoutMs = -5 }));
        DescribeReplicaLogDirsOptions replicas = new DescribeReplicaLogDirsOptions { TimeoutMs = 0 };
        replicas.TimeoutMs = 40;
        Assert.Equal(40, ReadBack(replicas));
        replicas.TimeoutMs = null;
        Assert.Equal(-1, ReadBack(replicas));
    }

    [Fact]
    public void AlterReplicaLogDirs_ANullLogDirectory_IsRejectedBeforeTheCall()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        ArgumentException failure = Assert.Throws<ArgumentException>(
            () => admin.AlterReplicaLogDirs(
                new Dictionary<TopicPartitionReplica, string?> { [s_replica] = null }!, options: null));

        Assert.Equal("replicaAssignment", failure.ParamName);
        Assert.Contains("logdir-topic-3-7", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AlterReplicaLogDirs_TwoEqualReplicasUnderTheCallersComparer_SendTheFirst()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        await TestTimeout.Run(
            () => AsyncCreateTopicsResult.OfClient(
                admin.CreateTopics(new[] { new NewTopic("first-wins-replicas", 1, 1) }, null)).All().Get(),
            s_deadline);

        TopicPartitionReplica first = new TopicPartitionReplica("first-wins-replicas", 0, 0);
        TopicPartitionReplica second = new TopicPartitionReplica("first-wins-replicas", 0, 0);
        Dictionary<TopicPartitionReplica, string> request =
            new Dictionary<TopicPartitionReplica, string>(ReferenceComparer<TopicPartitionReplica>.Instance)
            {
                [first] = "/first",
                [second] = "/second",
            };
        Assert.Equal(2, request.Count);

        AlterReplicaLogDirsResult result = AlterReplicaLogDirsResult.OfClient(admin.AlterReplicaLogDirs(request, null));

        Assert.Single(result.Values);
        KafkaException failure = Assert.Throws<KafkaException>(() => result.Values[second].Get(s_deadline));
        Assert.Equal(KafkaStorageErrorCode, failure.Code);
        Assert.Equal("Log directory /first is offline", failure.Message);
    }

    [Fact]
    public void ANullTopic_CannotReachEitherCall()
    {
        ArgumentNullException failure =
            Assert.Throws<ArgumentNullException>(() => new TopicPartitionReplica(null!, 0, 0));
        Assert.Equal("topic", failure.ParamName);
    }

    [Fact]
    public void NullInputs_AreRejectedWithTheirParameterNames()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        Assert.Equal(
            "brokers",
            Assert.Throws<ArgumentNullException>(() => admin.DescribeLogDirs(null!, options: null)).ParamName);
        Assert.Equal(
            "replicaAssignment",
            Assert.Throws<ArgumentNullException>(() => admin.AlterReplicaLogDirs(null!, options: null)).ParamName);
        Assert.Equal(
            "replicas",
            Assert.Throws<ArgumentNullException>(() => admin.DescribeReplicaLogDirs(null!, options: null)).ParamName);

        ArgumentException nullElement = Assert.Throws<ArgumentException>(
            () => admin.DescribeReplicaLogDirs(new TopicPartitionReplica?[] { null }!, options: null));
        Assert.Equal("replicas", nullElement.ParamName);
    }

    [Fact]
    public void EmptyInputs_YieldAnEmptyResult()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        var described = admin.DescribeLogDirs(Array.Empty<int>(), null);
        Assert.Empty(described.Descriptions);
        described.Result.Dispose();

        var altered = admin.AlterReplicaLogDirs(new Dictionary<TopicPartitionReplica, string>(), null);
        Assert.Empty(altered.Values);
        altered.Result.Dispose();

        var replicas = admin.DescribeReplicaLogDirs(Array.Empty<TopicPartitionReplica>(), null);
        Assert.Empty(replicas.Values);
        replicas.Result.Dispose();
    }

    private static int ReadBack(DescribeLogDirsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewDescribeLogDirsOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.DescribeLogDirsOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return timeoutMs;
        }
        finally
        {
            NativeAdminClient.FreeDescribeLogDirsOptions(native);
        }
    }

    private static int ReadBack(AlterReplicaLogDirsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewAlterReplicaLogDirsOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.AlterReplicaLogDirsOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return timeoutMs;
        }
        finally
        {
            NativeAdminClient.FreeAlterReplicaLogDirsOptions(native);
        }
    }

    private static int ReadBack(DescribeReplicaLogDirsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewDescribeReplicaLogDirsOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.DescribeReplicaLogDirsOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return timeoutMs;
        }
        finally
        {
            NativeAdminClient.FreeDescribeReplicaLogDirsOptions(native);
        }
    }

    /// <summary>Reference equality, so two equal replicas stay two keys.</summary>
    private sealed class ReferenceComparer<T> : IEqualityComparer<T>
        where T : class
    {
        public static readonly ReferenceComparer<T> Instance = new ReferenceComparer<T>();

        public bool Equals(T? x, T? y) => ReferenceEquals(x, y);

        public int GetHashCode(T obj) => RuntimeHelpers.GetHashCode(obj);
    }

    /// <summary>The C functions only these tests call.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeLogDirsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DescribeLogDirsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_AlterReplicaLogDirsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int AlterReplicaLogDirsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeReplicaLogDirsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DescribeReplicaLogDirsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);
    }
}
