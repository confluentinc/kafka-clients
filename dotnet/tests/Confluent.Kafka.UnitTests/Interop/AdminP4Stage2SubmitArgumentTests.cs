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

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// What the ListOffsets inputs become in C: each <see cref="OffsetSpec"/> kind and the
/// <see cref="ListOffsetsOptions"/>, read back through the C getters.
/// </summary>
public sealed class AdminP4Stage2SubmitArgumentTests
{
    private const int OffsetSpecTimestamp = 6;

    [Theory]
    [InlineData("latest", 1)]
    [InlineData("earliest", 0)]
    [InlineData("maxTimestamp", 2)]
    [InlineData("earliestLocal", 3)]
    [InlineData("latestTiered", 4)]
    [InlineData("earliestPendingUpload", 5)]
    [InlineData("null", 1)]
    [InlineData("timestamp:1700000000000", OffsetSpecTimestamp)]
    public void EveryOffsetSpecKind_BuildsItsCSpec(string kind, int expectedKind)
    {
        IntPtr native = NativeAdminClient.NativeOffsetSpec(Spec(kind), out IntPtr owned);
        try
        {
            Assert.Equal(expectedKind, Kind(native));
            if (expectedKind == OffsetSpecTimestamp)
            {
                Assert.Equal(native, owned);
            }
            else
            {
                Assert.Equal(IntPtr.Zero, owned);
                Assert.Equal(Singleton(expectedKind), native);
            }
        }
        finally
        {
            NativeAdminClient.DestroyOffsetSpec(owned);
        }
    }

    [Theory]
    [InlineData(-1L)]
    [InlineData(-2L)]
    [InlineData(-3L)]
    [InlineData(-4L)]
    [InlineData(-5L)]
    [InlineData(-6L)]
    [InlineData(0L)]
    public void ForTimestamp_AtAnyValue_StaysATimestampSpec(long timestamp)
    {
        IntPtr native = NativeAdminClient.NativeOffsetSpec(OffsetSpec.ForTimestamp(timestamp), out IntPtr owned);
        try
        {
            Assert.NotEqual(IntPtr.Zero, owned);
            Assert.Equal(OffsetSpecTimestamp, Kind(native));
        }
        finally
        {
            NativeAdminClient.DestroyOffsetSpec(owned);
        }
    }

    [Fact]
    public void ListOffsetsOptions_NoSetterRan_TheCOptionsCarryTheCoresDefaults()
    {
        Assert.Equal((-1, ReadUncommitted()), ReadBack(null));
        Assert.Equal((-1, ReadUncommitted()), ReadBack(new ListOffsetsOptions()));
        Assert.Empty(new ListOffsetsOptions().Setters.InOrder);
    }

    [Fact]
    public void ListOffsetsOptions_EachSetter_ReachesTheCOptions_AndASetterRunTwiceSendsItsLastValue()
    {
        Assert.Equal((4_242, ReadUncommitted()), ReadBack(new ListOffsetsOptions { TimeoutMs = 4_242 }));
        Assert.Equal((0, ReadUncommitted()), ReadBack(new ListOffsetsOptions { TimeoutMs = 0 }));
        Assert.Equal((0, ReadUncommitted()), ReadBack(new ListOffsetsOptions { TimeoutMs = -5 }));
        Assert.Equal((-1, ReadCommitted()), ReadBack(new ListOffsetsOptions { IsolationLevel = IsolationLevel.ReadCommitted }));
        Assert.Equal(
            (-1, ReadUncommitted()), ReadBack(new ListOffsetsOptions { IsolationLevel = IsolationLevel.ReadUncommitted }));

        ListOffsetsOptions options = new ListOffsetsOptions
        {
            TimeoutMs = 10,
            IsolationLevel = IsolationLevel.ReadUncommitted,
        };
        options.TimeoutMs = null;
        options.IsolationLevel = IsolationLevel.ReadCommitted;
        Assert.Equal((-1, ReadCommitted()), ReadBack(options));
    }

    [Fact]
    public void ListOffsets_RejectsBadArguments_ButNotANullSpec()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        Dictionary<TopicPartition, OffsetSpec> request = new Dictionary<TopicPartition, OffsetSpec>
        {
            [new TopicPartition("p4s2-args", 0)] = OffsetSpec.Latest(),
        };

        ArgumentOutOfRangeException isolation = Assert.Throws<ArgumentOutOfRangeException>(() =>
            admin.ListOffsets(request, new ListOffsetsOptions { IsolationLevel = (IsolationLevel)7 }));
        Assert.Equal("options", isolation.ParamName);

        ArgumentException nullTopic = Assert.Throws<ArgumentException>(() =>
            admin.ListOffsets(new Dictionary<TopicPartition, OffsetSpec> { [default] = OffsetSpec.Latest() }, null));
        Assert.Equal("topicPartitionOffsets", nullTopic.ParamName);

        Assert.Equal(
            "topicPartitionOffsets",
            Assert.Throws<ArgumentNullException>(() => admin.ListOffsets(null!, null)).ParamName);

        TopicPartition nullSpec = new TopicPartition("p4s2-args", 1);
        var listed = admin.ListOffsets(new Dictionary<TopicPartition, OffsetSpec> { [nullSpec] = null! }, null);
        Assert.Equal(-1L, listed.Futures[nullSpec].Get(TimeSpan.FromSeconds(30)).Offset);
    }

    private static OffsetSpec? Spec(string kind) => kind switch
    {
        "latest" => OffsetSpec.Latest(),
        "earliest" => OffsetSpec.Earliest(),
        "maxTimestamp" => OffsetSpec.MaxTimestamp(),
        "earliestLocal" => OffsetSpec.EarliestLocal(),
        "latestTiered" => OffsetSpec.LatestTiered(),
        "earliestPendingUpload" => OffsetSpec.EarliestPendingUpload(),
        "null" => null,
        _ => OffsetSpec.ForTimestamp(long.Parse(
            kind.Substring("timestamp:".Length), System.Globalization.CultureInfo.InvariantCulture)),
    };

    private static int Kind(IntPtr spec)
    {
        IntPtr outErr = IntPtr.Zero;
        int kind = Native.OffsetSpecEnum(spec, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return kind;
    }

    private static IntPtr Singleton(int kind)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr singleton = kind switch
        {
            0 => NativeMethods.OffsetSpecEarliest(ref outErr),
            1 => NativeMethods.OffsetSpecLatest(ref outErr),
            2 => NativeMethods.OffsetSpecMaxTimestamp(ref outErr),
            3 => NativeMethods.OffsetSpecEarliestLocal(ref outErr),
            4 => NativeMethods.OffsetSpecLatestTiered(ref outErr),
            _ => NativeMethods.OffsetSpecEarliestPendingUpload(ref outErr),
        };
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return singleton;
    }

    private static IntPtr ReadUncommitted()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr level = NativeMethods.IsolationLevelReadUncommitted(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return level;
    }

    private static IntPtr ReadCommitted()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr level = NativeMethods.IsolationLevelReadCommitted(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return level;
    }

    private static (int TimeoutMs, IntPtr IsolationLevel) ReadBack(ListOffsetsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewListOffsetsOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.ListOffsetsOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr isolationLevel = Native.ListOffsetsOptionsIsolationLevel(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return (timeoutMs, isolationLevel);
        }
        finally
        {
            NativeAdminClient.FreeListOffsetsOptions(native);
        }
    }

    /// <summary>The C getters the binding never reads, declared for the read-back tests.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_admin_OffsetSpec__enum_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int OffsetSpecEnum(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int ListOffsetsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ListOffsetsOptions_isolation_level_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ListOffsetsOptionsIsolationLevel(IntPtr self, ref IntPtr outErr);
    }
}
