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
using System.Runtime.InteropServices;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The values <c>DescribeClusterOptions</c> and <c>ListConfigResourcesOptions</c> send to the
/// core, read where they cross the C boundary: the core mock ignores both options objects, so
/// a behavioural test cannot see them.
/// </summary>
public sealed class AdminP3SubmitArgumentTests
{
    [Fact]
    public void DescribeCluster_NoSetterRan_TheCOptionsCarryTheCoresDefaults()
    {
        Assert.Equal((-1, (sbyte)0, (sbyte)0), ReadBack((DescribeClusterOptions?)null));
        Assert.Equal((-1, (sbyte)0, (sbyte)0), ReadBack(new DescribeClusterOptions()));
        Assert.Empty(new DescribeClusterOptions().Setters.InOrder);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void DescribeCluster_BothBools_ReachTheCOptionsIndependently(
        bool includeAuthorizedOperations, bool includeFencedBrokers)
    {
        (int _, sbyte authorized, sbyte fenced) = ReadBack(new DescribeClusterOptions
        {
            IncludeAuthorizedOperations = includeAuthorizedOperations,
            IncludeFencedBrokers = includeFencedBrokers,
        });

        Assert.Equal(includeAuthorizedOperations ? 1 : 0, authorized);
        Assert.Equal(includeFencedBrokers ? 1 : 0, fenced);
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(12_345, 12_345)]
    [InlineData(-1, 0)]
    [InlineData(int.MinValue, 0)]
    public void DescribeCluster_TimeoutMs_ReachesTheCOptions_NegativeAsZero(int timeoutMs, int expected) =>
        Assert.Equal((expected, (sbyte)0, (sbyte)0), ReadBack(new DescribeClusterOptions { TimeoutMs = timeoutMs }));

    [Fact]
    public void DescribeCluster_ASetterRunTwice_SendsItsLastValue()
    {
        DescribeClusterOptions options = new DescribeClusterOptions
        {
            TimeoutMs = 10,
            IncludeAuthorizedOperations = true,
            IncludeFencedBrokers = false,
        };
        options.TimeoutMs = null;
        options.IncludeAuthorizedOperations = false;
        options.IncludeFencedBrokers = true;

        Assert.Equal((-1, (sbyte)0, (sbyte)1), ReadBack(options));
    }

    [Fact]
    public void ListConfigResources_NoSetterRan_TheCOptionsCarryTheCoresDefault()
    {
        Assert.Equal(-1, ReadBack((ListConfigResourcesOptions?)null));
        Assert.Equal(-1, ReadBack(new ListConfigResourcesOptions()));
        Assert.Equal(-1, ReadBack(new ListConfigResourcesOptions { TimeoutMs = null }));
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(23_456, 23_456)]
    [InlineData(-1, 0)]
    [InlineData(int.MinValue, 0)]
    public void ListConfigResources_TimeoutMs_ReachesTheCOptions_NegativeAsZero(int timeoutMs, int expected) =>
        Assert.Equal(expected, ReadBack(new ListConfigResourcesOptions { TimeoutMs = timeoutMs }));

    [Fact]
    public void ListConfigResources_ASetterRunTwice_SendsItsLastValue()
    {
        ListConfigResourcesOptions options = new ListConfigResourcesOptions { TimeoutMs = 10 };
        options.TimeoutMs = 30;
        Assert.Equal(30, ReadBack(options));
        options.TimeoutMs = null;
        Assert.Equal(-1, ReadBack(options));
    }

    [Theory]
    [InlineData(ConfigResourceType.Unknown, 0)]
    [InlineData(ConfigResourceType.Topic, 2)]
    [InlineData(ConfigResourceType.Broker, 4)]
    [InlineData(ConfigResourceType.BrokerLogger, 8)]
    [InlineData(ConfigResourceType.ClientMetrics, 16)]
    [InlineData(ConfigResourceType.Group, 32)]
    [InlineData((ConfigResourceType)99, 0)]
    [InlineData((ConfigResourceType)(-3), 0)]
    public void ListConfigResources_ARequestedType_ReachesTheCoreAsItsJavaId(ConfigResourceType type, int expectedId)
    {
        IntPtr native = ConfigResourceMarshal.NativeType(type);
        IntPtr outErr = IntPtr.Zero;
        sbyte id = NativeMethods.ConfigResourceTypeId(native, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Assert.Equal(expectedId, id);
    }

    private static (int TimeoutMs, sbyte IncludeAuthorizedOperations, sbyte IncludeFencedBrokers) ReadBack(
        DescribeClusterOptions? options)
    {
        IntPtr native = NativeAdminClient.NewDescribeClusterOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.DescribeClusterOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            sbyte authorized = Native.DescribeClusterOptionsIncludeAuthorizedOperations(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            sbyte fenced = Native.DescribeClusterOptionsIncludeFencedBrokers(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return (timeoutMs, authorized, fenced);
        }
        finally
        {
            NativeAdminClient.FreeDescribeClusterOptions(native);
        }
    }

    private static int ReadBack(ListConfigResourcesOptions? options)
    {
        IntPtr native = NativeAdminClient.NewListConfigResourcesOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.ListConfigResourcesOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return timeoutMs;
        }
        finally
        {
            NativeAdminClient.FreeListConfigResourcesOptions(native);
        }
    }

    /// <summary>The options getters the binding never reads, declared for the read-back tests.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DescribeClusterOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterOptions_include_authorized_operations_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte DescribeClusterOptionsIncludeAuthorizedOperations(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeClusterOptions_include_fenced_brokers_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte DescribeClusterOptionsIncludeFencedBrokers(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ListConfigResourcesOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int ListConfigResourcesOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);
    }
}
