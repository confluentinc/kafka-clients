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
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// What the config RPCs hand the core: their C options, read back through the core's getters,
/// the native <c>AlterConfigOp_t</c> each operation becomes, and the inputs' de-duplication,
/// observed through the core mock.
/// </summary>
/// <remarks>
/// The core mock ignores the options of these RPCs, so the options are asserted on the C
/// object itself.
/// </remarks>
public sealed class AdminConfigsSubmitArgumentTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "cfg-topic";

    private static readonly ConfigResource s_topicResource = new ConfigResource(ConfigResourceType.Topic, Topic);

    [Fact]
    public void DescribeConfigs_NoSetterRan_TheCOptionsCarryTheCoresDefaults()
    {
        Assert.Equal((-1, (sbyte)0, (sbyte)0), ReadBack((DescribeConfigsOptions?)null));
        Assert.Equal((-1, (sbyte)0, (sbyte)0), ReadBack(new DescribeConfigsOptions()));
        Assert.Equal((-1, (sbyte)0, (sbyte)0), ReadBack(new DescribeConfigsOptions { TimeoutMs = null }));
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(23_456, 23_456)]
    [InlineData(-1, 0)]
    [InlineData(int.MinValue, 0)]
    public void DescribeConfigs_TimeoutMs_ReachesTheCOptions_NegativeAsZero(int timeoutMs, int expected) =>
        Assert.Equal(expected, ReadBack(new DescribeConfigsOptions { TimeoutMs = timeoutMs }).TimeoutMs);

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void DescribeConfigs_BothBools_ReachTheCOptionsIndependently(bool synonyms, bool documentation)
    {
        (int _, sbyte includeSynonyms, sbyte includeDocumentation) = ReadBack(new DescribeConfigsOptions
        {
            IncludeSynonyms = synonyms,
            IncludeDocumentation = documentation,
        });

        Assert.Equal(synonyms ? 1 : 0, includeSynonyms);
        Assert.Equal(documentation ? 1 : 0, includeDocumentation);
    }

    [Fact]
    public void DescribeConfigs_ASetterRunTwice_SendsItsLastValue()
    {
        DescribeConfigsOptions options = new DescribeConfigsOptions
        {
            TimeoutMs = 10,
            IncludeSynonyms = true,
            IncludeDocumentation = false,
        };
        options.TimeoutMs = null;
        options.IncludeSynonyms = false;
        options.IncludeDocumentation = true;

        Assert.Equal((-1, (sbyte)0, (sbyte)1), ReadBack(options));
    }

    [Fact]
    public void IncrementalAlterConfigs_NoSetterRan_TheCOptionsCarryTheCoresDefaults()
    {
        Assert.Equal((-1, (sbyte)0), ReadBack((AlterConfigsOptions?)null));
        Assert.Equal((-1, (sbyte)0), ReadBack(new AlterConfigsOptions()));
        Assert.Equal((-1, (sbyte)0), ReadBack(new AlterConfigsOptions { TimeoutMs = null }));
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(23_456, 23_456)]
    [InlineData(-1, 0)]
    [InlineData(int.MinValue, 0)]
    public void IncrementalAlterConfigs_TimeoutMs_ReachesTheCOptions_NegativeAsZero(int timeoutMs, int expected) =>
        Assert.Equal(expected, ReadBack(new AlterConfigsOptions { TimeoutMs = timeoutMs }).TimeoutMs);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void IncrementalAlterConfigs_ValidateOnly_ReachesTheCOptions(bool validateOnly) =>
        Assert.Equal(
            validateOnly ? 1 : 0,
            ReadBack(new AlterConfigsOptions { ValidateOnly = validateOnly }).ValidateOnly);

    [Fact]
    public void IncrementalAlterConfigs_ASetterRunTwice_SendsItsLastValue()
    {
        AlterConfigsOptions options = new AlterConfigsOptions { TimeoutMs = 10, ValidateOnly = true };
        options.TimeoutMs = 30;
        options.ValidateOnly = false;

        Assert.Equal((30, (sbyte)0), ReadBack(options));
    }

    [Theory]
    [InlineData(AlterConfigOpType.Set, "1", 0)]
    [InlineData(AlterConfigOpType.Delete, null, 1)]
    [InlineData(AlterConfigOpType.Append, "x,y", 2)]
    [InlineData(AlterConfigOpType.Subtract, "", 3)]
    [InlineData(AlterConfigOpType.Set, "", 0)]
    [InlineData(AlterConfigOpType.Delete, "ignored", 1)]
    public void AnOperation_ReachesTheCore_AsItsNameValueAndJavaId(AlterConfigOpType type, string? value, int javaId)
    {
        AlterConfigOp op = new AlterConfigOp(new ConfigEntry("cfg.näme", value), type);
        List<Utf8Marshal.PinnedUtf8String> pins = new List<Utf8Marshal.PinnedUtf8String>();
        List<IntPtr> entries = new List<IntPtr>();
        IntPtr native = IntPtr.Zero;
        try
        {
            native = NativeAdminClient.NewAlterConfigOp(op, pins, entries);
            IntPtr outErr = IntPtr.Zero;
            IntPtr entry = Native.AlterConfigOpConfigEntry(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr name = NativeMethods.ConfigEntryName(entry, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr nativeValue = NativeMethods.ConfigEntryValue(entry, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr opType = Native.AlterConfigOpOpType(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            sbyte id = Native.OpTypeId(opType, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);

            Assert.Equal("cfg.näme", Utf8Marshal.PtrToString(name));
            Assert.Equal(value, Utf8Marshal.PtrToString(nativeValue));
            Assert.Equal(javaId, id);
        }
        finally
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.AlterConfigOpDestroy(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            foreach (IntPtr entry in entries)
            {
                NativeMethods.ConfigEntryDestroy(entry, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            foreach (Utf8Marshal.PinnedUtf8String pin in pins)
            {
                pin.Dispose();
            }
        }
    }

    [Fact]
    public async Task IncrementalAlterConfigs_AnEmptyValueSets_AndANullDeleteRemoves()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        await CreateTopic(admin);

        await Alter(admin, Ops(
            s_topicResource,
            new AlterConfigOp(new ConfigEntry("retention.ms", "1000"), AlterConfigOpType.Set),
            new AlterConfigOp(new ConfigEntry("cleanup.policy", string.Empty), AlterConfigOpType.Set)));
        Config set = await Describe(admin, s_topicResource);
        Assert.Equal("1000", set.Get("retention.ms")!.Value);
        Assert.Equal(string.Empty, set.Get("cleanup.policy")!.Value);

        await Alter(admin, Ops(
            s_topicResource, new AlterConfigOp(new ConfigEntry("retention.ms", null), AlterConfigOpType.Delete)));
        Config deleted = await Describe(admin, s_topicResource);
        Assert.Null(deleted.Get("retention.ms"));
        Assert.Equal(string.Empty, deleted.Get("cleanup.policy")!.Value);
    }

    [Fact]
    public async Task IncrementalAlterConfigs_ValueEqualResources_MergeTheirOperations()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        await CreateTopic(admin);

        Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs =
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>(ReferenceComparer.Instance)
            {
                [new ConfigResource(ConfigResourceType.Topic, Topic)] =
                    new[] { new AlterConfigOp(new ConfigEntry("retention.ms", "1"), AlterConfigOpType.Set) },
                [new ConfigResource(ConfigResourceType.Topic, Topic)] = new[]
                {
                    new AlterConfigOp(new ConfigEntry("retention.ms", "2"), AlterConfigOpType.Set),
                    new AlterConfigOp(new ConfigEntry("cleanup.policy", "compact"), AlterConfigOpType.Set),
                },
            };
        AsyncAlterConfigsResult result = AsyncAlterConfigsResult.OfClient(admin.IncrementalAlterConfigs(configs, null));

        Assert.Equal(new[] { s_topicResource }, result.Values.Keys);
        await TestTimeout.Run(() => result.All().Get(), s_deadline);
        Config config = await Describe(admin, s_topicResource);
        Assert.Equal("2", config.Get("retention.ms")!.Value);
        Assert.Equal("compact", config.Get("cleanup.policy")!.Value);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task IncrementalAlterConfigs_AResourceWithNoOperations_IsSentAndAnswered(bool validateOnly)
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        await CreateTopic(admin);

        AsyncAlterConfigsResult result = AsyncAlterConfigsResult.OfClient(admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>
            {
                [s_topicResource] = Array.Empty<AlterConfigOp>(),
            },
            new AlterConfigsOptions { ValidateOnly = validateOnly }));

        await TestTimeout.Run(() => result.Values[s_topicResource].Get(), s_deadline);
        Assert.Empty((await Describe(admin, s_topicResource)).Entries);
    }

    [Fact]
    public async Task IncrementalAlterConfigs_AnEmptyMap_ResolvesToNoKeys()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncAlterConfigsResult result = AsyncAlterConfigsResult.OfClient(admin.IncrementalAlterConfigs(
            new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>(), null));

        Assert.Empty(result.Values);
        await TestTimeout.Run(() => result.All().Get(), s_deadline);
    }

    [Fact]
    public void DescribeConfigs_DeduplicatesResources()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);

        AsyncDescribeConfigsResult result = AsyncDescribeConfigsResult.OfClient(admin.DescribeConfigs(
            new[]
            {
                s_topicResource,
                new ConfigResource(ConfigResourceType.Topic, Topic),
                new ConfigResource(ConfigResourceType.Broker, "0"),
                new ConfigResource((ConfigResourceType)64, "x"),
                new ConfigResource(ConfigResourceType.Unknown, "x"),
            },
            null));

        Assert.Equal(3, result.Values.Count);
        Assert.Contains(s_topicResource, result.Values.Keys);
        Assert.Contains(new ConfigResource(ConfigResourceType.Broker, "0"), result.Values.Keys);
        Assert.Contains(new ConfigResource(ConfigResourceType.Unknown, "x"), result.Values.Keys);
    }

    private static Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> Ops(
        ConfigResource resource, params AlterConfigOp[] ops) =>
        new Dictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> { [resource] = ops };

    private static Task CreateTopic(NativeAdminClient admin) =>
        TestTimeout.Run(
            () => AsyncCreateTopicsResult.OfClient(admin.CreateTopics(new[] { new NewTopic(Topic, 1, 1) }, null)).All().Get(),
            s_deadline);

    private static Task Alter(
        NativeAdminClient admin, IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>> configs) =>
        TestTimeout.Run(
            () => AsyncAlterConfigsResult.OfClient(admin.IncrementalAlterConfigs(configs, null)).All().Get(),
            s_deadline);

    private static Task<Config> Describe(NativeAdminClient admin, ConfigResource resource) =>
        TestTimeout.Run(
            () => AsyncDescribeConfigsResult.OfClient(admin.DescribeConfigs(new[] { resource }, null)).Values[resource].Get(),
            s_deadline);

    private static (int TimeoutMs, sbyte IncludeSynonyms, sbyte IncludeDocumentation) ReadBack(
        DescribeConfigsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewDescribeConfigsOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.DescribeConfigsOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            sbyte synonyms = Native.DescribeConfigsOptionsIncludeSynonyms(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            sbyte documentation = Native.DescribeConfigsOptionsIncludeDocumentation(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return (timeoutMs, synonyms, documentation);
        }
        finally
        {
            NativeAdminClient.FreeDescribeConfigsOptions(native);
        }
    }

    private static (int TimeoutMs, sbyte ValidateOnly) ReadBack(AlterConfigsOptions? options)
    {
        IntPtr native = NativeAdminClient.NewAlterConfigsOptions(options);
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int timeoutMs = Native.AlterConfigsOptionsTimeoutMs(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            sbyte validateOnly = Native.AlterConfigsOptionsShouldValidateOnly(native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return (timeoutMs, validateOnly);
        }
        finally
        {
            NativeAdminClient.FreeAlterConfigsOptions(native);
        }
    }

    /// <summary>A comparer under which two value-equal resources are distinct keys.</summary>
    private sealed class ReferenceComparer : IEqualityComparer<ConfigResource>
    {
        internal static readonly ReferenceComparer Instance = new ReferenceComparer();

        public bool Equals(ConfigResource? x, ConfigResource? y) => ReferenceEquals(x, y);

        public int GetHashCode(ConfigResource obj) => RuntimeHelpers.GetHashCode(obj);
    }

    /// <summary>The getters the binding never reads, declared for the read-back tests.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConfigsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int DescribeConfigsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConfigsOptions_include_synonyms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte DescribeConfigsOptionsIncludeSynonyms(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_DescribeConfigsOptions_include_documentation_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte DescribeConfigsOptionsIncludeDocumentation(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigsOptions_timeout_ms_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int AlterConfigsOptionsTimeoutMs(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigsOptions_should_validate_only_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte AlterConfigsOptionsShouldValidateOnly(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigOp_config_entry_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr AlterConfigOpConfigEntry(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigOp_op_type_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr AlterConfigOpOpType(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_AlterConfigOp_OpType_id_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern sbyte OpTypeId(IntPtr self, ref IntPtr outErr);
    }
}
