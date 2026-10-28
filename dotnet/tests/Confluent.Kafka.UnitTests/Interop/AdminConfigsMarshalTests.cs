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
/// The copy-out of a native <c>Config_t</c> into a <see cref="Config"/>, over configs the test
/// builds through the core's own constructors, and the <c>describeConfigs</c> futures over the
/// core mock.
/// </summary>
public sealed class AdminConfigsMarshalTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "cfg-marshal-topic";

    private static readonly (Func<IntPtr> Singleton, ConfigEntry.ConfigSource Expected)[] s_sources =
    {
        (() => Singleton(Native.SourceDynamicTopicConfig), ConfigEntry.ConfigSource.DynamicTopicConfig),
        (() => Singleton(Native.SourceDynamicBrokerLoggerConfig), ConfigEntry.ConfigSource.DynamicBrokerLoggerConfig),
        (() => Singleton(Native.SourceDynamicBrokerConfig), ConfigEntry.ConfigSource.DynamicBrokerConfig),
        (() => Singleton(Native.SourceDynamicDefaultBrokerConfig), ConfigEntry.ConfigSource.DynamicDefaultBrokerConfig),
        (() => Singleton(Native.SourceDynamicClientMetricsConfig), ConfigEntry.ConfigSource.DynamicClientMetricsConfig),
        (() => Singleton(Native.SourceDynamicGroupConfig), ConfigEntry.ConfigSource.DynamicGroupConfig),
        (() => Singleton(Native.SourceStaticBrokerConfig), ConfigEntry.ConfigSource.StaticBrokerConfig),
        (() => Singleton(Native.SourceDefaultConfig), ConfigEntry.ConfigSource.DefaultConfig),
        (() => Singleton(Native.SourceUnknown), ConfigEntry.ConfigSource.Unknown),
    };

    private static readonly (Func<IntPtr> Singleton, ConfigEntry.ConfigType Expected)[] s_types =
    {
        (() => Singleton(Native.TypeUnknown), ConfigEntry.ConfigType.Unknown),
        (() => Singleton(Native.TypeBoolean), ConfigEntry.ConfigType.Boolean),
        (() => Singleton(Native.TypeString), ConfigEntry.ConfigType.String),
        (() => Singleton(Native.TypeInt), ConfigEntry.ConfigType.Int),
        (() => Singleton(Native.TypeShort), ConfigEntry.ConfigType.Short),
        (() => Singleton(Native.TypeLong), ConfigEntry.ConfigType.Long),
        (() => Singleton(Native.TypeDouble), ConfigEntry.ConfigType.Double),
        (() => Singleton(Native.TypeList), ConfigEntry.ConfigType.List),
        (() => Singleton(Native.TypeClass), ConfigEntry.ConfigType.Class),
        (() => Singleton(Native.TypePassword), ConfigEntry.ConfigType.Password),
    };

    private delegate IntPtr SingletonAccessor(ref IntPtr outErr);

    [Fact]
    public void TheConfigTree_IsCopiedOut_AndSurvivesTheConfigsDestroy()
    {
        Config config = CopyOut(
            new NativeEntry("a.name", "1", s_sources[0].Singleton(), true, false, s_types[1].Singleton(), "résumé"),
            new NativeEntry("b.name", null, s_sources[7].Singleton(), false, true, s_types[9].Singleton(), null),
            new NativeEntry("c.name", string.Empty, s_sources[8].Singleton(), false, false, s_types[0].Singleton(), string.Empty));

        Assert.Equal(3, config.Entries.Count);

        ConfigEntry a = Assert.IsType<ConfigEntry>(config.Get("a.name"));
        Assert.Equal("1", a.Value);
        Assert.Equal(ConfigEntry.ConfigSource.DynamicTopicConfig, a.Source);
        Assert.True(a.IsSensitive);
        Assert.False(a.IsReadOnly);
        Assert.Equal(ConfigEntry.ConfigType.Boolean, a.Type);
        Assert.Equal("résumé", a.Documentation);
        Assert.Empty(a.Synonyms);
        Assert.False(a.IsDefault);

        ConfigEntry b = Assert.IsType<ConfigEntry>(config.Get("b.name"));
        Assert.Null(b.Value);
        Assert.Equal(ConfigEntry.ConfigSource.DefaultConfig, b.Source);
        Assert.False(b.IsSensitive);
        Assert.True(b.IsReadOnly);
        Assert.Equal(ConfigEntry.ConfigType.Password, b.Type);
        Assert.Null(b.Documentation);
        Assert.True(b.IsDefault);

        ConfigEntry c = Assert.IsType<ConfigEntry>(config.Get("c.name"));
        Assert.Equal(string.Empty, c.Value);
        Assert.Equal(ConfigEntry.ConfigSource.Unknown, c.Source);
        Assert.Equal(ConfigEntry.ConfigType.Unknown, c.Type);
        Assert.Equal(string.Empty, c.Documentation);
    }

    [Fact]
    public void ConfigSource_EachJavaConstant_CopiesOutAsItsMember()
    {
        NativeEntry[] entries = new NativeEntry[s_sources.Length];
        for (int i = 0; i < entries.Length; i++)
        {
            entries[i] = new NativeEntry("s" + i, "v", s_sources[i].Singleton(), false, false, s_types[0].Singleton(), null);
        }

        Config config = CopyOut(entries);

        for (int i = 0; i < entries.Length; i++)
        {
            Assert.Equal(s_sources[i].Expected, config.Get("s" + i)!.Source);
        }
    }

    [Fact]
    public void ConfigType_EachJavaConstant_CopiesOutAsItsMember()
    {
        NativeEntry[] entries = new NativeEntry[s_types.Length];
        for (int i = 0; i < entries.Length; i++)
        {
            entries[i] = new NativeEntry("t" + i, "v", s_sources[8].Singleton(), false, false, s_types[i].Singleton(), null);
        }

        Config config = CopyOut(entries);

        for (int i = 0; i < entries.Length; i++)
        {
            Assert.Equal(s_types[i].Expected, config.Get("t" + i)!.Type);
        }
    }

    [Fact]
    public void ConfigType_ANullType_CopiesOutAsNull()
    {
        Config config = CopyOut(new[]
        {
            new NativeEntry("t", "v", s_sources[8].Singleton(), false, false, IntPtr.Zero, null),
        });

        ConfigEntry entry = config.Get("t")!;
        Assert.Null(entry.Type);
        Assert.Contains(", type=null, ", entry.ToString(), StringComparison.Ordinal);
        Assert.Equal(
            new ConfigEntry("t", "v", entry.Source, false, false, Array.Empty<ConfigEntry.ConfigSynonym>(), null, null),
            entry);
        Assert.NotEqual(
            new ConfigEntry(
                "t", "v", entry.Source, false, false, Array.Empty<ConfigEntry.ConfigSynonym>(), ConfigEntry.ConfigType.Unknown, null),
            entry);
    }

    [Fact]
    public async Task DescribeConfigs_APresentTopic_DeliversItsConfigs()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        await CreateTopic(admin, new Dictionary<string, string>
        {
            ["cleanup.policy"] = "compact",
            ["retention.ms"] = "604800000",
        });

        ConfigResource resource = new ConfigResource(ConfigResourceType.Topic, Topic);
        Config config = await TestTimeout.Run(
            () => AsyncDescribeConfigsResult.OfClient(admin.DescribeConfigs(new[] { resource }, null)).Values[resource].Get(),
            s_deadline);

        Assert.Equal(2, config.Entries.Count);
        ConfigEntry cleanup = Assert.IsType<ConfigEntry>(config.Get("cleanup.policy"));
        Assert.Equal("compact", cleanup.Value);
        Assert.False(cleanup.IsSensitive);
        Assert.False(cleanup.IsReadOnly);
        Assert.Empty(cleanup.Synonyms);
        Assert.Equal(ConfigEntry.ConfigSource.Unknown, cleanup.Source);
        Assert.Equal(ConfigEntry.ConfigType.Unknown, cleanup.Type);
        Assert.Null(cleanup.Documentation);
        Assert.Equal("604800000", config.Get("retention.ms")!.Value);
    }

    [Fact]
    public async Task AFailedResource_FailsOnlyItsOwnFuture()
    {
        NativeAdminClient admin = NativeAdminClient.CreateMock(1);
        using IDisposable adminDisposal = TestTimeout.Disposal(admin);
        await CreateTopic(admin, null);

        ConfigResource present = new ConfigResource(ConfigResourceType.Topic, Topic);
        ConfigResource missing = new ConfigResource(ConfigResourceType.Topic, "cfg-absent-topic");
        AsyncDescribeConfigsResult result =
            AsyncDescribeConfigsResult.OfClient(admin.DescribeConfigs(new[] { present, missing }, null));

        Assert.NotNull(await TestTimeout.Run(() => result.Values[present].Get(), s_deadline));

        KafkaException failure = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.Values[missing].Get()), s_deadline);
        Assert.Equal("Resource ConfigResource(type=TOPIC, name='cfg-absent-topic') not found.", failure.Message);

        KafkaException all = await TestTimeout.Run(
            () => Assert.ThrowsAsync<KafkaException>(() => result.All().Get()), s_deadline);
        Assert.Equal(failure.Message, all.Message);
    }

    // The only coverage of the synonym tree: the core mock builds every entry without synonyms.
    [Fact]
    public void TheCopiedOutTypes_HoldNoNativeState()
    {
        foreach (Type type in new[] { typeof(Config), typeof(ConfigEntry), typeof(ConfigEntry.ConfigSynonym) })
        {
            FieldInfo[] fields = type.GetFields(
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static);

            Assert.DoesNotContain(fields, field => field.FieldType == typeof(IntPtr));
            Assert.DoesNotContain(fields, field => typeof(SafeHandle).IsAssignableFrom(field.FieldType));
            Assert.DoesNotContain(fields, field => field.FieldType == typeof(UIntPtr));
        }
    }

    [Fact]
    public void ConfigSynonym_CarriesItsThreeFields_AndPreservesANullValue()
    {
        ConfigEntry.ConfigSynonym withValue =
            new ConfigEntry.ConfigSynonym("a", "1", ConfigEntry.ConfigSource.StaticBrokerConfig);
        Assert.Equal("a", withValue.Name);
        Assert.Equal("1", withValue.Value);
        Assert.Equal(ConfigEntry.ConfigSource.StaticBrokerConfig, withValue.Source);
        Assert.Equal("ConfigSynonym(name=a, value=1, source=STATIC_BROKER_CONFIG)", withValue.ToString());

        ConfigEntry.ConfigSynonym withNull =
            new ConfigEntry.ConfigSynonym("a", null, ConfigEntry.ConfigSource.DefaultConfig);
        Assert.Null(withNull.Value);
        Assert.NotEqual(withValue, withNull);

        ConfigEntry entry = new ConfigEntry(
            "k",
            "v",
            ConfigEntry.ConfigSource.DynamicTopicConfig,
            isSensitive: false,
            isReadOnly: false,
            new[] { withValue, withNull },
            ConfigEntry.ConfigType.String,
            "doc");
        Assert.Equal(new[] { withValue, withNull }, entry.Synonyms);
    }

    private static Task CreateTopic(NativeAdminClient admin, Dictionary<string, string>? configs) =>
        TestTimeout.Run(
            () => AsyncCreateTopicsResult.OfClient(admin.CreateTopics(
                new[] { new NewTopic(Topic, 1, 1) { Configs = configs } }, null)).All().Get(),
            s_deadline);

    private static IntPtr Singleton(SingletonAccessor accessor)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr singleton = accessor(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Assert.NotEqual(IntPtr.Zero, singleton);
        return singleton;
    }

    private static Config CopyOut(params NativeEntry[] entries)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr[] natives = new IntPtr[entries.Length];
        IntPtr list = IntPtr.Zero;
        IntPtr config = IntPtr.Zero;
        try
        {
            for (int i = 0; i < entries.Length; i++)
            {
                natives[i] = NewEntry(entries[i]);
            }

            list = NativeMethods.ListNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            foreach (IntPtr native in natives)
            {
                NativeMethods.ListAdd(list, native, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            config = Native.ConfigNew(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return ConfigEntriesMarshal.CopyOut(config);
        }
        finally
        {
            Native.ConfigDestroy(config, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            NativeMethods.ListDestroy(list, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            foreach (IntPtr native in natives)
            {
                NativeMethods.ConfigEntryDestroy(native, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }
        }
    }

    private static IntPtr NewEntry(NativeEntry entry)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr builder = Native.BuilderNew(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr options = IntPtr.Zero;
        try
        {
            using (Utf8Marshal.PinnedUtf8String name = Utf8Marshal.Pin(entry.Name))
            using (Utf8Marshal.PinnedUtf8String? value = PinOrNull(entry.Value))
            using (Utf8Marshal.PinnedUtf8String? documentation = PinOrNull(entry.Documentation))
            {
                Native.BuilderSetName(builder, name.Pointer, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                Native.BuilderSetValue(builder, value?.Pointer ?? IntPtr.Zero, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                Native.BuilderSetSource(builder, entry.Source, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                Native.BuilderSetIsSensitive(builder, entry.IsSensitive ? (sbyte)1 : (sbyte)0, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                Native.BuilderSetIsReadOnly(builder, entry.IsReadOnly ? (sbyte)1 : (sbyte)0, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                Native.BuilderSetConfigType(builder, entry.Type, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                Native.BuilderSetDocumentation(builder, documentation?.Pointer ?? IntPtr.Zero, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            options = Native.BuilderBuild(builder, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr native = Native.ConfigEntryWithOptions(options, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return native;
        }
        finally
        {
            Native.OptionsDestroy(options, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            Native.BuilderDestroy(builder, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
    }

    private static Utf8Marshal.PinnedUtf8String? PinOrNull(string? value) =>
        value is null ? null : Utf8Marshal.Pin(value);

    /// <summary>The fields of one native entry the test builds.</summary>
    private sealed class NativeEntry
    {
        internal NativeEntry(
            string name, string? value, IntPtr source, bool isSensitive, bool isReadOnly, IntPtr type, string? documentation)
        {
            Name = name;
            Value = value;
            Source = source;
            IsSensitive = isSensitive;
            IsReadOnly = isReadOnly;
            Type = type;
            Documentation = documentation;
        }

        internal string Name { get; }

        internal string? Value { get; }

        internal IntPtr Source { get; }

        internal bool IsSensitive { get; }

        internal bool IsReadOnly { get; }

        internal IntPtr Type { get; }

        internal string? Documentation { get; }
    }

    /// <summary>The core's constructors for a native config, which production never builds.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntryOptionsBuilder_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr BuilderNew(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntryOptionsBuilder_set_name_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void BuilderSetName(IntPtr self, IntPtr name, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntryOptionsBuilder_set_value_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void BuilderSetValue(IntPtr self, IntPtr value, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntryOptionsBuilder_set_source_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void BuilderSetSource(IntPtr self, IntPtr source, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntryOptionsBuilder_set_is_sensitive_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void BuilderSetIsSensitive(IntPtr self, sbyte isSensitive, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntryOptionsBuilder_set_is_read_only_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void BuilderSetIsReadOnly(IntPtr self, sbyte isReadOnly, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntryOptionsBuilder_set_config_type_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void BuilderSetConfigType(IntPtr self, IntPtr configType, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntryOptionsBuilder_set_documentation_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void BuilderSetDocumentation(IntPtr self, IntPtr documentation, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntryOptionsBuilder_build", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr BuilderBuild(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntryOptionsBuilder_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void BuilderDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntryOptions_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void OptionsDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_with_options_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ConfigEntryWithOptions(IntPtr options, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_Config_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ConfigNew(IntPtr entries, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_Config_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void ConfigDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigSource_dynamic_topic_config_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr SourceDynamicTopicConfig(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigSource_dynamic_broker_logger_config_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr SourceDynamicBrokerLoggerConfig(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigSource_dynamic_broker_config_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr SourceDynamicBrokerConfig(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigSource_dynamic_default_broker_config_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr SourceDynamicDefaultBrokerConfig(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigSource_dynamic_client_metrics_config_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr SourceDynamicClientMetricsConfig(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigSource_dynamic_group_config_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr SourceDynamicGroupConfig(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigSource_static_broker_config_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr SourceStaticBrokerConfig(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigSource_default_config_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr SourceDefaultConfig(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigSource_unknown_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr SourceUnknown(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigType_unknown_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TypeUnknown(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigType_boolean_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TypeBoolean(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigType_string_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TypeString(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigType_int_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TypeInt(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigType_short_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TypeShort(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigType_long_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TypeLong(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigType_double_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TypeDouble(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigType_list_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TypeList(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigType_class_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TypeClass(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_admin_ConfigEntry_ConfigType_password_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr TypePassword(ref IntPtr outErr);
    }
}
