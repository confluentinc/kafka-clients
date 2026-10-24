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
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

using Confluent.Kafka.Admin;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The public shape of the configs surface (<c>DescribeConfigs</c>, <c>IncrementalAlterConfigs</c>, their options,
/// results and value types) against the Java classes it mirrors.
/// </summary>
/// <remarks>
/// These are reflection assertions: a behavioural test passes equally against a property and a method, or against
/// <c>T</c> and <c>T?</c>.
/// </remarks>
public sealed class PublicAdminConfigsShapeParityTests
{
    [Fact]
    public void IAdmin_TheTwoNewRpcsAreSynchronous_WithTheJavaParameterShape()
    {
        MethodInfo describe = typeof(IAdmin).GetMethod(nameof(IAdmin.DescribeConfigs))!;
        MethodInfo alter = typeof(IAdmin).GetMethod(nameof(IAdmin.IncrementalAlterConfigs))!;

        Assert.Equal(typeof(DescribeConfigsResult), describe.ReturnType);

        // Java's incrementalAlterConfigs returns AlterConfigsResult: there is no IncrementalAlterConfigsResult.
        Assert.Equal(typeof(AlterConfigsResult), alter.ReturnType);

        Assert.Equal(
            new[] { typeof(IReadOnlyCollection<ConfigResource>), typeof(DescribeConfigsOptions) },
            describe.GetParameters().Select(parameter => parameter.ParameterType));

        Assert.Equal(
            new[]
            {
                typeof(IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>),
                typeof(AlterConfigsOptions),
            },
            alter.GetParameters().Select(parameter => parameter.ParameterType));

        foreach (MethodInfo rpc in new[] { describe, alter })
        {
            ParameterInfo[] parameters = rpc.GetParameters();
            Assert.False(parameters[0].IsOptional, $"{rpc.Name}'s input must be required");
            Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(parameters[0]));
            Assert.True(parameters[1].IsOptional, $"{rpc.Name}'s options must be optional");
            Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(parameters[1]));
        }

        Assert.Empty(
            typeof(IAdmin).GetMethods()
                .Where(method => typeof(Task).IsAssignableFrom(method.ReturnType))
                .Select(method => method.Name));
    }

    [Fact]
    public void TheTwoResults_DifferExactlyAsJavasDo()
    {
        PropertyInfo describeValues = typeof(DescribeConfigsResult).GetProperty(
            nameof(DescribeConfigsResult.Values))!;
        Assert.Equal(typeof(IReadOnlyDictionary<ConfigResource, KafkaFuture<Config>>), describeValues.PropertyType);
        Assert.Null(describeValues.SetMethod);

        MethodInfo describeAll = typeof(DescribeConfigsResult).GetMethod(
            nameof(DescribeConfigsResult.All), Type.EmptyTypes)!;
        Assert.Equal(typeof(KafkaFuture<IReadOnlyDictionary<ConfigResource, Config>>), describeAll.ReturnType);

        PropertyInfo alterValues = typeof(AlterConfigsResult).GetProperty(nameof(AlterConfigsResult.Values))!;

        Assert.Equal(typeof(IReadOnlyDictionary<ConfigResource, KafkaFuture>), alterValues.PropertyType);
        Assert.Null(alterValues.SetMethod);

        MethodInfo alterAll = typeof(AlterConfigsResult).GetMethod(nameof(AlterConfigsResult.All), Type.EmptyTypes)!;

        Assert.Equal(typeof(KafkaFuture), alterAll.ReturnType);

        Assert.Empty(typeof(DescribeConfigsResult).GetConstructors());
        Assert.Empty(typeof(AlterConfigsResult).GetConstructors());
    }

    [Fact]
    public void ConfigEntry_KeepsP1sFiveMembers_AndGainsJavasFour()
    {
        Assert.Equal(typeof(string), Property(typeof(ConfigEntry), nameof(ConfigEntry.Name)).PropertyType);
        Assert.Equal(typeof(string), Property(typeof(ConfigEntry), nameof(ConfigEntry.Value)).PropertyType);
        Assert.Equal(typeof(bool), Property(typeof(ConfigEntry), nameof(ConfigEntry.IsDefault)).PropertyType);
        Assert.Equal(typeof(bool), Property(typeof(ConfigEntry), nameof(ConfigEntry.IsSensitive)).PropertyType);
        Assert.Equal(typeof(bool), Property(typeof(ConfigEntry), nameof(ConfigEntry.IsReadOnly)).PropertyType);

        Assert.Equal(
            NullableAnnotation.NotAnnotated,
            NullableAnnotation.Flag(Property(typeof(ConfigEntry), nameof(ConfigEntry.Name))));

        Assert.Equal(
            NullableAnnotation.Annotated,
            NullableAnnotation.Flag(Property(typeof(ConfigEntry), nameof(ConfigEntry.Value))));

        Assert.Equal(
            typeof(ConfigEntry.ConfigSource), Property(typeof(ConfigEntry), nameof(ConfigEntry.Source)).PropertyType);
        Assert.Equal(
            typeof(ConfigEntry.ConfigType), Property(typeof(ConfigEntry), nameof(ConfigEntry.Type)).PropertyType);
        Assert.Equal(
            typeof(string), Property(typeof(ConfigEntry), nameof(ConfigEntry.Documentation)).PropertyType);
        Assert.Equal(
            typeof(IReadOnlyList<ConfigEntry.ConfigSynonym>),
            Property(typeof(ConfigEntry), nameof(ConfigEntry.Synonyms)).PropertyType);

        Assert.Equal(
            NullableAnnotation.Annotated,
            NullableAnnotation.Flag(Property(typeof(ConfigEntry), nameof(ConfigEntry.Documentation))));

        Assert.Equal(
            NullableAnnotation.NotAnnotated,
            NullableAnnotation.Flag(Property(typeof(ConfigEntry), nameof(ConfigEntry.Synonyms))));

        Assert.Equal(
            new[]
            {
                "Documentation", "IsDefault", "IsReadOnly", "IsSensitive", "Name", "Source", "Synonyms", "Type",
                "Value",
            },
            typeof(ConfigEntry).GetProperties().Select(p => p.Name).OrderBy(n => n, StringComparer.Ordinal));
        Assert.All(typeof(ConfigEntry).GetProperties(), property => Assert.Null(property.SetMethod));
    }

    [Fact]
    public void IsDefault_IsDerivedFromSource_NotStored()
    {
        Assert.True(
            new ConfigEntry(
                "k", "v", ConfigEntry.ConfigSource.DefaultConfig, false, false,
                Array.Empty<ConfigEntry.ConfigSynonym>(), ConfigEntry.ConfigType.String, null).IsDefault);

        foreach (ConfigEntry.ConfigSource source in Enum.GetValues(typeof(ConfigEntry.ConfigSource))
                     .Cast<ConfigEntry.ConfigSource>()
                     .Where(source => source != ConfigEntry.ConfigSource.DefaultConfig))
        {
            Assert.False(
                new ConfigEntry(
                    "k", "v", source, false, false, Array.Empty<ConfigEntry.ConfigSynonym>(),
                    ConfigEntry.ConfigType.String, null).IsDefault,
                $"{source} is not DEFAULT_CONFIG, so IsDefault must be false");
        }

        ConfigEntry plain = new ConfigEntry("k", "v");
        Assert.Equal(ConfigEntry.ConfigSource.Unknown, plain.Source);
        Assert.Equal(ConfigEntry.ConfigType.Unknown, plain.Type);
        Assert.Empty(plain.Synonyms);
        Assert.Null(plain.Documentation);
        Assert.False(plain.IsDefault);
    }

    [Fact]
    public void TheTwoNameEncodedEnums_StayNested_WithJavasMembers()
    {
        Assert.Equal(typeof(ConfigEntry), typeof(ConfigEntry.ConfigType).DeclaringType);
        Assert.Equal(typeof(ConfigEntry), typeof(ConfigEntry.ConfigSource).DeclaringType);

        Assert.Equal(
            new[] { "Unknown", "Boolean", "String", "Int", "Short", "Long", "Double", "List", "Class", "Password" },
            Enum.GetNames(typeof(ConfigEntry.ConfigType)));

        Assert.Equal(
            new[]
            {
                "DynamicTopicConfig", "DynamicBrokerLoggerConfig", "DynamicBrokerConfig",
                "DynamicDefaultBrokerConfig", "DynamicClientMetricsConfig", "DynamicGroupConfig",
                "StaticBrokerConfig", "DefaultConfig", "Unknown",
            },
            Enum.GetNames(typeof(ConfigEntry.ConfigSource)));
    }

    [Fact]
    public void ConfigSynonym_MirrorsJavasShape()
    {
        Assert.Equal(typeof(ConfigEntry), typeof(ConfigEntry.ConfigSynonym).DeclaringType);
        Assert.Empty(typeof(ConfigEntry.ConfigSynonym).GetConstructors());

        Assert.Equal(
            new[] { "Name", "Source", "Value" },
            typeof(ConfigEntry.ConfigSynonym).GetProperties()
                .Select(p => p.Name)
                .OrderBy(n => n, StringComparer.Ordinal));

        Assert.Equal(
            typeof(string),
            Property(typeof(ConfigEntry.ConfigSynonym), nameof(ConfigEntry.ConfigSynonym.Name)).PropertyType);
        Assert.Equal(
            typeof(ConfigEntry.ConfigSource),
            Property(typeof(ConfigEntry.ConfigSynonym), nameof(ConfigEntry.ConfigSynonym.Source)).PropertyType);

        Assert.Equal(
            NullableAnnotation.Annotated,
            NullableAnnotation.Flag(
                Property(typeof(ConfigEntry.ConfigSynonym), nameof(ConfigEntry.ConfigSynonym.Value))));
    }

    [Fact]
    public void AlterConfigOp_MirrorsJavasShape()
    {
        ConstructorInfo only = Assert.Single(typeof(AlterConfigOp).GetConstructors());
        Assert.Equal(
            new[] { typeof(ConfigEntry), typeof(AlterConfigOpType) },
            only.GetParameters().Select(parameter => parameter.ParameterType));

        Assert.Equal(
            typeof(ConfigEntry), Property(typeof(AlterConfigOp), nameof(AlterConfigOp.ConfigEntry)).PropertyType);
        Assert.Equal(
            typeof(AlterConfigOpType), Property(typeof(AlterConfigOp), nameof(AlterConfigOp.OpType)).PropertyType);

        ConfigEntry entry = new ConfigEntry("k", "v");
        AlterConfigOp op = new AlterConfigOp(entry, AlterConfigOpType.Set);

        Assert.Equal(new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOpType.Set), op);
        Assert.Equal(
            new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOpType.Set).GetHashCode(), op.GetHashCode());
        Assert.NotEqual(new AlterConfigOp(entry, AlterConfigOpType.Delete), op);
        Assert.NotEqual(new AlterConfigOp(new ConfigEntry("k", "other"), AlterConfigOpType.Set), op);

        Assert.StartsWith("AlterConfigOp{opType=SET, configEntry=ConfigEntry(", op.ToString(), StringComparison.Ordinal);

        // Java renders both booleans in lowercase, where bool.ToString() gives "True" and "False".
        Assert.Contains("isSensitive=false, isReadOnly=false", entry.ToString(), StringComparison.Ordinal);
        Assert.Contains(
            "isSensitive=true, isReadOnly=true",
            new ConfigEntry(
                "k",
                "v",
                ConfigEntry.ConfigSource.Unknown,
                isSensitive: true,
                isReadOnly: true,
                synonyms: Array.Empty<ConfigEntry.ConfigSynonym>(),
                type: ConfigEntry.ConfigType.Unknown,
                documentation: null).ToString(),
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(AlterConfigOpType.Set, 0)]
    [InlineData(AlterConfigOpType.Delete, 1)]
    [InlineData(AlterConfigOpType.Append, 2)]
    [InlineData(AlterConfigOpType.Subtract, 3)]
    public void AlterConfigOpType_CarriesJavasWireIds(AlterConfigOpType opType, int id)
    {
        Assert.Equal(id, (int)opType);
        Assert.Equal(opType, (AlterConfigOpType)id);
    }

    [Fact]
    public void AlterConfigOpType_IsFlattened_WithExactlyJavasFourMembers()
    {
        Assert.Null(typeof(AlterConfigOpType).DeclaringType);
        Assert.Equal("Confluent.Kafka.Admin", typeof(AlterConfigOpType).Namespace);
        Assert.Equal(
            new[] { "Append", "Delete", "Set", "Subtract" },
            Enum.GetNames(typeof(AlterConfigOpType)).OrderBy(name => name, StringComparer.Ordinal));

        // AlterConfigOp has a member named OpType, so a nested type of that name would be CS0102: the enum is flattened.
        Assert.NotNull(typeof(AlterConfigOp).GetProperty("OpType"));
        Assert.Null(typeof(AlterConfigOp).GetNestedType("OpType"));
    }

    [Theory]
    [InlineData(99)]
    [InlineData(-1)]
    public void AlterConfigOp_RejectsAnUndefinedOpType(int code)
    {
        AlterConfigOpType undefined = (AlterConfigOpType)code;

        ArgumentOutOfRangeException thrown = Assert.Throws<ArgumentOutOfRangeException>(
            () => new AlterConfigOp(new ConfigEntry("k", "v"), undefined));

        Assert.Equal("opType", thrown.ParamName);
        Assert.StartsWith("opType must be a defined AlterConfigOpType member.", thrown.Message, StringComparison.Ordinal);
        Assert.Equal(undefined, thrown.ActualValue);
    }

    [Fact]
    public void AlterConfigOp_AcceptsEveryDefinedOpType()
    {
        AlterConfigOpType[] members = (AlterConfigOpType[])Enum.GetValues(typeof(AlterConfigOpType));
        Assert.Equal(4, members.Length);

        foreach (AlterConfigOpType member in members)
        {
            Assert.Equal(member, new AlterConfigOp(new ConfigEntry("k", "v"), member).OpType);
        }
    }

    [Fact]
    public void Config_DuplicateNames_AreOneEntry_TheLastWinning_InTheFirstPosition()
    {
        ConfigEntry firstA = new ConfigEntry("a", "1");
        ConfigEntry b = new ConfigEntry("b", "2");
        ConfigEntry lastA = new ConfigEntry("a", "3");

        Config config = new Config(new[] { firstA, b, lastA });

        Assert.Equal(2, config.Entries.Count);
        Assert.Equal(new[] { "a", "b" }, config.Entries.Select(entry => entry.Name));
        Assert.Same(lastA, config.Entries.First());
        Assert.Same(b, config.Entries.Last());
        Assert.Same(lastA, config.Get("a"));
        Assert.Equal("3", config.Get("a")!.Value);
    }

    [Fact]
    public void Config_GetNull_ReturnsNull()
    {
        Config config = new Config(new[] { new ConfigEntry("a", "1") });

        Assert.Null(config.Get(null));
        Assert.Null(new Config(Array.Empty<ConfigEntry>()).Get(null));
    }

    [Fact]
    public void Config_Entries_CannotBeCastBackAndMutated()
    {
        Config config = new Config(new[] { new ConfigEntry("a", "1") });

        Assert.False(config.Entries is List<ConfigEntry>);
        ICollection<ConfigEntry> collection = Assert.IsAssignableFrom<ICollection<ConfigEntry>>(config.Entries);
        Assert.True(collection.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => collection.Add(new ConfigEntry("b", "2")));
        Assert.Equal(new[] { "a" }, config.Entries.Select(entry => entry.Name));
    }

    [Fact]
    public void Config_Equality_IsOrderInsensitiveMapEquality()
    {
        Config forward = new Config(new[] { new ConfigEntry("a", "1"), new ConfigEntry("b", "2") });
        Config reversed = new Config(new[] { new ConfigEntry("b", "2"), new ConfigEntry("a", "1") });

        Assert.NotSame(forward, reversed);
        Assert.True(forward.Equals(reversed));
        Assert.True(reversed.Equals(forward));
        Assert.Equal(forward.GetHashCode(), reversed.GetHashCode());

        Config withReplacedDuplicate = new Config(
            new[] { new ConfigEntry("a", "0"), new ConfigEntry("b", "2"), new ConfigEntry("a", "1") });
        Assert.True(forward.Equals(withReplacedDuplicate));
        Assert.Equal(forward.GetHashCode(), withReplacedDuplicate.GetHashCode());

        Config oneValueDiffers = new Config(new[] { new ConfigEntry("a", "1"), new ConfigEntry("b", "other") });
        Assert.False(forward.Equals(oneValueDiffers));
        Assert.False(oneValueDiffers.Equals(forward));

        Config subset = new Config(new[] { new ConfigEntry("a", "1") });
        Assert.False(forward.Equals(subset));
        Assert.False(subset.Equals(forward));

        Assert.False(forward.Equals(null));
        Assert.False(forward.Equals("Config(entries=[])"));
        Assert.True(new Config(Array.Empty<ConfigEntry>()).Equals(new Config(Array.Empty<ConfigEntry>())));
    }

    [Fact]
    public void Config_ToString_IsJavasLayout_InEntriesOrder()
    {
        ConfigEntry a = new ConfigEntry("a", "1");
        ConfigEntry b = new ConfigEntry("b", "2");

        Assert.Equal("Config(entries=[])", new Config(Array.Empty<ConfigEntry>()).ToString());
        Assert.Equal("Config(entries=[" + a + "])", new Config(new[] { a }).ToString());
        Assert.Equal("Config(entries=[" + b + ", " + a + "])", new Config(new[] { b, a }).ToString());
    }

    [Fact]
    public void Config_ValueMembers_AreDeclaredOverrides_AndGetTakesANullableName()
    {
        const BindingFlags Declared = BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly;
        MethodInfo[] declared = typeof(Config).GetMethods(Declared);

        MethodInfo equals = Assert.Single(declared, method => method.Name == nameof(object.Equals));
        Assert.Equal(new[] { typeof(object) }, equals.GetParameters().Select(parameter => parameter.ParameterType));
        MethodInfo hash = Assert.Single(declared, method => method.Name == nameof(object.GetHashCode));
        Assert.Empty(hash.GetParameters());
        MethodInfo toString = Assert.Single(declared, method => method.Name == nameof(object.ToString));
        Assert.Empty(toString.GetParameters());

        foreach (MethodInfo overridden in new[] { equals, hash, toString })
        {
            Assert.NotEqual(overridden.DeclaringType, overridden.GetBaseDefinition().DeclaringType);
        }

        Assert.DoesNotContain(typeof(IEquatable<Config>), typeof(Config).GetInterfaces());

        MethodInfo get = Assert.Single(declared, method => method.Name == nameof(Config.Get));
        ParameterInfo name = Assert.Single(get.GetParameters());
        Assert.Equal(typeof(string), name.ParameterType);
        Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(name));
        Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(get.ReturnParameter));

#if NET8_0_OR_GREATER
        Assert.Equal(NullabilityState.Nullable, new NullabilityInfoContext().Create(name).ReadState);
#endif
    }

    private static readonly Dictionary<ConfigResourceType, string> s_javaResourceTypes =
        new Dictionary<ConfigResourceType, string>
        {
            [ConfigResourceType.Unknown] = "UNKNOWN",
            [ConfigResourceType.Topic] = "TOPIC",
            [ConfigResourceType.Broker] = "BROKER",
            [ConfigResourceType.BrokerLogger] = "BROKER_LOGGER",
            [ConfigResourceType.ClientMetrics] = "CLIENT_METRICS",
            [ConfigResourceType.Group] = "GROUP",
        };

    private static readonly Dictionary<AlterConfigOpType, string> s_javaOpTypes =
        new Dictionary<AlterConfigOpType, string>
        {
            [AlterConfigOpType.Set] = "SET",
            [AlterConfigOpType.Delete] = "DELETE",
            [AlterConfigOpType.Append] = "APPEND",
            [AlterConfigOpType.Subtract] = "SUBTRACT",
        };

    private static readonly Dictionary<ConfigEntry.ConfigSource, string> s_javaSources =
        new Dictionary<ConfigEntry.ConfigSource, string>
        {
            [ConfigEntry.ConfigSource.DynamicTopicConfig] = "DYNAMIC_TOPIC_CONFIG",
            [ConfigEntry.ConfigSource.DynamicBrokerLoggerConfig] = "DYNAMIC_BROKER_LOGGER_CONFIG",
            [ConfigEntry.ConfigSource.DynamicBrokerConfig] = "DYNAMIC_BROKER_CONFIG",
            [ConfigEntry.ConfigSource.DynamicDefaultBrokerConfig] = "DYNAMIC_DEFAULT_BROKER_CONFIG",
            [ConfigEntry.ConfigSource.DynamicClientMetricsConfig] = "DYNAMIC_CLIENT_METRICS_CONFIG",
            [ConfigEntry.ConfigSource.DynamicGroupConfig] = "DYNAMIC_GROUP_CONFIG",
            [ConfigEntry.ConfigSource.StaticBrokerConfig] = "STATIC_BROKER_CONFIG",
            [ConfigEntry.ConfigSource.DefaultConfig] = "DEFAULT_CONFIG",
            [ConfigEntry.ConfigSource.Unknown] = "UNKNOWN",
        };

    private static readonly Dictionary<ConfigEntry.ConfigType, string> s_javaConfigTypes =
        new Dictionary<ConfigEntry.ConfigType, string>
        {
            [ConfigEntry.ConfigType.Unknown] = "UNKNOWN",
            [ConfigEntry.ConfigType.Boolean] = "BOOLEAN",
            [ConfigEntry.ConfigType.String] = "STRING",
            [ConfigEntry.ConfigType.Int] = "INT",
            [ConfigEntry.ConfigType.Short] = "SHORT",
            [ConfigEntry.ConfigType.Long] = "LONG",
            [ConfigEntry.ConfigType.Double] = "DOUBLE",
            [ConfigEntry.ConfigType.List] = "LIST",
            [ConfigEntry.ConfigType.Class] = "CLASS",
            [ConfigEntry.ConfigType.Password] = "PASSWORD",
        };

    [Fact]
    public void ConfigResource_ToString_PrintsJavasTypeConstantName_ForEveryMember()
    {
        ConfigResourceType[] members = (ConfigResourceType[])Enum.GetValues(typeof(ConfigResourceType));
        Assert.Equal(s_javaResourceTypes.Count, members.Length);

        foreach (ConfigResourceType member in members)
        {
            Assert.True(s_javaResourceTypes.TryGetValue(member, out string? java), member.ToString());
            Assert.Equal("ConfigResource(type=" + java + ", name='r')", new ConfigResource(member, "r").ToString());
        }
    }

    [Fact]
    public void AlterConfigOp_ToString_PrintsJavasOpTypeConstantName_ForEveryMember()
    {
        AlterConfigOpType[] members = (AlterConfigOpType[])Enum.GetValues(typeof(AlterConfigOpType));
        Assert.Equal(s_javaOpTypes.Count, members.Length);

        foreach (AlterConfigOpType member in members)
        {
            Assert.True(s_javaOpTypes.TryGetValue(member, out string? java), member.ToString());
            Assert.Equal(
                "AlterConfigOp{opType=" + java + ", configEntry=ConfigEntry(name=k, value=v, source=UNKNOWN, "
                    + "isSensitive=false, isReadOnly=false, synonyms=[], type=UNKNOWN, documentation=null)}",
                new AlterConfigOp(new ConfigEntry("k", "v"), member).ToString());
        }
    }

    [Fact]
    public void ConfigEntry_AndSynonym_ToString_PrintJavasConstantNames_ForEveryMember()
    {
        ConfigEntry.ConfigSource[] sources = (ConfigEntry.ConfigSource[])Enum.GetValues(typeof(ConfigEntry.ConfigSource));
        Assert.Equal(s_javaSources.Count, sources.Length);
        foreach (ConfigEntry.ConfigSource source in sources)
        {
            Assert.True(s_javaSources.TryGetValue(source, out string? java), source.ToString());
            Assert.Equal(
                "ConfigEntry(name=k, value=v, source=" + java + ", isSensitive=false, isReadOnly=false, "
                    + "synonyms=[], type=UNKNOWN, documentation=doc)",
                Entry(source, ConfigEntry.ConfigType.Unknown, "v", "doc").ToString());
            Assert.Equal(
                "ConfigSynonym(name=a, value=1, source=" + java + ")",
                new ConfigEntry.ConfigSynonym("a", "1", source).ToString());
        }

        ConfigEntry.ConfigType[] types = (ConfigEntry.ConfigType[])Enum.GetValues(typeof(ConfigEntry.ConfigType));
        Assert.Equal(s_javaConfigTypes.Count, types.Length);
        foreach (ConfigEntry.ConfigType type in types)
        {
            Assert.True(s_javaConfigTypes.TryGetValue(type, out string? java), type.ToString());
            Assert.Equal(
                "ConfigEntry(name=k, value=v, source=UNKNOWN, isSensitive=false, isReadOnly=false, "
                    + "synonyms=[], type=" + java + ", documentation=doc)",
                Entry(ConfigEntry.ConfigSource.Unknown, type, "v", "doc").ToString());
        }
    }

    [Fact]
    public void ConfigRenderings_PrintANullAsNull()
    {
        Assert.Equal(
            "ConfigEntry(name=k, value=null, source=UNKNOWN, isSensitive=false, isReadOnly=false, "
                + "synonyms=[], type=UNKNOWN, documentation=null)",
            new ConfigEntry("k", null).ToString());
        Assert.Equal(
            "ConfigSynonym(name=a, value=null, source=DEFAULT_CONFIG)",
            new ConfigEntry.ConfigSynonym("a", null, ConfigEntry.ConfigSource.DefaultConfig).ToString());
        Assert.Equal(
            "Config(entries=[ConfigEntry(name=a, value=1, source=UNKNOWN, isSensitive=false, isReadOnly=false, "
                + "synonyms=[], type=UNKNOWN, documentation=null)])",
            new Config(new[] { new ConfigEntry("a", "1") }).ToString());
    }

    [Fact]
    public void ConfigEntry_AnUndefinedSourceOrType_PrintsItsNumericValue()
    {
        Assert.Equal(
            "ConfigEntry(name=k, value=v, source=99, isSensitive=false, isReadOnly=false, "
                + "synonyms=[], type=77, documentation=doc)",
            Entry((ConfigEntry.ConfigSource)99, (ConfigEntry.ConfigType)77, "v", "doc").ToString());
        Assert.Equal(
            "ConfigSynonym(name=a, value=1, source=-5)",
            new ConfigEntry.ConfigSynonym("a", "1", (ConfigEntry.ConfigSource)(-5)).ToString());
    }

    [Fact]
    public void ConfigEntry_ASensitiveValue_StillRendersRedacted()
    {
        foreach (string? value in new[] { "secret", null })
        {
            ConfigEntry sensitive = new ConfigEntry(
                "k",
                value,
                ConfigEntry.ConfigSource.DynamicBrokerConfig,
                isSensitive: true,
                isReadOnly: false,
                synonyms: Array.Empty<ConfigEntry.ConfigSynonym>(),
                type: ConfigEntry.ConfigType.Password,
                documentation: null);

            Assert.Equal(
                "ConfigEntry(name=k, value=Redacted, source=DYNAMIC_BROKER_CONFIG, isSensitive=true, isReadOnly=false, "
                    + "synonyms=[], type=PASSWORD, documentation=null)",
                sensitive.ToString());
        }
    }

    private static ConfigEntry Entry(
        ConfigEntry.ConfigSource source, ConfigEntry.ConfigType type, string? value, string? documentation) =>
        new ConfigEntry(
            "k",
            value,
            source,
            isSensitive: false,
            isReadOnly: false,
            synonyms: Array.Empty<ConfigEntry.ConfigSynonym>(),
            type: type,
            documentation: documentation);

    [Fact]
    public void Options_MatchJavasFieldsAndDefaults()
    {
        DescribeConfigsOptions describe = new DescribeConfigsOptions();
        Assert.Null(describe.TimeoutMs);
        Assert.False(describe.IncludeSynonyms);
        Assert.False(describe.IncludeDocumentation);
        Assert.Equal(
            new[] { "IncludeDocumentation", "IncludeSynonyms", "TimeoutMs" },
            typeof(DescribeConfigsOptions).GetProperties()
                .Select(p => p.Name)
                .OrderBy(n => n, StringComparer.Ordinal));

        AlterConfigsOptions alter = new AlterConfigsOptions();
        Assert.Null(alter.TimeoutMs);
        Assert.False(alter.ValidateOnly);
        Assert.Equal(
            new[] { "TimeoutMs", "ValidateOnly" },
            typeof(AlterConfigsOptions).GetProperties()
                .Select(p => p.Name)
                .OrderBy(n => n, StringComparer.Ordinal));

        foreach (Type type in new[] { typeof(DescribeConfigsOptions), typeof(AlterConfigsOptions) })
        {
            PropertyInfo timeout = type.GetProperty("TimeoutMs")!;
            Assert.Equal(typeof(int?), timeout.PropertyType);
            Assert.NotNull(timeout.SetMethod);
        }
    }

    [Fact]
    public void NoIncrementalAlterConfigsResultType_Exists()
    {
        Assert.DoesNotContain(
            typeof(IAdmin).Assembly.GetExportedTypes(),
            type => type.Name.IndexOf("IncrementalAlterConfigsResult", StringComparison.Ordinal) >= 0);
    }

    [Fact]
    public void TheNamespaceSplit_FollowsJavasPackages()
    {
        foreach (Type type in new[]
                 {
                     typeof(AlterConfigOp), typeof(AlterConfigOpType), typeof(DescribeConfigsResult),
                     typeof(AlterConfigsResult), typeof(DescribeConfigsOptions), typeof(AlterConfigsOptions),
                     typeof(ConfigEntry), typeof(Config),
                 })
        {
            Assert.Equal("Confluent.Kafka.Admin", type.Namespace);
        }

        Assert.Equal("Confluent.Kafka", typeof(ConfigResource).Namespace);
        Assert.Equal("Confluent.Kafka", typeof(ConfigResourceType).Namespace);
    }

    private static PropertyInfo Property(Type type, string name) => type.GetProperty(name)!;
}
