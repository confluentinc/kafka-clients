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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Config;
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
        MethodInfo describe = typeof(IAdmin).GetMethod(
            nameof(IAdmin.DescribeConfigs),
            new[] { typeof(IReadOnlyCollection<ConfigResource>), typeof(DescribeConfigsOptions) })!;
        MethodInfo alter = typeof(IAdmin).GetMethod(
            nameof(IAdmin.IncrementalAlterConfigs),
            new[] { typeof(IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>), typeof(AlterConfigsOptions) })!;

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
            Assert.False(parameters[1].IsOptional, $"{rpc.Name}'s options must be required");
            Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(parameters[1]));
            MethodInfo? shortForm = typeof(IAdmin).GetMethod(rpc.Name, new[] { parameters[0].ParameterType });
            Assert.NotNull(shortForm);
            Assert.Equal(rpc.ReturnType, shortForm!.ReturnType);
        }

        Assert.Empty(
            typeof(IAdmin).GetMethods()
                .Where(method => typeof(Task).IsAssignableFrom(method.ReturnType))
                .Select(method => method.Name));
    }

    [Fact]
    public void TheTwoResults_DifferExactlyAsJavasDo()
    {
        MethodInfo describeValues = Getters.Get(typeof(DescribeConfigsResult), nameof(DescribeConfigsResult.Values));
        Assert.Equal(typeof(IReadOnlyDictionary<ConfigResource, KafkaFuture<Config>>), describeValues.ReturnType);

        MethodInfo describeAll = typeof(DescribeConfigsResult).GetMethod(
            nameof(DescribeConfigsResult.All), Type.EmptyTypes)!;
        Assert.Equal(typeof(KafkaFuture<IReadOnlyDictionary<ConfigResource, Config>>), describeAll.ReturnType);

        MethodInfo alterValues = Getters.Get(typeof(AlterConfigsResult), nameof(AlterConfigsResult.Values));

        Assert.Equal(typeof(IReadOnlyDictionary<ConfigResource, KafkaFuture>), alterValues.ReturnType);

        MethodInfo alterAll = typeof(AlterConfigsResult).GetMethod(nameof(AlterConfigsResult.All), Type.EmptyTypes)!;

        Assert.Equal(typeof(KafkaFuture), alterAll.ReturnType);

        Assert.Empty(typeof(DescribeConfigsResult).GetConstructors());
        Assert.Empty(typeof(AlterConfigsResult).GetConstructors());
    }

    [Fact]
    public void ConfigEntry_KeepsP1sFiveMembers_AndGainsJavasFour()
    {
        Assert.Equal(typeof(string), Getters.Get(typeof(ConfigEntry), nameof(ConfigEntry.Name)).ReturnType);
        Assert.Equal(typeof(string), Getters.Get(typeof(ConfigEntry), nameof(ConfigEntry.Value)).ReturnType);
        Assert.Equal(typeof(bool), Getters.Get(typeof(ConfigEntry), nameof(ConfigEntry.IsDefault)).ReturnType);
        Assert.Equal(typeof(bool), Getters.Get(typeof(ConfigEntry), nameof(ConfigEntry.IsSensitive)).ReturnType);
        Assert.Equal(typeof(bool), Getters.Get(typeof(ConfigEntry), nameof(ConfigEntry.IsReadOnly)).ReturnType);

        Assert.Equal(
            NullableAnnotation.NotAnnotated,
            NullableAnnotation.Flag(Getters.Get(typeof(ConfigEntry), nameof(ConfigEntry.Name))));

        Assert.Equal(
            NullableAnnotation.Annotated,
            NullableAnnotation.Flag(Getters.Get(typeof(ConfigEntry), nameof(ConfigEntry.Value))));

        Assert.Equal(
            typeof(ConfigEntry.ConfigSource), Getters.Get(typeof(ConfigEntry), nameof(ConfigEntry.Source)).ReturnType);
        Assert.Equal(
            typeof(ConfigEntry.ConfigType?), Getters.Get(typeof(ConfigEntry), nameof(ConfigEntry.Type)).ReturnType);
        Assert.Equal(
            typeof(string), Getters.Get(typeof(ConfigEntry), nameof(ConfigEntry.Documentation)).ReturnType);
        Assert.Equal(
            typeof(IReadOnlyList<ConfigEntry.ConfigSynonym>),
            Getters.Get(typeof(ConfigEntry), nameof(ConfigEntry.Synonyms)).ReturnType);

        Assert.Equal(
            NullableAnnotation.Annotated,
            NullableAnnotation.Flag(Getters.Get(typeof(ConfigEntry), nameof(ConfigEntry.Documentation))));

        Assert.Equal(
            NullableAnnotation.NotAnnotated,
            NullableAnnotation.Flag(Getters.Get(typeof(ConfigEntry), nameof(ConfigEntry.Synonyms))));

        Assert.Equal(
            new[]
            {
                "Documentation", "IsDefault", "IsReadOnly", "IsSensitive", "Name", "Source", "Synonyms", "Type",
                "Value",
            },
            Getters.NamesOf(typeof(ConfigEntry)));
        Assert.Empty(typeof(ConfigEntry).GetProperties());
    }

    [Fact]
    public void IsDefault_IsDerivedFromSource_NotStored()
    {
        Assert.True(
            new ConfigEntry(
                "k", "v", ConfigEntry.ConfigSource.DEFAULT_CONFIG, false, false,
                Array.Empty<ConfigEntry.ConfigSynonym>(), ConfigEntry.ConfigType.STRING, null).IsDefault());

        foreach (ConfigEntry.ConfigSource source in Enum.GetValues(typeof(ConfigEntry.ConfigSource))
                     .Cast<ConfigEntry.ConfigSource>()
                     .Where(source => source != ConfigEntry.ConfigSource.DEFAULT_CONFIG))
        {
            Assert.False(
                new ConfigEntry(
                    "k", "v", source, false, false, Array.Empty<ConfigEntry.ConfigSynonym>(),
                    ConfigEntry.ConfigType.STRING, null).IsDefault(),
                $"{source} is not DEFAULT_CONFIG, so IsDefault must be false");
        }

        ConfigEntry plain = new ConfigEntry("k", "v");
        Assert.Equal(ConfigEntry.ConfigSource.UNKNOWN, plain.Source());
        Assert.Equal(ConfigEntry.ConfigType.UNKNOWN, plain.Type());
        Assert.Empty(plain.Synonyms());
        Assert.Null(plain.Documentation());
        Assert.False(plain.IsDefault());
    }

    [Fact]
    public void TheTwoNameEncodedEnums_StayNested_WithJavasMembers()
    {
        Assert.Equal(typeof(ConfigEntry), typeof(ConfigEntry.ConfigType).DeclaringType);
        Assert.Equal(typeof(ConfigEntry), typeof(ConfigEntry.ConfigSource).DeclaringType);

        Assert.Equal(
            new[] { "UNKNOWN", "BOOLEAN", "STRING", "INT", "SHORT", "LONG", "DOUBLE", "LIST", "CLASS", "PASSWORD" },
            Enum.GetNames(typeof(ConfigEntry.ConfigType)));

        Assert.Equal(
            new[]
            {
                "DYNAMIC_TOPIC_CONFIG", "DYNAMIC_BROKER_LOGGER_CONFIG", "DYNAMIC_BROKER_CONFIG",
                "DYNAMIC_DEFAULT_BROKER_CONFIG", "DYNAMIC_CLIENT_METRICS_CONFIG", "DYNAMIC_GROUP_CONFIG",
                "STATIC_BROKER_CONFIG", "DEFAULT_CONFIG", "UNKNOWN",
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
            Getters.NamesOf(typeof(ConfigEntry.ConfigSynonym)));
        Assert.Empty(typeof(ConfigEntry.ConfigSynonym).GetProperties());

        Assert.Equal(
            typeof(string),
            Getters.Get(typeof(ConfigEntry.ConfigSynonym), nameof(ConfigEntry.ConfigSynonym.Name)).ReturnType);
        Assert.Equal(
            typeof(ConfigEntry.ConfigSource),
            Getters.Get(typeof(ConfigEntry.ConfigSynonym), nameof(ConfigEntry.ConfigSynonym.Source)).ReturnType);

        Assert.Equal(
            NullableAnnotation.Annotated,
            NullableAnnotation.Flag(
                Getters.Get(typeof(ConfigEntry.ConfigSynonym), nameof(ConfigEntry.ConfigSynonym.Value))));
    }

    [Fact]
    public void AlterConfigOp_MirrorsJavasShape()
    {
        ConstructorInfo only = Assert.Single(typeof(AlterConfigOp).GetConstructors());
        Assert.Equal(
            new[] { typeof(ConfigEntry), typeof(OpType) },
            only.GetParameters().Select(parameter => parameter.ParameterType));

        Assert.Equal(
            typeof(ConfigEntry), Getters.Get(typeof(AlterConfigOp), nameof(AlterConfigOp.ConfigEntry)).ReturnType);
        Assert.Equal(
            typeof(OpType), Getters.Get(typeof(AlterConfigOp), nameof(AlterConfigOp.OpType)).ReturnType);

        ConfigEntry entry = new ConfigEntry("k", "v");
        AlterConfigOp op = new AlterConfigOp(entry, OpType.SET);

        Assert.Equal(new AlterConfigOp(new ConfigEntry("k", "v"), OpType.SET), op);
        Assert.Equal(
            new AlterConfigOp(new ConfigEntry("k", "v"), OpType.SET).GetHashCode(), op.GetHashCode());
        Assert.NotEqual(new AlterConfigOp(entry, OpType.DELETE), op);
        Assert.NotEqual(new AlterConfigOp(new ConfigEntry("k", "other"), OpType.SET), op);

        Assert.StartsWith("AlterConfigOp{opType=SET, configEntry=ConfigEntry(", op.ToString(), StringComparison.Ordinal);

        // Java renders both booleans in lowercase, where bool.ToString() gives "True" and "False".
        Assert.Contains("isSensitive=false, isReadOnly=false", entry.ToString(), StringComparison.Ordinal);
        Assert.Contains(
            "isSensitive=true, isReadOnly=true",
            new ConfigEntry(
                "k",
                "v",
                ConfigEntry.ConfigSource.UNKNOWN,
                isSensitive: true,
                isReadOnly: true,
                synonyms: Array.Empty<ConfigEntry.ConfigSynonym>(),
                type: ConfigEntry.ConfigType.UNKNOWN,
                documentation: null).ToString(),
            StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(OpType.SET, 0)]
    [InlineData(OpType.DELETE, 1)]
    [InlineData(OpType.APPEND, 2)]
    [InlineData(OpType.SUBTRACT, 3)]
    public void OpType_CarriesJavasWireIds(OpType opType, int id)
    {
        Assert.Equal(id, (int)opType);
        Assert.Equal(opType, (OpType)id);
    }

    [Fact]
    public void OpType_IsFlattened_WithExactlyJavasFourMembers()
    {
        Assert.Null(typeof(OpType).DeclaringType);
        Assert.Equal(
            new[] { "APPEND", "DELETE", "SET", "SUBTRACT" },
            Enum.GetNames(typeof(OpType)).OrderBy(name => name, StringComparer.Ordinal));

        // AlterConfigOp has a member named OpType, so a nested type of that name would be CS0102: the enum is flattened.
        Assert.NotNull(typeof(AlterConfigOp).GetMethod("OpType", Type.EmptyTypes));
        Assert.Null(typeof(AlterConfigOp).GetNestedType("OpType"));
    }

    [Theory]
    [InlineData(99)]
    [InlineData(-1)]
    public void AlterConfigOp_RejectsAnUndefinedOpType(int code)
    {
        OpType undefined = (OpType)code;

        ArgumentOutOfRangeException thrown = Assert.Throws<ArgumentOutOfRangeException>(
            () => new AlterConfigOp(new ConfigEntry("k", "v"), undefined));

        Assert.Equal("operationType", thrown.ParamName);
        Assert.StartsWith("operationType must be a defined OpType member.", thrown.Message, StringComparison.Ordinal);
        Assert.Equal(undefined, thrown.ActualValue);
    }

    [Fact]
    public void AlterConfigOp_AcceptsEveryDefinedOpType()
    {
        OpType[] members = (OpType[])Enum.GetValues(typeof(OpType));
        Assert.Equal(4, members.Length);

        foreach (OpType member in members)
        {
            Assert.Equal(member, new AlterConfigOp(new ConfigEntry("k", "v"), member).OpType());
        }
    }

    [Fact]
    public void Config_DuplicateNames_AreOneEntry_TheLastWinning_InTheFirstPosition()
    {
        ConfigEntry firstA = new ConfigEntry("a", "1");
        ConfigEntry b = new ConfigEntry("b", "2");
        ConfigEntry lastA = new ConfigEntry("a", "3");

        Config config = new Config(new[] { firstA, b, lastA });

        Assert.Equal(2, config.Entries().Count);
        Assert.Equal(new[] { "a", "b" }, config.Entries().Select(entry => entry.Name()));
        Assert.Same(lastA, config.Entries().First());
        Assert.Same(b, config.Entries().Last());
        Assert.Same(lastA, config.Get("a"));
        Assert.Equal("3", config.Get("a")!.Value());
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

        Assert.False(config.Entries() is List<ConfigEntry>);
        ICollection<ConfigEntry> collection = Assert.IsAssignableFrom<ICollection<ConfigEntry>>(config.Entries());
        Assert.True(collection.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => collection.Add(new ConfigEntry("b", "2")));
        Assert.Equal(new[] { "a" }, config.Entries().Select(entry => entry.Name()));
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
            [ConfigResourceType.UNKNOWN] = "UNKNOWN",
            [ConfigResourceType.TOPIC] = "TOPIC",
            [ConfigResourceType.BROKER] = "BROKER",
            [ConfigResourceType.BROKER_LOGGER] = "BROKER_LOGGER",
            [ConfigResourceType.CLIENT_METRICS] = "CLIENT_METRICS",
            [ConfigResourceType.GROUP] = "GROUP",
        };

    private static readonly Dictionary<OpType, string> s_javaOpTypes =
        new Dictionary<OpType, string>
        {
            [OpType.SET] = "SET",
            [OpType.DELETE] = "DELETE",
            [OpType.APPEND] = "APPEND",
            [OpType.SUBTRACT] = "SUBTRACT",
        };

    private static readonly Dictionary<ConfigEntry.ConfigSource, string> s_javaSources =
        new Dictionary<ConfigEntry.ConfigSource, string>
        {
            [ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG] = "DYNAMIC_TOPIC_CONFIG",
            [ConfigEntry.ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG] = "DYNAMIC_BROKER_LOGGER_CONFIG",
            [ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG] = "DYNAMIC_BROKER_CONFIG",
            [ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG] = "DYNAMIC_DEFAULT_BROKER_CONFIG",
            [ConfigEntry.ConfigSource.DYNAMIC_CLIENT_METRICS_CONFIG] = "DYNAMIC_CLIENT_METRICS_CONFIG",
            [ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG] = "DYNAMIC_GROUP_CONFIG",
            [ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG] = "STATIC_BROKER_CONFIG",
            [ConfigEntry.ConfigSource.DEFAULT_CONFIG] = "DEFAULT_CONFIG",
            [ConfigEntry.ConfigSource.UNKNOWN] = "UNKNOWN",
        };

    private static readonly Dictionary<ConfigEntry.ConfigType, string> s_javaConfigTypes =
        new Dictionary<ConfigEntry.ConfigType, string>
        {
            [ConfigEntry.ConfigType.UNKNOWN] = "UNKNOWN",
            [ConfigEntry.ConfigType.BOOLEAN] = "BOOLEAN",
            [ConfigEntry.ConfigType.STRING] = "STRING",
            [ConfigEntry.ConfigType.INT] = "INT",
            [ConfigEntry.ConfigType.SHORT] = "SHORT",
            [ConfigEntry.ConfigType.LONG] = "LONG",
            [ConfigEntry.ConfigType.DOUBLE] = "DOUBLE",
            [ConfigEntry.ConfigType.LIST] = "LIST",
            [ConfigEntry.ConfigType.CLASS] = "CLASS",
            [ConfigEntry.ConfigType.PASSWORD] = "PASSWORD",
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
        OpType[] members = (OpType[])Enum.GetValues(typeof(OpType));
        Assert.Equal(s_javaOpTypes.Count, members.Length);

        foreach (OpType member in members)
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
                Entry(source, ConfigEntry.ConfigType.UNKNOWN, "v", "doc").ToString());
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
                Entry(ConfigEntry.ConfigSource.UNKNOWN, type, "v", "doc").ToString());
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
            new ConfigEntry.ConfigSynonym("a", null, ConfigEntry.ConfigSource.DEFAULT_CONFIG).ToString());
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
                ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG,
                isSensitive: true,
                isReadOnly: false,
                synonyms: Array.Empty<ConfigEntry.ConfigSynonym>(),
                type: ConfigEntry.ConfigType.PASSWORD,
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
        Assert.Null(describe.TimeoutMs());
        Assert.False(describe.IncludeSynonyms());
        Assert.False(describe.IncludeDocumentation());
        SetterMethods.AssertGettersAndSetters(
            typeof(DescribeConfigsOptions), "IncludeDocumentation", "IncludeSynonyms", "TimeoutMs");

        AlterConfigsOptions alter = new AlterConfigsOptions();
        Assert.Null(alter.TimeoutMs());
        Assert.False(alter.ShouldValidateOnly());
        SetterMethods.AssertGettersAndSetters(
            typeof(AlterConfigsOptions),
            getters: new[] { "ShouldValidateOnly", "TimeoutMs" },
            setters: new[] { "TimeoutMs", "ValidateOnly" });

        foreach (Type type in new[] { typeof(DescribeConfigsOptions), typeof(AlterConfigsOptions) })
        {
            Assert.Equal(typeof(int?), Getters.Get(type, "TimeoutMs").ReturnType);
            Assert.Equal(typeof(int?), SetterMethods.ParameterTypeOf(type, "TimeoutMs"));
        }
    }

    [Fact]
    public void NoIncrementalAlterConfigsResultType_Exists()
    {
        Assert.DoesNotContain(
            typeof(IAdmin).Assembly.GetExportedTypes(),
            type => type.Name.IndexOf("IncrementalAlterConfigsResult", StringComparison.Ordinal) >= 0);
    }
}
