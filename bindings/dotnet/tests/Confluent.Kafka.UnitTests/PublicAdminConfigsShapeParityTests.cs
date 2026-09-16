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
/// Pins the <b>public shape</b> of M15/P3 Stage 2's surface against the Java classes it
/// mirrors.
/// </summary>
/// <remarks>
/// ⚠ <b>C# upcasts and widens silently, so these have to be reflection assertions.</b> A
/// behavioural test passes equally against <c>Task</c> and <c>Task&lt;Map&gt;</c>, against a
/// property and a method, and against <c>T</c> and <c>T?</c>.
/// </remarks>
public sealed class PublicAdminConfigsShapeParityTests
{
    /// <summary>
    /// The two new RPCs return their <c>*Result</c> <b>synchronously</b>
    /// (<c>admin-client.md</c> §1), with Java's parameter shape.
    /// </summary>
    [Fact]
    public void IAdmin_TheTwoNewRpcsAreSynchronous_WithTheJavaParameterShape()
    {
        MethodInfo describe = typeof(IAdmin).GetMethod(nameof(IAdmin.DescribeConfigs))!;
        MethodInfo alter = typeof(IAdmin).GetMethod(nameof(IAdmin.IncrementalAlterConfigs))!;

        Assert.Equal(typeof(DescribeConfigsResult), describe.ReturnType);

        // ⚠ Java's incrementalAlterConfigs returns AlterConfigsResult — there is no
        // IncrementalAlterConfigsResult in Java or in the ABI.
        Assert.Equal(typeof(AlterConfigsResult), alter.ReturnType);

        // describeConfigs(Collection<ConfigResource>, DescribeConfigsOptions)
        Assert.Equal(
            new[] { typeof(IReadOnlyCollection<ConfigResource>), typeof(DescribeConfigsOptions) },
            describe.GetParameters().Select(parameter => parameter.ParameterType));

        // incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>>, AlterConfigsOptions)
        Assert.Equal(
            new[]
            {
                typeof(IReadOnlyDictionary<ConfigResource, IReadOnlyCollection<AlterConfigOp>>),
                typeof(AlterConfigsOptions),
            },
            alter.GetParameters().Select(parameter => parameter.ParameterType));

        // The options parameter is optional and nullable on each; the required one is not.
        foreach (MethodInfo rpc in new[] { describe, alter })
        {
            ParameterInfo[] parameters = rpc.GetParameters();
            Assert.False(parameters[0].IsOptional, $"{rpc.Name}'s input must be required");
            Assert.Equal(NullableAnnotation.NotAnnotated, NullableAnnotation.Flag(parameters[0]));
            Assert.True(parameters[1].IsOptional, $"{rpc.Name}'s options must be optional");
            Assert.Equal(NullableAnnotation.Annotated, NullableAnnotation.Flag(parameters[1]));
        }

        // Close remains the ONLY Task-returning member on IAdmin.
        Assert.Equal(
            new[] { nameof(IAdmin.Close) },
            typeof(IAdmin).GetMethods()
                .Where(method => typeof(Task).IsAssignableFrom(method.ReturnType))
                .Select(method => method.Name)
                .OrderBy(name => name, StringComparer.Ordinal));
    }

    /// <summary>
    /// ⚠ <b>The two results are one line apart in Java and easy to swap</b>:
    /// <c>DescribeConfigsResult.all()</c> carries the whole map, while
    /// <c>AlterConfigsResult.all()</c> is <c>KafkaFuture&lt;Void&gt;</c>. Both shapes are
    /// pinned here, together, because that is where the confusion lives.
    /// </summary>
    [Fact]
    public void TheTwoResults_DifferExactlyAsJavasDo()
    {
        PropertyInfo describeValues = typeof(DescribeConfigsResult).GetProperty(
            nameof(DescribeConfigsResult.Values))!;
        Assert.Equal(typeof(IReadOnlyDictionary<ConfigResource, Task<Config>>), describeValues.PropertyType);
        Assert.Null(describeValues.SetMethod);

        MethodInfo describeAll = typeof(DescribeConfigsResult).GetMethod(
            nameof(DescribeConfigsResult.All), Type.EmptyTypes)!;
        Assert.Equal(typeof(Task<IReadOnlyDictionary<ConfigResource, Config>>), describeAll.ReturnType);

        PropertyInfo alterValues = typeof(AlterConfigsResult).GetProperty(nameof(AlterConfigsResult.Values))!;

        // ⚠ Task, not Task<bool>: the void bridge's success token must not leak out.
        Assert.Equal(typeof(IReadOnlyDictionary<ConfigResource, Task>), alterValues.PropertyType);
        Assert.Null(alterValues.SetMethod);

        MethodInfo alterAll = typeof(AlterConfigsResult).GetMethod(nameof(AlterConfigsResult.All), Type.EmptyTypes)!;

        // ⚠ A BARE Task — this is the line that differs from DescribeConfigsResult.
        Assert.Equal(typeof(Task), alterAll.ReturnType);

        // Java's constructors are package-private / protected; nothing public builds one.
        Assert.Empty(typeof(DescribeConfigsResult).GetConstructors());
        Assert.Empty(typeof(AlterConfigsResult).GetConstructors());
    }

    /// <summary>
    /// ⚠ <b>P1's five <see cref="ConfigEntry"/> members are unchanged</b>, and the four Java
    /// members Stage 2 adds have Java's types and nullability.
    /// </summary>
    /// <remarks>
    /// The boundary condition for reopening P1's foundation: this asserts the shipped five
    /// by name, type and nullability, so a widening or a rename during the extension turns
    /// it red. P1's own
    /// <c>PublicAdminShapeParityTests.ConfigEntry_PublishesOnlyTheConstructorJavaHas</c>
    /// still passes <b>unmodified</b> and is deliberately not duplicated here.
    /// </remarks>
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

        // Java's value() is nullable — "null is returned if the config is unset or if
        // isSensitive is true" (ConfigEntry.java:86).
        Assert.Equal(
            NullableAnnotation.Annotated,
            NullableAnnotation.Flag(Property(typeof(ConfigEntry), nameof(ConfigEntry.Value))));

        // The four new ones.
        Assert.Equal(
            typeof(ConfigEntry.ConfigSource), Property(typeof(ConfigEntry), nameof(ConfigEntry.Source)).PropertyType);
        Assert.Equal(
            typeof(ConfigEntry.ConfigType), Property(typeof(ConfigEntry), nameof(ConfigEntry.Type)).PropertyType);
        Assert.Equal(
            typeof(string), Property(typeof(ConfigEntry), nameof(ConfigEntry.Documentation)).PropertyType);
        Assert.Equal(
            typeof(IReadOnlyList<ConfigEntry.ConfigSynonym>),
            Property(typeof(ConfigEntry), nameof(ConfigEntry.Synonyms)).PropertyType);

        // documentation() is nullable — "or null when the broker did not report it".
        Assert.Equal(
            NullableAnnotation.Annotated,
            NullableAnnotation.Flag(Property(typeof(ConfigEntry), nameof(ConfigEntry.Documentation))));

        // synonyms() is a LIST, not a set — the order is Java's precedence order.
        Assert.Equal(
            NullableAnnotation.NotAnnotated,
            NullableAnnotation.Flag(Property(typeof(ConfigEntry), nameof(ConfigEntry.Synonyms))));

        // Every accessor is read-only, and there are exactly these nine.
        Assert.Equal(
            new[]
            {
                "Documentation", "IsDefault", "IsReadOnly", "IsSensitive", "Name", "Source", "Synonyms", "Type",
                "Value",
            },
            typeof(ConfigEntry).GetProperties().Select(p => p.Name).OrderBy(n => n, StringComparer.Ordinal));
        Assert.All(typeof(ConfigEntry).GetProperties(), property => Assert.Null(property.SetMethod));
    }

    /// <summary>
    /// <see cref="ConfigEntry.IsDefault"/> is <b>derived</b> from
    /// <see cref="ConfigEntry.Source"/>, exactly as Java derives it — not stored.
    /// </summary>
    /// <remarks>
    /// This is P1's own recorded plan carried out: it recorded that "when <c>Source</c>
    /// lands it becomes the source of truth and <c>IsDefault</c> derives from it, exactly as
    /// in Java". A stored flag could contradict the source; a derived one cannot.
    /// </remarks>
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

        // The public 2-argument constructor defaults the source to UNKNOWN (Java :45).
        ConfigEntry plain = new ConfigEntry("k", "v");
        Assert.Equal(ConfigEntry.ConfigSource.Unknown, plain.Source);
        Assert.Equal(ConfigEntry.ConfigType.Unknown, plain.Type);
        Assert.Empty(plain.Synonyms);
        Assert.Null(plain.Documentation);
        Assert.False(plain.IsDefault);
    }

    /// <summary>
    /// The two name-encoded enums stay <b>nested</b> inside <see cref="ConfigEntry"/>
    /// (decision D16) and carry Java's members in Java's declaration order.
    /// </summary>
    [Fact]
    public void TheTwoNameEncodedEnums_StayNested_WithJavasMembers()
    {
        Assert.Equal(typeof(ConfigEntry), typeof(ConfigEntry.ConfigType).DeclaringType);
        Assert.Equal(typeof(ConfigEntry), typeof(ConfigEntry.ConfigSource).DeclaringType);

        // ConfigEntry.java:199-210, in declaration order.
        Assert.Equal(
            new[] { "Unknown", "Boolean", "String", "Int", "Short", "Long", "Double", "List", "Class", "Password" },
            Enum.GetNames(typeof(ConfigEntry.ConfigType)));

        // ConfigEntry.java:215-225, in declaration order.
        Assert.Equal(
            new[]
            {
                "DynamicTopicConfig", "DynamicBrokerLoggerConfig", "DynamicBrokerConfig",
                "DynamicDefaultBrokerConfig", "DynamicClientMetricsConfig", "DynamicGroupConfig",
                "StaticBrokerConfig", "DefaultConfig", "Unknown",
            },
            Enum.GetNames(typeof(ConfigEntry.ConfigSource)));
    }

    /// <summary>
    /// <c>ConfigEntry.ConfigSynonym</c> stays nested with Java's three accessors and value
    /// equality, and no public constructor (Java's is package-private).
    /// </summary>
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

        // value() "may be null if the configuration is sensitive" (ConfigEntry.java:257).
        Assert.Equal(
            NullableAnnotation.Annotated,
            NullableAnnotation.Flag(
                Property(typeof(ConfigEntry.ConfigSynonym), nameof(ConfigEntry.ConfigSynonym.Value))));
    }

    /// <summary>
    /// <see cref="AlterConfigOp"/> mirrors Java's constructor, its two accessors — as
    /// <b>properties</b> per decision D18 — and its value equality.
    /// </summary>
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

        // Java compares the entry with Objects.equals, so equal-valued distinct entries are
        // equal — which only holds because ConfigEntry has value equality.
        Assert.Equal(new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOpType.Set), op);
        Assert.Equal(
            new AlterConfigOp(new ConfigEntry("k", "v"), AlterConfigOpType.Set).GetHashCode(), op.GetHashCode());
        Assert.NotEqual(new AlterConfigOp(entry, AlterConfigOpType.Delete), op);
        Assert.NotEqual(new AlterConfigOp(new ConfigEntry("k", "other"), AlterConfigOpType.Set), op);

        Assert.StartsWith("AlterConfigOp{opType=Set, configEntry=ConfigEntry(", op.ToString(), StringComparison.Ordinal);
    }

    /// <summary>
    /// <see cref="AlterConfigOpType"/> carries Java's <c>OpType.id()</c> wire codes, has
    /// exactly Java's four members, and is <b>flattened out of</b> <see cref="AlterConfigOp"/>.
    /// </summary>
    /// <remarks>
    /// ⚠ The flattening is forced, not stylistic: Java's accessor is <c>opType()</c> and D18
    /// makes it a property <c>OpType</c>, which cannot coexist with a nested type of the
    /// same name (<c>CS0102</c>). Contrast the two enums above, which stay nested because
    /// Java's accessors there are <c>source()</c> / <c>type()</c> — different names.
    /// </remarks>
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

    /// <inheritdoc cref="AlterConfigOpType_CarriesJavasWireIds"/>
    [Fact]
    public void AlterConfigOpType_IsFlattened_WithExactlyJavasFourMembers()
    {
        Assert.Null(typeof(AlterConfigOpType).DeclaringType);
        Assert.Equal("Confluent.Kafka.Admin", typeof(AlterConfigOpType).Namespace);
        Assert.Equal(
            new[] { "Append", "Delete", "Set", "Subtract" },
            Enum.GetNames(typeof(AlterConfigOpType)).OrderBy(name => name, StringComparer.Ordinal));

        // The forcing condition, asserted rather than asserted-in-prose: AlterConfigOp
        // really does have a member named OpType, so a nested type of that name is CS0102.
        Assert.NotNull(typeof(AlterConfigOp).GetProperty("OpType"));
        Assert.Null(typeof(AlterConfigOp).GetNestedType("OpType"));
    }

    /// <summary>
    /// The two new options types match Java's fields and defaults exactly.
    /// </summary>
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

    /// <summary>
    /// ⚠ <b>No <c>IncrementalAlterConfigsResult</c> type exists</b> — Java's return type is
    /// <see cref="AlterConfigsResult"/>, and inventing a name for the RPC instead would be a
    /// <c>definition-of-done.md</c> §7 violation.
    /// </summary>
    /// <remarks>
    /// The whole exported surface is swept, so the type cannot reappear elsewhere and
    /// satisfy a narrower check.
    /// </remarks>
    [Fact]
    public void NoIncrementalAlterConfigsResultType_Exists()
    {
        Assert.DoesNotContain(
            typeof(IAdmin).Assembly.GetExportedTypes(),
            type => type.Name.IndexOf("IncrementalAlterConfigsResult", StringComparison.Ordinal) >= 0);
    }

    /// <summary>
    /// Both new config types live under <c>Confluent.Kafka.Admin</c> (Java's
    /// <c>clients.admin</c> package), unlike <see cref="ConfigResource"/> and
    /// <see cref="ConfigResourceType"/>, which are <c>common.config</c> and sit at the root
    /// (decisions D13 / D16).
    /// </summary>
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
