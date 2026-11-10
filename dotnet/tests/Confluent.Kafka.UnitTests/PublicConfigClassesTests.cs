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
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Producer;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Checks the eight config classes against Java: every <c>String</c> constant the Java sources initialize with one
/// literal, the evaluated constants with Java's types, which fields are <c>const</c>, which classes construct, the
/// deprecated and untranslated names left out, and the typed defaults accepted on their own keys.
/// </summary>
public sealed class PublicConfigClassesTests
{
    private const BindingFlags PublicStatic = BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly;

    private static readonly Regex s_literal =
        new(@"public static final String (\w+)\s*=\s*""((?:[^""\\]|\\.)*)"";");

    public static TheoryData<Type, string> JavaClasses => new()
    {
        { typeof(ProducerConfig), "clients/producer/ProducerConfig" },
        { typeof(ConsumerConfig), "clients/consumer/ConsumerConfig" },
        { typeof(AdminClientConfig), "clients/admin/AdminClientConfig" },
        { typeof(TopicConfig), "common/config/TopicConfig" },
        { typeof(SaslConfigs), "common/config/SaslConfigs" },
        { typeof(SslConfigs), "common/config/SslConfigs" },
        { typeof(SecurityConfig), "common/config/SecurityConfig" },
        { typeof(LogLevelConfig), "common/config/LogLevelConfig" },
    };

    public static TheoryData<Type, int, string[]> ConstantsAndReadOnlyFields => new()
    {
        { typeof(ProducerConfig), 62, Array.Empty<string>() },
        {
            typeof(ConsumerConfig), 67, new[]
            {
                nameof(ConsumerConfig.ASSIGN_FROM_SUBSCRIBED_ASSIGNORS),
                nameof(ConsumerConfig.DEFAULT_GROUP_PROTOCOL),
                nameof(ConsumerConfig.DEFAULT_GROUP_REMOTE_ASSIGNOR),
                nameof(ConsumerConfig.DEFAULT_ISOLATION_LEVEL),
                nameof(ConsumerConfig.DEFAULT_SHARE_ACQUIRE_MODE),
            }
        },
        { typeof(AdminClientConfig), 38, new[] { nameof(AdminClientConfig.DEFAULT_METADATA_RECOVERY_STRATEGY) } },
        { typeof(TopicConfig), 72, Array.Empty<string>() },
        { typeof(SaslConfigs), 119, Array.Empty<string>() },
        {
            typeof(SslConfigs), 43, new[]
            {
                nameof(SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM),
                nameof(SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM),
                nameof(SslConfigs.RECONFIGURABLE_CONFIGS),
                nameof(SslConfigs.NON_RECONFIGURABLE_CONFIGS),
            }
        },
        { typeof(SecurityConfig), 2, Array.Empty<string>() },
        { typeof(LogLevelConfig), 6, new[] { nameof(LogLevelConfig.VALID_LOG_LEVELS) } },
    };

    [Theory]
    [MemberData(nameof(JavaClasses))]
    public void ConfigClasses_test_every_literal_constant_matches_java(Type type, string java)
    {
        string root = PublicNamespaceLayoutTests.KafkaSourceRoot();
        if (!Directory.Exists(Path.Combine(root, "main", "java")))
        {
            Assert.Fail($"The Kafka sources are not at {root}; run `git submodule update --init kafka`.");
        }

        string source = File.ReadAllText(Path.Combine(root, "main", "java", "org", "apache", "kafka", java + ".java"));
        string fqn = "org.apache.kafka." + java.Replace('/', '.');
        var deprecated = new HashSet<string>(
            File.ReadAllLines(Path.Combine(root, "..", "..", "..", "design", "current", "java-deprecated.txt"))
                .Where(line => line.StartsWith(fqn + "#", StringComparison.Ordinal))
                .Select(line => line.Split(new[] { '#' }, 2)[1]),
            StringComparer.Ordinal);

        MatchCollection found = s_literal.Matches(source);
        Assert.NotEmpty(found);
        foreach (Match match in found)
        {
            string name = match.Groups[1].Value;
            if (deprecated.Contains(name))
            {
                Assert.True(
                    type.GetMember(name, PublicStatic).Length == 0, $"{type.Name}.{name} is deprecated in Java.");
            }
            else
            {
                Assert.Equal((name, (object?)match.Groups[2].Value), (name, Value(type, name)));
            }
        }
    }

    [Fact]
    public void ConfigClasses_test_evaluated_constants()
    {
        AssertConstant(typeof(ProducerConfig), nameof(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), "bootstrap.servers");
        AssertConstant(typeof(ProducerConfig), nameof(ProducerConfig.CONFIG_PROVIDERS_CONFIG), "config.providers");
        AssertConstant(typeof(ConsumerConfig), nameof(ConsumerConfig.DEFAULT_FETCH_MAX_BYTES), 50 * 1024 * 1024);
        AssertReadOnlyField(typeof(ConsumerConfig), nameof(ConsumerConfig.DEFAULT_GROUP_PROTOCOL), "classic");
        AssertReadOnlyField(typeof(ConsumerConfig), nameof(ConsumerConfig.DEFAULT_ISOLATION_LEVEL), "read_uncommitted");
        AssertReadOnlyField<string?>(typeof(ConsumerConfig), nameof(ConsumerConfig.DEFAULT_GROUP_REMOTE_ASSIGNOR), null);
        AssertConstant(typeof(ConsumerConfig), nameof(ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS), true);
        AssertConstant(
            typeof(AdminClientConfig), nameof(AdminClientConfig.DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS),
            300_000L);
        AssertReadOnlyField(
            typeof(AdminClientConfig), nameof(AdminClientConfig.DEFAULT_METADATA_RECOVERY_STRATEGY), "rebootstrap");
        AssertConstant(typeof(ProducerConfig), nameof(ProducerConfig.SECURITY_PROVIDERS_CONFIG), "security.providers");
        AssertConstant(typeof(ConsumerConfig), nameof(ConsumerConfig.GROUP_ID_CONFIG), "group.id");
        AssertConstant(typeof(SaslConfigs), nameof(SaslConfigs.DEFAULT_SASL_MECHANISM), "GSSAPI");
        AssertConstant(typeof(SaslConfigs), nameof(SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR), 0.8);
        AssertConstant(typeof(SaslConfigs), nameof(SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS), (short)60);
        AssertConstant(typeof(SaslConfigs), nameof(SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN), 60_000L);
        AssertReadOnlyField(typeof(SslConfigs), nameof(SslConfigs.DEFAULT_SSL_KEYMANGER_ALGORITHM), "SunX509");
        AssertReadOnlyField(typeof(SslConfigs), nameof(SslConfigs.DEFAULT_SSL_TRUSTMANAGER_ALGORITHM), "PKIX");
    }

    [Fact]
    public void ConfigClasses_test_evaluated_constants_TheCollectionsAreReadOnly_InJavasOrder()
    {
        IReadOnlyList<string> assignors = AssertReadOnlyField<IReadOnlyList<string>>(
            typeof(ConsumerConfig), nameof(ConsumerConfig.ASSIGN_FROM_SUBSCRIBED_ASSIGNORS));
        Assert.Equal(new[] { "range", "roundrobin", "sticky", "cooperative-sticky" }, assignors);
        Assert.Equal("cooperative-sticky", assignors[3]);
        AssertReadOnly(assignors);

        IReadOnlyCollection<string> reconfigurable = AssertReadOnlyField<IReadOnlyCollection<string>>(
            typeof(SslConfigs), nameof(SslConfigs.RECONFIGURABLE_CONFIGS));
        Assert.Equal(
            new[]
            {
                "ssl.keystore.type", "ssl.keystore.location", "ssl.keystore.password", "ssl.key.password",
                "ssl.truststore.type", "ssl.truststore.location", "ssl.truststore.password",
                "ssl.keystore.certificate.chain", "ssl.keystore.key", "ssl.truststore.certificates",
            },
            reconfigurable);
        Assert.Contains("ssl.keystore.type", reconfigurable);
        AssertReadOnly(reconfigurable);

        IReadOnlyCollection<string> nonReconfigurable = AssertReadOnlyField<IReadOnlyCollection<string>>(
            typeof(SslConfigs), nameof(SslConfigs.NON_RECONFIGURABLE_CONFIGS));
        Assert.Equal(
            new[]
            {
                "ssl.client.auth", "ssl.protocol", "ssl.provider", "ssl.cipher.suites", "ssl.enabled.protocols",
                "ssl.keymanager.algorithm", "ssl.trustmanager.algorithm", "ssl.endpoint.identification.algorithm",
                "ssl.secure.random.implementation", "ssl.engine.factory.class",
            },
            nonReconfigurable);
        AssertReadOnly(nonReconfigurable);

        IReadOnlyCollection<string> logLevels = AssertReadOnlyField<IReadOnlyCollection<string>>(
            typeof(LogLevelConfig), nameof(LogLevelConfig.VALID_LOG_LEVELS));
        Assert.Equal(new[] { "FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE" }, logLevels);
        AssertReadOnly(logLevels);
    }

    [Theory]
    [MemberData(nameof(ConstantsAndReadOnlyFields))]
    public void ConfigClasses_test_evaluated_constants_JavasCompileTimeConstantsAreConst_TheOthersStaticReadOnlyFields(
        Type type, int constants, string[] readOnlyFields)
    {
        MemberInfo[] members = type.GetMembers(PublicStatic)
            .Where(member => member is not MethodInfo { IsSpecialName: true })
            .ToArray();

        Assert.All(
            members,
            member => Assert.True(
                member is FieldInfo { IsLiteral: true } or FieldInfo { IsInitOnly: true },
                $"{type.Name}.{member.Name} is a {member.MemberType} that is neither a const nor a static readonly field."));
        Assert.Equal(constants, members.OfType<FieldInfo>().Count(field => field.IsLiteral));
        Assert.Equal(
            readOnlyFields.OrderBy(name => name, StringComparer.Ordinal),
            members.OfType<FieldInfo>()
                .Where(field => field.IsInitOnly)
                .Select(field => field.Name)
                .OrderBy(name => name, StringComparer.Ordinal));
        Assert.Empty(type.GetProperties(
            BindingFlags.Public | BindingFlags.Static | BindingFlags.Instance | BindingFlags.DeclaredOnly));
    }

    [Fact]
    public void ConfigClasses_test_deprecated_constants_absent()
    {
        Assert.Empty(typeof(ProducerConfig).GetMember("PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG", PublicStatic));
        AssertConstant(
            typeof(ProducerConfig), nameof(ProducerConfig.PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG),
            "partitioner.adaptive.partitioning.enable");
        Assert.Empty(typeof(TopicConfig).GetMember("MESSAGE_DOWNCONVERSION_ENABLE_CONFIG", PublicStatic));
        Assert.Empty(typeof(TopicConfig).GetMember("MESSAGE_DOWNCONVERSION_ENABLE_DOC", PublicStatic));
    }

    [Fact]
    public void ConfigClasses_test_common_client_configs_is_not_translated() =>
        Assert.DoesNotContain(
            typeof(ProducerConfig).Assembly.GetExportedTypes(), type => type.Name == "CommonClientConfigs");

    [Fact]
    public void ConfigClasses_test_share_consumer_config_is_not_translated() =>
        Assert.DoesNotContain(
            typeof(ConsumerConfig).Assembly.GetExportedTypes(), type => type.Name == "ShareConsumerConfig");

    [Theory]
    [InlineData(typeof(ProducerConfig))]
    [InlineData(typeof(ConsumerConfig))]
    [InlineData(typeof(AdminClientConfig))]
    public void ConfigClasses_test_constants_only_classes_are_not_instantiable(Type type)
    {
        Assert.Empty(type.GetConstructors());
        ConstructorInfo constructor = Assert.Single(
            type.GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance));
        Assert.True(constructor.IsPrivate, $"{type.Name}'s constructor is not private.");
        Assert.Empty(constructor.GetParameters());
        Assert.False(type.IsAbstract, $"{type.Name} is abstract or static.");
        Assert.False(type.IsSealed, $"{type.Name} is sealed.");
    }

    [Theory]
    [InlineData(typeof(TopicConfig))]
    [InlineData(typeof(SaslConfigs))]
    [InlineData(typeof(SslConfigs))]
    [InlineData(typeof(SecurityConfig))]
    [InlineData(typeof(LogLevelConfig))]
    public void ConfigClasses_test_classes_with_java_default_constructor_construct(Type type) =>
        Assert.IsType(type, Activator.CreateInstance(type));

    [Theory]
    [InlineData(
        "producer", SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS,
        SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS, "60")]
    [InlineData(
        "consumer", SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS,
        SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS, "60")]
    [InlineData(
        "admin", SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS,
        SaslConfigs.DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS, "60")]
    [InlineData(
        "producer", SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS,
        SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS, "300")]
    [InlineData(
        "consumer", SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS,
        SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS, "300")]
    [InlineData(
        "admin", SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS,
        SaslConfigs.DEFAULT_LOGIN_REFRESH_BUFFER_SECONDS, "300")]
    [InlineData(
        "consumer", ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
        ConsumerConfig.DEFAULT_MAX_POLL_RECORDS, "500")]
    [InlineData(
        "producer", SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN,
        SaslConfigs.DEFAULT_KERBEROS_MIN_TIME_BEFORE_RELOGIN, "60000")]
    [InlineData(
        "admin", AdminClientConfig.METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS_CONFIG,
        AdminClientConfig.DEFAULT_METADATA_RECOVERY_REBOOTSTRAP_TRIGGER_MS, "300000")]
    [InlineData(
        "consumer", SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR,
        SaslConfigs.DEFAULT_LOGIN_REFRESH_WINDOW_FACTOR, "0.80000000000000004")]
    [InlineData(
        "consumer", ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG,
        ConsumerConfig.DEFAULT_EXCLUDE_INTERNAL_TOPICS, "true")]
    public void ConfigClasses_TheTypedDefaultsPassTheTypedCheckOnTheirKeys(
        string client, string key, object value, string text)
    {
        ClientKind kind = client switch
        {
            "producer" => ClientKind.Producer,
            "consumer" => ClientKind.Consumer,
            _ => ClientKind.Admin,
        };

        Assert.Equal(
            new[] { new KeyValuePair<string, string>(key, text) },
            ClientConfigs.Prepare(new Dictionary<string, object?> { [key] = value }, kind));
    }

    private static object? Value(Type type, string name)
    {
        MemberInfo[] members = type.GetMember(name, PublicStatic);
        Assert.True(members.Length == 1, $"{type.Name} has {members.Length} public static members named {name}.");
        return members[0] switch
        {
            FieldInfo field => field.GetValue(null),
            _ => throw new InvalidOperationException($"{type.Name}.{name} is a {members[0].MemberType}."),
        };
    }

    private static void AssertConstant<T>(Type type, string name, T expected)
    {
        FieldInfo? field = type.GetField(name, PublicStatic);
        Assert.True(field is { IsLiteral: true }, $"{type.Name}.{name} is not a const.");
        Assert.Equal(typeof(T), field!.FieldType);
        Assert.Equal(expected, (T)field.GetRawConstantValue()!);
    }

    private static void AssertReadOnlyField<T>(Type type, string name, T expected) =>
        Assert.Equal(expected, AssertReadOnlyField<T>(type, name));

    private static T AssertReadOnlyField<T>(Type type, string name)
    {
        FieldInfo? field = type.GetField(name, PublicStatic);
        Assert.True(
            field is { IsInitOnly: true, IsLiteral: false },
            $"{type.Name}.{name} is not a static readonly field.");
        Assert.Equal(typeof(T), field!.FieldType);
        return (T)field.GetValue(null)!;
    }

    private static void AssertReadOnly(IEnumerable<string> collection)
    {
        string first = collection.First();

        IList<string> typed = Assert.IsAssignableFrom<IList<string>>(collection);
        Assert.True(typed.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => typed[0] = "x");
        Assert.Throws<NotSupportedException>(() => typed.Add("x"));
        Assert.Throws<NotSupportedException>(() => typed.Insert(0, "x"));
        Assert.Throws<NotSupportedException>(() => typed.Remove(first));
        Assert.Throws<NotSupportedException>(() => typed.RemoveAt(0));
        Assert.Throws<NotSupportedException>(() => typed.Clear());

        IList untyped = Assert.IsAssignableFrom<IList>(collection);
        Assert.True(untyped.IsReadOnly);
        Assert.Throws<NotSupportedException>(() => untyped[0] = "x");
        Assert.Throws<NotSupportedException>(() => untyped.Add("x"));
        Assert.Throws<NotSupportedException>(() => untyped.Insert(0, "x"));
        Assert.Throws<NotSupportedException>(() => untyped.Remove(first));
        Assert.Throws<NotSupportedException>(() => untyped.RemoveAt(0));
        Assert.Throws<NotSupportedException>(() => untyped.Clear());

        Assert.Equal(first, collection.First());
        Assert.IsType<ReadOnlyCollection<string>>(collection);
    }
}
