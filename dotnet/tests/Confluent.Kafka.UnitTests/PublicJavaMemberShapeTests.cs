// Copyright 2026 Confluent Inc.
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

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Record;
using Confluent.Kafka.Common.Resource;
using Confluent.Kafka.Common.Security.Auth;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Pins the shapes Java's members take in the public API: a Java method is a C# method, getters included, and a
/// Java <c>public static final</c> constant is a <c>static readonly</c> field. The only public properties left are
/// the ones .NET requires.
/// </summary>
/// <remarks>
/// A new Java member needs no change here: translate a Java method, a getter included, as a C# method with Java's
/// name. A new Java <c>public static final</c> constant is added to <c>s_javaConstants</c>, under the Java name, and
/// a property .NET requires is added to <c>s_requiredProperties</c>, with its reason.
/// </remarks>
public class PublicJavaMemberShapeTests
{
    /// <summary>
    /// The public properties .NET requires, each with its reason. None is left: a member a .NET interface requires
    /// as a property is implemented explicitly, beside Java's method (<c>ConsumerRecords.Count()</c> and
    /// <see cref="IReadOnlyCollection{T}.Count"/>). Add a name only for a property no explicit implementation can
    /// give, and give the reason here.
    /// </summary>
    private static readonly HashSet<string> s_requiredProperties = new HashSet<string>(StringComparer.Ordinal);

    [Fact]
    public void NoPublicType_DeclaresAPublicProperty_ExceptTheOnesDotNetRequires()
    {
        List<string> properties = typeof(Serdes).Assembly.GetExportedTypes()
            .SelectMany(type => type
                .GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance
                    | BindingFlags.Static | BindingFlags.DeclaredOnly)
                .Where(property => property.GetAccessors(nonPublic: true)
                    .Any(accessor => accessor.IsPublic || accessor.IsFamily || accessor.IsFamilyOrAssembly))
                .Select(property => $"{type.FullName}.{property.Name}"))
            .Where(name => !s_requiredProperties.Contains(name))
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToList();

        Assert.Empty(properties);
    }

    [Fact]
    public void ConsumerRecordsCount_IsJavasMethod_AndImplementsTheCollectionCountExplicitly()
    {
        MethodInfo count = typeof(ConsumerRecords<,>).GetMethod(
            "Count", BindingFlags.Public | BindingFlags.Instance, null, Type.EmptyTypes, null)!;
        Assert.NotNull(count);
        Assert.Equal(typeof(int), count.ReturnType);
        Assert.Null(typeof(ConsumerRecords<,>).GetProperty("Count", BindingFlags.Public | BindingFlags.Instance));

        ConsumerRecords<byte[], byte[]> records = new ConsumerRecords<byte[], byte[]>(
            new[]
            {
                Record(0),
                Record(1),
            });
        Assert.Equal(2, records.Count());
        Assert.Equal(2, ((IReadOnlyCollection<ConsumerRecord<byte[], byte[]>>)records).Count);

        static ConsumerRecord<byte[], byte[]> Record(long offset) => new ConsumerRecord<byte[], byte[]>(
            "t", 0, offset, 0, TimestampType.CREATE_TIME, new byte[] { 1 }, new byte[] { 2 }, new Headers(),
            leaderEpoch: null, serializedKeySize: 1, serializedValueSize: 1);
    }

    [Fact]
    public void Serdes_AreJavasStaticMethods_EachOverTheSameStatelessInstance()
    {
        Assert.Empty(typeof(Serdes).GetProperties(BindingFlags.Public | BindingFlags.Static));

        foreach ((string name, Type type) in new[]
        {
            ("String", typeof(ISerde<string>)),
            ("ByteArray", typeof(ISerde<byte[]>)),
            ("Integer", typeof(ISerde<int>)),
            ("Long", typeof(ISerde<long>)),
            ("Double", typeof(ISerde<double>)),
            ("UUID", typeof(ISerde<Guid>)),
            ("Void", typeof(ISerde<object?>)),
        })
        {
            MethodInfo method = typeof(Serdes).GetMethod(name, BindingFlags.Public | BindingFlags.Static, null, Type.EmptyTypes, null)!;
            Assert.NotNull(method);
            Assert.Equal(type, method.ReturnType);
            Assert.Same(method.Invoke(null, null), method.Invoke(null, null));
        }

        Assert.Equal(
            new[] { "ByteArray", "Double", "Integer", "Long", "String", "UUID", "Void" },
            typeof(Serdes).GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
                .Select(method => method.Name)
                .OrderBy(name => name, StringComparer.Ordinal));
    }

    [Fact]
    public void JavasConstants_AreStaticReadonlyFields()
    {
        foreach ((Type owner, string name, Type type) in new[]
        {
            (typeof(Uuid), nameof(Uuid.ZERO_UUID), typeof(Uuid)),
            (typeof(Uuid), nameof(Uuid.ONE_UUID), typeof(Uuid)),
            (typeof(Uuid), nameof(Uuid.METADATA_TOPIC_ID), typeof(Uuid)),
            (typeof(Uuid), nameof(Uuid.RESERVED), typeof(IReadOnlyCollection<Uuid>)),
            (typeof(KafkaPrincipal), nameof(KafkaPrincipal.ANONYMOUS), typeof(KafkaPrincipal)),
            (typeof(AccessControlEntryFilter), nameof(AccessControlEntryFilter.ANY), typeof(AccessControlEntryFilter)),
            (typeof(AclBindingFilter), nameof(AclBindingFilter.ANY), typeof(AclBindingFilter)),
            (typeof(ResourcePatternFilter), nameof(ResourcePatternFilter.ANY), typeof(ResourcePatternFilter)),
            (typeof(MockAdminClient), nameof(MockAdminClient.DEFAULT_LOG_DIRS), typeof(IReadOnlyList<string>)),
            (typeof(AsyncMockAdminClient), nameof(AsyncMockAdminClient.DEFAULT_LOG_DIRS), typeof(IReadOnlyList<string>)),
            (typeof(DisconnectException), nameof(DisconnectException.INSTANCE), typeof(DisconnectException)),
            (typeof(CoordinatorNotAvailableException), nameof(CoordinatorNotAvailableException.INSTANCE),
                typeof(CoordinatorNotAvailableException)),
        })
        {
            FieldInfo field = owner.GetField(name, BindingFlags.Public | BindingFlags.Static)!;
            Assert.NotNull(field);
            Assert.Equal(type, field.FieldType);
            Assert.True(field.IsInitOnly, $"{owner.Name}.{name} is not readonly");
        }
    }

    [Fact]
    public void JavasConstants_AreOneSharedInstanceEach()
    {
        Assert.Same(KafkaPrincipal.ANONYMOUS, KafkaPrincipal.ANONYMOUS);
        Assert.Equal(KafkaPrincipal.USER_TYPE, KafkaPrincipal.ANONYMOUS.GetPrincipalType());
        Assert.Equal("ANONYMOUS", KafkaPrincipal.ANONYMOUS.GetName());
        Assert.Same(AccessControlEntryFilter.ANY, AccessControlEntryFilter.ANY);
        Assert.Same(ResourcePatternFilter.ANY, ResourcePatternFilter.ANY);
        Assert.Same(AclBindingFilter.ANY, AclBindingFilter.ANY);
        Assert.Same(ResourcePatternFilter.ANY, AclBindingFilter.ANY.PatternFilter());
        Assert.Same(AccessControlEntryFilter.ANY, AclBindingFilter.ANY.EntryFilter());
        Assert.Same(Uuid.RESERVED, Uuid.RESERVED);
        Assert.Same(MockAdminClient.DEFAULT_LOG_DIRS, MockAdminClient.DEFAULT_LOG_DIRS);
        Assert.Same(MockAdminClient.DEFAULT_LOG_DIRS, AsyncMockAdminClient.DEFAULT_LOG_DIRS);
        Assert.Same(DisconnectException.INSTANCE, DisconnectException.INSTANCE);
        Assert.Same(CoordinatorNotAvailableException.INSTANCE, CoordinatorNotAvailableException.INSTANCE);
    }

    /// <summary>
    /// Every public static field and const outside an enum and the generated config classes, each the
    /// <c>public static final</c> field of the same name on the Java class it translates (checked against Kafka 4.4),
    /// so a new one cannot take a .NET name. To add one, check the Java class declares it <c>public static final</c>,
    /// declare it with Java's spelling, as a <c>const</c> for a primitive or a string that Java holds as a
    /// compile-time constant and a <c>static readonly</c> field otherwise, and add its full name here in ordinal
    /// order.
    /// </summary>
    private static readonly string[] s_javaConstants =
    {
        "Confluent.Kafka.Admin.AsyncMockAdminClient.DEFAULT_CLUSTER_ID",
        "Confluent.Kafka.Admin.AsyncMockAdminClient.DEFAULT_LOG_DIRS",
        "Confluent.Kafka.Admin.MockAdminClient.DEFAULT_CLUSTER_ID",
        "Confluent.Kafka.Admin.MockAdminClient.DEFAULT_LOG_DIRS",
        "Confluent.Kafka.Common.Acl.AccessControlEntryFilter.ANY",
        "Confluent.Kafka.Common.Acl.AclBindingFilter.ANY",
        "Confluent.Kafka.Common.Config.SslClientAuths.VALUES",
        "Confluent.Kafka.Common.Errors.CoordinatorNotAvailableException.INSTANCE",
        "Confluent.Kafka.Common.Errors.DisconnectException.INSTANCE",
        "Confluent.Kafka.Common.Quota.ClientQuotaEntity.CLIENT_ID",
        "Confluent.Kafka.Common.Quota.ClientQuotaEntity.IP",
        "Confluent.Kafka.Common.Quota.ClientQuotaEntity.USER",
        "Confluent.Kafka.Common.Resource.ResourcePattern.WILDCARD_RESOURCE",
        "Confluent.Kafka.Common.Resource.ResourcePatternFilter.ANY",
        "Confluent.Kafka.Common.Security.Auth.KafkaPrincipal.ANONYMOUS",
        "Confluent.Kafka.Common.Security.Auth.KafkaPrincipal.USER_TYPE",
        "Confluent.Kafka.Common.Uuid.METADATA_TOPIC_ID",
        "Confluent.Kafka.Common.Uuid.ONE_UUID",
        "Confluent.Kafka.Common.Uuid.RESERVED",
        "Confluent.Kafka.Common.Uuid.ZERO_UUID",
        "Confluent.Kafka.Consumer.ConsumerRecord`2.NO_TIMESTAMP",
        "Confluent.Kafka.Consumer.ConsumerRecord`2.NULL_SIZE",
        "Confluent.Kafka.Producer.AsyncKafkaProducer`2.NETWORK_THREAD_PREFIX",
        "Confluent.Kafka.Producer.AsyncKafkaProducer`2.PRODUCER_METRIC_GROUP_NAME",
        "Confluent.Kafka.Producer.KafkaProducer`2.NETWORK_THREAD_PREFIX",
        "Confluent.Kafka.Producer.KafkaProducer`2.PRODUCER_METRIC_GROUP_NAME",
        "Confluent.Kafka.Producer.RecordMetadata.UNKNOWN_PARTITION",
    };

    // The generated config classes are checked against kafka/ by PublicConfigClassesTests, check-generated and
    // CKRDA0013.
    private static readonly HashSet<Type> s_generatedConfigClasses = new HashSet<Type>
    {
        typeof(ProducerConfig),
        typeof(ConsumerConfig),
        typeof(AdminClientConfig),
        typeof(TopicConfig),
        typeof(SaslConfigs),
        typeof(SslConfigs),
        typeof(SecurityConfig),
        typeof(LogLevelConfig),
    };

    [Fact]
    public void ThePublicStaticFields_AreExactlyJavasConstants_AllReadonly()
    {
        List<FieldInfo> fields = typeof(Serdes).Assembly.GetExportedTypes()
            .Where(type => !type.IsEnum && !s_generatedConfigClasses.Contains(type))
            .SelectMany(type => type.GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly))
            .ToList();

        Assert.Equal(
            s_javaConstants,
            fields.Select(field => $"{field.DeclaringType!.FullName}.{field.Name}").OrderBy(name => name, StringComparer.Ordinal));
        Assert.All(fields, field => Assert.True(field.IsLiteral || field.IsInitOnly, field.Name));
    }
}
