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
using System.Globalization;
using System.Linq;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Quota;
using Confluent.Kafka.Common.Record;
using Confluent.Kafka.Common.Resource;
using Confluent.Kafka.Common.Security.Auth;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Pins every public enum that stands for a Java enum to Java's constants, listed here in Java's declaration order
/// (Java's <c>testExhaustive</c>): the enum has Java's names and Java's count, and each constant's value is Java's
/// integer field, or, where Java has none, the number it was released with. A constant a Kafka upgrade adds turns its
/// row red. The .NET-only <see cref="ClientQuotaMatchType"/> is pinned to its released numbers too, so every public
/// enum is pinned here. The values are read by name, never by reflection order, which .NET does not promise.
/// </summary>
/// <remarks>
/// C# compiles an enum member into its callers as its number, so a released member keeps its number: a constant Java
/// adds takes Java's integer field, or the next unused number, wherever Java declares it.
/// </remarks>
public sealed class PublicJavaEnumConstantsTests
{
    private static readonly Dictionary<string, (Type Enum, (string Name, long Value)[] Constants)> s_java =
        new Dictionary<string, (Type, (string, long)[])>(StringComparer.Ordinal)
        {
            ["org.apache.kafka.clients.admin.AlterConfigOp.OpType"] =
                (typeof(OpType), Ids(("SET", 0), ("DELETE", 1), ("APPEND", 2), ("SUBTRACT", 3))),
            ["org.apache.kafka.clients.admin.ConfigEntry.ConfigSource"] = (
                typeof(ConfigEntry.ConfigSource),
                Ids(
                    ("DYNAMIC_TOPIC_CONFIG", 0), ("DYNAMIC_BROKER_LOGGER_CONFIG", 1), ("DYNAMIC_BROKER_CONFIG", 2),
                    ("DYNAMIC_DEFAULT_BROKER_CONFIG", 3), ("DYNAMIC_CLIENT_METRICS_CONFIG", 4),
                    ("DYNAMIC_GROUP_CONFIG", 5), ("STATIC_BROKER_CONFIG", 6), ("DEFAULT_CONFIG", 7), ("UNKNOWN", 8))),
            ["org.apache.kafka.clients.admin.ConfigEntry.ConfigType"] = (
                typeof(ConfigEntry.ConfigType),
                Ids(
                    ("UNKNOWN", 0), ("BOOLEAN", 1), ("STRING", 2), ("INT", 3), ("SHORT", 4), ("LONG", 5), ("DOUBLE", 6),
                    ("LIST", 7), ("CLASS", 8), ("PASSWORD", 9))),
            ["org.apache.kafka.clients.admin.FeatureUpdate.UpgradeType"] = (
                typeof(UpgradeType),
                Ids(("UNKNOWN", 0), ("UPGRADE", 1), ("SAFE_DOWNGRADE", 2), ("UNSAFE_DOWNGRADE", 3))),
            ["org.apache.kafka.clients.admin.ScramMechanism"] =
                (typeof(ScramMechanism), Ids(("UNKNOWN", 0), ("SCRAM_SHA_256", 1), ("SCRAM_SHA_512", 2))),
            ["org.apache.kafka.clients.admin.TransactionState"] = (
                typeof(TransactionState),
                Ids(
                    ("ONGOING", 0), ("PREPARE_ABORT", 1), ("PREPARE_COMMIT", 2), ("COMPLETE_ABORT", 3),
                    ("COMPLETE_COMMIT", 4), ("EMPTY", 5), ("PREPARE_EPOCH_FENCE", 6), ("UNKNOWN", 7))),
            ["org.apache.kafka.clients.consumer.CloseOptions.GroupMembershipOperation"] = (
                typeof(GroupMembershipOperation),
                Ids(("LEAVE_GROUP", 0), ("REMAIN_IN_GROUP", 1), ("DEFAULT", 2))),
            ["org.apache.kafka.clients.consumer.GroupProtocol"] =
                (typeof(GroupProtocol), Ids(("CLASSIC", 0), ("CONSUMER", 1))),
            ["org.apache.kafka.common.ClassicGroupState"] = (
                typeof(ClassicGroupState),
                Ids(
                    ("UNKNOWN", 0), ("PREPARING_REBALANCE", 1), ("COMPLETING_REBALANCE", 2), ("STABLE", 3), ("DEAD", 4),
                    ("EMPTY", 5))),
            ["org.apache.kafka.common.ElectionType"] =
                (typeof(ElectionType), Ids(("PREFERRED", 0), ("UNCLEAN", 1))),
            ["org.apache.kafka.common.GroupState"] = (
                typeof(GroupState),
                Ids(
                    ("UNKNOWN", 0), ("PREPARING_REBALANCE", 1), ("COMPLETING_REBALANCE", 2), ("STABLE", 3), ("DEAD", 4),
                    ("EMPTY", 5), ("ASSIGNING", 6), ("RECONCILING", 7), ("NOT_READY", 8))),
            ["org.apache.kafka.common.GroupType"] = (
                typeof(GroupType),
                Ids(("UNKNOWN", 0), ("CONSUMER", 1), ("CLASSIC", 2), ("SHARE", 3), ("STREAMS", 4))),
            ["org.apache.kafka.common.IsolationLevel"] =
                (typeof(IsolationLevel), Ids(("READ_UNCOMMITTED", 0), ("READ_COMMITTED", 1))),
            ["org.apache.kafka.common.acl.AclOperation"] = (
                typeof(AclOperation),
                Ids(
                    ("UNKNOWN", 0), ("ANY", 1), ("ALL", 2), ("READ", 3), ("WRITE", 4), ("CREATE", 5), ("DELETE", 6),
                    ("ALTER", 7), ("DESCRIBE", 8), ("CLUSTER_ACTION", 9), ("DESCRIBE_CONFIGS", 10),
                    ("ALTER_CONFIGS", 11), ("IDEMPOTENT_WRITE", 12), ("CREATE_TOKENS", 13), ("DESCRIBE_TOKENS", 14),
                    ("TWO_PHASE_COMMIT", 15))),
            ["org.apache.kafka.common.acl.AclPermissionType"] =
                (typeof(AclPermissionType), Ids(("UNKNOWN", 0), ("ANY", 1), ("DENY", 2), ("ALLOW", 3))),
            ["org.apache.kafka.common.config.ConfigResource.Type"] = (
                typeof(ConfigResourceType),
                Ids(
                    ("GROUP", 32), ("CLIENT_METRICS", 16), ("BROKER_LOGGER", 8), ("BROKER", 4), ("TOPIC", 2),
                    ("UNKNOWN", 0))),
            ["org.apache.kafka.common.config.SslClientAuth"] =
                (typeof(SslClientAuth), Ids(("REQUIRED", 0), ("REQUESTED", 1), ("NONE", 2))),
            ["org.apache.kafka.common.errors.RecordDeserializationException.DeserializationExceptionOrigin"] = (
                typeof(RecordDeserializationException.DeserializationExceptionOrigin),
                Ids(("KEY", 0), ("VALUE", 1))),
            ["org.apache.kafka.common.record.TimestampType"] = (
                typeof(TimestampType),
                Ids(("NO_TIMESTAMP_TYPE", -1), ("CREATE_TIME", 0), ("LOG_APPEND_TIME", 1))),
            ["org.apache.kafka.common.resource.PatternType"] = (
                typeof(PatternType),
                Ids(("UNKNOWN", 0), ("ANY", 1), ("MATCH", 2), ("LITERAL", 3), ("PREFIXED", 4))),
            ["org.apache.kafka.common.resource.ResourceType"] = (
                typeof(ResourceType),
                Ids(
                    ("UNKNOWN", 0), ("ANY", 1), ("TOPIC", 2), ("GROUP", 3), ("CLUSTER", 4), ("TRANSACTIONAL_ID", 5),
                    ("DELEGATION_TOKEN", 6), ("USER", 7))),
            ["org.apache.kafka.common.security.auth.SecurityProtocol"] = (
                typeof(SecurityProtocol),
                Ids(("PLAINTEXT", 0), ("SSL", 1), ("SASL_PLAINTEXT", 2), ("SASL_SSL", 3))),
        };

    private static readonly Dictionary<Type, (string Name, long Value)[]> s_dotNetOnly =
        new Dictionary<Type, (string, long)[]>
        {
            [typeof(ClientQuotaMatchType)] = Ids(("Exact", 0), ("Default", 1), ("Specified", 2)),
        };

    public static TheoryData<string> JavaEnums
    {
        get
        {
            var data = new TheoryData<string>();
            foreach (string javaEnum in s_java.Keys)
            {
                data.Add(javaEnum);
            }

            return data;
        }
    }

    [Theory]
    [MemberData(nameof(JavaEnums))]
    public void TheEnum_HasJavasConstants_WithJavasValues(string javaEnum)
    {
        (Type type, (string Name, long Value)[] constants) = s_java[javaEnum];

        AssertPinned(type, constants);
    }

    [Fact]
    public void TheDotNetOnlyEnum_KeepsItsReleasedValues()
    {
        foreach (KeyValuePair<Type, (string Name, long Value)[]> row in s_dotNetOnly)
        {
            AssertPinned(row.Key, row.Value);
        }
    }

    [Fact]
    public void EveryPublicEnum_IsPinnedHere()
    {
        IEnumerable<string> pinned =
            s_java.Values.Select(row => row.Enum).Concat(s_dotNetOnly.Keys).Select(type => type.FullName!);
        IEnumerable<string> exported = typeof(AclOperation).Assembly.GetExportedTypes()
            .Where(type => type.IsEnum)
            .Select(type => type.FullName!);

        Assert.Equal(22, s_java.Count);
        Assert.Single(s_dotNetOnly);
        Assert.Equal(
            pinned.OrderBy(name => name, StringComparer.Ordinal),
            exported.OrderBy(name => name, StringComparer.Ordinal));
    }

    [Fact]
    public void GetValues_OrdersByValue_NotByJavasOrder()
    {
        foreach ((Type type, (string Name, long Value)[] constants) in s_java.Values)
        {
            string[] byValue =
                Enum.GetValues(type).Cast<object>().Select(member => Enum.GetName(type, member)!).ToArray();

            // Enum.GetValues reads the value as unsigned, so a negative one sorts last.
            Assert.Equal(
                constants.OrderBy(constant => unchecked((ulong)constant.Value)).Select(constant => constant.Name),
                byValue);
        }

        Assert.Equal(
            new[] { TimestampType.CREATE_TIME, TimestampType.LOG_APPEND_TIME, TimestampType.NO_TIMESTAMP_TYPE },
            (TimestampType[])Enum.GetValues(typeof(TimestampType)));
        Assert.True(TimestampType.NO_TIMESTAMP_TYPE.CompareTo(TimestampType.CREATE_TIME) < 0);
    }

    private static (string, long)[] Ids(params (string Name, long Value)[] constants) => constants;

    private static void AssertPinned(Type type, (string Name, long Value)[] constants)
    {
        string[] names = Enum.GetNames(type);

        Assert.Equal(constants.Length, names.Length);
        Assert.Equal(
            constants.Select(constant => constant.Name).OrderBy(name => name, StringComparer.Ordinal),
            names.OrderBy(name => name, StringComparer.Ordinal));
        foreach ((string name, long value) in constants)
        {
            object member = type.GetField(name)!.GetValue(null)!;

            Assert.Equal(value, Convert.ToInt64(member, CultureInfo.InvariantCulture));
            Assert.Equal(name, Enum.GetName(type, member));
        }
    }
}
