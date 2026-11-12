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
/// integer field, or its ordinal where Java has none. A constant a Kafka upgrade adds turns its row red. The values
/// are read by name, never by reflection order, which .NET does not promise.
/// </summary>
public sealed class PublicJavaEnumConstantsTests
{
    private static readonly Dictionary<string, (Type Enum, (string Name, long Value)[] Constants)> s_java =
        new Dictionary<string, (Type, (string, long)[])>(StringComparer.Ordinal)
        {
            ["org.apache.kafka.clients.admin.AlterConfigOp.OpType"] =
                (typeof(OpType), Ids(("SET", 0), ("DELETE", 1), ("APPEND", 2), ("SUBTRACT", 3))),
            ["org.apache.kafka.clients.admin.ConfigEntry.ConfigSource"] = (
                typeof(ConfigEntry.ConfigSource),
                Ordinals(
                    "DYNAMIC_TOPIC_CONFIG", "DYNAMIC_BROKER_LOGGER_CONFIG", "DYNAMIC_BROKER_CONFIG",
                    "DYNAMIC_DEFAULT_BROKER_CONFIG", "DYNAMIC_CLIENT_METRICS_CONFIG", "DYNAMIC_GROUP_CONFIG",
                    "STATIC_BROKER_CONFIG", "DEFAULT_CONFIG", "UNKNOWN")),
            ["org.apache.kafka.clients.admin.ConfigEntry.ConfigType"] = (
                typeof(ConfigEntry.ConfigType),
                Ordinals(
                    "UNKNOWN", "BOOLEAN", "STRING", "INT", "SHORT", "LONG", "DOUBLE", "LIST", "CLASS", "PASSWORD")),
            ["org.apache.kafka.clients.admin.FeatureUpdate.UpgradeType"] = (
                typeof(UpgradeType),
                Ids(("UNKNOWN", 0), ("UPGRADE", 1), ("SAFE_DOWNGRADE", 2), ("UNSAFE_DOWNGRADE", 3))),
            ["org.apache.kafka.clients.admin.ScramMechanism"] =
                (typeof(ScramMechanism), Ids(("UNKNOWN", 0), ("SCRAM_SHA_256", 1), ("SCRAM_SHA_512", 2))),
            ["org.apache.kafka.clients.admin.TransactionState"] = (
                typeof(TransactionState),
                Ordinals(
                    "ONGOING", "PREPARE_ABORT", "PREPARE_COMMIT", "COMPLETE_ABORT", "COMPLETE_COMMIT", "EMPTY",
                    "PREPARE_EPOCH_FENCE", "UNKNOWN")),
            ["org.apache.kafka.clients.consumer.CloseOptions.GroupMembershipOperation"] =
                (typeof(GroupMembershipOperation), Ordinals("LEAVE_GROUP", "REMAIN_IN_GROUP", "DEFAULT")),
            ["org.apache.kafka.clients.consumer.GroupProtocol"] =
                (typeof(GroupProtocol), Ordinals("CLASSIC", "CONSUMER")),
            ["org.apache.kafka.common.ClassicGroupState"] = (
                typeof(ClassicGroupState),
                Ordinals("UNKNOWN", "PREPARING_REBALANCE", "COMPLETING_REBALANCE", "STABLE", "DEAD", "EMPTY")),
            ["org.apache.kafka.common.ElectionType"] =
                (typeof(ElectionType), Ids(("PREFERRED", 0), ("UNCLEAN", 1))),
            ["org.apache.kafka.common.GroupState"] = (
                typeof(GroupState),
                Ordinals(
                    "UNKNOWN", "PREPARING_REBALANCE", "COMPLETING_REBALANCE", "STABLE", "DEAD", "EMPTY", "ASSIGNING",
                    "RECONCILING", "NOT_READY")),
            ["org.apache.kafka.common.GroupType"] =
                (typeof(GroupType), Ordinals("UNKNOWN", "CONSUMER", "CLASSIC", "SHARE", "STREAMS")),
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
                (typeof(SslClientAuth), Ordinals("REQUIRED", "REQUESTED", "NONE")),
            ["org.apache.kafka.common.errors.RecordDeserializationException.DeserializationExceptionOrigin"] =
                (typeof(RecordDeserializationException.DeserializationExceptionOrigin), Ordinals("KEY", "VALUE")),
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

    [Fact]
    public void EveryPublicEnum_StandsForAJavaEnumPinnedHere_ButTheDotNetOnlyClientQuotaMatchType()
    {
        IEnumerable<string> pinned = s_java.Values.Select(row => row.Enum.FullName!);
        IEnumerable<string> exported = typeof(AclOperation).Assembly.GetExportedTypes()
            .Where(type => type.IsEnum && type != typeof(ClientQuotaMatchType))
            .Select(type => type.FullName!);

        Assert.Equal(22, s_java.Count);
        Assert.Equal(
            pinned.OrderBy(name => name, StringComparer.Ordinal),
            exported.OrderBy(name => name, StringComparer.Ordinal));
    }

    [Fact]
    public void GetValues_KeepsJavasOrder_ButForTheTwoEnumsWhoseValuesAreNotOrdinals()
    {
        foreach ((Type type, (string Name, long Value)[] constants) in s_java.Values)
        {
            string[] byValue =
                Enum.GetValues(type).Cast<object>().Select(member => Enum.GetName(type, member)!).ToArray();
            string[] javaOrder = constants.Select(constant => constant.Name).ToArray();

            if (type == typeof(ConfigResourceType) || type == typeof(TimestampType))
            {
                Assert.NotEqual(javaOrder, byValue);
            }
            else
            {
                Assert.Equal(javaOrder, byValue);
            }
        }

        Assert.Equal(
            new[] { TimestampType.CREATE_TIME, TimestampType.LOG_APPEND_TIME, TimestampType.NO_TIMESTAMP_TYPE },
            (TimestampType[])Enum.GetValues(typeof(TimestampType)));
        Assert.True(TimestampType.NO_TIMESTAMP_TYPE.CompareTo(TimestampType.CREATE_TIME) < 0);
    }

    private static (string, long)[] Ids(params (string Name, long Value)[] constants) => constants;

    private static (string, long)[] Ordinals(params string[] names) =>
        names.Select((name, ordinal) => (name, (long)ordinal)).ToArray();
}
