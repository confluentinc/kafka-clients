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

using Confluent.Kafka.Admin;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Acl;
using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Common.Record;
using Confluent.Kafka.Common.Resource;
using Confluent.Kafka.Common.Security.Auth;
using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Checks where the members Java declares on an enum live: each <c>&lt;Enum&gt;Methods</c> class is a top-level
/// public static class beside its enum, the old class names are gone, Java's <c>toString()</c> override is a static
/// <c>ToString</c> of the enum, and Java's static <c>GroupState.groupStatesForType(GroupType)</c> answers Java's four
/// sets on <see cref="GroupState"/>, in both the C# 14 form and the form older C# calls. A <c>Code()</c> or
/// <c>Id()</c> keeps only the low byte of a value no member has.
/// </summary>
public sealed class PublicEnumMethodsTests
{
    public static TheoryData<GroupType, GroupState[]> JavasStatesForType => new TheoryData<GroupType, GroupState[]>
    {
        {
            GroupType.CLASSIC,
            new[]
            {
                GroupState.PREPARING_REBALANCE, GroupState.COMPLETING_REBALANCE, GroupState.STABLE, GroupState.DEAD,
                GroupState.EMPTY,
            }
        },
        {
            GroupType.CONSUMER,
            new[]
            {
                GroupState.PREPARING_REBALANCE, GroupState.COMPLETING_REBALANCE, GroupState.STABLE, GroupState.DEAD,
                GroupState.EMPTY, GroupState.ASSIGNING, GroupState.RECONCILING,
            }
        },
        {
            GroupType.STREAMS,
            new[]
            {
                GroupState.STABLE, GroupState.DEAD, GroupState.EMPTY, GroupState.ASSIGNING, GroupState.RECONCILING,
                GroupState.NOT_READY,
            }
        },
        { GroupType.SHARE, new[] { GroupState.STABLE, GroupState.DEAD, GroupState.EMPTY } },
    };

    [Theory]
    [InlineData(typeof(ScramMechanismMethods), typeof(ScramMechanism))]
    [InlineData(typeof(UpgradeTypeMethods), typeof(UpgradeType))]
    [InlineData(typeof(SecurityProtocolMethods), typeof(SecurityProtocol))]
    [InlineData(typeof(SslClientAuthMethods), typeof(SslClientAuth))]
    [InlineData(typeof(GroupProtocolMethods), typeof(GroupProtocol))]
    [InlineData(typeof(GroupStateMethods), typeof(GroupState))]
    [InlineData(typeof(AclOperationMethods), typeof(AclOperation))]
    [InlineData(typeof(AclPermissionTypeMethods), typeof(AclPermissionType))]
    [InlineData(typeof(ResourceTypeMethods), typeof(ResourceType))]
    [InlineData(typeof(PatternTypeMethods), typeof(PatternType))]
    [InlineData(typeof(ConfigResourceTypeMethods), typeof(ConfigResourceType))]
    [InlineData(typeof(OpTypeMethods), typeof(OpType))]
    [InlineData(typeof(IsolationLevelMethods), typeof(IsolationLevel))]
    [InlineData(typeof(ElectionTypeMethods), typeof(ElectionType))]
    [InlineData(typeof(GroupTypeMethods), typeof(GroupType))]
    [InlineData(typeof(ClassicGroupStateMethods), typeof(ClassicGroupState))]
    [InlineData(typeof(TransactionStateMethods), typeof(TransactionState))]
    [InlineData(typeof(TimestampTypeMethods), typeof(TimestampType))]
    public void TheMethodsClass_IsATopLevelPublicStaticClass_BesideItsEnum(Type methods, Type enumType)
    {
        Assert.True(enumType.IsEnum, $"{enumType} is not an enum.");
        Assert.Equal(enumType.Name + "Methods", methods.Name);
        Assert.Equal(enumType.Namespace, methods.Namespace);
        Assert.Null(methods.DeclaringType);
        Assert.True(methods.IsPublic, $"{methods} is not public.");
        Assert.True(
            methods is { IsClass: true, IsAbstract: true, IsSealed: true }, $"{methods} is not a static class.");
        Assert.Contains(methods, typeof(KafkaException).Assembly.GetExportedTypes());
    }

    [Theory]
    [InlineData(typeof(GroupStateMethods), typeof(GroupState))]
    [InlineData(typeof(GroupTypeMethods), typeof(GroupType))]
    [InlineData(typeof(ClassicGroupStateMethods), typeof(ClassicGroupState))]
    [InlineData(typeof(TransactionStateMethods), typeof(TransactionState))]
    [InlineData(typeof(TimestampTypeMethods), typeof(TimestampType))]
    [InlineData(typeof(IsolationLevelMethods), typeof(IsolationLevel))]
    [InlineData(typeof(SslClientAuthMethods), typeof(SslClientAuth))]
    public void JavasToString_IsAStaticToStringOfTheEnum_NeverAnExtensionOfIt(Type methods, Type enumType)
    {
        MethodInfo toString = Assert.Single(
            methods.GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly),
            method => method.Name == nameof(ToString));

        Assert.Equal(typeof(string), toString.ReturnType);
        Assert.Equal(enumType, Assert.Single(toString.GetParameters()).ParameterType);
        Assert.False(toString.IsDefined(typeof(ExtensionAttribute), false), $"{toString} is an extension method.");
    }

    [Theory]
    [InlineData("ScramMechanisms")]
    [InlineData("UpgradeTypes")]
    [InlineData("SecurityProtocols")]
    [InlineData("SslClientAuths")]
    [InlineData("GroupProtocols")]
    [InlineData("GroupStateExtensions")]
    [InlineData("AlterConfigOpType")]
    public void TheOldName_IsNoTypeOfTheAssembly(string name) =>
        Assert.DoesNotContain(typeof(KafkaException).Assembly.GetTypes(), type => type.Name == name);

    [Fact]
    public void ACodeOrIdOfAValueOutsideAByte_KeepsOnlyItsLowByte()
    {
        Assert.Equal((byte)3, ((AclOperation)259).Code());
        Assert.Equal((byte)3, ((AclPermissionType)259).Code());
        Assert.Equal((byte)3, ((ResourceType)259).Code());
        Assert.Equal((byte)3, ((PatternType)259).Code());
        Assert.Equal((byte)3, ((ConfigResourceType)259).Id());
        Assert.Equal((byte)3, ((OpType)259).Id());
        Assert.Equal((byte)3, ((IsolationLevel)259).Id());
    }

    [Fact]
    public void OpType_IsTheTopLevelEnumBesideAlterConfigOp()
    {
        Assert.True(typeof(OpType).IsEnum);
        Assert.True(typeof(OpType).IsPublic);
        Assert.Equal(typeof(AlterConfigOp).Namespace, typeof(OpType).Namespace);
        Assert.Equal(OpType.DELETE, new AlterConfigOp(new ConfigEntry("k", null), OpType.DELETE).OpType());
    }

    [Theory]
    [MemberData(nameof(JavasStatesForType))]
    public void GroupStatesForType_IsJavasSetForEachType(GroupType type, GroupState[] expected)
    {
        IReadOnlyCollection<GroupState> states = GroupState.GroupStatesForType(type);

        Assert.Equal(expected.Length, states.Count);
        Assert.Equal(expected.OrderBy(state => state), states.OrderBy(state => state));
        Assert.DoesNotContain(GroupState.UNKNOWN, states);
        Assert.Equal(
            expected.OrderBy(state => state), GroupStateMethods.GroupStatesForType(type).OrderBy(state => state));
    }

    [Fact]
    public void GroupStatesForType_GivesANewCollectionOnEachCall() =>
        Assert.NotSame(GroupState.GroupStatesForType(GroupType.SHARE), GroupState.GroupStatesForType(GroupType.SHARE));

    [Fact]
    public void GroupStatesForType_RefusesUnknownWithJavasMessage()
    {
        var e = Assert.Throws<IllegalArgumentException>(() => GroupState.GroupStatesForType(GroupType.UNKNOWN));
        Assert.Equal("Group type not known", e.Message);

        var older = Assert.Throws<IllegalArgumentException>(
            () => GroupStateMethods.GroupStatesForType(GroupType.UNKNOWN));
        Assert.Equal("Group type not known", older.Message);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(99)]
    public void GroupStatesForType_RefusesAnUndefinedTypeWithJavasMessage(int value)
    {
        var e = Assert.Throws<IllegalArgumentException>(() => GroupState.GroupStatesForType((GroupType)value));

        Assert.Equal("Group type not known", e.Message);
    }

    [Theory]
    [InlineData(0, UpgradeType.UNKNOWN)]
    [InlineData(1, UpgradeType.UPGRADE)]
    [InlineData(2, UpgradeType.SAFE_DOWNGRADE)]
    [InlineData(3, UpgradeType.UNSAFE_DOWNGRADE)]
    [InlineData(4, UpgradeType.UNKNOWN)]
    [InlineData(-1, UpgradeType.UNKNOWN)]
    public void UpgradeType_FromCode_IsCalledOnTheEnumOrOnItsMethodsClass(int code, UpgradeType expected)
    {
        Assert.Equal(expected, UpgradeType.FromCode(code));
        Assert.Equal(expected, UpgradeTypeMethods.FromCode(code));
    }
}
