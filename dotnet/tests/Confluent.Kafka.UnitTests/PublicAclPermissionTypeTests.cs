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

using Confluent.Kafka.Common.Acl;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Java's <c>AclPermissionTypeTest</c> on <see cref="AclPermissionTypeMethods"/>, under Java's names, with Java's
/// table; then Python's <c>test_none_dereference</c> row for <c>fromString(null)</c>, the texts Java's
/// <c>valueOf</c> refuses, the code an instance member reads from an undefined value, and the form older C# calls.
/// </summary>
public sealed class PublicAclPermissionTypeTests
{
    private static readonly (AclPermissionType Ty, int Code, string Name, bool Unknown)[] s_infos =
    {
        (AclPermissionType.UNKNOWN, 0, "unknown", true),
        (AclPermissionType.ANY, 1, "any", false),
        (AclPermissionType.DENY, 2, "deny", false),
        (AclPermissionType.ALLOW, 3, "allow", false),
    };

    [Fact]
    public void TestIsUnknown()
    {
        foreach (var info in s_infos)
        {
            Assert.True(
                info.Unknown == info.Ty.IsUnknown(), $"{info.Ty} was supposed to have unknown == {info.Unknown}");
        }
    }

    [Fact]
    public void TestCode()
    {
        Assert.Equal(Enum.GetValues(typeof(AclPermissionType)).Length, s_infos.Length);
        foreach (var info in s_infos)
        {
            byte code = info.Ty.Code();
            Assert.True(info.Code == code, $"{info.Ty} was supposed to have code == {info.Code}");
            Assert.True(
                info.Ty == AclPermissionType.FromCode((byte)info.Code),
                $"AclPermissionType.FromCode({info.Code}) was supposed to be {info.Ty}");
        }

        Assert.Equal(AclPermissionType.UNKNOWN, AclPermissionType.FromCode((byte)120));
    }

    [Fact]
    public void TestName()
    {
        foreach (var info in s_infos)
        {
            Assert.True(
                info.Ty == AclPermissionType.FromString(info.Name),
                $"AclPermissionType.FromString({info.Name}) was supposed to be {info.Ty}");
        }

        Assert.Equal(AclPermissionType.UNKNOWN, AclPermissionType.FromString("something"));
    }

    [Fact]
    public void TestExhaustive()
    {
        var values = (AclPermissionType[])Enum.GetValues(typeof(AclPermissionType));

        Assert.Equal(s_infos.Length, values.Length);
        for (int i = 0; i < s_infos.Length; i++)
        {
            Assert.Equal(s_infos[i].Ty, values[i]);
        }
    }

    [Fact]
    public void NoneDereference_test_a_dereferenced_none_is_a_type_error_FromStringOfNullNamesStr()
    {
        var e = Assert.Throws<ArgumentNullException>(() => AclPermissionType.FromString(null!));

        Assert.Equal("str", e.ParamName);
        Assert.Equal(new ArgumentNullException("str").Message, e.Message);
    }

    [Theory]
    [InlineData("Allow", AclPermissionType.ALLOW)]
    [InlineData("DENY", AclPermissionType.DENY)]
    public void FromString_IgnoresCase(string str, AclPermissionType expected) =>
        Assert.Equal(expected, AclPermissionType.FromString(str));

    [Theory]
    [InlineData("DENY, ALLOW")]
    [InlineData("2")]
    [InlineData(" deny")]
    [InlineData("deny ")]
    [InlineData("")]
    public void FromString_IsUnknownForWhatJavasValueOfRefuses(string str) =>
        Assert.Equal(AclPermissionType.UNKNOWN, AclPermissionType.FromString(str));

    [Theory]
    [InlineData(4)]
    [InlineData(128)]
    [InlineData(255)]
    public void FromCode_IsUnknownForACodeNoPermissionTypeHas(int code) =>
        Assert.Equal(AclPermissionType.UNKNOWN, AclPermissionType.FromCode((byte)code));

    [Theory]
    [InlineData(4, 4)]
    [InlineData(99, 99)]
    [InlineData(-1, 255)]
    public void Code_AndIsUnknown_ReadAnUndefinedValueAsItIs(int value, int code)
    {
        var permissionType = (AclPermissionType)value;

        Assert.Equal(code, permissionType.Code());
        Assert.False(permissionType.IsUnknown());
    }

    [Fact]
    public void TheMembers_AreCalledOnTheMethodsClassToo()
    {
        Assert.Equal(AclPermissionType.ALLOW, AclPermissionTypeMethods.FromString("allow"));
        Assert.Equal(AclPermissionType.DENY, AclPermissionTypeMethods.FromCode(2));
        Assert.Equal(3, AclPermissionTypeMethods.Code(AclPermissionType.ALLOW));
        Assert.True(AclPermissionTypeMethods.IsUnknown(AclPermissionType.UNKNOWN));
        Assert.False(AclPermissionTypeMethods.IsUnknown(AclPermissionType.ANY));

        var e = Assert.Throws<ArgumentNullException>(() => AclPermissionTypeMethods.FromString(null!));
        Assert.Equal("str", e.ParamName);
    }
}
