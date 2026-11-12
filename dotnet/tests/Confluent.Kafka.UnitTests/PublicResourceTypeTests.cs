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
using System.Globalization;

using Confluent.Kafka.Common.Resource;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Java's <c>ResourceTypeTest</c> on <see cref="ResourceTypeMethods"/>, under Java's names, with Java's table; then
/// Python's <c>test_none_dereference</c> row for <c>fromString(null)</c>, Java's case folding and the texts Java's
/// <c>valueOf</c> refuses, the code an instance member reads from an undefined value, and the form older C# calls.
/// </summary>
public sealed class PublicResourceTypeTests
{
    private static readonly (ResourceType ResourceType, int Code, string Name, bool Unknown)[] s_infos =
    {
        (ResourceType.UNKNOWN, 0, "unknown", true),
        (ResourceType.ANY, 1, "any", false),
        (ResourceType.TOPIC, 2, "topic", false),
        (ResourceType.GROUP, 3, "group", false),
        (ResourceType.CLUSTER, 4, "cluster", false),
        (ResourceType.TRANSACTIONAL_ID, 5, "transactional_id", false),
        (ResourceType.DELEGATION_TOKEN, 6, "delegation_token", false),
        (ResourceType.USER, 7, "user", false),
    };

    [Fact]
    public void TestIsUnknown()
    {
        foreach (var info in s_infos)
        {
            Assert.True(
                info.Unknown == info.ResourceType.IsUnknown(),
                $"{info.ResourceType} was supposed to have unknown == {info.Unknown}");
        }
    }

    [Fact]
    public void TestCode()
    {
        Assert.Equal(Enum.GetValues(typeof(ResourceType)).Length, s_infos.Length);
        foreach (var info in s_infos)
        {
            byte code = info.ResourceType.Code();
            Assert.True(info.Code == code, $"{info.ResourceType} was supposed to have code == {info.Code}");
            Assert.True(
                info.ResourceType == ResourceType.FromCode((byte)info.Code),
                $"AclResourceType.FromCode({info.Code}) was supposed to be {info.ResourceType}");
        }

        Assert.Equal(ResourceType.UNKNOWN, ResourceType.FromCode((byte)120));
    }

    [Fact]
    public void TestName()
    {
        foreach (var info in s_infos)
        {
            Assert.True(
                info.ResourceType == ResourceType.FromString(info.Name),
                $"ResourceType.FromString({info.Name}) was supposed to be {info.ResourceType}");
        }

        Assert.Equal(ResourceType.UNKNOWN, ResourceType.FromString("something"));
    }

    [Fact]
    public void TestExhaustive()
    {
        var values = (ResourceType[])Enum.GetValues(typeof(ResourceType));

        Assert.Equal(s_infos.Length, values.Length);
        for (int i = 0; i < s_infos.Length; i++)
        {
            Assert.Equal(s_infos[i].ResourceType, values[i]);
        }
    }

    [Fact]
    public void NoneDereference_test_a_dereferenced_none_is_a_type_error_FromStringOfNullNamesStr()
    {
        var e = Assert.Throws<ArgumentNullException>(() => ResourceType.FromString(null!));

        Assert.Equal("str", e.ParamName);
        Assert.Equal(new ArgumentNullException("str").Message, e.Message);
    }

    [Theory]
    [InlineData("tran\u017Factional_id", ResourceType.TRANSACTIONAL_ID)]
    [InlineData("top\u0131c", ResourceType.TOPIC)]
    [InlineData("clu\uFB06er", ResourceType.CLUSTER)]
    [InlineData("Delegation_Token", ResourceType.DELEGATION_TOKEN)]
    public void FromString_UpperCasesTheNameAsJavasToUpperCaseRoot(string str, ResourceType expected) =>
        Assert.Equal(expected, ResourceType.FromString(str));

    [Theory]
    [InlineData("TOPIC, GROUP")]
    [InlineData("2")]
    [InlineData(" topic")]
    [InlineData("topic ")]
    [InlineData("")]
    [InlineData("TRANSACTIONAL-ID")]
    public void FromString_IsUnknownForWhatJavasValueOfRefuses(string str) =>
        Assert.Equal(ResourceType.UNKNOWN, ResourceType.FromString(str));

    [Theory]
    [InlineData("tr-TR")]
    [InlineData("az-Latn-AZ")]
    public void FromString_UpperCasesTheNameInTheInvariantCulture(string culture)
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);
        try
        {
            Assert.NotEqual("I", "i".ToUpper(CultureInfo.CurrentCulture));

            Assert.Equal(ResourceType.TOPIC, ResourceType.FromString("topic"));
            Assert.Equal(ResourceType.TRANSACTIONAL_ID, ResourceType.FromString("transactional_id"));
            Assert.Equal(ResourceType.DELEGATION_TOKEN, ResourceType.FromString("delegation_token"));
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Theory]
    [InlineData(8)]
    [InlineData(128)]
    [InlineData(255)]
    public void FromCode_IsUnknownForACodeNoResourceTypeHas(int code) =>
        Assert.Equal(ResourceType.UNKNOWN, ResourceType.FromCode((byte)code));

    [Theory]
    [InlineData(8, 8)]
    [InlineData(99, 99)]
    [InlineData(-1, 255)]
    public void Code_AndIsUnknown_ReadAnUndefinedValueAsItIs(int value, int code)
    {
        var resourceType = (ResourceType)value;

        Assert.Equal(code, resourceType.Code());
        Assert.False(resourceType.IsUnknown());
    }

    [Fact]
    public void TheMembers_AreCalledOnTheMethodsClassToo()
    {
        Assert.Equal(ResourceType.GROUP, ResourceTypeMethods.FromString("group"));
        Assert.Equal(ResourceType.USER, ResourceTypeMethods.FromCode(7));
        Assert.Equal(5, ResourceTypeMethods.Code(ResourceType.TRANSACTIONAL_ID));
        Assert.True(ResourceTypeMethods.IsUnknown(ResourceType.UNKNOWN));
        Assert.False(ResourceTypeMethods.IsUnknown(ResourceType.ANY));

        var e = Assert.Throws<ArgumentNullException>(() => ResourceTypeMethods.FromString(null!));
        Assert.Equal("str", e.ParamName);
    }
}
