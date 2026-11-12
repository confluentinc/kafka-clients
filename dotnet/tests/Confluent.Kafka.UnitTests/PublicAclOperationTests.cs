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

using Confluent.Kafka.Common.Acl;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Java's <c>AclOperationTest</c> on <see cref="AclOperationMethods"/>, under Java's names, with Java's table; then
/// Python's <c>test_none_dereference</c> row for <c>fromString(null)</c>, Java's case folding and the texts Java's
/// <c>valueOf</c> refuses, the code an instance member reads from an undefined value, and the form older C# calls.
/// </summary>
public sealed class PublicAclOperationTests
{
    private static readonly (AclOperation Operation, int Code, string Name, bool Unknown)[] s_infos =
    {
        (AclOperation.UNKNOWN, 0, "unknown", true),
        (AclOperation.ANY, 1, "any", false),
        (AclOperation.ALL, 2, "all", false),
        (AclOperation.READ, 3, "read", false),
        (AclOperation.WRITE, 4, "write", false),
        (AclOperation.CREATE, 5, "create", false),
        (AclOperation.DELETE, 6, "delete", false),
        (AclOperation.ALTER, 7, "alter", false),
        (AclOperation.DESCRIBE, 8, "describe", false),
        (AclOperation.CLUSTER_ACTION, 9, "cluster_action", false),
        (AclOperation.DESCRIBE_CONFIGS, 10, "describe_configs", false),
        (AclOperation.ALTER_CONFIGS, 11, "alter_configs", false),
        (AclOperation.IDEMPOTENT_WRITE, 12, "idempotent_write", false),
        (AclOperation.CREATE_TOKENS, 13, "create_tokens", false),
        (AclOperation.DESCRIBE_TOKENS, 14, "describe_tokens", false),
        (AclOperation.TWO_PHASE_COMMIT, 15, "two_phase_commit", false),
    };

    [Fact]
    public void TestIsUnknown()
    {
        foreach (var info in s_infos)
        {
            Assert.True(
                info.Unknown == info.Operation.IsUnknown(),
                $"{info.Operation} was supposed to have unknown == {info.Unknown}");
        }
    }

    [Fact]
    public void TestCode()
    {
        Assert.Equal(Enum.GetValues(typeof(AclOperation)).Length, s_infos.Length);
        foreach (var info in s_infos)
        {
            byte code = info.Operation.Code();
            Assert.True(info.Code == code, $"{info.Operation} was supposed to have code == {info.Code}");
            Assert.True(
                info.Operation == AclOperation.FromCode((byte)info.Code),
                $"AclOperation.FromCode({info.Code}) was supposed to be {info.Operation}");
        }

        Assert.Equal(AclOperation.UNKNOWN, AclOperation.FromCode((byte)120));
    }

    [Fact]
    public void TestName()
    {
        foreach (var info in s_infos)
        {
            Assert.True(
                info.Operation == AclOperation.FromString(info.Name),
                $"AclOperation.FromString({info.Name}) was supposed to be {info.Operation}");
        }

        Assert.Equal(AclOperation.UNKNOWN, AclOperation.FromString("something"));
    }

    [Fact]
    public void TestExhaustive()
    {
        var values = (AclOperation[])Enum.GetValues(typeof(AclOperation));

        Assert.Equal(s_infos.Length, values.Length);
        for (int i = 0; i < s_infos.Length; i++)
        {
            Assert.Equal(s_infos[i].Operation, values[i]);
        }
    }

    [Fact]
    public void NoneDereference_test_a_dereferenced_none_is_a_type_error_FromStringOfNullNamesStr()
    {
        var e = Assert.Throws<ArgumentNullException>(() => AclOperation.FromString(null!));

        Assert.Equal("str", e.ParamName);
        Assert.Equal(new ArgumentNullException("str").Message, e.Message);
    }

    [Theory]
    [InlineData("de\u017Fcribe", AclOperation.DESCRIBE)]
    [InlineData("wr\u0131te", AclOperation.WRITE)]
    [InlineData("describe_con\uFB01gs", AclOperation.DESCRIBE_CONFIGS)]
    [InlineData("Idempotent_Write", AclOperation.IDEMPOTENT_WRITE)]
    public void FromString_UpperCasesTheNameAsJavasToUpperCaseRoot(string str, AclOperation expected) =>
        Assert.Equal(expected, AclOperation.FromString(str));

    [Theory]
    [InlineData("READ, WRITE")]
    [InlineData("3")]
    [InlineData(" read")]
    [InlineData("read ")]
    [InlineData("")]
    [InlineData("TWO-PHASE-COMMIT")]
    public void FromString_IsUnknownForWhatJavasValueOfRefuses(string str) =>
        Assert.Equal(AclOperation.UNKNOWN, AclOperation.FromString(str));

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

            Assert.Equal(AclOperation.WRITE, AclOperation.FromString("write"));
            Assert.Equal(AclOperation.IDEMPOTENT_WRITE, AclOperation.FromString("idempotent_write"));
            Assert.Equal(AclOperation.DESCRIBE_CONFIGS, AclOperation.FromString("describe_configs"));
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Theory]
    [InlineData(16)]
    [InlineData(127)]
    [InlineData(128)]
    [InlineData(255)]
    public void FromCode_IsUnknownForACodeNoOperationHas(int code) =>
        Assert.Equal(AclOperation.UNKNOWN, AclOperation.FromCode((byte)code));

    [Theory]
    [InlineData(16, 16)]
    [InlineData(99, 99)]
    [InlineData(-1, 255)]
    public void Code_AndIsUnknown_ReadAnUndefinedValueAsItIs(int value, int code)
    {
        var operation = (AclOperation)value;

        Assert.Equal(code, operation.Code());
        Assert.False(operation.IsUnknown());
    }

    [Fact]
    public void TheMembers_AreCalledOnTheMethodsClassToo()
    {
        Assert.Equal(AclOperation.READ, AclOperationMethods.FromString("read"));
        Assert.Equal(AclOperation.ALTER, AclOperationMethods.FromCode(7));
        Assert.Equal(15, AclOperationMethods.Code(AclOperation.TWO_PHASE_COMMIT));
        Assert.True(AclOperationMethods.IsUnknown(AclOperation.UNKNOWN));
        Assert.False(AclOperationMethods.IsUnknown(AclOperation.ANY));

        var e = Assert.Throws<ArgumentNullException>(() => AclOperationMethods.FromString(null!));
        Assert.Equal("str", e.ParamName);
    }
}
