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
using System.Linq;

using Confluent.Kafka.Common.Resource;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Python's <c>test_pattern_type</c> and <c>test_none_dereference</c> row for <see cref="PatternTypeMethods"/>, as
/// Java has no <c>PatternTypeTest</c>: the codes in Java's order, <c>fromCode</c>, the exact and case-sensitive
/// <c>fromString</c> (<see cref="PatternType.UNKNOWN"/> for <see langword="null"/>), <c>isSpecific</c> and
/// <c>isUnknown</c>; then the texts an exact match refuses, what an instance member reads from an undefined value,
/// and the form older C# calls.
/// </summary>
public sealed class PublicPatternTypeTests
{
    [Fact]
    public void AdminCommonTypes_test_pattern_type_TheCodesAreJavasInOrder() =>
        Assert.Equal(
            new byte[] { 0, 1, 2, 3, 4 },
            ((PatternType[])Enum.GetValues(typeof(PatternType))).Select(type => type.Code()));

    [Theory]
    [InlineData(0, PatternType.UNKNOWN)]
    [InlineData(1, PatternType.ANY)]
    [InlineData(2, PatternType.MATCH)]
    [InlineData(3, PatternType.LITERAL)]
    [InlineData(4, PatternType.PREFIXED)]
    [InlineData(5, PatternType.UNKNOWN)]
    [InlineData(99, PatternType.UNKNOWN)]
    [InlineData(255, PatternType.UNKNOWN)]
    public void AdminCommonTypes_test_pattern_type_FromCodeFindsTheTypeOrUnknown(int code, PatternType expected) =>
        Assert.Equal(expected, PatternType.FromCode((byte)code));

    [Theory]
    [InlineData("UNKNOWN", PatternType.UNKNOWN)]
    [InlineData("ANY", PatternType.ANY)]
    [InlineData("MATCH", PatternType.MATCH)]
    [InlineData("LITERAL", PatternType.LITERAL)]
    [InlineData("PREFIXED", PatternType.PREFIXED)]
    public void AdminCommonTypes_test_pattern_type_FromStringFindsTheConstantName(string name, PatternType expected) =>
        Assert.Equal(expected, PatternType.FromString(name));

    [Theory]
    [InlineData("literal")]
    [InlineData("Prefixed")]
    [InlineData("PRE\uFB01XED")]
    [InlineData("LITERAL, PREFIXED")]
    [InlineData("3")]
    [InlineData(" LITERAL")]
    [InlineData("LITERAL ")]
    [InlineData("")]
    public void AdminCommonTypes_test_pattern_type_FromStringIsExactAndCaseSensitive(string name) =>
        Assert.Equal(PatternType.UNKNOWN, PatternType.FromString(name));

    [Fact]
    public void AdminCommonTypes_test_pattern_type_OnlyLiteralAndPrefixedAreSpecific() =>
        Assert.Equal(
            new[] { PatternType.LITERAL, PatternType.PREFIXED },
            ((PatternType[])Enum.GetValues(typeof(PatternType))).Where(type => type.IsSpecific()));

    [Fact]
    public void AdminCommonTypes_test_pattern_type_OnlyUnknownIsUnknown() =>
        Assert.Equal(
            new[] { PatternType.UNKNOWN },
            ((PatternType[])Enum.GetValues(typeof(PatternType))).Where(type => type.IsUnknown()));

    [Fact]
    public void NoneDereference_test_java_does_not_dereference_there_FromStringOfNullIsUnknown() =>
        Assert.Equal(PatternType.UNKNOWN, PatternType.FromString(null!));

    [Fact]
    public void EveryMember_RoundTripsThroughItsCodeAndItsName()
    {
        foreach (PatternType type in (PatternType[])Enum.GetValues(typeof(PatternType)))
        {
            Assert.Equal(type, PatternType.FromCode(type.Code()));
            Assert.Equal(type, PatternType.FromString(type.ToString()));
        }
    }

    [Theory]
    [InlineData(5, 5)]
    [InlineData(99, 99)]
    [InlineData(-1, 255)]
    public void TheInstanceMembers_ReadAnUndefinedValueAsJavasBodiesRead(int value, int code)
    {
        var patternType = (PatternType)value;

        Assert.Equal(code, patternType.Code());
        Assert.False(patternType.IsUnknown());
        Assert.True(patternType.IsSpecific());
    }

    [Fact]
    public void TheMembers_AreCalledOnTheMethodsClassToo()
    {
        Assert.Equal(PatternType.LITERAL, PatternTypeMethods.FromString("LITERAL"));
        Assert.Equal(PatternType.UNKNOWN, PatternTypeMethods.FromString(null!));
        Assert.Equal(PatternType.PREFIXED, PatternTypeMethods.FromCode(4));
        Assert.Equal(2, PatternTypeMethods.Code(PatternType.MATCH));
        Assert.True(PatternTypeMethods.IsUnknown(PatternType.UNKNOWN));
        Assert.False(PatternTypeMethods.IsUnknown(PatternType.LITERAL));
        Assert.True(PatternTypeMethods.IsSpecific(PatternType.PREFIXED));
        Assert.False(PatternTypeMethods.IsSpecific(PatternType.MATCH));
    }
}
