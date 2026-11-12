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

using Confluent.Kafka.Common;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The <see cref="ClassicGroupState"/> part of Python's <c>test_group_state_and_types</c> and its
/// <c>test_none_dereference</c> row on <see cref="ClassicGroupStateMethods"/>, under Java's names; then Java's text of
/// every constant and its round trip, Java's case folding, the text of an undefined value, and the form older C#
/// calls.
/// </summary>
public sealed class PublicClassicGroupStateTests
{
    private static readonly (ClassicGroupState State, string Text)[] s_texts =
    {
        (ClassicGroupState.UNKNOWN, "Unknown"),
        (ClassicGroupState.PREPARING_REBALANCE, "PreparingRebalance"),
        (ClassicGroupState.COMPLETING_REBALANCE, "CompletingRebalance"),
        (ClassicGroupState.STABLE, "Stable"),
        (ClassicGroupState.DEAD, "Dead"),
        (ClassicGroupState.EMPTY, "Empty"),
    };

    [Fact]
    public void AdminCommonTypes_test_group_state_and_types_ClassicGroupStatesTextAndParse()
    {
        Assert.Equal(ClassicGroupState.PREPARING_REBALANCE, ClassicGroupState.Parse("PREPARINGREBALANCE"));
        Assert.Equal(ClassicGroupState.UNKNOWN, ClassicGroupState.Parse("x"));
        Assert.Equal("CompletingRebalance", ClassicGroupState.ToString(ClassicGroupState.COMPLETING_REBALANCE));
        Assert.Equal("STABLE", ClassicGroupState.STABLE.ToString());
    }

    [Fact]
    public void NoneDereference_test_a_dereferenced_none_is_a_type_error_ParseOfNullNamesName()
    {
        var e = Assert.Throws<ArgumentNullException>(() => ClassicGroupState.Parse(null!));

        Assert.Equal("name", e.ParamName);
        Assert.Equal(new ArgumentNullException("name").Message, e.Message);
    }

    [Fact]
    public void ToString_IsJavasText_AndParseRoundTripsIt_ForEveryConstant()
    {
        Assert.Equal(Enum.GetValues(typeof(ClassicGroupState)).Length, s_texts.Length);
        foreach ((ClassicGroupState state, string text) in s_texts)
        {
            Assert.Equal(text, ClassicGroupState.ToString(state));
            Assert.Equal(state, ClassicGroupState.Parse(ClassicGroupState.ToString(state)));
            Assert.Equal(state, ClassicGroupState.Parse(text.ToUpperInvariant()));
            Assert.Equal(state, ClassicGroupState.Parse(text.ToLowerInvariant()));
        }
    }

    [Fact]
    public void ToString_OfTheEnumValue_IsStillTheConstantName()
    {
        Assert.Equal("COMPLETING_REBALANCE", ClassicGroupState.COMPLETING_REBALANCE.ToString());
        Assert.Equal("DEAD", $"{ClassicGroupState.DEAD}");
        Assert.Equal("CompletingRebalance", ClassicGroupState.ToString(ClassicGroupState.COMPLETING_REBALANCE));
    }

    [Theory]
    [InlineData("PREPARING_REBALANCE")]
    [InlineData("COMPLETING_REBALANCE")]
    [InlineData("NotReady")]
    [InlineData("Assigning")]
    [InlineData(" dead")]
    [InlineData("1")]
    [InlineData("")]
    public void Parse_IsUnknownForAnythingButJavasText(string name) =>
        Assert.Equal(ClassicGroupState.UNKNOWN, ClassicGroupState.Parse(name));

    [Theory]
    [InlineData("\u017Ftable", ClassicGroupState.STABLE)]
    [InlineData("\uFB06able", ClassicGroupState.STABLE)]
    [InlineData("\uFB05ABLE", ClassicGroupState.STABLE)]
    [InlineData("prepar\u0131ngrebalance", ClassicGroupState.PREPARING_REBALANCE)]
    [InlineData("Complet\u0131ngRebalance", ClassicGroupState.COMPLETING_REBALANCE)]
    public void Parse_UpperCasesBothSidesAsJavasToUpperCaseRoot(string name, ClassicGroupState expected) =>
        Assert.Equal(expected, ClassicGroupState.Parse(name));

    [Theory]
    [InlineData("tr-TR")]
    [InlineData("az-Latn-AZ")]
    public void Parse_UpperCasesInTheInvariantCulture(string culture)
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);
        try
        {
            Assert.NotEqual("I", "i".ToUpper(CultureInfo.CurrentCulture));

            Assert.Equal(ClassicGroupState.PREPARING_REBALANCE, ClassicGroupState.Parse("preparingrebalance"));
            Assert.Equal(ClassicGroupState.COMPLETING_REBALANCE, ClassicGroupState.Parse("completingrebalance"));
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Theory]
    [InlineData(99, "99")]
    [InlineData(-1, "-1")]
    public void ToString_OfAnUndefinedValue_IsItsNumberInTheInvariantCulture(int value, string text)
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        var minus = (CultureInfo)CultureInfo.GetCultureInfo("sv-SE").Clone();
        minus.NumberFormat.NegativeSign = "\u2212";
        CultureInfo.CurrentCulture = minus;
        try
        {
            Assert.NotEqual("-1", (-1).ToString(CultureInfo.CurrentCulture));

            Assert.Equal(text, ClassicGroupState.ToString((ClassicGroupState)value));
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Fact]
    public void TheMethods_AreCalledOnTheMethodsClassToo()
    {
        Assert.Equal(ClassicGroupState.DEAD, ClassicGroupStateMethods.Parse("dead"));
        Assert.Equal("Dead", ClassicGroupStateMethods.ToString(ClassicGroupState.DEAD));

        var e = Assert.Throws<ArgumentNullException>(() => ClassicGroupStateMethods.Parse(null!));
        Assert.Equal("name", e.ParamName);
    }
}
