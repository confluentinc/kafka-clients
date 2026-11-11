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
/// The <see cref="GroupState"/> part of Python's <c>test_group_state_and_types</c> and its
/// <c>test_none_dereference</c> row on <see cref="GroupStateMethods"/>, under Java's names; then Java's text of every
/// constant and its round trip, Java's case folding, the text of an undefined value, and the form older C# calls.
/// </summary>
public sealed class PublicGroupStateTests
{
    private static readonly (GroupState State, string Text)[] s_texts =
    {
        (GroupState.UNKNOWN, "Unknown"),
        (GroupState.PREPARING_REBALANCE, "PreparingRebalance"),
        (GroupState.COMPLETING_REBALANCE, "CompletingRebalance"),
        (GroupState.STABLE, "Stable"),
        (GroupState.DEAD, "Dead"),
        (GroupState.EMPTY, "Empty"),
        (GroupState.ASSIGNING, "Assigning"),
        (GroupState.RECONCILING, "Reconciling"),
        (GroupState.NOT_READY, "NotReady"),
    };

    [Fact]
    public void AdminCommonTypes_test_group_state_and_types_GroupStatesTextsAndParse()
    {
        Assert.Equal(
            new[]
            {
                "Unknown", "PreparingRebalance", "CompletingRebalance", "Stable", "Dead", "Empty", "Assigning",
                "Reconciling", "NotReady",
            },
            Array.ConvertAll((GroupState[])Enum.GetValues(typeof(GroupState)), state => GroupState.ToString(state)));
        Assert.Equal(GroupState.STABLE, GroupState.Parse("stable"));
        Assert.Equal(GroupState.NOT_READY, GroupState.Parse("notready"));
        Assert.Equal(GroupState.UNKNOWN, GroupState.Parse("bogus"));
    }

    [Fact]
    public void NoneDereference_test_a_dereferenced_none_is_a_type_error_ParseOfNullNamesName()
    {
        var e = Assert.Throws<ArgumentNullException>(() => GroupState.Parse(null!));

        Assert.Equal("name", e.ParamName);
        Assert.Equal(new ArgumentNullException("name").Message, e.Message);
    }

    [Fact]
    public void ToString_IsJavasText_AndParseRoundTripsIt_ForEveryConstant()
    {
        Assert.Equal(Enum.GetValues(typeof(GroupState)).Length, s_texts.Length);
        foreach ((GroupState state, string text) in s_texts)
        {
            Assert.Equal(text, GroupState.ToString(state));
            Assert.Equal(state, GroupState.Parse(GroupState.ToString(state)));
            Assert.Equal(state, GroupState.Parse(text.ToUpperInvariant()));
            Assert.Equal(state, GroupState.Parse(text.ToLowerInvariant()));
        }
    }

    [Fact]
    public void ToString_OfTheEnumValue_IsStillTheConstantName()
    {
        Assert.Equal("PREPARING_REBALANCE", GroupState.PREPARING_REBALANCE.ToString());
        Assert.Equal("NOT_READY", $"{GroupState.NOT_READY}");
        Assert.Equal("PreparingRebalance", GroupState.ToString(GroupState.PREPARING_REBALANCE));
    }

    [Theory]
    [InlineData("PREPARING_REBALANCE")]
    [InlineData("COMPLETING_REBALANCE")]
    [InlineData("NOT_READY")]
    [InlineData(" stable")]
    [InlineData("stable ")]
    [InlineData("3")]
    [InlineData("Stable, Dead")]
    [InlineData("")]
    public void Parse_IsUnknownForAnythingButJavasText(string name) =>
        Assert.Equal(GroupState.UNKNOWN, GroupState.Parse(name));

    [Theory]
    [InlineData("\u017Ftable", GroupState.STABLE)]
    [InlineData("\uFB06able", GroupState.STABLE)]
    [InlineData("\uFB05ABLE", GroupState.STABLE)]
    [InlineData("A\u00DFigning", GroupState.ASSIGNING)]
    [InlineData("Ass\u0131gn\u0131ng", GroupState.ASSIGNING)]
    [InlineData("reconc\u0131l\u0131ng", GroupState.RECONCILING)]
    public void Parse_UpperCasesBothSidesAsJavasToUpperCaseRoot(string name, GroupState expected) =>
        Assert.Equal(expected, GroupState.Parse(name));

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

            Assert.Equal(GroupState.ASSIGNING, GroupState.Parse("assigning"));
            Assert.Equal(GroupState.RECONCILING, GroupState.Parse("reconciling"));
            Assert.Equal(GroupState.PREPARING_REBALANCE, GroupState.Parse("preparingrebalance"));
            Assert.Equal(GroupState.COMPLETING_REBALANCE, GroupState.Parse("completingrebalance"));
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

            Assert.Equal(text, GroupState.ToString((GroupState)value));
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Fact]
    public void TheMethods_AreCalledOnTheMethodsClassToo()
    {
        Assert.Equal(GroupState.STABLE, GroupStateMethods.Parse("STABLE"));
        Assert.Equal("Stable", GroupStateMethods.ToString(GroupState.STABLE));

        var e = Assert.Throws<ArgumentNullException>(() => GroupStateMethods.Parse(null!));
        Assert.Equal("name", e.ParamName);
    }
}
