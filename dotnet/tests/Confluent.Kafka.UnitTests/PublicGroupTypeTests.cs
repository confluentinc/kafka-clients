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
/// The <see cref="GroupType"/> part of Python's <c>test_group_state_and_types</c> on <see cref="GroupTypeMethods"/>,
/// under Java's names; then Java's text of every constant and its round trip, Java's lower-casing, the text of an
/// undefined value, and the form older C# calls.
/// </summary>
public sealed class PublicGroupTypeTests
{
    private static readonly (GroupType Type, string Text)[] s_texts =
    {
        (GroupType.UNKNOWN, "Unknown"),
        (GroupType.CONSUMER, "Consumer"),
        (GroupType.CLASSIC, "Classic"),
        (GroupType.SHARE, "Share"),
        (GroupType.STREAMS, "Streams"),
    };

    [Fact]
    public void AdminCommonTypes_test_group_state_and_types_GroupTypesTextAndParse()
    {
        Assert.Equal(GroupType.CONSUMER, GroupType.Parse("CONSUMER"));
        Assert.Equal(GroupType.UNKNOWN, GroupType.Parse(null));
        Assert.Equal("Classic", GroupType.ToString(GroupType.CLASSIC));
    }

    [Fact]
    public void ToString_IsJavasText_AndParseRoundTripsIt_ForEveryConstant()
    {
        Assert.Equal(Enum.GetValues(typeof(GroupType)).Length, s_texts.Length);
        foreach ((GroupType type, string text) in s_texts)
        {
            Assert.Equal(text, GroupType.ToString(type));
            Assert.Equal(type, GroupType.Parse(GroupType.ToString(type)));
            Assert.Equal(type, GroupType.Parse(text.ToUpperInvariant()));
            Assert.Equal(type, GroupType.Parse(text.ToLowerInvariant()));
        }
    }

    [Fact]
    public void ToString_OfTheEnumValue_IsStillTheConstantName()
    {
        Assert.Equal("CONSUMER", GroupType.CONSUMER.ToString());
        Assert.Equal("STREAMS", $"{GroupType.STREAMS}");
        Assert.Equal("Consumer", GroupType.ToString(GroupType.CONSUMER));
    }

    [Theory]
    [InlineData(" consumer")]
    [InlineData("consumer ")]
    [InlineData("1")]
    [InlineData("Consumer, Classic")]
    [InlineData("")]
    [InlineData("bogus")]
    public void Parse_IsUnknownForAnythingButJavasText(string name) =>
        Assert.Equal(GroupType.UNKNOWN, GroupType.Parse(name));

    [Theory]
    [InlineData("CLASS\u0130C", GroupType.UNKNOWN)]
    [InlineData("\u0130", GroupType.UNKNOWN)]
    [InlineData("UN\u212ANOWN", GroupType.UNKNOWN)]
    [InlineData("SHARE", GroupType.SHARE)]
    [InlineData("Cla\u017F\u017Fic", GroupType.UNKNOWN)]
    [InlineData("cla\u00DFic", GroupType.UNKNOWN)]
    [InlineData("Class\u0131c", GroupType.UNKNOWN)]
    [InlineData("\u017Fhare", GroupType.UNKNOWN)]
    [InlineData("Con\u017Fumer", GroupType.UNKNOWN)]
    [InlineData("\uFB06reams", GroupType.UNKNOWN)]
    public void Parse_LowerCasesBothSidesAsJavasToLowerCaseRoot(string name, GroupType expected) =>
        Assert.Equal(expected, GroupType.Parse(name));

    [Theory]
    [InlineData("tr-TR")]
    [InlineData("az-Latn-AZ")]
    public void Parse_LowerCasesInTheInvariantCulture(string culture)
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(culture);
        try
        {
            Assert.NotEqual("i", "I".ToLower(CultureInfo.CurrentCulture));

            Assert.Equal(GroupType.CLASSIC, GroupType.Parse("CLASSIC"));
            Assert.Equal(GroupType.CONSUMER, GroupType.Parse("CONSUMER"));
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

            Assert.Equal(text, GroupType.ToString((GroupType)value));
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Fact]
    public void TheMethods_AreCalledOnTheMethodsClassToo()
    {
        Assert.Equal(GroupType.SHARE, GroupTypeMethods.Parse("share"));
        Assert.Equal(GroupType.UNKNOWN, GroupTypeMethods.Parse(null));
        Assert.Equal("Share", GroupTypeMethods.ToString(GroupType.SHARE));
    }
}
