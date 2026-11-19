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

using Confluent.Kafka.Common.Record;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Python's <c>test_timestamp_type</c> on <see cref="TimestampTypeMethods"/>, under Java's names; then Java's text of
/// every constant and its round trip, the exact match Java's <c>forName</c> makes and its message for a
/// <see langword="null"/> name, the text of an undefined value, and the form older C# calls.
/// </summary>
public sealed class PublicTimestampTypeTests
{
    private static readonly (TimestampType Type, int Id, string Text)[] s_texts =
    {
        (TimestampType.NO_TIMESTAMP_TYPE, -1, "NoTimestampType"),
        (TimestampType.CREATE_TIME, 0, "CreateTime"),
        (TimestampType.LOG_APPEND_TIME, 1, "LogAppendTime"),
    };

    [Fact]
    public void CommonTypes_test_timestamp_type_ValuesTextsAndForName()
    {
        Assert.Equal(
            new[] { -1, 0, 1 },
            new[] { (int)TimestampType.NO_TIMESTAMP_TYPE, (int)TimestampType.CREATE_TIME, (int)TimestampType.LOG_APPEND_TIME });
        Assert.Equal(
            new[] { "NoTimestampType", "CreateTime", "LogAppendTime" },
            new[]
            {
                TimestampType.ToString(TimestampType.NO_TIMESTAMP_TYPE),
                TimestampType.ToString(TimestampType.CREATE_TIME),
                TimestampType.ToString(TimestampType.LOG_APPEND_TIME),
            });
        Assert.Equal(TimestampType.CREATE_TIME, TimestampType.ForName("CreateTime"));

        var e = Assert.Throws<NoSuchElementException>(() => TimestampType.ForName("nope"));
        Assert.Equal("Invalid timestamp type nope", e.Message);
    }

    [Fact]
    public void ToString_IsJavasText_AndForNameRoundTripsIt_ForEveryConstant()
    {
        Assert.Equal(Enum.GetValues(typeof(TimestampType)).Length, s_texts.Length);
        foreach ((TimestampType type, int id, string text) in s_texts)
        {
            Assert.Equal(id, (int)type);
            Assert.Equal(text, TimestampType.ToString(type));
            Assert.Equal(type, TimestampType.ForName(TimestampType.ToString(type)));
        }
    }

    [Fact]
    public void ToString_OfTheEnumValue_IsStillTheConstantName()
    {
        Assert.Equal("CREATE_TIME", TimestampType.CREATE_TIME.ToString());
        Assert.Equal("NO_TIMESTAMP_TYPE", $"{TimestampType.NO_TIMESTAMP_TYPE}");
        Assert.Equal("CreateTime", TimestampType.ToString(TimestampType.CREATE_TIME));
    }

    [Fact]
    public void ForName_OfNull_ThrowsWithJavasNullText()
    {
        var e = Assert.Throws<NoSuchElementException>(() => TimestampType.ForName(null!));

        Assert.Equal("Invalid timestamp type null", e.Message);
    }

    [Theory]
    [InlineData("createtime")]
    [InlineData("CREATETIME")]
    [InlineData("CREATE_TIME")]
    [InlineData(" CreateTime")]
    [InlineData("0")]
    [InlineData("-1")]
    [InlineData("")]
    public void ForName_ThrowsForAnythingButJavasExactText(string name)
    {
        var e = Assert.Throws<NoSuchElementException>(() => TimestampType.ForName(name));

        Assert.Equal("Invalid timestamp type " + name, e.Message);
    }

    [Theory]
    [InlineData(5, "5")]
    [InlineData(-2, "-2")]
    public void ToString_OfAnUndefinedValue_IsItsNumberInTheInvariantCulture(int value, string text)
    {
        CultureInfo previous = CultureInfo.CurrentCulture;
        var minus = (CultureInfo)CultureInfo.GetCultureInfo("sv-SE").Clone();
        minus.NumberFormat.NegativeSign = "\u2212";
        CultureInfo.CurrentCulture = minus;
        try
        {
            Assert.NotEqual("-1", (-1).ToString(CultureInfo.CurrentCulture));

            Assert.Equal(text, TimestampType.ToString((TimestampType)value));
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Fact]
    public void TheMethods_AreCalledOnTheMethodsClassToo()
    {
        Assert.Equal(TimestampType.LOG_APPEND_TIME, TimestampTypeMethods.ForName("LogAppendTime"));
        Assert.Equal("LogAppendTime", TimestampTypeMethods.ToString(TimestampType.LOG_APPEND_TIME));

        var e = Assert.Throws<NoSuchElementException>(() => TimestampTypeMethods.ForName("x"));
        Assert.Equal("Invalid timestamp type x", e.Message);
    }
}
