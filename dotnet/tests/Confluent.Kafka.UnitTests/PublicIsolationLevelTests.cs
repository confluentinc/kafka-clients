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
/// The <see cref="IsolationLevel"/> part of Python's <c>test_election_type_and_isolation_level</c> on
/// <see cref="IsolationLevelMethods"/>, under Java's names; then every byte id, every constant's round trip and Java
/// text, the id an instance member reads from an undefined value and its text, and the form older C# calls.
/// </summary>
public sealed class PublicIsolationLevelTests
{
    [Fact]
    public void AdminCommonTypes_test_election_type_and_isolation_level_IsolationLevelsIdsAndLookups()
    {
        Assert.Equal(IsolationLevel.READ_COMMITTED, IsolationLevel.ForId(1));
        Assert.Equal(0, IsolationLevel.READ_UNCOMMITTED.Id());

        Assert.Equal("read_committed", IsolationLevel.ToString(IsolationLevel.READ_COMMITTED));

        var e = Assert.Throws<IllegalArgumentException>(() => IsolationLevel.ForId(7));
        Assert.Equal("Unknown isolation level 7", e.Message);
    }

    [Fact]
    public void ToString_IsJavasText_ForEveryConstant()
    {
        Assert.Equal(2, Enum.GetValues(typeof(IsolationLevel)).Length);
        Assert.Equal("read_uncommitted", IsolationLevel.ToString(IsolationLevel.READ_UNCOMMITTED));
        Assert.Equal("read_committed", IsolationLevel.ToString(IsolationLevel.READ_COMMITTED));
    }

    [Fact]
    public void ToString_OfTheEnumValue_IsStillTheConstantName()
    {
        Assert.Equal("READ_COMMITTED", IsolationLevel.READ_COMMITTED.ToString());
        Assert.Equal("READ_UNCOMMITTED", $"{IsolationLevel.READ_UNCOMMITTED}");
        Assert.Equal("read_committed", IsolationLevel.ToString(IsolationLevel.READ_COMMITTED));
    }

    [Theory]
    [InlineData(5, "5")]
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

            Assert.Equal(text, IsolationLevel.ToString((IsolationLevel)value));
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Fact]
    public void Id_IsJavasIdForEveryConstant()
    {
        Assert.Equal(0, IsolationLevel.READ_UNCOMMITTED.Id());
        Assert.Equal(1, IsolationLevel.READ_COMMITTED.Id());
    }

    [Fact]
    public void ForId_RoundTripsEveryConstant()
    {
        foreach (IsolationLevel level in (IsolationLevel[])Enum.GetValues(typeof(IsolationLevel)))
        {
            Assert.True(level == IsolationLevel.ForId(level.Id()), level.ToString());
        }
    }

    [Fact]
    public void ForId_EveryByte_IsTheLevelWithThatId_OrThrowsWithJavasMessage()
    {
        Assert.Equal(IsolationLevel.READ_UNCOMMITTED, IsolationLevel.ForId(0));
        Assert.Equal(IsolationLevel.READ_COMMITTED, IsolationLevel.ForId(1));
        for (int id = 2; id <= byte.MaxValue; id++)
        {
            var e = Assert.Throws<IllegalArgumentException>(() => IsolationLevel.ForId((byte)id));

            Assert.Equal("Unknown isolation level " + id.ToString(CultureInfo.InvariantCulture), e.Message);
        }
    }

    [Theory]
    [InlineData(5, 5)]
    [InlineData(-1, 255)]
    [InlineData(257, 1)]
    public void Id_ReadsAnUndefinedValueAsItIs(int value, int id) =>
        Assert.Equal(id, ((IsolationLevel)value).Id());

    [Fact]
    public void TheMembers_AreCalledOnTheMethodsClassToo()
    {
        Assert.Equal(IsolationLevel.READ_UNCOMMITTED, IsolationLevelMethods.ForId(0));
        Assert.Equal(1, IsolationLevelMethods.Id(IsolationLevel.READ_COMMITTED));
        Assert.Equal("read_uncommitted", IsolationLevelMethods.ToString(IsolationLevel.READ_UNCOMMITTED));

        var e = Assert.Throws<IllegalArgumentException>(() => IsolationLevelMethods.ForId(2));
        Assert.Equal("Unknown isolation level 2", e.Message);
    }
}
