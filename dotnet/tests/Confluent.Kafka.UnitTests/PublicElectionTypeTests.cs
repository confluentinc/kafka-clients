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
/// The <see cref="ElectionType"/> part of Python's <c>test_election_type_and_isolation_level</c> on
/// <see cref="ElectionTypeMethods"/>, under Java's names; then every byte value, every constant's round trip, and the
/// form older C# calls.
/// </summary>
public sealed class PublicElectionTypeTests
{
    [Fact]
    public void AdminCommonTypes_test_election_type_and_isolation_level_ElectionTypesValuesAndLookups()
    {
        Assert.Equal(0, (int)ElectionType.PREFERRED);
        Assert.Equal(1, (int)ElectionType.UNCLEAN);
        Assert.Equal(ElectionType.UNCLEAN, ElectionType.ValueOf(1));

        var e = Assert.Throws<IllegalArgumentException>(() => ElectionType.ValueOf(5));
        Assert.Equal("Value 5 must be one of [PREFERRED, UNCLEAN]", e.Message);
    }

    [Fact]
    public void ValueOf_RoundTripsEveryConstant()
    {
        foreach (ElectionType type in (ElectionType[])Enum.GetValues(typeof(ElectionType)))
        {
            Assert.True(type == ElectionType.ValueOf((byte)type), type.ToString());
        }
    }

    [Fact]
    public void ValueOf_EveryByte_IsTheTypeWithThatValue_OrThrowsWithJavasMessage()
    {
        Assert.Equal(ElectionType.PREFERRED, ElectionType.ValueOf(0));
        Assert.Equal(ElectionType.UNCLEAN, ElectionType.ValueOf(1));
        for (int value = 2; value <= byte.MaxValue; value++)
        {
            var e = Assert.Throws<IllegalArgumentException>(() => ElectionType.ValueOf((byte)value));

            Assert.Equal("Value " + value.ToString(CultureInfo.InvariantCulture) + " must be one of [PREFERRED, UNCLEAN]", e.Message);
        }
    }

    [Fact]
    public void TheMembers_AreCalledOnTheMethodsClassToo()
    {
        Assert.Equal(ElectionType.PREFERRED, ElectionTypeMethods.ValueOf(0));

        var e = Assert.Throws<IllegalArgumentException>(() => ElectionTypeMethods.ValueOf(2));
        Assert.Equal("Value 2 must be one of [PREFERRED, UNCLEAN]", e.Message);
    }
}
