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

using Confluent.Kafka.Admin;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The <see cref="OpType"/> part of Python's <c>test_enums</c> on <see cref="OpTypeMethods"/>, under Java's names;
/// then every byte id, every constant's round trip, the id an instance member reads from an undefined value, and the
/// form older C# calls.
/// </summary>
public sealed class PublicOpTypeTests
{
    [Fact]
    public void AdminValueTypes_test_enums_OpTypesIdsAndLookups()
    {
        Assert.Equal(new[] { 0, 1, 2, 3 }, ((OpType[])Enum.GetValues(typeof(OpType))).Select(op => (int)op));
        Assert.Equal(OpType.SUBTRACT, OpType.ForId(3));
        Assert.Null(OpType.ForId(7));
        Assert.Equal(2, OpType.APPEND.Id());
        Assert.Equal("DELETE", OpType.DELETE.ToString());
    }

    [Fact]
    public void Id_IsJavasIdForEveryConstant()
    {
        Assert.Equal(
            new byte[] { 0, 1, 2, 3 },
            new[] { OpType.SET, OpType.DELETE, OpType.APPEND, OpType.SUBTRACT }.Select(op => op.Id()));
    }

    [Fact]
    public void ForId_RoundTripsEveryConstant()
    {
        foreach (OpType op in (OpType[])Enum.GetValues(typeof(OpType)))
        {
            Assert.True(op == OpType.ForId(op.Id()), op.ToString());
        }
    }

    [Fact]
    public void ForId_EveryByte_IsTheTypeWithThatIdOrNull()
    {
        for (int id = 0; id <= byte.MaxValue; id++)
        {
            OpType? expected = id <= 3 ? (OpType)id : null;

            Assert.Equal(expected, OpType.ForId((byte)id));
        }
    }

    [Theory]
    [InlineData(99, 99)]
    [InlineData(4, 4)]
    [InlineData(-1, 255)]
    [InlineData(259, 3)]
    public void Id_ReadsAnUndefinedValueAsItIs(int value, int id) =>
        Assert.Equal(id, ((OpType)value).Id());

    [Fact]
    public void TheMembers_AreCalledOnTheMethodsClassToo()
    {
        Assert.Equal(OpType.DELETE, OpTypeMethods.ForId(1));
        Assert.Null(OpTypeMethods.ForId(4));
        Assert.Equal(3, OpTypeMethods.Id(OpType.SUBTRACT));
    }
}
