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

using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Internal.Interop;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Java's <c>ConfigResourceTest</c> on <see cref="ConfigResourceTypeMethods"/>, under Java's names; then every byte
/// id, Java's names beside the value order <see cref="Enum.GetValues(Type)"/> gives (Java's order and values are
/// pinned by name in <see cref="PublicJavaEnumConstantsTests"/>), the id an instance member reads from an undefined
/// value, the internal id lookup, and the form older C# calls.
/// </summary>
public sealed class PublicConfigResourceTypeTests
{
    private static readonly (ConfigResourceType Type, byte Id)[] s_javaOrder =
    {
        (ConfigResourceType.GROUP, 32),
        (ConfigResourceType.CLIENT_METRICS, 16),
        (ConfigResourceType.BROKER_LOGGER, 8),
        (ConfigResourceType.BROKER, 4),
        (ConfigResourceType.TOPIC, 2),
        (ConfigResourceType.UNKNOWN, 0),
    };

    [Fact]
    public void ShouldGetTypeFromId()
    {
        Assert.Equal(ConfigResourceType.TOPIC, ConfigResourceType.ForId((byte)2));
        Assert.Equal(ConfigResourceType.BROKER, ConfigResourceType.ForId((byte)4));
        Assert.Equal(ConfigResourceType.CLIENT_METRICS, ConfigResourceType.ForId((byte)16));
        Assert.Equal(ConfigResourceType.GROUP, ConfigResourceType.ForId((byte)32));
    }

    [Fact]
    public void ShouldReturnUnknownForUnknownCode()
    {
        Assert.Equal(ConfigResourceType.UNKNOWN, ConfigResourceType.ForId(unchecked((byte)-1)));
        Assert.Equal(ConfigResourceType.UNKNOWN, ConfigResourceType.ForId((byte)0));
        Assert.Equal(ConfigResourceType.UNKNOWN, ConfigResourceType.ForId((byte)1));
    }

    [Fact]
    public void ShouldRoundTripEveryType()
    {
        foreach (ConfigResourceType type in (ConfigResourceType[])Enum.GetValues(typeof(ConfigResourceType)))
        {
            Assert.True(type == ConfigResourceType.ForId(type.Id()), type.ToString());
        }
    }

    [Fact]
    public void Id_IsJavasIdForEveryConstant()
    {
        foreach ((ConfigResourceType type, byte id) in s_javaOrder)
        {
            Assert.Equal(id, type.Id());
        }
    }

    [Fact]
    public void ForId_EveryByte_IsTheTypeWithThatIdOrUnknown()
    {
        for (int id = 0; id <= byte.MaxValue; id++)
        {
            ConfigResourceType expected = s_javaOrder.Where(info => info.Id == id).Select(info => info.Type)
                .DefaultIfEmpty(ConfigResourceType.UNKNOWN).Single();

            Assert.Equal(expected, ConfigResourceType.ForId((byte)id));
        }
    }

    [Fact]
    public void GetValues_OrdersTheMembersByValue_NotInJavasOrder()
    {
        Assert.Equal(
            s_javaOrder.Select(info => info.Type.ToString()).OrderBy(name => name, StringComparer.Ordinal),
            Enum.GetNames(typeof(ConfigResourceType)).OrderBy(name => name, StringComparer.Ordinal));
        Assert.Equal(
            new[]
            {
                ConfigResourceType.UNKNOWN, ConfigResourceType.TOPIC, ConfigResourceType.BROKER,
                ConfigResourceType.BROKER_LOGGER, ConfigResourceType.CLIENT_METRICS, ConfigResourceType.GROUP,
            },
            (ConfigResourceType[])Enum.GetValues(typeof(ConfigResourceType)));
        Assert.True(ConfigResourceType.GROUP.CompareTo(ConfigResourceType.TOPIC) > 0);
    }

    [Theory]
    [InlineData(99, 99)]
    [InlineData(64, 64)]
    [InlineData(-1, 255)]
    [InlineData(258, 2)]
    public void Id_ReadsAnUndefinedValueAsItIs(int value, int id) =>
        Assert.Equal(id, ((ConfigResourceType)value).Id());

    [Fact]
    public void TypeFromId_EveryByte_IsForIdsAnswer()
    {
        for (int id = 0; id <= byte.MaxValue; id++)
        {
            Assert.Equal(ConfigResourceType.ForId((byte)id), ConfigResourceMarshal.TypeFromId(id));
        }
    }

    [Theory]
    [InlineData(256)]
    [InlineData(258)]
    [InlineData(288)]
    [InlineData(-254)]
    [InlineData(-1)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MinValue)]
    public void TypeFromId_AnIdOutsideAByte_IsUnknown_NotTheTypeItsLowByteNames(int id) =>
        Assert.Equal(ConfigResourceType.UNKNOWN, ConfigResourceMarshal.TypeFromId(id));

    [Fact]
    public void TheMembers_AreCalledOnTheMethodsClassToo()
    {
        Assert.Equal(ConfigResourceType.BROKER_LOGGER, ConfigResourceTypeMethods.ForId(8));
        Assert.Equal(ConfigResourceType.UNKNOWN, ConfigResourceTypeMethods.ForId(64));
        Assert.Equal(32, ConfigResourceTypeMethods.Id(ConfigResourceType.GROUP));
        Assert.Equal(0, ConfigResourceTypeMethods.Id(ConfigResourceType.UNKNOWN));
    }
}
