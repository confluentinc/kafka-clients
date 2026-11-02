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
using System.Reflection;

using Confluent.Kafka.Consumer;
using Xunit;

namespace Confluent.Kafka.UnitTests;
/// <summary>
/// <see cref="OffsetAndMetadata"/> and <see cref="OffsetAndTimestamp"/>: their fields, the leader epoch, value
/// equality and the <c>ToString</c> forms. A negative leader epoch is absent, as Java's <c>leaderEpoch()</c> reports
/// it: .NET has no raw accessor, so the constructor normalises it.
/// </summary>
public sealed class PublicConsumerOffsetValueTypeTests
{
    [Fact]
    public void OffsetAndMetadata_StoresFields_WithPresentLeaderEpoch()
    {
        OffsetAndMetadata value = new OffsetAndMetadata(42, "meta", 3);

        Assert.Equal(42, value.Offset);
        Assert.Equal("meta", value.Metadata);
        Assert.True(value.LeaderEpoch.HasValue);
        Assert.Equal(3, value.LeaderEpoch!.Value);
    }

    [Fact]
    public void OffsetAndMetadata_AbsentLeaderEpoch_IsNull()
    {
        OffsetAndMetadata value = new OffsetAndMetadata(7, string.Empty, null);

        Assert.Equal(7, value.Offset);
        Assert.Equal(string.Empty, value.Metadata);
        Assert.Null(value.LeaderEpoch);
    }

    [Fact]
    public void OffsetAndMetadata_ToString_IncludesAllFields()
    {
        Assert.Equal(
            "OffsetAndMetadata{offset=42, leaderEpoch=3, metadata='meta'}",
            new OffsetAndMetadata(42, "meta", 3).ToString());
        Assert.Equal(
            "OffsetAndMetadata{offset=7, leaderEpoch=null, metadata=''}",
            new OffsetAndMetadata(7, string.Empty, null).ToString());
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(int.MinValue)]
    public void G5_3_NegativeLeaderEpoch_IsAbsent(int epoch)
    {
        OffsetAndMetadata value = new OffsetAndMetadata(5, "m", epoch);

        Assert.Null(value.LeaderEpoch);
        Assert.Equal("OffsetAndMetadata{offset=5, leaderEpoch=null, metadata='m'}", value.ToString());

        OffsetAndMetadata zero = new OffsetAndMetadata(5, "m", 0);
        Assert.Equal(0, zero.LeaderEpoch);
        Assert.Equal("OffsetAndMetadata{offset=5, leaderEpoch=0, metadata='m'}", zero.ToString());
    }

    [Fact]
    public void G5_2_OffsetAndMetadata_HasValueEquality()
    {
        OffsetAndMetadata absent = new OffsetAndMetadata(5, "m", null);
        OffsetAndMetadata negative = new OffsetAndMetadata(5, "m", -1);

        Assert.True(absent.Equals(negative));
        Assert.True(absent.Equals((object)negative));
        Assert.True(negative.Equals(absent));
        Assert.Equal(absent.GetHashCode(), negative.GetHashCode());

        OffsetAndMetadata nullMetadata = new OffsetAndMetadata(5, null, 3);
        OffsetAndMetadata emptyMetadata = new OffsetAndMetadata(5, string.Empty, 3);
        Assert.True(nullMetadata.Equals(emptyMetadata));
        Assert.Equal(nullMetadata.GetHashCode(), emptyMetadata.GetHashCode());

        OffsetAndMetadata baseline = new OffsetAndMetadata(5, "m", 3);
        Assert.True(baseline.Equals(new OffsetAndMetadata(5, "m", 3)));
        Assert.Equal(baseline.GetHashCode(), new OffsetAndMetadata(5, "m", 3).GetHashCode());
        Assert.False(baseline.Equals(new OffsetAndMetadata(6, "m", 3)));
        Assert.False(baseline.Equals(new OffsetAndMetadata(5, "M", 3)));
        Assert.False(baseline.Equals(new OffsetAndMetadata(5, "m", 4)));
        Assert.False(baseline.Equals(new OffsetAndMetadata(5, "m", null)));
        Assert.False(baseline.Equals(null));
        Assert.False(baseline.Equals((object?)null));
        Assert.False(baseline.Equals("OffsetAndMetadata"));

        Assert.Equal(baseline.GetHashCode(), baseline.GetHashCode());

        Type type = typeof(OffsetAndMetadata);
        Assert.Contains(typeof(IEquatable<OffsetAndMetadata>), type.GetInterfaces());
        MethodInfo typed = Assert.Single(
            type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly),
            method => method.Name == nameof(Equals)
                && method.GetParameters().Single().ParameterType == type);
        Assert.Equal(typeof(bool), typed.ReturnType);
        Assert.Same(type, type.GetMethod(nameof(GetHashCode), Type.EmptyTypes)!.DeclaringType);
        Assert.Null(type.GetMethod("op_Equality"));
        Assert.Null(type.GetMethod("op_Inequality"));
    }

    [Fact]
    public void OffsetAndTimestamp_StoresFields_WithPresentLeaderEpoch()
    {
        OffsetAndTimestamp value = new OffsetAndTimestamp(100, 1_700_000_000_000L, 5);

        Assert.Equal(100, value.Offset);
        Assert.Equal(1_700_000_000_000L, value.Timestamp);
        Assert.True(value.LeaderEpoch.HasValue);
        Assert.Equal(5, value.LeaderEpoch!.Value);
    }

    [Fact]
    public void OffsetAndTimestamp_AbsentLeaderEpoch_IsNull()
    {
        OffsetAndTimestamp value = new OffsetAndTimestamp(9, 123L, null);

        Assert.Equal(9, value.Offset);
        Assert.Equal(123L, value.Timestamp);
        Assert.Null(value.LeaderEpoch);
    }

    [Fact]
    public void OffsetAndTimestamp_ToString_IncludesAllFields()
    {
        Assert.Equal(
            "OffsetAndTimestamp{offset=100, timestamp=123, leaderEpoch=5}",
            new OffsetAndTimestamp(100, 123L, 5).ToString());
        Assert.Equal(
            "OffsetAndTimestamp{offset=9, timestamp=123, leaderEpoch=null}",
            new OffsetAndTimestamp(9, 123L, null).ToString());
    }
}
