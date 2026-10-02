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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Unit tests for the two new public value types <see cref="OffsetAndMetadata"/> /
/// <see cref="OffsetAndTimestamp"/> (M5/P4, PLAN §1). Their <c>internal</c> constructors are
/// reachable from the test project via <c>InternalsVisibleTo</c> — this proves the field
/// storage, the <c>int?</c> <c>LeaderEpoch</c> mapping (present vs absent) at the value level
/// (the non-empty container path that drives these from native is not reachable broker-free
/// — see <c>OffsetMapMarshalTests</c>), the non-null <c>Metadata</c> contract, and the debug
/// <c>ToString</c>.
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
        Assert.Equal(string.Empty, value.Metadata); // never null (Java default "")
        Assert.Null(value.LeaderEpoch);
    }

    [Fact]
    public void OffsetAndMetadata_ToString_IncludesAllFields()
    {
        Assert.Equal(
            "OffsetAndMetadata{offset=42, metadata='meta', leaderEpoch=3}",
            new OffsetAndMetadata(42, "meta", 3).ToString());
        Assert.Equal(
            "OffsetAndMetadata{offset=7, metadata='', leaderEpoch=null}",
            new OffsetAndMetadata(7, string.Empty, null).ToString());
    }

    /// <summary>
    /// G5-3: a negative leader epoch is absent. Java stores the raw value but its
    /// <c>leaderEpoch()</c> getter reports a null or negative one as <c>Optional.empty()</c>
    /// (<c>OffsetAndMetadata.java:98-101</c>), and its <c>toString</c> prints <c>null</c> for it
    /// (<c>:123</c>); .NET has no raw accessor, so the constructor normalises it.
    /// </summary>
    /// <param name="epoch">A negative epoch.</param>
    [Theory]
    [InlineData(-1)]
    [InlineData(int.MinValue)]
    public void G5_3_NegativeLeaderEpoch_IsAbsent(int epoch)
    {
        OffsetAndMetadata value = new OffsetAndMetadata(5, "m", epoch);

        Assert.Null(value.LeaderEpoch);
        Assert.Equal("OffsetAndMetadata{offset=5, metadata='m', leaderEpoch=null}", value.ToString());

        // Zero is the boundary Java keeps: "leaderEpoch < 0" is the only rejection.
        OffsetAndMetadata zero = new OffsetAndMetadata(5, "m", 0);
        Assert.Equal(0, zero.LeaderEpoch);
        Assert.Equal("OffsetAndMetadata{offset=5, metadata='m', leaderEpoch=0}", zero.ToString());
    }

    /// <summary>
    /// G5-2: value equality over the offset, the metadata and the <em>normalised</em> leader
    /// epoch — Java's <c>equals</c> / <c>hashCode</c> (<c>OffsetAndMetadata.java:105-116</c>),
    /// which compare <c>leaderEpoch()</c>, so an epoch of <c>-1</c> equals an absent one.
    /// </summary>
    [Fact]
    public void G5_2_OffsetAndMetadata_HasValueEquality()
    {
        OffsetAndMetadata absent = new OffsetAndMetadata(5, "m", null);
        OffsetAndMetadata negative = new OffsetAndMetadata(5, "m", -1);

        Assert.True(absent.Equals(negative));
        Assert.True(absent.Equals((object)negative));
        Assert.True(negative.Equals(absent));
        Assert.Equal(absent.GetHashCode(), negative.GetHashCode());

        // A null metadata is Java's "" (Objects.requireNonNullElse), so the two are equal.
        OffsetAndMetadata nullMetadata = new OffsetAndMetadata(5, null, 3);
        OffsetAndMetadata emptyMetadata = new OffsetAndMetadata(5, string.Empty, 3);
        Assert.True(nullMetadata.Equals(emptyMetadata));
        Assert.Equal(nullMetadata.GetHashCode(), emptyMetadata.GetHashCode());

        // Each field on its own makes the two unequal.
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

        // The hash is stable for one instance.
        Assert.Equal(baseline.GetHashCode(), baseline.GetHashCode());

        // Reflection pins the shape: IEquatable<T>, a typed Equals, and no operators (the
        // MemberToRemove / RecordsToDelete precedent; Java has none either).
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
