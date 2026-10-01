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
