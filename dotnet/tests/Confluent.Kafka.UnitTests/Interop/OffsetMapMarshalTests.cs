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

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The offset-map marshallers' managed pieces: the leader-epoch presence flag and the shared empty read-only
/// dictionary.
/// </summary>
public sealed class OffsetMapMarshalTests
{
    [Fact]
    public void ReadLeaderEpoch_Present_ReturnsEpoch()
    {
        int? epoch = OffsetMapMarshalShared.ReadLeaderEpoch(present: true, epoch: 17);

        Assert.True(epoch.HasValue);
        Assert.Equal(17, epoch!.Value);
    }

    [Fact]
    public void ReadLeaderEpoch_Absent_ReturnsNull()
    {
        int? epoch = OffsetMapMarshalShared.ReadLeaderEpoch(present: false, epoch: 999);

        Assert.False(epoch.HasValue);
    }

    [Fact]
    public void EmptyReadOnlyDictionary_IsEmptyAndSharedInstance()
    {
        EmptyReadOnlyDictionary<TopicPartition, OffsetAndMetadata> a =
            EmptyReadOnlyDictionary<TopicPartition, OffsetAndMetadata>.Instance;
        EmptyReadOnlyDictionary<TopicPartition, OffsetAndMetadata> b =
            EmptyReadOnlyDictionary<TopicPartition, OffsetAndMetadata>.Instance;

        Assert.Same(a, b);
        Assert.Empty(a);
        Assert.Empty(a.Keys);
        Assert.Empty(a.Values);
        Assert.False(a.ContainsKey(new TopicPartition("t", 0)));
        Assert.False(a.TryGetValue(new TopicPartition("t", 0), out _));
    }
}
