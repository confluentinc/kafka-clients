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
/// Direct unit tests for the offset-map copy-out marshaller machinery (M5/P4, PLAN §7 case
/// 6/7) — the pure-managed pieces that are reachable without a native container. The
/// <b>non-empty</b> <c>OffsetMap_t</c> / <c>OffsetAndTimestampMap_t</c> copy-out (the
/// <see cref="OffsetAndMetadata"/> / <see cref="OffsetAndTimestamp"/> value copy-out incl.
/// the leader-epoch presence flag through the borrowed element) is <b>not reachable
/// broker-free</b>: the ABI exposes no container constructor, the mock's <c>committed</c> map
/// is empty (the commit-with-offsets family is not yet wired) and its <c>offsets_for_times</c>
/// always errors — so a non-empty container of those two types cannot be produced this phase.
/// That path is <b>deferred, not skipped</b> (documented in COMMENTS.DONE.13), and the
/// <c>LongOffsetMap_t</c> copy-out IS proven end-to-end by
/// <c>PublicConsumerOffsetQueryTests</c> (via <c>BeginningOffsets</c> / <c>EndOffsets</c>).
/// What is directly testable here: the shared leader-epoch presence-flag decode
/// (<see cref="OffsetMapMarshalShared.ReadLeaderEpoch"/>), the empty-container path of each
/// marshaller, and the shared empty read-only dictionary.
/// </summary>
public sealed class OffsetMapMarshalTests
{
    // ---- Leader-epoch presence flag → int? (PLAN §1 Critic check / §7 case 7) ----

    [Fact]
    public void ReadLeaderEpoch_Present_ReturnsEpoch()
    {
        // present=true (the ABI bool return) → the out epoch is honored, NOT dropped.
        int? epoch = OffsetMapMarshalShared.ReadLeaderEpoch(present: true, epoch: 17);

        Assert.True(epoch.HasValue);
        Assert.Equal(17, epoch!.Value);
    }

    [Fact]
    public void ReadLeaderEpoch_Absent_ReturnsNull()
    {
        // present=false → null, REGARDLESS of the out-value: the presence flag governs,
        // the epoch is not hardcoded (the finding PLAN §1 asks the Critic to guard).
        int? epoch = OffsetMapMarshalShared.ReadLeaderEpoch(present: false, epoch: 999);

        Assert.False(epoch.HasValue);
    }

    // NOTE — the empty (valid, non-null) container copy-out path is exercised END-TO-END,
    // not unit-tested here: the container _count / _get accessors are NOT null-safe per the
    // ABI contract ("map must be a valid handle"), so CopyOut(IntPtr.Zero) would be a null
    // deref (only _destroy is null-safe). A valid EMPTY OffsetMap_t is produced by the mock's
    // Committed({uncommitted}) path (PublicConsumerOffsetQueryTests.
    // Committed_UncommittedPartition_ReturnsEmptyMap → an empty, non-null dictionary), and the
    // valid non-empty LongOffsetMap_t copy-out by BeginningOffsets / EndOffsets there. The
    // production trampolines only ever call CopyOut on the SUCCESS branch (map non-null); the
    // failure branch has map == null and calls only the null-safe *Destroy.

    // ---- Shared empty read-only dictionary ----

    [Fact]
    public void EmptyReadOnlyDictionary_IsEmptyAndSharedInstance()
    {
        EmptyReadOnlyDictionary<TopicPartition, OffsetAndMetadata> a =
            EmptyReadOnlyDictionary<TopicPartition, OffsetAndMetadata>.Instance;
        EmptyReadOnlyDictionary<TopicPartition, OffsetAndMetadata> b =
            EmptyReadOnlyDictionary<TopicPartition, OffsetAndMetadata>.Instance;

        Assert.Same(a, b); // shared instance (Array.Empty analog — no per-empty allocation)
        Assert.Empty(a);
        Assert.Empty(a.Keys);
        Assert.Empty(a.Values);
        Assert.False(a.ContainsKey(new TopicPartition("t", 0)));
        Assert.False(a.TryGetValue(new TopicPartition("t", 0), out _));
    }
}
