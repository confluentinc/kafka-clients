// Copyright 2026 Confluent Inc.
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

using System.Collections.Generic;

namespace Confluent.Kafka.Soak;

/// <summary>
/// Per-partition high-water-mark bookkeeping — a direct port of
/// <c>soakclient.py</c>'s <c>HighWaterMarks</c>, which is itself the part of the
/// reference librdkafka soak's consumer loop worth keeping verbatim:
/// <list type="bullet">
///   <item><description><c>offset &lt;= hw</c> → duplicate, <c>(hw + 1) - offset</c> messages</description></item>
///   <item><description><c>offset &gt; hw + 1</c> → <b>loss</b>, <c>offset - (hw + 1)</c> messages</description></item>
///   <item><description>the first offset seen on a partition establishes the mark and is never
///     counted (Java and librdkafka both start consuming at an arbitrary committed
///     position, which is not a gap)</description></item>
/// </list>
/// <para>
/// The mark is set to the observed offset <b>unconditionally</b> — it is NOT
/// advance-only. That matters: when a rebalance replays a partition from an older
/// offset, the single jump-back reports exactly the number of records about to be
/// redelivered, and the replayed records that follow are then in order and counted once.
/// </para>
/// <para>
/// ⚠ KNOWN LIMITATION, inherited from the reference and deliberately preserved: the
/// marks map defaults to <c>0</c>, so "never seen" and "last seen at offset 0" are the
/// same state, and the <c>hw &gt; 0</c> guard therefore skips the check for exactly one
/// transition per partition. A duplicate of offset 0, or a gap immediately after it, is
/// not counted. The blast radius is ~2 records, at the start of the first run against a
/// fresh topic only (every later restart begins at a committed non-zero offset). The
/// correct fix is a <c>-1</c> sentinel; it is NOT applied here, and
/// <c>HighWaterMarksTests.OffsetZeroBlindSpotIsAKnownLimitation</c> pins the current
/// behaviour so that changing the sentinel fails loudly rather than silently.
/// </para>
/// </summary>
internal sealed class HighWaterMarks
{
    private readonly Dictionary<string, long> _marks = new Dictionary<string, long>();

    /// <summary>Number of partitions with an established mark.</summary>
    internal int Count => _marks.Count;

    /// <summary>
    /// Returns the <c>(duplicates, missed)</c> implied by seeing <paramref name="offset"/>
    /// on <paramref name="key"/>, and advances the mark to it.
    /// </summary>
    internal (long Duplicates, long Missed) Observe(string key, long offset)
    {
        _marks.TryGetValue(key, out long hw);

        long duplicates = 0;
        long missed = 0;
        if (hw > 0)
        {
            if (offset <= hw)
            {
                duplicates = (hw + 1) - offset;
            }
            else if (offset > hw + 1)
            {
                missed = offset - (hw + 1);
            }
        }

        _marks[key] = offset;
        return (duplicates, missed);
    }

    /// <summary>A snapshot of the current marks, keyed as <c>topic-partition</c>.</summary>
    internal IReadOnlyDictionary<string, long> Marks() => new Dictionary<string, long>(_marks);
}
