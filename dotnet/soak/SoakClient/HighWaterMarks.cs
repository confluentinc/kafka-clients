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
/// Per-partition high-water marks, as Python's soak <c>HighWaterMarks</c>: an offset at or below
/// the mark counts duplicates, a jump past it counts missed records, and the mark always moves to
/// the observed offset, so a rebalance replay is counted once.
/// </summary>
internal sealed class HighWaterMarks
{
    private readonly Dictionary<string, long> _marks = new Dictionary<string, long>();

    internal int Count => _marks.Count;

    // As Python: a mark of 0 also means "never seen", so the step after offset 0 is not checked;
    // HighWaterMarksTests.OffsetZeroBlindSpotIsAKnownLimitation pins it.
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

    internal IReadOnlyDictionary<string, long> Marks() => new Dictionary<string, long>(_marks);
}
