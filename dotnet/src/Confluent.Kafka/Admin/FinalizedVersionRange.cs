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

namespace Confluent.Kafka.Admin;

/// <summary>
/// The finalized version levels of one feature — Java's
/// <c>org.apache.kafka.clients.admin.FinalizedVersionRange</c>.
/// </summary>
/// <remarks>
/// ⚠ Member names differ from <see cref="SupportedVersionRange"/>'s (<c>MinVersionLevel</c> vs
/// <c>MinVersion</c>); that asymmetry is Java's and is preserved.
/// </remarks>
public sealed class FinalizedVersionRange
{
    /// <summary>Creates a finalized range — Java's <c>:38</c>.</summary>
    /// <param name="minVersionLevel">The minimum finalized version level.</param>
    /// <param name="maxVersionLevel">The maximum finalized version level.</param>
    /// <exception cref="ArgumentException">
    /// Either level is negative, or the maximum is below the minimum. ⚠ The bound is
    /// <c>&gt;= 0</c>: Java's <b>code</b> (<c>:39</c>) enforces that, though its javadoc
    /// (<c>:31</c>) says <c>&gt;= 1</c>.
    /// </exception>
    public FinalizedVersionRange(short minVersionLevel, short maxVersionLevel)
    {
        if (minVersionLevel < 0 || maxVersionLevel < 0 || maxVersionLevel < minVersionLevel)
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Expected minVersionLevel >= 0, maxVersionLevel >= 0 and"
                    + " maxVersionLevel >= minVersionLevel, but received"
                    + " minVersionLevel: {0}, maxVersionLevel: {1}",
                    minVersionLevel,
                    maxVersionLevel));
        }

        MinVersionLevel = minVersionLevel;
        MaxVersionLevel = maxVersionLevel;
    }

    /// <summary>The minimum level — Java's <c>minVersionLevel()</c> (<c>:50</c>).</summary>
    public short MinVersionLevel { get; }

    /// <summary>The maximum level — Java's <c>maxVersionLevel()</c> (<c>:54</c>).</summary>
    public short MaxVersionLevel { get; }

    /// <summary>Value equality over both levels — Java's <c>equals</c> (<c>:59</c>).</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same range.</returns>
    public override bool Equals(object? obj) =>
        obj is FinalizedVersionRange other
        && MinVersionLevel == other.MinVersionLevel
        && MaxVersionLevel == other.MaxVersionLevel;

    /// <summary>The hash of both levels — Java's <c>hashCode</c> (<c>:73</c>).</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            return (((int)MinVersionLevel * 31) + 1) * 31 + MaxVersionLevel;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:78</c>).</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "FinalizedVersionRange[min_version_level:{0}, max_version_level:{1}]",
            MinVersionLevel,
            MaxVersionLevel);
}
