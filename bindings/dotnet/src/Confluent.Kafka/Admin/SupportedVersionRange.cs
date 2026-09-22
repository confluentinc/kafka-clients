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
/// The versions of one feature a broker supports — Java's
/// <c>org.apache.kafka.clients.admin.SupportedVersionRange</c>.
/// </summary>
/// <remarks>
/// ⚠ Member names differ from <see cref="FinalizedVersionRange"/>'s — Java's asymmetry,
/// preserved.
/// </remarks>
public sealed class SupportedVersionRange
{
    /// <summary>Creates a supported range — Java's <c>:38</c>.</summary>
    /// <param name="minVersion">The minimum supported version.</param>
    /// <param name="maxVersion">The maximum supported version.</param>
    /// <exception cref="ArgumentException">
    /// Either version is negative, or the maximum is below the minimum (<c>:39</c>).
    /// </exception>
    public SupportedVersionRange(short minVersion, short maxVersion)
    {
        if (minVersion < 0 || maxVersion < 0 || maxVersion < minVersion)
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Expected 0 <= minVersion <= maxVersion but received minVersion:{0}, maxVersion:{1}.",
                    minVersion,
                    maxVersion));
        }

        MinVersion = minVersion;
        MaxVersion = maxVersion;
    }

    /// <summary>The minimum version — Java's <c>minVersion()</c> (<c>:50</c>).</summary>
    public short MinVersion { get; }

    /// <summary>The maximum version — Java's <c>maxVersion()</c> (<c>:54</c>).</summary>
    public short MaxVersion { get; }

    /// <summary>Value equality over both versions — Java's <c>equals</c> (<c>:59</c>).</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same range.</returns>
    public override bool Equals(object? obj) =>
        obj is SupportedVersionRange other
        && MinVersion == other.MinVersion
        && MaxVersion == other.MaxVersion;

    /// <summary>The hash of both versions — Java's <c>hashCode</c> (<c>:73</c>).</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            return (((int)MinVersion * 31) + 1) * 31 + MaxVersion;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:78</c>).</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "SupportedVersionRange[min_version:{0}, max_version:{1}]",
            MinVersion,
            MaxVersion);
}
