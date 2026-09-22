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
/// One feature's requested version level — Java's
/// <c>org.apache.kafka.clients.admin.FeatureUpdate</c>.
/// </summary>
public sealed class FeatureUpdate
{
    /// <summary>
    /// What kind of change the update performs — Java's nested <c>FeatureUpdate.UpgradeType</c>
    /// (<c>:28</c>).
    /// </summary>
    public enum UpgradeType : byte
    {
        /// <summary>Unrecognized — Java's <c>UNKNOWN</c> (<c>:29</c>).</summary>
        Unknown = 0,

        /// <summary>Raise the feature level — Java's <c>UPGRADE</c> (<c>:30</c>).</summary>
        Upgrade = 1,

        /// <summary>
        /// Lower the level, but only where no metadata is lost — Java's <c>SAFE_DOWNGRADE</c>
        /// (<c>:31</c>).
        /// </summary>
        SafeDowngrade = 2,

        /// <summary>
        /// Lower the level, allowing metadata loss — Java's <c>UNSAFE_DOWNGRADE</c>
        /// (<c>:32</c>).
        /// </summary>
        UnsafeDowngrade = 3,
    }

    /// <summary>Creates an update — Java's <c>:67</c>.</summary>
    /// <param name="maxVersionLevel">
    /// The new maximum version level. <c>0</c> deletes the finalized feature and must be
    /// accompanied by a downgrade type.
    /// </param>
    /// <param name="upgradeType">The kind of change.</param>
    /// <exception cref="ArgumentException">
    /// <paramref name="maxVersionLevel"/> is <c>0</c> with
    /// <see cref="UpgradeType.Upgrade"/> (<c>:68</c>), or is negative (<c>:73</c>).
    /// </exception>
    public FeatureUpdate(short maxVersionLevel, UpgradeType upgradeType)
    {
        if (maxVersionLevel == 0 && upgradeType == UpgradeType.Upgrade)
        {
            throw new ArgumentException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "The upgradeType flag should be set to SAFE_DOWNGRADE or UNSAFE_DOWNGRADE"
                    + " when the provided maxVersionLevel:{0} is < 1.",
                    maxVersionLevel));
        }

        if (maxVersionLevel < 0)
        {
            throw new ArgumentException("Cannot specify a negative version level.");
        }

        MaxVersionLevel = maxVersionLevel;
        Type = upgradeType;
    }

    /// <summary>
    /// The new maximum version level — Java's <c>maxVersionLevel()</c> (<c>:80</c>).
    /// </summary>
    public short MaxVersionLevel { get; }

    /// <summary>
    /// The kind of change — Java's <c>upgradeType()</c> (<c>:84</c>). Named
    /// <see cref="Type"/> because C# forbids a member with the same name as the nested
    /// <see cref="UpgradeType"/> it returns.
    /// </summary>
    public UpgradeType Type { get; }

    /// <summary>Value equality over both fields — Java's <c>equals</c> (<c>:89</c>).</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same update.</returns>
    public override bool Equals(object? obj) =>
        obj is FeatureUpdate other && MaxVersionLevel == other.MaxVersionLevel && Type == other.Type;

    /// <summary>The hash of both fields — Java's <c>hashCode</c> (<c>:103</c>).</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            return (((int)MaxVersionLevel * 31) + 1) * 31 + (int)Type;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:108</c>).</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "FeatureUpdate{{maxVersionLevel:{0}, upgradeType:{1}}}",
            MaxVersionLevel,
            Type);
}

/// <summary>
/// The static member Java declares on <see cref="FeatureUpdate.UpgradeType"/> itself, which a
/// C# enum cannot carry — host-language scaffolding, not new Kafka surface. The <c>code()</c>
/// accessor (<c>FeatureUpdate.java:40</c>) needs none: a cast yields it.
/// </summary>
public static class UpgradeTypes
{
    /// <summary>
    /// The upgrade type for a code — Java's <c>UpgradeType.fromCode(int)</c>
    /// (<c>FeatureUpdate.java:44</c>). Falls back to
    /// <see cref="FeatureUpdate.UpgradeType.Unknown"/>; never throws.
    /// </summary>
    /// <param name="code">The code.</param>
    /// <returns>The upgrade type.</returns>
    public static FeatureUpdate.UpgradeType FromCode(int code) =>
        code switch
        {
            1 => FeatureUpdate.UpgradeType.Upgrade,
            2 => FeatureUpdate.UpgradeType.SafeDowngrade,
            3 => FeatureUpdate.UpgradeType.UnsafeDowngrade,
            _ => FeatureUpdate.UpgradeType.Unknown,
        };
}
