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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The cluster's finalized and supported features — Java's
/// <c>org.apache.kafka.clients.admin.FeatureMetadata</c>.
/// </summary>
/// <remarks>
/// ⚠ The two maps are <b>independent</b>: they can differ in both size and key set
/// (<c>confluent_kafka.h:9682-9684</c>). Java's constructor is package-private (<c>:38</c>), so
/// this one is <c>internal</c>.
/// </remarks>
public sealed class FeatureMetadata
{
    private readonly IReadOnlyDictionary<string, FinalizedVersionRange> _finalizedFeatures;
    private readonly IReadOnlyDictionary<string, SupportedVersionRange> _supportedFeatures;

    internal FeatureMetadata(
        IReadOnlyDictionary<string, FinalizedVersionRange> finalizedFeatures,
        long? finalizedFeaturesEpoch,
        IReadOnlyDictionary<string, SupportedVersionRange> supportedFeatures)
    {
        _finalizedFeatures = finalizedFeatures;
        FinalizedFeaturesEpoch = finalizedFeaturesEpoch;
        _supportedFeatures = supportedFeatures;
    }

    /// <summary>
    /// The finalized version level of each feature — Java's <c>finalizedFeatures()</c>
    /// (<c>:51</c>).
    /// </summary>
    public IReadOnlyDictionary<string, FinalizedVersionRange> FinalizedFeatures => _finalizedFeatures;

    /// <summary>
    /// The finalized-features epoch, or <see langword="null"/> when the finalized features are
    /// unavailable — Java's <c>finalizedFeaturesEpoch()</c> (<c>:59</c>), an
    /// <c>Optional&lt;Long&gt;</c>.
    /// </summary>
    /// <remarks>
    /// ⚠ Absence is carried by the ABI's <c>bool</c> return, never by a sentinel: every
    /// <c>long</c>, <c>0</c> and <c>-1</c> included, is a legal epoch.
    /// </remarks>
    public long? FinalizedFeaturesEpoch { get; }

    /// <summary>
    /// The versions of each feature the brokers support — Java's <c>supportedFeatures()</c>
    /// (<c>:68</c>).
    /// </summary>
    public IReadOnlyDictionary<string, SupportedVersionRange> SupportedFeatures => _supportedFeatures;

    /// <summary>Value equality over all three members — Java's <c>equals</c> (<c>:73</c>).</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same feature metadata.</returns>
    public override bool Equals(object? obj) =>
        obj is FeatureMetadata other
        && FinalizedFeaturesEpoch == other.FinalizedFeaturesEpoch
        && MapEquals(_finalizedFeatures, other._finalizedFeatures)
        && MapEquals(_supportedFeatures, other._supportedFeatures);

    /// <summary>The hash of all three members — Java's <c>hashCode</c> (<c>:88</c>).</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            // Order-independent, because the underlying maps are unordered.
            int hash = FinalizedFeaturesEpoch.GetHashCode();
            foreach (KeyValuePair<string, FinalizedVersionRange> entry in _finalizedFeatures)
            {
                hash ^= StringComparer.Ordinal.GetHashCode(entry.Key) * 31 + entry.Value.GetHashCode();
            }

            foreach (KeyValuePair<string, SupportedVersionRange> entry in _supportedFeatures)
            {
                hash ^= StringComparer.Ordinal.GetHashCode(entry.Key) * 37 + entry.Value.GetHashCode();
            }

            return hash;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:104</c>).</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "FeatureMetadata{{finalizedFeatures:{0}, finalizedFeaturesEpoch:{1}, supportedFeatures:{2}}}",
            MapToString(_finalizedFeatures),
            FinalizedFeaturesEpoch?.ToString(CultureInfo.InvariantCulture) ?? "<none>",
            MapToString(_supportedFeatures));

    private static bool MapEquals<TValue>(
        IReadOnlyDictionary<string, TValue> left, IReadOnlyDictionary<string, TValue> right)
        where TValue : class =>
        left.Count == right.Count
        && left.All(entry =>
            right.TryGetValue(entry.Key, out TValue? value) && entry.Value.Equals(value));

    private static string MapToString<TValue>(IReadOnlyDictionary<string, TValue> features) =>
        string.Format(
            CultureInfo.InvariantCulture,
            "{{{0}}}",
            string.Join(
                ", ",
                features.Select(entry =>
                    string.Format(CultureInfo.InvariantCulture, "({0} -> {1})", entry.Key, entry.Value))));
}
