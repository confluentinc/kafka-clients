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

using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Copies a whole <c>kafka_admin_DescribeFeaturesResult_t</c> out into the owned
/// <see cref="FeatureMetadata"/> composite — the <c>describeCluster</c> snapshot shape, for a
/// result with two tables and a scalar.
/// </summary>
/// <remarks>
/// ⚠⚠ <b>The two tables are NOT co-indexed</b> — "the two maps can differ in both size and
/// contents" (<c>confluent_kafka.h:9682-9684</c>) — so each is walked by <b>its own</b> count.
/// Driving the supported walk from <c>finalized_count</c> compiles, reads naturally, and both
/// truncates and over-reads depending on which table is longer.
/// </remarks>
internal static class FeatureMetadataMarshal
{
    /// <summary>The production <c>kafka_admin_DescribeFeaturesResult_*</c> set.</summary>
    internal static readonly Accessors NativeAccessors = new Accessors(
        NativeMethods.DescribeFeaturesResultFinalizedCount,
        NativeMethods.DescribeFeaturesResultGetFinalizedFeature,
        NativeMethods.DescribeFeaturesResultGetFinalizedMinVersionLevel,
        NativeMethods.DescribeFeaturesResultGetFinalizedMaxVersionLevel,
        NativeMethods.DescribeFeaturesResultSupportedCount,
        NativeMethods.DescribeFeaturesResultGetSupportedFeature,
        NativeMethods.DescribeFeaturesResultGetSupportedMinVersion,
        NativeMethods.DescribeFeaturesResultGetSupportedMaxVersion,
        NativeMethods.DescribeFeaturesResultFinalizedFeaturesEpoch);

    /// <summary>Copies the whole root out before the trampoline destroys it.</summary>
    /// <param name="result">The owned result root.</param>
    /// <returns>The owned metadata.</returns>
    internal static FeatureMetadata CopyOut(IntPtr result) => CopyOut(result, NativeAccessors);

    /// <summary>Copies the whole root out through an injected accessor set.</summary>
    /// <remarks>
    /// The set is a parameter because the mock derives both tables from one seeded key set, so
    /// the ABI cannot produce the differing sizes and key sets this walk exists to handle.
    /// </remarks>
    /// <param name="result">The result root, or a stand-in under an injected set.</param>
    /// <param name="accessors">The nine accessors to decode it with.</param>
    /// <returns>The owned metadata.</returns>
    internal static FeatureMetadata CopyOut(IntPtr result, Accessors accessors)
    {
        int finalizedCount = accessors.FinalizedCount(result);
        Dictionary<string, FinalizedVersionRange> finalized =
            new Dictionary<string, FinalizedVersionRange>(Math.Max(finalizedCount, 0), StringComparer.Ordinal);
        for (int i = 0; i < finalizedCount; i++)
        {
            finalized.Add(
                ReadFeatureName(accessors.GetFinalizedFeature(result, i)),
                new FinalizedVersionRange(
                    accessors.GetFinalizedMinVersionLevel(result, i),
                    accessors.GetFinalizedMaxVersionLevel(result, i)));
        }

        // ⚠ Its own count — never finalizedCount. See the type remarks.
        int supportedCount = accessors.SupportedCount(result);
        Dictionary<string, SupportedVersionRange> supported =
            new Dictionary<string, SupportedVersionRange>(Math.Max(supportedCount, 0), StringComparer.Ordinal);
        for (int i = 0; i < supportedCount; i++)
        {
            supported.Add(
                ReadFeatureName(accessors.GetSupportedFeature(result, i)),
                new SupportedVersionRange(
                    accessors.GetSupportedMinVersion(result, i),
                    accessors.GetSupportedMaxVersion(result, i)));
        }

        // Presence is the return value, never a sentinel: every long is a legal epoch.
        long? epoch = accessors.FinalizedFeaturesEpoch(result, out long value) ? value : null;

        return new FeatureMetadata(finalized, epoch, supported);
    }

    /// <summary>Reads a <c>short</c> version bound indexed within one of the two tables.</summary>
    /// <param name="result">The result root.</param>
    /// <param name="index">The index, inside that table's own count.</param>
    /// <returns>The bound; <c>-1</c> when the index is out of range.</returns>
    internal delegate short VersionAccessor(IntPtr result, int index);

    /// <summary>Reads the finalized-features epoch, reporting presence as the return.</summary>
    /// <param name="result">The result root.</param>
    /// <param name="epoch">The epoch, when present.</param>
    /// <returns><c>true</c> when an epoch is present.</returns>
    internal delegate bool EpochAccessor(IntPtr result, out long epoch);

    /// <summary>The nine result accessors, as one set.</summary>
    internal sealed class Accessors
    {
        /// <summary>Creates a set, in the ABI's own accessor order.</summary>
        /// <param name="finalizedCount">The finalized table's own bound.</param>
        /// <param name="getFinalizedFeature">One finalized feature's name.</param>
        /// <param name="getFinalizedMinVersionLevel">One finalized range's minimum.</param>
        /// <param name="getFinalizedMaxVersionLevel">One finalized range's maximum.</param>
        /// <param name="supportedCount">The supported table's own bound.</param>
        /// <param name="getSupportedFeature">One supported feature's name.</param>
        /// <param name="getSupportedMinVersion">One supported range's minimum.</param>
        /// <param name="getSupportedMaxVersion">One supported range's maximum.</param>
        /// <param name="finalizedFeaturesEpoch">The epoch, with presence as the return.</param>
        internal Accessors(
            KeyedResultMarshal.CountAccessor finalizedCount,
            KeyedResultMarshal.IndexedAccessor getFinalizedFeature,
            VersionAccessor getFinalizedMinVersionLevel,
            VersionAccessor getFinalizedMaxVersionLevel,
            KeyedResultMarshal.CountAccessor supportedCount,
            KeyedResultMarshal.IndexedAccessor getSupportedFeature,
            VersionAccessor getSupportedMinVersion,
            VersionAccessor getSupportedMaxVersion,
            EpochAccessor finalizedFeaturesEpoch)
        {
            FinalizedCount = finalizedCount;
            GetFinalizedFeature = getFinalizedFeature;
            GetFinalizedMinVersionLevel = getFinalizedMinVersionLevel;
            GetFinalizedMaxVersionLevel = getFinalizedMaxVersionLevel;
            SupportedCount = supportedCount;
            GetSupportedFeature = getSupportedFeature;
            GetSupportedMinVersion = getSupportedMinVersion;
            GetSupportedMaxVersion = getSupportedMaxVersion;
            FinalizedFeaturesEpoch = finalizedFeaturesEpoch;
        }

        /// <summary><c>finalized_count()</c> — the finalized walk's bound.</summary>
        internal KeyedResultMarshal.CountAccessor FinalizedCount { get; }

        /// <summary><c>get_finalized_feature(i)</c>.</summary>
        internal KeyedResultMarshal.IndexedAccessor GetFinalizedFeature { get; }

        /// <summary><c>get_finalized_min_version_level(i)</c>.</summary>
        internal VersionAccessor GetFinalizedMinVersionLevel { get; }

        /// <summary><c>get_finalized_max_version_level(i)</c>.</summary>
        internal VersionAccessor GetFinalizedMaxVersionLevel { get; }

        /// <summary><c>supported_count()</c> — the supported walk's own bound.</summary>
        internal KeyedResultMarshal.CountAccessor SupportedCount { get; }

        /// <summary><c>get_supported_feature(i)</c>.</summary>
        internal KeyedResultMarshal.IndexedAccessor GetSupportedFeature { get; }

        /// <summary><c>get_supported_min_version(i)</c>.</summary>
        internal VersionAccessor GetSupportedMinVersion { get; }

        /// <summary><c>get_supported_max_version(i)</c>.</summary>
        internal VersionAccessor GetSupportedMaxVersion { get; }

        /// <summary><c>finalized_features_epoch(&amp;epoch)</c>.</summary>
        internal EpochAccessor FinalizedFeaturesEpoch { get; }
    }

    private static string ReadFeatureName(IntPtr name) =>
        Utf8Marshal.PtrToString(name)
        ?? throw new KafkaException(
            "The describeFeatures result produced no feature name for an index within its own count.");
}
