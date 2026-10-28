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
/// Copies the <c>kafka_admin_FeatureMetadata_t</c> a <c>describeFeatures</c> future delivers into the owned
/// <see cref="FeatureMetadata"/>.
/// </summary>
internal static class FeatureMetadataMarshal
{
    internal static FeatureMetadata CopyOut(IntPtr metadata)
    {
        if (metadata == IntPtr.Zero)
        {
            throw new KafkaException("The describeFeatures result produced no feature metadata.");
        }

        IntPtr outErr = IntPtr.Zero;
        IntPtr finalizedMap = NativeMethods.FeatureMetadataFinalizedFeatures(metadata, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Dictionary<string, FinalizedVersionRange> finalized;
        try
        {
            finalized = CopyOutFinalized(finalizedMap);
        }
        finally
        {
            DestroyMap(finalizedMap);
        }

        IntPtr supportedMap = NativeMethods.FeatureMetadataSupportedFeatures(metadata, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Dictionary<string, SupportedVersionRange> supported;
        try
        {
            supported = CopyOutSupported(supportedMap);
        }
        finally
        {
            DestroyMap(supportedMap);
        }

        long epoch = NativeMethods.FeatureMetadataFinalizedFeaturesEpoch(metadata, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new FeatureMetadata(finalized, ToEpoch(epoch), supported);
    }

    // The core reports no epoch as -1, and Java reads any negative epoch as an empty Optional.
    internal static long? ToEpoch(long epoch) => epoch < 0 ? null : epoch;

    internal static Dictionary<string, FinalizedVersionRange> CopyOutFinalized(IntPtr map)
    {
        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.MapSize(map, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Dictionary<string, FinalizedVersionRange> copied =
            new Dictionary<string, FinalizedVersionRange>(Math.Max(count, 0), StringComparer.Ordinal);
        for (int i = 0; i < count; i++)
        {
            string name = ReadName(map, i);
            IntPtr range = NativeMethods.MapValue(map, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            short min = NativeMethods.FinalizedVersionRangeMinVersionLevel(range, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            short max = NativeMethods.FinalizedVersionRangeMaxVersionLevel(range, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            copied[name] = new FinalizedVersionRange(min, max);
        }

        return copied;
    }

    internal static Dictionary<string, SupportedVersionRange> CopyOutSupported(IntPtr map)
    {
        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.MapSize(map, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Dictionary<string, SupportedVersionRange> copied =
            new Dictionary<string, SupportedVersionRange>(Math.Max(count, 0), StringComparer.Ordinal);
        for (int i = 0; i < count; i++)
        {
            string name = ReadName(map, i);
            IntPtr range = NativeMethods.MapValue(map, i, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            short min = NativeMethods.SupportedVersionRangeMinVersion(range, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            short max = NativeMethods.SupportedVersionRangeMaxVersion(range, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            copied[name] = new SupportedVersionRange(min, max);
        }

        return copied;
    }

    private static string ReadName(IntPtr map, int index)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr name = NativeMethods.MapKey(map, index, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return Utf8Marshal.PtrToString(name)
            ?? throw new KafkaException("The describeFeatures result produced a feature with no name.");
    }

    private static void DestroyMap(IntPtr map)
    {
        if (map == IntPtr.Zero)
        {
            return;
        }

        IntPtr outErr = IntPtr.Zero;
        NativeMethods.MapDestroy(map, ref outErr);
        if (outErr != IntPtr.Zero)
        {
            NativeMethods.ErrorDestroy(outErr);
        }
    }
}
