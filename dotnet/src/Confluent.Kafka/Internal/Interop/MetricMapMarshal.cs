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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Marshals an owned (Category-3) <c>MetricMap_t</c> handle into a public
/// <see cref="IReadOnlyDictionary{TKey, TValue}"/> of <see cref="MetricName"/> →
/// <see cref="IMetric"/> and then destroys it (ffi-marshalling.md §B2/§B3). The
/// symmetric sibling of <see cref="ConsumerGroupMetadataMarshal"/> for the
/// <c>metrics()</c> getter — it keeps the owned-handle read-then-destroy discipline out
/// of <c>NativeConsumer</c> and needs no <c>unsafe</c> (the NUL-terminated
/// <see cref="Utf8Marshal.PtrToString(IntPtr)"/> reads are safe managed API).
/// </summary>
internal static class MetricMapMarshal
{
    /// <summary>
    /// Copies every metric entry out of the owned <paramref name="map"/> handle and frees
    /// it exactly once in a <c>finally</c> (even if a read throws). Every borrowed
    /// (name / group / description / tag / string-value) pointer is copied out
    /// <b>before</b> the destroy — they die with the handle (§B3). The result dictionary
    /// keys on <see cref="MetricName"/> value identity (built via the indexer: the core's
    /// <c>HashMap</c> guarantees unique names, but the indexer is used defensively so a
    /// duplicate key would overwrite rather than throw, PLAN D1).
    /// </summary>
    /// <param name="map">The owned, non-null metric-map handle.</param>
    internal static IReadOnlyDictionary<MetricName, IMetric> CopyOutAndDestroy(IntPtr map)
    {
        try
        {
            int count = NativeMethods.MetricMapCount(map);
            Dictionary<MetricName, IMetric> result = new Dictionary<MetricName, IMetric>(count < 0 ? 0 : count);

            for (int i = 0; i < count; i++)
            {
                // Copy the three NUL-terminated identity strings out of the batch now
                // (borrowed until destroy). name/group are non-null for a valid index;
                // coerce a defensive null to string.Empty so the MetricName ctor (which
                // requires non-null) never trips on a well-formed map.
                string name = Utf8Marshal.PtrToString(NativeMethods.MetricMapGetName(map, i)) ?? string.Empty;
                string group = Utf8Marshal.PtrToString(NativeMethods.MetricMapGetGroup(map, i)) ?? string.Empty;
                string description = Utf8Marshal.PtrToString(NativeMethods.MetricMapGetDescription(map, i)) ?? string.Empty;

                Dictionary<string, string> tags = ReadTags(map, i);
                object value = ReadValue(map, i);

                MetricName metricName = new MetricName(name, group, description, tags);
                result[metricName] = new Metric(metricName, value);
            }

            return result;
        }
        finally
        {
            // Owned Category-3 handle — free it exactly once after reading (ffi §B2).
            NativeMethods.MetricMapDestroy(map);
        }
    }

    // Reads the tag_count key/value pairs of entry `index` into an ordinal-keyed dict.
    // The keys/values are borrowed const char* copied out here (before destroy, §B3).
    private static Dictionary<string, string> ReadTags(IntPtr map, int index)
    {
        int tagCount = NativeMethods.MetricMapGetTagCount(map, index);
        Dictionary<string, string> tags = new Dictionary<string, string>(
            tagCount < 0 ? 0 : tagCount,
            StringComparer.Ordinal);

        for (int t = 0; t < tagCount; t++)
        {
            string key = Utf8Marshal.PtrToString(NativeMethods.MetricMapGetTagKey(map, index, t)) ?? string.Empty;
            string value = Utf8Marshal.PtrToString(NativeMethods.MetricMapGetTagValue(map, index, t)) ?? string.Empty;
            tags[key] = value;
        }

        return tags;
    }

    // Reads entry `index`'s value with the accessor selected by its value-kind, boxing it
    // as the matching CLR type (the boxed type IS the kind — PLAN D2). The string value is
    // a borrowed const char* copied out here (before destroy, §B3).
    private static object ReadValue(IntPtr map, int index)
    {
        int kind = NativeMethods.MetricMapGetValueKind(map, index);
        switch (kind)
        {
            case NativeMethods.MetricValueKindString:
                return Utf8Marshal.PtrToString(NativeMethods.MetricMapGetValueString(map, index)) ?? string.Empty;
            case NativeMethods.MetricValueKindLong:
                return NativeMethods.MetricMapGetValueLong(map, index);
            case NativeMethods.MetricValueKindInt:
                return NativeMethods.MetricMapGetValueInt(map, index);
            case NativeMethods.MetricValueKindDouble:
            default:
                // DOUBLE is also the ABI's documented out-of-range default.
                return NativeMethods.MetricMapGetValueDouble(map, index);
        }
    }
}
