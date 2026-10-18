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
/// Copies an owned <c>kafka_consumer_MetricMap_t</c> into a dictionary of <see cref="MetricName"/> to
/// <see cref="IMetric"/>, then destroys it.
/// </summary>
internal static class MetricMapMarshal
{
    internal static IReadOnlyDictionary<MetricName, IMetric> CopyOutAndDestroy(IntPtr map)
    {
        try
        {
            int count = NativeMethods.MetricMapCount(map);
            Dictionary<MetricName, IMetric> result = new Dictionary<MetricName, IMetric>(count < 0 ? 0 : count);

            for (int i = 0; i < count; i++)
            {
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
            NativeMethods.MetricMapDestroy(map);
        }
    }

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
                return NativeMethods.MetricMapGetValueDouble(map, index);
        }
    }
}
