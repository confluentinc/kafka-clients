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
/// Copies an owned <c>kafka_common_metrics_MetricsView_t</c> of <c>kafka_common_MetricName_t</c> keys and
/// <c>kafka_common_metrics_KafkaMetric_t</c> values into managed metrics, through an iterator over it, then frees the
/// iterator, which frees its entries, and the view.
/// </summary>
internal static class MetricsViewMarshal
{
    internal static IReadOnlyDictionary<MetricName, IMetric> CopyOutAndDestroy(IntPtr view)
    {
        IntPtr iterator = IntPtr.Zero;
        try
        {
            IntPtr outErr = IntPtr.Zero;
            iterator = NativeMethods.MetricsViewIterator(view, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            Dictionary<MetricName, IMetric> result = new Dictionary<MetricName, IMetric>();
            while (true)
            {
                sbyte hasNext = NativeMethods.IteratorHasNext(iterator, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                if (hasNext == 0)
                {
                    return result;
                }

                IntPtr entry = NativeMethods.IteratorNext(iterator, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr key = NativeMethods.MapEntryKey(entry, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr value = NativeMethods.MapEntryValue(entry, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                MetricName name = CopyOutName(key);
                result[name] = new Metric(name, CopyOutValue(value));
            }
        }
        finally
        {
            try
            {
                IntPtr iteratorErr = IntPtr.Zero;
                NativeMethods.IteratorDestroy(iterator, ref iteratorErr);
                KafkaException.ThrowIfFfiFailed(ref iteratorErr);
            }
            finally
            {
                IntPtr viewErr = IntPtr.Zero;
                NativeMethods.MetricsViewDestroy(view, ref viewErr);
                KafkaException.ThrowIfFfiFailed(ref viewErr);
            }
        }
    }

    private static MetricName CopyOutName(IntPtr name)
    {
        IntPtr outErr = IntPtr.Zero;
        string metric = ReadString(NativeMethods.MetricNameName(name, ref outErr), ref outErr);
        string group = ReadString(NativeMethods.MetricNameGroup(name, ref outErr), ref outErr);
        string description = ReadString(NativeMethods.MetricNameDescription(name, ref outErr), ref outErr);
        IntPtr tags = NativeMethods.MetricNameTags(name, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new MetricName(metric, group, description, CopyOutTagsAndDestroy(tags));
    }

    private static Dictionary<string, string> CopyOutTagsAndDestroy(IntPtr tags)
    {
        try
        {
            IntPtr outErr = IntPtr.Zero;
            int count = NativeMethods.MapSize(tags, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            Dictionary<string, string> result = new Dictionary<string, string>(count, StringComparer.Ordinal);
            for (int i = 0; i < count; i++)
            {
                string key = ReadString(NativeMethods.MapKey(tags, i, ref outErr), ref outErr);
                result[key] = ReadString(NativeMethods.MapValue(tags, i, ref outErr), ref outErr);
            }

            return result;
        }
        finally
        {
            IntPtr destroyErr = IntPtr.Zero;
            NativeMethods.MapDestroy(tags, ref destroyErr);
            KafkaException.ThrowIfFfiFailed(ref destroyErr);
        }
    }

    private static object CopyOutValue(IntPtr kafkaMetric)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr metric = NativeMethods.KafkaMetricAsMetric(kafkaMetric, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr value = NativeMethods.MetricMetricValue(metric, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            int kind = NativeMethods.MetricValueEnum(value, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            object read;
            switch (kind)
            {
                case NativeMethods.MetricValueKindString:
                    return ReadString(NativeMethods.MetricValueAsString(value, ref outErr), ref outErr);
                case NativeMethods.MetricValueKindLong:
                    read = NativeMethods.MetricValueAsLong(value, ref outErr);
                    break;
                case NativeMethods.MetricValueKindInt:
                    read = NativeMethods.MetricValueAsInt(value, ref outErr);
                    break;
                default:
                    read = NativeMethods.MetricValueAsDouble(value, ref outErr);
                    break;
            }

            KafkaException.ThrowIfFfiFailed(ref outErr);
            return read;
        }
        finally
        {
            IntPtr destroyErr = IntPtr.Zero;
            NativeMethods.MetricValueDestroy(value, ref destroyErr);
            KafkaException.ThrowIfFfiFailed(ref destroyErr);
        }
    }

    private static string ReadString(IntPtr value, ref IntPtr outErr)
    {
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return Utf8Marshal.PtrToString(value) ?? string.Empty;
    }
}
