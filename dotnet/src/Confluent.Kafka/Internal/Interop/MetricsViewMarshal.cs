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
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Metrics;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Reads a client's live <c>kafka_common_metrics_MetricsView_t</c> of <c>kafka_common_MetricName_t</c> keys and
/// <c>kafka_common_metrics_KafkaMetric_t</c> values: its size, a lookup by a managed <see cref="MetricName"/>, and its
/// entries through an iterator over a snapshot of it, each value a <see cref="KafkaMetric"/> over its own owned metric
/// handle; and reads the name, the value and the text of such a handle.
/// </summary>
internal static class MetricsViewMarshal
{
    internal static int Count(SafeMetricsViewHandle view)
    {
        IntPtr outErr = IntPtr.Zero;
        int count = NativeMethods.MetricsViewLen(view, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return count;
    }

    internal static bool Contains(SafeMetricsViewHandle view, MetricName key)
    {
        IntPtr name = ToNativeName(key);
        if (name == IntPtr.Zero)
        {
            return false;
        }

        try
        {
            IntPtr outErr = IntPtr.Zero;
            sbyte contains = NativeMethods.MetricsViewContainsKey(view, name, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return contains != 0;
        }
        finally
        {
            DestroyName(name);
        }
    }

    internal static KafkaMetric? Get(SafeMetricsViewHandle view, MetricName key)
    {
        IntPtr name = ToNativeName(key);
        if (name == IntPtr.Zero)
        {
            return null;
        }

        try
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr metric = NativeMethods.MetricsViewGet(view, name, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return metric == IntPtr.Zero ? null : Keep(metric);
        }
        finally
        {
            DestroyName(name);
        }
    }

    internal static List<MetricName> Names(SafeMetricsViewHandle view)
    {
        List<MetricName> names = new List<MetricName>();
        ForEachEntry(view, key => names.Add(CopyOutName(key)));
        return names;
    }

    // The iterator owns each entry's value until its next step, so each metric is kept through the view's own get, taken
    // while the entry is current. A metric removed since the iterator's snapshot is left out, as an iterator of Java's
    // ConcurrentHashMap may leave it out. The key is the entry's own name, which a mock producer's metric may not carry.
    internal static List<KeyValuePair<MetricName, IMetric>> Entries(SafeMetricsViewHandle view)
    {
        List<KeyValuePair<MetricName, IMetric>> entries = new List<KeyValuePair<MetricName, IMetric>>();
        ForEachEntry(view, key =>
        {
            MetricName name = CopyOutName(key);
            if (KeepEntry(view, key) is { } metric)
            {
                entries.Add(new KeyValuePair<MetricName, IMetric>(name, metric));
            }
        });
        return entries;
    }

    internal static List<IMetric> Metrics(SafeMetricsViewHandle view)
    {
        List<IMetric> metrics = new List<IMetric>();
        ForEachEntry(view, key =>
        {
            if (KeepEntry(view, key) is { } metric)
            {
                metrics.Add(metric);
            }
        });
        return metrics;
    }

    /// <summary>
    /// An owned <c>kafka_common_MetricName_t</c> for <paramref name="name"/>, freed with <see cref="DestroyName"/>; or
    /// <see cref="IntPtr.Zero"/> when its name, group or tags would not cross the C ABI unchanged (a <c>'\0'</c>, an
    /// unpaired surrogate, which would cross as <c>'?'</c>, or a <see langword="null"/> tag value). The core holds
    /// every name as UTF-8 text with no NUL, so a lookup by such a name finds nothing, without a call into the core,
    /// never a metric whose name holds the <c>'?'</c> (ledger 653). The trade-off: a client whose configuration holds
    /// an unpaired surrogate (its <c>client.id</c>, a topic) registers its metrics under the <c>'?'</c> that
    /// configuration crossed as, so a lookup by the configured string does not find them, where Java's finds them, and
    /// only a lookup by the <c>'?'</c> form does. The description is not part of the name's equality, so a
    /// <c>'\0'</c> in it only shortens it, and an unpaired surrogate in it crosses as <c>'?'</c>.
    /// </summary>
    internal static IntPtr ToNativeName(MetricName name)
    {
        IReadOnlyDictionary<string, string> tags = name.Tags();
        if (!CrossesUnchanged(name.Name()) || !CrossesUnchanged(name.Group()))
        {
            return IntPtr.Zero;
        }

        foreach (KeyValuePair<string, string> tag in tags)
        {
            if (!CrossesUnchanged(tag.Key) || !CrossesUnchanged(tag.Value))
            {
                return IntPtr.Zero;
            }
        }

        List<Utf8Marshal.PinnedUtf8String> pins = new List<Utf8Marshal.PinnedUtf8String>((tags.Count * 2) + 3);
        IntPtr map = IntPtr.Zero;
        try
        {
            IntPtr outErr = IntPtr.Zero;
            map = NativeMethods.MapNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            foreach (KeyValuePair<string, string> tag in tags)
            {
                Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(tag.Key);
                pins.Add(key);
                Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin(tag.Value);
                pins.Add(value);
                NativeMethods.MapPut(map, key.Pointer, value.Pointer, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            Utf8Marshal.PinnedUtf8String metric = Utf8Marshal.Pin(name.Name());
            pins.Add(metric);
            Utf8Marshal.PinnedUtf8String group = Utf8Marshal.Pin(name.Group());
            pins.Add(group);
            Utf8Marshal.PinnedUtf8String description = Utf8Marshal.Pin(name.Description());
            pins.Add(description);
            IntPtr native = NativeMethods.MetricNameNew(
                metric.Pointer, group.Pointer, description.Pointer, map, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return native;
        }
        finally
        {
            try
            {
                IntPtr mapErr = IntPtr.Zero;
                NativeMethods.MapDestroy(map, ref mapErr);
                KafkaException.ThrowIfFfiFailed(ref mapErr);
            }
            finally
            {
                foreach (Utf8Marshal.PinnedUtf8String pin in pins)
                {
                    pin.Dispose();
                }
            }
        }
    }

    internal static void DestroyName(IntPtr name)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.MetricNameDestroy(name, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    internal static KafkaMetric Keep(IntPtr metric)
    {
        SafeKafkaMetricHandle handle = new SafeKafkaMetricHandle(metric);
        try
        {
            return new KafkaMetric(ReadName(handle), handle);
        }
        catch
        {
            handle.Dispose();
            throw;
        }
    }

    internal static object Measure(SafeKafkaMetricHandle metric)
    {
        bool added = false;
        try
        {
            metric.DangerousAddRef(ref added);
            IntPtr outErr = IntPtr.Zero;
            IntPtr view = NativeMethods.KafkaMetricAsMetric(metric, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr value = NativeMethods.MetricMetricValue(view, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return CopyOutValueAndDestroy(value);
        }
        finally
        {
            if (added)
            {
                metric.DangerousRelease();
            }
        }
    }

    internal static string ToText(SafeKafkaMetricHandle metric)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr text = NativeMethods.KafkaMetricToString(metric, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            return Utf8Marshal.PtrToString(text) ?? string.Empty;
        }
        finally
        {
            IntPtr destroyErr = IntPtr.Zero;
            NativeMethods.StringDestroy(text, ref destroyErr);
            KafkaException.ThrowIfFfiFailed(ref destroyErr);
        }
    }

    internal static MetricName CopyOutName(IntPtr name)
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

    private static bool CrossesUnchanged(string? value) =>
        value is not null && value.IndexOf('\0') < 0 && !Utf8Marshal.HasUnpairedSurrogate(value);

    private static void ForEachEntry(SafeMetricsViewHandle view, Action<IntPtr> entry)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr iterator = NativeMethods.MetricsViewIterator(view, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            while (true)
            {
                sbyte hasNext = NativeMethods.IteratorHasNext(iterator, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                if (hasNext == 0)
                {
                    return;
                }

                IntPtr next = NativeMethods.IteratorNext(iterator, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr key = NativeMethods.MapEntryKey(next, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                entry(key);
            }
        }
        finally
        {
            IntPtr destroyErr = IntPtr.Zero;
            NativeMethods.IteratorDestroy(iterator, ref destroyErr);
            KafkaException.ThrowIfFfiFailed(ref destroyErr);
        }
    }

    private static KafkaMetric? KeepEntry(SafeMetricsViewHandle view, IntPtr key)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr metric = NativeMethods.MetricsViewGet(view, key, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return metric == IntPtr.Zero ? null : Keep(metric);
    }

    private static MetricName ReadName(SafeKafkaMetricHandle metric)
    {
        bool added = false;
        try
        {
            metric.DangerousAddRef(ref added);
            IntPtr outErr = IntPtr.Zero;
            IntPtr view = NativeMethods.KafkaMetricAsMetric(metric, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr name = NativeMethods.MetricMetricName(view, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return CopyOutName(name);
        }
        finally
        {
            if (added)
            {
                metric.DangerousRelease();
            }
        }
    }

    private static object CopyOutValueAndDestroy(IntPtr value)
    {
        try
        {
            IntPtr outErr = IntPtr.Zero;
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
