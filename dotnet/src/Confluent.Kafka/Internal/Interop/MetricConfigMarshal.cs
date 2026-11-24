// Copyright 2026 Confluent Inc.
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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Metrics;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Converts a managed <see cref="MetricConfig"/> to an owned <c>kafka_common_metrics_MetricConfig_t</c> and back: its
/// quota, samples, event window, time window, tags and recording level. A native config is transient: the caller
/// destroys the one <see cref="ToNative"/> returns with <see cref="Destroy"/> in a <c>finally</c>, and
/// <see cref="CopyOutAndDestroy"/> frees the one it reads.
/// </summary>
internal static class MetricConfigMarshal
{
    internal const string InvalidTagMessage =
        "A metric tag must not contain a NUL character: such a string cannot be passed to the native client unchanged.";

    /// <summary>
    /// Checks, before anything native is built, what <see cref="ToNative"/> cannot convert: a <see langword="null"/>
    /// config and a recording level no member has. The tags are checked as <see cref="ToNative"/> reads them, so a map
    /// that answers differently at a second read cannot reach the C ABI.
    /// </summary>
    internal static void Validate(MetricConfig? config, string parameterName)
    {
        if (config is null)
        {
            throw new ArgumentNullException(parameterName);
        }

        _ = RecordingLevelMethods.RequireDefined(config.RecordLevel());
    }

    /// <summary>
    /// An owned native config holding <paramref name="config"/>'s values, which <see cref="Validate"/> accepted. A
    /// <see langword="null"/> quota or tags map is not set, so the native config keeps none. A tag with a
    /// <see langword="null"/> key or value throws <see cref="ArgumentNullException"/>, and one with a NUL
    /// <see cref="ArgumentException"/>, both naming <paramref name="parameterName"/>.
    /// </summary>
    internal static IntPtr ToNative(MetricConfig config, string parameterName)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr native = NativeMethods.MetricConfigNew(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            if (config.Quota() is { } quota)
            {
                SetQuota(native, quota);
            }

            NativeMethods.MetricConfigSetSamples(native, config.Samples(), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            NativeMethods.MetricConfigSetEventWindow(native, config.EventWindow(), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr milliseconds = NativeMethods.TimeUnitMilliseconds(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            NativeMethods.MetricConfigSetTimeWindow(native, config.TimeWindowMs(), milliseconds, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            if (config.Tags() is { } tags)
            {
                SetTags(native, tags, parameterName);
            }

            NativeMethods.MetricConfigSetRecordLevel(native, NativeRecordingLevel(config.RecordLevel()), ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return native;
        }
        catch
        {
            Destroy(native);
            throw;
        }
    }

    /// <summary>A new managed config with the values of the owned <paramref name="native"/>, which it then frees.</summary>
    internal static MetricConfig CopyOutAndDestroy(IntPtr native)
    {
        try
        {
            return CopyOut(native);
        }
        finally
        {
            Destroy(native);
        }
    }

    internal static void Destroy(IntPtr native)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.MetricConfigDestroy(native, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }

    private static MetricConfig CopyOut(IntPtr native)
    {
        IntPtr outErr = IntPtr.Zero;

        // The quota is borrowed from the config: read before the config is freed, never destroyed.
        IntPtr nativeQuota = NativeMethods.MetricConfigQuota(native, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Quota? quota = null;
        if (nativeQuota != IntPtr.Zero)
        {
            double bound = NativeMethods.QuotaBound(nativeQuota, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            sbyte upper = NativeMethods.QuotaIsUpperBound(nativeQuota, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            quota = new Quota(bound, upper != 0);
        }

        int samples = NativeMethods.MetricConfigSamples(native, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        long eventWindow = NativeMethods.MetricConfigEventWindow(native, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        long timeWindowMs = NativeMethods.MetricConfigTimeWindowMs(native, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr recordLevel = NativeMethods.MetricConfigRecordLevel(native, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        int level = NativeMethods.RecordingLevelEnum(recordLevel, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        IntPtr tags = NativeMethods.MetricConfigTags(native, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);

        return new MetricConfig(
            quota, samples, eventWindow, timeWindowMs, CopyOutTagsAndDestroy(tags), ManagedRecordingLevel(level));
    }

    private static void SetQuota(IntPtr native, Quota quota)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr nativeQuota = NativeMethods.QuotaNew(quota.Bound(), quota.IsUpperBound() ? (sbyte)1 : (sbyte)0, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            // The config copies the quota.
            NativeMethods.MetricConfigSetQuota(native, nativeQuota, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
        finally
        {
            IntPtr destroyErr = IntPtr.Zero;
            NativeMethods.QuotaDestroy(nativeQuota, ref destroyErr);
            KafkaException.ThrowIfFfiFailed(ref destroyErr);
        }
    }

    // Each tag is checked as it is read, then pinned; nothing of the user's map is read twice.
    private static void SetTags(IntPtr native, IReadOnlyDictionary<string, string> tags, string parameterName)
    {
        List<Utf8Marshal.PinnedUtf8String> pins = new List<Utf8Marshal.PinnedUtf8String>();
        IntPtr map = IntPtr.Zero;
        try
        {
            IntPtr outErr = IntPtr.Zero;
            map = NativeMethods.MapNew(ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            foreach (KeyValuePair<string, string> tag in tags)
            {
                CheckTag(tag, parameterName);
                Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(tag.Key);
                pins.Add(key);
                Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin(tag.Value);
                pins.Add(value);
                NativeMethods.MapPut(map, key.Pointer, value.Pointer, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
            }

            // The config copies the map's strings.
            NativeMethods.MetricConfigSetTags(native, map, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
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

    private static void CheckTag(KeyValuePair<string, string> tag, string parameterName)
    {
        if (tag.Key is null)
        {
            throw new ArgumentNullException(parameterName, parameterName + " must not contain a null tag key");
        }

        if (tag.Value is null)
        {
            throw new ArgumentNullException(parameterName, parameterName + " must not contain a null tag value");
        }

        AdminStrings.Validate(tag.Key, InvalidTagMessage, parameterName);
        AdminStrings.Validate(tag.Value, InvalidTagMessage, parameterName);
    }

    // The tags in the core's key order, which a Dictionary keeps while nothing is removed from it.
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
                IntPtr key = NativeMethods.MapKey(tags, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                IntPtr value = NativeMethods.MapValue(tags, i, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                result[Utf8Marshal.PtrToString(key) ?? string.Empty] = Utf8Marshal.PtrToString(value) ?? string.Empty;
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

    // A static singleton, never freed.
    private static IntPtr NativeRecordingLevel(Sensor.RecordingLevel level)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr native = RecordingLevelMethods.RequireDefined(level) switch
        {
            Sensor.RecordingLevel.INFO => NativeMethods.RecordingLevelInfo(ref outErr),
            Sensor.RecordingLevel.DEBUG => NativeMethods.RecordingLevelDebug(ref outErr),
            _ => NativeMethods.RecordingLevelTrace(ref outErr),
        };
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return native;
    }

    private static Sensor.RecordingLevel ManagedRecordingLevel(int level) => level switch
    {
        NativeMethods.RecordingLevelEnumInfo => Sensor.RecordingLevel.INFO,
        NativeMethods.RecordingLevelEnumDebug => Sensor.RecordingLevel.DEBUG,
        NativeMethods.RecordingLevelEnumTrace => Sensor.RecordingLevel.TRACE,
        _ => throw new InvalidOperationException(
            "Unknown native recording level " + level.ToString(CultureInfo.InvariantCulture) + "."),
    };
}
