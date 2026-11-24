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
using System.Runtime.InteropServices;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Metrics;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The metrics of Java's <c>KafkaMetricTest</c>, which builds each one over a provider of its own: a Java
/// <c>Metrics</c> registry made through the C ABI, which the binding does not expose, holds a C provider the tests
/// write, and each metric is kept as a client's <c>Metrics()</c> keeps one.
/// </summary>
/// <remarks>
/// Each provider returns Java's constant and has no state (<c>self_</c> is <c>NULL</c>) and no type name, as a Java
/// lambda's text names none. Its function is a rooted delegate, so a metric stays measurable after its registry is
/// destroyed ("Sensor and metric handles taken from it stay valid").
/// </remarks>
internal static class NativeMetricRegistry
{
    /// <summary>
    /// The reading of Java's <c>MetricValueProvider&lt;String&gt; provider = (config, now) -> "metric value provider"</c>.
    /// </summary>
    internal const string ProviderValue = "metric value provider";

    private static readonly ProviderValueFn s_gaugeValue = GaugeValue;

    private static readonly ProviderValueFn s_stringValue = StringValue;

    private static readonly MeasureFn s_measure = Measure;

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    private delegate IntPtr ProviderValueFn(IntPtr self, IntPtr config, long now);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    private delegate double MeasureFn(IntPtr self, IntPtr config, long now);

    /// <summary>
    /// Java's <c>Gauge&lt;Double&gt; metricValueProvider = (config, now) -> 0.0</c>. The C ABI has no <c>Gauge</c>
    /// interface: a provider that is not a <c>Measurable</c> is what a <c>Gauge</c> is to <c>isMeasurable()</c>.
    /// </summary>
    internal static KafkaMetric Gauge(MetricName name) => AddProvider(name, s_gaugeValue);

    /// <summary>
    /// A provider that is neither a <c>Measurable</c> nor a <c>Gauge</c>, returning <see cref="ProviderValue"/>.
    /// </summary>
    internal static KafkaMetric StringProvider(MetricName name) => AddProvider(name, s_stringValue);

    /// <summary>Java's <c>Measurable metricValueProvider = (config, now) -> 0</c>.</summary>
    internal static KafkaMetric Measurable(MetricName name)
    {
        return Add(name, (registry, nativeName) =>
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr measurable = Native.MeasurableNew(IntPtr.Zero, s_measure, IntPtr.Zero, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);

            // The registry consumes the measurable, whatever the outcome.
            Native.MetricsAddMetricWithMeasurable(registry, nativeName, measurable, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            IntPtr metric = Native.MetricsMetric(registry, nativeName, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return metric;
        });
    }

    private static KafkaMetric AddProvider(MetricName name, ProviderValueFn value)
    {
        return Add(name, (registry, nativeName) =>
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr provider = Native.MetricValueProviderNew(IntPtr.Zero, value, IntPtr.Zero, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);

            // The registry consumes the provider, whatever the outcome.
            IntPtr metric = Native.MetricsAddMetricIfAbsent(registry, nativeName, IntPtr.Zero, provider, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return metric;
        });
    }

    // Registers a metric under `name` in a new registry, keeps the owned KafkaMetric_t `register` returns, and
    // destroys the registry.
    private static KafkaMetric Add(MetricName name, Func<IntPtr, IntPtr, IntPtr> register)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr registry = Native.MetricsNew(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        try
        {
            IntPtr nativeName = MetricsViewMarshal.ToNativeName(name);
            if (nativeName == IntPtr.Zero)
            {
                throw new ArgumentException("the name cannot cross the C ABI: " + name, nameof(name));
            }

            try
            {
                return MetricsViewMarshal.Keep(register(registry, nativeName));
            }
            finally
            {
                MetricsViewMarshal.DestroyName(nativeName);
            }
        }
        finally
        {
            IntPtr destroyErr = IntPtr.Zero;
            Native.MetricsDestroy(registry, ref destroyErr);
            KafkaException.ThrowIfFfiFailed(ref destroyErr);
        }
    }

    // The providers run inside a native call, so nothing may throw out of them, and the core takes their reading over
    // without a NULL check ("the returned reading (never null)"), so a reading that cannot be built ends the process.
    // CKRDA0004 accepts no catch that returns a value, so it cannot see that: each body is a constant, or one try
    // whose catch-all ends the process.
#pragma warning disable CKRDA0004
    private static IntPtr GaugeValue(IntPtr self, IntPtr config, long now)
    {
        try
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr value = Native.MetricValueDouble(0.0, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return value;
        }
        catch (Exception e)
        {
            Environment.FailFast("NativeMetricRegistry: the gauge's reading could not be built", e);
            return IntPtr.Zero;
        }
    }

    private static IntPtr StringValue(IntPtr self, IntPtr config, long now)
    {
        try
        {
            using Utf8Marshal.PinnedUtf8String text = Utf8Marshal.Pin(ProviderValue);
            IntPtr outErr = IntPtr.Zero;
            IntPtr value = Native.MetricValueString(text.Pointer, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            return value;
        }
        catch (Exception e)
        {
            Environment.FailFast("NativeMetricRegistry: the provider's reading could not be built", e);
            return IntPtr.Zero;
        }
    }

    private static double Measure(IntPtr self, IntPtr config, long now) => 0.0;
#pragma warning restore CKRDA0004

    /// <summary>The C ABI calls only these tests make.</summary>
    private static class Native
    {
        private const string DllName = "confluent_kafka";

        [DllImport(DllName, EntryPoint = "kafka_common_metrics_Metrics_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr MetricsNew(ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_metrics_Metrics_destroy_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void MetricsDestroy(IntPtr self, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_metrics_MetricValueProvider_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr MetricValueProviderNew(
            IntPtr self, ProviderValueFn value, IntPtr typeName, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_metrics_Measurable_new_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr MeasurableNew(IntPtr self, MeasureFn measure, IntPtr typeName, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_metrics_Metrics_add_metric_if_absent_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr MetricsAddMetricIfAbsent(
            IntPtr self, IntPtr metricName, IntPtr config, IntPtr provider, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_metrics_Metrics_add_metric_with_measurable", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void MetricsAddMetricWithMeasurable(
            IntPtr self, IntPtr metricName, IntPtr measurable, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_metrics_Metrics_metric_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr MetricsMetric(IntPtr self, IntPtr metricName, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_MetricValue_double_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr MetricValueDouble(double value, ref IntPtr outErr);

        [DllImport(DllName, EntryPoint = "kafka_common_MetricValue_string_err", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr MetricValueString(IntPtr value, ref IntPtr outErr);
    }
}
