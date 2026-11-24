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
using System.Threading;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Common.Metrics;

/// <summary>
/// A metric and its value, Java's <c>org.apache.kafka.common.metrics.KafkaMetric</c>: each value of a client's
/// <c>Metrics()</c>, and the metric a <see cref="QuotaViolationException"/> names. A client's metric is measured on each
/// call to <see cref="MetricValue"/>, as Java's, and stays readable after its client is closed. A quota error's metric
/// holds the name and the value the core reported, because the error carries no metric of its own: it is not
/// measurable, and has no configuration until one is set. Equality is identity, as Java's, but two reads of a
/// <c>Metrics()</c> map return two objects for one metric, where Java returns one. It has no public constructor, as
/// Java's takes a lock, a <c>MetricValueProvider</c>, a <c>MetricConfig</c> and a <c>Time</c>, which .NET does not
/// have.
/// </summary>
public sealed class KafkaMetric : IMetric
{
    private const string NotMeasurableMessage =
        "Not a measurable: the metric of a quota violation holds only its name and its value";

    private readonly MetricName _metricName;
    private readonly SafeKafkaMetricHandle? _handle;
    private readonly object? _constantValue;

    // A quota error's metric keeps the configuration it is given here, as the core does a client's metric's.
    private MetricConfig? _constantConfig;

    internal KafkaMetric(MetricName name, object value)
    {
        _metricName = name;
        _constantValue = value;
    }

    internal KafkaMetric(MetricName name, SafeKafkaMetricHandle handle)
    {
        _metricName = name;
        _handle = handle;
    }

    // The metric's own handle, which a NativeMeasurable reads through; null for a quota error's metric.
    internal SafeKafkaMetricHandle? Handle => _handle;

    /// <inheritdoc/>
    public MetricName MetricName() => _metricName;

    /// <inheritdoc/>
    public object MetricValue() => _handle is null ? _constantValue! : MetricsViewMarshal.Measure(_handle);

    /// <summary>Get the configuration of this metric. This is supposed to be used by server only.</summary>
    /// <returns>
    /// Return the config of this metric: a new copy on each call, so changing it does not change the metric, where
    /// Java returns the metric's own object. <see langword="null"/> for a quota error's metric until a configuration is
    /// set.
    /// </returns>
    public MetricConfig? Config()
    {
        if (_handle is null)
        {
            MetricConfig? kept = Volatile.Read(ref _constantConfig);
            return kept is null
                ? null
                : MetricConfigMarshal.CopyOutAndDestroy(MetricConfigMarshal.ToNative(kept, nameof(kept)));
        }

        IntPtr outErr = IntPtr.Zero;
        IntPtr native = NativeMethods.KafkaMetricConfig(_handle, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return MetricConfigMarshal.CopyOutAndDestroy(native);
    }

    /// <summary>Set the metric config. This is supposed to be used by server only.</summary>
    /// <param name="config">
    /// Configuration for this metrics, copied, so changing it afterwards does not change the metric, where Java's
    /// metric holds the object itself. A <see langword="null"/> tags map is copied as no tags.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="config"/> is <see langword="null"/>, or one of its tags has a <see langword="null"/> key or
    /// value, which the core cannot hold; Java stores a <see langword="null"/> config.
    /// </exception>
    /// <exception cref="ArgumentException">A tag holds a NUL character, which the core cannot hold.</exception>
    /// <exception cref="IllegalArgumentException">
    /// The recording level is no <see cref="Sensor.RecordingLevel"/> member, with the message of
    /// <see cref="RecordingLevelMethods.ForId(int)"/>.
    /// </exception>
    public void Config(MetricConfig config)
    {
        MetricConfigMarshal.Validate(config, nameof(config));
        IntPtr native = MetricConfigMarshal.ToNative(config, nameof(config));
        if (_handle is null)
        {
            Volatile.Write(ref _constantConfig, MetricConfigMarshal.CopyOutAndDestroy(native));
            return;
        }

        try
        {
            IntPtr outErr = IntPtr.Zero;
            NativeMethods.KafkaMetricSetConfig(_handle, native, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }
        finally
        {
            MetricConfigMarshal.Destroy(native);
        }
    }

    /// <summary>The method determines if the metric value provider is of type Measurable.</summary>
    /// <returns>
    /// <see langword="true"/> if the metric value provider is of type Measurable, <see langword="false"/> otherwise,
    /// as for a quota error's metric.
    /// </returns>
    public bool IsMeasurable()
    {
        if (_handle is null)
        {
            return false;
        }

        IntPtr outErr = IntPtr.Zero;
        sbyte measurable = NativeMethods.KafkaMetricIsMeasurable(_handle, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return measurable != 0;
    }

    /// <summary>Get the underlying metric provider, which should be a <see cref="IMeasurable"/>.</summary>
    /// <returns>
    /// Return the metric provider: a new object on each call, which measures the metric's own provider, where Java
    /// returns the provider itself.
    /// </returns>
    /// <exception cref="IllegalStateException">
    /// If the underlying metric is not a <see cref="IMeasurable"/>, with the core's message, which begins
    /// "Not a measurable: " as Java's does: "Not a measurable: class &lt;provider&gt;", naming the core's provider type
    /// where Java names the Java class, or, for a provider with no type name, as every one a client registers,
    /// "Not a measurable: the metric value provider is a closure"; and for a quota error's metric.
    /// </exception>
    public IMeasurable Measurable()
    {
        if (_handle is null)
        {
            throw new IllegalStateException(NotMeasurableMessage);
        }

        IntPtr outErr = IntPtr.Zero;
        IntPtr measurable = NativeMethods.KafkaMetricMeasurable(_handle, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        return new NativeMeasurable(this, measurable);
    }

    /// <summary>
    /// A diagnostic rendering in Java's <c>toString()</c> form: the core's text for a client's metric, which names the
    /// core's value provider type where Java names the Java class, and <c>KafkaMetric [metricName=…]</c> for a quota
    /// error's metric.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        _handle is null ? "KafkaMetric [metricName=" + _metricName + "]" : MetricsViewMarshal.ToText(_handle);
}
