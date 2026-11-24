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

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Common.Metrics;

/// <summary>
/// A metric and its value, Java's <c>org.apache.kafka.common.metrics.KafkaMetric</c>: each value of a client's
/// <c>Metrics()</c>, and the metric a <see cref="QuotaViolationException"/> names. A client's metric is measured on each
/// call to <see cref="MetricValue"/>, as Java's, and stays readable after its client is closed. A quota error's metric
/// holds the name and the value the core reported, because the error carries no metric of its own. Equality is
/// identity, as Java's, but two calls to <c>Metrics()</c> return two objects for one metric, where Java returns one.
/// It has no public constructor, as Java's takes a lock, a <c>MetricValueProvider</c>, a <c>MetricConfig</c> and a
/// <c>Time</c>, which .NET does not have.
/// </summary>
public sealed class KafkaMetric : IMetric
{
    private readonly MetricName _metricName;
    private readonly SafeKafkaMetricHandle? _handle;
    private readonly object? _constantValue;

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

    /// <inheritdoc/>
    public MetricName MetricName() => _metricName;

    /// <inheritdoc/>
    public object MetricValue() => _handle is null ? _constantValue! : MetricsViewMarshal.Measure(_handle);

    /// <summary>
    /// A diagnostic rendering in Java's <c>toString()</c> form: the core's text for a client's metric, which names the
    /// core's value provider type where Java names the Java class, and <c>KafkaMetric [metricName=…]</c> for a quota
    /// error's metric.
    /// </summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        _handle is null ? "KafkaMetric [metricName=" + _metricName + "]" : MetricsViewMarshal.ToText(_handle);
}
