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

namespace Confluent.Kafka.Common.Metrics;

/// <summary>
/// A metric and its value, Java's <c>org.apache.kafka.common.metrics.KafkaMetric</c>: the metric a
/// <see cref="QuotaViolationException"/> names. It has no public constructor, as Java's takes a
/// <c>MetricValueProvider</c>, a <c>MetricConfig</c> and a <c>Time</c>, which .NET does not have.
/// </summary>
public sealed class KafkaMetric : IMetric
{
    internal KafkaMetric(MetricName name, object value)
    {
        Name = name;
        Value = value;
    }

    /// <inheritdoc/>
    public MetricName Name { get; }

    /// <inheritdoc/>
    public object Value { get; }
}
