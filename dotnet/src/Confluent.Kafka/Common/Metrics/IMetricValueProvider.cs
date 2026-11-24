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
/// Super-interface for <see cref="IMeasurable"/> or <c>Gauge</c> that provides metric values, Java's
/// <c>org.apache.kafka.common.metrics.MetricValueProvider</c>.
/// </summary>
/// <typeparam name="T">The type of the metric's value.</typeparam>
public interface IMetricValueProvider<T>
{
    /// <summary>Returns the current value associated with this metric.</summary>
    /// <param name="config">The configuration for this metric.</param>
    /// <param name="now">The POSIX time in milliseconds the measurement is being taken.</param>
    /// <returns>The current metric value.</returns>
    T Value(MetricConfig config, long now);
}
