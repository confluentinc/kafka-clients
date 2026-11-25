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
/// A gauge metric is an instantaneous reading of a particular value, Java's
/// <c>org.apache.kafka.common.metrics.Gauge</c>.
/// </summary>
/// <typeparam name="T">The type of the gauge's value.</typeparam>
/// <remarks>
/// As Java's, it declares no member of its own: its reading is <see cref="IMetricValueProvider{T}.Value"/>, Java's
/// <c>value(MetricConfig, long)</c>.
/// </remarks>
public interface IGauge<T> : IMetricValueProvider<T>
{
}
