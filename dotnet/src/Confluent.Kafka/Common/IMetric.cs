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

using System.Collections.Generic;

namespace Confluent.Kafka.Common;

/// <summary>
/// A metric and its value, Java's <c>Metric</c>: the value type of the
/// <see cref="IReadOnlyDictionary{TKey, TValue}"/> a client's <c>Metrics()</c> returns. The value was measured
/// once, when <c>Metrics()</c> was called, where Java measures on each read.
/// </summary>
public interface IMetric
{
    /// <summary>The metric's name.</summary>
    MetricName Name { get; }

    /// <summary>
    /// The measured value: a boxed <see cref="double"/>, <see cref="string"/>, <see cref="long"/> or
    /// <see cref="int"/>, whose type is the metric's kind.
    /// </summary>
    object Value { get; }
}

/// <summary>The <see cref="IMetric"/> a client's metric map holds: an immutable name and value.</summary>
internal sealed class Metric : IMetric
{
    internal Metric(MetricName name, object value)
    {
        Name = name;
        Value = value;
    }

    /// <inheritdoc/>
    public MetricName Name { get; }

    /// <inheritdoc/>
    public object Value { get; }
}
