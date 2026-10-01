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

namespace Confluent.Kafka;

/// <summary>
/// A single metric reading — the .NET realization of Java's
/// <c>org.apache.kafka.common.Metric</c> (Java <c>MetricName metricName()</c> +
/// <c>Object metricValue()</c>). It is the value type of the
/// <see cref="IReadOnlyDictionary{TKey, TValue}"/> returned by
/// <see cref="IConsumerCommon.Metrics"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Snapshot, not a live handle.</b> Java's <c>Metric.metricValue()</c> re-measures on
/// each read; the value here was measured once, when <see cref="IConsumerCommon.Metrics"/>
/// was called (the only shape that can cross the C ABI without an upcall per read). Both
/// members are cheap cached reads of the already-marshalled snapshot, so they are
/// <b>properties</b> (not methods).
/// </para>
/// <para>
/// <b>No <c>Kind</c> member (recorded decision).</b> <see cref="Value"/> is a boxed CLR
/// value whose runtime type conveys the kind: a <see cref="double"/>, a <see cref="string"/>,
/// an <see cref="long"/> (<c>Int64</c>), or an <see cref="int"/> (<c>Int32</c>). The core
/// distinguishes <c>Long</c> from <c>Int</c>, and .NET preserves that distinction through
/// the boxed type — so no separate discriminator is exposed (unlike the Python sibling,
/// whose single <c>int</c> forces an explicit <c>kind</c>).
/// </para>
/// </remarks>
public interface IMetric
{
    /// <summary>The metric's name (Java <c>metricName()</c>).</summary>
    MetricName Name { get; }

    /// <summary>
    /// The measured value (Java <c>metricValue()</c>) — a boxed <see cref="double"/>,
    /// <see cref="string"/>, <see cref="long"/>, or <see cref="int"/> (the boxed type is
    /// the kind).
    /// </summary>
    object Value { get; }
}

/// <summary>
/// The internal snapshot implementation of <see cref="IMetric"/> — an immutable
/// <c>(Name, Value)</c> pair built by the metric-map marshaller (ffi-marshalling.md §B2/§B3).
/// Internal because a binding produces metrics; users only consume the <see cref="IMetric"/>
/// interface.
/// </summary>
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
