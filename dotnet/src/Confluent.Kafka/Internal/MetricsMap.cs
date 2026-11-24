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
using System.Collections;
using System.Collections.Generic;
using Confluent.Kafka.Common;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The map a client's <c>Metrics()</c> returns, Java's <c>Collections.unmodifiableMap(metrics.metrics())</c>: a
/// read-only wrapper over the client's live view, so every member reads the registry at the time of the call. A lookup
/// returns a new <see cref="Common.Metrics.KafkaMetric"/> over the registered metric. <c>Keys</c> and <c>Values</c> are
/// live too, as Java's <c>keySet()</c> and <c>values()</c>: each enumeration of the map, of <c>Keys</c> or of
/// <c>Values</c> reads a snapshot of the registry into a list when it starts, so a kept <c>Keys</c> sees a metric
/// registered later, and an enumerator left unfinished holds no native iterator. A kept <c>Keys</c> or <c>Values</c>
/// holds the view, as the map does. It is not an <see cref="IDictionary{TKey, TValue}"/>, so it has nothing to
/// modify.
/// </summary>
internal sealed class MetricsMap : IReadOnlyDictionary<MetricName, IMetric>
{
    private readonly SafeMetricsViewHandle _view;
    private readonly SnapshotView<MetricName> _keys;
    private readonly SnapshotView<IMetric> _values;

    internal MetricsMap(SafeMetricsViewHandle view)
    {
        _view = view;
        _keys = new SnapshotView<MetricName>(() => MetricsViewMarshal.Names(view));
        _values = new SnapshotView<IMetric>(() => MetricsViewMarshal.Metrics(view));
    }

    public int Count => MetricsViewMarshal.Count(_view);

    public IEnumerable<MetricName> Keys => _keys;

    public IEnumerable<IMetric> Values => _values;

    public IMetric this[MetricName key] =>
        TryGetValue(key, out IMetric value)
            ? value
            : throw new KeyNotFoundException($"The given key '{key}' was not present in the dictionary.");

    public bool ContainsKey(MetricName key) => MetricsViewMarshal.Contains(_view, RequireKey(key));

    public bool TryGetValue(MetricName key, out IMetric value)
    {
        IMetric? metric = MetricsViewMarshal.Get(_view, RequireKey(key));
        value = metric!;
        return metric is not null;
    }

    public IEnumerator<KeyValuePair<MetricName, IMetric>> GetEnumerator() =>
        MetricsViewMarshal.Entries(_view).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private static MetricName RequireKey(MetricName key) => key ?? throw new ArgumentNullException(nameof(key));

    // Keys or Values: each enumeration reads the registry's snapshot into a list when it starts.
    private sealed class SnapshotView<T> : IEnumerable<T>
    {
        private readonly Func<List<T>> _read;

        internal SnapshotView(Func<List<T>> read)
        {
            _read = read;
        }

        public IEnumerator<T> GetEnumerator() => _read().GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
