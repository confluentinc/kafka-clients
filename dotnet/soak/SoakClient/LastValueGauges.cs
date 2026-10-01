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
using System.Linq;

namespace Confluent.Kafka.Soak;

/// <summary>
/// The most recent observation per <c>(metric, tag-set)</c>, retained — a direct port of
/// <c>soakclient.py</c>'s <c>LastValueGauges</c>.
/// <para>
/// The reference soak buffers gauge observations in a list and <i>clears</i> it in the
/// observable-gauge callback. That works only for gauges written on every interval: an
/// event-driven gauge is exported once and then, on the next collection, the callback
/// yields nothing — and a series that yields nothing is not reported as "unchanged", it
/// <b>disappears from the backend entirely</b>. Observed against a real collector:
/// <c>consumer.assignment_size</c> was absent from the export despite the assignment
/// having changed at startup. That and <c>consumer.recovery_ms</c> are precisely the two
/// metrics a run against a rolled cluster exists to produce.
/// </para>
/// <para>
/// So the last value is retained and re-yielded on every subsequent collection, which is
/// ordinary gauge semantics. Retention is <b>per tag-set</b>, not per metric name:
/// <c>consumer.e2e_latency{partition=0}</c> and <c>{partition=1}</c> are distinct series
/// and must not overwrite each other. Bounded: keys are (metric, tag-set) where tags are
/// partitions, error codes and the fixed base tags — the same bounded cardinality as the
/// JSONL counters, which also removes the unbounded-growth failure mode where a
/// misconfigured exporter that never collected let a buffer grow inside the very process
/// being watched for leaks.
/// </para>
/// </summary>
internal sealed class LastValueGauges
{
    private readonly object _lock = new object();
    private readonly Dictionary<string, Dictionary<string, (double Value, IReadOnlyDictionary<string, string> Tags)>> _latest =
        new Dictionary<string, Dictionary<string, (double, IReadOnlyDictionary<string, string>)>>(StringComparer.Ordinal);

    /// <summary>Records (or overwrites) the latest value for <paramref name="name"/> at <paramref name="tags"/>.</summary>
    internal void Record(string name, double value, IReadOnlyDictionary<string, string> tags)
    {
        string key = TagKey(tags);
        var snapshot = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, string> tag in tags)
        {
            snapshot[tag.Key] = tag.Value;
        }

        lock (_lock)
        {
            if (!_latest.TryGetValue(name, out var series))
            {
                series = new Dictionary<string, (double, IReadOnlyDictionary<string, string>)>(StringComparer.Ordinal);
                _latest[name] = series;
            }

            series[key] = (value, snapshot);
        }
    }

    /// <summary>Every retained series for <paramref name="name"/>, as a fresh list.</summary>
    internal IReadOnlyList<(double Value, IReadOnlyDictionary<string, string> Tags)> Snapshot(string name)
    {
        lock (_lock)
        {
            if (!_latest.TryGetValue(name, out var series))
            {
                return Array.Empty<(double, IReadOnlyDictionary<string, string>)>();
            }

            return series.Values.ToList();
        }
    }

    /// <summary>Number of distinct tag-sets retained for <paramref name="name"/>.</summary>
    internal int SeriesCount(string name)
    {
        lock (_lock)
        {
            return _latest.TryGetValue(name, out var series) ? series.Count : 0;
        }
    }

    private static string TagKey(IReadOnlyDictionary<string, string> tags)
    {
        if (tags.Count == 0)
        {
            return string.Empty;
        }

        return string.Join(
            ",",
            tags.OrderBy(tag => tag.Key, StringComparer.Ordinal)
                .Select(tag => tag.Key + "=" + tag.Value));
    }
}
