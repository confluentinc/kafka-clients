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
/// The latest value per (metric, tag-set), re-yielded on every collection, as Python's soak
/// <c>LastValueGauges</c>: a gauge callback that yields nothing makes the series vanish.
/// </summary>
internal sealed class LastValueGauges
{
    private readonly object _lock = new object();
    private readonly Dictionary<string, Dictionary<string, (double Value, IReadOnlyDictionary<string, string> Tags)>> _latest =
        new Dictionary<string, Dictionary<string, (double, IReadOnlyDictionary<string, string>)>>(StringComparer.Ordinal);

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
