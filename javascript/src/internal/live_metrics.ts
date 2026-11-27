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

// A client's metrics() map: Java's unmodifiable live view, read at each call
// through the native view the client's metrics() returned (native/metrics.c).
// Each KafkaMetric it hands out holds a native metric handle and measures it
// at each metricValue(), as a KafkaMetric kept from Java's map does. The map
// keeps one KafkaMetric per key, so get() and every iteration give the same
// object for the same metric while it stays registered; a key read as absent
// leaves that cache.

import { KafkaMetric } from '../common/metrics/kafka_metric';
import type { Metric } from '../common/metric';
import { MetricName } from '../common/metric_name';
import { MetricNameMap } from '../common/metric_name_map';
// eslint-disable-next-line @typescript-eslint/no-require-imports -- src/internal/native.ts is an `export =` module, which TypeScript imports this way (the addon is loaded there alone, javascript/CLAUDE.md "Files and layout")
import native = require('./native');

type MetricsView = ReturnType<typeof native.producerMetrics>;
type MetricEntry = ReturnType<typeof native.metricsViewEntries>[number];

/**
 * A key's tags as the addon takes them: `[key, value]` pairs, or `null` when a
 * tag value is null, which no metric of the core has (its tags are strings).
 *
 * @param name - the metric name
 * @returns its tags, or `null` when no metric of the core can have them
 */
function tagPairs(name: MetricName): Array<[string, string]> | null {
  const pairs: Array<[string, string]> = [];
  for (const [key, value] of name.tags()) {
    if (value === null) return null;
    pairs.push([key, value]);
  }
  return pairs;
}

/**
 * The live map over `view`, a client's native metrics view.
 *
 * @param view - the view the client's `metrics()` returned
 * @returns the map
 */
export function liveMetrics(view: MetricsView): MetricNameMap<Metric> {
  const cache = new Map<string, KafkaMetric>();
  // The entry's key, and its cached metric or a new one over its handle,
  // cached.
  const metricOf = (entry: MetricEntry): [string, KafkaMetric] => {
    const [fields, handle] = entry;
    const name = new MetricName({ name: fields.name, group: fields.group, description: fields.description, tags: new Map(fields.tags) });
    const key = name.internalKey();
    let metric = cache.get(key);
    if (metric === undefined) {
      metric = KafkaMetric.internalCreate(
        name,
        () => native.metricValue(handle),
        () => native.metricIsMeasurable(handle),
      );
      cache.set(key, metric);
    }
    return [key, metric];
  };
  return MetricNameMap.internalLive<Metric>({
    size: () => native.metricsViewSize(view),
    has: (key) => {
      const tags = tagPairs(key);
      return tags !== null && native.metricsViewHas(view, key.name(), key.group(), tags);
    },
    get: (key) => {
      const tags = tagPairs(key);
      const entry = tags === null ? null : native.metricsViewGet(view, key.name(), key.group(), tags);
      if (entry === null) {
        cache.delete(key.internalKey());
        return null;
      }
      return metricOf(entry)[1];
    },
    entries: () => {
      const seen = new Set<string>();
      const out = native.metricsViewEntries(view).map((entry): [MetricName, Metric] => {
        const [key, metric] = metricOf(entry);
        seen.add(key);
        return [metric.metricName(), metric];
      });
      for (const key of cache.keys()) if (!seen.has(key)) cache.delete(key);
      return out;
    },
  });
}
