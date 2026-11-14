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
'use strict';

// Java's KafkaMetricTest. Java's lambda providers are objects with the Java
// method: a Measurable's measure(config, now), a Gauge's or a
// MetricValueProvider's value(config, now); Java's MockTime is an object
// with milliseconds(). Not translated: testMeasurableValueReturnsZeroWhenNotMeasurable,
// whose measurableValue(long) is package-private in Java.

const test = require('node:test');
const assert = require('node:assert/strict');

const { IllegalArgumentError, IllegalStateError } = require('@confluentinc/kafka-javascript');
const { MetricName } = require('@confluentinc/kafka-javascript/common');
const { KafkaMetric } = require('@confluentinc/kafka-javascript/common/metrics');

const METRIC_NAME_1 = new MetricName({ name: 'name', group: 'group', description: 'description', tags: new Map() });
const METRIC_NAME_2 = new MetricName({
  name: 'request-latency-avg',
  group: 'consumer-fetch-manager-metrics',
  description: 'The average request latency in ms',
  tags: new Map([['client-id', 'consumer-1']]),
});
const mockTime = () => ({ milliseconds: () => 1000 });
const metricConfig = () => ({});

test('testIsMeasurable', () => {
  const metricValueProvider = { measure: (config, now) => (void config, void now, 0) };
  const metric = new KafkaMetric({ lock: {}, metricName: METRIC_NAME_1, valueProvider: metricValueProvider, config: metricConfig(), time: mockTime() });
  assert.equal(metric.isMeasurable(), true);
  assert.equal(metric.measurable(), metricValueProvider);
});

test('testIsMeasurableWithGaugeProvider', () => {
  const metricValueProvider = { value: () => 0.0 };
  const metric = new KafkaMetric({ lock: {}, metricName: METRIC_NAME_1, valueProvider: metricValueProvider, config: metricConfig(), time: mockTime() });
  assert.equal(metric.isMeasurable(), false);
  assert.throws(() => metric.measurable(), { constructor: IllegalStateError, message: 'Not a measurable: class object' });
});

test('testKafkaMetricAcceptsNonMeasurableNonGaugeProvider', () => {
  const provider = { value: () => 'metric value provider' };
  const metric = new KafkaMetric({ lock: {}, metricName: METRIC_NAME_1, valueProvider: provider, config: metricConfig(), time: mockTime() });
  const value = metric.metricValue();
  assert.equal(value, 'metric value provider');
});

test('testConstructorWithNullProvider', () => {
  assert.throws(
    () => new KafkaMetric({ lock: {}, metricName: METRIC_NAME_1, valueProvider: null, config: metricConfig(), time: mockTime() }),
    { constructor: TypeError, message: 'valueProvider must not be null' },
  );
});

function testToStringOnLambdaOrAnonymousClass(metricValueProvider) {
  const metric = new KafkaMetric({ lock: {}, metricName: METRIC_NAME_2, valueProvider: metricValueProvider, config: metricConfig(), time: mockTime() });
  const result = metric.toString();
  assert.equal(
    result,
    'KafkaMetric [metricName=MetricName [name=request-latency-avg, ' +
      'group=consumer-fetch-manager-metrics, ' +
      'description=The average request latency in ms, ' +
      'tags={client-id=consumer-1}]]',
  );
}

test('testToStringWithLambdaProvider', () => {
  testToStringOnLambdaOrAnonymousClass({ measure: () => 0 });
});

test('testToStringWithAnonymousClassProvider', () => {
  testToStringOnLambdaOrAnonymousClass(
    new (class {
      measure() {
        return 0;
      }
    })(),
  );
});

test('testToStringWithStatProvider', () => {
  class Avg {
    measure() {
      return 0;
    }
  }
  const metric = new KafkaMetric({ lock: {}, metricName: METRIC_NAME_2, valueProvider: new Avg(), config: metricConfig(), time: mockTime() });
  assert.equal(
    metric.toString(),
    'KafkaMetric [metricName=MetricName [name=request-latency-avg, ' +
      'group=consumer-fetch-manager-metrics, ' +
      'description=The average request latency in ms, ' +
      'tags={client-id=consumer-1}], ' +
      'metricValueProvider=Avg]',
  );
});

test("metricValue passes the config and the time's milliseconds; a Measurable's value is its measure; config() reads and sets", () => {
  const seen = [];
  const config = { samples: 2 };
  const measured = new KafkaMetric({
    lock: null,
    metricName: METRIC_NAME_1,
    valueProvider: { measure: (c, now) => (seen.push([c, now]), 4.5) },
    config,
    time: mockTime(),
  });
  assert.equal(measured.metricValue(), 4.5);
  assert.deepEqual(seen, [[config, 1000]]);
  assert.equal(measured.config(), config);
  // A plain object is the options object; any other value is the config.
  const other = { samples: 3 };
  assert.equal(measured.config({ config: other }), undefined);
  assert.equal(measured.config(), other);
  class MetricConfig {}
  const instance = new MetricConfig();
  measured.config(instance);
  assert.equal(measured.config(), instance);
  assert.equal(measured.metricName(), METRIC_NAME_1);
  const noTime = new KafkaMetric({ lock: null, metricName: METRIC_NAME_1, valueProvider: { value: () => 1 }, config: null, time: null });
  assert.throws(() => noTime.metricValue(), { constructor: TypeError, message: 'time must not be null' });
  assert.throws(() => measured.metricValue(1), { constructor: TypeError, message: 'metricValue() only reads: it takes no argument' });
  assert.throws(() => new KafkaMetric({ metricName: METRIC_NAME_1 }), {
    constructor: IllegalArgumentError,
    message: 'KafkaMetric() takes one of (lock, metricName, valueProvider, config, time); got (metricName)',
  });
});
