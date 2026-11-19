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

// The ProducerService of the gRPC conformance server (grpc-server/), called
// in-process, as dotnet/tests/Confluent.Kafka.GrpcServer.UnitTests calls its
// server's: the requests are the plain objects @grpc/proto-loader hands the
// handlers (snake_case fields, int64s as decimal strings, an unset field
// absent), and no broker is needed: an empty config takes the mock path, and
// a real producer is pointed at an address where nothing listens.

const assert = require('node:assert/strict');

const { ProducerService, selectsMock } = require('../../grpc-server/grpc_server');
const { errorFfiId } = require('../../dist/internal/errors');
const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { ConfigError } = require('@confluentinc/kafka-javascript/common/config');
const { KafkaProducer } = require('@confluentinc/kafka-javascript/producer');

// The FFI ids the server sends for the client's errors.
const KAFKA_ERROR_CODES = {
  ConfigError: errorFfiId(new ConfigError('x')),
  IllegalArgumentError: errorFfiId(new IllegalArgumentError('x')),
};

const NOWHERE = { 'bootstrap.servers': '127.0.0.1:9', 'max.block.ms': '500', 'request.timeout.ms': '500', 'delivery.timeout.ms': '1000' };

/**
 * @typedef {{ code: number, message: string }} ProtoError
 * @typedef {{ producer_id: number, error?: ProtoError }} CreateResponse
 * @typedef {{ topic: string, partition: number, offset: string, timestamp: string, serialized_key_size: number, serialized_value_size: number }} ProtoMetadata
 * @typedef {{ name: string, group: string, description: string, tags: Record<string, string>, double_value?: number, string_value?: string, long_value?: string }} ProtoMetric
 */

/** @type {ProducerService[]} */
const services = [];

/**
 * A new service, closed after the test.
 *
 * @returns {ProducerService} the service
 */
function newService() {
  const service = new ProducerService();
  services.push(service);
  return service;
}

afterEach(async () => {
  await Promise.all(services.splice(0).map((s) => s.closeAll(0)));
});

/**
 * A producer of the service, created from `config`.
 *
 * @param {ProducerService} service the service
 * @param {Record<string, string>} config the CreateProducer config
 * @returns {Promise<string>} its producer_id, as the wire carries a uint64
 */
async function create(service, config) {
  const response = /** @type {CreateResponse} */ (await service.createProducer({ config }));
  assert.equal(response.error, undefined, JSON.stringify(response.error));
  assert.ok(response.producer_id > 0);
  return String(response.producer_id);
}

test('CreateProducer with an empty config, or one whose values are all empty, takes the mock path', async () => {
  assert.equal(selectsMock({}), true);
  assert.equal(selectsMock({ 'bootstrap.servers': '' }), true);
  assert.equal(selectsMock({ 'bootstrap.servers': 'localhost:9092' }), false);
  const service = newService();
  const id = await create(service, {});
  // A mock completes the send at once, at partition 0 and offset 0, with no
  // broker, and reports no timestamp and no sizes, as Java's MockProducer
  // builds its RecordMetadata.
  const response = await service.send({ producer_id: id, record: { topic: 'grpc-mock', key: Buffer.from('k'), value: Buffer.from('v') } });
  assert.deepEqual(response, {
    metadata: { offset: '0', timestamp: '-1', serialized_key_size: 0, serialized_value_size: 0, topic: 'grpc-mock', partition: 0 },
  });
  // A request with no config at all is the empty config.
  const second = /** @type {CreateResponse} */ (await service.createProducer({}));
  assert.equal(second.producer_id, 2);
});

test('CreateProducer with a config the core refuses answers producer_id 0 and the core error', async () => {
  const service = newService();
  const response = /** @type {CreateResponse} */ (
    await service.createProducer({ config: { 'bootstrap.servers': '127.0.0.1:9', 'linger.ms': 'soon' } })
  );
  assert.equal(response.producer_id, 0);
  assert.ok(response.error);
  assert.equal(response.error.code, KAFKA_ERROR_CODES.ConfigError);
  assert.match(response.error.message, /linger\.ms/);
});

test('Send with a callback is logged once, and GetCallbackLog reads it, before and after Close', async () => {
  const service = newService();
  const id = await create(service, {});
  for (const value of ['a', 'b']) {
    const response = await service.send({
      producer_id: id,
      record: { topic: 'grpc-log', partition: 0, value: Buffer.from(value) },
      with_callback: true,
    });
    assert.ok(response.metadata, JSON.stringify(response));
  }
  // A send without a callback is not logged.
  await service.send({ producer_id: id, record: { topic: 'grpc-log', value: Buffer.from('c') } });
  const entry = (/** @type {string} */ offset) => ({
    kind: 'delivery',
    partitions: [{ topic: 'grpc-log', partition: 0 }],
    offsets: { 'grpc-log-0': offset },
    error: '',
  });
  assert.deepEqual(await service.getCallbackLog({ producer_id: id }), { entries: [entry('0'), entry('1')] });
  assert.deepEqual(await service.close({ producer_id: id }), {});
  assert.deepEqual(await service.getCallbackLog({ producer_id: id }), { entries: [entry('0'), entry('1')] });
  // Another producer's log is its own.
  assert.deepEqual(await service.getCallbackLog({ producer_id: '99' }), { entries: [] });
});

test('Send answers a record the client refuses with the client error, and a closed producer with the core error', async () => {
  const service = newService();
  const id = await create(service, {});
  const refused = await service.send({ producer_id: id, record: { topic: 'grpc-refused', partition: -2, value: Buffer.from('v') } });
  assert.ok(refused.error, JSON.stringify(refused));
  assert.equal(refused.error.code, KAFKA_ERROR_CODES.IllegalArgumentError);
  assert.match(refused.error.message, /partition/i);
});

test('Flush and PartitionsFor go to the producer: the mock flushes and knows no partition', async () => {
  const service = newService();
  const id = await create(service, {});
  assert.deepEqual(await service.flush({ producer_id: id }), {});
  assert.deepEqual(await service.partitionsFor({ producer_id: id, topic: 'grpc-partitions' }), { partitions: [] });
});

test('Metrics: the mock has none, and a producer answers each value in its proto form', async () => {
  const service = newService();
  const mock = await create(service, {});
  assert.deepEqual(await service.metrics({ producer_id: mock }), { metrics: { metrics: [] } });
  const real = await create(service, NOWHERE);
  const response = await service.metrics({ producer_id: real });
  assert.ok(response.metrics, JSON.stringify(response));
  const metrics = /** @type {ProtoMetric[]} */ (response.metrics.metrics);
  assert.ok(metrics.length > 0);
  for (const metric of metrics) {
    assert.equal(typeof metric.name, 'string');
    assert.equal(typeof metric.group, 'string');
    assert.equal(typeof metric.description, 'string');
    assert.equal(typeof metric.tags, 'object');
    const forms = ['double_value', 'string_value', 'long_value'].filter((k) => k in metric);
    assert.equal(forms.length, 1, JSON.stringify(metric));
    if ('double_value' in metric) assert.equal(typeof metric.double_value, 'number');
    if ('string_value' in metric) assert.equal(typeof metric.string_value, 'string');
    if (metric.long_value !== undefined) assert.match(metric.long_value, /^-?\d+$/);
  }
  const rate = metrics.find((m) => m.name === 'record-send-rate' && m.group === 'producer-metrics');
  assert.ok(rate, 'no record-send-rate');
  assert.equal(rate.double_value, 0);
  assert.equal(rate.tags['client-id'] !== undefined, true);
});

test('CloseTimeout takes a finite timeout, i64::MAX as no limit, and answers a negative one with the core error', async () => {
  const service = newService();
  const finite = await create(service, NOWHERE);
  assert.deepEqual(await service.closeTimeout({ producer_id: finite, timeout_ms: '1000' }), {});
  const unbounded = await create(service, NOWHERE);
  assert.deepEqual(await service.closeTimeout({ producer_id: unbounded, timeout_ms: '9223372036854775807' }), {});
  // The server drops the producer before it closes it, as Python's server
  // does; the refused close leaves it open, so the test closes it itself.
  const closes = jest.spyOn(KafkaProducer.prototype, 'close');
  try {
    const negative = await create(service, NOWHERE);
    assert.deepEqual(await service.closeTimeout({ producer_id: negative, timeout_ms: '-1' }), {
      error: { code: KAFKA_ERROR_CODES.IllegalArgumentError, message: 'The timeout cannot be negative.' },
    });
  } finally {
    const open = /** @type {KafkaProducer<unknown, unknown>[]} */ (closes.mock.contexts);
    closes.mockRestore();
    await Promise.all(open.map((p) => p.close({ timeout: 0 })));
  }
  // An absent timeout is 0 ms.
  const absent = await create(service, NOWHERE);
  assert.deepEqual(await service.closeTimeout({ producer_id: absent }), {});
});

test('a producer id the server does not hold is an error for every call but Close, which succeeds', async () => {
  const service = newService();
  const unknown = { code: -4, message: 'unknown producer_id 42' };
  const request = { producer_id: '42' };
  assert.deepEqual(await service.send({ ...request, record: { topic: 't', value: Buffer.from('v') } }), { error: unknown });
  assert.deepEqual(await service.flush(request), { error: unknown });
  assert.deepEqual(await service.partitionsFor({ ...request, topic: 't' }), { error: unknown });
  assert.deepEqual(await service.metrics(request), { error: unknown });
  assert.deepEqual(await service.close(request), {});
  assert.deepEqual(await service.closeTimeout({ ...request, timeout_ms: '10' }), {});
  // An absent id is the id 0, which no producer has.
  assert.deepEqual(await service.flush({}), { error: { code: -4, message: 'unknown producer_id 0' } });
  // A closed producer's id is unknown after its Close.
  const id = await create(service, {});
  await service.close({ producer_id: id });
  assert.deepEqual(await service.flush({ producer_id: id }), { error: { code: -4, message: `unknown producer_id ${id}` } });
});

test('the handlers answer through the gRPC callback: a response as the result, a server failure as INTERNAL', async () => {
  const service = newService();
  const handlers =
    /** @type {Record<string, (call: { request: unknown }, callback: (error: unknown, response: unknown) => void) => void>} */ (
      service.handlers()
    );
  assert.deepEqual(Object.keys(handlers).sort(), [
    'Close',
    'CloseTimeout',
    'CreateProducer',
    'Flush',
    'GetCallbackLog',
    'Metrics',
    'PartitionsFor',
    'Send',
  ]);
  /**
   * @param {string} name the RPC
   * @param {unknown} request its request
   * @returns {Promise<[unknown, unknown]>} the error and the response the handler gave its callback
   */
  const call = (name, request) =>
    new Promise((resolve) =>
      handlers[name]({ request }, (/** @type {unknown} */ error, /** @type {unknown} */ response) => resolve([error, response])),
    );
  const [error, response] = await call('CreateProducer', { config: {} });
  assert.equal(error, null);
  assert.deepEqual(response, { producer_id: 1 });
  // A request the service cannot read is the server's own failure.
  const [failure] = await call('Send', null);
  assert.ok(failure);
  assert.equal(/** @type {{ code: number }} */ (failure).code, 13);
  assert.match(/** @type {{ details: string }} */ (failure).details, /^js server: TypeError/);
});
