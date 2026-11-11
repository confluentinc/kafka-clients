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

// The chaos workload service. The Python servers' ChaosWorkloadService has no
// tests of its own; these cover the record encoding against the Rust
// harness's build_value (rust/tests/chaos/workload.rs), the settle-once rule,
// the event stream's batching, flow control and end, the RPC handlers over a
// stand-in for grpc-js's call objects, and a whole producer workload against
// an unreachable broker. Needs the compiled library (dist/).

const test = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');

const { KafkaError } = require('@confluentinc/kafka-javascript/common');

const { ChaosWorkloadService, EventStream, RecordOutcomes, keyOf, valueOf } = require('../chaos_workload');

/** The translation grpc_translate.js makes, reduced to what these tests read. */
function errorToProto(error) {
  return { code: error instanceof KafkaError ? -1 : -4, message: error && error.message };
}

/** A stand-in for grpc-js's ServerWritableStream. */
class FakeCall extends EventEmitter {
  constructor(request, { writable = () => true } = {}) {
    super();
    this.request = request;
    this.writes = [];
    this.ended = false;
    this.metadataSent = false;
    this.writable = writable;
  }

  sendMetadata() {
    this.metadataSent = true;
  }

  write(message) {
    assert.equal(this.ended, false, 'write after end');
    this.writes.push(message);
    return this.writable(this.writes.length);
  }

  end() {
    this.ended = true;
    this.emit('ended');
  }

  /** Every event written, in order. */
  events() {
    return this.writes.flatMap((message) => message.events);
  }
}

function turn() {
  return new Promise((resolve) => setImmediate(resolve));
}

function unary(handler, request) {
  return new Promise((resolve, reject) => {
    handler({ request }, (error, response) => (error ? reject(error) : resolve(response)));
  });
}

test('the key is the 8-byte big-endian index', () => {
  assert.deepEqual([...keyOf(0)], [0, 0, 0, 0, 0, 0, 0, 0]);
  assert.deepEqual([...keyOf(0x0102030405)], [0, 0, 0, 1, 2, 3, 4, 5]);
});

test('the value is the index zero-padded to msg_size, or cut to it', () => {
  assert.deepEqual([...valueOf(1, 12)], [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0]);
  assert.deepEqual([...valueOf(0x0102, 8)], [0, 0, 0, 0, 0, 0, 1, 2]);
  assert.deepEqual([...valueOf(0x0102, 7)], [0, 0, 0, 0, 0, 0, 1]);
  assert.deepEqual([...valueOf(5, 0)], []);
});

test('a delivery is reported with its partition and offset', () => {
  const events = [];
  const outcomes = new RecordOutcomes((e) => events.push(e), 'w', errorToProto);
  const { callback } = outcomes.onDelivery(3);
  callback({ metadata: { partition: 2, offset: 40 }, exception: null });
  assert.deepEqual(events, [{ delivered: { index: 3, partition: 2, offset: 40 } }]);
});

test('a failed delivery is reported with its error', () => {
  const events = [];
  const outcomes = new RecordOutcomes((e) => events.push(e), 'w', errorToProto);
  const { callback } = outcomes.onDelivery(1);
  callback({ metadata: { partition: -1, offset: -1 }, exception: new KafkaError('boom') });
  assert.deepEqual(events, [{ send_failed: { index: 1, error: { code: -1, message: 'boom' } } }]);
});

test('a second callback for one record is reported too', () => {
  const events = [];
  const outcomes = new RecordOutcomes((e) => events.push(e), 'w', errorToProto);
  const { callback } = outcomes.onDelivery(0);
  callback({ metadata: { partition: 0, offset: 1 }, exception: null });
  callback({ metadata: { partition: 0, offset: 2 }, exception: null });
  assert.equal(events.length, 2);
});

test('a rejected send settles its record once: the later callback is absorbed', () => {
  const events = [];
  const outcomes = new RecordOutcomes((e) => events.push(e), 'w', errorToProto);
  const { callback, state } = outcomes.onDelivery(4);
  outcomes.sendRejected(4, state, new KafkaError('rejected'));
  callback({ metadata: { partition: 0, offset: 9 }, exception: null });
  assert.deepEqual(events, [{ send_failed: { index: 4, error: { code: -1, message: 'rejected' } } }]);
  // A further callback is a client that settled the record twice.
  callback({ metadata: { partition: 0, offset: 10 }, exception: null });
  assert.equal(events.length, 2);
});

test('a rejection after the callback settled the record is not reported', () => {
  const events = [];
  const outcomes = new RecordOutcomes((e) => events.push(e), 'w', errorToProto);
  const { callback, state } = outcomes.onDelivery(5);
  callback({ metadata: { partition: 0, offset: 1 }, exception: null });
  outcomes.sendRejected(5, state, new KafkaError('late'));
  assert.deepEqual(events, [{ delivered: { index: 5, partition: 0, offset: 1 } }]);
});

test('a callback with neither metadata nor error is the server reporting a failure', () => {
  const events = [];
  const outcomes = new RecordOutcomes((e) => events.push(e), 'w', errorToProto);
  outcomes.onDelivery(6).callback({ metadata: null, exception: null });
  assert.deepEqual(events, [
    {
      send_failed: {
        index: 6,
        error: { code: -4, message: 'js server: delivery callback fired with neither metadata nor error' },
      },
    },
  ]);
});

test('the stream batches the events queued in one turn and ends after Finished', async () => {
  const call = new FakeCall({});
  const stream = new EventStream(call);
  stream.emit({ sent: { index: 0 } });
  stream.emit({ sent: { index: 1 } });
  await turn();
  assert.deepEqual(call.writes, [{ events: [{ sent: { index: 0 } }, { sent: { index: 1 } }] }]);
  stream.emit({ finished: {} });
  stream.emit({ sent: { index: 2 } });
  await turn();
  assert.equal(call.ended, true);
  assert.deepEqual(call.writes[1], { events: [{ finished: {} }, { sent: { index: 2 } }] });
  // Nothing is written after the end.
  stream.emit({ sent: { index: 3 } });
  await turn();
  assert.equal(call.writes.length, 2);
});

test('the stream caps a batch at 4096 events', async () => {
  const call = new FakeCall({});
  const stream = new EventStream(call);
  for (let i = 0; i < 5000; i++) stream.emit({ sent: { index: i } });
  await turn();
  assert.deepEqual(
    call.writes.map((message) => message.events.length),
    [4096, 904],
  );
});

test('the stream waits for drain once a write reports a full buffer', async () => {
  const call = new FakeCall({}, { writable: (count) => count !== 1 });
  const stream = new EventStream(call);
  for (let i = 0; i < 5000; i++) stream.emit({ sent: { index: i } });
  await turn();
  assert.equal(call.writes.length, 1);
  stream.emit({ sent: { index: 5000 } });
  await turn();
  assert.equal(call.writes.length, 1);
  call.emit('drain');
  assert.deepEqual(
    call.writes.map((message) => message.events.length),
    [4096, 905],
  );
});

test('StopWorkload of an unknown workload succeeds', async () => {
  const handlers = new ChaosWorkloadService({ errorToProto }).handlers();
  assert.deepEqual(await unary(handlers.StopWorkload, { workload_id: 'nobody' }), {});
});

test('MarkWorkload of an unknown workload is not found', async () => {
  const handlers = new ChaosWorkloadService({ errorToProto }).handlers();
  assert.deepEqual(await unary(handlers.MarkWorkload, { workload_id: 'nobody', marker: '1' }), { found: false });
});

test('the service has no RunConsumer: the binding has no consumer', () => {
  const handlers = new ChaosWorkloadService({ errorToProto }).handlers();
  assert.deepEqual(Object.keys(handlers).sort(), ['MarkWorkload', 'RunProducer', 'StopWorkload']);
});

test('the service needs the error translation', () => {
  assert.throws(() => new ChaosWorkloadService({}), { name: 'TypeError', message: 'errorToProto must be a function' });
});

/** Waits until `call` has ended, or fails after `ms`. */
function ended(call, ms) {
  if (call.ended) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('the stream did not end')), ms);
    call.once('ended', () => {
      clearTimeout(timer);
      resolve();
    });
  });
}

const UNREACHABLE = {
  'bootstrap.servers': '127.0.0.1:1',
  'max.block.ms': '200',
  'delivery.timeout.ms': '1000',
  'request.timeout.ms': '500',
};

test('a producer workload streams its records, a marker, its stats, then Finished', async () => {
  const service = new ChaosWorkloadService({ errorToProto });
  const handlers = service.handlers();
  const call = new FakeCall({ workload_id: 'p1', config: UNREACHABLE, topic: 'chaos-t', target_rps: 20, msg_size: 16 });
  handlers.RunProducer(call);
  assert.equal(call.metadataSent, true);
  // A second workload under the same id fails at once.
  const duplicate = new FakeCall({ workload_id: 'p1', config: UNREACHABLE, topic: 'chaos-t' });
  handlers.RunProducer(duplicate);
  assert.equal(duplicate.ended, true);
  assert.deepEqual(duplicate.events(), [
    { failed: { error: { code: -3, message: "js server: workload_id 'p1' is already running" } } },
  ]);

  await new Promise((resolve) => setTimeout(resolve, 600));
  assert.deepEqual(await unary(handlers.MarkWorkload, { workload_id: 'p1', marker: '7' }), { found: true });
  assert.deepEqual(await unary(handlers.StopWorkload, { workload_id: 'p1' }), {});
  await ended(call, 30_000);

  const events = call.events();
  const kinds = events.map((event) => Object.keys(event)[0]);
  assert.deepEqual(kinds.slice(-2), ['producer_stats', 'finished']);
  assert.ok(kinds.includes('marker'));
  const sentIndexes = events.filter((e) => e.sent).map((e) => e.sent.index);
  assert.deepEqual(
    sentIndexes,
    sentIndexes.map((_, i) => i),
  );
  // No broker: every record sent fails, after its Sent, once.
  const failedIndexes = events.filter((e) => e.send_failed).map((e) => e.send_failed.index);
  assert.deepEqual([...failedIndexes].sort((a, b) => a - b), sentIndexes);
  for (const index of sentIndexes) {
    const sentAt = events.findIndex((e) => e.sent && e.sent.index === index);
    const settledAt = events.findIndex((e) => e.send_failed && e.send_failed.index === index);
    assert.ok(sentAt < settledAt, `record ${index} settled before its Sent`);
  }
  const stats = events[events.length - 2].producer_stats;
  assert.equal(stats.sent, sentIndexes.length);
  assert.ok(stats.elapsed_seconds > 0);
  // Gone once finished.
  assert.deepEqual(await unary(handlers.MarkWorkload, { workload_id: 'p1', marker: '8' }), { found: false });
});

test('a producer that cannot be built fails its workload', async () => {
  const handlers = new ChaosWorkloadService({ errorToProto }).handlers();
  const call = new FakeCall({ workload_id: 'bad', config: { acks: 'not-a-number' }, topic: 't' });
  handlers.RunProducer(call);
  await ended(call, 30_000);
  const events = call.events();
  assert.equal(events.length, 1);
  assert.ok(events[0].failed, JSON.stringify(events));
});

test('stopAll stops and drains every running workload', async () => {
  const service = new ChaosWorkloadService({ errorToProto });
  const call = new FakeCall({ workload_id: 'p2', config: UNREACHABLE, topic: 'chaos-u', target_rps: 10, msg_size: 8 });
  service.handlers().RunProducer(call);
  await new Promise((resolve) => setTimeout(resolve, 100));
  await service.stopAll();
  await ended(call, 30_000);
  assert.equal(Object.keys(call.events().at(-1))[0], 'finished');
});
