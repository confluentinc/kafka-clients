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

// ChaosWorkloadService for the JavaScript gRPC server: runs a chaos-harness
// producer workload (rust/tests/chaos/, `--workload producer:javascript`)
// inside the server's process, against the @confluentinc/kafka-javascript
// producer, and streams its events back to the harness. See
// rust/multilanguage-test-server/proto/chaos_service.proto for the contract.
//
// The loop is the JavaScript counterpart of the asyncio Python server's
// (python/grpc_chaos.py `_run_producer_async`), itself the Rust harness's
// in-process ProducerWorkload (rust/tests/chaos/workload.rs), step for step:
// it sends on an absolute schedule, awaiting each send() only to its
// hand-over (Java's send() returning), never to its delivery, so the client
// batches and pipelines as an application's would, and reports each record's
// outcome from its delivery callback; at StopWorkload it reports its stats,
// closes the producer (every record settles first) and finishes.
//
// RunConsumer is not implemented: the binding has no consumer yet, so grpc-js
// answers it UNIMPLEMENTED, and the harness rejects `consumer:javascript`
// before it starts a run.
//
// Every event is queued from the callback or loop step that observed it, and
// MarkWorkload queues its marker on the same FIFO, so the harness can tell
// which events its client observed before a topic-id switch however far
// behind the stream it reads (chaos_service.proto, "Topic ids"). Everything
// runs on the one JavaScript thread, so the queue and the registry need no
// lock.
//
// Wiring, in grpc-server/grpc_server.js (which loads chaos_service.proto already):
//
//     const { ChaosWorkloadService } = require('../verifiable_clients/chaos_workload');
//     const chaos = new ChaosWorkloadService({ errorToProto: kafkaErrorToProto });
//     server.addService(pkg.ChaosWorkloadService.service, chaos.handlers());
//     // at shutdown, before the producers close:
//     await chaos.stopAll();

const grpc = require('@grpc/grpc-js');

const { KafkaProducer, ProducerRecord } = require('@confluentinc/kafka-javascript/producer');

// Upper bound on events per stream message. The server flushes whatever it has
// queued each time it writes; this only caps one message's size when the
// harness falls behind a very fast workload.
const MAX_BATCH = 4096;

// A schedule more than this far behind (a long send() wait while the client
// waits out a fault) resumes from now rather than replaying the backlog.
const MAX_SCHEDULE_LAG_MS = 1000;

// A producer that is behind schedule (or unlimited) yields to the event loop
// once per this many records, as the asyncio Python server does: the loop
// also writes the gRPC stream and serves StopWorkload / MarkWorkload, so it
// must get turns, while a turn per record would cap the rate.
const YIELD_EVERY = 10;

// The FFI code of the errors this server manufactures (grpc_translate.js's
// LOCAL_ILLEGAL_STATE / python's _LOCAL_ILLEGAL_ARGUMENT).
const LOCAL_ILLEGAL_STATE = -4;
const LOCAL_ILLEGAL_ARGUMENT = -3;

// The prefix of every message this server writes itself.
const SERVER_PREFIX = 'js server: ';

function log(message) {
  process.stderr.write(`${SERVER_PREFIX}chaos: ${message}\n`);
}

// ---------------------------------------------------------------------------
// Record encoding and events
// ---------------------------------------------------------------------------

/** The record key: its 8-byte big-endian logical index. */
function keyOf(index) {
  const key = Buffer.alloc(8);
  key.writeBigUInt64BE(BigInt(index));
  return key;
}

/**
 * The record value: the index's 8 bytes zero-padded to `msgSize`, or the first
 * `msgSize` of those bytes when smaller (rust/tests/chaos/workload.rs
 * `build_value`).
 */
function valueOf(index, msgSize) {
  const key = keyOf(index);
  if (msgSize <= key.length) return key.subarray(0, msgSize);
  const value = Buffer.alloc(msgSize);
  key.copy(value);
  return value;
}

function sent(index) {
  return { sent: { index } };
}

function delivered(index, metadata) {
  return { delivered: { index, partition: metadata.partition, offset: metadata.offset } };
}

function stats(count, elapsedSeconds) {
  return { producer_stats: { sent: count, elapsed_seconds: elapsedSeconds } };
}

function marker(value) {
  return { marker: { marker: value } };
}

function finished() {
  return { finished: {} };
}

function isTerminal(event) {
  return event.finished !== undefined || event.failed !== undefined;
}

/**
 * Settles each record from its delivery callback, exactly once against a
 * send() that rejected (python/grpc_chaos.py `_RecordOutcomes`).
 *
 * A send() can reject after the client has queued the record (while it waits
 * for buffer space), and then the record's callback still fires. So a
 * rejection is reported as `SendFailed` only if the callback has not fired
 * yet, and the first callback after such a report is absorbed (logged): either
 * way the record is settled once. Any other callback is reported, including a
 * second one for the same record, so the verifier sees a client that settles a
 * record twice. An error building the outcome event is not left to the client,
 * which logs and drops what a callback throws (the record would then look
 * unsettled and the client would be blamed): it ends the workload with
 * `Failed`, as a server-side fault.
 */
class RecordOutcomes {
  constructor(emit, workloadId, errorToProto) {
    this.emit = emit;
    this.workloadId = workloadId;
    this.errorToProto = errorToProto;
  }

  /** The delivery callback for record `index`, and its settlement state. */
  onDelivery(index) {
    const state = { callbacks: 0, sendFailed: false };
    const callback = ({ metadata, exception }) => {
      state.callbacks += 1;
      if (state.sendFailed && state.callbacks === 1) {
        log(
          `producer ${this.workloadId}: record ${index} settled by its failed send(); its callback ` +
            `fired afterwards with ${exception ? 'an error' : 'metadata'}`,
        );
        return;
      }
      let event;
      try {
        event = this.outcome(index, metadata, exception);
      } catch (error) {
        log(`producer ${this.workloadId}: building record ${index}'s outcome failed: ${error && error.stack}`);
        event = this.failed(error);
      }
      this.emit(event);
    };
    return { callback, state };
  }

  /** send() of record `index` rejected with `error`. */
  sendRejected(index, state, error) {
    const report = state.callbacks === 0 && !state.sendFailed;
    state.sendFailed = true;
    if (report) {
      this.emit({ send_failed: { index, error: this.errorToProto(error) } });
    } else {
      log(`producer ${this.workloadId}: send() of record ${index} rejected after its callback had settled it: ${error}`);
    }
  }

  outcome(index, metadata, error) {
    if (error !== null && error !== undefined) return { send_failed: { index, error: this.errorToProto(error) } };
    if (metadata === null || metadata === undefined) {
      return {
        send_failed: {
          index,
          error: {
            code: LOCAL_ILLEGAL_STATE,
            message: `${SERVER_PREFIX}delivery callback fired with neither metadata nor error`,
          },
        },
      };
    }
    return delivered(index, metadata);
  }

  failed(error) {
    return { failed: { error: this.errorToProto(error) } };
  }
}

/**
 * Resolves after `ms`, or as soon as `stop.stopped` is set (`stop.wake` is
 * called then).
 */
function sleep(ms, stop) {
  return new Promise((resolve) => {
    const timer = setTimeout(done, ms);
    function done() {
      clearTimeout(timer);
      stop.wakers.delete(done);
      resolve();
    }
    stop.wakers.add(done);
  });
}

/**
 * The producer workload: sends until stopped, then reports its stats, closes
 * the producer and finishes. Emits `Failed` if the producer cannot be built or
 * the loop dies.
 */
async function runProducer(request, emit, stop, errorToProto) {
  const outcomes = new RecordOutcomes(emit, request.workload_id, errorToProto);
  let producer;
  try {
    // Forwarded verbatim; keys and values are the bytes below.
    producer = new KafkaProducer({ configs: { ...(request.config || {}) } });
  } catch (error) {
    log(`producer ${request.workload_id}: construction failed: ${error && error.stack}`);
    emit(outcomes.failed(error));
    return;
  }

  const topic = request.topic;
  const msgSize = Number(request.msg_size || 0);
  const targetRps = Number(request.target_rps || 0);
  const intervalMs = targetRps ? 1000 / targetRps : 0;
  const started = performance.now();
  let nextDue = started;
  let index = 0;
  try {
    while (!stop.stopped) {
      // Java's ProducerRecord(topic, key, value): no partition, no timestamp.
      const record = new ProducerRecord({ topic, key: keyOf(index), value: valueOf(index, msgSize) });
      // Open the record's in-flight window before handing it over; the
      // delivery callback's event closes it.
      emit(sent(index));
      const { callback, state } = outcomes.onDelivery(index);
      try {
        // Awaits the hand-over only (metadata, buffer space); the outcome
        // arrives through the callback.
        await producer.send({ record, callback });
      } catch (error) {
        outcomes.sendRejected(index, state, error);
      }
      index += 1;
      if (intervalMs) {
        nextDue += intervalMs;
        const now = performance.now();
        if (nextDue > now) {
          await sleep(nextDue - now, stop);
          continue;
        }
        if (now - nextDue > MAX_SCHEDULE_LAG_MS) nextDue = now;
      }
      if (index % YIELD_EVERY === 0) await new Promise((resolve) => setImmediate(resolve));
    }
    emit(stats(index, Math.max((performance.now() - started) / 1000, 1e-9)));
  } catch (error) {
    log(`producer ${request.workload_id}: send loop died: ${error && error.stack}`);
    try {
      await producer.close();
    } catch (closeError) {
      log(`close after a failed loop rejected: ${closeError && closeError.stack}`);
    }
    emit(outcomes.failed(error));
    return;
  }

  try {
    await producer.close();
  } catch (error) {
    emit(outcomes.failed(error));
    return;
  }
  emit(finished());
}

/**
 * The events of one workload's stream, in the order they were queued, written
 * in batches of at most MAX_BATCH, with the stream's flow control respected:
 * once a write reports a full buffer, the next waits for `drain`. The stream
 * ends after the batch holding `Finished` or `Failed`.
 */
class EventStream {
  #call;
  #queue = [];
  #scheduled = false;
  #waitingDrain = false;
  #ended = false;

  constructor(call) {
    this.#call = call;
  }

  get ended() {
    return this.#ended;
  }

  emit(event) {
    if (this.#ended) return;
    this.#queue.push(event);
    this.#schedule();
  }

  /** Ends the stream with nothing more written (the call was cancelled). */
  abandon() {
    this.#ended = true;
    this.#queue.length = 0;
  }

  #schedule() {
    if (this.#scheduled || this.#waitingDrain) return;
    this.#scheduled = true;
    setImmediate(() => {
      this.#scheduled = false;
      this.#flush();
    });
  }

  #flush() {
    while (this.#queue.length > 0 && !this.#waitingDrain && !this.#ended) {
      const batch = this.#queue.splice(0, MAX_BATCH);
      const terminal = batch.some(isTerminal);
      const ok = this.#call.write({ events: batch });
      if (terminal) {
        this.#ended = true;
        this.#queue.length = 0;
        this.#call.end();
        return;
      }
      if (!ok) {
        this.#waitingDrain = true;
        this.#call.once('drain', () => {
          this.#waitingDrain = false;
          this.#flush();
        });
      }
    }
  }
}

/** A stop signal: `stopped`, and the sleeps to cut short when it is set. */
function stopSignal() {
  const stop = { stopped: false, wakers: new Set() };
  stop.set = () => {
    stop.stopped = true;
    for (const wake of [...stop.wakers]) wake();
  };
  return stop;
}

/**
 * ChaosWorkloadService over the producer: each workload is a loop on the
 * server's event loop.
 */
class ChaosWorkloadService {
  // workload_id -> { stop, emit, done }
  #workloads = new Map();
  #errorToProto;

  /**
   * @param {object} options
   * @param {(error: unknown) => {code: number, message: string}} options.errorToProto -
   *   the server's translation of an error into a proto KafkaError
   *   (grpc_translate.js `kafkaErrorToProto`)
   */
  constructor({ errorToProto }) {
    if (typeof errorToProto !== 'function') throw new TypeError('errorToProto must be a function');
    this.#errorToProto = errorToProto;
  }

  /** The handlers, by RPC name, for grpc-js's addService (no RunConsumer). */
  handlers() {
    return {
      RunProducer: (call) => this.#runProducer(call),
      StopWorkload: (call, callback) => {
        this.#stop(call.request.workload_id);
        callback(null, {});
      },
      MarkWorkload: (call, callback) => {
        callback(null, { found: this.#mark(call.request.workload_id, call.request.marker) });
      },
    };
  }

  /** Stops every running workload and waits for each to drain. */
  async stopAll() {
    const running = [...this.#workloads.values()];
    for (const workload of running) workload.stop.set();
    await Promise.allSettled(running.map((workload) => workload.done));
  }

  #stop(workloadId) {
    const workload = this.#workloads.get(workloadId);
    if (workload !== undefined) workload.stop.set();
  }

  #mark(workloadId, value) {
    const workload = this.#workloads.get(workloadId);
    // A workload that queued its last event carries no further events.
    if (workload === undefined || workload.terminal) return false;
    workload.emit(marker(value));
    return true;
  }

  #runProducer(call) {
    const request = call.request;
    const workloadId = request.workload_id;
    const stream = new EventStream(call);
    if (this.#workloads.has(workloadId)) {
      call.write({
        events: [
          {
            failed: {
              error: {
                code: LOCAL_ILLEGAL_ARGUMENT,
                message: `${SERVER_PREFIX}workload_id '${workloadId}' is already running`,
              },
            },
          },
        ],
      });
      call.end();
      return;
    }
    const stop = stopSignal();
    const workload = { stop, emit: null, done: null, terminal: false };
    const emit = (event) => {
      if (isTerminal(event)) workload.terminal = true;
      stream.emit(event);
    };
    workload.emit = emit;
    this.#workloads.set(workloadId, workload);
    // Headers now, not with the first event: the harness's call resolves on
    // them (chaos_service.proto, lifecycle step 1).
    call.sendMetadata(new grpc.Metadata());
    // The harness going away stops the workload, which still drains and
    // closes its producer.
    call.on('cancelled', () => {
      stream.abandon();
      stop.set();
    });
    workload.done = runProducer(request, emit, stop, this.#errorToProto)
      .catch((error) => {
        // Never leave the stream open.
        log(`workload ${workloadId} died: ${error && error.stack}`);
        emit({ failed: { error: this.#errorToProto(error) } });
      })
      .finally(() => {
        this.#workloads.delete(workloadId);
      });
  }
}

module.exports = { ChaosWorkloadService, keyOf, valueOf, RecordOutcomes, EventStream, runProducer };
