#!/usr/bin/env node
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

'use strict';

// gRPC server exposing the Node.js producer (dist/producer, compiled from
// src/producer) over the ProducerService of
// rust/multilanguage-test-server/proto, the JavaScript twin of
// python/grpc_server_async.py's ProducerService: the Rust multilanguage
// integration tests run each producer test body against a `__grpc_js` backend
// that tunnels every Producer call to this server (rust/tests/common/
// backend_pool.rs, BackendKind::Js).
//
// Only ProducerService is registered: the binding has no consumer or admin
// client yet. Within it, the RPCs for what the JS producer does not have yet
// are deliberately left out, so grpc-js answers them with UNIMPLEMENTED rather
// than this server inventing a result: InitTransactions, BeginTransaction,
// CommitTransaction, AbortTransaction and SendOffsetsToTransaction, as there
// is no transaction surface.
// The tests needing them are skipped for this backend only, by JS_GRPC_SKIPS in
// rust/Makefile, each with the condition for removing its skip.
//
// Everything runs on the one JS thread (the delivery callbacks too), so the
// producer map and the callback log need no lock; ids are allocated without an
// intervening await.
//
// Environment:
//   GRPC_PROTO_DIR  directory holding the four .proto files (required)
//   GRPC_HOST       address to bind, default 127.0.0.1 (the server is
//                   unauthenticated; the Docker image sets 0.0.0.0)
//   GRPC_PORT       port to bind, default 50054; 0 binds an ephemeral port
// Once serving it prints "js server: listening on <host>:<port>" to stderr with
// the bound port, which the Rust BackendPool waits for (container mode matches
// "listening", native mode parses the port from the last ':'-separated field)
// -- keep that format stable.

const path = require('node:path');

const grpc = require('@grpc/grpc-js');
// grpc-js's server-side call, whose deadline handling keepLongDeadlines adjusts.
const { BaseServerInterceptingCall } = require('@grpc/grpc-js/build/src/server-interceptors');
const protoLoader = require('@grpc/proto-loader');

const { KafkaProducer, MockProducer } = require('../dist/producer');
const {
  CallbackLog,
  LOCAL_TIMEOUT,
  futureError,
  int64,
  kafkaErrorToProto,
  makeLoggingDeliveryCallback,
  metricToProto,
  partitionInfoToProto,
  protoToProducerRecord,
  recordMetadataToProto,
  serverError,
  unknownProducer,
} = require('./grpc_translate');
const { ChaosWorkloadService } = require('../verifiable_clients/chaos_workload');

const PROTO_FILES = ['producer_service.proto', 'consumer_service.proto', 'admin_service.proto', 'chaos_service.proto'];
// How long Send waits for a record's future, as python's servers do.
const SEND_TIMEOUT_MS = 120_000;
// How long a shutdown waits for each producer still open to close.
const SHUTDOWN_CLOSE_TIMEOUT_MS = 5_000;

function log(message) {
  process.stderr.write(`js server: ${message}\n`);
}

/** Load the protos and return the confluent.kafka.test package. */
function loadProtos(protoDir) {
  // The files are named relative to includeDirs, which also resolves the
  // imports between them (consumer_service.proto imports producer_service.proto).
  const definition = protoLoader.loadSync(PROTO_FILES, {
    includeDirs: [path.resolve(protoDir)],
    keepCase: true,
    longs: String,
    enums: Number,
    defaults: false,
    oneofs: true,
  });
  return grpc.loadPackageDefinition(definition).confluent.kafka.test;
}

/**
 * The config selects a mock producer in the other servers (python's
 * `not config or all(not v for v in config.values())`, the rule
 * admin_service.proto states for CreateAdminRequest).
 */
function selectsMock(config) {
  return Object.values(config).every((value) => !value);
}

/** Settle with `promise`, or reject with `onTimeout()` after `ms`. */
function withTimeout(promise, ms, onTimeout) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(onTimeout()), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

/** A sentinel for the Send timeout, told apart from the client's own errors. */
class SendTimeout {}

/** The longest delay a Node.js timer keeps: 2^31 - 1 ms, 24.8 days. */
const MAX_TIMEOUT_MS = 2 ** 31 - 1;

/**
 * That delay as a `grpc-timeout` value, which takes at most 8 digits: whole
 * seconds, so it stays within it.
 */
const MAX_TIMEOUT_HEADER = `${Math.floor(MAX_TIMEOUT_MS / 1000)}S`;

/** The milliseconds of each `grpc-timeout` unit, as grpc-js reads them. */
const TIMEOUT_UNIT_MS = { H: 3_600_000, M: 60_000, S: 1_000, m: 1, u: 0.001, n: 0.000001 };

/**
 * A `grpc-timeout` header value, or the longest one a Node.js timer keeps
 * (MAX_TIMEOUT_HEADER) when it asks for more; a value it cannot read as it
 * is.
 *
 * @param {string} header the header's value, such as `30S` or `2147513S`
 * @returns {string} the value to arm the deadline with
 */
function clampedTimeout(header) {
  const match = /^(\d{1,8})\s*([HMSmun])$/.exec(header.trim());
  if (match === null) return header;
  const ms = Number(match[1]) * TIMEOUT_UNIT_MS[/** @type {keyof TIMEOUT_UNIT_MS} */ (match[2])];
  return ms > MAX_TIMEOUT_MS ? MAX_TIMEOUT_HEADER : header;
}

/**
 * Lets an incoming call keep a deadline longer than 24.8 days.
 *
 * grpc-js arms an incoming call's deadline with one setTimeout, its delay the
 * `grpc-timeout` header's value truncated to a 32-bit integer
 * (BaseServerInterceptingCall#handleTimeoutHeader in its server-interceptors),
 * so a deadline past 2^31 - 1 ms wraps to a negative delay and fails the call
 * at once with DEADLINE_EXCEEDED. The Rust harness sends such deadlines with
 * a producer's blocking calls when the producer's config allows a wait that
 * long (delivery.timeout.ms at Integer.MAX_VALUE, plus a margin:
 * rust/tests/common/multilanguage_producer.rs, blocking_deadline), and the
 * other servers' gRPC stacks keep them. So a longer deadline is armed as the
 * longest a timer keeps, which no test outlasts.
 */
function keepLongDeadlines() {
  const call = BaseServerInterceptingCall.prototype;
  const handleTimeoutHeader = call.handleTimeoutHeader;
  call.handleTimeoutHeader = function (/** @type {string} */ header) {
    return handleTimeoutHeader.call(this, clampedTimeout(String(header)));
  };
}

/** The ProducerService of the multilanguage protocol, over the client's producers. */
class ProducerService {
  #producers = new Map();
  #nextId = 1;
  #callbackLog = new CallbackLog();

  /** The handlers, by RPC name, for grpc-js's addService. */
  handlers() {
    const unary = (fn) => (call, callback) => {
      fn.call(this, call.request).then(
        (response) => callback(null, response),
        (e) => {
          log(`unexpected failure: ${e && e.stack ? e.stack : e}`);
          callback({ code: grpc.status.INTERNAL, details: `js server: ${e}` });
        },
      );
    };
    return {
      CreateProducer: unary(this.createProducer),
      Send: unary(this.send),
      Flush: unary(this.flush),
      PartitionsFor: unary(this.partitionsFor),
      Metrics: unary(this.metrics),
      Close: unary(this.close),
      CloseTimeout: unary(this.closeTimeout),
      GetCallbackLog: unary(this.getCallbackLog),
    };
  }

  async createProducer(request) {
    const config = request.config || {};
    let producer;
    try {
      // An empty config selects a MockProducer, as Python's server does:
      // Java's MockProducer(autoComplete, partitioner, keySerializer,
      // valueSerializer), completing every send at once, with no
      // serializers (the records' bytes are kept as they are).
      producer = selectsMock(config)
        ? new MockProducer({ autoComplete: true, partitioner: null, keySerializer: null, valueSerializer: null })
        : new KafkaProducer({ configs: config });
    } catch (e) {
      return { producer_id: 0, error: kafkaErrorToProto(e) };
    }
    const id = this.#nextId++;
    this.#producers.set(String(id), producer);
    return { producer_id: id };
  }

  async send(request) {
    const id = idOf(request.producer_id);
    const producer = this.#producers.get(id);
    if (producer === undefined) return { error: unknownProducer(id) };
    let record;
    try {
      record = protoToProducerRecord(request.record);
    } catch (e) {
      return { error: kafkaErrorToProto(e) };
    }
    // with_callback registers a real delivery callback through the client, so
    // the harness can read back (GetCallbackLog) what the binding's own
    // callback saw.
    const callback = request.with_callback ? makeLoggingDeliveryCallback(this.#callbackLog, id) : null;
    let future;
    try {
      // Resolves at hand-over (where Java's send() returns), rejecting with
      // what Java's send() would throw, a closed producer included.
      future = await producer.send({ record, callback });
    } catch (e) {
      // Raised by send() itself, Java's send throwing, as opposed to the
      // record's future failing below: raised_by_send tells the harness which.
      return { error: kafkaErrorToProto(e), raised_by_send: true };
    }
    try {
      const metadata = await withTimeout(future.get(), SEND_TIMEOUT_MS, () => new SendTimeout());
      return { metadata: recordMetadataToProto(metadata) };
    } catch (e) {
      if (e instanceof SendTimeout) {
        return {
          error: serverError(`producer future timed out after ${SEND_TIMEOUT_MS / 1000}s`, LOCAL_TIMEOUT),
        };
      }
      // get() rejects with ExecutionError (Java's ExecutionException) whose
      // cause is the delivery error, which is what the harness reads.
      return { error: kafkaErrorToProto(futureError(e)) };
    }
  }

  async flush(request) {
    const id = idOf(request.producer_id);
    const producer = this.#producers.get(id);
    if (producer === undefined) return { error: unknownProducer(id) };
    try {
      await producer.flush();
    } catch (e) {
      return { error: kafkaErrorToProto(e) };
    }
    return {};
  }

  async partitionsFor(request) {
    const id = idOf(request.producer_id);
    const producer = this.#producers.get(id);
    if (producer === undefined) return { error: unknownProducer(id) };
    let partitions;
    try {
      partitions = await producer.partitionsFor(request.topic === undefined ? '' : request.topic);
    } catch (e) {
      return { error: kafkaErrorToProto(e) };
    }
    return { partitions: partitions.map(partitionInfoToProto) };
  }

  async metrics(request) {
    const id = idOf(request.producer_id);
    const producer = this.#producers.get(id);
    if (producer === undefined) return { error: unknownProducer(id) };
    let metrics;
    try {
      metrics = producer.metrics();
    } catch (e) {
      return { error: kafkaErrorToProto(e) };
    }
    return { metrics: { metrics: [...metrics.values()].map(metricToProto) } };
  }

  async close(request) {
    return this.#close(idOf(request.producer_id), undefined);
  }

  async closeTimeout(request) {
    // Java's close(Duration). The wire carries int64 milliseconds, which a
    // JS number holds exactly only up to 2^53, the most close() takes: a
    // timeout past that (the harness sends i64::MAX for Duration::MAX) is
    // Infinity, no limit, which crosses as Long.MAX_VALUE ms. A negative one
    // is passed on for the client to reject.
    const ms = int64(request.timeout_ms) ?? 0n;
    // int64 gives a bigint; close() takes a number of milliseconds.
    const timeout = ms > BigInt(Number.MAX_SAFE_INTEGER) ? Infinity : Number(ms);
    return this.#close(idOf(request.producer_id), { timeout });
  }

  async #close(id, options) {
    const producer = this.#producers.get(id);
    // Close is idempotent: an unknown id is a silent success, as Java's
    // close() on a closed producer is.
    if (producer === undefined) return {};
    this.#producers.delete(id);
    try {
      // Java's close() takes no argument: `undefined` is not "not given".
      await (options === undefined ? producer.close() : producer.close(options));
    } catch (e) {
      return { error: kafkaErrorToProto(e) };
    }
    return {};
  }

  async getCallbackLog(request) {
    // Readable after Close on purpose: close() flushes, so the delivery
    // entries it drives land last.
    return this.#callbackLog.response(idOf(request.producer_id));
  }

  /** Close every producer still open, each bounded by `timeoutMs` milliseconds. */
  async closeAll(timeoutMs) {
    const producers = [...this.#producers.values()];
    this.#producers.clear();
    await Promise.allSettled(producers.map((p) => p.close({ timeout: timeoutMs })));
  }
}

/** A uint64 producer id (a decimal string under `longs: String`) as a map key. */
function idOf(producerId) {
  return producerId === undefined ? '0' : String(producerId);
}

function main() {
  const protoDir = process.env.GRPC_PROTO_DIR;
  if (!protoDir) {
    log('GRPC_PROTO_DIR must name the directory holding the service .proto files');
    process.exit(2);
  }
  const host = process.env.GRPC_HOST || '127.0.0.1';
  const port = process.env.GRPC_PORT === undefined || process.env.GRPC_PORT === '' ? 50054 : Number(process.env.GRPC_PORT);
  const pkg = loadProtos(protoDir);
  keepLongDeadlines();

  const service = new ProducerService();
  const chaos = new ChaosWorkloadService({ errorToProto: kafkaErrorToProto });
  const server = new grpc.Server();
  server.addService(pkg.ProducerService.service, service.handlers());
  server.addService(pkg.ChaosWorkloadService.service, chaos.handlers());

  server.bindAsync(`${host}:${port}`, grpc.ServerCredentials.createInsecure(), (err, boundPort) => {
    if (err) {
      log(`failed to bind ${host}:${port}: ${err.message}`);
      process.exit(1);
    }
    log(`listening on ${host}:${boundPort}`);
  });

  let shuttingDown = false;
  const shutdown = (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    log(`${signal}: closing producers and shutting down`);
    // The chaos workloads stop first, before the producers close.
    chaos
      .stopAll()
      .then(() => service.closeAll(SHUTDOWN_CLOSE_TIMEOUT_MS))
      .finally(() => {
        // In-flight calls (a Send awaiting delivery) would hold tryShutdown open.
        setTimeout(() => {
          server.forceShutdown();
          process.exit(0);
        }, 2_000).unref();
        server.tryShutdown(() => process.exit(0));
      });
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

// Run as a program it serves; required (test/grpc_server/), it only hands out
// the service, whose handlers the tests call in-process.
if (require.main === module) main();

module.exports = { ProducerService, clampedTimeout, selectsMock };
