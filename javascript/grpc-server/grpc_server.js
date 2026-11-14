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
// than this server inventing a result:
//   - InitTransactions, BeginTransaction, CommitTransaction, AbortTransaction,
//     SendOffsetsToTransaction: no transaction surface;
//   - PartitionsFor: no partitionsFor();
//   - Metrics: no metrics().
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
const protoLoader = require('@grpc/proto-loader');

const { KafkaProducer } = require('../dist/producer');
const {
  CallbackLog,
  LOCAL_UNSUPPORTED_OPERATION,
  REQUEST_TIMED_OUT,
  futureError,
  int64,
  kafkaErrorToProto,
  makeLoggingDeliveryCallback,
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
      Close: unary(this.close),
      CloseTimeout: unary(this.closeTimeout),
      GetCallbackLog: unary(this.getCallbackLog),
    };
  }

  async createProducer(request) {
    const config = request.config || {};
    if (selectsMock(config)) {
      // Python creates a MockProducer here; the JS binding has none.
      return {
        producer_id: 0,
        error: serverError('MockProducer is not implemented', LOCAL_UNSUPPORTED_OPERATION),
      };
    }
    let producer;
    try {
      producer = new KafkaProducer({ configs: config });
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
      return { error: kafkaErrorToProto(e) };
    }
    try {
      const metadata = await withTimeout(future.get(), SEND_TIMEOUT_MS, () => new SendTimeout());
      return { metadata: recordMetadataToProto(metadata) };
    } catch (e) {
      if (e instanceof SendTimeout) {
        return {
          error: serverError(`producer future timed out after ${SEND_TIMEOUT_MS / 1000}s`, REQUEST_TIMED_OUT),
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

main();
