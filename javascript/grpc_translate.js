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

// Proto <-> JavaScript translation helpers for grpc_server.js, the JavaScript
// twin of python/grpc_translate.py (producer half only: the JavaScript binding
// has no consumer or admin client yet).
//
// Messages are the plain objects @grpc/proto-loader hands out with the options
// grpc_server.js loads the protos with (keepCase, longs: String,
// defaults: false, oneofs: true): field names are the proto's snake_case, int64
// / uint64 fields arrive as decimal strings, and an unset field -- a proto3
// `optional` one or a scalar at its default -- is absent (undefined).

const { KafkaError } = require('./lib/common');
const { ProducerRecord } = require('./lib/producer');

// The FFI error codes (kafka_common_Error_e in confluent_kafka_bindings.h,
// mirrored by rust/tests/common/error_code.rs) of the only errors this server
// manufactures, so the Rust harness can tell them apart from an error the
// client reported (python/grpc_translate.py's _LOCAL_ILLEGAL_STATE /
// _REQUEST_TIMED_OUT, plus the -31 the .NET servers stamp for what their
// binding lacks).
const LOCAL_ILLEGAL_STATE = -4;
const LOCAL_UNSUPPORTED_OPERATION = -31;
// Java's wire TimeoutException code, which the harness decodes as Error::Timeout.
const REQUEST_TIMED_OUT = 7;

// The prefix of every message this server writes itself, naming the server
// that manufactured it ("python server: " / "c server: " in the other servers).
const SERVER_PREFIX = 'js server: ';

/** The CallbackLogEntry.kind of a producer delivery callback. */
const KIND_DELIVERY = 'delivery';

/**
 * Translate an error into a proto KafkaError (python/grpc_translate.py's
 * `_kafka_error_to_proto`).
 *
 * A KafkaError from the client crosses with its FFI code and the core's message
 * unchanged: `code` is the only discriminator the proto carries, and it is
 * injective over the client's error classes. Anything else (a TypeError from
 * an argument check, a bug in this server) is this server's own failure, not
 * an error the client reported, so it is stamped LOCAL_ILLEGAL_STATE with the
 * error's name in the message.
 *
 * @param {unknown} err
 * @returns {{code: number, message: string}}
 */
function kafkaErrorToProto(err) {
  if (err instanceof KafkaError) return { code: err.code, message: err.message || '' };
  if (err instanceof Error) return serverError(`${err.name}: ${err.message}`);
  return serverError(`${typeof err}: ${String(err)}`);
}

/** A KafkaError of this server's own making, LOCAL_ILLEGAL_STATE by default. */
function serverError(message, code = LOCAL_ILLEGAL_STATE) {
  return { code, message: `${SERVER_PREFIX}${message}` };
}

/** The error for a request naming a producer this server does not hold. */
function unknownProducer(producerId) {
  return { code: LOCAL_ILLEGAL_STATE, message: `unknown producer_id ${producerId}` };
}

/**
 * A proto int64 (a decimal string under `longs: String`) as a Number, or null
 * when absent. Values beyond Number.MAX_SAFE_INTEGER are rejected by the
 * client's own range check (ProducerRecord) rather than silently rounded.
 */
function int64(value) {
  if (value === undefined || value === null) return null;
  return Number(value);
}

/**
 * Translate a proto ProducerRecord into a ProducerRecord
 * (python/grpc_translate.py's `_proto_to_producer_record`).
 *
 * Only the fields the request carries are passed: an absent `optional`
 * partition / timestamp / key / value is Java's null (an absent value is a
 * tombstone). Headers are forwarded as the C++ server does: the proto's header
 * value is plain `bytes`, so a null header value arrives as an empty one.
 *
 * @throws {KafkaError | TypeError | RangeError} what ProducerRecord's own checks throw
 */
function protoToProducerRecord(proto) {
  const record = proto || {};
  const options = { topic: record.topic === undefined ? '' : record.topic };
  if (record.partition !== undefined) options.partition = record.partition;
  if (record.timestamp !== undefined) options.timestamp = int64(record.timestamp);
  if (record.key !== undefined) options.key = record.key;
  if (record.value !== undefined) options.value = record.value;
  const headers = record.headers || [];
  if (headers.length > 0) {
    options.headers = headers.map((h) => ({ key: h.key === undefined ? '' : h.key, value: h.value || Buffer.alloc(0) }));
  }
  return new ProducerRecord(options);
}

/** Translate a RecordMetadata into its proto form (`_record_metadata_to_proto`). */
function recordMetadataToProto(metadata) {
  return {
    offset: metadata.offset,
    timestamp: metadata.timestamp,
    serialized_key_size: metadata.serializedKeySize,
    serialized_value_size: metadata.serializedValueSize,
    topic: metadata.topic,
    partition: metadata.partition,
  };
}

/** The CallbackLogEntry.offsets key for a partition: "<topic>-<partition>". */
function offsetKey(topic, partition) {
  return `${topic}-${partition}`;
}

/**
 * Per-producer log of user-callback invocations, keyed by the server-local
 * producer id (python/grpc_translate.py's CallbackLog). Everything runs on the
 * one JS thread, so it needs no lock.
 *
 * Entries are never dropped, not even when the producer is closed: the
 * delivery callbacks a close() drives are exactly the ones a test reads
 * afterwards. The server lives for one test session, so the map stays small.
 */
class CallbackLog {
  #entries = new Map();

  /**
   * Record one invocation.
   *
   * @param {string} clientId the server-local producer id
   * @param {string} kind a CallbackLogEntry kind
   * @param {Array<{topic: string, partition: number}>} partitions
   * @param {Record<string, number>} offsets `{"<topic>-<partition>": offset}`
   * @param {string} error the error message, or "" when the callback saw none
   */
  append(clientId, kind, partitions, offsets, error) {
    let entries = this.#entries.get(clientId);
    if (entries === undefined) {
      entries = [];
      this.#entries.set(clientId, entries);
    }
    entries.push({ kind, partitions, offsets, error: error || '' });
  }

  /** The client's log as a CallbackLogResponse, oldest entry first. */
  response(clientId) {
    return { entries: [...(this.#entries.get(clientId) || [])] };
  }
}

/**
 * A real delivery Callback recording the delivered record's metadata
 * (python/grpc_translate.py's `make_logging_delivery_callback`).
 *
 * The metadata is never null: on failure it is Java's placeholder with -1
 * offset beside the error, recorded as it is.
 */
function makeLoggingDeliveryCallback(log, clientId) {
  return (metadata, error) => {
    const { topic, partition } = metadata;
    log.append(
      clientId,
      KIND_DELIVERY,
      [{ topic, partition }],
      { [offsetKey(topic, partition)]: metadata.offset },
      error === null || error === undefined ? '' : error.message,
    );
  };
}

module.exports = {
  CallbackLog,
  KIND_DELIVERY,
  LOCAL_ILLEGAL_STATE,
  LOCAL_UNSUPPORTED_OPERATION,
  REQUEST_TIMED_OUT,
  int64,
  kafkaErrorToProto,
  makeLoggingDeliveryCallback,
  protoToProducerRecord,
  recordMetadataToProto,
  serverError,
  unknownProducer,
};
