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

// errorFfiId: the private FFI id of an error of the client's (an instance of
// KafkaError or of a Java built-in such as IllegalStateError): the core's id
// for an error the core reported, its class's otherwise; undefined for
// anything else.
const { UNKNOWN_SERVER_ERROR, errorFfiId } = require('../dist/internal/errors');
const { ExecutionError } = require('../dist');
const { KafkaError } = require('../dist/common');
const { ApiError, GroupAuthorizationError, InvalidTopicError, TopicAuthorizationError } = require('../dist/common/errors');
const { ProducerRecord } = require('../dist/producer');

// The FFI error codes (kafka_common_Error_e in confluent_kafka_bindings.h,
// mirrored by rust/tests/common/error_code.rs) of the only errors this server
// manufactures, so the Rust harness can tell them apart from an error the
// client reported (python/grpc_translate.py's _LOCAL_ILLEGAL_STATE /
// _LOCAL_TIMEOUT, plus the -31 the .NET servers stamp for what their
// binding lacks).
const LOCAL_ILLEGAL_STATE = -4;
const LOCAL_UNSUPPORTED_OPERATION = -31;
// A bounded wait of this server's own that expired (Send's 120 s): Java's
// java.util.concurrent.TimeoutException, the root TimeoutError, not Kafka's
// wire TimeoutException (REQUEST_TIMED_OUT, 7), which a test may accept as the
// broker's answer although the send never completed.
const LOCAL_TIMEOUT = -5;

// The prefix of every message this server writes itself, naming the server
// that manufactured it ("python server: " / "c server: " in the other servers).
const SERVER_PREFIX = 'js server: ';

/** The CallbackLogEntry.kind of a producer delivery callback. */
const KIND_DELIVERY = 'delivery';

/**
 * Translate an error into a proto KafkaError (python/grpc_translate.py's
 * `_kafka_error_to_proto`).
 *
 * An error from the client (a KafkaError or a Java built-in such as
 * IllegalStateError) crosses with its FFI id (private to the client, read with
 * errorFfiId), the core's message unchanged, and what `errorDetails` adds:
 * `code` is the discriminator the proto carries, injective over the client's
 * error classes but for -1, which `bare_kafka_error` splits. Anything else (a
 * TypeError from an argument check, a bug in this server) is this server's
 * own failure, not an error the client reported, so it is stamped
 * LOCAL_ILLEGAL_STATE with the error's name in the message.
 *
 * @param {unknown} err
 * @returns {{code: number, message: string}} the proto KafkaError; for a
 *   client error, with the fields of `errorDetails` it has
 */
function kafkaErrorToProto(err) {
  const code = errorFfiId(err);
  if (code !== undefined) {
    const error = /** @type {Error} */ (err);
    return { code, message: error.message || '', ...errorDetails(code, error) };
  }
  if (err instanceof Error) return serverError(`${err.name}: ${err.message}`);
  return serverError(`${typeof err}: ${String(err)}`);
}

/**
 * The fields a proto KafkaError carries beside its code and message
 * (python/grpc_translate.py's `_fill_error_payload`), each only when the error
 * has it: the class-specific payload, read through the error's own getters
 * (the consumer's payloads have no class here: the binding has no consumer);
 * `bare_kafka_error`, true for a bare KafkaError (Java's KafkaException),
 * which shares -1 with UnknownServerError, an ApiError; and the cause, one
 * level deep (Java's getCause()), when it is a client error.
 *
 * @param {number} code the error's FFI id
 * @param {Error} err a client error
 * @returns {Record<string, unknown>} the fields
 */
function errorDetails(code, err) {
  /** @type {Record<string, unknown>} */
  const out = {};
  const unauthorizedTopics = err instanceof TopicAuthorizationError ? sortedStrings(err.unauthorizedTopics()) : [];
  if (unauthorizedTopics.length > 0) out.unauthorized_topics = unauthorizedTopics;
  const invalidTopics = err instanceof InvalidTopicError ? sortedStrings(err.invalidTopics()) : [];
  if (invalidTopics.length > 0) out.invalid_topics = invalidTopics;
  if (err instanceof GroupAuthorizationError && err.groupId() !== null) out.group_id = err.groupId();
  if (isBareKafkaError(code, err)) out.bare_kafka_error = true;
  const causeCode = errorFfiId(err.cause);
  if (causeCode !== undefined) {
    const cause = /** @type {Error} */ (err.cause);
    /** @type {{code: number, message: string, bare_kafka_error?: boolean}} */
    const proto = { code: causeCode, message: cause.message || '' };
    if (isBareKafkaError(causeCode, cause)) proto.bare_kafka_error = true;
    out.cause = proto;
  }
  return out;
}

/**
 * Whether a client error of FFI id `code` is a bare KafkaError (Java's
 * KafkaException), not an ApiError. A Java built-in with no id of its own,
 * such as ExecutionError, also crosses as -1, but is no KafkaError.
 *
 * @param {number} code the error's FFI id
 * @param {unknown} err the error
 * @returns {boolean} whether it is a bare KafkaError
 */
function isBareKafkaError(code, err) {
  return code === UNKNOWN_SERVER_ERROR && err instanceof KafkaError && !(err instanceof ApiError);
}

/**
 * A set of strings as the proto's sorted list, so the encoding is the same on
 * every run; null (no set) as an empty one.
 *
 * @param {Set<string> | null | undefined} set the set
 * @returns {string[]} its strings, sorted
 */
function sortedStrings(set) {
  return set === null || set === undefined ? [] : [...set].sort();
}

/**
 * The error a failed future's get() reports: Java's ExecutionError wraps the
 * delivery error as its cause, and the cause is what crosses the proto, as
 * python/grpc_server.py sends `e.__cause__`; any other error as it is.
 *
 * @param {unknown} err
 * @returns {unknown}
 */
function futureError(err) {
  return err instanceof ExecutionError && err.cause !== null && err.cause !== undefined ? err.cause : err;
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
 * A proto int64 (a decimal string under `longs: String`) as a bigint, Java's
 * long (exact at every value), or null when absent.
 */
function int64(value) {
  if (value === undefined || value === null) return null;
  return BigInt(value);
}

/**
 * Translate a proto ProducerRecord into a ProducerRecord
 * (python/grpc_translate.py's `_proto_to_producer_record`).
 *
 * Java's `(topic, partition, timestamp, key, value)` constructor, as Python's
 * translation calls it, or the one with `headers` when the request carries
 * headers: an absent `optional` partition / timestamp / key / value is Java's
 * null (an absent value is a tombstone). Headers are forwarded as the C++
 * server does: the proto's header value is plain `bytes`, so a null header
 * value arrives as an empty one.
 *
 * @throws {import('../dist/illegal_argument_error').IllegalArgumentError | TypeError | RangeError} what ProducerRecord's own checks throw
 */
function protoToProducerRecord(proto) {
  const record = proto || {};
  const options = {
    topic: record.topic === undefined ? '' : record.topic,
    partition: record.partition === undefined ? null : record.partition,
    timestamp: record.timestamp === undefined ? null : int64(record.timestamp),
    key: record.key === undefined ? null : record.key,
    value: record.value === undefined ? null : record.value,
  };
  const headers = record.headers || [];
  if (headers.length > 0) {
    options.headers = headers.map((h) => [h.key === undefined ? '' : h.key, h.value || Buffer.alloc(0)]);
  }
  return new ProducerRecord(options);
}

/** Translate a RecordMetadata into its proto form (`_record_metadata_to_proto`). */
function recordMetadataToProto(metadata) {
  return {
    // int64s, written as decimal strings, exact at every value.
    offset: metadata.offset.toString(),
    timestamp: metadata.timestamp.toString(),
    serialized_key_size: metadata.serializedKeySize,
    serialized_value_size: metadata.serializedValueSize,
    topic: metadata.topic,
    partition: metadata.partition,
  };
}

/** A Node as the proto's Node (python/grpc_translate.py's _node_to_proto). */
function nodeToProto(node) {
  if (node === null) return undefined;
  const out = { id: node.id, host: node.host === null ? '' : node.host, port: node.port, is_fenced: node.isFenced };
  if (node.rack !== null) out.rack = node.rack;
  return out;
}

/** A PartitionInfo as the proto's PartitionInfo (python/grpc_translate.py's _partition_info_to_proto). */
function partitionInfoToProto(info) {
  const nodes = (array) => (array === null ? [] : array.map(nodeToProto));
  const out = {
    topic: info.topic(),
    partition: info.partition(),
    replicas: nodes(info.replicas()),
    in_sync_replicas: nodes(info.inSyncReplicas()),
    offline_replicas: nodes(info.offlineReplicas()),
  };
  const leader = nodeToProto(info.leader());
  if (leader !== undefined) out.leader = leader;
  return out;
}

/**
 * One entry of a metrics() snapshot as the proto's Metric
 * (python/grpc_translate.py's _metric_to_proto): the MetricName's fields and
 * the value, a bigint (Java's long) as long_value, a string as string_value
 * and a number as double_value.
 */
function metricToProto(metric) {
  const name = metric.metricName();
  const out = { name: name.name(), group: name.group(), description: name.description(), tags: Object.fromEntries(name.tags()) };
  const value = metric.metricValue();
  if (typeof value === 'string') out.string_value = value;
  else if (typeof value === 'bigint') out.long_value = value.toString();
  else out.double_value = Number(value);
  return out;
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
   * The error the callback saw is logged as its message, its FFI code
   * (`error_code`) and, as `error_detail`, the whole proto KafkaError a reply
   * carries (`kafkaErrorToProto`), as python/grpc_translate.py's
   * CallbackLog.append logs it. A callback that saw none is logged with an
   * empty message and neither field.
   *
   * @param {string} clientId the server-local producer id
   * @param {string} kind a CallbackLogEntry kind
   * @param {Array<{topic: string, partition: number}>} partitions
   * @param {Record<string, string>} offsets `{"<topic>-<partition>": offset}`, each a decimal int64
   * @param {unknown} error the error the callback saw, or null / undefined when it saw none
   */
  append(clientId, kind, partitions, offsets, error) {
    let entries = this.#entries.get(clientId);
    if (entries === undefined) {
      entries = [];
      this.#entries.set(clientId, entries);
    }
    /** @type {Record<string, unknown>} */
    const entry = { kind, partitions, offsets, error: '' };
    if (error !== null && error !== undefined) {
      const detail = kafkaErrorToProto(error);
      entry.error = error instanceof Error ? error.message : String(error);
      entry.error_code = detail.code;
      entry.error_detail = detail;
    }
    entries.push(entry);
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
  return ({ metadata, exception }) => {
    const { topic, partition } = metadata;
    log.append(clientId, KIND_DELIVERY, [{ topic, partition }], { [offsetKey(topic, partition)]: metadata.offset.toString() }, exception);
  };
}

module.exports = {
  CallbackLog,
  KIND_DELIVERY,
  LOCAL_ILLEGAL_STATE,
  LOCAL_TIMEOUT,
  LOCAL_UNSUPPORTED_OPERATION,
  futureError,
  int64,
  kafkaErrorToProto,
  makeLoggingDeliveryCallback,
  protoToProducerRecord,
  metricToProto,
  partitionInfoToProto,
  recordMetadataToProto,
  serverError,
  unknownProducer,
};
