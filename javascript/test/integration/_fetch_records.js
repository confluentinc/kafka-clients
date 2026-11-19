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

// A minimal Kafka protocol client for the integration tests, no part of the
// client. Fetch (v4) reads back what the producer wrote, so a test can check
// record values and batch fields (codec, timestamp type, producer id and base
// sequence), not only metadata; gzip and zstd batches are decoded with
// node:zlib, snappy and lz4 ones are reported without their records. The
// admin requests (Metadata v4, CreateTopics v4, DeleteTopics v3,
// CreatePartitions v1, IncrementalAlterConfigs v0) create and change the
// topics a test needs. It speaks to one broker (the single-broker test
// cluster). The container control at the end works only when the Makefile
// started that broker (KAFKA_CONTAINER), so the suite still runs against any
// KAFKA_BOOTSTRAP_SERVERS.

const { spawnSync } = require('node:child_process');
const net = require('node:net');
const zlib = require('node:zlib');

/** @param {string} s */
function writeString(s) {
  const b = Buffer.from(s, 'utf8');
  const len = Buffer.alloc(2);
  len.writeInt16BE(b.length);
  return Buffer.concat([len, b]);
}

/** @param {number} v */
function int32(v) {
  const b = Buffer.alloc(4);
  b.writeInt32BE(v);
  return b;
}

/**
 * @param {number} correlationId
 * @param {string} topic
 * @param {number} partition
 * @param {number} offset
 */
function fetchRequest(correlationId, topic, partition, offset) {
  const header = Buffer.concat([
    Buffer.from([0, 1, 0, 4]), // Fetch, v4
    int32(correlationId),
    writeString('js-it-fetch'),
  ]);
  const p = Buffer.alloc(16);
  p.writeInt32BE(partition, 0);
  p.writeBigInt64BE(BigInt(offset), 4);
  p.writeInt32BE(16 * 1024 * 1024, 12);
  const body = Buffer.concat([
    int32(-1), // replica_id
    int32(500), // max_wait_ms
    int32(1), // min_bytes
    int32(64 * 1024 * 1024), // max_bytes
    Buffer.from([0]), // isolation_level READ_UNCOMMITTED
    int32(1),
    writeString(topic),
    int32(1),
    p,
  ]);
  const msg = Buffer.concat([header, body]);
  return Buffer.concat([int32(msg.length), msg]);
}

class Reader {
  /** @param {Buffer} buf */
  constructor(buf) {
    this.buf = buf;
    this.pos = 0;
  }
  i8() {
    return this.buf.readInt8(this.pos++);
  }
  i16() {
    const v = this.buf.readInt16BE(this.pos);
    this.pos += 2;
    return v;
  }
  i32() {
    const v = this.buf.readInt32BE(this.pos);
    this.pos += 4;
    return v;
  }
  i64() {
    return Number(this.i64big());
  }
  i64big() {
    const v = this.buf.readBigInt64BE(this.pos);
    this.pos += 8;
    return v;
  }
  nullableString() {
    const len = this.i16();
    if (len < 0) return null;
    const s = this.buf.toString('utf8', this.pos, this.pos + len);
    this.pos += len;
    return s;
  }
  string() {
    const len = this.i16();
    const s = this.buf.toString('utf8', this.pos, this.pos + len);
    this.pos += len;
    return s;
  }
  /** @param {number} len */
  bytes(len) {
    const b = this.buf.subarray(this.pos, this.pos + len);
    this.pos += len;
    return b;
  }
  varlong() {
    let shift = 0n;
    let raw = 0n;
    for (;;) {
      const byte = this.buf[this.pos++];
      raw |= BigInt(byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) break;
      shift += 7n;
    }
    return Number((raw >> 1n) ^ -(raw & 1n));
  }
  nullableVarBytes() {
    const len = this.varlong();
    return len < 0 ? null : Buffer.from(this.bytes(len));
  }
}

/** The codecs of a batch's attributes, bits 0-2 (org.apache.kafka.common.record.CompressionType). */
const CODECS = ['none', 'gzip', 'snappy', 'lz4', 'zstd'];

/**
 * A fetched record: `headers` an array of {key, value, keyBytes} (the key
 * decoded as UTF-8, and its raw bytes) and `timestamp` the batch's base
 * timestamp plus the record's delta (the record's CreateTime on a CreateTime
 * topic, the batch's max timestamp being the append time on a LogAppendTime
 * one).
 *
 * @typedef {{ offset: number, timestamp: number, key: Buffer | null, value: Buffer | null, headers: { key: string, value: Buffer | null, keyBytes: Buffer }[] }} FetchedRecord
 */

/**
 * A fetched v2 record batch: its header fields, and its records (`null` for
 * a snappy or lz4 batch, which these tests do not decode).
 *
 * @typedef {{ baseOffset: number, lastOffsetDelta: number, attributes: number, codec: string, logAppendTime: boolean, transactional: boolean, control: boolean, baseTimestamp: bigint, maxTimestamp: bigint, producerId: bigint, producerEpoch: number, baseSequence: number, count: number, records: FetchedRecord[] | null }} FetchedBatch
 */

/**
 * The records of a batch's (decompressed) records section.
 *
 * @param {Buffer} buf
 * @param {number} count
 * @param {number} baseOffset
 * @param {number} baseTimestamp
 * @returns {FetchedRecord[]}
 */
function parseBatchRecords(buf, count, baseOffset, baseTimestamp) {
  const r = new Reader(buf);
  /** @type {FetchedRecord[]} */
  const out = [];
  for (let i = 0; i < count; i++) {
    r.varlong(); // record length
    r.i8(); // attributes
    const timestampDelta = r.varlong();
    const offsetDelta = r.varlong();
    const key = r.nullableVarBytes();
    const value = r.nullableVarBytes();
    const headerCount = r.varlong();
    const headers = [];
    for (let h = 0; h < headerCount; h++) {
      const keyBytes = Buffer.from(r.bytes(r.varlong()));
      headers.push({ key: keyBytes.toString('utf8'), value: r.nullableVarBytes(), keyBytes });
    }
    out.push({ offset: baseOffset + offsetDelta, timestamp: baseTimestamp + timestampDelta, key, value, headers });
  }
  return out;
}

/**
 * The v2 batches of a record set; a partial batch at the end is left out.
 *
 * @param {Buffer} buf
 * @returns {FetchedBatch[]}
 */
function parseBatches(buf) {
  /** @type {FetchedBatch[]} */
  const out = [];
  const r = new Reader(buf);
  while (buf.length - r.pos >= 12) {
    const baseOffset = r.i64();
    const batchLength = r.i32();
    if (buf.length - r.pos < batchLength) break; // a partial batch at the end
    const end = r.pos + batchLength;
    r.i32(); // partition leader epoch
    const magic = r.i8();
    if (magic !== 2) throw new Error(`unsupported magic ${magic}`);
    r.i32(); // crc
    const attributes = r.i16();
    const lastOffsetDelta = r.i32();
    const baseTimestamp = r.i64big();
    const maxTimestamp = r.i64big();
    const producerId = r.i64big();
    const producerEpoch = r.i16();
    const baseSequence = r.i32();
    const count = r.i32();
    const codec = CODECS[attributes & 0x7] ?? `codec ${attributes & 0x7}`;
    const section = r.bytes(end - r.pos);
    /** @type {Buffer | null} */
    let plain = null;
    if (codec === 'none') plain = section;
    else if (codec === 'gzip') plain = zlib.gunzipSync(section);
    else if (codec === 'zstd') {
      // Node 22.15 and later; the pinned @types/node predates it.
      const { zstdDecompressSync } = /** @type {{ zstdDecompressSync: (buf: Buffer) => Buffer }} */ (/** @type {unknown} */ (zlib));
      plain = zstdDecompressSync(section);
    }
    out.push({
      baseOffset,
      lastOffsetDelta,
      attributes,
      codec,
      logAppendTime: (attributes & 0x8) !== 0,
      transactional: (attributes & 0x10) !== 0,
      control: (attributes & 0x20) !== 0,
      baseTimestamp,
      maxTimestamp,
      producerId,
      producerEpoch,
      baseSequence,
      count,
      records: plain === null ? null : parseBatchRecords(plain, count, baseOffset, Number(baseTimestamp)),
    });
    r.pos = end;
  }
  return out;
}

// The data records of a record set: [{offset, timestamp, key, value,
// headers}] (control records left out). A snappy or lz4 batch fails it.
/**
 * @param {Buffer} buf
 * @returns {FetchedRecord[]}
 */
function parseRecords(buf) {
  /** @type {FetchedRecord[]} */
  const out = [];
  for (const batch of parseBatches(buf)) {
    if (batch.records === null) throw new Error(`${batch.codec} batches are not decoded`);
    if (!batch.control) out.push(...batch.records);
  }
  return out;
}

/** @typedef {Record<string, (...args: unknown[]) => void>} Listeners */

// A Promise over `socket`: `start(listeners, done)` adds its own listeners to
// the socket and to `listeners` and calls `done(value)` to resolve; 'error'
// rejects with the error, 'end' and 'close' with the message `closed()` gives.
// Once it settles every listener in `listeners` is removed, so none piles up
// across calls.
/**
 * @template [T=void]
 * @param {import('node:net').Socket} socket
 * @param {() => string} closed
 * @param {(listeners: Listeners, done: (value: T) => void) => void} start
 * @returns {Promise<T>}
 */
function settleOn(socket, closed, start) {
  return new Promise((resolve, reject) => {
    /** @type {Listeners} */
    const listeners = {};
    /**
     * @template U
     * @param {(value: U) => void} fn
     * @param {U} value
     */
    const settle = (fn, value) => {
      for (const [event, listener] of Object.entries(listeners)) socket.off(event, listener);
      fn(value);
    };
    listeners.error = (error) => settle(reject, error);
    listeners.end = () => settle(reject, new Error(closed()));
    listeners.close = () => settle(reject, new Error(closed()));
    for (const [event, listener] of Object.entries(listeners)) socket.on(event, listener);
    start(listeners, (value) => settle(resolve, value));
  });
}

/**
 * How long a request waits for its response: longer than any timeout a
 * request here gives the broker (CreateTopics' 30 s), so only a response that
 * never comes reaches it, as after a pause of the broker's container.
 */
const RESPONSE_TIMEOUT_MS = 45000;

/**
 * Writes `request` and resolves to the response's body, once complete; rejects
 * when the socket fails, ends or closes first, or when no complete response
 * came within `timeoutMs` (the socket is then destroyed).
 *
 * @param {import('node:net').Socket} socket
 * @param {Buffer} request
 * @param {number} [timeoutMs]
 * @returns {Promise<Buffer>}
 */
function roundTrip(socket, request, timeoutMs = RESPONSE_TIMEOUT_MS) {
  let chunks = Buffer.alloc(0);
  const timer = setTimeout(
    () => socket.destroy(new Error(`no complete response within ${timeoutMs} ms (${chunks.length} bytes came)`)),
    timeoutMs,
  );
  /** @type {Promise<Buffer>} */
  const response = settleOn(
    socket,
    () => `the connection closed after ${chunks.length} bytes of the response`,
    (listeners, done) => {
      listeners.data = (chunk) => {
        // A 'data' listener on a socket without an encoding gets a Buffer.
        chunks = Buffer.concat([chunks, /** @type {Buffer} */ (chunk)]);
        if (chunks.length >= 4 && chunks.length >= 4 + chunks.readInt32BE(0)) {
          done(chunks.subarray(4, 4 + chunks.readInt32BE(0)));
        }
      };
      socket.on('data', listeners.data);
      socket.write(request);
    },
  );
  return response.finally(() => clearTimeout(timer));
}

/**
 * Connects to `host:port`; rejects when the socket fails or closes first.
 *
 * @param {string} host
 * @param {number} port
 */
async function connect(host, port) {
  const socket = net.connect({ host, port });
  await settleOn(
    socket,
    () => `the connection to ${host}:${port} closed before it was established`,
    (listeners, done) => {
      listeners.connect = () => done();
      socket.on('connect', listeners.connect);
    },
  ).catch((error) => {
    socket.destroy();
    throw error;
  });
  return socket;
}

/**
 * Reads partition `partition` of `topic` from offset 0 up to its high
 * watermark: `{highWatermark, batches}`, every v2 batch fetched.
 *
 * @param {string} bootstrap
 * @param {string} topic
 * @param {number} partition
 * @returns {Promise<{ highWatermark: number, batches: FetchedBatch[] }>}
 */
async function fetchBatches(bootstrap, topic, partition) {
  const [host, port] = bootstrap.split(',')[0].split(':');
  const socket = await connect(host, Number(port));
  // An error between two round trips, with none of theirs listening, would
  // otherwise be an uncaught 'error' event: the next round trip sees the
  // socket closed instead.
  socket.on('error', () => {});
  try {
    /** @type {FetchedBatch[]} */
    const batches = [];
    let highWatermark = -1;
    let offset = 0;
    for (let correlationId = 1; ; correlationId++) {
      const r = new Reader(await roundTrip(socket, fetchRequest(correlationId, topic, partition, offset)));
      if (r.i32() !== correlationId) throw new Error('correlation id mismatch');
      r.i32(); // throttle time
      r.i32(); // one topic
      r.string();
      r.i32(); // one partition
      r.i32();
      const errorCode = r.i16();
      if (errorCode !== 0) throw new Error(`fetch error code ${errorCode}`);
      highWatermark = r.i64();
      r.i64(); // last stable offset
      const aborted = r.i32();
      for (let i = 0; i < aborted; i++) r.bytes(16);
      const size = r.i32();
      const fetched = parseBatches(size < 0 ? Buffer.alloc(0) : r.bytes(size));
      // A fetch from the middle of a batch returns the whole batch again.
      for (const batch of fetched) if (batch.baseOffset + batch.lastOffsetDelta >= offset) batches.push(batch);
      if (batches.length > 0) {
        const last = batches[batches.length - 1];
        offset = last.baseOffset + last.lastOffsetDelta + 1;
      }
      if (offset >= highWatermark || fetched.length === 0) break;
    }
    return { highWatermark, batches };
  } finally {
    socket.destroy();
  }
}

/**
 * Reads partition `partition` of `topic` from offset 0 up to its high
 * watermark: `{highWatermark, records: [{offset, timestamp, key, value,
 * headers}]}`, the data records (control records left out).
 *
 * @param {string} bootstrap
 * @param {string} topic
 * @param {number} partition
 * @returns {Promise<{ highWatermark: number, records: FetchedRecord[] }>}
 */
async function fetchRecords(bootstrap, topic, partition) {
  const { highWatermark, batches } = await fetchBatches(bootstrap, topic, partition);
  /** @type {FetchedRecord[]} */
  const records = [];
  for (const batch of batches) {
    if (batch.records === null) throw new Error(`${batch.codec} batches are not decoded`);
    if (!batch.control) for (const record of batch.records) if (record.offset >= 0) records.push(record);
  }
  // A batch fetched twice (a fetch from its middle) contributes its records once.
  const seen = new Set();
  return { highWatermark, records: records.filter((record) => !seen.has(record.offset) && seen.add(record.offset)) };
}

// The admin requests: a request header v1 (api key, version, correlation id,
// client id) and a non-flexible body, one round trip on a new connection.

/** @param {number} v */
function int16(v) {
  const b = Buffer.alloc(2);
  b.writeInt16BE(v);
  return b;
}

/** @param {string | null} s */
function writeNullableString(s) {
  return s === null ? int16(-1) : writeString(s);
}

/**
 * A non-flexible array: its int32 count, then each element's bytes.
 *
 * @template T
 * @param {T[]} items
 * @param {(item: T) => Buffer} write
 */
function array(items, write) {
  return Buffer.concat([int32(items.length), ...items.map(write)]);
}

let adminCorrelationId = 1000;

/**
 * Sends one request to the first bootstrap broker and returns a Reader over
 * its response body, after the correlation id.
 *
 * @param {string} bootstrap
 * @param {number} apiKey
 * @param {number} apiVersion
 * @param {Buffer} body
 * @returns {Promise<Reader>}
 */
async function request(bootstrap, apiKey, apiVersion, body) {
  const [host, port] = bootstrap.split(',')[0].split(':');
  const socket = await connect(host, Number(port));
  socket.on('error', () => {});
  try {
    const correlationId = adminCorrelationId++;
    const msg = Buffer.concat([int16(apiKey), int16(apiVersion), int32(correlationId), writeString('js-it-admin'), body]);
    const r = new Reader(await roundTrip(socket, Buffer.concat([int32(msg.length), msg])));
    if (r.i32() !== correlationId) throw new Error('correlation id mismatch');
    return r;
  } finally {
    socket.destroy();
  }
}

/**
 * The partitions of `topic` from a Metadata (v4) request that does not create
 * it: `{errorCode, partitions: [{partition, errorCode, leader}]}`.
 *
 * @param {string} bootstrap
 * @param {string} topic
 * @returns {Promise<{ errorCode: number, partitions: { partition: number, errorCode: number, leader: number }[] }>}
 */
async function topicMetadata(bootstrap, topic) {
  const r = await request(bootstrap, 3, 4, Buffer.concat([array([topic], writeString), Buffer.from([0])]));
  r.i32(); // throttle time
  const brokers = r.i32();
  for (let i = 0; i < brokers; i++) {
    r.i32();
    r.string();
    r.i32();
    r.nullableString();
  }
  r.nullableString(); // cluster id
  r.i32(); // controller id
  const topics = r.i32();
  let result = { errorCode: 3, partitions: /** @type {{ partition: number, errorCode: number, leader: number }[]} */ ([]) };
  for (let t = 0; t < topics; t++) {
    const errorCode = r.i16();
    const name = r.string();
    r.i8(); // is internal
    const count = r.i32();
    const partitions = [];
    for (let p = 0; p < count; p++) {
      const partitionError = r.i16();
      const partition = r.i32();
      const leader = r.i32();
      for (let list = 0; list < 2; list++) {
        // the replica nodes, then the in-sync replica nodes
        const n = r.i32();
        for (let i = 0; i < n; i++) r.i32();
      }
      partitions.push({ partition, errorCode: partitionError, leader });
    }
    if (name === topic) result = { errorCode, partitions };
  }
  return result;
}

/**
 * Waits until `topic` has `partitions` partitions, each with a leader that
 * serves it: a fetch from offset 0 answers with no error.
 *
 * The metadata names a new partition's leader a moment before the leader
 * takes requests for it, and a produce in that moment gets
 * NOT_LEADER_OR_FOLLOWER. An idempotent producer retries that batch, but a
 * later batch of the partition, already in flight, may be accepted first, as
 * the broker takes any sequence from a producer it has no state for; the
 * retried batch is then out of sequence until its delivery timeout expires
 * it, as with Java's producer.
 *
 * @param {string} bootstrap
 * @param {string} topic
 * @param {number} partitions
 * @param {number} [timeoutMs]
 */
async function waitForPartitions(bootstrap, topic, partitions, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs;
  await waitForLeaders(bootstrap, topic, partitions, deadline, timeoutMs);
  for (let partition = 0; partition < partitions; partition++) {
    for (;;) {
      try {
        await fetchBatches(bootstrap, topic, partition);
        break;
      } catch (error) {
        if (Date.now() > deadline)
          throw new Error(
            `partition ${topic}-${partition} is not served after ${timeoutMs} ms: ${error instanceof Error ? error.message : String(error)}`,
          );
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }
  }
}

/**
 * Waits until the metadata gives `topic` `partitions` partitions, each with a
 * leader.
 *
 * @param {string} bootstrap
 * @param {string} topic
 * @param {number} partitions
 * @param {number} deadline - the `Date.now()` value to give up at
 * @param {number} timeoutMs - the wait's length, for the message
 */
async function waitForLeaders(bootstrap, topic, partitions, deadline, timeoutMs) {
  for (;;) {
    /** @type {{ errorCode: number, partitions: { partition: number, errorCode: number, leader: number }[] } | Error} */
    let md;
    try {
      md = await topicMetadata(bootstrap, topic);
    } catch (error) {
      // A request that failed, or found no response: the next one may not.
      md = /** @type {Error} */ (error);
    }
    if (
      !(md instanceof Error) &&
      md.errorCode === 0 &&
      md.partitions.length === partitions &&
      md.partitions.every((p) => p.errorCode === 0 && p.leader >= 0)
    ) {
      return;
    }
    if (Date.now() > deadline)
      throw new Error(
        `topic ${topic} has no ${partitions} led partitions after ${timeoutMs} ms: ${md instanceof Error ? md.message : JSON.stringify(md)}`,
      );
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

/**
 * Throws when an admin response carries an error code other than 0 (or
 * `allowed`).
 *
 * @param {string} what
 * @param {number} errorCode
 * @param {string | null} message
 * @param {number[]} [allowed]
 */
function checkError(what, errorCode, message, allowed = []) {
  if (errorCode !== 0 && !allowed.includes(errorCode)) throw new Error(`${what}: error code ${errorCode}${message ? `: ${message}` : ''}`);
}

/**
 * Creates `topic` (CreateTopics v4) with `partitions` partitions, replication
 * factor 1 and `configs`, and waits until every partition has a leader that
 * serves it.
 *
 * @param {string} bootstrap
 * @param {string} topic
 * @param {{ partitions?: number, configs?: Record<string, string> }} [options]
 */
async function createTopic(bootstrap, topic, { partitions = 1, configs = {} } = {}) {
  const body = Buffer.concat([
    array([topic], (name) =>
      Buffer.concat([
        writeString(name),
        int32(partitions),
        int16(1), // replication factor
        int32(0), // no assignments
        array(Object.entries(configs), ([key, value]) => Buffer.concat([writeString(key), writeNullableString(value)])),
      ]),
    ),
    int32(30000), // timeout
    Buffer.from([0]), // validate only
  ]);
  const r = await request(bootstrap, 19, 4, body);
  r.i32(); // throttle time
  const count = r.i32();
  for (let i = 0; i < count; i++) {
    const name = r.string();
    checkError(`CreateTopics ${name}`, r.i16(), r.nullableString());
  }
  await waitForPartitions(bootstrap, topic, partitions);
}

/**
 * Deletes `topic` (DeleteTopics v3) and waits until the broker no longer
 * knows it.
 *
 * @param {string} bootstrap
 * @param {string} topic
 */
async function deleteTopic(bootstrap, topic) {
  const r = await request(bootstrap, 20, 3, Buffer.concat([array([topic], writeString), int32(30000)]));
  r.i32(); // throttle time
  const count = r.i32();
  for (let i = 0; i < count; i++) {
    const name = r.string();
    checkError(`DeleteTopics ${name}`, r.i16(), null);
  }
  const deadline = Date.now() + 30000;
  while ((await topicMetadata(bootstrap, topic)).errorCode === 0) {
    if (Date.now() > deadline) throw new Error(`topic ${topic} still exists 30 s after its deletion`);
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

/**
 * Grows `topic` to `count` partitions (CreatePartitions v1) and waits until
 * each has a leader that serves it.
 *
 * @param {string} bootstrap
 * @param {string} topic
 * @param {number} count
 */
async function createPartitions(bootstrap, topic, count) {
  const body = Buffer.concat([
    array([topic], (name) => Buffer.concat([writeString(name), int32(count), int32(-1)])),
    int32(30000),
    Buffer.from([0]),
  ]);
  const r = await request(bootstrap, 37, 1, body);
  r.i32(); // throttle time
  const n = r.i32();
  for (let i = 0; i < n; i++) {
    const name = r.string();
    checkError(`CreatePartitions ${name}`, r.i16(), r.nullableString());
  }
  await waitForPartitions(bootstrap, topic, count);
}

/**
 * Sets `configs` on `topic` (IncrementalAlterConfigs v0, operation SET; a
 * `null` value DELETEs the key).
 *
 * @param {string} bootstrap
 * @param {string} topic
 * @param {Record<string, string | null>} configs
 */
async function alterTopicConfigs(bootstrap, topic, configs) {
  const body = Buffer.concat([
    array([topic], (name) =>
      Buffer.concat([
        Buffer.from([2]), // resource type TOPIC
        writeString(name),
        array(Object.entries(configs), ([key, value]) =>
          Buffer.concat([writeString(key), Buffer.from([value === null ? 1 : 0]), writeNullableString(value)]),
        ),
      ]),
    ),
    Buffer.from([0]),
  ]);
  const r = await request(bootstrap, 44, 0, body);
  r.i32(); // throttle time
  const n = r.i32();
  for (let i = 0; i < n; i++) {
    const errorCode = r.i16();
    const message = r.nullableString();
    r.i8();
    const name = r.string();
    checkError(`IncrementalAlterConfigs ${name}`, errorCode, message);
  }
}

// The broker's container, when the Makefile started it (KAFKA_CONTAINER):
// `null` otherwise, so a test that needs it skips against any other broker.

/**
 * Runs `docker <args>` and returns its stdout; throws on failure.
 *
 * @param {string[]} args
 */
function docker(args) {
  const run = spawnSync('docker', args, { encoding: 'utf8', timeout: 120000 });
  if (run.status !== 0) throw new Error(`docker ${args.join(' ')} failed (${run.status}): ${run.stderr}`);
  return run.stdout;
}

/**
 * Waits until the broker answers a Metadata request.
 *
 * @param {string} bootstrap
 * @param {number} [timeoutMs]
 */
async function waitForBroker(bootstrap, timeoutMs = 60000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    try {
      await topicMetadata(bootstrap, '__js_it_probe');
      return;
    } catch (error) {
      if (Date.now() > deadline) throw error;
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
  }
}

/**
 * The broker container's control, or `null` when the Makefile did not start
 * it (KAFKA_CONTAINER unset).
 *
 * @returns {{ name: string, pause(): void, unpause(): void, restart(bootstrap: string): Promise<void> } | null}
 */
function brokerContainer() {
  const name = process.env.KAFKA_CONTAINER;
  if (!name) return null;
  return {
    name,
    pause: () => void docker(['pause', name]),
    unpause: () => void docker(['unpause', name]),
    restart: async (bootstrap) => {
      docker(['restart', '--time', '0', name]);
      await waitForBroker(bootstrap);
    },
  };
}

module.exports = {
  fetchRecords,
  fetchBatches,
  roundTrip,
  connect,
  topicMetadata,
  waitForPartitions,
  createTopic,
  deleteTopic,
  createPartitions,
  alterTopicConfigs,
  brokerContainer,
  waitForBroker,
};
