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

// A minimal Fetch (v4) client for the integration tests: reads back what the
// producer wrote, so a test can check record values, not only metadata. It
// speaks to one broker (the single-broker test cluster), handles only
// uncompressed v2 record batches and is no part of the client.

const net = require('node:net');

function writeString(s) {
  const b = Buffer.from(s, 'utf8');
  const len = Buffer.alloc(2);
  len.writeInt16BE(b.length);
  return Buffer.concat([len, b]);
}

function int32(v) {
  const b = Buffer.alloc(4);
  b.writeInt32BE(v);
  return b;
}

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
    const v = this.buf.readBigInt64BE(this.pos);
    this.pos += 8;
    return Number(v);
  }
  string() {
    const len = this.i16();
    const s = this.buf.toString('utf8', this.pos, this.pos + len);
    this.pos += len;
    return s;
  }
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

// The records of an uncompressed record set: [{offset, timestamp, key, value,
// headers}], `headers` an array of {key, value, keyBytes} (the key decoded as
// UTF-8, and its raw bytes) and `timestamp` the batch's base
// timestamp plus the record's delta (the record's CreateTime on a CreateTime
// topic).
function parseRecords(buf) {
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
    if ((attributes & 0x7) !== 0) throw new Error('compressed batches are not supported');
    r.i32(); // last offset delta
    const baseTimestamp = r.i64();
    r.i64(); // max timestamp
    r.i64(); // producer id
    r.i16(); // producer epoch
    r.i32(); // base sequence
    const count = r.i32();
    const control = (attributes & 0x20) !== 0;
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
      if (!control) {
        out.push({ offset: baseOffset + offsetDelta, timestamp: baseTimestamp + timestampDelta, key, value, headers });
      }
    }
    r.pos = end;
  }
  return out;
}

// A Promise over `socket`: `start(listeners, done)` adds its own listeners to
// the socket and to `listeners` and calls `done(value)` to resolve; 'error'
// rejects with the error, 'end' and 'close' with the message `closed()` gives.
// Once it settles every listener in `listeners` is removed, so none piles up
// across calls.
function settleOn(socket, closed, start) {
  return new Promise((resolve, reject) => {
    const listeners = {};
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
 * Writes `request` and resolves to the response's body, once complete; rejects
 * when the socket fails, ends or closes first.
 */
function roundTrip(socket, request) {
  let chunks = Buffer.alloc(0);
  return settleOn(
    socket,
    () => `the connection closed after ${chunks.length} bytes of the response`,
    (listeners, done) => {
      listeners.data = (chunk) => {
        chunks = Buffer.concat([chunks, chunk]);
        if (chunks.length >= 4 && chunks.length >= 4 + chunks.readInt32BE(0)) {
          done(chunks.subarray(4, 4 + chunks.readInt32BE(0)));
        }
      };
      socket.on('data', listeners.data);
      socket.write(request);
    },
  );
}

/** Connects to `host:port`; rejects when the socket fails or closes first. */
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
 * watermark: `{highWatermark, records: [{offset, timestamp, key, value,
 * headers}]}`.
 */
async function fetchRecords(bootstrap, topic, partition) {
  const [host, port] = bootstrap.split(',')[0].split(':');
  const socket = await connect(host, Number(port));
  // An error between two round trips, with none of theirs listening, would
  // otherwise be an uncaught 'error' event: the next round trip sees the
  // socket closed instead.
  socket.on('error', () => {});
  try {
    const records = [];
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
      const batch = parseRecords(size < 0 ? Buffer.alloc(0) : r.bytes(size));
      for (const record of batch) if (record.offset >= offset) records.push(record);
      if (records.length > 0) offset = records[records.length - 1].offset + 1;
      if (offset >= highWatermark || batch.length === 0) break;
    }
    return { highWatermark, records };
  } finally {
    socket.destroy();
  }
}

module.exports = { fetchRecords, roundTrip, connect };
