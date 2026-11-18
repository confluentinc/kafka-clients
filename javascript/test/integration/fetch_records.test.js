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

// The integration tests' Fetch reader (_fetch_records.js) against a fake
// server: no broker is needed, so these run whether or not one is set.

const test = require('node:test');
const assert = require('node:assert/strict');
const net = require('node:net');

const { connect, roundTrip } = require('./_fetch_records');

const EVENTS = ['data', 'error', 'end', 'close', 'connect'];

/** A server answering each request with `respond(socket)`; resolves to {server, port}. */
async function fakeServer(respond) {
  const server = net.createServer((socket) => socket.once('data', () => respond(socket)));
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  return { server, port: server.address().port };
}

/** A framed response: its 4-byte size, then `body`. */
function frame(body) {
  const size = Buffer.alloc(4);
  size.writeInt32BE(body.length);
  return Buffer.concat([size, body]);
}

function listenerCounts(socket) {
  return Object.fromEntries(EVENTS.map((event) => [event, socket.listenerCount(event)]));
}

test('a round trip rejects promptly when the connection closes mid-response', { timeout: 10000 }, async () => {
  // The size announces 100 bytes; 10 arrive, then the server closes.
  const { server, port } = await fakeServer((socket) => {
    socket.end(frame(Buffer.alloc(100)).subarray(0, 14));
  });
  const socket = await connect('127.0.0.1', port);
  try {
    const before = listenerCounts(socket);
    await assert.rejects(roundTrip(socket, Buffer.from('request')), {
      message: 'the connection closed after 14 bytes of the response',
    });
    assert.deepEqual(listenerCounts(socket), before);
  } finally {
    socket.destroy();
    server.close();
  }
});

test('a completed round trip removes every listener it added', async () => {
  const { server, port } = await fakeServer((socket) => {
    // The response arrives in two pieces.
    const response = frame(Buffer.from('hello'));
    socket.write(response.subarray(0, 3));
    setImmediate(() => socket.write(response.subarray(3)));
  });
  const socket = await connect('127.0.0.1', port);
  try {
    // connect() left none of its own.
    assert.deepEqual(listenerCounts(socket), listenerCounts(new net.Socket()));
    const before = listenerCounts(socket);
    assert.equal((await roundTrip(socket, Buffer.from('request'))).toString(), 'hello');
    assert.deepEqual(listenerCounts(socket), before);
  } finally {
    socket.destroy();
    server.close();
  }
});

test('connecting rejects when the connection is refused', async () => {
  const { server, port } = await fakeServer(() => {});
  await new Promise((resolve) => server.close(resolve));
  await assert.rejects(connect('127.0.0.1', port), { code: 'ECONNREFUSED' });
});
