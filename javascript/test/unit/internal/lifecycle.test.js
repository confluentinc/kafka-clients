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

// src/internal/lifecycle.ts: the preview notice, once per process, and the
// warning about producers never closed. Each case runs in a child process,
// whose warnings are its own.

const test = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

const ROOT = path.join(__dirname, '..', '..', '..');

function runNode(script, flags = []) {
  return spawnSync(process.execPath, [...flags, '-e', script], { cwd: ROOT, encoding: 'utf8', timeout: 30000 });
}

const lines = (text, needle) => text.split('\n').filter((l) => l.includes(needle));

test('the preview notice is emitted once per process, the workers included', () => {
  const r = runNode(`
    const { Worker } = require('node:worker_threads');
    const lifecycle = require('./dist/internal/lifecycle.js');
    lifecycle.warnPreview();
    lifecycle.warnPreview();
    const w = new Worker("require('./dist/internal/lifecycle.js').warnPreview();", { eval: true });
    w.on('exit', () => console.log('done'));
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(r.stdout, 'done\n');
  const notices = lines(r.stderr, 'ExperimentalWarning');
  assert.equal(notices.length, 1, r.stderr);
  // No warning code: the type alone, ExperimentalWarning.
  assert.ok(notices[0].includes(') ExperimentalWarning: The JavaScript client is in preview'), r.stderr);
  assert.ok(!notices[0].includes('['), r.stderr);
});

test('a worker that builds the first client warns, and the main thread then does not', () => {
  const r = runNode(`
    const { Worker } = require('node:worker_threads');
    const w = new Worker("require('./dist/internal/lifecycle.js').warnPreview();", { eval: true });
    w.on('exit', () => {
      require('./dist/internal/lifecycle.js').warnPreview();
      console.log('done');
    });
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.equal(lines(r.stderr, 'ExperimentalWarning').length, 1, r.stderr);
});

test('the producers never closed are counted once, when the event loop empties', () => {
  let r = runNode(`
    const lifecycle = require('./dist/internal/lifecycle.js');
    lifecycle.producerOpened();
    lifecycle.producerOpened();
    lifecycle.producerOpened();
    lifecycle.producerClosed();
    // A second beforeExit, after more work, does not warn again.
    let again = true;
    process.on('beforeExit', () => { if (again) { again = false; setTimeout(() => {}, 10); } });
  `);
  assert.equal(r.status, 0, r.stderr);
  const warned = lines(r.stderr, 'never closed');
  assert.equal(warned.length, 1, r.stderr);
  assert.ok(
    warned[0].includes(
      '[CONFLUENT_KAFKA_PRODUCER_NOT_CLOSED] Warning: 2 KafkaProducers were never closed. Call close() (or use `await using`) ' +
        'before the process exits: a producer not closed keeps running until the process exits, and the records it has ' +
        'not delivered by then fail.',
    ),
    r.stderr,
  );
  // Every producer closed: nothing.
  r = runNode(`
    const lifecycle = require('./dist/internal/lifecycle.js');
    lifecycle.producerOpened();
    lifecycle.producerClosed();
  `);
  assert.equal(r.status, 0, r.stderr);
  assert.deepEqual(lines(r.stderr, 'never closed'), []);
});
