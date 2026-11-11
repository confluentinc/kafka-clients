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

// Tests of _common.js, translated from python/verifiable_clients/test/
// test_common.py: the properties reader (the Rust tool's
// parse_properties_handles_common_forms, extended with the
// java.util.Properties forms Kafka's system tests write), printJson on a
// stdout that fails, and the shutdown hook.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const { parseProperties, printJson } = require('../_common');

const TOOLS_DIR = path.join(__dirname, '..');

test('parseProperties handles the common forms', () => {
  const contents =
    '# a comment\n' +
    '! another comment\n' +
    '\n' +
    'bootstrap.servers=localhost:9092\n' +
    'linger.ms : 5\n' +
    'compression.type snappy\n' +
    '  padded.key = padded.value\n';
  assert.deepEqual(parseProperties(contents), {
    'bootstrap.servers': 'localhost:9092',
    'linger.ms': '5',
    'compression.type': 'snappy',
    'padded.key': 'padded.value',
  });
});

test('parseProperties keeps separators inside the value', () => {
  assert.deepEqual(parseProperties('listeners=PLAINTEXT://host:9092\nkey:=value\n'), {
    listeners: 'PLAINTEXT://host:9092',
    key: '=value',
  });
});

test('parseProperties: a later line wins', () => {
  assert.deepEqual(parseProperties('acks=1\nacks=all\n'), { acks: 'all' });
});

test('parseProperties joins continued lines', () => {
  // What kafkatest's SecurityConfig writes for a SASL_PLAINTEXT client:
  // sasl.jaas.config is the JAAS file with each newline as " \\\n".
  const jaas =
    '/**\n' +
    ' * Licensed to the Apache Software Foundation (ASF)\n' +
    ' */\n' +
    '\n' +
    'org.apache.kafka.common.security.plain.PlainLoginModule required\n' +
    '    username="client"\n' +
    '    password="client-secret";\n';
  const contents =
    'security.protocol=SASL_PLAINTEXT\n' +
    `sasl.jaas.config=${jaas.replace(/\n/g, ' \\\n')}\n` +
    'sasl.mechanism=PLAIN\n';
  assert.deepEqual(parseProperties(contents), {
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.jaas.config':
      '/** * Licensed to the Apache Software Foundation (ASF) */ ' +
      'org.apache.kafka.common.security.plain.PlainLoginModule required ' +
      'username="client" password="client-secret";',
    'sasl.mechanism': 'PLAIN',
  });
});

test('parseProperties: comment characters on a continuation are text', () => {
  assert.deepEqual(parseProperties('key=a \\\n  # not a comment\n'), { key: 'a # not a comment' });
});

test('parseProperties: even backslashes do not continue', () => {
  assert.deepEqual(parseProperties('path=C:\\\\\nnext=1\n'), { path: 'C:\\', next: '1' });
});

test('parseProperties escapes', () => {
  assert.deepEqual(parseProperties('a\\=b=c\\td\nu=\\u0041\\x\n'), { 'a=b': 'c\td', u: 'Ax' });
});

test('parseProperties: a malformed unicode escape throws', () => {
  assert.throws(() => parseProperties('u=\\u00G1\n'), { message: 'Malformed \\uxxxx encoding.' });
});

test('parseProperties handles CRLF and CR', () => {
  assert.deepEqual(parseProperties('a=1\r\nb=2\rc=3'), { a: '1', b: '2', c: '3' });
});

test('parseProperties joins surrogate pairs', () => {
  // Java's escapes are UTF-16 code units, as JavaScript's strings are: the
  // pair is one character.
  const props = parseProperties('client.id=\\uD83D\\uDE80\n');
  assert.deepEqual(props, { 'client.id': '\u{1F680}' });
  assert.deepEqual([...Buffer.from(props['client.id'], 'utf8')], [0xf0, 0x9f, 0x9a, 0x80]);
});

test('parseProperties keeps a lone surrogate', () => {
  assert.deepEqual(parseProperties('a=x\\uD83Dy\n'), { a: 'x\uD83Dy' });
});

test('printJson writes one compact line', () => {
  const written = [];
  const original = fs.writeSync;
  fs.writeSync = (fd, buffer) => {
    written.push([fd, Buffer.from(buffer).toString('utf8')]);
    return buffer.length;
  };
  try {
    printJson({ timestamp: 1, name: 'x', key: null });
  } finally {
    fs.writeSync = original;
  }
  assert.deepEqual(written, [[1, '{"timestamp":1,"name":"x","key":null}\n']]);
});

test('printJson reports data it cannot write as JSON', () => {
  const written = [];
  const original = fs.writeSync;
  fs.writeSync = (fd, buffer) => {
    written.push(Buffer.from(buffer).toString('utf8'));
    return buffer.length;
  };
  try {
    printJson({ offset: 1n });
  } finally {
    fs.writeSync = original;
  }
  assert.equal(written.length, 1);
  assert.ok(written[0].startsWith("Bad data can't be written as json: "), written[0]);
});

test('printJson drops a failed write, and the later ones', () => {
  // Java's PrintStream only sets an error flag: the tools carry on. Run in a
  // child, since the flag is the module's.
  const script = `
    const fs = require('node:fs');
    const { printJson } = require('./_common');
    let calls = 0;
    const original = fs.writeSync;
    fs.writeSync = (fd, ...rest) => {
      if (fd !== 1) return original.call(fs, fd, ...rest);
      calls += 1;
      const e = new Error('broken pipe');
      e.code = 'EPIPE';
      throw e;
    };
    printJson({ name: 'startup_complete' });
    printJson({ name: 'shutdown_complete' });
    fs.writeSync = original;
    process.stderr.write(String(calls));
  `;
  const result = spawnSync(process.execPath, ['-e', script], { cwd: TOOLS_DIR, encoding: 'utf8', timeout: 20_000 });
  assert.deepEqual([result.status, result.stderr], [0, '1']);
});

test('the shutdown hook runs once, at the first signal, and ignores the later ones', () => {
  const script = `
    const { ShutdownHook } = require('./_common');
    let runs = 0;
    const hook = new ShutdownHook(async () => {
      runs += 1;
      // A second signal while the hook runs neither runs it again nor kills
      // the process.
      process.kill(process.pid, 'SIGTERM');
      await new Promise((resolve) => setTimeout(resolve, 200));
    });
    process.kill(process.pid, 'SIGTERM');
    setTimeout(async () => {
      await hook.finish();
      process.stdout.write(runs === 1 && hook.started ? 'ran' : 'runs=' + runs);
    }, 500);
  `;
  const result = spawnSync(process.execPath, ['-e', script], { cwd: TOOLS_DIR, encoding: 'utf8', timeout: 20_000 });
  assert.deepEqual([result.status, result.stdout, result.stderr], [0, 'ran', '']);
});

test('finish() without a signal runs no hook', () => {
  // In a child: the hook's signal listeners stay installed for good.
  const script = `
    const { ShutdownHook } = require('./_common');
    let runs = 0;
    const hook = new ShutdownHook(() => {
      runs += 1;
    });
    hook.finish().then(() => process.stdout.write(runs + ' ' + hook.started));
  `;
  const result = spawnSync(process.execPath, ['-e', script], { cwd: TOOLS_DIR, encoding: 'utf8', timeout: 20_000 });
  assert.deepEqual([result.status, result.stdout, result.stderr], [0, '0 false', '']);
});
