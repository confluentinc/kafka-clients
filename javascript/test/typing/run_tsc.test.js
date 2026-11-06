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

// How scripts/typescript.js runs the compiler for the typing tests: the
// default compiler with no shell, a `TSC` override through the shell with
// every argument quoted, so arguments reach the compiler literally.

const test = require('node:test');
const assert = require('node:assert/strict');
const childProcess = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');

const SCRIPT = require.resolve('../../scripts/typescript');
const { ROOT, shellQuote } = require(SCRIPT);

// Arguments a shell would split, substitute or unquote.
const ARGS = ['a b', '$(echo substituted)', '`echo substituted`', "it's", '"double"', '$HOME', '', 'back\\slash'];

/** Runs `fn` with `TSC` set to `value` (unset when undefined), restoring it after. */
function withTsc(value, fn) {
  const saved = process.env.TSC;
  if (value === undefined) delete process.env.TSC;
  else process.env.TSC = value;
  try {
    return fn();
  } finally {
    if (saved === undefined) delete process.env.TSC;
    else process.env.TSC = saved;
  }
}

test('shellQuote quotes for POSIX sh', () => {
  assert.equal(shellQuote('plain'), "'plain'");
  assert.equal(shellQuote("it's"), "'it'\\''s'");
  assert.equal(shellQuote(''), "''");
});

test('a TSC override is a command line; every argument reaches the compiler literally', () => {
  // A fake compiler that prints its arguments, under a directory whose name
  // holds a space; TSC names it with two words, as `npx tsc` would.
  const dir = path.join(ROOT, 'build', 'run tsc test');
  fs.mkdirSync(dir, { recursive: true });
  const fake = path.join(dir, 'fake-tsc.js');
  fs.writeFileSync(fake, 'process.stdout.write(JSON.stringify(process.argv.slice(2)));\n');
  const { runTsc } = require(SCRIPT);
  const r = withTsc(`${shellQuote(process.execPath)} ${shellQuote(fake)}`, () => runTsc(ARGS));
  assert.equal(r.status, 0, r.output);
  assert.deepEqual(JSON.parse(r.output), ARGS);
});

test('the default compiler is run directly, with no shell', () => {
  const calls = [];
  const original = childProcess.spawnSync;
  childProcess.spawnSync = (...callArgs) => {
    calls.push(callArgs);
    return { status: 0, stdout: 'ok', stderr: '' };
  };
  // scripts/typescript.js reads spawnSync when it is loaded: load a fresh copy
  // that sees the stub.
  delete require.cache[SCRIPT];
  try {
    const { runTsc } = require(SCRIPT);
    withTsc(undefined, () => runTsc(ARGS));
    withTsc('', () => runTsc(ARGS));
  } finally {
    childProcess.spawnSync = original;
    delete require.cache[SCRIPT];
  }
  assert.equal(calls.length, 2);
  for (const [file, args, options] of calls) {
    assert.equal(file, path.join(ROOT, 'node_modules', '.bin', 'tsc'));
    assert.deepEqual(args, ARGS);
    assert.equal(options.shell, undefined);
  }
});
