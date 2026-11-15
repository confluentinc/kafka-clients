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

// The type check of the tests (`make typecheck-tests`; root conventions,
// Tests, Q67, and Build, packaging and release, Q74): `tsc --strict` over
// every file under test/, with test/tsconfig.json, the JavaScript tests
// through `checkJs`. It fails on any finding in a file under test/, and on a
// finding with no file (a configuration error).
//
// The compiler also reads the files the tests require from outside test/:
// the rule linter (scripts/), the gRPC translation (grpc-server/) and the
// compiled private modules (dist/internal/, which have no declarations). It
// checks those too, since `checkJs` has no per-file exclusion, but they are
// not tests: their own checks are the build (src/internal/*.ts compiles under
// --strict) and the rule linter's tests, so their findings are counted here,
// not failed on.
//
// A test marks a deliberate misuse (a wrong type, a null the types forbid)
// with `// @ts-expect-error <the run-time check it covers>`, which also proves
// that tsc catches it for a TypeScript caller. `@ts-ignore` and `@ts-nocheck`
// are refused, and so is an `@ts-expect-error` that gives no reason.
//
// Usage: node scripts/typecheck_tests.js

const fs = require('node:fs');
const path = require('node:path');
const { ROOT, runTsc } = require('./typescript');

const CONFIG = path.join('test', 'tsconfig.json');
const TEST_DIR = 'test';

/**
 * The files under `dir`, relative to the client's directory.
 *
 * @param {string} dir a directory relative to the client's directory
 * @returns {string[]} its files, recursively
 */
function walk(dir) {
  return fs.readdirSync(path.join(ROOT, dir), { withFileTypes: true }).flatMap((entry) => {
    const rel = path.join(dir, entry.name);
    return entry.isDirectory() ? walk(rel) : [rel];
  });
}

/**
 * The directives the type check refuses in a test file.
 *
 * @param {string} file the file, relative to the client's directory
 * @param {string} source its text
 * @returns {string[]} one message per refused directive
 */
function refusedDirectives(file, source) {
  const found = [];
  source.split('\n').forEach((line, i) => {
    const at = `${file}:${i + 1}`;
    if (/@ts-(ignore|nocheck)\b/.test(line)) found.push(`${at}: ${RegExp.lastMatch} is not allowed; use @ts-expect-error <why>`);
    const expect = /@ts-expect-error\b(.*)$/.exec(line);
    if (expect && !/\w/.test(expect[1])) found.push(`${at}: @ts-expect-error gives no reason`);
  });
  return found;
}

function main() {
  const failures = [];
  for (const file of walk(TEST_DIR).filter((f) => /\.(js|mjs|cjs|ts)$/.test(f))) {
    failures.push(...refusedDirectives(file, fs.readFileSync(path.join(ROOT, file), 'utf8')));
  }

  const r = runTsc(['-p', CONFIG, '--pretty', 'false']);
  if (r.status === null) {
    console.error(`typecheck-tests: tsc did not run\n${r.output}`);
    return 1;
  }
  // A finding is a line `file(line,col): error TS...: message`, or a line
  // `error TS...: message` with no file, followed by its indented details.
  let current = null;
  let outside = 0;
  const outsideFiles = new Set();
  for (const line of r.output.split('\n')) {
    const located = /^(.+?)\(\d+,\d+\): error TS\d+/.exec(line);
    if (located) {
      const file = path.normalize(located[1]);
      if (file.startsWith(`${TEST_DIR}${path.sep}`)) {
        current = [line];
        failures.push(current);
      } else {
        current = null;
        outside += 1;
        outsideFiles.add(file);
      }
    } else if (/^error TS\d+/.test(line)) {
      current = [line];
      failures.push(current);
    } else if (current !== null && /^\s/.test(line)) {
      current.push(line);
    }
  }
  if (r.status !== 0 && failures.length === 0 && outside === 0) {
    console.error(`typecheck-tests: tsc failed\n${r.output}`);
    return 1;
  }
  for (const failure of failures) console.error(Array.isArray(failure) ? failure.join('\n') : failure);
  if (outside > 0) {
    console.log(
      `typecheck-tests: ${outside} finding(s) in ${outsideFiles.size} file(s) the tests require from outside test/, not failed on`,
    );
  }
  if (failures.length > 0) {
    console.error(`typecheck-tests: ${failures.length} finding(s) under test/`);
    return 1;
  }
  console.log('typecheck-tests: no finding under test/');
  return 0;
}

process.exitCode = main();
