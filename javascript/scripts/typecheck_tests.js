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
// every test file, those under test/ and the verifiable clients' under
// verifiable_clients/test/ (TEST_DIRS), with test/tsconfig.json, the
// JavaScript tests through `checkJs`. It fails on any finding in a test file,
// and on a finding with no file (a configuration error). The file kinds it
// checks are EXTENSIONS: the config's `include` must be the one these give
// (`expectedInclude`), and a declaration file (`.d.ts`) in a test directory is
// refused, since `skipLibCheck` would leave it unchecked.
//
// The compiler also reads the files the tests require from elsewhere: the
// rule linter (scripts/), the gRPC translation (grpc-server/), the verifiable
// clients themselves (verifiable_clients/), the compiled private modules
// (dist/internal/, which have no declarations) and the sources of the three
// internal functions whose signatures a test takes from src/internal/. Their
// findings are counted, not failed on: they are not tests, and their own
// checks are the build (src/ compiles under --strict) and the linter's and
// the clients' tests. The path filter is what draws the line, not a comment
// in each file: `// @ts-nocheck` in those files, or `checkJs: false` with a
// `// @ts-check` opt-in in each test, would draw it too, but the first edits
// files that are not tests (dist/ is generated) and the second lets a test
// file that forgets the comment go unchecked.
//
// Usage: node scripts/typecheck_tests.js

const fs = require('node:fs');
const path = require('node:path');
const { ROOT, runTsc } = require('./typescript');

const CONFIG = path.join('test', 'tsconfig.json');
/** The directories of the test files, relative to the client's directory. */
const TEST_DIRS = ['test', path.join('verifiable_clients', 'test')];
/** The extensions of the files checked: every kind of JavaScript and TypeScript file Node or tsc reads. */
const EXTENSIONS = ['.js', '.mjs', '.cjs', '.ts', '.mts', '.cts'];
/** A declaration file, which `skipLibCheck` leaves unchecked. */
const DECLARATION = /\.d\.[mc]?ts$/;

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

/**
 * The `include` of the config that checks every file of `EXTENSIONS` under
 * `TEST_DIRS`, its patterns relative to the config's directory.
 *
 * @returns {string[]} the patterns, in order
 */
function expectedInclude() {
  const base = path.dirname(CONFIG);
  return TEST_DIRS.flatMap((dir) => {
    const rel = path.relative(base, dir).split(path.sep).join('/');
    return EXTENSIONS.map((ext) => `${rel === '' ? '' : `${rel}/`}**/*${ext}`);
  });
}

/**
 * Whether `file` is a test file, one under `TEST_DIRS`.
 *
 * @param {string} file a path relative to the client's directory
 * @returns {boolean} whether it is under a test directory
 */
function isTestFile(file) {
  return TEST_DIRS.some((dir) => file.startsWith(`${dir}${path.sep}`));
}

/**
 * What is wrong with the config's file list, if anything.
 *
 * @returns {string[]} one message per problem
 */
function configProblems() {
  const ts = require('typescript');
  const { config, error } = ts.readConfigFile(path.join(ROOT, CONFIG), ts.sys.readFile);
  if (error !== undefined) return [`${CONFIG}: ${ts.flattenDiagnosticMessageText(error.messageText, '\n')}`];
  const include = /** @type {{ include?: unknown }} */ (config).include;
  const expected = expectedInclude();
  return JSON.stringify(include) === JSON.stringify(expected)
    ? []
    : [`${CONFIG}: "include" must be ${JSON.stringify(expected)}, the test directories and extensions of scripts/typecheck_tests.js`];
}

function main() {
  const failures = configProblems();
  const files = TEST_DIRS.flatMap((dir) => walk(dir)).filter((f) => EXTENSIONS.includes(path.extname(f)));
  for (const file of files) {
    if (DECLARATION.test(file)) {
      failures.push(`${file}: a declaration file is not type-checked (skipLibCheck); write the types in the test or in src/`);
      continue;
    }
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
      if (isTestFile(file)) {
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
      `typecheck-tests: ${outside} finding(s) in ${outsideFiles.size} file(s) the tests require from outside the test directories, not failed on`,
    );
  }
  if (failures.length > 0) {
    console.error(`typecheck-tests: ${failures.length} finding(s) in the test files`);
    return 1;
  }
  console.log(`typecheck-tests: no finding in the test files (${TEST_DIRS.join(', ')})`);
  return 0;
}

process.exitCode = main();
