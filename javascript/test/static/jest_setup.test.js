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

// test/jest_setup.js stands in for Jest's own timeout and `done` handling,
// which jest-light-runner does not apply: it decides whether a timed test can
// fail at all. These tests run scratch test files through Jest, with the
// repository's runner (test/jest_runner.mjs) and setup, in a child process,
// and check each test's outcome against what Jest's own runner gives it.

const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const ROOT = path.join(__dirname, '..', '..');
const JEST = path.join(ROOT, 'node_modules', 'jest', 'bin', 'jest.js');

const SLEEP = 'const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));\n';

// Each file runs in a process of its own; `fit` and `test.only` skip the
// other tests of their file, so each has a file of its own.
const FIXTURES = {
  'plain.test.js': `${SLEEP}
test('a timeout fails', () => sleep(2000), 200);
test('a test within its timeout passes', () => sleep(10), 2000);
test('a test with no timeout has no limit', () => sleep(300));
test('done passes', (done) => { setTimeout(done, 10); });
test('done with a timeout passes', (done) => { setTimeout(done, 10); }, 2000);
test('done(error) fails', (done) => { setTimeout(() => done(new Error('boom')), 10); });
test('done never called fails at the timeout', (done) => {}, 200);
test('async done fails', async (done) => {});
test('async done with a timeout fails', async (done) => {}, 2000);
test('a done that returns a Promise fails', (done) => Promise.resolve());
test.each([[1, 'one'], [2, 'two']])('each %i passes', (n, name) => {
  if (!['1 one', '2 two'].includes(\`\${n} \${name}\`)) throw new Error(\`row \${n} \${name}\`);
});
test.each([[3]])('each %i with a timeout passes', (n) => { if (n !== 3) throw new Error(String(n)); }, 2000);
test.each\`
  a    | b
  \${1} | \${2}
\`('table $a $b passes', ({ a, b }) => { if (!(a === 1 && b === 2)) throw new Error(\`table \${a} \${b}\`); });
test.each([[1]])('each %i with done passes', (n, done) => { setTimeout(done, 10); });
test.each([[1]])('each %i done(error) fails', (n, done) => { setTimeout(() => done(new Error('row ' + n)), 10); });
test.each([[1]])('each %i timeout fails', (n) => sleep(2000), 200);
describe('a timed hook', () => {
  beforeAll(() => sleep(2000), 200);
  test('after a timed hook', () => {});
});
describe('a done hook', () => {
  beforeEach((done) => { setTimeout(() => done(new Error('hook')), 10); });
  test('after a done hook', () => {});
});
`,
  'fit.test.js': `${SLEEP}
fit('fit honours its timeout', () => sleep(2000), 200);
test('another test is skipped', () => {});
`,
  'only.test.js': `${SLEEP}
test.only('test.only honours its timeout', () => sleep(2000), 200);
test('another test is skipped', () => {});
`,
  'failing.test.js': `test.failing('a failing test whose body passes', () => {});\n`,
  'failing_throws.test.js': `test.failing('a failing test whose body throws', () => { throw new Error('x'); });\n`,
  'concurrent.test.js': `test.concurrent('a concurrent test', async () => {}, 1000);\n`,
  'tripwire.test.js': `${SLEEP}
test('the test process runs with strict unhandled rejections', () => {
  if (!process.execArgv.includes('--unhandled-rejections=strict')) throw new Error(process.execArgv.join(' '));
});
test('an unhandled rejection fails the test it arrives in', async () => {
  Promise.reject(new Error('lost'));
  await sleep(5000);
});
test('an uncaught exception fails the test it arrives in', async () => {
  setTimeout(() => { throw new Error('thrown'); }, 10);
  await sleep(5000);
});
describe('a hook', () => {
  beforeEach(async () => { Promise.reject(new Error('in a hook')); await sleep(5000); });
  test('after a hook that tripped', () => {});
});
test('a test after them passes', () => {});
test('a test listening for unhandledRejection itself sees its rejection', async () => {
  const seen = [];
  const listener = (reason) => seen.push(reason.message);
  process.on('unhandledRejection', listener);
  Promise.reject(new Error('mine'));
  await sleep(50);
  process.off('unhandledRejection', listener);
  if (seen.join() !== 'mine') throw new Error(\`seen: \${seen.join()}\`);
});
test('a test listening for uncaughtException itself sees its error', async () => {
  const seen = [];
  const listener = (error) => seen.push(error.message);
  process.on('uncaughtException', listener);
  setTimeout(() => { throw new Error('also mine'); }, 0);
  await sleep(50);
  process.off('uncaughtException', listener);
  if (seen.join() !== 'also mine') throw new Error(\`seen: \${seen.join()}\`);
});
test('a test that leaves an error behind passes', () => {
  setTimeout(() => { throw new Error('late'); }, 0);
});
`,
};

/** @typedef {{ ancestorTitles: string[], title: string, status: string, failureMessages: string[] }} Assertion */
/** @typedef {{ name: string, status: string, message: string, startTime: number, endTime: number, assertionResults: Assertion[] }} FileResult */

/**
 * The first project of jest.config.cjs, every `<rootDir>` in it replaced with
 * the client's directory, so that the settings still point there once the
 * fixtures' directory is the root.
 *
 * @returns {Record<string, unknown>} the project's settings
 */
function projectConfig() {
  const { projects } = /** @type {{ projects: Record<string, unknown>[] }} */ (require('../../jest.config.cjs'));
  /** @type {(value: unknown) => unknown} */
  const resolve = (value) =>
    typeof value === 'string' ? value.replaceAll('<rootDir>', ROOT) : Array.isArray(value) ? value.map(resolve) : value;
  return Object.fromEntries(Object.entries(projects[0]).map(([key, value]) => [key, resolve(value)]));
}

/** @type {Map<string, FileResult>} */
const results = new Map();
/** @type {string | null} */
let dir = null;

beforeAll(() => {
  dir = fs.mkdtempSync(path.join(os.tmpdir(), 'jest-setup-'));
  for (const [name, source] of Object.entries(FIXTURES)) fs.writeFileSync(path.join(dir, name), source);
  const out = path.join(dir, 'results.json');
  // The fixtures run with jest.config.cjs's own project settings (its runner,
  // its setup and whatever it adds later), its <rootDir> made the client's
  // directory, so that only the root and the test files change.
  const config = {
    ...projectConfig(),
    rootDir: dir,
    testMatch: ['<rootDir>/*.test.js'],
  };
  const r = spawnSync(
    process.execPath,
    [JEST, '--ci', '--no-watchman', '--json', '--outputFile', out, '--config', JSON.stringify(config)],
    // A setup that leaves a test hanging must fail this test, not hang it:
    // spawnSync blocks the timer of this hook's own timeout.
    { cwd: dir, encoding: 'utf8', timeout: 90_000 },
  );
  assert.equal(r.signal, null, `Jest was stopped after 90 s, a fixture hanging:\n${r.stderr}`);
  assert.ok(fs.existsSync(out), `Jest wrote no results (status ${r.status}):\n${r.stderr}`);
  const report = /** @type {{ testResults: FileResult[] }} */ (JSON.parse(fs.readFileSync(out, 'utf8')));
  for (const file of report.testResults) results.set(path.basename(file.name), file);
  assert.deepEqual([...results.keys()].sort(), Object.keys(FIXTURES).sort());
}, 120_000);

afterAll(() => {
  if (dir !== null) fs.rmSync(dir, { recursive: true, force: true });
});

/**
 * The outcome of each test of a fixture file, by title.
 *
 * @param {string} file the fixture's name
 * @returns {Map<string, Assertion>} its tests
 */
function outcomes(file) {
  const result = results.get(file);
  assert.ok(result, file);
  return new Map(result.assertionResults.map((a) => [a.title, a]));
}

/**
 * Asserts the status of a test of a fixture file, and that a failure says `message`.
 *
 * @param {Map<string, Assertion>} tests the file's tests
 * @param {string} title the test's title
 * @param {'passed' | 'failed' | 'pending'} status its status
 * @param {string} [message] a text its failure holds
 */
function assertOutcome(tests, title, status, message) {
  const test = tests.get(title);
  assert.ok(test, `no test ${title}`);
  assert.equal(test.status, status, title);
  if (message !== undefined) assert.match(test.failureMessages.join('\n'), new RegExp(escape(message)), title);
}

/**
 * `text` as a regular expression matching itself.
 *
 * @param {string} text the text
 * @returns {string} the expression
 */
function escape(text) {
  return text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

const DONE_AND_RETURN = "Test functions cannot both take a 'done' callback and return something.";

test('a timeout fails a test that runs longer, and a test with none has no limit', () => {
  const tests = outcomes('plain.test.js');
  assertOutcome(tests, 'a timeout fails', 'failed', 'Exceeded timeout of 200 ms for a test.');
  assertOutcome(tests, 'a test within its timeout passes', 'passed');
  assertOutcome(tests, 'a test with no timeout has no limit', 'passed');
});

test('a done test ends at done, fails with its error, and fails at its timeout', () => {
  const tests = outcomes('plain.test.js');
  assertOutcome(tests, 'done passes', 'passed');
  assertOutcome(tests, 'done with a timeout passes', 'passed');
  assertOutcome(tests, 'done(error) fails', 'failed', 'boom');
  assertOutcome(tests, 'done never called fails at the timeout', 'failed', 'Exceeded timeout of 200 ms for a test.');
});

test("a done test that returns something fails with Jest's message, with a timeout or not", () => {
  const tests = outcomes('plain.test.js');
  assertOutcome(tests, 'async done fails', 'failed', DONE_AND_RETURN);
  assertOutcome(tests, 'async done with a timeout fails', 'failed', DONE_AND_RETURN);
  assertOutcome(tests, 'a done that returns a Promise fails', 'failed', DONE_AND_RETURN);
});

test('test.each passes its rows, takes done, and applies its timeout', () => {
  const tests = outcomes('plain.test.js');
  assertOutcome(tests, 'each 1 passes', 'passed');
  assertOutcome(tests, 'each 2 passes', 'passed');
  assertOutcome(tests, 'each 3 with a timeout passes', 'passed');
  assertOutcome(tests, 'table 1 2 passes', 'passed');
  assertOutcome(tests, 'each 1 with done passes', 'passed');
  assertOutcome(tests, 'each 1 done(error) fails', 'failed', 'row 1');
  assertOutcome(tests, 'each 1 timeout fails', 'failed', 'Exceeded timeout of 200 ms for a test.');
});

test('a hook fails at its timeout and with the error done is given', () => {
  // The light runner reports a failed hook as an entry of its own, `(beforeAll)` or `(beforeEach)`.
  const result = results.get('plain.test.js');
  assert.ok(result);
  /** @type {(group: string, hook: string) => Assertion | undefined} */
  const hookOf = (group, hook) => result.assertionResults.find((a) => a.title === hook && a.ancestorTitles[0] === group);
  const timedHook = hookOf('a timed hook', '(beforeAll)');
  assert.ok(timedHook, 'no (beforeAll) entry');
  assert.equal(timedHook.status, 'failed');
  assert.match(timedHook.failureMessages.join('\n'), /Exceeded timeout of 200 ms for a test\./);
  const doneHook = hookOf('a done hook', '(beforeEach)');
  assert.ok(doneHook, 'no (beforeEach) entry');
  assert.equal(doneHook.status, 'failed');
  assert.match(doneHook.failureMessages.join('\n'), /Error: hook/);
});

test('fit and test.only honour their timeout and skip the other tests', () => {
  for (const [file, title] of [
    ['fit.test.js', 'fit honours its timeout'],
    ['only.test.js', 'test.only honours its timeout'],
  ]) {
    const tests = outcomes(file);
    assertOutcome(tests, title, 'failed', 'Exceeded timeout of 200 ms for a test.');
    assertOutcome(tests, 'another test is skipped', 'pending');
  }
});

test('test.failing is refused whatever its body does, and test.concurrent refuses a timeout', () => {
  for (const file of ['failing.test.js', 'failing_throws.test.js']) {
    const result = results.get(file);
    assert.ok(result, file);
    assert.equal(result.status, 'failed', file);
    assert.match(result.message, /test\.failing: jest-light-runner runs it as a plain test; test\/jest_setup\.js refuses it/);
  }
  const concurrent = results.get('concurrent.test.js');
  assert.ok(concurrent);
  assert.equal(concurrent.status, 'failed');
  assert.match(concurrent.message, /test\.concurrent: test\/jest_setup\.js does not apply a timeout to it/);
});

test('an unhandled rejection or an uncaught exception fails the test or hook it arrives in, at once', () => {
  const tests = outcomes('tripwire.test.js');
  assertOutcome(tests, 'the test process runs with strict unhandled rejections', 'passed');
  assertOutcome(tests, 'an unhandled rejection fails the test it arrives in', 'failed', 'An unhandled rejection: Error: lost');
  assertOutcome(tests, 'an uncaught exception fails the test it arrives in', 'failed', 'An uncaught exception: Error: thrown');
  assertOutcome(tests, 'a test after them passes', 'passed');
  const result = results.get('tripwire.test.js');
  assert.ok(result);
  const hook = result.assertionResults.find((a) => a.title === '(beforeEach)' && a.ancestorTitles[0] === 'a hook');
  assert.ok(hook, 'no (beforeEach) entry');
  assert.equal(hook.status, 'failed');
  assert.match(hook.failureMessages.join('\n'), /An unhandled rejection: Error: in a hook/);
  // Each failed at once, not at the end of its 5 s sleep.
  assert.ok(result.endTime - result.startTime < 4000, `the file ran ${result.endTime - result.startTime} ms`);
});

test('a test listening itself opts out, and an error left behind fails the file', () => {
  const tests = outcomes('tripwire.test.js');
  assertOutcome(tests, 'a test listening for unhandledRejection itself sees its rejection', 'passed');
  assertOutcome(tests, 'a test listening for uncaughtException itself sees its error', 'passed');
  assertOutcome(tests, 'a test that leaves an error behind passes', 'passed');
  const result = results.get('tripwire.test.js');
  assert.ok(result);
  const late = result.assertionResults.filter((a) => a.title === '(afterAll)' && a.ancestorTitles.length === 0);
  assert.equal(late.length, 1, JSON.stringify(result.assertionResults.map((a) => a.title)));
  assert.equal(late[0].status, 'failed');
  assert.match(late[0].failureMessages.join('\n'), /An uncaught exception: Error: late/);
});
