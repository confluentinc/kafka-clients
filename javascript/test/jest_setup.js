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

// Jest's `timeout` argument, of `test(name, fn, timeout)`, `it`, `test.each`,
// `test.only` and the hooks (`beforeAll`, `beforeEach`, `afterAll`,
// `afterEach`), and of `fit`, the light runner's alias of `test.only`:
// jest-light-runner (jest.config.cjs) registers a timeout but does not apply
// it, so a function given one fails once it has run that long, as under
// Jest's own runner. A function given none has no limit. A function taking
// more arguments than it is given, Jest's `done` form, ends when it calls
// `done` (failing with the error `done` is given), and one that takes `done`
// and returns a value fails, with Jest's message, whether it is given a
// timeout or not. `test.concurrent` refuses a timeout, which the light
// runner does not apply to it. `test.failing` is refused whatever it is
// given: the light runner runs it as a plain test, which would pass a test
// Jest fails and fail one Jest passes.
//
// An unhandled rejection or an uncaught exception fails the test running when
// it arrives, at once, and one that arrives between tests fails the next
// `afterEach`, or the file's `afterAll` (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Tests, Q67: no unhandled rejection). Node runs the tests with
// --unhandled-rejections=strict (test/jest_runner.mjs), so a rejection is
// raised as an uncaught exception even when something listens for
// `unhandledRejection`. A test that expects one opts out while it listens
// itself: with a listener of its own for `uncaughtException`, or for
// `unhandledRejection` when the error is a rejection, the error is its own.
// test/static/jest_setup.test.js runs each of these through Jest and checks
// its outcome.

/**
 * A test or hook function, or a `test.each` row's: it takes the row's values,
 * then Jest's `done` when it declares one more parameter.
 *
 * @typedef {(...args: unknown[]) => unknown} TestFn
 */
/**
 * A Jest member registering a test, as this file calls it.
 *
 * @typedef {(name: string, fn?: TestFn, timeout?: number) => void} Register
 */
/**
 * A Jest member registering a test, with its `each`.
 *
 * @typedef {Register & { each: (...table: unknown[]) => Register }} Form
 */
/**
 * Jest's `test`, with the members this file replaces or keeps.
 *
 * @typedef {Form & { only: Form, skip: Register, todo: (name: string) => void, concurrent?: Register, failing?: Register }} Test
 */
/**
 * A Jest hook (`beforeAll`, ...).
 *
 * @typedef {(fn?: TestFn, timeout?: number) => void} Hook
 */

// The globals this file replaces, read as the shapes above: Jest's own types
// (`jest.It`, `jest.Lifecycle`) take only Jest's own function forms, while
// this file passes any of them through as it is given.
const globals = /** @type {{ test: Test, it: Test, fit: Form } & Record<string, Hook | undefined>} */ (/** @type {unknown} */ (globalThis));
const register = globals.test;
const hooks = Object.fromEntries(['beforeAll', 'beforeEach', 'afterAll', 'afterEach'].map((name) => [name, globals[name]]));

/**
 * Runs `fn` with `args`, Jest's `done` form included: a Promise of its end.
 *
 * @param {TestFn} fn the test function
 * @param {unknown[]} args the values it is called with, before `done`
 * @returns {Promise<unknown>} its end
 */
function call(fn, args) {
  if (fn.length <= args.length) return Promise.resolve().then(() => fn(...args));
  return new Promise((/** @type {(value?: unknown) => void} */ resolve, reject) => {
    const settle = (/** @type {unknown} */ error) => (error === undefined || error === null ? resolve() : reject(error));
    // A `done` called before `fn` returned settles once its return is checked.
    let returned = false;
    /** @type {{ error: unknown } | null} */
    let early = null;
    const done = (/** @type {unknown} */ error) => {
      if (returned) settle(error);
      else early = { error };
    };
    let value;
    try {
      value = fn(...args, done);
    } catch (error) {
      reject(error);
      return;
    }
    if (value !== undefined) {
      reject(
        new Error(
          "Test functions cannot both take a 'done' callback and return something. Either use a 'done' callback, or return a promise.",
        ),
      );
      return;
    }
    returned = true;
    // `done` may have set `early` while `fn` ran, which tsc cannot see.
    const pending = /** @type {{ error: unknown } | null} */ (early);
    if (pending !== null) settle(pending.error);
  });
}

/**
 * `fn`, failing once it has run `timeout` ms; arity 0, so the runner passes
 * it no `done`. `rows`: it is `test.each`'s, which calls it with the row's
 * values; otherwise the runner calls it with a value to ignore.
 *
 * @param {TestFn} fn the test function
 * @param {unknown} timeout its timeout, in ms
 * @param {boolean} rows whether it is `test.each`'s
 * @returns {TestFn} the function the runner calls
 */
function withTimeout(fn, timeout, rows) {
  if (typeof timeout !== 'number' || !(timeout > 0))
    throw new TypeError(`a timeout must be a positive number of ms, not ${String(timeout)}`);
  return async (...given) => {
    const args = rows ? given : [];
    /** @type {NodeJS.Timeout | undefined} */
    let timer;
    const expired = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`Exceeded timeout of ${timeout} ms for a test.`)), timeout);
    });
    try {
      await Promise.race([call(fn, args), expired]);
    } finally {
      clearTimeout(timer);
    }
  };
}

/**
 * `fn` as the runner should call it: with the timeout applied when it is
 * given one, otherwise through `call` when it may take `done` (a `test.each`
 * row's function always goes there: its row's length is known only when it
 * runs), otherwise as it is.
 *
 * @param {TestFn | undefined} fn the test function
 * @param {number | undefined} timeout its timeout, in ms, if any
 * @param {boolean} [rows] whether it is `test.each`'s
 * @returns {TestFn | undefined} the function the runner calls
 */
function timed(fn, timeout, rows = false) {
  if (typeof fn !== 'function') return fn;
  if (timeout !== undefined) return guarded(withTimeout(fn, timeout, rows));
  if (!rows && fn.length === 0) return guarded(fn);
  return guarded((...given) => call(fn, rows ? given : []));
}

// --- Unhandled rejections and uncaught exceptions ---------------------------

/** The errors that arrived while no test ran, for the next `afterEach` or `afterAll`. */
/** @type {unknown[]} */
const stray = [];
/** The test or hook running, which an error that arrives fails at once. */
/** @type {((error: unknown) => void) | null} */
let failRunning = null;

/**
 * An error Node reports as uncaught or as an unhandled rejection: it fails the
 * running test, or waits for the next hook, unless the test listens itself.
 *
 * @param {unknown} error the error, or the rejection's reason
 * @param {string} origin `uncaughtException` or `unhandledRejection`
 */
function trip(error, origin) {
  const mine = origin === 'unhandledRejection' ? 'unhandledRejection' : 'uncaughtException';
  // This file's own listener is one of them.
  if (process.listenerCount('uncaughtException') > 1 || process.listenerCount(mine) > 1) return;
  const wrapped = new Error(`${origin === 'unhandledRejection' ? 'An unhandled rejection' : 'An uncaught exception'}: ${describe(error)}`, {
    cause: error,
  });
  if (failRunning !== null) failRunning(wrapped);
  else stray.push(wrapped);
}

/**
 * The error's name and message, or the value as text.
 *
 * @param {unknown} error the error
 * @returns {string} its description
 */
function describe(error) {
  if (error instanceof Error) return `${error.name}: ${error.message}`;
  try {
    return String(error);
  } catch {
    return Object.prototype.toString.call(error);
  }
}

// Under --unhandled-rejections=strict a rejection reaches the
// `uncaughtException` listener, with the origin `unhandledRejection`, and then
// `unhandledRejection`; without it, only `unhandledRejection`. Each error is
// counted once.
const STRICT = [...process.execArgv, ...(process.env.NODE_OPTIONS ?? '').split(/\s+/)].includes('--unhandled-rejections=strict');
process.on('uncaughtException', (error, origin) => trip(error, origin));
process.on('unhandledRejection', (reason) => {
  if (!STRICT) trip(reason, 'unhandledRejection');
});

/**
 * `fn`, failing at once when an error arrives while it runs. Arity 0: `fn` is
 * one `timed` already made safe to call without `done`, or takes none.
 *
 * @param {TestFn} fn the test or hook function
 * @returns {TestFn} the function the runner calls
 */
function guarded(fn) {
  return async (...given) => {
    /** @type {(error: unknown) => void} */
    let fail = () => {};
    const tripped = new Promise((_, reject) => {
      fail = reject;
    });
    failRunning = fail;
    try {
      await Promise.race([Promise.resolve().then(() => fn(...given)), tripped]);
    } finally {
      if (failRunning === fail) failRunning = null;
    }
  };
}

/**
 * Fails with the errors that arrived while no test ran, if any.
 */
function throwStray() {
  if (stray.length === 0) return;
  const errors = stray.splice(0);
  throw errors.length === 1 ? errors[0] : new AggregateError(errors, `${errors.length} errors arrived between tests`);
}

/**
 * `registerFn`'s form with the timeout applied, and its `each`.
 *
 * @param {Form} registerFn Jest's member
 * @returns {Form} the member with the timeout applied
 */
function timedForm(registerFn) {
  /** @type {Register} */
  const form = (name, fn, timeout) => registerFn(name, timed(fn, timeout));
  /** @type {Form['each']} */
  const each =
    (...table) =>
    (name, fn, timeout) =>
      registerFn.each(...table)(name, timed(fn, timeout, true));
  return Object.assign(form, { each });
}

/**
 * A member whose timeout the light runner does not apply: it refuses one.
 *
 * @param {Register} member Jest's member
 * @param {string} label its name, for the message
 * @returns {Register} the member refusing a timeout
 */
function refusingTimeout(member, label) {
  return (name, fn, timeout) => {
    if (timeout !== undefined) throw new Error(`${label}: test/jest_setup.js does not apply a timeout to it`);
    return member(name, timed(fn, undefined));
  };
}

/** @type {Test} */
const test = Object.assign(timedForm(register), { only: timedForm(register.only), skip: register.skip, todo: register.todo });
if (register.concurrent !== undefined) test.concurrent = refusingTimeout(register.concurrent, 'test.concurrent');
if (register.failing !== undefined) {
  const refused = () => {
    throw new Error('test.failing: jest-light-runner runs it as a plain test; test/jest_setup.js refuses it');
  };
  test.failing = Object.assign(refused, { each: refused });
}

for (const [name, hook] of Object.entries(hooks)) {
  if (typeof hook === 'function') globals[name] = (fn, timeout) => hook(timed(fn, timeout));
}
// An error that arrived after a test ended fails the next afterEach, or the
// file once its tests have run: a root afterEach and afterAll, registered by
// the file's first registration, which is at its root. (The runner clears
// what this file would register now, before it loads a test file.)
let rootHooksRegistered = false;
/** @type {WeakMap<Function, Function>} */
const rootedMembers = new WeakMap();
function registerRootHooks() {
  if (rootHooksRegistered) return;
  rootHooksRegistered = true;
  const { afterEach, afterAll } = hooks;
  if (typeof afterEach === 'function') afterEach(throwStray);
  // One more turn of the timers first, for an error the last test left behind.
  if (typeof afterAll === 'function') afterAll(guarded(() => new Promise((resolve) => setTimeout(resolve, 10)).then(throwStray)));
}

/**
 * `member`, and its `only`, `skip` and `each`, registering the root hooks first.
 *
 * @template {Function} M
 * @param {M} member a Jest member
 * @returns {M} the member
 */
function rooted(member) {
  const known = rootedMembers.get(member);
  if (known !== undefined) return /** @type {M} */ (known);
  const call = (/** @type {unknown[]} */ ...args) => {
    registerRootHooks();
    return member(...args);
  };
  // Jest's members refer to one another (a member's `only` can be itself).
  rootedMembers.set(member, call);
  for (const key of ['only', 'skip', 'each', 'concurrent', 'failing', 'todo']) {
    const value = /** @type {Record<string, unknown>} */ (/** @type {unknown} */ (member))[key];
    if (typeof value === 'function') /** @type {Record<string, unknown>} */ (/** @type {unknown} */ (call))[key] = rooted(value);
  }
  return /** @type {M} */ (/** @type {unknown} */ (call));
}

globals.test = rooted(test);
globals.it = globals.test;
globals.fit = rooted(test.only);
for (const name of ['describe', 'fdescribe', 'xdescribe', 'xit', 'xtest']) {
  const member = globals[name];
  if (typeof member === 'function') globals[name] = rooted(member);
}
for (const name of Object.keys(hooks)) {
  const member = globals[name];
  if (typeof member === 'function') globals[name] = rooted(member);
}
