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
// `afterEach`): jest-light-runner (jest.config.cjs) registers a timeout but
// does not apply it, so a function given one fails once it has run that
// long, as under Jest's own runner. A function given none has no limit. A
// function taking more arguments than it is given, Jest's `done` form, ends
// when it calls `done` (failing with the error `done` is given), whether it
// is given a timeout or not, as the light runner runs one, and one that
// takes `done` and returns a value fails, with Jest's message. The members
// the light runner gives a timeout no meaning (`test.concurrent`,
// `test.failing`) refuse one.

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
const globals = /** @type {{ test: Test, it: Test } & Record<string, Hook | undefined>} */ (/** @type {unknown} */ (globalThis));
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

/** @type {(fn: TestFn | undefined, timeout: number | undefined, rows?: boolean) => TestFn | undefined} */
const timed = (fn, timeout, rows = false) => (timeout === undefined || typeof fn !== 'function' ? fn : withTimeout(fn, timeout, rows));

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
    return member(name, fn);
  };
}

/** @type {Test} */
const test = Object.assign(timedForm(register), { only: timedForm(register.only), skip: register.skip, todo: register.todo });
if (register.concurrent !== undefined) test.concurrent = refusingTimeout(register.concurrent, 'test.concurrent');
if (register.failing !== undefined) test.failing = refusingTimeout(register.failing, 'test.failing');

globals.test = test;
globals.it = test;
for (const [name, hook] of Object.entries(hooks)) {
  if (typeof hook === 'function') globals[name] = (fn, timeout) => hook(timed(fn, timeout));
}
