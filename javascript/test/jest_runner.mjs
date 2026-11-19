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

// The Jest runner of the tests (jest.config.cjs): jest-light-runner's test
// worker, which runs a test file in plain Node.js, each file in a child
// process of its own. jest-light-runner itself reuses a process for the next
// file, and a test file here changes its process for good: it loads a second
// copy of the package (whose error classes the addon then knows), reloads a
// module or counts the process's threads. Each file gets a fresh process, as
// it would on its own; at most `maxWorkers` run at once (one with
// --runInBand). Each runs with --unhandled-rejections=strict
// (test/jest_setup.js).

import Tinypool from 'tinypool';

// jest-light-runner's internals: the path of its test worker and its
// `jest-light-runner-get-worker-data` message protocol, which hold for the
// version package.json pins exactly (0.8.2). A version bump must check both.
const WORKER = new URL('./worker-runner.js', import.meta.resolve('jest-light-runner'));

/** @typedef {import('@jest/test-result').Test} Test */
/** @typedef {import('@jest/test-result').TestResult} TestResult */
/** @typedef {import('@jest/types').Config.GlobalConfig} GlobalConfig */
/** @typedef {import('@jest/types').Config.ProjectConfig} ProjectConfig */
/** @typedef {(message: unknown) => void} Listener */

export default class IsolatedLightRunner {
  /** @type {GlobalConfig} */
  #globalConfig;

  /** @param {GlobalConfig} globalConfig */
  constructor(globalConfig) {
    this.#globalConfig = globalConfig;
  }

  /**
   * @param {Array<Test>} tests
   * @param {unknown} watcher
   * @param {(test: Test) => Promise<void>} onStart
   * @param {(test: Test, result: TestResult) => Promise<void>} onResult
   * @param {(test: Test, error: unknown) => Promise<void>} onFailure
   * @returns {Promise<void>}
   */
  async runTests(tests, watcher, onStart, onResult, onFailure) {
    const { maxWorkers } = this.#globalConfig;
    /** @type {Map<ProjectConfig, {pool: Tinypool, run: (path: string) => Promise<TestResult>}>} */
    const pools = new Map();
    /**
     * @param {ProjectConfig} projectConfig
     * @returns {{pool: Tinypool, run: (path: string) => Promise<TestResult>}}
     */
    const poolOf = (projectConfig) => {
      if (!pools.has(projectConfig)) {
        const workerData = { globalConfig: this.#globalConfig, projectConfig, runtime: 'child_process' };
        const pool = new Tinypool({
          filename: WORKER.href,
          runtime: 'child_process',
          isolateWorkers: true,
          minThreads: 0,
          maxThreads: maxWorkers,
          // Tinypool's type has no undefined values; Node's environment has none either.
          env: /** @type {Record<string, string>} */ (process.env),
          trackUnmanagedFds: false,
          // An unhandled rejection is raised as an uncaught exception even
          // when something listens for `unhandledRejection`, so that
          // test/jest_setup.js fails the test it arrives in. Only the test
          // files' own processes: a child a test starts gets Node's default.
          execArgv: [...process.execArgv, '--unhandled-rejections=strict'],
        });
        // The worker asks for its configuration once it starts
        // (jest-light-runner's protocol for a child process).
        /**
         * @param {string} path
         * @returns {Promise<TestResult>}
         */
        const run = (path) => {
          /** @type {Set<Listener>} */
          const listeners = new Set();
          const channel = {
            /** @param {Listener} listener */
            onMessage(listener) {
              listeners.add(listener);
            },
            /** @param {unknown} message */
            postMessage(message) {
              if (message !== 'jest-light-runner-get-worker-data') return;
              for (const listener of listeners) {
                listener({ type: 'jest-light-runner-worker-data', workerData });
                listeners.delete(listener);
              }
            },
          };
          return pool.run(path, { channel });
        };
        pools.set(projectConfig, { pool, run });
      }
      const entry = pools.get(projectConfig);
      if (entry === undefined) throw new Error('the pool of a project was not created');
      return entry;
    };

    // At most maxWorkers files at once, each reported as started when it does.
    const queue = [...tests];
    const lane = async () => {
      for (let test = queue.shift(); test !== undefined; test = queue.shift()) {
        try {
          await onStart(test);
          await onResult(test, await poolOf(test.context.config).run(test.path));
        } catch (error) {
          await onFailure(test, error);
        }
      }
    };
    try {
      await Promise.all(Array.from({ length: Math.max(1, Math.min(maxWorkers, tests.length)) }, lane));
    } finally {
      await Promise.all([...pools.values()].map(({ pool }) => pool.destroy()));
    }
  }
}
