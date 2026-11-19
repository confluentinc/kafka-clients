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

// The tests run on Jest (root CLAUDE.md, JavaScript Binding Conventions,
// Tests, Q66), one project per Makefile target: `make test-unit` runs
// `jest --selectProjects unit`, and so on.
//
// They run under jest-light-runner, in child processes of plain Node.js, not
// in Jest's vm contexts: the tests load the addon, whose values come from
// Node's own realm (an Array or a TypeError it makes is not a vm context's),
// they import the package through Node's own resolver (its exports map, a
// deep import refused, an ES module import, a second copy of the package),
// and the linter they test requires ES modules. In a vm context none of those
// behaves as it does for a user. The runner gives a test no timeout of its
// own, so test/jest_setup.js applies the timeout a test is given.

const runner = '<rootDir>/test/jest_runner.mjs';

/** @type {(displayName: string, testMatch: string[]) => Record<string, unknown>} */
const project = (displayName, testMatch) => ({
  displayName,
  rootDir: __dirname,
  runner,
  testMatch,
  setupFilesAfterEnv: ['<rootDir>/test/jest_setup.js'],
});

module.exports = {
  projects: [
    project('static', ['<rootDir>/test/static/*.test.js']),
    project('lint', ['<rootDir>/test/lint/*.test.js']),
    project('unit', ['<rootDir>/test/unit/**/*.test.js', '<rootDir>/test/unit/*.test.js']),
    project('typing', ['<rootDir>/test/typing/*.test.js']),
    project('verifiable-clients', ['<rootDir>/verifiable_clients/test/*.test.js']),
    project('integration', ['<rootDir>/test/integration/*.test.js']),
    // A short stress of the producer against a broker (test/stress/), run in
    // band as the integration tests are: `make test-stress`.
    project('stress', ['<rootDir>/test/stress/*.test.js']),
    // The native layer's memory probes (test/memory/_probe.js), and the
    // mutants of the addon that prove each probe fails when broken.
    project('memory', ['<rootDir>/test/memory/*.test.js']),
    project('memory-mutation', ['<rootDir>/test/memory_mutation/*.test.js']),
    project('grpc-server', ['<rootDir>/test/grpc_server/*.test.js']),
  ],
};
