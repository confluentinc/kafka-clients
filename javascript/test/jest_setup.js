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

// Jest's `test(name, fn, timeout)`: jest-light-runner (jest.config.cjs)
// registers a test's timeout but does not apply it, so a test given one fails
// once it has run that long, as under Jest's own runner. A test given none has
// no limit.

const register = globalThis.test;

function withTimeout(fn, timeout) {
  return async () => {
    let timer;
    const expired = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error(`Exceeded timeout of ${timeout} ms for a test.`)), timeout);
    });
    try {
      await Promise.race([fn(), expired]);
    } finally {
      clearTimeout(timer);
    }
  };
}

function test(name, fn, timeout) {
  return register(name, timeout === undefined ? fn : withTimeout(fn, timeout));
}
test.skip = register.skip;
test.only = (name, fn, timeout) => register.only(name, timeout === undefined ? fn : withTimeout(fn, timeout));
test.each = register.each;
test.todo = register.todo;

globalThis.test = test;
