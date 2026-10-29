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

// Every kafka_* function the addon calls is declared in the bindings header,
// and none is a plain name that has an `_err` twin (javascript/CLAUDE.md;
// Python's test_ffi_twins.py, .NET's NativeAbiConformanceTests).

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { headerPath, stripComments, declaredFunctions, errorEnumerators } = require('../../scripts/header');

const SRC = path.join(__dirname, '..', '..', 'src');
const header = fs.readFileSync(headerPath(), 'utf8');
const declared = declaredFunctions(header);

/** The kafka_* functions a C source calls (an identifier followed by `(`). */
function calledFunctions(file) {
  const code = stripComments(fs.readFileSync(file, 'utf8'));
  const names = new Set();
  const re = /\b(kafka_\w+)\s*\(/g;
  let m;
  while ((m = re.exec(code)) !== null) names.add(m[1]);
  return names;
}

const sources = fs.readdirSync(SRC).filter((f) => f.endsWith('.c'));

test('the addon has C sources to check', () => {
  assert.ok(sources.length >= 4, sources.join(', '));
});

for (const source of sources) {
  test(`${source} calls only functions the bindings header declares`, () => {
    for (const name of calledFunctions(path.join(SRC, source))) {
      assert.ok(declared.has(name), `${name} is not declared in confluent_kafka_bindings.h`);
      assert.ok(!declared.has(`${name}_err`), `${name} has an _err twin, which must be called instead`);
    }
  });
}

test('the producer uses the functions of the plan', () => {
  const called = new Set(sources.flatMap((s) => [...calledFunctions(path.join(SRC, s))]));
  for (const name of [
    'kafka_producer_Producer_send_with_callback_cb',
    'kafka_producer_Producer_flush_cb',
    'kafka_producer_Producer_close_cb',
    'kafka_producer_Producer_close_with_timeout_cb',
    'kafka_producer_Producer__execute_callbacks_err',
    'kafka_producer_Producer__set_callbacks_notify_err',
    'kafka_producer_Producer__set_callback_result_err',
    'kafka_producer_KafkaProducer_destroy_err',
    'kafka_common_KafkaFuture_destroy_err',
  ]) {
    assert.ok(called.has(name), `${name} is not called`);
  }
  // Rules 4 and 5: no blocking twin, never read the future.
  for (const name of [
    'kafka_producer_Producer_send',
    'kafka_producer_Producer_send_with_callback',
    'kafka_producer_Producer_flush',
    'kafka_producer_Producer_close',
    'kafka_producer_Producer_close_with_timeout',
    'kafka_common_KafkaFuture_get',
    'kafka_common_KafkaFuture_get_err',
  ]) {
    assert.ok(!called.has(name), `${name} must not be called`);
  }
});

test("the binding's own error codes are the header's", () => {
  const values = new Map(errorEnumerators(header));
  assert.equal(values.get('LOCAL_ILLEGAL_ARGUMENT'), -3);
  assert.equal(values.get('LOCAL_ILLEGAL_STATE'), -4);
});
