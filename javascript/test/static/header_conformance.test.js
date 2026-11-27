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

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { headerPath, stripComments, declaredFunctions, errorEnumerators } = require('../../scripts/header');
const { calledFunctions: calledIn } = require('../../scripts/lint/c_source');

const NATIVE = path.join(__dirname, '..', '..', 'native');
const header = fs.readFileSync(headerPath(), 'utf8');
const declared = declaredFunctions(header);

/**
 * The kafka_* functions a C source file calls (an identifier followed by `(`).
 *
 * @param {string} file
 */
function calledFunctions(file) {
  return calledIn(fs.readFileSync(file, 'utf8'));
}

// The .h files too: a macro's body makes its calls wherever it is used.
const sources = fs.readdirSync(NATIVE).filter((f) => f.endsWith('.c') || f.endsWith('.h'));

test('the addon has C sources to check', () => {
  assert.ok(sources.length >= 4, sources.join(', '));
});

for (const source of sources) {
  test(`${source} calls only functions the bindings header declares`, () => {
    for (const name of calledFunctions(path.join(NATIVE, source))) {
      assert.ok(declared.has(name), `${name} is not declared in confluent_kafka_bindings.h`);
      assert.ok(!declared.has(`${name}_err`), `${name} has an _err twin, which must be called instead`);
    }
  });
}

test('the producer uses the functions of the plan', () => {
  const called = new Set(sources.flatMap((s) => [...calledFunctions(path.join(NATIVE, s))]));
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
  // Rules 4 and 5 (no blocking twin; a record's future waited on only
  // through kafka_common_KafkaFuture_get_cb and get_with_timeout_cb) are the
  // rule linter's check-cb-twin-only (make lint-rules), which forbids the
  // blocking form of every function with a `_cb` twin, the future's `get` and
  // `get_with_timeout` included, and allows their `_cb` twins.
});

test('no blocking close: every close is a _cb twin, and no finalizer closes a producer (rule 4)', () => {
  for (const name of ['kafka_producer_Producer_close', 'kafka_producer_Producer_close_with_timeout']) {
    for (const source of sources) {
      const code = stripComments(fs.readFileSync(path.join(NATIVE, source), 'utf8'));
      const calls = [...code.matchAll(new RegExp(`\\b${name}(_err)?\\s*\\(`, 'g'))];
      assert.equal(calls.length, 0, `${source} calls ${name}, the blocking form`);
    }
  }
});

test('the consumer uses the functions of the plan', () => {
  const called = new Set(sources.flatMap((s) => [...calledFunctions(path.join(NATIVE, s))]));
  for (const name of [
    'kafka_consumer_ConsumerConfig_new',
    'kafka_consumer_KafkaConsumer_new',
    'kafka_consumer_KafkaConsumer__as_Consumer_err',
    'kafka_consumer_KafkaConsumer_destroy_err',
    'kafka_consumer_Consumer__set_callbacks_notify_err',
    'kafka_consumer_Consumer__execute_callbacks_err',
    'kafka_consumer_Consumer_poll_cb',
    'kafka_consumer_ConsumerRecords_partitions_err',
    'kafka_consumer_ConsumerRecords_records_with_partition_err',
    'kafka_consumer_ConsumerRecords_next_offsets_err',
    'kafka_consumer_ConsumerRecords_destroy_err',
    'kafka_consumer_ConsumerRecord_headers_err',
    'kafka_consumer_Consumer_subscribe_with_topics_cb',
    'kafka_consumer_Consumer_subscribe_with_pattern_cb',
    'kafka_consumer_SubscriptionPattern_new_err',
    'kafka_consumer_Consumer_assign_cb',
    'kafka_consumer_Consumer_unsubscribe_cb',
    'kafka_consumer_Consumer_assignment',
    'kafka_consumer_Consumer_subscription',
    'kafka_consumer_Consumer_paused',
    'kafka_consumer_Consumer_pause_cb',
    'kafka_consumer_Consumer_resume_cb',
    'kafka_consumer_Consumer_wakeup_err',
    'kafka_consumer_Consumer_metrics_err',
    'kafka_consumer_Consumer_close_cb',
    'kafka_consumer_Consumer_close_with_options_cb',
    'kafka_consumer_CloseOptions_new_with_timeout_err',
    'kafka_consumer_CloseOptions_with_operation_err',
    'kafka_consumer_CloseOptions_with_group_membership_operation_err',
  ]) {
    assert.ok(called.has(name), `${name} is not called`);
  }
});

test('both clients parse their configuration first and read the client.id it generated, and mark no key as used', () => {
  // Java's order: parse, read client.id, configure the serdes, build the
  // client from the same configuration (native/config.c). A key a serde
  // config function reads is not marked with ignore(): the client's copy of
  // its configuration would not keep the mark (the binding's notes).
  const called = new Set(sources.flatMap((s) => [...calledFunctions(path.join(NATIVE, s))]));
  for (const name of [
    'kafka_producer_ProducerConfig_new',
    'kafka_producer_ProducerConfig_get_string',
    'kafka_producer_ProducerConfig_destroy_err',
    'kafka_producer_KafkaProducer_new',
    'kafka_consumer_ConsumerConfig_new',
    'kafka_consumer_ConsumerConfig_get_string',
    'kafka_consumer_ConsumerConfig_destroy_err',
    'kafka_consumer_KafkaConsumer_new',
  ]) {
    assert.ok(called.has(name), `${name} is not called`);
  }
  for (const name of [
    'kafka_producer_ProducerConfig_ignore',
    'kafka_producer_ProducerConfig_ignore_err',
    'kafka_consumer_ConsumerConfig_ignore',
    'kafka_consumer_ConsumerConfig_ignore_err',
  ]) {
    assert.ok(!called.has(name), `${name} is called`);
  }
});

test("the consumer's commits, seeks, positions, offset and metadata queries use their entry points", () => {
  const called = new Set(sources.flatMap((s) => [...calledFunctions(path.join(NATIVE, s))]));
  for (const name of [
    'kafka_consumer_Consumer_commit_sync_cb',
    'kafka_consumer_Consumer_commit_sync_with_timeout_cb',
    'kafka_consumer_Consumer_commit_sync_with_offsets_cb',
    'kafka_consumer_Consumer_commit_sync_with_offsets_timeout_cb',
    'kafka_consumer_Consumer_commit_async_cb',
    'kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata',
    'kafka_consumer_OffsetAndMetadata_destroy_err',
    'kafka_consumer_Consumer_seek_with_offset_cb',
    'kafka_consumer_Consumer_seek_with_offset_and_metadata_cb',
    'kafka_consumer_Consumer_seek_to_beginning_cb',
    'kafka_consumer_Consumer_seek_to_end_cb',
    'kafka_consumer_Consumer_position_cb',
    'kafka_consumer_Consumer_position_with_timeout_cb',
    'kafka_consumer_Consumer_committed_cb',
    'kafka_consumer_Consumer_committed_with_timeout_cb',
    'kafka_consumer_Consumer_current_lag_cb',
    'kafka_consumer_Consumer_group_metadata',
    'kafka_consumer_ConsumerGroupMetadata_group_id_err',
    'kafka_consumer_ConsumerGroupMetadata_generation_id_err',
    'kafka_consumer_ConsumerGroupMetadata_member_id_err',
    'kafka_consumer_ConsumerGroupMetadata_group_instance_id_err',
    'kafka_consumer_ConsumerGroupMetadata_destroy_err',
    'kafka_consumer_Consumer_partitions_for_cb',
    'kafka_consumer_Consumer_partitions_for_with_timeout_cb',
    'kafka_consumer_Consumer_list_topics_cb',
    'kafka_consumer_Consumer_list_topics_with_timeout_cb',
    'kafka_consumer_Consumer_offsets_for_times_cb',
    'kafka_consumer_Consumer_offsets_for_times_with_timeout_cb',
    'kafka_consumer_OffsetAndTimestamp_offset_err',
    'kafka_consumer_OffsetAndTimestamp_timestamp_err',
    'kafka_consumer_OffsetAndTimestamp_leader_epoch_err',
    'kafka_consumer_Consumer_beginning_offsets_cb',
    'kafka_consumer_Consumer_beginning_offsets_with_timeout_cb',
    'kafka_consumer_Consumer_end_offsets_cb',
    'kafka_consumer_Consumer_end_offsets_with_timeout_cb',
  ]) {
    assert.ok(called.has(name), `${name} is not called`);
  }
});

test("the mock consumer's calls use their entry points", () => {
  const called = new Set(sources.flatMap((s) => [...calledFunctions(path.join(NATIVE, s))]));
  for (const name of [
    'kafka_consumer_MockConsumer_new',
    'kafka_consumer_MockConsumer__as_Consumer_err',
    'kafka_consumer_MockConsumer_destroy_err',
    'kafka_consumer_MockConsumer_rebalance_cb',
    'kafka_consumer_MockConsumer_lose_partitions_cb',
    'kafka_consumer_MockConsumer_add_record',
    'kafka_consumer_ConsumerRecordOptionsBuilder_new_err',
    'kafka_consumer_ConsumerRecordOptionsBuilder_set_topic_err',
    'kafka_consumer_ConsumerRecordOptionsBuilder_set_partition_err',
    'kafka_consumer_ConsumerRecordOptionsBuilder_set_offset_err',
    'kafka_consumer_ConsumerRecordOptionsBuilder_set_key_err',
    'kafka_consumer_ConsumerRecordOptionsBuilder_set_value_err',
    'kafka_consumer_ConsumerRecordOptionsBuilder_set_leader_epoch_err',
    'kafka_consumer_ConsumerRecordOptionsBuilder_build',
    'kafka_consumer_ConsumerRecordOptionsBuilder_destroy_err',
    'kafka_consumer_ConsumerRecordOptions_destroy_err',
    'kafka_consumer_ConsumerRecord_with_options_err',
    'kafka_consumer_ConsumerRecord_destroy_err',
    'kafka_consumer_ConsumerRecord_key_err',
    'kafka_consumer_MockConsumer_set_max_poll_records',
    'kafka_consumer_MockConsumer_set_poll_error_err',
    'kafka_consumer_MockConsumer_set_offsets_error_err',
    'kafka_common_Error_local_callback_err',
    'kafka_consumer_MockConsumer_update_beginning_offsets_err',
    'kafka_consumer_MockConsumer_update_end_offsets_err',
    'kafka_consumer_MockConsumer_update_duration_offsets_err',
    'kafka_consumer_MockConsumer_update_partitions',
    'kafka_common_PartitionInfo_with_offline_replicas_err',
    'kafka_common_PartitionInfo_destroy_err',
    'kafka_common_Node_with_rack_is_fenced_err',
    'kafka_common_Node_destroy_err',
    'kafka_consumer_MockConsumer_closed_err',
    'kafka_consumer_MockConsumer_schedule_nop_poll_task_err',
    'kafka_consumer_MockConsumer_last_poll_timeout_err',
  ]) {
    assert.ok(called.has(name), `${name} is not called`);
  }
});

test('no blocking mock consumer call, and none of the dropped enforceRebalance helpers', () => {
  for (const name of [
    'kafka_consumer_MockConsumer_rebalance',
    'kafka_consumer_MockConsumer_lose_partitions',
    // Dropped as Python drops them, with enforceRebalance.
    'kafka_consumer_MockConsumer_should_rebalance',
    'kafka_consumer_MockConsumer_reset_should_rebalance',
  ]) {
    for (const source of sources) {
      const code = stripComments(fs.readFileSync(path.join(NATIVE, source), 'utf8'));
      const calls = [...code.matchAll(new RegExp(`\\b${name}(_err)?\\s*\\(`, 'g'))];
      assert.equal(calls.length, 0, `${source} calls ${name}`);
    }
  }
});

test('no blocking consumer call: every waiting call is a _cb twin (rule 4)', () => {
  for (const name of [
    'kafka_consumer_Consumer_poll',
    'kafka_consumer_Consumer_close',
    'kafka_consumer_Consumer_close_with_options',
    'kafka_consumer_Consumer_subscribe_with_topics',
    'kafka_consumer_Consumer_subscribe_with_pattern',
    'kafka_consumer_Consumer_assign',
    'kafka_consumer_Consumer_unsubscribe',
    'kafka_consumer_Consumer_pause',
    'kafka_consumer_Consumer_resume',
    'kafka_consumer_Consumer_commit_sync',
    'kafka_consumer_Consumer_commit_sync_with_timeout',
    'kafka_consumer_Consumer_commit_sync_with_offsets',
    'kafka_consumer_Consumer_commit_sync_with_offsets_timeout',
    'kafka_consumer_Consumer_commit_async',
    'kafka_consumer_Consumer_seek_with_offset',
    'kafka_consumer_Consumer_seek_with_offset_and_metadata',
    'kafka_consumer_Consumer_seek_to_beginning',
    'kafka_consumer_Consumer_seek_to_end',
    'kafka_consumer_Consumer_position',
    'kafka_consumer_Consumer_position_with_timeout',
    'kafka_consumer_Consumer_committed',
    'kafka_consumer_Consumer_committed_with_timeout',
    'kafka_consumer_Consumer_current_lag',
    'kafka_consumer_Consumer_partitions_for',
    'kafka_consumer_Consumer_partitions_for_with_timeout',
    'kafka_consumer_Consumer_list_topics',
    'kafka_consumer_Consumer_list_topics_with_timeout',
    'kafka_consumer_Consumer_offsets_for_times',
    'kafka_consumer_Consumer_offsets_for_times_with_timeout',
    'kafka_consumer_Consumer_beginning_offsets',
    'kafka_consumer_Consumer_beginning_offsets_with_timeout',
    'kafka_consumer_Consumer_end_offsets',
    'kafka_consumer_Consumer_end_offsets_with_timeout',
    'kafka_consumer_Consumer_enforce_rebalance',
    'kafka_consumer_Consumer_enforce_rebalance_with_reason',
    // Dropped as Python drops them (no generated method calls them): no form
    // of enforceRebalance is called.
    'kafka_consumer_Consumer_enforce_rebalance_cb',
    'kafka_consumer_Consumer_enforce_rebalance_with_reason_cb',
  ]) {
    for (const source of sources) {
      const code = stripComments(fs.readFileSync(path.join(NATIVE, source), 'utf8'));
      const calls = [...code.matchAll(new RegExp(`\\b${name}(_err)?\\s*\\(`, 'g'))];
      assert.equal(calls.length, 0, `${source} calls ${name}, the blocking form`);
    }
  }
});

test("the consumer's listeners, commit callbacks, their reports and the window use their entry points", () => {
  const called = new Set(sources.flatMap((s) => [...calledFunctions(path.join(NATIVE, s))]));
  for (const name of [
    'kafka_consumer_Consumer_subscribe_with_topics_listener_cb',
    'kafka_consumer_Consumer_subscribe_with_pattern_listener_cb',
    'kafka_consumer_ConsumerRebalanceListener_new_err',
    'kafka_consumer_ConsumerRebalanceListener_destroy_err',
    'kafka_consumer_Consumer__set_callback_result_err',
    'kafka_consumer_Consumer_commit_async_with_callback_cb',
    'kafka_consumer_Consumer_commit_async_with_offsets_callback_cb',
    'kafka_consumer_OffsetCommitCallback_new_err',
    'kafka_consumer_OffsetCommitCallback_destroy_err',
    'kafka_consumer_ConsumerHandle_commit_async_with_callback_cb',
    'kafka_consumer_ConsumerHandle_commit_async_with_offsets_callback_cb',
    'kafka_consumer_Consumer_handle_err',
    'kafka_consumer_ConsumerHandle_destroy_err',
    'kafka_consumer_ConsumerHandle_commit_sync_cb',
    'kafka_consumer_ConsumerHandle_commit_sync_with_timeout_cb',
    'kafka_consumer_ConsumerHandle_commit_sync_with_offsets_cb',
    'kafka_consumer_ConsumerHandle_commit_sync_with_offsets_timeout_cb',
    'kafka_consumer_ConsumerHandle_commit_async_cb',
    'kafka_consumer_ConsumerHandle_seek_with_offset_cb',
    'kafka_consumer_ConsumerHandle_seek_with_offset_and_metadata_cb',
    'kafka_consumer_ConsumerHandle_seek_to_beginning_cb',
    'kafka_consumer_ConsumerHandle_seek_to_end_cb',
    'kafka_consumer_ConsumerHandle_pause_cb',
    'kafka_consumer_ConsumerHandle_resume_cb',
    'kafka_consumer_ConsumerHandle_position_cb',
    'kafka_consumer_ConsumerHandle_position_with_timeout_cb',
    'kafka_consumer_ConsumerHandle_committed_cb',
    'kafka_consumer_ConsumerHandle_committed_with_timeout_cb',
    'kafka_consumer_ConsumerHandle_partitions_for_cb',
    'kafka_consumer_ConsumerHandle_partitions_for_with_timeout_cb',
    'kafka_consumer_ConsumerHandle_list_topics_cb',
    'kafka_consumer_ConsumerHandle_list_topics_with_timeout_cb',
    'kafka_consumer_ConsumerHandle_offsets_for_times_cb',
    'kafka_consumer_ConsumerHandle_offsets_for_times_with_timeout_cb',
    'kafka_consumer_ConsumerHandle_beginning_offsets_cb',
    'kafka_consumer_ConsumerHandle_beginning_offsets_with_timeout_cb',
    'kafka_consumer_ConsumerHandle_end_offsets_cb',
    'kafka_consumer_ConsumerHandle_end_offsets_with_timeout_cb',
    'kafka_consumer_ConsumerHandle_current_lag_cb',
    'kafka_consumer_ConsumerHandle_assignment_err',
    'kafka_consumer_ConsumerHandle_paused_err',
    'kafka_consumer_ConsumerHandle_group_metadata_err',
    'kafka_consumer_ConsumerHandle_wakeup_err',
  ]) {
    assert.ok(called.has(name), `${name} is not called`);
  }
});

test("no blocking listener, commit callback or handle call, and none of the C invokers of a listener's or commit callback's methods", () => {
  // Every blocking ConsumerHandle form, read from the header: each one with a _cb twin.
  const handleBlocking = [...stripComments(header).matchAll(/\b(kafka_consumer_ConsumerHandle_\w+)_cb\s*\(/g)].map((m) => m[1]);
  assert.ok(handleBlocking.length > 20, handleBlocking.join(', '));
  for (const name of [
    'kafka_consumer_Consumer_subscribe_with_topics_listener',
    'kafka_consumer_Consumer_subscribe_with_pattern_listener',
    ...handleBlocking,
    'kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked',
    'kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned',
    'kafka_consumer_ConsumerRebalanceListener_on_partitions_lost',
    'kafka_consumer_ConsumerRebalanceListener_on_partitions_revoked_cb',
    'kafka_consumer_ConsumerRebalanceListener_on_partitions_assigned_cb',
    'kafka_consumer_ConsumerRebalanceListener_on_partitions_lost_cb',
    'kafka_consumer_Consumer_commit_async_with_callback',
    'kafka_consumer_Consumer_commit_async_with_offsets_callback',
    'kafka_consumer_OffsetCommitCallback_on_complete',
    'kafka_consumer_OffsetCommitCallback_on_complete_cb',
  ]) {
    for (const source of sources) {
      const code = stripComments(fs.readFileSync(path.join(NATIVE, source), 'utf8'));
      const calls = [...code.matchAll(new RegExp(`\\b${name}(_err)?\\s*\\(`, 'g'))];
      assert.equal(calls.length, 0, `${source} calls ${name}`);
    }
  }
});

test("the generated FFI_NAMES table is the header's kafka_common_Error_e, both ways", () => {
  // src/internal/error_registry.ts is generated from rust/src/ffi/common.rs;
  // the addon is built against the header cbindgen generates from it. Read from the
  // source (no build): the `[value, 'NAME'],` rows of its FFI_NAMES literal.
  const registry = fs.readFileSync(path.join(__dirname, '..', '..', 'src', 'internal', 'error_registry.ts'), 'utf8');
  const start = registry.indexOf('export const FFI_NAMES');
  assert.ok(start >= 0, 'src/internal/error_registry.ts has no FFI_NAMES');
  const body = registry.slice(start, registry.indexOf(']);', start));
  /** @type {Array<[number, string]>} */
  const FFI_NAMES = [...body.matchAll(/^ {2}\[(-?\d+), '([A-Z0-9_]+)'\],$/gm)].map((m) => [Number(m[1]), m[2]]);
  assert.ok(FFI_NAMES.length > 0, 'FFI_NAMES has no rows');
  // scripts/header.js's `[name, value]` pairs, which its JavaScript does not type.
  const values = new Map(/** @type {Array<[string, number]>} */ (errorEnumerators(header)));
  assert.deepEqual(
    [...FFI_NAMES].sort((a, b) => a[0] - b[0]),
    [...values].map(/** @returns {[number, string]} */ ([n, v]) => [v, n]).sort((a, b) => a[0] - b[0]),
  );
  assert.equal(values.get('LOCAL_ILLEGAL_ARGUMENT'), -3);
  assert.equal(values.get('LOCAL_ILLEGAL_STATE'), -4);
});

// The ABI check, as .NET's NativeAbiConformanceTests checks its P/Invokes
// against the same header: the addon's calls against the header's
// prototypes. The addon includes the header itself (native/util.h), so the C
// compiler checks each argument's and the return's type, and native/binding.gyp
// makes every warning (an incompatible pointer, a wrong integer width) an
// error; this test checks what the compiler cannot see once the header
// changes under a stale build or a macro, and that the build keeps -Werror.

/**
 * The text between the parenthesis at `open` (its index in `code`) and its
 * match, and the index after the match.
 *
 * @param {string} code
 * @param {number} open
 * @returns {{inner: string, end: number}}
 */
function balanced(code, open) {
  let depth = 0;
  for (let i = open; i < code.length; i++) {
    const c = code[i];
    if (c === '(') depth += 1;
    else if (c === ')') {
      depth -= 1;
      if (depth === 0) return { inner: code.slice(open + 1, i), end: i + 1 };
    } else if (c === '"' || c === "'") {
      // A string or character literal: skip it, escapes included.
      for (i += 1; i < code.length && code[i] !== c; i++) if (code[i] === '\\') i += 1;
    }
  }
  throw new Error(`unbalanced parenthesis at ${open}`);
}

/**
 * The top-level comma-separated parts of an argument or parameter list.
 *
 * @param {string} list
 * @returns {Array<string>}
 */
function topLevelParts(list) {
  /** @type {Array<string>} */
  const parts = [];
  let depth = 0;
  let start = 0;
  for (let i = 0; i < list.length; i++) {
    const c = list[i];
    if (c === '(' || c === '[' || c === '{') depth += 1;
    else if (c === ')' || c === ']' || c === '}') depth -= 1;
    else if (c === '"' || c === "'") {
      for (i += 1; i < list.length && list[i] !== c; i++) if (list[i] === '\\') i += 1;
    } else if (c === ',' && depth === 0) {
      parts.push(list.slice(start, i).trim());
      start = i + 1;
    }
  }
  const last = list.slice(start).trim();
  if (last !== '' || parts.length > 0) parts.push(last);
  return parts;
}

/**
 * {name: [parameter, ...]} of every function prototype in the bindings header.
 *
 * @param {string} text
 * @returns {Map<string, Array<string>>}
 */
function prototypes(text) {
  const code = stripComments(text);
  /** @type {Map<string, Array<string>>} */
  const out = new Map();
  const re = /^[A-Za-z_][\w \t*]*?\b(kafka_\w+)\s*\(/gm;
  let m;
  while ((m = re.exec(code)) !== null) {
    const { inner, end } = balanced(code, m.index + m[0].length - 1);
    // A prototype ends with `;`; a typedef of a function pointer is not one.
    if (!/^\s*;/.test(code.slice(end))) continue;
    const params = topLevelParts(inner);
    out.set(m[1], params.length === 1 && params[0] === 'void' ? [] : params);
  }
  return out;
}

/**
 * [{name, args, at}] of every call of a kafka_* function in C source text.
 *
 * @param {string} text
 * @returns {Array<{name: string, args: Array<string>, at: number}>}
 */
function calls(text) {
  const code = stripComments(text);
  /** @type {Array<{name: string, args: Array<string>, at: number}>} */
  const out = [];
  const re = /\b(kafka_\w+)\s*\(/g;
  let m;
  while ((m = re.exec(code)) !== null) {
    const { inner } = balanced(code, m.index + m[0].length - 1);
    out.push({ name: m[1], args: topLevelParts(inner), at: code.slice(0, m.index).split('\n').length });
  }
  return out;
}

test('every call of a kafka_* function passes the arguments of its header prototype', () => {
  const protos = prototypes(header);
  assert.ok(protos.size > 100, `the header has ${protos.size} prototypes`);
  let checked = 0;
  let withOutErr = 0;
  for (const source of sources) {
    for (const call of calls(fs.readFileSync(path.join(NATIVE, source), 'utf8'))) {
      const params = protos.get(call.name);
      assert.ok(params !== undefined, `${source}:${call.at}: ${call.name} has no prototype in confluent_kafka_bindings.h`);
      assert.equal(
        call.args.length,
        params.length,
        `${source}:${call.at}: ${call.name} takes ${params.length} arguments (${params.join(', ')}), not ${call.args.length}`,
      );
      // A fallible function's trailing out_err is the call's error slot.
      if (/\*\s*\*\s*out_err$/.test(params[params.length - 1] ?? '')) {
        assert.match(
          call.args[call.args.length - 1],
          /^&?\w*err\w*$/,
          `${source}:${call.at}: ${call.name}'s out_err is ${call.args[call.args.length - 1]}`,
        );
        withOutErr += 1;
      }
      checked += 1;
    }
  }
  assert.ok(checked > 40 && withOutErr > 20, `checked ${checked} calls, ${withOutErr} with an out_err`);
});

test('the addon declares no prototype of its own for a kafka_* function, and builds with -Werror', () => {
  for (const source of sources) {
    const code = stripComments(fs.readFileSync(path.join(NATIVE, source), 'utf8'));
    // A declaration is at file scope, at column 0; a statement is indented.
    assert.doesNotMatch(
      code,
      /^(?!(?:else|return|if|while|for|do|case)\b)(?:extern\s+)?[A-Za-z_][\w \t*]*\bkafka_\w+\s*\([^;{]*\)\s*;/m,
      `${source} redeclares a function of the bindings header`,
    );
  }
  const gyp = fs.readFileSync(path.join(NATIVE, 'binding.gyp'), 'utf8');
  for (const flag of ['"-Wall"', '"-Wextra"', '"-Werror"']) {
    assert.ok(gyp.split(flag).length - 1 >= 2, `native/binding.gyp does not pass ${flag} on both platforms`);
  }
});

test('a source using a GNU extension of glibc defines _GNU_SOURCE before its first #include, so it builds on Linux', () => {
  // native/binding.gyp compiles with -std=c11, which defines no feature macro,
  // and glibc's <dlfcn.h> declares dladdr and Dl_info only under _GNU_SOURCE:
  // without it the addon builds on macOS and not on Linux.
  const gyp = fs.readFileSync(path.join(NATIVE, 'binding.gyp'), 'utf8');
  assert.match(gyp, /"-std=c11"/, 'native/binding.gyp compiles with -std=c11');
  let users = 0;
  for (const source of sources.filter((f) => f.endsWith('.c'))) {
    const code = stripComments(fs.readFileSync(path.join(NATIVE, source), 'utf8'));
    if (!/\b(?:dladdr|Dl_info)\b/.test(code)) continue;
    users += 1;
    const define = code.search(/^[ \t]*#[ \t]*define[ \t]+_GNU_SOURCE\b/m);
    const include = code.search(/^[ \t]*#[ \t]*include\b/m);
    assert.ok(
      define >= 0 && (include < 0 || define < include),
      `native/${source} uses dladdr or Dl_info without #define _GNU_SOURCE before its first #include`,
    );
  }
  assert.ok(users > 0, 'no native source uses dladdr: the check has nothing to check');
});
