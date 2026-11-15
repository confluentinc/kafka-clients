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

// check-tsfn-lifetime (scripts/lint/rules/tsfn_lifetime.js).

const assert = require('node:assert/strict');

const { project, lint, findings } = require('./_project');

const RULE = 'check-tsfn-lifetime';

const HEADER =
  'void kafka_x_P_flush_cb(const kafka_x_P_t *self_, kafka_x_P_flush_cb_t cb, void *opaque);\n\n' +
  'void kafka_x_P_close_cb(const kafka_x_P_t *self_, kafka_x_P_close_cb_t cb, void *opaque);\n\n' +
  'void kafka_x_P_close_with_timeout_cb(const kafka_x_P_t *self_, int64_t timeout, kafka_x_P_close_with_timeout_cb_t cb, void *opaque);\n';

const QUEUE =
  'static void cbq_hold_update(napi_env env, Q* q) {\n' +
  '  if (q->stopped) return;\n' +
  '  int hold = q->self_strong || q->pending > 0;\n' +
  '  if (hold) napi_ref_threadsafe_function(env, q->tsfn);\n' +
  '  else napi_unref_threadsafe_function(env, q->tsfn);\n' +
  '}\n' +
  'void cbq_call_begin(napi_env env, Q* q) {\n  q->pending++;\n  cbq_hold_update(env, q);\n}\n' +
  'void cbq_call_end(napi_env env, Q* q) {\n  q->pending--;\n  cbq_hold_update(env, q);\n}\n' +
  'int cbq_closed(napi_env env, Q* q) {\n  q->self_strong = 0;\n  cbq_hold_update(env, q);\n  return 1;\n}\n';

const FLUSH_COMPLETE =
  'static void flush_on_complete(kafka_common_Error_t* error, void* opaque) {\n' +
  '  call_ref(env, ctx->done, 1, &arg, NULL);\n' +
  '  free(ctx);\n' +
  '  cbq_call_end(env, &p->queue);\n' +
  '}\n';

const FLUSH =
  'napi_value js_flush(napi_env env, P* p) {\n' +
  '  if (p == NULL) return NULL;\n' +
  '  cbq_call_begin(env, &p->queue);\n' +
  '  kafka_x_P_flush_cb(p->view, flush_on_complete, ctx);\n' +
  '  return NULL;\n' +
  '}\n';

const CLOSE =
  'static void close_on_complete(kafka_common_Error_t* error, void* opaque) {\n' +
  '  cbq_closed(env, &p->queue);\n' +
  '  call_ref(env, ctx->done, 2, argv, NULL);\n' +
  '  cbq_call_end(env, &p->queue);\n' +
  '  producer_release(p);\n' +
  '}\n' +
  'napi_value js_close(napi_env env, P* p, int timed) {\n' +
  '  if (!timed) {\n' +
  '    cbq_call_begin(env, &p->queue);\n' +
  '    kafka_x_P_close_cb(p->view, close_on_complete, ctx);\n' +
  '  } else {\n' +
  '    cbq_call_begin(env, &p->queue);\n' +
  '    kafka_x_P_close_with_timeout_cb(p->view, 5, (kafka_x_P_close_with_timeout_cb_t)close_on_complete, ctx);\n' +
  '  }\n' +
  '  return NULL;\n' +
  '}\n';

/**
 * The fixture with `producer` in place of the producer's source (`QUEUE` the queue's unless given).
 *
 * @param {string} producer
 * @param {string} [queue]
 * @returns {import('./_project').Report}
 */
function run(producer, queue = QUEUE) {
  return lint(project({ 'confluent_kafka_bindings.h': HEADER, 'native/callback_queue.c': queue, 'native/producer.c': producer }), [RULE]);
}

test('a hold changed only by cbq_hold_update, and every _cb call counted until its completion, passes', () => {
  const r = run(FLUSH_COMPLETE + FLUSH + CLOSE);
  assert.equal(r.status, 0, r.text);
  // Two calls of the hold, the hold function, three begins, three _cb calls, two ends and two completions.
  assert.ok(r.out.includes('✅ check-tsfn-lifetime: 13 item(s) checked'), r.text);
});

test('a reference or an unreference outside the hold update, or none in it, is a finding', () => {
  let r = run(
    FLUSH_COMPLETE +
      FLUSH +
      CLOSE +
      'void start(napi_env env, P* p) {\n  napi_ref_threadsafe_function(env, p->tsfn);\n}\n' +
      'void idle(napi_env env, P* p) {\n  napi_unref_threadsafe_function(env, p->tsfn);\n}\n',
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    "native/producer.c:29: references the thread-safe function in start, not in the hold update (cbq_hold_update): the hold is decided in one place, from the client's open state and its calls waiting for their answer",
    "native/producer.c:32: unreferences the thread-safe function in idle, not in the hold update (cbq_hold_update): the hold is decided in one place, from the client's open state and its calls waiting for their answer",
  ]);
  // The unreference cbq_closed made before calls were counted.
  r = run(
    FLUSH_COMPLETE + FLUSH + CLOSE,
    QUEUE.replace('  q->self_strong = 0;\n', '  q->self_strong = 0;\n  napi_unref_threadsafe_function(env, q->tsfn);\n'),
  );
  assert.deepEqual(findings(r), [
    "native/callback_queue.c:17: unreferences the thread-safe function in cbq_closed, not in the hold update (cbq_hold_update): the hold is decided in one place, from the client's open state and its calls waiting for their answer",
  ]);
  r = run(
    FLUSH_COMPLETE + FLUSH + CLOSE,
    QUEUE.replace('  if (hold) napi_ref_threadsafe_function(env, q->tsfn);\n  else ', '  if (!hold) '),
  );
  assert.deepEqual(findings(r), [
    'cbq_hold_update: calls no napi_ref_threadsafe_function: a call made on a closed client would not hold Node until its answer',
  ]);
  r = run(FLUSH_COMPLETE + FLUSH + CLOSE, QUEUE.replace('\n  else napi_unref_threadsafe_function(env, q->tsfn);', ''));
  assert.deepEqual(findings(r), ['cbq_hold_update: calls no napi_unref_threadsafe_function: a closed client would keep Node running']);
});

test('cbq_closed called before the core answered the close, outside the close completion, is a finding', () => {
  const r = run(
    FLUSH_COMPLETE +
      FLUSH.replace('  cbq_call_begin(env, &p->queue);\n', '  cbq_closed(env, &p->queue);\n  cbq_call_begin(env, &p->queue);\n') +
      CLOSE,
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    "native/producer.c:8: calls cbq_closed in js_flush, not in a close completion (close_on_complete): the open client's hold ends only once the core answered the close",
  ]);
});

const UNCOUNTED =
  'js_flush calls kafka_x_P_flush_cb with no cbq_call_begin as the statement right before it: the call is not counted, so its answer may come once nothing holds Node';
const STRAY_BEGIN =
  'calls cbq_call_begin other than as the whole statement right before its own kafka_*_cb( call, in the same block: it may count a call not made, or miss one made, so the count holds Node forever or lets it go too soon';

test('a _cb call not right after a cbq_call_begin of its own, or a cbq_call_begin anywhere else, is a finding', () => {
  let r = run(FLUSH_COMPLETE + FLUSH.replace('  cbq_call_begin(env, &p->queue);\n', '') + CLOSE);
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [`native/producer.c:8: ${UNCOUNTED}`]);
  const flush = (/** @type {string} */ body) =>
    FLUSH_COMPLETE +
    FLUSH.replace('  cbq_call_begin(env, &p->queue);\n  kafka_x_P_flush_cb(p->view, flush_on_complete, ctx);\n', body) +
    CLOSE;
  const CALL = '  kafka_x_P_flush_cb(p->view, flush_on_complete, ctx);\n';
  // The begin moved above an early return: the count stays up when it returns.
  r = run(
    FLUSH_COMPLETE +
      FLUSH.replace(
        '  if (p == NULL) return NULL;\n  cbq_call_begin(env, &p->queue);\n',
        '  cbq_call_begin(env, &p->queue);\n  if (p == NULL) return NULL;\n',
      ) +
      CLOSE,
  );
  assert.deepEqual(findings(r), [`native/producer.c:7: js_flush ${STRAY_BEGIN}`, `native/producer.c:9: ${UNCOUNTED}`]);
  // One begin before two calls counts one of them.
  r = run(flush(`  cbq_call_begin(env, &p->queue);\n${CALL}${CALL}`));
  assert.deepEqual(findings(r), [`native/producer.c:10: ${UNCOUNTED}`]);
  // A begin after the call counts nothing.
  r = run(flush(`${CALL}  cbq_call_begin(env, &p->queue);\n`));
  assert.deepEqual(findings(r), [`native/producer.c:9: js_flush ${STRAY_BEGIN}`, `native/producer.c:8: ${UNCOUNTED}`]);
  // Two begins count one call twice.
  r = run(flush(`  cbq_call_begin(env, &p->queue);\n  cbq_call_begin(env, &p->queue);\n${CALL}`));
  assert.deepEqual(findings(r), [`native/producer.c:8: js_flush ${STRAY_BEGIN}`]);
  // Under a condition the call is not under, braced or not, or the call under one the begin is not under.
  /** @type {[string, string[]][]} */
  const conditions = [
    [
      `  if (p->owners > 2) cbq_call_begin(env, &p->queue);\n${CALL}`,
      [`native/producer.c:8: js_flush ${STRAY_BEGIN}`, `native/producer.c:9: ${UNCOUNTED}`],
    ],
    [
      `  if (p->owners > 2) {\n    cbq_call_begin(env, &p->queue);\n  }\n${CALL}`,
      [`native/producer.c:9: js_flush ${STRAY_BEGIN}`, `native/producer.c:11: ${UNCOUNTED}`],
    ],
    [
      `  cbq_call_begin(env, &p->queue);\n  if (p->owners > 2) ${CALL.trimStart()}`,
      [`native/producer.c:8: js_flush ${STRAY_BEGIN}`, `native/producer.c:9: ${UNCOUNTED}`],
    ],
  ];
  for (const [body, found] of conditions) {
    r = run(flush(body));
    assert.deepEqual(findings(r), found);
  }
  // In a function that makes no _cb call.
  r = run(
    FLUSH_COMPLETE +
      FLUSH +
      CLOSE +
      'void js_pump(napi_env env, P* p) {\n  cbq_drain(env, &p->queue);\n  cbq_call_begin(env, &p->queue);\n}\n',
  );
  assert.deepEqual(findings(r), [`native/producer.c:30: js_pump ${STRAY_BEGIN}`]);
});

test('a completion that does not end its count once, last, on every path, or an end anywhere else, is a finding', () => {
  const ending = (/** @type {string} */ body) =>
    FLUSH_COMPLETE.replace('  call_ref(env, ctx->done, 1, &arg, NULL);\n  free(ctx);\n  cbq_call_end(env, &p->queue);\n', body);
  // None, and two.
  let r = run(ending('  call_ref(env, ctx->done, 1, &arg, NULL);\n  free(ctx);\n') + FLUSH + CLOSE);
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'native/producer.c:1: the completion flush_on_complete calls cbq_call_end 0 time(s), not once: each counted call ends exactly once',
  ]);
  r = run(
    ending('  cbq_call_end(env, &p->queue);\n  call_ref(env, ctx->done, 1, &arg, NULL);\n  cbq_call_end(env, &p->queue);\n') +
      FLUSH +
      CLOSE,
  );
  assert.deepEqual(findings(r), [
    'native/producer.c:1: the completion flush_on_complete calls cbq_call_end 2 time(s), not once: each counted call ends exactly once',
  ]);
  // Before the JS callback.
  r = run(ending('  cbq_call_end(env, &p->queue);\n  call_ref(env, ctx->done, 1, &arg, NULL);\n  free(ctx);\n') + FLUSH + CLOSE);
  assert.deepEqual(findings(r), [
    'native/producer.c:1: the completion flush_on_complete calls cbq_call_end (line 2) before its end or under a condition: it is the last statement of the body, after the JS callback, and only producer_release may follow it',
  ]);
  // Under a condition, braced or not.
  /** @type {[string, number][]} */
  const conditions = [
    ['  call_ref(env, ctx->done, 1, &arg, NULL);\n  if (ctx->done != NULL) cbq_call_end(env, &p->queue);\n', 3],
    ['  call_ref(env, ctx->done, 1, &arg, NULL);\n  if (ctx->done != NULL) {\n    cbq_call_end(env, &p->queue);\n  }\n', 4],
  ];
  for (const [body, line] of conditions) {
    r = run(ending(body) + FLUSH + CLOSE);
    assert.deepEqual(findings(r), [
      `native/producer.c:1: the completion flush_on_complete calls cbq_call_end (line ${line}) before its end or under a condition: it is the last statement of the body, after the JS callback, and only producer_release may follow it`,
    ]);
  }
  // An early return skips it.
  r = run(
    ending('  if (ctx == NULL) return;\n  call_ref(env, ctx->done, 1, &arg, NULL);\n  cbq_call_end(env, &p->queue);\n') + FLUSH + CLOSE,
  );
  assert.deepEqual(findings(r), [
    'native/producer.c:1: the completion flush_on_complete has a return: a path that returns skips its cbq_call_end (line 4)',
  ]);
  // In a function no _cb call is handed, the delivery callback's, or through a helper the completion calls.
  r = run(
    FLUSH_COMPLETE +
      FLUSH +
      CLOSE +
      'static void on_delivery(void* self_, const kafka_x_M_t* metadata) {\n  call_ref(env, ctx->on_complete, 2, argv, NULL);\n  cbq_call_end(env, &p->queue);\n}\n',
  );
  assert.deepEqual(findings(r), [
    'native/producer.c:30: on_delivery calls cbq_call_end, but no kafka_*_cb( call is handed it as its completion: only the completion of a counted call ends the count, once',
  ]);
  r = run(
    'static void flush_end(P* p) {\n  cbq_call_end(p->env, &p->queue);\n}\n' +
      ending('  call_ref(env, ctx->done, 1, &arg, NULL);\n  flush_end(p);\n  cbq_call_end(env, &p->queue);\n') +
      FLUSH +
      CLOSE,
  );
  assert.deepEqual(findings(r), [
    'native/producer.c:2: flush_end calls cbq_call_end, but no kafka_*_cb( call is handed it as its completion: only the completion of a counted call ends the count, once',
    "native/producer.c:6: flush_on_complete calls flush_end, which calls cbq_call_end: a count ends only in its completion's own body, which the core runs once",
  ]);
  // One the rule cannot read.
  r = run(FLUSH.replace('flush_on_complete, ctx', 'elsewhere_on_complete, ctx') + CLOSE);
  assert.deepEqual(findings(r), [
    'native/producer.c:4: js_flush hands kafka_x_P_flush_cb the completion elsewhere_on_complete, which no native/ function defines: its cbq_call_end cannot be checked',
  ]);
});
