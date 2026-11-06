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

// scripts/lint/c_source.js: the C scanning the rule linter and the
// header-conformance test share.

const test = require('node:test');
const assert = require('node:assert/strict');

const c = require('../../scripts/lint/c_source');

const SOURCE = `#include "util.h"
#define CHECK(env, call) \\
    do_check((env), (call))

/* a block
   comment with f(x) */
static const char* NAME = "not(a) /* comment */ // either";
static char QUOTE = '"';

static void helper(int a);

static napi_value js_run(napi_env env, napi_callback_info info) {
    int n = kafka_x_f(a, g(b, c), "s, t"); // trailing f(y)
    if (n) { CHECK(env, kafka_x_g(&ffi_err)); }
    return sizeof(int) ? NULL : NULL;
}

void helper(int a) {
    (void)a;
}

napi_value addon_init(napi_env env, napi_value exports) {
    napi_property_descriptor props[] = {
        {"run", NULL, js_run, NULL, NULL, NULL, napi_default, NULL},
        // a comment between entries
        {"value", NULL, NULL, get_value, set_value, NULL, napi_default, NULL},
    };
    return exports;
}
`;

test('scan: comments with their lines; literals and comments blanked in code, offsets kept', () => {
  const s = c.scan(SOURCE);
  assert.deepEqual(
    s.comments.map((x) => [x.line, x.text]),
    [
      [5, '/* a block\n   comment with f(x) */'],
      [13, '// trailing f(y)'],
      [25, '// a comment between entries'],
    ],
  );
  assert.equal(s.code.length, SOURCE.length);
  assert.equal(s.noComments.length, SOURCE.length);
  const literal = '"not(a) /* comment */ // either"';
  assert.ok(s.code.includes(`NAME = "${' '.repeat(literal.length - 2)}";`));
  assert.ok(s.noComments.includes('"not(a) /* comment */ // either"'));
  assert.ok(!s.noComments.includes('a block'));
  const idents = s.tokens.filter((t) => t.type === 'ident').map((t) => t.value);
  assert.ok(!idents.includes('comment') && !idents.includes('either') && !idents.includes('y'));
  assert.deepEqual(
    s.tokens.filter((t) => t.type === 'string' || t.type === 'char').map((t) => [t.line, t.value]),
    [
      [1, '"util.h"'],
      [7, '"not(a) /* comment */ // either"'],
      [8, `'"'`],
      [13, '"s, t"'],
      [24, '"run"'],
      [26, '"value"'],
    ],
  );
});

test('functionBodies: the definitions by name, not the prototypes', () => {
  const bodies = c.functionBodies(SOURCE);
  assert.deepEqual([...bodies.keys()], ['js_run', 'helper', 'addon_init']);
  const run = bodies.get('js_run');
  assert.equal(run.line, 12);
  assert.equal(run.signature, 'static napi_value js_run(napi_env env, napi_callback_info info)');
  assert.match(run.body, /^\n {4}int n = kafka_x_f/);
  assert.match(run.body, /NULL;\n$/);
  assert.equal(bodies.get('helper').body, '\n    (void)a;\n');
});

test('callSites: calls with their arguments, enclosing function and macro bodies', () => {
  const calls = c.callSites(SOURCE).map((x) => [x.name, x.args, x.line, x.function, x.directive]);
  assert.deepEqual(calls, [
    ['do_check', ['(env)', '(call)'], 3, null, true],
    ['kafka_x_f', ['a', 'g(b, c)', '"s, t"'], 13, 'js_run', false],
    ['g', ['b', 'c'], 13, 'js_run', false],
    ['CHECK', ['env', 'kafka_x_g(&ffi_err)'], 14, 'js_run', false],
    ['kafka_x_g', ['&ffi_err'], 14, 'js_run', false],
  ]);
  assert.deepEqual(
    c.callSites(SOURCE, 'kafka_x_g').map((x) => x.args),
    [['&ffi_err']],
  );
});

test('propertyDescriptors: the napi_property_descriptor table', () => {
  assert.deepEqual(c.propertyDescriptors(SOURCE), [
    { name: 'run', method: 'js_run', getter: null, setter: null, line: 24 },
    { name: 'value', method: null, getter: 'get_value', setter: 'set_value', line: 26 },
  ]);
});

const HEADER = `/**
 * Not kafka_doc_only(void).
 */
typedef void (*kafka_x_Done_cb_t)(kafka_common_Error_t *error, void *opaque);

void kafka_x_flush(const kafka_x_T_t *self_,
                   kafka_common_Error_t **out_err);

const kafka_common_Error_t *kafka_x_last(void);

void kafka_x_flush_cb(const kafka_x_T_t *self_, kafka_x_Done_cb_t cb, void *opaque);
`;

test('prototypes: the header functions with their parameters, typedefs and comments left out', () => {
  const p = c.prototypes(HEADER);
  assert.deepEqual([...p.keys()], ['kafka_x_flush', 'kafka_x_last', 'kafka_x_flush_cb']);
  assert.deepEqual(p.get('kafka_x_flush'), {
    doc: '',
    name: 'kafka_x_flush',
    returnType: 'void',
    params: [
      { type: 'const kafka_x_T_t *', name: 'self_' },
      { type: 'kafka_common_Error_t **', name: 'out_err' },
    ],
  });
  assert.deepEqual(p.get('kafka_x_last'), { name: 'kafka_x_last', returnType: 'const kafka_common_Error_t *', params: [], doc: '' });
  assert.deepEqual(p.get('kafka_x_flush_cb').params.map((x) => x.name), ['self_', 'cb', 'opaque']);
});

test('the shared helpers of the header-conformance test keep their behaviour', () => {
  assert.deepEqual([...c.declaredFunctions(HEADER)], ['kafka_x_flush', 'kafka_x_last', 'kafka_x_flush_cb']);
  assert.deepEqual([...c.calledFunctions(SOURCE)], ['kafka_x_f', 'kafka_x_g']);
  assert.equal(c.stripComments('a /* b */ c // d\ne'), 'a  c \ne');
  // scripts/header.js re-exports the same functions.
  const header = require('../../scripts/header');
  assert.equal(header.stripComments, c.stripComments);
  assert.equal(header.declaredFunctions, c.declaredFunctions);
});

test('prototypes carry the doc comment right before them; signatureParams reads a definition', () => {
  const header = '/**\n * Sends.\n *\n * `callback` (consumed).\n */\nvoid kafka_x_send(kafka_x_Callback_t *callback);\n\nvoid kafka_x_bare(void);\n';
  const p = c.prototypes(header);
  assert.equal(p.get('kafka_x_send').doc, 'Sends.\n\n`callback` (consumed).');
  assert.equal(p.get('kafka_x_bare').doc, '');
  assert.deepEqual(c.signatureParams('static void f(void* self_, const kafka_x_M_t* m, int64_t id)'), [
    { type: 'void*', name: 'self_' },
    { type: 'const kafka_x_M_t*', name: 'm' },
    { type: 'int64_t', name: 'id' },
  ]);
  assert.deepEqual(c.signatureParams('int g(void)'), []);
});

test("a typedef's doc before a function's is not taken for the function's", () => {
  const header = '/**\n * The completion.\n */\ntypedef void (*kafka_x_done_t)(void *opaque);\n\n/**\n * Closes.\n */\nvoid kafka_x_close(void);\n';
  assert.equal(c.prototypes(header).get('kafka_x_close').doc, 'Closes.');
});
