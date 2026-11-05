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

// The mutation smoke test of the rule linter
// (design/current/javascript-rule-linter/PLAN.md §6): a copy of the real lib/
// and src/ passes every rule; one known violation per rule, injected into the
// copy, makes exactly that rule fail. It catches a rule whose path through
// the real sources is broken, which a fixture of the rule alone would miss.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { TODO_MARK, project, copy, lint, failedRules } = require('./_project');

const ROOT = path.join(__dirname, '..', '..');

/** The last line of every source's license header. */
const LICENSE_END = 'limitations under the License.\n';

/** A copy of the client's lib/, src/, tsconfig.json and CLAUDE.md (whose rule sentence check-no-todo exempts). */
function realCopy() {
  const root = project({});
  for (const rel of ['lib', 'src', 'tsconfig.json', 'CLAUDE.md']) copy(ROOT, root, rel);
  return root;
}

/** Replaces `from` (which must occur) by `to` in root-relative file `rel`. */
function edit(root, rel, from, to) {
  const file = path.join(root, rel);
  const text = fs.readFileSync(file, 'utf8');
  assert.ok(text.includes(from), `${rel} has no ${JSON.stringify(from)}`);
  fs.writeFileSync(file, text.replace(from, to));
}

test('the unmutated copy passes every rule', () => {
  const r = lint(realCopy());
  assert.equal(r.status, 0, r.text);
});

const MUTATIONS = [
  {
    rule: 'check-no-todo',
    what: 'a marker in a comment',
    apply: (root) => edit(root, 'lib/producer/kafka_producer.ts', LICENSE_END, `${LICENSE_END}// ${TODO_MARK} later\n`),
  },
  {
    rule: 'check-no-todo',
    what: 'a marker in the addon',
    apply: (root) => edit(root, 'src/producer.c', '#include', `/* ${TODO_MARK} */\n#include`),
  },
  {
    rule: 'check-exception-word',
    what: 'a C identifier with the word',
    apply: (root) => edit(root, 'src/error.c', '#include', 'static int last_exception_code;\n#include'),
  },
  {
    rule: 'check-exception-word',
    what: 'a TypeScript identifier with the word',
    apply: (root) => edit(root, 'lib/common/kafka_future.ts', 'isCancelled()', 'isCancelledException()'),
  },
  {
    rule: 'check-private-members',
    what: 'a `_` member made public',
    apply: (root) => edit(root, 'lib/producer/producer_record.ts', 'private _toNative(', '_toNative('),
  },
  {
    rule: 'check-java-line',
    what: 'a comment citing a Java line',
    apply: (root) =>
      edit(root, 'lib/producer/record_metadata.ts', LICENSE_END, [LICENSE_END, "// As Java's Record", 'Metadata.java', ':49.\n'].join('')),
  },
];

for (const m of MUTATIONS) {
  test(`${m.what} fails exactly ${m.rule}`, () => {
    const root = realCopy();
    m.apply(root);
    const r = lint(root);
    assert.equal(r.status, 1, r.text);
    assert.deepEqual(failedRules(r), [m.rule], r.text);
  });
}
