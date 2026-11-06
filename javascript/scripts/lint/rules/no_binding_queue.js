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

// N8 check-no-binding-queue (javascript/CLAUDE.md boundary rule 7: no
// binding-side send queue, buffer, cap or lock). A heuristic (PLAN.md §8
// decision 13):
//   - no lock outside src/callback_queue.c: no `pthread_mutex` / `cond` /
//     `rwlock` / `spin` or libuv / C11 lock declared (each declaration keyed
//     by its declarator, `src/util.h:CallbackQueue.notify_mtx`) or called
//     (`file:function:callee#n`) in another src/ file;
//   - no store of records in lib/producer/: no class field, constructor
//     parameter property or module variable holding an array, a `Map` or a
//     `Set`, and no module constant naming a cap (`MAX`, `LIMIT`, `CAP`,
//     `QUEUE`).
// What is listed is allowed, with its reason; an entry that matches nothing
// is a finding.

const { cModel, applyAllowlist } = require('../c_model');
const { scan } = require('../c_source');

/** The name of the struct whose braces enclose offset `at` of `code` (`typedef struct {...} Name;` or `struct Name {`), or null. */
function enclosingStruct(code, at) {
  let depth = 0;
  for (let i = at; i >= 0; i--) {
    if (code[i] === '}') depth++;
    else if (code[i] === '{') {
      if (depth > 0) {
        depth--;
        continue;
      }
      const before = code.slice(Math.max(0, i - 80), i);
      const named = /struct\s+(\w+)\s*$/.exec(before);
      if (named) return named[1];
      if (!/struct\s*$/.test(before)) return null;
      // An anonymous typedef struct: its name follows the closing brace.
      let d = 0;
      for (let j = i; j < code.length; j++) {
        if (code[j] === '{') d++;
        else if (code[j] === '}' && --d === 0) {
          const after = /^\s*(\w+)/.exec(code.slice(j + 1));
          return after ? after[1] : null;
        }
      }
      return null;
    }
  }
  return null;
}

/** The function of src file `file` whose body holds offset `at`, or `(top level)`. */
function functionAt(ctx, file, at) {
  const { bodies } = cModel(ctx);
  for (const [name, b] of bodies) if (b.file === file && at >= b.bodyStart && at < b.bodyEnd) return name;
  return '(top level)';
}

const LOCK = /^(pthread_(mutex|cond|rwlock|spin)\w*|uv_(mutex|rwlock|cond|sem)\w*|mtx_\w+|cnd_\w+)$/;
const CAP = /(^|_)(MAX|LIMIT|CAP|QUEUE)(_|$)/;

/** What is allowed, by `<file>:<identifier>` (C) or `<Class>.<field>` (TypeScript). */
const ALLOWED = [
  { key: 'src/util.h:CallbackQueue.notify_mtx', reason: "the callback queue's notify_mtx, which only callback_queue.c locks" },
  {
    key: 'lib/producer/producer_record.ts:INT32_MAX',
    reason: "the largest partition the C ABI's int32_t carries: a range check of one record, not a cap of records",
  },
  { key: 'ProducerRecord.#headers', reason: "a record's own headers (Java's ProducerRecord#headers), not a store of records" },
  {
    key: 'KafkaProducer.#deferred',
    reason: "the settlements waiting for the event-loop turn (javascript/CLAUDE.md rule 3); the records are handed over at once",
  },
];

module.exports = {
  name: 'check-no-binding-queue',
  kinds: ['lock', 'record-store', 'cap', 'queue-unused-allowance'],
  needs: ['program', 'src'],
  hint:
    'Hand every record to the core at once and hold no queue, buffer, cap or lock of records in the ' +
    'binding (javascript/CLAUDE.md boundary rule 7).',
  ALLOWED,
  check(ctx) {
    const { ts, checker, sourceFiles } = ctx.program();
    const raw = [];
    let checked = 0;
    for (const file of ctx.files(['src'], ['.c', '.h'])) {
      if (file === 'src/callback_queue.c') continue;
      const { tokens, code } = scan(ctx.read(file));
      const perCallee = new Map();
      tokens.forEach((t, i) => {
        if (t.type !== 'ident') return;
        checked += 1;
        if (!LOCK.test(t.value)) return;
        const next = tokens[i + 1];
        // A declaration: the type, then its declarator (`pthread_mutex_t notify_mtx;`).
        if (next && next.type === 'ident') {
          const owner = enclosingStruct(code, t.start);
          const name = owner ? `${owner}.${next.value}` : next.value;
          raw.push({
            kind: 'lock',
            subject: `${file}:${t.line}`,
            message: `declares the ${t.value} ${name}: no lock outside src/callback_queue.c`,
            allow: `${file}:${name}`,
          });
        } else if (next && next.value === '(') {
          const fn = functionAt(ctx, file, t.start);
          const n = (perCallee.get(`${fn}:${t.value}`) || 0) + 1;
          perCallee.set(`${fn}:${t.value}`, n);
          raw.push({
            kind: 'lock',
            subject: `${file}:${t.line}`,
            message: `calls ${t.value} in ${fn}: no lock outside src/callback_queue.c`,
            allow: `${file}:${fn}:${t.value}#${n}`,
          });
        } else {
          raw.push({ kind: 'lock', subject: `${file}:${t.line}`, message: `names ${t.value}: no lock outside src/callback_queue.c`, allow: `${file}:${t.value}` });
        }
      });
    }
    /** Whether the TypeScript type of `node` stores values: an array, a tuple, a Map or a Set. */
    const stores = (node) => {
      const type = checker.getNonNullableType(checker.getTypeAtLocation(node));
      const name = type.symbol ? type.symbol.name : '';
      return checker.isArrayType(type) || checker.isTupleType(type) || ['Map', 'Set', 'Array', 'WeakMap'].includes(name)
        ? checker.typeToString(type)
        : null;
    };
    for (const sf of sourceFiles) {
      const rel = ctx.rel(sf.fileName);
      if (!rel.startsWith('lib/producer/')) continue;
      for (const st of sf.statements) {
        if (ts.isVariableStatement(st)) {
          for (const d of st.declarationList.declarations) {
            if (!ts.isIdentifier(d.name)) continue;
            checked += 1;
            if (CAP.test(d.name.text)) {
              raw.push({ kind: 'cap', subject: `${rel}:${d.name.text}`, message: 'names a cap: the binding caps no records', allow: `${rel}:${d.name.text}` });
            }
            const held = stores(d);
            if (held) {
              raw.push({
                kind: 'record-store',
                subject: `${rel}:${d.name.text}`,
                message: `holds a ${held} at module level: the binding keeps no records`,
                allow: `${rel}:${d.name.text}`,
              });
            }
          }
        }
        if (!ts.isClassDeclaration(st) || !st.name) continue;
        // Its fields, and its constructor's parameter properties.
        const fields = st.members.filter((m) => ts.isPropertyDeclaration(m) && m.name);
        for (const ctor of st.members.filter((m) => ts.isConstructorDeclaration(m))) {
          for (const p of ctor.parameters) {
            if (ts.getEffectiveModifierFlags(p) & ts.ModifierFlags.ParameterPropertyModifier) fields.push(p);
          }
        }
        for (const m of fields) {
          checked += 1;
          const held = stores(m);
          if (held) {
            const field = `${st.name.text}.${m.name.getText(sf)}`;
            raw.push({ kind: 'record-store', subject: field, message: `holds a ${held}: the binding keeps no records`, allow: field });
          }
        }
      }
    }
    const { kept, unused } = applyAllowlist(raw, this.ALLOWED, 'queue-unused-allowance');
    return { findings: [...kept, ...unused], checked };
  },
};
