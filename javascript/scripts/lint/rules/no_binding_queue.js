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
//   - no lock outside native/callback_queue.c: no `pthread_mutex` / `cond` /
//     `rwlock` / `spin` or libuv / C11 lock declared (each declaration keyed
//     by its declarator, `native/util.h:CallbackQueue.notify_mtx`) or called
//     (`file:function:callee#n`) in another native/ file;
//   - no store of records in a hand-written module of src/ (src/producer/,
//     and the private helpers, `src/internal/*.ts`, where one would go): no class
//     field, constructor parameter property or module variable holding an
//     array, a `Map` or a `Set`, and no module constant naming a cap (`MAX`,
//     `LIMIT`, `CAP`, `QUEUE`). The generated modules are not read.
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

/** The function of native file `file` whose body holds offset `at`, or `(top level)`. */
function functionAt(ctx, file, at) {
  const { bodies } = cModel(ctx);
  for (const b of bodies.values()) if (b.file === file && at >= b.bodyStart && at < b.bodyEnd) return b.name;
  return '(top level)';
}

const LOCK = /^(pthread_(mutex|cond|rwlock|spin)\w*|uv_(mutex|rwlock|cond|sem)\w*|mtx_\w+|cnd_\w+)$/;
const CAP = /(^|_)(MAX|LIMIT|CAP|QUEUE)(_|$)/;
/** The mark of a file `cargo xtask generate-error-codes` writes. */
const GENERATED = /GENERATED, DO NOT EDIT/;

/** What is allowed, by `<file>:<identifier>` (C) or `<Class>.<field>` (TypeScript). */
const ALLOWED = [
  { key: 'native/util.h:CallbackQueue.notify_mtx', reason: "the callback queue's notify_mtx, which only callback_queue.c locks" },
  {
    key: 'src/producer/producer_record.ts:INT32_MAX',
    reason: "the largest partition the C ABI's int32_t carries: a range check of one record, not a cap of records",
  },
  {
    key: 'src/producer/producer_record.ts:INT64_MAX',
    reason: "the largest timestamp Java's long carries: a range check of one record, not a cap of records",
  },
  {
    key: 'src/internal/java_number.ts:INT32_MAX',
    reason: "the largest Java int: a range check of one argument, not a cap of records",
  },
  {
    key: 'src/internal/java_number.ts:INT64_MAX',
    reason: "the largest Java long: a range check of one argument, not a cap of records",
  },
  { key: 'Headers.#headers', reason: "one record's headers (Java's RecordHeaders' list), not a store of records" },
  {
    key: 'src/common/header/headers.ts:EMPTY',
    reason: 'the one frozen empty list of every Headers that has none: no array per record, not a store of records',
  },
  { key: 'src/internal/errors.ts:classes', reason: 'the generated error classes, loaded once: a table of classes, not records' },
  { key: 'src/internal/errors.ts:byId', reason: 'the error classes by FFI id, built once from the generated registry' },
  { key: 'src/internal/errors.ts:roots', reason: 'the generated error classes at the top of a chain, found once' },
  { key: 'src/internal/native_loader.ts:SUPPORTED_PLATFORMS', reason: 'the platform packages the loader knows, a constant list' },
  { key: 'src/internal/throwable.ts:registered', reason: "the Java constructor forms of each error class, keyed weakly by the class" },
  {
    key: 'JavaForms.#bare',
    reason: "a method's one-parameter Java forms, fixed when its module loads: a table of forms, not records",
  },
  { key: 'PartitionInfo.#replicas', reason: "one partition's replicas (Java's Node[]), not a store of records" },
  { key: 'PartitionInfo.#inSyncReplicas', reason: "one partition's in-sync replicas (Java's Node[]), not a store of records" },
  { key: 'PartitionInfo.#offlineReplicas', reason: "one partition's offline replicas (Java's Node[]), not a store of records" },
  { key: 'SslClientAuth.VALUES', reason: "Java's SslClientAuth.VALUES, the enum's constants in order" },
  {
    key: 'src/common/kafka_future.ts:MAX_TIMER_MS',
    reason:
      "the longest delay a Node timer holds (2^31 - 1 ms): one get(timeout)'s wait in steps, for a future with no native " +
      "handle (allOf(), completedFuture(), thenApply's): a record's future waits natively; not a cap of records",
  },
  {
    key: 'src/common/kafka_future.ts:STAGES',
    reason: "the Promise toCompletionStage() returned, keyed weakly by its future: one per future, made on first use, not a store of records",
  },
  {
    key: 'src/common/kafka_future.ts:COMPLETIONS',
    reason: "the futures completed in one call whose waiters have not run yet, emptied before the call returns: Java's postComplete loop, not a store of records",
  },
  {
    key: 'KafkaFutureImpl.#waiters',
    reason: "the get() Promises and dependants waiting on one future, run once at its completion: not a store of records",
  },
  {
    key: 'KafkaProducer.#deferred',
    reason: "the settlements waiting for the event-loop turn (javascript/CLAUDE.md rule 3); the records are handed over at once",
  },
];

module.exports = {
  name: 'check-no-binding-queue',
  kinds: ['lock', 'record-store', 'cap', 'queue-unused-allowance'],
  needs: ['program', 'native'],
  hint:
    'Hand every record to the core at once and hold no queue, buffer, cap or lock of records in the ' +
    'binding (javascript/CLAUDE.md boundary rule 7).',
  ALLOWED,
  check(ctx) {
    const { ts, checker, sourceFiles } = ctx.program();
    const raw = [];
    let checked = 0;
    for (const file of ctx.files(['native'], ['.c', '.h'])) {
      if (file === 'native/callback_queue.c') continue;
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
            message: `declares the ${t.value} ${name}: no lock outside native/callback_queue.c`,
            allow: `${file}:${name}`,
          });
        } else if (next && next.value === '(') {
          const fn = functionAt(ctx, file, t.start);
          const n = (perCallee.get(`${fn}:${t.value}`) || 0) + 1;
          perCallee.set(`${fn}:${t.value}`, n);
          raw.push({
            kind: 'lock',
            subject: `${file}:${t.line}`,
            message: `calls ${t.value} in ${fn}: no lock outside native/callback_queue.c`,
            allow: `${file}:${fn}:${t.value}#${n}`,
          });
        } else {
          raw.push({ kind: 'lock', subject: `${file}:${t.line}`, message: `names ${t.value}: no lock outside native/callback_queue.c`, allow: `${file}:${t.value}` });
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
      // Every hand-written module of src/, the private helpers (`src/internal/*.ts`) included:
      // a generated one holds the error and config tables, never records.
      if (!rel.startsWith('src/') || GENERATED.test(sf.text.slice(0, 2000))) continue;
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
