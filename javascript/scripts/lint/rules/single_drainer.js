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

// N4 check-single-drainer (javascript/CLAUDE.md boundary rules 3 and 10: one
// drainer per client, never a destroy inside a drain):
//   - a client's `__execute_callbacks_err` is called only from the queue's
//     executor, the `cbq_execute_fn` a client hands to `cbq_init`, which only
//     `cbq_drain` calls (`q->execute(...)`);
//   - `cbq_drain` is called only from the threadsafe function's `call_js` and
//     from the producer's pump;
//   - no `_destroy` call sits lexically inside `cbq_drain`'s loop, whatever
//     its form (a `cbq_drain` without a loop the rule can read fails);
//   - no function that runs inside the drain (a completion src/ hands to a
//     `_cb` twin or to an interface's constructor) reaches a client's
//     destroy (`<Class>_destroy` of a class with `__execute_callbacks`,
//     directly or through an `__as_` view) through the C call graph.
// The executors and drain callers are listed with their reasons; an entry
// that matches nothing is a finding.

const { cModel, applyAllowlist } = require('../c_model');
const { scan, matching } = require('../c_source');

/** The functions allowed to call `__execute_callbacks_err`: the queue's executors. */
const EXECUTORS = [
  { key: 'producer_execute', reason: "the producer's cbq_execute_fn, which cbq_drain alone calls (q->execute)" },
];

/** The functions allowed to call `cbq_drain`. */
const DRAIN_CALLERS = [
  { key: 'call_js', reason: "the threadsafe function's call_js: the drain the notify hook asked for" },
  { key: 'js_producer_pump', reason: "send()'s pump (javascript/CLAUDE.md rule 3)" },
];

module.exports = {
  name: 'check-single-drainer',
  kinds: [
    'execute-outside-drain',
    'executor-called',
    'drain-caller',
    'destroy-in-drain',
    'destroy-in-completion',
    'drain-missing',
    'drain-unused-allowance',
  ],
  needs: ['src', 'header'],
  EXECUTORS,
  DRAIN_CALLERS,
  hint:
    "Run a client's __execute_callbacks only through cbq_drain, from call_js or the pump, and destroy after " +
    'the drain loop (javascript/CLAUDE.md boundary rules 3 and 10).',
  check(ctx) {
    const { bodies, calls, header } = cModel(ctx);
    const raw = [];
    let checked = 0;
    const drain = bodies.get('cbq_drain');
    if (!drain) {
      return { findings: [{ kind: 'drain-missing', subject: 'cbq_drain', message: 'is defined in no src/*.c' }], checked: 1 };
    }
    for (const c of calls) {
      if (c.directive) continue;
      if (c.name.endsWith('__execute_callbacks_err')) {
        checked += 1;
        raw.push({
          kind: 'execute-outside-drain',
          subject: `${c.file}:${c.line}`,
          message: `${c.function} calls ${c.name}, which only the queue's executor may`,
          allow: c.function,
          list: 'executors',
        });
      } else if (c.name === 'cbq_drain') {
        checked += 1;
        raw.push({
          kind: 'drain-caller',
          subject: `${c.file}:${c.line}`,
          message: `${c.function} calls cbq_drain, which only call_js and the pump may`,
          allow: c.function,
          list: 'drain',
        });
      } else if (c.name === 'execute' && c.function !== 'cbq_drain') {
        raw.push({ kind: 'executor-called', subject: `${c.file}:${c.line}`, message: `${c.function} calls a queue's execute outside cbq_drain` });
      }
    }
    // An executor is never called directly: only through q->execute.
    for (const e of this.EXECUTORS) {
      for (const c of calls.filter((x) => x.name === e.key && !x.directive)) {
        raw.push({ kind: 'executor-called', subject: `${c.file}:${c.line}`, message: `${c.function} calls the executor ${e.key} directly` });
      }
    }
    // The drain loop, whatever its form (`while`, `for`, `do`), read on the
    // code with comments and literals blanked: no destroy inside it.
    const code = scan(drain.body).code;
    const loop = /\b(while|for)\s*\(|\bdo\s*\{/.exec(code);
    if (loop === null) {
      raw.push({ kind: 'drain-missing', subject: 'cbq_drain', message: 'has no loop (while, for, do) the rule can read' });
    } else {
      const open = loop[0].startsWith('do') ? loop.index + loop[0].length - 1 : code.indexOf('{', matching(code, code.indexOf('(', loop.index)));
      let depth = 0;
      let end = open;
      for (; end < code.length; end++) {
        if (code[end] === '{') depth++;
        else if (code[end] === '}' && --depth === 0) break;
      }
      const loopFrom = drain.bodyStart + loop.index;
      const loopTo = drain.bodyStart + end;
      for (const c of calls.filter((x) => x.file === drain.file && x.start >= loopFrom && x.start < loopTo)) {
        checked += 1;
        if (/_destroy(_err)?$/.test(c.name) && header.has(c.name)) {
          raw.push({ kind: 'destroy-in-drain', subject: `${c.file}:${c.line}`, message: `cbq_drain's loop calls ${c.name}: destroy after the loop` });
        }
      }
    }
    // What runs inside the drain: every function src/ hands to a `_cb` twin
    // (its completion) or to an interface's constructor (`..._new` /
    // `..._new_err`, as an `_fn_t`), and what they call. None reaches a
    // client's destroy: a close completion only marks the client, and the
    // drain's after-hook destroys it (rule 3).
    const clientDestroys = new Set();
    for (const name of header.keys()) {
      const m = /^(kafka_\w+?)_(\w+?)_destroy(_err)?$/.exec(name);
      if (!m) continue;
      const [, prefix, cls] = m;
      const views = [...header.keys()].filter((h) => h.startsWith(`${prefix}_${cls}__as_`)).map((h) => h.slice(`${prefix}_${cls}__as_`.length).replace(/_err$/, ''));
      const executes = (iface) => header.has(`${prefix}_${iface}__execute_callbacks_err`) || header.has(`${prefix}_${iface}__execute_callbacks`);
      if (executes(cls) || views.some(executes)) clientDestroys.add(name);
    }
    const graph = new Map();
    for (const c of calls) {
      if (!c.function) continue;
      if (!graph.has(c.function)) graph.set(c.function, []);
      graph.get(c.function).push(c);
    }
    const roots = new Map();
    for (const c of calls) {
      const proto = header.get(c.name);
      if (!proto) continue;
      proto.params.forEach((p, i) => {
        const arg = c.args[i];
        if (!bodies.has(arg)) return;
        if (c.name.endsWith('_cb') || (/_new(_err)?$/.test(c.name) && /_fn_t$/.test(p.type))) roots.set(arg, c.name);
      });
    }
    for (const [root, via] of [...roots].sort()) {
      checked += 1;
      const seen = new Map([[root, [root]]]);
      const work = [root];
      while (work.length > 0) {
        const fn = work.shift();
        for (const c of graph.get(fn) || []) {
          if (clientDestroys.has(c.name)) {
            raw.push({
              kind: 'destroy-in-completion',
              subject: root,
              message: `runs inside the drain (a completion given to ${via}) and reaches ${c.name} through ${[...seen.get(fn), c.name].join(' -> ')} (${c.file}:${c.line}): mark the client and destroy after the drain`,
            });
          } else if (bodies.has(c.name) && !seen.has(c.name)) {
            seen.set(c.name, [...seen.get(fn), c.name]);
            work.push(c.name);
          }
        }
      }
    }
    const executors = applyAllowlist(raw.filter((f) => f.list === 'executors'), this.EXECUTORS, 'drain-unused-allowance');
    const drains = applyAllowlist(raw.filter((f) => f.list === 'drain'), this.DRAIN_CALLERS, 'drain-unused-allowance');
    const other = raw.filter((f) => !f.list);
    const strip = (fs) => fs.map(({ list, allow, ...f }) => f);
    return { findings: [...strip(other), ...executors.kept, ...executors.unused, ...strip(drains.kept), ...drains.unused].map(({ list, ...f }) => f), checked };
  },
};
