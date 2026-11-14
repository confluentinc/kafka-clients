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
// drainer per client, never a destroy inside a drain, a destroy only from the
// handle object's finalizer, and only for a closed client):
//   - a client's `__execute_callbacks_err` is called only from the queue's
//     executor, the `cbq_execute_fn` a client hands to `cbq_init`, which only
//     `cbq_drain` calls (`q->execute(...)`);
//   - `cbq_drain` is called only from the threadsafe function's `call_js` and
//     from the producer's pump;
//   - no `_destroy` call sits lexically inside `cbq_drain`'s loop, whatever
//     its form (a `cbq_drain` without a loop the rule can read fails);
//   - no function that runs inside the drain (a completion native/ hands to a
//     `_cb` twin or to an interface's constructor) reaches a client's
//     destroy (`<Class>_destroy` of a class with `__execute_callbacks`,
//     directly or through an `__as_` view) through the C call graph;
//   - a client's destroy is called only from a function the handle object's
//     finalizer (the finalize callback handed to `napi_wrap`) reaches, or
//     from a listed function (the constructor's failure paths, before the
//     handle exists);
//   - a finalizer that reaches a client's destroy asks the queue whether the
//     core reported the client closed (`cbq_is_closed`) in its own body: no
//     finalizer closes or destroys an open client.
// The executors, drain callers and destroy callers are listed with their
// reasons; an entry that matches nothing is a finding.

const { cModel, bodyKey, applyAllowlist } = require('../c_model');
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

/** The functions allowed to call a client's destroy outside the finalizer. */
const DESTROY_CALLERS = [
  {
    key: 'js_producer_create',
    reason: 'the constructor\'s failure paths: the class handle is destroyed before a handle object, and so a finalizer, exists',
  },
];

module.exports = {
  name: 'check-single-drainer',
  kinds: [
    'execute-outside-drain',
    'executor-called',
    'drain-caller',
    'destroy-in-drain',
    'destroy-in-completion',
    'destroy-outside-finalizer',
    'destroy-of-open-client',
    'drain-missing',
    'drain-unused-allowance',
  ],
  needs: ['native', 'header'],
  EXECUTORS,
  DRAIN_CALLERS,
  DESTROY_CALLERS,
  hint:
    "Run a client's __execute_callbacks only through cbq_drain, from call_js or the pump, and destroy a closed " +
    "client only from its handle object's finalizer (javascript/CLAUDE.md boundary rules 3 and 10).",
  check(ctx) {
    const { calls, header, resolve } = cModel(ctx);
    const raw = [];
    let checked = 0;
    const drain = resolve('cbq_drain');
    if (!drain) {
      return { findings: [{ kind: 'drain-missing', subject: 'cbq_drain', message: 'is defined in no native/*.c' }], checked: 1 };
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
    // The native functions the loop calls by name run inside the drain too.
    const loopCalls = [];
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
        } else if (resolve(c.name, c.file)) {
          loopCalls.push(c);
        }
      }
    }
    // What runs inside the drain: every function native/ hands to a `_cb` twin
    // (its completion) or to an interface's constructor (`..._new` /
    // `..._new_err`, as an `_fn_t`), and what they call. None reaches a
    // client's destroy: only the handle object's finalizer destroys one
    // (rule 10).
    const clientDestroys = new Set();
    for (const name of header.keys()) {
      const m = /^(kafka_\w+?)_(\w+?)_destroy(_err)?$/.exec(name);
      if (!m) continue;
      const [, prefix, cls] = m;
      const views = [...header.keys()].filter((h) => h.startsWith(`${prefix}_${cls}__as_`)).map((h) => h.slice(`${prefix}_${cls}__as_`.length).replace(/_err$/, ''));
      const executes = (iface) => header.has(`${prefix}_${iface}__execute_callbacks_err`) || header.has(`${prefix}_${iface}__execute_callbacks`);
      if (executes(cls) || views.some(executes)) clientDestroys.add(name);
    }
    // The call graph, by definition (`file:name`): a call reaches the
    // definition C's linkage gives it (`resolve`).
    const graph = new Map();
    for (const c of calls) {
      if (!c.function) continue;
      const k = `${c.file}:${c.function}`;
      if (!graph.has(k)) graph.set(k, []);
      graph.get(k).push(c);
    }
    const roots = new Map();
    for (const c of calls) {
      const proto = header.get(c.name);
      if (!proto) continue;
      proto.params.forEach((p, i) => {
        const body = resolve(c.args[i], c.file);
        if (!body) return;
        if (c.name.endsWith('_cb') || (/_new(_err)?$/.test(c.name) && /_fn_t$/.test(p.type))) roots.set(bodyKey(body), { body, via: c.name });
      });
    }
    // The executor each client hands to cbq_init (its `cbq_execute_fn`
    // parameter): the loop calls it through `q->execute`, so it runs inside the
    // drain and must not reach a destroy either.
    const init = resolve('cbq_init');
    const executeAt = init ? init.params.findIndex((p) => p.type === 'cbq_execute_fn') : -1;
    let handed = 0;
    for (const c of calls.filter((x) => x.name === 'cbq_init' && !x.directive)) {
      const body = executeAt >= 0 ? resolve(c.args[executeAt], c.file) : undefined;
      if (!body) continue;
      handed += 1;
      roots.set(bodyKey(body), { body, via: 'cbq_init as the executor', kind: 'destroy-in-drain' });
    }
    if (calls.some((c) => c.name === 'execute' && c.file === drain.file && c.function === 'cbq_drain') && handed === 0) {
      raw.push({
        kind: 'drain-missing',
        subject: 'cbq_init',
        message: "is passed no native function as its cbq_execute_fn: the rule cannot follow the executor cbq_drain's loop calls",
      });
    }
    for (const c of loopCalls) {
      const body = resolve(c.name, c.file);
      if (!roots.has(bodyKey(body))) roots.set(bodyKey(body), { body, via: "cbq_drain's loop", kind: 'destroy-in-drain', called: true });
    }
    for (const [rootKey, { body: root, via, kind, called }] of [...roots].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) {
      checked += 1;
      const seen = new Map([[rootKey, [root.name]]]);
      const work = [rootKey];
      while (work.length > 0) {
        const fn = work.shift();
        for (const c of graph.get(fn) || []) {
          const callee = resolve(c.name, c.file);
          if (clientDestroys.has(c.name)) {
            raw.push({
              kind: kind || 'destroy-in-completion',
              subject: root.name,
              message: `runs inside the drain (${called ? 'called by' : kind ? 'given to' : 'a completion given to'} ${via}) and reaches ${c.name} through ${[...seen.get(fn), c.name].join(' -> ')} (${c.file}:${c.line}): mark the client and destroy after the drain`,
            });
          } else if (callee && !seen.has(bodyKey(callee))) {
            seen.set(bodyKey(callee), [...seen.get(fn), c.name]);
            work.push(bodyKey(callee));
          }
        }
      }
    }
    // Every destroy of a client: the functions that reach one, through the C
    // call graph, are reached only from the handle object's finalizer (the
    // finalize callback handed to napi_wrap, its 4th argument), which asks
    // cbq_is_closed first, or from a listed function. A function of that set
    // no native function calls (one JavaScript calls, a completion, a
    // finalizer) is where a destroy starts: it must be one of those.
    const finalizers = new Set();
    for (const c of calls.filter((x) => x.name === 'napi_wrap' && !x.directive)) {
      const body = c.args.length > 3 ? resolve(c.args[3].trim(), c.file) : undefined;
      if (body) finalizers.add(bodyKey(body));
    }
    const callers = new Map();
    for (const c of calls) {
      if (c.directive || !c.function) continue;
      const callee = resolve(c.name, c.file);
      if (!callee) continue;
      if (!callers.has(bodyKey(callee))) callers.set(bodyKey(callee), new Set());
      callers.get(bodyKey(callee)).add(`${c.file}:${c.function}`);
    }
    // Each function reaching a destroy, with the first destroy call it reaches.
    const reaching = new Map();
    const work = [];
    for (const c of calls) {
      if (c.directive || !c.function || !clientDestroys.has(c.name)) continue;
      checked += 1;
      const k = `${c.file}:${c.function}`;
      if (!reaching.has(k)) {
        reaching.set(k, c);
        work.push(k);
      }
    }
    while (work.length > 0) {
      const k = work.shift();
      for (const caller of callers.get(k) || []) {
        if (!reaching.has(caller)) {
          reaching.set(caller, reaching.get(k));
          work.push(caller);
        }
      }
    }
    for (const [k, c] of [...reaching].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) {
      if ((callers.get(k) || new Set()).size > 0) continue;
      const name = k.slice(k.lastIndexOf(':') + 1);
      if (finalizers.has(k)) {
        const file = k.slice(0, k.lastIndexOf(':'));
        const asked = calls.some((x) => x.file === file && x.function === name && x.name === 'cbq_is_closed');
        if (!asked) {
          raw.push({
            kind: 'destroy-of-open-client',
            subject: name,
            message: `reaches ${c.name} (${c.file}:${c.line}) without asking cbq_is_closed: a finalizer destroys a closed client only, never an open one`,
          });
        }
        continue;
      }
      raw.push({
        kind: 'destroy-outside-finalizer',
        subject: name,
        message: `reaches ${c.name} (${c.file}:${c.line}), which only the handle object's finalizer (napi_wrap's) may: no close, completion or call destroys a client`,
        allow: name,
        list: 'destroy',
      });
    }
    const destroys = applyAllowlist(raw.filter((f) => f.list === 'destroy'), this.DESTROY_CALLERS, 'drain-unused-allowance');
    const executors = applyAllowlist(raw.filter((f) => f.list === 'executors'), this.EXECUTORS, 'drain-unused-allowance');
    const drains = applyAllowlist(raw.filter((f) => f.list === 'drain'), this.DRAIN_CALLERS, 'drain-unused-allowance');
    const other = raw.filter((f) => !f.list);
    const strip = (fs) => fs.map(({ list, allow, ...f }) => f);
    return {
      findings: [
        ...strip(other),
        ...executors.kept,
        ...executors.unused,
        ...strip(drains.kept),
        ...drains.unused,
        ...strip(destroys.kept),
        ...destroys.unused,
      ].map(({ list, ...f }) => f),
      checked,
    };
  },
};
