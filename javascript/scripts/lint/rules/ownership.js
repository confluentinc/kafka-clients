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

// N6 check-ownership (javascript/CLAUDE.md boundary rule 9: ownership follows
// the signature). Owned and borrowed are read from the bindings header:
//   - native/ receives a handle type OWNED when it gets it as the return value
//     of a header function returning a non-const `kafka_..._t *` (an
//     `__as_` view excepted: it is borrowed, root CLAUDE.md §4), through an
//     `out_err` (`kafka_common_Error_t`), or as a non-const `kafka_..._t *`
//     parameter of a completion native/ implements for a `_cb` twin;
//   - each owned type is destroyed somewhere in native/ (a header
//     `<Type>_destroy` / `_destroy_err`), or handed to a header function whose
//     documentation says it consumes that parameter ("(consumed)"). A
//     set-level check;
//   - a delivery completion (an interface method native/ implements, an
//     `_fn_t`) never passes a `const` (borrowed) parameter to a destroy: a
//     header `_destroy`, or a native/ function that destroys its parameter,
//     whatever casts the argument carries (one is needed to pass a `const`
//     handle to a destroy at all).

const { atLine } = require('../location');
const { cModel, bodyKey } = require('../c_model');
const { bareArg } = require('../c_source');

const HANDLE = /^(const\s+)?(kafka_\w+_t)\s*\*$/;

/** `kafka_x_Y_t` -> the header's destroy functions for it. */
function destroysOf(type) {
  const base = type.slice(0, -'_t'.length);
  return [`${base}_destroy`, `${base}_destroy_err`];
}

module.exports = {
  name: 'check-ownership',
  kinds: ['never-destroyed', 'borrowed-destroyed'],
  needs: ['native', 'header'],
  hint:
    'Destroy every handle native/ receives owned, or hand it to a call that consumes it; never destroy a ' +
    'borrowed (const) one (javascript/CLAUDE.md boundary rule 9).',
  check(ctx) {
    const { bodies, calls, header, resolve } = cModel(ctx);
    const findings = [];
    let checked = 0;
    const owned = new Map(); // type -> where first received
    const firstAt = new Map(); // type -> the call that first received it
    const receive = (type, where, c) => {
      if (!owned.has(type)) {
        owned.set(type, where);
        firstAt.set(type, atLine(c.file, c.line));
      }
    };
    for (const c of calls) {
      const proto = header.get(c.name);
      if (!proto) continue;
      const ret = HANDLE.exec(proto.returnType);
      if (ret && !ret[1] && !c.name.includes('__as_')) receive(ret[2], `from ${c.name}, ${c.file}:${c.line}`, c);
      const last = proto.params[proto.params.length - 1];
      if (last && last.name === 'out_err') receive('kafka_common_Error_t', `through the out_err of ${c.name}, ${c.file}:${c.line}`, c);
      // A completion of a _cb twin: its non-const handle parameters are owned.
      if (c.name.endsWith('_cb')) {
        for (const a of c.args) {
          const fn = resolve(a, c.file);
          if (!fn) continue;
          for (const p of fn.params) {
            const m = HANDLE.exec(p.type.replace(/\s*\*$/, ' *'));
            if (m && !m[1]) receive(m[2], `as ${a}'s ${p.name}, a completion of ${c.name}`, c);
          }
        }
      }
    }
    const called = new Set(calls.map((c) => c.name));
    for (const [type, where] of [...owned].sort()) {
      checked += 1;
      if (destroysOf(type).some((d) => called.has(d))) continue;
      const consumed = calls.some((c) => {
        const proto = header.get(c.name);
        return (
          proto &&
          proto.params.some(
            (p) =>
              HANDLE.test(p.type.replace(/\s*\*$/, ' *')) &&
              HANDLE.exec(p.type.replace(/\s*\*$/, ' *'))[2] === type &&
              new RegExp(`\`${p.name}\`[^.;]*\\(consumed\\)|\`${p.name}\` is consumed`).test(proto.doc),
          )
        );
      });
      if (consumed) continue;
      findings.push({
        kind: 'never-destroyed',
        subject: type,
        at: firstAt.get(type),
        message: `native/ receives it owned (${where}) but never destroys it (${destroysOf(type).join(' / ')}) nor hands it to a call that consumes it`,
      });
    }
    // The native functions that destroy a parameter (`error_discard(e)`), to a fixed point:
    // `<function>#<parameter index>`.
    // `<file>:<function>#<parameter index>`.
    const destroyers = new Set();
    const destroysArg = (c, k) => {
      if (header.has(c.name) && /_destroy(_err)?$/.test(c.name)) return true;
      const callee = resolve(c.name, c.file);
      return callee !== undefined && destroyers.has(`${bodyKey(callee)}#${k}`);
    };
    for (let grew = true; grew; ) {
      grew = false;
      for (const [key0, fn] of bodies) {
        fn.params.forEach((p, i) => {
          const key = `${key0}#${i}`;
          if (!p.name || destroyers.has(key)) return;
          const own = calls.filter((c) => c.function === fn.name && c.file === fn.file);
          if (own.some((c) => c.args.some((a, k) => bareArg(a) === p.name && destroysArg(c, k)))) {
            destroyers.add(key);
            grew = true;
          }
        });
      }
    }
    // The interface methods native/ implements: functions passed to an interface's constructor as an _fn_t.
    const methods = new Map();
    for (const c of calls) {
      const proto = header.get(c.name);
      if (!proto || !/_new(_err)?$/.test(c.name)) continue;
      proto.params.forEach((p, i) => {
        const fn = /_fn_t$/.test(p.type) ? resolve(c.args[i], c.file) : undefined;
        if (fn) methods.set(bodyKey(fn), fn);
      });
    }
    for (const [, fn] of [...methods].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) {
      const { name } = fn;
      for (const p of fn.params) {
        const m = HANDLE.exec(p.type.replace(/\s*\*$/, ' *'));
        if (!m || !m[1]) continue;
        checked += 1;
        for (const c of calls.filter((x) => x.function === name && x.file === fn.file)) {
          c.args.forEach((a, k) => {
            if (bareArg(a) === p.name && destroysArg(c, k)) {
              findings.push({
                kind: 'borrowed-destroyed',
                subject: `${c.file}:${c.line}`,
                message: `${name} passes its borrowed ${p.name} (const) to ${c.name}, which destroys it`,
              });
            }
          });
        }
      }
    }
    return { findings, checked };
  },
};
