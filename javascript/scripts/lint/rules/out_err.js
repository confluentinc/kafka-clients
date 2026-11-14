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

// N1 check-out-err (root CLAUDE.md §4: `out_err` is never NULL and points at
// NULL on entry; nothing is silently dropped). An error slot is passed to
//   - every header function whose last parameter is
//     `kafka_common_Error_t **out_err`;
//   - every native function that forwards a `kafka_common_Error_t **` parameter
//     of its own as such a slot (`producer_execute`), found to a fixed point;
//   - every call through a function pointer whose typedef takes one (a
//     `cbq_execute_fn`, called as `q->execute(...)`).
// Each such call passes a slot, never `NULL`, and the slot is read: the call
// is the argument of `FFI_ERR` / `FFI_SET` / `FFI_DISCARD` (native/util.h) with
// the slot those macros read, `&ffi_err`; or a `ffi_take(&slot)` follows it
// in the next statement; or the slot is the calling function's own
// parameter, forwarded: its callers are checked in turn.

const { cModel, bodyKey, siteKey, applyAllowlist } = require('../c_model');

/**
 * The unread slots accepted, by call site, with their reasons (PLAN.md §8
 * decision 14).
 */
const ALLOWED = [
  {
    key: 'native/error.c:error_discard:kafka_common_Error_destroy_err#2',
    reason:
      'the end of the chain: the destroy of the panic error a failed destroy reported, whose own error ' +
      'cannot be destroyed in turn without looping (the user\'s decision of 2026-11-06)',
  },
];
const { scan, bareArg, signatureParams } = require('../c_source');

const MACROS = new Set(['FFI_ERR', 'FFI_SET', 'FFI_DISCARD']);

module.exports = {
  name: 'check-out-err',
  kinds: ['out-err-null', 'out-err-unread', 'out-err-unused-allowance'],
  ALLOWED,
  needs: ['native', 'header'],
  hint:
    'Pass the call an error slot (`&ffi_err`) and read it: wrap the call in FFI_ERR / FFI_SET / FFI_DISCARD ' +
    '(native/util.h), so no error is dropped (root CLAUDE.md §4).',
  check(ctx) {
    const { bodies, calls, header, resolve } = cModel(ctx);
    const findings = [];
    let checked = 0;
    const isSlot = (type) => /kafka_common_Error_t\s*\*\s*\*/.test(type);
    const noComments = (file) => ctx.once(`noComments:${file}`, () => scan(ctx.read(file)).noComments);
    // The function-pointer typedefs taking a slot (`cbq_execute_fn`), and every
    // name declared with one (a struct member, a parameter): a call by that
    // name is a call through the pointer.
    const pointerTypes = new Map();
    for (const file of ctx.files(['native'], ['.c', '.h'])) {
      const re = /\btypedef\s+[^;()]*\(\s*\*\s*(\w+)\s*\)\s*\(([^;]*?)\)\s*;/g;
      let m;
      while ((m = re.exec(scan(ctx.read(file)).code)) !== null) {
        const at = signatureParams(`f(${m[2]})`).findIndex((p) => isSlot(p.type));
        if (at >= 0) pointerTypes.set(m[1], at);
      }
    }
    const pointers = new Map();
    for (const file of ctx.files(['native'], ['.c', '.h'])) {
      const code = scan(ctx.read(file)).code;
      for (const [type, at] of pointerTypes) {
        const re = new RegExp(`\\b${type}\\s+(\\w+)\\s*[;,)]`, 'g');
        let m;
        while ((m = re.exec(code)) !== null) pointers.set(m[1], at);
      }
    }
    // The slot index of call `c`, or -1: a header out_err, a forwarder's, a pointer's.
    const forwarders = new Map();
    const slotIndex = (c) => {
      const proto = header.get(c.name);
      if (proto) {
        const last = proto.params[proto.params.length - 1];
        return last && last.name === 'out_err' && /kafka_common_Error_t \*\*/.test(last.type) ? proto.params.length - 1 : -1;
      }
      const callee = resolve(c.name, c.file);
      if (callee) return forwarders.has(bodyKey(callee)) ? forwarders.get(bodyKey(callee)) : -1;
      return pointers.has(c.name) ? pointers.get(c.name) : -1;
    };
    // The forwarders: native functions passing a slot parameter of theirs on as a slot, to a fixed point.
    for (let grew = true; grew; ) {
      grew = false;
      for (const [key, fn] of bodies) {
        if (forwarders.has(key)) continue;
        const own = calls.filter((c) => c.file === fn.file && c.function === fn.name && !c.directive);
        const at = fn.params.findIndex(
          (p) => isSlot(p.type) && own.some((c) => slotIndex(c) >= 0 && bareArg(c.args[slotIndex(c)] || '') === p.name),
        );
        if (at >= 0) {
          forwarders.set(key, at);
          grew = true;
        }
      }
    }
    for (const c of calls) {
      if (c.directive) continue;
      const index = slotIndex(c);
      if (index < 0) continue;
      checked += 1;
      // The call site (`file:function:callee#nth`), not the line, so a key survives edits elsewhere.
      const subject = siteKey(c);
      const slot = c.args[index] || '';
      if (slot === 'NULL' || slot === '0' || slot === 'nullptr') {
        findings.push({ kind: 'out-err-null', subject, message: `gets NULL as its out_err (line ${c.line})` });
        continue;
      }
      const macro = calls.find(
        (m) =>
          MACROS.has(m.name) &&
          m.file === c.file &&
          m.start < c.start &&
          m.end > c.end &&
          // The call is one of its arguments, possibly behind a cast.
          m.args.some((a) => a.replace(/^(\(\s*[\w\s*]+\)\s*)+/, '').startsWith(`${c.name}(`)),
      );
      if (macro) {
        // The macros read `ffi_err` alone (native/util.h): another slot is never read.
        if (slot.replace(/\s+/g, '') !== '&ffi_err') {
          findings.push({
            kind: 'out-err-unread',
            subject,
            allow: subject,
            message: `its out_err ${slot} (line ${c.line}) is wrapped in ${macro.name}, which reads &ffi_err alone`,
          });
        }
        continue;
      }
      const fn = c.function ? bodies.get(`${c.file}:${c.function}`) : null;
      // Forwarded: the function's own slot parameter, whose callers are checked as forwarder calls.
      const own = fn && forwarders.has(bodyKey(fn)) && fn.params[forwarders.get(bodyKey(fn))].name === bareArg(slot);
      if (own) continue;
      const after = noComments(c.file).slice(c.end, c.end + 400);
      const nextStatement = after.slice(0, after.indexOf(';', after.indexOf(';') + 1) + 1);
      const name = slot.replace(/^&\s*/, '');
      if (slot.startsWith('&') && new RegExp(`ffi_take\\(\\s*&\\s*${name.replace(/[.*+?^${}()|[\]\\-]/g, '\\$&')}\\s*\\)`).test(nextStatement)) continue;
      findings.push({
        kind: 'out-err-unread',
        subject,
        allow: subject,
        message: `its out_err ${slot} (line ${c.line}) is read by no FFI_ERR / FFI_SET / FFI_DISCARD, nor ffi_take in the next statement`,
      });
    }
    const { kept, unused } = applyAllowlist(findings, this.ALLOWED, 'out-err-unused-allowance');
    return { findings: [...kept, ...unused], checked };
  },
};
