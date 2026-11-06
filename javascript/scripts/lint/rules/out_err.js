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
// NULL on entry; nothing is silently dropped): every call of a header
// function whose last parameter is `kafka_common_Error_t **out_err` passes an
// error slot, never `NULL`, and the slot is read: the call is the argument
// of `FFI_ERR` / `FFI_SET` / `FFI_DISCARD` (src/util.h), or a `ffi_take(&slot)`
// follows it in the next statement, or the slot is the calling function's
// own `out_err` parameter, which its caller reads.

const { cModel, siteKey, applyAllowlist } = require('../c_model');

/**
 * The unread slots accepted, by call site, with their reasons (PLAN.md §8
 * decision 14).
 */
const ALLOWED = [
  {
    key: 'src/error.c:error_discard:kafka_common_Error_destroy_err#2',
    reason:
      'the end of the chain: the destroy of the panic error a failed destroy reported, whose own error ' +
      'cannot be destroyed in turn without looping (the user\'s decision of 2026-11-06)',
  },
];
const { scan } = require('../c_source');

const MACROS = new Set(['FFI_ERR', 'FFI_SET', 'FFI_DISCARD']);

module.exports = {
  name: 'check-out-err',
  kinds: ['out-err-null', 'out-err-unread', 'out-err-unused-allowance'],
  ALLOWED,
  needs: ['src', 'header'],
  hint:
    'Pass the call an error slot (`&ffi_err`) and read it: wrap the call in FFI_ERR / FFI_SET / FFI_DISCARD ' +
    '(src/util.h), so no error is dropped (root CLAUDE.md §4).',
  check(ctx) {
    const { bodies, calls, header } = cModel(ctx);
    const findings = [];
    let checked = 0;
    for (const c of calls) {
      const proto = header.get(c.name);
      if (!proto || c.directive) continue;
      const last = proto.params[proto.params.length - 1];
      if (!last || last.name !== 'out_err' || !/kafka_common_Error_t \*\*/.test(last.type)) continue;
      checked += 1;
      // The call site (`file:function:callee#nth`), not the line, so a key survives edits elsewhere.
      const subject = siteKey(c);
      const slot = c.args[c.args.length - 1] || '';
      if (slot === 'NULL' || slot === '0' || slot === 'nullptr') {
        findings.push({ kind: 'out-err-null', subject, message: `gets NULL as its out_err (line ${c.line})` });
        continue;
      }
      const wrapped = calls.some(
        (m) =>
          MACROS.has(m.name) &&
          m.file === c.file &&
          m.start < c.start &&
          m.end > c.end &&
          // The call is one of its arguments, possibly behind a cast.
          m.args.some((a) => a.replace(/^(\(\s*[\w\s*]+\)\s*)+/, '').startsWith(`${c.name}(`)),
      );
      if (wrapped) continue;
      const fn = c.function ? bodies.get(c.function) : null;
      const own = fn && fn.params.some((p) => p.name === slot && /kafka_common_Error_t\s*\*\*/.test(p.type));
      if (own) continue;
      const after = ctx.once(`noComments:${c.file}`, () => scan(ctx.read(c.file)).noComments).slice(c.end, c.end + 400);
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
