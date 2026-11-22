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

// check-tsfn-lifetime (root CLAUDE.md, JavaScript Binding Conventions,
// Lifetime, Q21: a client keeps Node running from its creation until
// `close()` closes it, "so no pending send or poll is cut off";
// javascript/CLAUDE.md "Threads and liveness", which reads it as: the
// thread-safe function is referenced while the client is open or one of its
// `_cb` calls waits for its answer). In native/:
//   - `napi_ref_threadsafe_function` and `napi_unref_threadsafe_function` are
//     called only by the hold update, the functions of `HOLD` (`tsfn-ref`,
//     `tsfn-unref`), which calls both (`tsfn-ref-missing`,
//     `tsfn-unref-missing`): the hold changes in one place;
//   - `cbq_closed`, where the open half of the hold ends, is called only from
//     a close completion, the functions of `CLOSE_COMPLETIONS`
//     (`tsfn-close-call`), so it ends only once the core answered a close;
//   - every `kafka_*_cb(` call is a whole statement right after a
//     `cbq_call_begin(` call that is a whole statement too, in the same block
//     (so under the same condition), one begin each, and a `cbq_call_begin`
//     anywhere else is a finding (`tsfn-call-begin`): each call is counted
//     once, after every early return, and nothing else is counted;
//   - a `cbq_call_end` call appears only in a completion handed to a `_cb`
//     call (the argument for the header parameter typed `..._cb_t`), and no
//     function that calls it is itself called (`tsfn-call-end`), so an end
//     runs only when the core runs its completion, not from a delivery
//     callback or through a helper;
//   - each such completion calls `cbq_call_end` once, as a statement of its
//     body's own block, with no `return` in the body and nothing after it
//     but the release of the client struct (`RELEASES`), which may free the
//     queue (`tsfn-call-end`): each counted call ends once, on every path,
//     after its JS callback ran.
// Lexical, by function, as the other native/ rules read the C sources. Not
// checked: that a begin and its call name the same client's queue, that the
// core fires each completion exactly once (the FFI's contract), and what the
// hold update decides, which the unit tests check
// (test/unit/producer/kafka_producer.test.js).

const { cModel, bodyKey } = require('../c_model');
const { scan, bareArg } = require('../c_source');

/** The functions that reference and unreference the thread-safe function: the callback queue's hold update. */
const HOLD = ['cbq_hold_update'];

/** Where the open half of the hold ends: the callback queue's record of a close the core reported. */
const CLOSE_PATH = ['cbq_closed'];

/** The close completions, the only callers of the close path: the core's answer to a client's close. */
const CLOSE_COMPLETIONS = ['close_on_complete'];

/** The callback queue's count of the calls waiting for their answer. */
const BEGIN = 'cbq_call_begin';
const END = 'cbq_call_end';

/** The releases of a client struct, which may free the queue the count is on, so they may follow `END`. */
const RELEASES = ['producer_release', 'admin_release'];

/** A `_cb` twin of the bindings header: `kafka_..._cb`. */
const CB_CALL = /^kafka_\w+_cb$/;

/** `s` escaped for a RegExp. */
const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/** The offset of the first non-blank character of `code` at or after `k` (`code.length` when none). */
function nextSolid(code, k) {
  while (k < code.length && /\s/.test(code[k])) k++;
  return k;
}

/** The offset of the last non-blank character of `code` before `k` and at or after `from`, or -1. */
function prevSolid(code, k, from) {
  let i = k - 1;
  while (i >= from && /\s/.test(code[i])) i--;
  return i >= from ? i : -1;
}

/**
 * Whether call `c` is a whole statement of the function body that starts at
 * `bodyStart` (`code` with comments and literals blanked): it opens a
 * statement (the body's start, or after `;`, `{` or `}`, never under an
 * `if`, `else`, `for` or `while` without braces) and its `;` follows it.
 */
function wholeStatement(code, c, bodyStart) {
  const before = prevSolid(code, c.start, bodyStart);
  if (before >= 0 && !';{}'.includes(code[before])) return false;
  return code[nextSolid(code, c.end)] === ';';
}

/**
 * Whether the call at file offset `at` of `code` is a statement of the block
 * of the body that starts at `bodyStart`, rather than of a nested block or of
 * an `if`, `else`, `for` or `while` without braces: no unclosed brace between
 * them, and the call opens a statement.
 */
function topLevelStatement(code, bodyStart, at) {
  let depth = 0;
  for (let k = bodyStart; k < at; k++) {
    if (code[k] === '{') depth++;
    else if (code[k] === '}') depth--;
  }
  if (depth !== 0) return false;
  let k = at - 1;
  while (k >= bodyStart && /\s/.test(code[k])) k--;
  return k < bodyStart || code[k] === ';' || code[k] === '}';
}

module.exports = {
  name: 'check-tsfn-lifetime',
  kinds: ['tsfn-ref', 'tsfn-unref', 'tsfn-ref-missing', 'tsfn-unref-missing', 'tsfn-close-call', 'tsfn-call-begin', 'tsfn-call-end'],
  needs: ['native', 'header'],
  HOLD,
  CLOSE_PATH,
  CLOSE_COMPLETIONS,
  RELEASES,
  hint:
    "Change the thread-safe function's reference only in the hold update, and count every _cb call: cbq_call_begin " +
    'right before it, cbq_call_end as the last statement of its completion, so a client keeps Node running while it ' +
    'is open or a call made on it waits for its answer (root CLAUDE.md, JavaScript Binding Conventions, Lifetime, ' +
    'Q21; javascript/CLAUDE.md "Threads and liveness").',
  check(ctx) {
    const { bodies, calls, header, resolve } = cModel(ctx);
    const findings = [];
    let checked = 0;
    const codeOf = (file) => ctx.once(`tsfn-code:${file}`, () => scan(ctx.read(file)).code);
    const where = (c) => c.function || (c.macro ? `the macro ${c.macro}` : 'a macro');
    // The hold changes in one place.
    const inHold = { napi_ref_threadsafe_function: new Set(), napi_unref_threadsafe_function: new Set() };
    for (const c of calls) {
      if (c.name === 'napi_ref_threadsafe_function' || c.name === 'napi_unref_threadsafe_function') {
        checked++;
        if (c.function !== null && HOLD.includes(c.function)) {
          inHold[c.name].add(c.function);
          continue;
        }
        const ref = c.name === 'napi_ref_threadsafe_function';
        findings.push({
          kind: ref ? 'tsfn-ref' : 'tsfn-unref',
          subject: `${c.file}:${c.line}`,
          message: `${ref ? 'references' : 'unreferences'} the thread-safe function in ${where(c)}, not in the hold update (${HOLD.join(', ')}): the hold is decided in one place, from the client's open state and its calls waiting for their answer`,
        });
      } else if (CLOSE_PATH.includes(c.name) && !CLOSE_COMPLETIONS.includes(c.function)) {
        checked++;
        findings.push({
          kind: 'tsfn-close-call',
          subject: `${c.file}:${c.line}`,
          message: `calls ${c.name} in ${where(c)}, not in a close completion (${CLOSE_COMPLETIONS.join(', ')}): the open client's hold ends only once the core answered the close`,
        });
      }
    }
    for (const fn of HOLD) {
      checked++;
      if (!inHold.napi_ref_threadsafe_function.has(fn)) {
        findings.push({
          kind: 'tsfn-ref-missing',
          subject: fn,
          message: 'calls no napi_ref_threadsafe_function: a call made on a closed client would not hold Node until its answer',
        });
      }
      if (!inHold.napi_unref_threadsafe_function.has(fn)) {
        findings.push({
          kind: 'tsfn-unref-missing',
          subject: fn,
          message: 'calls no napi_unref_threadsafe_function: a closed client would keep Node running',
        });
      }
    }
    // Every _cb call counted by the statement right before it, in the same
    // block, one begin each, and no begin anywhere else.
    const cbCalls = calls.filter((c) => CB_CALL.test(c.name));
    const paired = new Set();
    for (const b of calls.filter((c) => c.name === BEGIN)) {
      checked++;
      const body = b.function === null ? undefined : bodies.get(`${b.file}:${b.function}`);
      let counted;
      if (body !== undefined) {
        const code = codeOf(b.file);
        if (wholeStatement(code, b, body.bodyStart)) {
          const next = nextSolid(code, nextSolid(code, b.end) + 1);
          counted = cbCalls.find((c) => c.file === b.file && c.start === next && wholeStatement(code, c, body.bodyStart));
        }
      }
      if (counted !== undefined) {
        paired.add(counted);
        continue;
      }
      findings.push({
        kind: 'tsfn-call-begin',
        subject: `${b.file}:${b.line}`,
        message: `${where(b)} calls ${BEGIN} other than as the whole statement right before its own kafka_*_cb( call, in the same block: it may count a call not made, or miss one made, so the count holds Node forever or lets it go too soon`,
      });
    }
    // The completion of each _cb call: the argument for its header parameter typed `..._cb_t`.
    const completions = new Map();
    for (const c of cbCalls) {
      checked++;
      const subject = `${c.file}:${c.line}`;
      if (!paired.has(c)) {
        findings.push({
          kind: 'tsfn-call-begin',
          subject,
          message:
            c.function === null
              ? `${where(c)} calls ${c.name} outside a function: the call cannot be counted`
              : `${c.function} calls ${c.name} with no ${BEGIN} as the statement right before it: the call is not counted, so its answer may come once nothing holds Node`,
        });
      }
      const proto = header.get(c.name);
      const i = proto ? proto.params.findIndex((p) => /_cb_t$/.test(p.type)) : -1;
      if (i < 0 || c.args[i] === undefined) continue;
      const arg = bareArg(c.args[i]);
      const body = resolve(arg, c.file);
      if (!body) {
        findings.push({
          kind: 'tsfn-call-end',
          subject,
          message: `${where(c)} hands ${c.name} the completion ${arg}, which no native/ function defines: its ${END} cannot be checked`,
        });
        continue;
      }
      completions.set(bodyKey(body), body);
    }
    // A count ends only in a completion, and no function that ends one is
    // called directly: its end runs when the core runs the completion, once.
    const enders = new Set();
    for (const e of calls.filter((c) => c.name === END)) {
      checked++;
      const key = e.function === null ? null : `${e.file}:${e.function}`;
      if (key !== null) enders.add(key);
      if (key !== null && completions.has(key)) continue;
      findings.push({
        kind: 'tsfn-call-end',
        subject: `${e.file}:${e.line}`,
        message: `${where(e)} calls ${END}, but no kafka_*_cb( call is handed it as its completion: only the completion of a counted call ends the count, once`,
      });
    }
    for (const c of calls) {
      const target = resolve(c.name, c.file);
      if (target === undefined || !enders.has(bodyKey(target))) continue;
      checked++;
      findings.push({
        kind: 'tsfn-call-end',
        subject: `${c.file}:${c.line}`,
        message: `${where(c)} calls ${c.name}, which calls ${END}: a count ends only in its completion's own body, which the core runs once`,
      });
    }
    for (const body of [...completions.values()].sort((a, b) => (bodyKey(a) < bodyKey(b) ? -1 : 1))) {
      checked++;
      const code = codeOf(body.file);
      const own = calls.filter((c) => c.file === body.file && c.function === body.name);
      const ends = own.filter((c) => c.name === END);
      const subject = `${body.file}:${body.line}`;
      if (ends.length !== 1) {
        findings.push({
          kind: 'tsfn-call-end',
          subject,
          message: `the completion ${body.name} calls ${END} ${ends.length} time(s), not once: each counted call ends exactly once`,
        });
        continue;
      }
      const [end] = ends;
      if (/\breturn\b/.test(code.slice(body.bodyStart, body.bodyEnd))) {
        findings.push({
          kind: 'tsfn-call-end',
          subject,
          message: `the completion ${body.name} has a return: a path that returns skips its ${END} (line ${end.line})`,
        });
        continue;
      }
      const releases = RELEASES.map(esc).join('|');
      const tail = new RegExp(`^\\s*;(\\s*(${releases})\\s*\\([^;{}]*\\)\\s*;)*\\s*$`);
      if (!topLevelStatement(code, body.bodyStart, end.start) || !tail.test(code.slice(end.end, body.bodyEnd))) {
        findings.push({
          kind: 'tsfn-call-end',
          subject,
          message: `the completion ${body.name} calls ${END} (line ${end.line}) before its end or under a condition: it is the last statement of the body, after the JS callback, and only ${RELEASES.join(', ')} may follow it`,
        });
      }
    }
    return { findings, checked };
  },
};
