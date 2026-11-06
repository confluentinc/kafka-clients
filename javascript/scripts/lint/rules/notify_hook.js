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

// N3 check-notify-hook (javascript/CLAUDE.md boundary rule 2: the notify hook
// only signals): every function src/ installs as a client's notify hook (the
// second argument of a `__set_callbacks_notify_err` call) is, statement for
// statement,
//
//     CallbackQueue* q = (CallbackQueue*)opaque;      (optional)
//     pthread_mutex_lock(&q->notify_mtx);
//     if (q->notify_open) napi_call_threadsafe_function(q->tsfn, NULL, napi_tsfn_nonblocking);
//     pthread_mutex_unlock(&q->notify_mtx);
//
// the call braced or not, the flag tested as it is (a negated or compared
// guard is a finding), and nothing else: no other statement (a write to the
// queue races the JS thread) and no `return` (one between the lock and the
// unlock leaves `notify_mtx` held). A hook whose statements are only
// `(void)opaque;` (the finalizer's no-op) passes. The calls, the guard and
// the lock are reported on their own first; `notify-body` reports any other
// departure from that shape.

const { cModel, bodyKey, lineIn } = require('../c_model');
const { scan } = require('../c_source');

const ALLOWED_CALLS = new Set(['pthread_mutex_lock', 'pthread_mutex_unlock', 'napi_call_threadsafe_function']);

/** `s` escaped for a RegExp. */
const esc = (s) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

/**
 * The hook's body `code` (comments and literals blanked) against the one
 * shape a signalling hook has: `null` when it matches, else the offset in
 * `code` of the first statement that departs from it.
 */
function departure(code, opaque) {
  const decl = new RegExp(`^\\s*(\\w+)\\s*\\*\\s*(\\w+)\\s*=\\s*\\(\\s*\\1\\s*\\*\\s*\\)\\s*${esc(opaque)}\\s*;`).exec(code);
  const q = decl ? decl[2] : opaque;
  const from = decl ? decl[0].length : 0;
  const Q = `${esc(q)}\\s*->\\s*`;
  const call = `napi_call_threadsafe_function\\s*\\(\\s*${Q}tsfn\\s*,\\s*NULL\\s*,\\s*napi_tsfn_nonblocking\\s*\\)\\s*;`;
  const shape = new RegExp(
    `^(\\s*)pthread_mutex_lock\\s*\\(\\s*&\\s*${Q}notify_mtx\\s*\\)\\s*;` +
      `(\\s*)if\\s*\\(\\s*${Q}notify_open\\s*\\)\\s*(?:\\{\\s*${call}\\s*\\}|${call})` +
      `(\\s*)pthread_mutex_unlock\\s*\\(\\s*&\\s*${Q}notify_mtx\\s*\\)\\s*;(\\s*)$`,
  );
  if (shape.test(code.slice(from))) return null;
  // Where it departs: the pieces found in order, the first stray text between them.
  const pieces = [
    new RegExp(`pthread_mutex_lock\\s*\\(\\s*&\\s*${Q}notify_mtx\\s*\\)\\s*;`, 'g'),
    new RegExp(`if\\s*\\(\\s*${Q}notify_open\\s*\\)\\s*(?:\\{\\s*${call}\\s*\\}|${call})`, 'g'),
    new RegExp(`pthread_mutex_unlock\\s*\\(\\s*&\\s*${Q}notify_mtx\\s*\\)\\s*;`, 'g'),
  ];
  // The first non-blank offset from `k` (or `k` itself at the end).
  const firstFrom = (k) => {
    const m = /\S/.exec(code.slice(k));
    return m ? k + m.index : k;
  };
  let at = from;
  for (const re of pieces) {
    re.lastIndex = at;
    const m = re.exec(code);
    if (m === null || /\S/.test(code.slice(at, m.index))) {
      // Inside the guard's block: the first statement that is not the call.
      const open = new RegExp(`\\s*if\\s*\\(\\s*${Q}notify_open\\s*\\)\\s*\\{\\s*`, 'y');
      open.lastIndex = at;
      if (re === pieces[1] && open.exec(code) !== null) {
        const inner = new RegExp(`${call}\\s*`, 'y');
        inner.lastIndex = open.lastIndex;
        return firstFrom(inner.exec(code) !== null ? inner.lastIndex : open.lastIndex);
      }
      return firstFrom(at);
    }
    at = m.index + m[0].length;
  }
  return firstFrom(at);
}

/** The first statement of `code` from offset `at`, collapsed: up to its `;` or its block. */
function statementAt(code, source, at) {
  const rest = code.slice(at);
  const end = /[;{]/.exec(rest);
  return source.slice(at, at + (end ? end.index + 1 : rest.length)).replace(/\s+/g, ' ').trim();
}

module.exports = {
  name: 'check-notify-hook',
  kinds: ['notify-call', 'notify-blocking', 'notify-unguarded', 'notify-unlocked', 'notify-body', 'notify-hook-missing'],
  needs: ['src', 'header'],
  hint:
    'Keep the notify hook to its signal: lock notify_mtx, call the threadsafe function nonblocking while ' +
    'notify_open, unlock (javascript/CLAUDE.md boundary rule 2).',
  check(ctx) {
    const { calls, resolve } = cModel(ctx);
    // Each hook as the definition its installing call reaches (a `static` one is its own file's).
    const hooks = new Map();
    for (const c of calls.filter((x) => x.name.endsWith('__set_callbacks_notify_err'))) {
      const body = resolve(c.args[1], c.file);
      hooks.set(body ? bodyKey(body) : c.args[1], { hook: c.args[1], body });
    }
    const findings = [];
    let checked = 0;
    for (const [, { hook, body }] of [...hooks].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))) {
      checked += 1;
      if (!body) {
        findings.push({ kind: 'notify-hook-missing', subject: hook, message: 'is installed as a notify hook but defined in no src/*.c' });
        continue;
      }
      const own = calls.filter((c) => c.file === body.file && c.function === hook);
      for (const c of own) {
        if (!ALLOWED_CALLS.has(c.name)) {
          findings.push({ kind: 'notify-call', subject: `${c.file}:${c.line}`, message: `${hook} calls ${c.name}: the hook only signals` });
        }
      }
      const code = scan(body.body).code;
      const opaque = (body.params[0] || {}).name || '';
      const tsfn = own.filter((c) => c.name === 'napi_call_threadsafe_function');
      if (tsfn.length === 0) {
        // The no-op hook: nothing but `(void)opaque;`.
        const stray = /\S/.exec(code.replace(new RegExp(`\\(\\s*void\\s*\\)\\s*${esc(opaque)}\\s*;`, 'g'), (m) => ' '.repeat(m.length)));
        if (stray && own.length === 0) {
          findings.push({
            kind: 'notify-body',
            subject: `${body.file}:${lineIn(ctx, body.file, body.bodyStart + stray.index)}`,
            message: `${hook} runs \`${statementAt(code, body.body, stray.index)}\`: a hook that does not signal does nothing`,
          });
        }
        continue;
      }
      const before = findings.length;
      for (const c of tsfn) {
        if (c.args[c.args.length - 1] !== 'napi_tsfn_nonblocking') {
          findings.push({ kind: 'notify-blocking', subject: `${c.file}:${c.line}`, message: `${hook} calls the threadsafe function ${c.args[c.args.length - 1]}, not napi_tsfn_nonblocking` });
        }
      }
      // The guard is the positive test of the flag itself (`if (q->notify_open)`, the call braced or not: what else
      // its block holds is notify-body's), never a negated one.
      if (tsfn.length > 1 || !/if\s*\(\s*(\w+\s*(->|\.)\s*)*notify_open\s*\)\s*(\{[^{}]*?)?napi_call_threadsafe_function\s*\(/.test(code)) {
        findings.push({ kind: 'notify-unguarded', subject: hook, message: 'calls the threadsafe function other than once, under if (…notify_open)' });
      }
      const lock = code.search(/pthread_mutex_lock\s*\([^)]*notify_mtx/);
      const unlock = code.search(/pthread_mutex_unlock\s*\([^)]*notify_mtx/);
      const call = code.search(/napi_call_threadsafe_function\s*\(/);
      if (lock < 0 || unlock < 0 || !(lock < call && call < unlock)) {
        findings.push({ kind: 'notify-unlocked', subject: hook, message: 'calls the threadsafe function outside a notify_mtx lock / unlock' });
      }
      // The rest of the shape: no other statement, anywhere in the body.
      const off = findings.length === before && own.every((c) => ALLOWED_CALLS.has(c.name)) ? departure(code, opaque) : null;
      if (off !== null) {
        findings.push({
          kind: 'notify-body',
          subject: `${body.file}:${lineIn(ctx, body.file, body.bodyStart + off)}`,
          message:
            `${hook} runs \`${statementAt(code, body.body, off)}\`: the hook is the notify_mtx lock, ` +
            'if (q->notify_open) and its one nonblocking call, and the unlock, nothing else',
        });
      }
    }
    return { findings, checked };
  },
};
