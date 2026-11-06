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
// second argument of a `__set_callbacks_notify_err` call) calls nothing but
// `pthread_mutex_lock` / `pthread_mutex_unlock` of the queue's `notify_mtx`
// and one `napi_call_threadsafe_function(..., napi_tsfn_nonblocking)`, made
// under that lock and only while `notify_open`: under `if (q->notify_open)`,
// the flag tested as it is (a negated or compared guard is a finding). A hook that calls nothing
// (the finalizer's no-op) passes.

const { cModel, bodyKey } = require('../c_model');
const { scan } = require('../c_source');

const ALLOWED_CALLS = new Set(['pthread_mutex_lock', 'pthread_mutex_unlock', 'napi_call_threadsafe_function']);

module.exports = {
  name: 'check-notify-hook',
  kinds: ['notify-call', 'notify-blocking', 'notify-unguarded', 'notify-unlocked', 'notify-hook-missing'],
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
      const tsfn = own.filter((c) => c.name === 'napi_call_threadsafe_function');
      if (tsfn.length === 0) continue;
      const code = scan(body.body).code;
      for (const c of tsfn) {
        if (c.args[c.args.length - 1] !== 'napi_tsfn_nonblocking') {
          findings.push({ kind: 'notify-blocking', subject: `${c.file}:${c.line}`, message: `${hook} calls the threadsafe function ${c.args[c.args.length - 1]}, not napi_tsfn_nonblocking` });
        }
      }
      // The guard is the positive test of the flag itself (`if (q->notify_open)`), never a negated one.
      if (tsfn.length > 1 || !/if\s*\(\s*(\w+\s*(->|\.)\s*)*notify_open\s*\)\s*napi_call_threadsafe_function\s*\(/.test(code)) {
        findings.push({ kind: 'notify-unguarded', subject: hook, message: 'calls the threadsafe function other than once, under if (…notify_open)' });
      }
      const lock = code.search(/pthread_mutex_lock\s*\([^)]*notify_mtx/);
      const unlock = code.search(/pthread_mutex_unlock\s*\([^)]*notify_mtx/);
      const call = code.search(/napi_call_threadsafe_function\s*\(/);
      if (lock < 0 || unlock < 0 || !(lock < call && call < unlock)) {
        findings.push({ kind: 'notify-unlocked', subject: hook, message: 'calls the threadsafe function outside a notify_mtx lock / unlock' });
      }
    }
    return { findings, checked };
  },
};
