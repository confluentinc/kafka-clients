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
// Lifetime, Q21: "The binding leaves the client's thread-safe function
// referenced, as Node-API creates it, and calls
// `napi_unref_threadsafe_function` once `close()` has closed the client";
// javascript/CLAUDE.md "Threads and liveness"): in native/*.c,
//   - no call of `napi_ref_threadsafe_function` (`tsfn-ref`): the function is
//     referenced from its creation, and nothing references it again;
//   - `napi_unref_threadsafe_function` is called only on the close path, the
//     functions of `CLOSE_PATH` (`tsfn-unref`), and is called there
//     (`tsfn-unref-missing`), so a client keeps Node running exactly until
//     the core reports it closed;
//   - `cbq_closed` itself is called only from a close completion, the
//     functions of `CLOSE_COMPLETIONS` (`tsfn-close-call`), so the function
//     is unreferenced only once the core answered a close.
// Not checked: that the completion calls it only for a close the core
// reported (javascript/CLAUDE.md boundary rule 10).

const { callSites } = require('../c_source');

/** The functions that unreference the thread-safe function: the callback queue's record of a close the core reported. */
const CLOSE_PATH = ['cbq_closed'];

/** The close completions, the only callers of the close path: the core's answer to a client's close. */
const CLOSE_COMPLETIONS = ['close_on_complete'];

module.exports = {
  name: 'check-tsfn-lifetime',
  kinds: ['tsfn-ref', 'tsfn-unref', 'tsfn-unref-missing', 'tsfn-close-call'],
  needs: ['native'],
  CLOSE_PATH,
  CLOSE_COMPLETIONS,
  hint:
    "Leave the client's thread-safe function referenced from its creation and unreference it only once the core " +
    'reported the client closed (root CLAUDE.md, JavaScript Binding Conventions, Lifetime, Q21).',
  check(ctx) {
    const findings = [];
    let checked = 0;
    const unrefIn = new Set();
    for (const rel of ctx.files(['native'], ['.c'])) {
      const text = ctx.read(rel);
      for (const call of callSites(text)) {
        if (call.name === 'napi_ref_threadsafe_function') {
          checked++;
          findings.push({
            kind: 'tsfn-ref',
            subject: `${rel}:${call.line}`,
            message: 'references the thread-safe function again: Node-API creates it referenced, and nothing references it later',
          });
        } else if (CLOSE_PATH.includes(call.name) && !CLOSE_COMPLETIONS.includes(call.function)) {
          checked++;
          findings.push({
            kind: 'tsfn-close-call',
            subject: `${rel}:${call.line}`,
            message: `calls ${call.name} in ${call.function || 'a macro'}, not in a close completion (${CLOSE_COMPLETIONS.join(', ')}): the thread-safe function is unreferenced only once the core answered the close`,
          });
        } else if (call.name === 'napi_unref_threadsafe_function') {
          checked++;
          if (call.function !== null && CLOSE_PATH.includes(call.function)) {
            unrefIn.add(call.function);
          } else {
            findings.push({
              kind: 'tsfn-unref',
              subject: `${rel}:${call.line}`,
              message: `unreferences the thread-safe function in ${call.function || 'a macro'}, not on the close path (${CLOSE_PATH.join(', ')}): a client keeps Node running until the core reports it closed`,
            });
          }
        }
      }
    }
    for (const fn of CLOSE_PATH) {
      if (!unrefIn.has(fn)) {
        findings.push({
          kind: 'tsfn-unref-missing',
          subject: fn,
          message: 'calls no napi_unref_threadsafe_function: a closed client would keep Node running',
        });
      }
    }
    return { findings, checked: checked + CLOSE_PATH.length };
  },
};
