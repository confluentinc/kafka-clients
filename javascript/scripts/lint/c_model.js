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

// The addon's C sources as the N rules read them, built once per run: every
// function definition of src/*.c and src/*.h with its file, every call site
// with its file, and the bindings header's prototypes. Allowlists, which the
// heuristic rules use (PLAN.md §8 decision 13), are judged here too: an entry
// that waives nothing is a finding.

const { functionBodies, callSites, prototypes, signatureParams } = require('./c_source');

/**
 * `{bodies, calls, header}`: bodies by name (`{file, params, ...}`), every
 * call (`{file, nth, ...}`), the prototypes. A call site's key is
 * `file:function:callee#nth` (`siteKey`).
 */
function cModel(ctx) {
  return ctx.once('cModel', () => {
    const bodies = new Map();
    const calls = [];
    for (const file of ctx.files(['src'], ['.c', '.h'])) {
      const text = ctx.read(file);
      for (const [name, b] of functionBodies(text)) bodies.set(name, { ...b, file, params: signatureParams(b.signature) });
      // `nth`: the call's rank among the calls of its callee in its function,
      // from 1, which names a call site stably across edits elsewhere.
      const seen = new Map();
      for (const c of callSites(text)) {
        const k = `${c.function}:${c.name}`;
        seen.set(k, (seen.get(k) || 0) + 1);
        calls.push({ ...c, file, nth: seen.get(k) });
      }
    }
    return { bodies, calls, header: prototypes(ctx.header()) };
  });
}

/** The 1-based line of a body's offset `at` in its file. */
function lineIn(ctx, file, at) {
  const text = ctx.read(file);
  let line = 1;
  for (let i = 0; i < at; i++) if (text[i] === '\n') line++;
  return line;
}

/**
 * Applies allowlist `entries` (`{key, reason}`) to `findings`, each finding
 * carrying the `allow` key it would be waived by: `{kept, unused}`, `unused`
 * one finding of kind `unusedKind` per entry that waived nothing.
 */
function applyAllowlist(findings, entries, unusedKind) {
  const used = new Set();
  const kept = [];
  for (const f of findings) {
    const entry = entries.find((e) => e.key === f.allow);
    if (entry) used.add(entry.key);
    else kept.push(f);
  }
  const unused = entries
    .filter((e) => !used.has(e.key))
    .map((e) => ({
      kind: unusedKind,
      subject: e.key,
      message: `is allowed as ${e.reason}, but nothing matches it any more: delete the entry`,
    }));
  return { kept: kept.map(({ allow, ...f }) => f), unused };
}

/** The stable key of call site `c`: `file:function:callee#nth`. */
function siteKey(c) {
  return `${c.file}:${c.function || '(top level)'}:${c.name}#${c.nth}`;
}

module.exports = { cModel, lineIn, applyAllowlist, siteKey };
