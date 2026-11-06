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

// N2 check-cb-twin-only (javascript/CLAUDE.md boundary rule 4: waiting calls
// use the `_cb` twin): for every header function `f` with an `f_cb` twin,
// src/ calls `f_cb`, never `f` or its `_err` twin, which would block the
// event loop and run callbacks on the caller's thread. This generalises
// test/static/header_conformance.test.js's fixed producer list to every
// client. The one exception is listed with its reason and the exact call it
// allows; an entry that allows nothing is a finding.
//
// It also owns javascript/CLAUDE.md boundary rule 5 (never read the future):
// no function named by `FORBIDDEN` is called in src/, its `_cb` twins
// included (`kafka_common_KafkaFuture_get_cb`), which read the future too.

const { cModel, applyAllowlist } = require('../c_model');

/** The blocking calls allowed: `<file>:<function>:<callee>(<args>)`. */
const ALLOWED = [
  {
    key: 'src/producer.c:producer_force_close:kafka_producer_Producer_close_with_timeout(p->view, 0, &ffi_err)',
    reason:
      "the finalizer's force-close (javascript/CLAUDE.md rule 4's exception, pinned by " +
      'test/static/header_conformance.test.js): it runs only while the handle is finalized, where no JS runs',
  },
];

/** The header functions src/ never calls, whatever their form: each prefix with its reason. */
const FORBIDDEN = [
  {
    prefix: 'kafka_common_KafkaFuture_get',
    reason:
      "never read the future (javascript/CLAUDE.md rule 5): a send's future is destroyed unread, the delivery " +
      "result comes from the record's Callback_t",
  },
];

module.exports = {
  name: 'check-cb-twin-only',
  kinds: ['blocking-call', 'forbidden-call', 'cb-unused-allowance'],
  FORBIDDEN,
  needs: ['src', 'header'],
  hint: 'Call the _cb twin and complete in its callback (javascript/CLAUDE.md boundary rule 4).',
  ALLOWED,
  check(ctx) {
    const { calls, header } = cModel(ctx);
    const blocking = new Map();
    for (const name of header.keys()) {
      if (!name.endsWith('_cb')) continue;
      const base = name.slice(0, -'_cb'.length);
      if (header.has(base)) blocking.set(base, name);
      if (header.has(`${base}_err`)) blocking.set(`${base}_err`, name);
    }
    const raw = [];
    let checked = 0;
    for (const c of calls) {
      if (!header.has(c.name) || c.directive) continue;
      checked += 1;
      const forbidden = this.FORBIDDEN.find((f) => c.name.startsWith(f.prefix));
      if (forbidden) {
        raw.push({ kind: 'forbidden-call', subject: `${c.file}:${c.line}`, message: `${c.function} calls ${c.name}: ${forbidden.reason}` });
        continue;
      }
      const twin = blocking.get(c.name);
      if (!twin) continue;
      raw.push({
        kind: 'blocking-call',
        subject: `${c.file}:${c.line}`,
        message: `${c.function} calls ${c.name}, the blocking form of ${twin}: call the _cb twin`,
        allow: `${c.file}:${c.function}:${c.name}(${c.args.join(', ')})`,
      });
    }
    const { kept, unused } = applyAllowlist(raw, this.ALLOWED, 'cb-unused-allowance');
    return { findings: [...kept, ...unused], checked };
  },
};
