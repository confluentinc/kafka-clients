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

// check-mock-native (root CLAUDE.md, JavaScript Binding Conventions,
// Implementation over the FFI, Q62: "The mocks wrap the FFI's mocks, as .NET
// does, and aren't translations of Java's"; "Only KafkaProducer,
// KafkaConsumer, the admin client and the three mocks call the addon"): each
// exported class named `Mock...` holds a native handle: its constructor, or
// a field initializer, reaches a `native.<fn>` use, followed through the
// src/ functions it calls as check-waiting-call-promise follows a method
// (`mock-not-native`). Not checked: that the use creates the FFI's mock, and
// that every method goes through it.

const { exportedItems } = require('../java_ts');
const { nativeUses, reachedBodies } = require('../native_uses');

module.exports = {
  name: 'check-mock-native',
  kinds: ['mock-not-native'],
  needs: ['program'],
  hint:
    "Build the mock over the FFI's mock, holding its native handle, as the real client does (root CLAUDE.md, " +
    'JavaScript Binding Conventions, Implementation over the FFI, Q62).',
  check(ctx) {
    const { ts } = ctx.program();
    const findings = [];
    let checked = 0;
    for (const item of exportedItems(ctx)) {
      if (item.kind !== 'class') continue;
      checked++;
      if (!/^Mock[A-Z]/.test(item.name)) continue;
      const roots = [];
      for (const m of item.node.members) {
        if (ts.isConstructorDeclaration(m) && m.body) roots.push(...reachedBodies(ctx, m));
        else if (ts.isPropertyDeclaration(m) && m.initializer) roots.push(m.initializer);
      }
      if (!roots.some((r) => nativeUses(ctx, r).length > 0)) {
        findings.push({
          kind: 'mock-not-native',
          subject: `${item.rel}:${item.name}`,
          message: "holds no native handle: a mock wraps the FFI's mock, its constructor or a field reaching native.<fn>",
        });
      }
    }
    return { findings, checked };
  },
};
