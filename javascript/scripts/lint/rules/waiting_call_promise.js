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

// A1 check-waiting-call-promise (javascript/CLAUDE.md boundary rule 4 and
// "Java shape, JS idiom": a blocking Java method returns a Promise, and a
// waiting call uses the FFI's `_cb` twin): a public method of an exported
// class whose native path reaches a `_cb` twin of the bindings header returns
// a Promise, and one whose native path reaches none does not. The path is
// followed:
//   1. the method's `native.<fn>` uses (the handle under any name, by its
//      type), in its body or in the body of any src/ function or method it
//      calls, transitively, as the type checker resolves the calls (a module
//      helper, a private method, another class's `x['_m']()`);
//   2. each `<fn>`'s registration in native/addon.c (`{"<fn>", ..., Impl}`);
//   3. the `kafka_*` functions `Impl` calls, directly or through the
//      functions of native/*.c it calls.
// A use that leads nowhere (no registration, no C definition) is a finding of
// its own: an unresolved path never passes. A method with no native use is
// not checked. A member that reaches a future's waits only to complete the
// dependant it returns (`DEPENDANTS`) need not return a Promise.

const { applyAllowlist } = require('../c_model');
const { atNode } = require('../location');
const { functionBodies, callSites, propertyDescriptors, prototypes } = require('../c_source');
const { exportedItems, publicMembers } = require('../java_ts');
const { nativeUses, reachedBodies } = require('../native_uses');

/**
 * The members that reach a future's waits (its `_cb` twins
 * `kafka_common_KafkaFuture_get_cb` / `_get_with_timeout_cb`) only to
 * complete what they return, a dependant, without waiting themselves: Java's
 * returns at once, so theirs does. Each with its reason; an entry that
 * matches nothing is a finding.
 */
const DEPENDANTS = [
  { key: 'KafkaFuture.allOf', reason: "allOf's future, completed once every source's wait completed" },
];

/** The C call graph of native/*.c: function name -> the names it calls. */
function callGraph(ctx) {
  return ctx.once('cCallGraph', () => {
    const graph = new Map();
    for (const rel of ctx.files(['native'], ['.c'])) {
      const text = ctx.read(rel);
      for (const name of functionBodies(text).keys()) graph.set(name, new Set());
      for (const call of callSites(text)) {
        if (call.function !== null) graph.get(call.function).add(call.name);
      }
    }
    return graph;
  });
}

/** The `kafka_*` functions C function `start` reaches, through the functions of native/*.c. */
function reachedKafka(graph, start) {
  const out = new Set();
  const seen = new Set([start]);
  const work = [start];
  while (work.length > 0) {
    for (const callee of graph.get(work.pop()) || []) {
      if (callee.startsWith('kafka_')) out.add(callee);
      else if (graph.has(callee) && !seen.has(callee)) {
        seen.add(callee);
        work.push(callee);
      }
    }
  }
  return out;
}

/**
 * Every public method or getter of an exported class with a native use on
 * its path: `{item, m, subject, uses, twins, unresolved, returned, promise}`.
 */
function analyse(ctx) {
  return ctx.once('waitingAnalysis', () => {
    const { ts, checker } = ctx.program();
    if (!ctx.files(['native'], ['addon.c']).includes('native/addon.c')) throw new Error('no native/addon.c');
    const registrations = new Map(propertyDescriptors(ctx.read('native/addon.c')).map((r) => [r.name, r]));
    const graph = callGraph(ctx);
    const header = prototypes(ctx.header());
    const isTwin = (name) => name.endsWith('_cb') && header.has(name) && header.has(name.slice(0, -'_cb'.length));
    const out = [];
    for (const item of exportedItems(ctx)) {
      if (item.kind !== 'class') continue;
      for (const m of publicMembers(ts, item.node)) {
        if (m.kind !== 'method' && m.kind !== 'get') continue;
        const uses = reachedBodies(ctx, m.node).flatMap((b) => nativeUses(ctx, b));
        if (uses.length === 0) continue;
        const twins = new Set();
        const unresolved = [];
        for (const name of [...new Set(uses.map((u) => u.name))]) {
          const r = registrations.get(name);
          const impl = r && (r.method || r.getter);
          if (!impl) unresolved.push(`native.${name} is registered by no napi_property_descriptor of native/addon.c`);
          else if (!graph.has(impl)) unresolved.push(`native.${name}'s implementation ${impl} is defined in no native/*.c`);
          else for (const k of reachedKafka(graph, impl)) if (isTwin(k)) twins.add(k);
        }
        const signature = checker.getSignatureFromDeclaration(m.node);
        const returned =
          m.kind === 'get' || !signature ? checker.getTypeAtLocation(m.node.name) : checker.getReturnTypeOfSignature(signature);
        const promise = (returned.symbol && returned.symbol.name === 'Promise') || /^Promise</.test(checker.typeToString(returned));
        out.push({ item, m, subject: `${item.name}.${m.name}`, uses, twins, unresolved, returned, promise });
      }
    }
    return out;
  });
}

/**
 * The `Class.member` subjects whose native path reaches a `_cb` twin: the
 * blocking methods (a dependant's excepted), and the members of the
 * interfaces a class implements (`implements Producer`) that one of them is.
 */
function waitingMembers(ctx) {
  const { ts } = ctx.program();
  const dependants = new Set(DEPENDANTS.map((d) => d.key));
  const out = new Set(
    analyse(ctx)
      .filter((w) => w.unresolved.length === 0 && w.twins.size > 0 && !dependants.has(w.subject))
      .map((w) => w.subject),
  );
  for (const item of exportedItems(ctx)) {
    if (item.kind !== 'class') continue;
    for (const clause of item.node.heritageClauses || []) {
      if (clause.token !== ts.SyntaxKind.ImplementsKeyword) continue;
      for (const t of clause.types) {
        const iface = t.expression.getText();
        for (const subject of [...out]) {
          if (subject.startsWith(`${item.name}.`)) out.add(`${iface}.${subject.slice(item.name.length + 1)}`);
        }
      }
    }
  }
  return out;
}

module.exports = {
  name: 'check-waiting-call-promise',
  kinds: ['not-promise', 'promise-not-waiting', 'unresolved-native', 'waiting-unused-allowance'],
  DEPENDANTS,
  needs: ['program', 'native', 'header'],
  hint:
    'A method whose native call waits on a _cb twin returns a Promise settled by that completion; one ' +
    'that waits on none returns its value (javascript/CLAUDE.md boundary rule 4).',
  waitingMembers,
  check(ctx) {
    const { checker } = ctx.program();
    // In order; a not-promise finding of a dependant DEPENDANTS allows is dropped.
    const raw = [];
    let checked = 0;
    for (const w of analyse(ctx)) {
      checked += 1;
      const { subject, uses, twins, unresolved, returned, promise } = w;
      const at = atNode(ctx, w.m.node);
      for (const message of unresolved) raw.push({ at, kind: 'unresolved-native', subject, message });
      if (unresolved.length > 0) continue;
      if (twins.size > 0 && !promise) {
        raw.push({
          at,
          kind: 'not-promise',
          subject,
          message: `reaches ${[...twins].sort().join(', ')} but returns ${checker.typeToString(returned)}`,
          // Only a dependant that reaches nothing but a future's waits.
          allow: [...twins].every((t) => /^kafka_common_KafkaFuture_get(_with_timeout)?_cb$/.test(t)) ? subject : undefined,
        });
      } else if (twins.size === 0 && promise) {
        raw.push({
          at,
          kind: 'promise-not-waiting',
          subject,
          message: `returns ${checker.typeToString(returned)} but its native path (${[...new Set(uses.map((u) => `native.${u.name}`))].join(', ')}) reaches no _cb twin`,
        });
      }
    }
    const { kept, unused } = applyAllowlist(raw, this.DEPENDANTS, 'waiting-unused-allowance');
    return { findings: [...kept, ...unused], checked };
  },
};
