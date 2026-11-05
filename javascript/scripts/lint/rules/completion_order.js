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

// N5 check-completion-order (javascript/CLAUDE.md boundary rule 6: one
// `Callback_t` per record; the user callback, then the future, then one
// `__set_callback_result_err`). Lexical, not path-sensitive (PLAN.md §8
// decision 13):
//   - C: a delivery-completion function (one src/ hands to an interface's
//     constructor, a header `..._new` / `..._new_err`, as an `..._fn_t`
//     method, such as
//     `kafka_producer_Callback_new_err`'s `on_completion`) calls the JS side
//     (`call_ref`) at one site, and `__set_callback_result_err` at one site,
//     after it, under an `if (!…destroying)` guard (no report once destroy
//     has begun);
//   - TypeScript: the function lib/ hands the native send as its delivery
//     completion (a function argument of a `native.<fn>` call that completes a
//     future, `future['_complete'](...)`) runs the user callback (a value of the
//     `Callback` type) before it completes the future, each at one site.
// The completion functions are listed with what they run; an entry that
// matches nothing is a finding.

const { cModel, applyAllowlist } = require('../c_model');
const { nativeUses } = require('../native_uses');
const { scan } = require('../c_source');

/** The C delivery-completion functions, with why they are one. */
const COMPLETIONS = [
  { key: 'send_on_completion', reason: "the record's Callback_t on_completion (kafka_producer_Callback_new_err)" },
];

module.exports = {
  name: 'check-completion-order',
  kinds: ['completion-order', 'completion-count', 'completion-unlisted', 'report-unguarded', 'order-unused-allowance'],
  needs: ['program', 'src', 'header'],
  COMPLETIONS,
  hint:
    'Run the user callback, then complete the future, then report __set_callback_result once ' +
    '(javascript/CLAUDE.md boundary rule 6).',
  check(ctx) {
    const { ts, checker } = ctx.program();
    const { bodies, calls, header } = cModel(ctx);
    const findings = [];
    let checked = 0;
    // The C completion functions: src functions passed for an `_fn_t` parameter.
    const completions = new Map();
    for (const c of calls) {
      const proto = header.get(c.name);
      // An interface's constructor (`..._new` / `..._new_err`) takes its methods as `_fn_t`s.
      if (!proto || !/_new(_err)?$/.test(c.name)) continue;
      proto.params.forEach((p, i) => {
        if (/_fn_t$/.test(p.type) && bodies.has(c.args[i])) completions.set(c.args[i], c);
      });
    }
    const raw = [];
    for (const [name] of completions) {
      raw.push({ kind: 'completion-unlisted', subject: name, message: 'is a delivery completion the rule does not list', allow: name });
    }
    const listed = applyAllowlist(raw, this.COMPLETIONS, 'order-unused-allowance');
    findings.push(...listed.kept, ...listed.unused);
    for (const [name] of completions) {
      checked += 1;
      const body = bodies.get(name);
      const own = calls.filter((c) => c.file === body.file && c.function === name);
      const js = own.filter((c) => c.name === 'call_ref');
      const report = own.filter((c) => c.name.endsWith('__set_callback_result_err'));
      if (js.length !== 1 || report.length !== 1) {
        findings.push({
          kind: 'completion-count',
          subject: name,
          message: `calls call_ref ${js.length} time(s) and __set_callback_result_err ${report.length} time(s), not once each`,
        });
      } else if (report[0].start < js[0].start) {
        findings.push({
          kind: 'completion-order',
          subject: name,
          message: `reports __set_callback_result_err (line ${report[0].line}) before running the callback (line ${js[0].line})`,
        });
      }
      // Rule 6: no report once destroy has begun, so the report sits under `if (!...destroying)`.
      if (report.length === 1) {
        const code = scan(ctx.read(body.file)).code;
        const before = code.slice(body.bodyStart, report[0].start);
        if (!/if\s*\(\s*!\s*(\w+\s*(->|\.)\s*)*destroying\s*\)\s*\{?[^{}]*$/.test(before)) {
          findings.push({
            kind: 'report-unguarded',
            subject: name,
            message: `reports __set_callback_result_err (line ${report[0].line}) outside an if (!…destroying) guard`,
          });
        }
      }
    }
    // The TypeScript side: each function given to the native layer that completes a future.
    for (const u of nativeUses(ctx)) {
      const call = u.node.parent;
      if (!ts.isCallExpression(call)) continue;
      for (const arg of call.arguments) {
        const fn = ts.isIdentifier(arg) ? (checker.getSymbolAtLocation(arg) || {}).valueDeclaration : arg;
        const literal = fn && ts.isVariableDeclaration(fn) ? fn.initializer : fn;
        if (!literal || !(ts.isArrowFunction(literal) || ts.isFunctionExpression(literal))) continue;
        const sf = literal.getSourceFile();
        const completes = [];
        const userCalls = [];
        const visit = (n) => {
          if (ts.isCallExpression(n)) {
            const e = n.expression;
            if (ts.isElementAccessExpression(e) && ts.isStringLiteralLike(e.argumentExpression) && e.argumentExpression.text === '_complete') {
              completes.push(n);
            } else if (/\bCallback\b/.test(checker.typeToString(checker.getTypeAtLocation(e)) + ' ' + (checker.getTypeAtLocation(e).aliasSymbol || { name: '' }).name)) {
              userCalls.push(n);
            }
          }
          ts.forEachChild(n, visit);
        };
        visit(literal.body);
        if (completes.length === 0) continue;
        checked += 1;
        const at = (n) => `${ctx.rel(sf.fileName)}:${sf.getLineAndCharacterOfPosition(n.getStart(sf)).line + 1}`;
        if (completes.length !== 1 || userCalls.length !== 1) {
          findings.push({
            kind: 'completion-count',
            subject: at(literal),
            message: `runs the user callback ${userCalls.length} time(s) and completes the future ${completes.length} time(s), not once each`,
          });
        } else if (completes[0].getStart() < userCalls[0].getStart()) {
          findings.push({ kind: 'completion-order', subject: at(literal), message: 'completes the future before running the user callback' });
        }
      }
    }
    return { findings, checked };
  },
};
