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
//   - C: a delivery-completion function (one native/ hands to an interface's
//     constructor, a header `..._new` / `..._new_err`, as an `..._fn_t`
//     method, such as
//     `kafka_producer_Callback_new_err`'s `on_completion`) calls the JS side
//     (`call_ref`) at one site, and `__set_callback_result_err` at one site,
//     after it, as the statement an `if (!…destroying)` guards or inside its
//     braced block (no report once destroy has begun);
//   - TypeScript: the function src/ hands the native send as its delivery
//     completion (an argument for a native parameter whose function type
//     takes a `RecordMetadata`, and any function argument of a `native.<fn>`
//     call that completes a future, `future.internalComplete(...)`) runs the user
//     callback (a value of the `Callback` type) before it completes the
//     future, each once, the functions it calls in src/ followed; a delivery
//     callback the rule cannot resolve to a function is a finding. A
//     completion site is counted once however many calls reach it: a
//     callback that returns a Promise completes the future at the same site
//     once the Promise settled, a function both paths call
//     (`completeRecord`).
// The completion functions are listed with what they run; an entry that
// matches nothing is a finding.

const { cModel, bodyKey, applyAllowlist } = require('../c_model');
const { nativeUses } = require('../native_uses');
const { scan, matching } = require('../c_source');

/**
 * Whether offset `at` of `code` (comments and literals blanked) lies in what
 * an `if (!…destroying)` between `from` and `to` guards: its braced block, or
 * the single statement after it (to that statement's `;`).
 */
function guarded(code, from, to, at) {
  const re = /\bif\s*\(\s*!\s*(\w+\s*(->|\.)\s*)*destroying\s*\)/g;
  re.lastIndex = from;
  let m;
  while ((m = re.exec(code)) !== null && m.index < to) {
    let k = m.index + m[0].length;
    while (/\s/.test(code[k])) k++;
    if (code[k] === '{') {
      if (at > k && at < matching(code, k)) return true;
      continue;
    }
    // One statement: to its `;` outside brackets.
    let depth = 0;
    let end = k;
    for (; end < to; end++) {
      if ('([{'.includes(code[end])) depth++;
      else if (')]}'.includes(code[end])) depth--;
      else if (code[end] === ';' && depth === 0) break;
    }
    if (at >= k && at < end) return true;
  }
  return false;
}

/** The C delivery-completion functions, with why they are one. */
const COMPLETIONS = [
  { key: 'send_on_completion', reason: "the record's Callback_t on_completion (kafka_producer_Callback_new_err)" },
];

module.exports = {
  name: 'check-completion-order',
  kinds: ['completion-order', 'completion-count', 'completion-unlisted', 'completion-unresolved', 'report-unguarded', 'order-unused-allowance'],
  needs: ['program', 'native', 'header'],
  COMPLETIONS,
  hint:
    'Run the user callback, then complete the future, then report __set_callback_result once ' +
    '(javascript/CLAUDE.md boundary rule 6).',
  check(ctx) {
    const { ts, checker } = ctx.program();
    const { calls, header, resolve } = cModel(ctx);
    const findings = [];
    let checked = 0;
    // The C completion functions: native functions passed for an `_fn_t` parameter.
    const completions = new Map();
    for (const c of calls) {
      const proto = header.get(c.name);
      // An interface's constructor (`..._new` / `..._new_err`) takes its methods as `_fn_t`s.
      if (!proto || !/_new(_err)?$/.test(c.name)) continue;
      proto.params.forEach((p, i) => {
        const body = /_fn_t$/.test(p.type) ? resolve(c.args[i], c.file) : undefined;
        if (body) completions.set(bodyKey(body), body);
      });
    }
    const raw = [];
    for (const { name } of completions.values()) {
      raw.push({ kind: 'completion-unlisted', subject: name, message: 'is a delivery completion the rule does not list', allow: name });
    }
    const listed = applyAllowlist(raw, this.COMPLETIONS, 'order-unused-allowance');
    findings.push(...listed.kept, ...listed.unused);
    for (const body of completions.values()) {
      const { name } = body;
      checked += 1;
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
      // Rule 6: no report once destroy has begun, so the report is the
      // statement an `if (!...destroying)` guards, or inside its braced block.
      if (report.length === 1) {
        const code = scan(ctx.read(body.file)).code;
        if (!guarded(code, body.bodyStart, body.bodyEnd, report[0].start)) {
          findings.push({
            kind: 'report-unguarded',
            subject: name,
            message: `reports __set_callback_result_err (line ${report[0].line}) outside an if (!…destroying) guard`,
          });
        }
      }
    }
    // The TypeScript side: each function given to the native layer that
    // completes a future, and every delivery callback (an argument for a
    // native parameter whose function type takes a `RecordMetadata`), which
    // must be a function the rule can follow: a function literal, or a
    // `const` / `readonly` / module function of src/ naming one. Calls to such
    // functions inside it are followed, in call order, so a body moved into a
    // helper is still read.
    const isConst = (d) =>
      (ts.isVariableDeclaration(d) && (ts.getCombinedNodeFlags(d) & ts.NodeFlags.Const) !== 0) ||
      (ts.isPropertyDeclaration(d) && (ts.getCombinedModifierFlags(d) & ts.ModifierFlags.Readonly) !== 0);
    const fnOf = (expr) => {
      if (ts.isParenthesizedExpression(expr)) return fnOf(expr.expression);
      if (ts.isArrowFunction(expr) || ts.isFunctionExpression(expr)) return expr;
      if (!ts.isIdentifier(expr) && !ts.isPropertyAccessExpression(expr)) return null;
      let symbol = checker.getSymbolAtLocation(ts.isPropertyAccessExpression(expr) ? expr.name : expr);
      if (symbol && symbol.flags & ts.SymbolFlags.Alias) symbol = checker.getAliasedSymbol(symbol);
      const d = symbol && symbol.valueDeclaration;
      if (!d || d.getSourceFile().isDeclarationFile) return null;
      if (ts.isFunctionDeclaration(d) || ts.isMethodDeclaration(d)) return d.body ? d : null;
      return isConst(d) && d.initializer ? fnOf(d.initializer) : null;
    };
    const named = (t) => [t.symbol, t.aliasSymbol].some((sym) => sym && sym.name === 'RecordMetadata');
    const takesMetadata = (type) =>
      type !== undefined &&
      (type.isUnion() ? type.types : [type]).some((t) =>
        t.getCallSignatures().some((sig) => sig.parameters.some((p) => named(checker.getNonNullableType(checker.getTypeOfSymbol(p))))),
      );
    // A value of the `Callback` type itself (not a function taking one).
    const isCallback = (t) => (t.aliasSymbol && t.aliasSymbol.name === 'Callback') || checker.typeToString(t) === 'Callback';
    /**
     * The user-callback runs and future completions of function `fn`, in call
     * order, each completion site once (`sites`, shared by the functions followed).
     */
    const events = (fn, stack = [fn], sites = new Set()) => {
      const out = [];
      const visit = (n) => {
        ts.forEachChild(n, visit);
        if (!ts.isCallExpression(n)) return;
        const e = n.expression;
        if (ts.isPropertyAccessExpression(e) && e.name.text === 'internalComplete') {
          if (sites.has(n)) return;
          sites.add(n);
          out.push({ what: 'complete', node: n });
        } else if (isCallback(checker.getNonNullableType(checker.getTypeAtLocation(e)))) {
          out.push({ what: 'user', node: n });
        } else {
          const callee = fnOf(e);
          if (callee && !stack.includes(callee)) out.push(...events(callee, [...stack, callee], sites));
        }
      };
      visit(fn.body);
      return out;
    };
    for (const u of nativeUses(ctx)) {
      const call = u.node.parent;
      if (!ts.isCallExpression(call) || call.expression !== u.node) continue;
      for (const arg of call.arguments) {
        if (ts.isSpreadElement(arg)) continue;
        const delivery = takesMetadata(checker.getContextualType(arg));
        const fn = fnOf(arg);
        const where = (n) => {
          const sf = n.getSourceFile();
          return `${ctx.rel(sf.fileName)}:${sf.getLineAndCharacterOfPosition(n.getStart(sf)).line + 1}`;
        };
        if (!fn) {
          if (delivery) {
            checked += 1;
            findings.push({
              kind: 'completion-unresolved',
              subject: where(arg),
              message: `hands native.${u.name} a delivery callback the rule cannot follow: a function literal, or a const, readonly or module function of src/`,
            });
          }
          continue;
        }
        const ev = events(fn);
        const completes = ev.filter((x) => x.what === 'complete');
        const userCalls = ev.filter((x) => x.what === 'user');
        if (completes.length === 0 && !delivery) continue;
        checked += 1;
        const subject = where(ts.isFunctionDeclaration(fn) || ts.isMethodDeclaration(fn) ? fn.name || fn : fn);
        if (completes.length !== 1 || userCalls.length !== 1) {
          findings.push({
            kind: 'completion-count',
            subject,
            message: `runs the user callback ${userCalls.length} time(s) and completes the future ${completes.length} time(s), not once each`,
          });
        } else if (ev.indexOf(completes[0]) < ev.indexOf(userCalls[0])) {
          findings.push({ kind: 'completion-order', subject, message: 'completes the future before running the user callback' });
        }
      }
    }
    return { findings, checked };
  },
};
