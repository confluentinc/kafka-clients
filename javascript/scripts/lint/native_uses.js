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

// The native addon as src/ reaches it, for check-native-registry,
// check-native-wrappers and check-waiting-call-promise: the handle
// src/internal/native.ts exports (`export = native`), and every use of a
// function of it, found through the type checker, so an import under another
// name counts: `<handle>.<fn>`, `<handle>['<fn>']` and `const { <fn> } =
// <handle>`. Not found: an element access by a computed name
// (`<handle>[name]`), and a destructuring of the handle in a parameter or an
// assignment.

/** The symbol of the handle src/internal/native.ts exports, or null. */
function nativeSymbol(ctx) {
  return ctx.once('nativeSymbol', () => {
    const { ts, checker, sourceFiles } = ctx.program();
    const sf = sourceFiles.find((s) => ctx.rel(s.fileName) === 'src/internal/native.ts');
    if (!sf) return null;
    for (const st of sf.statements) {
      if (ts.isExportAssignment(st) && st.isExportEquals && ts.isIdentifier(st.expression)) {
        return checker.getSymbolAtLocation(st.expression) || null;
      }
    }
    return null;
  });
}

/** The handle's type, or null: a value of it is the handle whatever its name (`const n = native`). */
function nativeType(ctx) {
  return ctx.once('nativeType', () => {
    const { checker } = ctx.program();
    const target = nativeSymbol(ctx);
    return target === null ? null : checker.getTypeOfSymbol(target);
  });
}

/** Whether expression `e` is the native handle: its symbol (through imports), or a value of its type. */
function isNative(ctx, e) {
  const { ts, checker } = ctx.program();
  const target = nativeSymbol(ctx);
  if (target === null) return false;
  if (ts.isIdentifier(e)) {
    let symbol = checker.getSymbolAtLocation(e);
    if (symbol && symbol.flags & ts.SymbolFlags.Alias) symbol = checker.getAliasedSymbol(symbol);
    if (symbol === target) return true;
  }
  const type = nativeType(ctx);
  return type !== null && checker.getTypeAtLocation(e) === type;
}

/**
 * The uses of the handle's functions under `node` (default: every src/
 * source): `{rel, line, name, node}`, `node` the property or element access,
 * or the binding element of a destructuring.
 */
function nativeUses(ctx, node) {
  const { ts, sourceFiles } = ctx.program();
  const out = [];
  const roots = node ? [node] : sourceFiles;
  for (const root of roots) {
    const sf = root.getSourceFile();
    const rel = ctx.rel(sf.fileName);
    const use = (n, name) => out.push({ rel, line: sf.getLineAndCharacterOfPosition(n.getStart(sf)).line + 1, name, node: n });
    const visit = (n) => {
      if (ts.isPropertyAccessExpression(n) && !ts.isPrivateIdentifier(n.name) && isNative(ctx, n.expression)) {
        use(n, n.name.text);
      } else if (ts.isElementAccessExpression(n) && ts.isStringLiteralLike(n.argumentExpression) && isNative(ctx, n.expression)) {
        use(n, n.argumentExpression.text);
      } else if (ts.isVariableDeclaration(n) && ts.isObjectBindingPattern(n.name) && n.initializer && isNative(ctx, n.initializer)) {
        for (const e of n.name.elements) {
          const key = e.propertyName || e.name;
          if (!e.dotDotDotToken && (ts.isIdentifier(key) || ts.isStringLiteralLike(key))) use(e, key.text);
        }
      }
      ts.forEachChild(n, visit);
    };
    visit(root);
  }
  return out;
}

/** The body a declaration runs, if it is a src/ function, method, accessor or constructor with one. */
function bodyOf(ts, d) {
  if (
    (ts.isFunctionDeclaration(d) ||
      ts.isMethodDeclaration(d) ||
      ts.isGetAccessorDeclaration(d) ||
      ts.isSetAccessorDeclaration(d) ||
      ts.isConstructorDeclaration(d) ||
      ts.isFunctionExpression(d) ||
      ts.isArrowFunction(d)) &&
    d.body
  ) {
    return d;
  }
  if (ts.isVariableDeclaration(d) && d.initializer && (ts.isArrowFunction(d.initializer) || ts.isFunctionExpression(d.initializer))) {
    return d.initializer;
  }
  if (ts.isPropertyDeclaration(d) && d.initializer && (ts.isArrowFunction(d.initializer) || ts.isFunctionExpression(d.initializer))) {
    return d.initializer;
  }
  return null;
}

/**
 * `root` and every src/ function, method or constructor it calls, as the
 * type checker resolves the calls (a module helper, another class's method,
 * `x['_m']()`, `this.#m()`, `new X()`), transitively, each once; with
 * `follow`, only the bodies it accepts; with `references`, also each
 * function it names as a value (a callback it hands on).
 */
function reachedBodies(ctx, root, follow = null, { references = false } = {}) {
  const { ts, checker } = ctx.program();
  // An overload signature has no body: its implementation is what runs.
  if (root.body === undefined && root.parent && root.parent.members) {
    const impl = root.parent.members.find(
      (o) =>
        o.body !== undefined &&
        o.kind === root.kind &&
        (ts.isConstructorDeclaration(root) || (o.name && root.name && o.name.getText() === root.name.getText())),
    );
    if (impl) root = impl;
  }
  const srcDir = ctx.srcDir;
  const out = [root];
  const seen = new Set([root]);
  /** Adds the bodies of the declarations `node` (a callee or a name) resolves to. */
  const reach = (node) => {
    let symbol = checker.getSymbolAtLocation(node);
    if (symbol && symbol.flags & ts.SymbolFlags.Alias) symbol = checker.getAliasedSymbol(symbol);
    for (const d of (symbol && symbol.declarations) || []) {
      const decls = ts.isClassDeclaration(d) ? d.members.filter((m) => ts.isConstructorDeclaration(m)) : [d];
      for (const x of decls) {
        const body = bodyOf(ts, x);
        if (body && !seen.has(body) && body.getSourceFile().fileName.startsWith(srcDir) && (follow === null || follow(body))) {
          seen.add(body);
          out.push(body);
        }
      }
    }
  };
  for (let i = 0; i < out.length; i++) {
    const visit = (n) => {
      if (ts.isCallExpression(n) || ts.isNewExpression(n)) {
        const callee = n.expression;
        const at = ts.isElementAccessExpression(callee) ? callee.argumentExpression : callee;
        reach(ts.isPropertyAccessExpression(at) ? at.name : at);
      } else if (references && ts.isIdentifier(n) && !(n.parent && ts.isPropertyAccessExpression(n.parent) && n.parent.name === n)) {
        // A function named as a value, not as its own declaration's name.
        const declaration = n.parent && (ts.isFunctionDeclaration(n.parent) || ts.isVariableDeclaration(n.parent)) && n.parent.name === n;
        if (!declaration) reach(n);
      }
      ts.forEachChild(n, visit);
    };
    visit(out[i]);
  }
  return out;
}

module.exports = { nativeSymbol, nativeUses, reachedBodies, bodyOf };
