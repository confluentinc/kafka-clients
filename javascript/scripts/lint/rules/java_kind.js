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

// check-java-kind (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Class family): each export translating a Java type takes the TypeScript
// form of its Java kind:
//   - a Java interface is an `interface`, beside which a runtime `const` of
//     the same name may hold its statics (`Admin.create`);
//   - a Java class, record or enum is a class ("An enum is a class with
//     frozen instances"), abstract exactly when Java's is; a Java abstract
//     class's constructor throws `TypeError` when `new.target` is the class
//     itself (`KafkaFuture`'s), and its translations of Java's abstract
//     methods are `abstract`;
//   - no export is a TypeScript `enum` ("There's no TypeScript `enum`").
// The types the JS conventions give another form are the table below. The
// mapping is check-java-name's (scripts/lint/java_ts.js `mappedItems`,
// `javaTargets` included). Not checked: an export whose Java class the index
// does not hold (the root's built-in errors), a nested type (a namespace's
// member), and an abstract Java method that has a concrete overload.

const { atNode } = require('../location');
const {
  javaIndexOf,
  mappedItems,
  exportedItems,
  publicMembers,
  javaMember,
  memberTags,
  shortName,
  PLACEHOLDERS,
  functionType,
} = require('../java_ts');
const { taggedMember } = require('./java_members');

/** The Java types the JS conventions translate in another form than their kind's: the form, and the rule. */
const OTHER_FORMS = new Map([
  [
    'org.apache.kafka.common.header.Headers',
    {
      form: 'class',
      reason:
        "javascript/CLAUDE.md, JavaScript Binding Conventions, Serialization: Headers wraps the FFI's RecordHeaders, a TypeScript class over a native handle",
    },
  ],
  [
    'org.apache.kafka.common.header.Header',
    {
      form: 'class',
      reason: 'javascript/CLAUDE.md, JavaScript Binding Conventions, Serialization: Header wraps the C RecordHeader too',
    },
  ],
]);

/** The rule a form comes from, for the findings. */
const RULE = {
  interface: 'a Java interface is a TypeScript interface (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family)',
  function: 'a callback is a function type (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family)',
  placeholder:
    'a type that is not translated is a placeholder, `type X = unknown` (javascript/CLAUDE.md, JavaScript Binding Conventions, Scope)',
  class: 'a Java class is a TypeScript class (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family)',
  enum: 'an enum is a class with frozen instances (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family)',
};

/** The TypeScript forms of a symbol's declarations (a merged namespace, which holds nested types, left out). */
function formsOf(ts, declarations) {
  const out = new Set();
  for (const d of declarations) {
    if (ts.isClassDeclaration(d)) out.add('class');
    else if (ts.isInterfaceDeclaration(d)) out.add('interface');
    else if (ts.isTypeAliasDeclaration(d)) {
      if (ts.isFunctionTypeNode(d.type)) out.add('function');
      else if (d.type.kind === ts.SyntaxKind.UnknownKeyword) out.add('placeholder');
      else out.add('type');
    } else if (ts.isVariableDeclaration(d)) out.add('const');
    else if (ts.isFunctionDeclaration(d)) out.add('function declaration');
    else if (ts.isEnumDeclaration(d)) out.add('enum');
  }
  return out;
}

/** How a finding names a form. */
const FORM_TEXT = {
  class: 'a class',
  interface: 'an interface',
  function: 'a function type',
  placeholder: 'a placeholder `unknown`',
  type: 'a type alias',
  const: 'a constant',
  'function declaration': 'a function',
  enum: 'a TypeScript enum',
};

/** The form the conventions give Java class `cls`: `{form, reason}`. */
function expectedForm(cls) {
  if (OTHER_FORMS.has(cls.fqn)) return OTHER_FORMS.get(cls.fqn);
  if (PLACEHOLDERS.has(cls.path[cls.path.length - 1])) return { form: 'placeholder', reason: RULE.placeholder };
  if (functionType(cls)) return { form: 'function', reason: RULE.function };
  if (cls.kind === 'interface') return { form: 'interface', reason: RULE.interface };
  return { form: 'class', reason: cls.kind === 'enum' ? RULE.enum : RULE.class };
}

/** Whether `e` is `new.target`. */
function isNewTarget(ts, e) {
  return ts.isMetaProperty(e) && e.keywordToken === ts.SyntaxKind.NewKeyword && e.name.text === 'target';
}

/** Whether condition `e` holds `new.target === name` (or `name === new.target`), alone or in an `&&`. */
function testsNewTarget(ts, e, name) {
  while (ts.isParenthesizedExpression(e)) e = e.expression;
  if (!ts.isBinaryExpression(e)) return false;
  if (e.operatorToken.kind === ts.SyntaxKind.AmpersandAmpersandToken) {
    return testsNewTarget(ts, e.left, name) || testsNewTarget(ts, e.right, name);
  }
  if (e.operatorToken.kind !== ts.SyntaxKind.EqualsEqualsEqualsToken) return false;
  const named = (x) => ts.isIdentifier(x) && x.text === name;
  return (isNewTarget(ts, e.left) && named(e.right)) || (isNewTarget(ts, e.right) && named(e.left));
}

/** Whether statement `s` throws `new TypeError(...)`, alone or as a statement of its block. */
function throwsTypeError(ts, s) {
  const statements = ts.isBlock(s) ? s.statements : [s];
  return statements.some(
    (x) =>
      ts.isThrowStatement(x) &&
      x.expression &&
      ts.isNewExpression(x.expression) &&
      ts.isIdentifier(x.expression.expression) &&
      x.expression.expression.text === 'TypeError',
  );
}

/** Whether class declaration `node`'s constructor throws `TypeError` when `new.target` is the class itself. */
function refusesItself(ts, node) {
  const name = node.name ? node.name.text : null;
  const ctor = node.members.find((m) => ts.isConstructorDeclaration(m) && m.body);
  if (!ctor || name === null) return false;
  return ctor.body.statements.some(
    (s) => ts.isIfStatement(s) && testsNewTarget(ts, s.expression, name) && throwsTypeError(ts, s.thenStatement),
  );
}

module.exports = {
  name: 'check-java-kind',
  kinds: ['ts-enum', 'kind-form', 'abstract-mismatch', 'abstract-new-target', 'abstract-method'],
  needs: ['program', 'java'],
  hint:
    'Translate a Java interface as an interface (a const of its name may hold its statics), a class, record or ' +
    "enum as a class, abstract exactly when Java's is, with a constructor that throws TypeError for new.target " +
    "itself and Java's abstract methods abstract, and no TypeScript enum (javascript/CLAUDE.md, JavaScript " +
    'Binding Conventions, Class family).',
  OTHER_FORMS,
  expectedForm,
  formsOf,
  check(ctx) {
    const { ts, checker } = ctx.program();
    const index = javaIndexOf(ctx);
    const findings = [];
    let checked = 0;
    // Any TypeScript enum, mapped or not.
    for (const item of exportedItems(ctx)) {
      if (item.kind !== 'enum') continue;
      checked += 1;
      findings.push({
        kind: 'ts-enum',
        subject: `${item.rel}:${item.name}`,
        at: atNode(ctx, item.node),
        message: `exports ${item.name} as a TypeScript enum, which no Java type translates to: ${RULE.enum}`,
      });
    }
    for (const { item, java } of mappedItems(ctx)) {
      if (item.tags.jsOnly || java === null || !java.cls) continue;
      checked += 1;
      const cls = java.cls;
      const subject = `${item.rel}:${item.name}`;
      const at = atNode(ctx, item.node);
      const symbol = item.node.name ? checker.getSymbolAtLocation(item.node.name) : undefined;
      const forms = formsOf(ts, (symbol && symbol.declarations) || [item.node]);
      const { form, reason } = expectedForm(cls);
      const javaKind = cls.kind === 'class' && cls.abstract ? 'abstract class' : cls.kind;
      const allowed = form === 'interface' ? ['interface', 'const'] : [form];
      const wrong = [...forms].filter((f) => !allowed.includes(f));
      if (!forms.has(form) || wrong.length > 0) {
        const shown = [...forms].map((f) => FORM_TEXT[f]).join(' and ');
        findings.push({
          kind: 'kind-form',
          subject,
          at,
          message: `translates the Java ${javaKind} ${shortName(cls)} as ${shown}: ${reason}`,
        });
        continue;
      }
      if (form !== 'class' || !ts.isClassDeclaration(item.node)) continue;
      const isAbstract = (ts.getCombinedModifierFlags(item.node) & ts.ModifierFlags.Abstract) !== 0;
      // A class the conventions give for a Java interface is not abstract.
      const javaAbstract = cls.kind === 'class' && cls.abstract;
      if (isAbstract !== javaAbstract) {
        findings.push({
          kind: 'abstract-mismatch',
          subject,
          at,
          message: javaAbstract
            ? `is not abstract, but the Java class ${shortName(cls)} is: make it an abstract class, so tsc refuses new (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family)`
            : `is abstract, but the Java ${cls.kind} ${shortName(cls)} is not: make it a class`,
        });
        continue;
      }
      if (!javaAbstract) continue;
      if (!refusesItself(ts, item.node)) {
        findings.push({
          kind: 'abstract-new-target',
          subject,
          at,
          message:
            `translates the Java abstract class ${shortName(cls)}, but no constructor statement throws TypeError when ` +
            `new.target is ${item.name} itself: plain JavaScript can construct it (javascript/CLAUDE.md, JavaScript ` +
            'Binding Conventions, Class family)',
        });
      }
      for (const m of publicMembers(ts, item.node)) {
        if (m.kind === 'constructor') continue;
        const tags = memberTags(ctx, item, m);
        if (tags.jsOnly) continue;
        const jm = tags.java ? taggedMember(index, tags.java.text) : javaMember(index, cls, m);
        if (jm === null || jm.kind !== 'methods' || jm.overloads.length === 0 || !jm.overloads.every((o) => o.abstract)) continue;
        checked += 1;
        if ((ts.getCombinedModifierFlags(m.node) & ts.ModifierFlags.Abstract) !== 0) continue;
        findings.push({
          kind: 'abstract-method',
          subject: `${item.name}.${m.name}`,
          at: atNode(ctx, m.node),
          message: `translates ${shortName(jm.owner)}#${jm.name}, which is abstract: make it abstract, as Java's abstract methods stay abstract (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family)`,
        });
      }
    }
    return { findings, checked };
  },
};
