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

// J7 check-type-mapping (javascript/CLAUDE.md "Java shape, JS idiom": Java
// `null` is `null`, a `long` crosses as a `number`, `send` returns a
// `KafkaFuture`, a blocking method returns a Promise): every parameter,
// return and property type of a translated member is the TypeScript type the
// table below gives its Java type (`return_type` and field `type` of the Java
// index; the TypeScript type as the checker resolves it). `null` and
// `undefined` are stripped first (a Java reference can be null). A return is
// a Promise of the mapped type only on a waiting member: one whose native
// path A1 resolves to a `_cb` twin, or one of `PURE_JS_WAITING`; a Promise
// elsewhere is a `type-mismatch`. A parameter translating a Java `Duration x`
// is named `xMs` (decision 8; `duration-name`), an options object's option is
// checked against the union of the Java types its forms give it.
//
// | Java                                         | TypeScript                              |
// |----------------------------------------------|-----------------------------------------|
// | byte, short, int, long, float, double, boxed | number                                  |
// | boolean, Boolean                             | boolean                                 |
// | String                                       | string                                  |
// | void, Void                                   | void                                    |
// | Object                                       | unknown                                 |
// | Duration                                     | number (milliseconds, PLAN.md decision 8)|
// | byte[], a key or value type variable K / V   | Data (bytes, or a string as UTF-8)      |
// | Future<T>, KafkaFuture<T>                    | KafkaFuture<T>                          |
// | List / Set / Collection<T>, T[]              | an array of T                           |
// | Iterable<T>                                  | Iterable<T>, or an array of T           |
// | Headers                                      | an array, or Iterable, of Header        |
// | Map, Properties (a client's configuration)   | the `...Configs` configuration type     |
// | a type variable                              | a type parameter                        |
// | a Java class translated in lib/              | its TypeScript type                     |
//
// Any other Java type is `unmapped`, naming both types.

const { atNode } = require('../location');
const { mappedMembers } = require('./java_members');
const { liveForms, optionName, formOptionNames } = require('./overload_shape');
const { waitingMembers } = require('./waiting_call_promise');

/**
 * The Java members that block in Java and wait in JavaScript without a native
 * call, so their Promise return is accepted as A1's waiting calls' is.
 */
const PURE_JS_WAITING = new Map([
  [
    'org.apache.kafka.common.KafkaFuture#get',
    "Java's get() blocks until the future completes; the JavaScript future settles in JavaScript, from the delivery callback",
  ],
]);
const { mappedItems, functionType, constantsOnly } = require('../java_ts');

const NUMBERS = new Set(['byte', 'short', 'int', 'long', 'float', 'double', 'Byte', 'Short', 'Integer', 'Long', 'Float', 'Double', 'Number']);
const LISTS = new Set(['List', 'Set', 'Collection', 'SortedSet', 'Queue']);

/** A Java type as written (`Map<String, ? extends List<T>>[]`) parsed: `{name, args, dims}`. */
function parseJava(text) {
  let i = 0;
  const s = text.replace(/\s+/g, ' ').trim();
  const parse = () => {
    while (s[i] === ' ') i++;
    let name = '';
    while (i < s.length && /[\w.?]/.test(s[i])) name += s[i++];
    while (s[i] === ' ') i++;
    if (name === '?' && /^(extends|super) /.test(s.slice(i))) {
      i = s.indexOf(' ', i) + 1;
      return parse();
    }
    const args = [];
    if (s[i] === '<') {
      i++;
      for (;;) {
        args.push(parse());
        while (s[i] === ' ') i++;
        if (s[i] === ',') i++;
        else break;
      }
      if (s[i] === '>') i++;
    }
    let dims = 0;
    while (s.slice(i, i + 2) === '[]') {
      dims++;
      i += 2;
    }
    return { name: name.split('.').pop(), args, dims };
  };
  return parse();
}

/** `java` as text again. */
function javaText(j) {
  return `${j.name}${j.args.length ? `<${j.args.map(javaText).join(', ')}>` : ''}${'[]'.repeat(j.dims)}`;
}

module.exports = {
  name: 'check-type-mapping',
  kinds: ['type-mismatch', 'unmapped', 'duration-name'],
  needs: ['program', 'java', 'src', 'header'],
  hint:
    'Give the member the TypeScript type the table of scripts/lint/rules/type_mapping.js gives its Java type ' +
    '(javascript/CLAUDE.md "Java shape, JS idiom").',
  parseJava,
  check(ctx) {
    const { ts, checker } = ctx.program();
    // The TypeScript name of each Java class lib/ translates, by Java simple name.
    const tsNames = new Map();
    for (const { item, java } of mappedItems(ctx)) {
      if (java && java.cls) tsNames.set(java.cls.path[java.cls.path.length - 1], item.name);
    }
    // `null` / `undefined` stripped, a `Readonly<T>` read as `T`; `void`, a type parameter and
    // `unknown` kept (stripping would make them `never`, `T & {}` and `{}`).
    const strip = (t) => {
      if (t.flags & (ts.TypeFlags.Void | ts.TypeFlags.TypeParameter | ts.TypeFlags.Unknown | ts.TypeFlags.Any)) return t;
      const s = checker.getNonNullableType(t);
      if (s.aliasSymbol && s.aliasSymbol.name === 'Readonly' && s.aliasTypeArguments) return s.aliasTypeArguments[0];
      return s;
    };
    const parts = (t) => (t.isUnion() ? t.types : [t]);
    const symbolName = (t) => (t.aliasSymbol && t.aliasSymbol.name) || (t.symbol && t.symbol.name) || '';
    const elementOf = (t) => {
      if (checker.isArrayType(t) || checker.isTupleType(t)) return checker.getTypeArguments(t)[0];
      const n = symbolName(t);
      if (n === 'ReadonlyArray' || n === 'Array' || n === 'Iterable') return (t.aliasTypeArguments || checker.getTypeArguments(t))[0];
      return undefined;
    };

    /** Whether TS type `t` (null stripped) is what Java type `j` maps to: true, false, or 'unmapped'. */
    const matches = (j, t0) => {
      const t = strip(t0);
      if (j.dims > 0) {
        if (j.name === 'byte' && j.dims === 1) return matches({ name: 'K', args: [], dims: 0 }, t);
        const e = elementOf(t);
        return e === undefined ? false : matches({ ...j, dims: j.dims - 1 }, e);
      }
      if (NUMBERS.has(j.name) || j.name === 'Duration') return (t.flags & ts.TypeFlags.NumberLike) !== 0;
      if (j.name === 'boolean' || j.name === 'Boolean') return (t.flags & ts.TypeFlags.BooleanLike) !== 0;
      if (j.name === 'String' || j.name === 'char' || j.name === 'Character') return (t.flags & ts.TypeFlags.StringLike) !== 0;
      if (j.name === 'void' || j.name === 'Void') return (t.flags & (ts.TypeFlags.Void | ts.TypeFlags.Undefined)) !== 0;
      if (j.name === 'Object') return (t.flags & (ts.TypeFlags.Unknown | ts.TypeFlags.Any)) !== 0;
      if (j.name === 'K' || j.name === 'V') {
        return parts(t).every(
          (p) => (p.flags & ts.TypeFlags.StringLike) !== 0 || ['Buffer', 'Uint8Array'].includes(symbolName(p)),
        );
      }
      if (j.name === 'Future' || j.name === 'KafkaFuture') {
        return symbolName(t) === 'KafkaFuture' && (j.args.length === 0 || matches(j.args[0], checker.getTypeArguments(t)[0]) === true);
      }
      if (LISTS.has(j.name) || j.name === 'Iterable' || j.name === 'Headers') {
        const e = elementOf(t);
        if (e === undefined) return false;
        const elem = j.name === 'Headers' ? { name: 'Header', args: [], dims: 0 } : j.args[0];
        return elem === undefined ? true : matches(elem, e);
      }
      if (j.name === 'Map' || j.name === 'Properties') {
        const n = (t0.aliasSymbol && t0.aliasSymbol.name) || symbolName(t);
        return n.endsWith('Configs') ? true : 'unmapped';
      }
      if (/^[A-Z]$/.test(j.name)) return (t.flags & ts.TypeFlags.TypeParameter) !== 0;
      if (tsNames.has(j.name)) {
        const want = tsNames.get(j.name);
        return symbolName(t) === want || parts(t).every((p) => symbolName(p) === want || checker.typeToString(p) === want);
      }
      return 'unmapped';
    };

    const findings = [];
    let checked = 0;
    const waiting = waitingMembers(ctx);
    // Where the member being checked is declared, for its findings.
    let at = null;
    /**
     * Checks TS type `tsType` against Java type(s) `javaTypes` (several: the
     * forms naming one option, their union); `promise` unwraps a Promise
     * return of a waiting member.
     */
    const check = (subject, where, javaTypes, tsType, { promise = false } = {}) => {
      const list = (Array.isArray(javaTypes) ? javaTypes : [javaTypes]).filter((x) => x);
      if (list.length === 0) return;
      checked += 1;
      const js = [...new Map(list.map((x) => [x, parseJava(x)])).values()];
      let t = tsType;
      if (promise) {
        const s = strip(t);
        if (symbolName(s) === 'Promise') t = checker.getTypeArguments(s)[0] || (s.aliasTypeArguments || [])[0] || t;
      }
      let ok;
      if (js.length === 1) {
        ok = matches(js[0], t);
      } else {
        const ps = parts(strip(t));
        ok = js.some((j) => matches(j, ps[0]) === 'unmapped')
          ? 'unmapped'
          : js.every((j) => ps.some((p) => matches(j, p) === true)) && ps.every((p) => js.some((j) => matches(j, p) === true));
      }
      if (ok === true) return;
      const java = js.map(javaText).join(' | ');
      findings.push(
        ok === 'unmapped'
          ? { at, kind: 'unmapped', subject, message: `${where}: Java ${java} has no row in the table (TypeScript ${checker.typeToString(tsType)})` }
          : { at, kind: 'type-mismatch', subject, message: `${where}: Java ${java} is not TypeScript ${checker.typeToString(tsType)}` },
      );
    };
    /** A parameter translating a Java `Duration x` is named `xMs` (decision 8). */
    const durationName = (subject, p, javaType, javaName) => {
      if (javaType !== 'Duration' || !javaName) return;
      const want = optionName(javaType, javaName);
      if (p.name.getText() !== want) {
        findings.push({
          at: atNode(ctx, p),
          kind: 'duration-name',
          subject,
          message: `parameter ${p.name.getText()}: it translates Java's Duration ${javaName}, so it is named ${want}`,
        });
      }
    };

    for (const { item, java, m, jm } of mappedMembers(ctx)) {
      if (jm === null || functionType(java.cls)) continue;
      // A config class translates its constants only: its constructor is no Java one's shape.
      if (constantsOnly(java.cls) && m.kind === 'constructor') continue;
      const subject = `${item.name}.${m.name}`;
      at = atNode(ctx, m.node);
      if (m.kind === 'property' || m.kind === 'get') {
        const javaType =
          jm.kind === 'field' ? jm.field.type : (liveForms(jm.overloads)[0] || jm.overloads[0]).return_type;
        const tsType = m.kind === 'get' ? checker.getReturnTypeOfSignature(checker.getSignatureFromDeclaration(m.node)) : checker.getTypeAtLocation(m.node);
        check(subject, 'type', javaType, tsType);
        continue;
      }
      if (jm.kind === 'field') continue;
      const forms = liveForms(jm.overloads);
      const params = [...(m.node.parameters || [])];
      const sameArity = forms.find((f) => f.params.length === params.length);
      if (m.kind === 'method') {
        const sig = checker.getSignatureFromDeclaration(m.node);
        const javaReturn = (sameArity || forms[0] || jm.overloads[0]).return_type;
        const blocking = waiting.has(subject) || PURE_JS_WAITING.has(`${jm.owner.fqn}#${jm.name}`);
        if (sig) check(subject, 'return', javaReturn, checker.getReturnTypeOfSignature(sig), { promise: blocking });
      }
      const options = params.length === 1 ? strip(checker.getTypeAtLocation(params[0])) : null;
      const isOptions =
        forms.length > 1 &&
        options !== null &&
        (options.flags & ts.TypeFlags.Object) !== 0 &&
        !options.isUnion() &&
        !checker.isArrayType(options) &&
        options.getCallSignatures().length === 0 &&
        !(options.symbol && options.symbol.flags & ts.SymbolFlags.Class);
      if (isOptions) {
        // The options object of an overload set (J6 checks its names): each
        // option against the union of the Java types the forms naming it give.
        const names = formOptionNames(forms);
        for (const prop of checker.getPropertiesOfType(options)) {
          const types = [];
          forms.forEach((f, k) => names[k].forEach((n, i) => n === prop.name && types.push(f.params[i])));
          if (types.length > 0) check(subject, `option ${prop.name}`, types, checker.getTypeOfSymbolAtLocation(prop, params[0]));
        }
      } else if (sameArity) {
        params.forEach((p, i) => {
          check(subject, `parameter ${p.name.getText()}`, sameArity.params[i], checker.getTypeAtLocation(p));
          durationName(subject, p, sameArity.params[i], sameArity.param_names[i]);
        });
      }
    }
    return { findings, checked };
  },
};
