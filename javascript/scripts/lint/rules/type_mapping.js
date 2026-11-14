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

// J7 check-type-mapping (root CLAUDE.md, JavaScript Binding Conventions,
// Types, and javascript/CLAUDE.md "Java shape, JS idiom": Java `null` is
// `null`, a `long` is a `bigint`, `send` returns a `KafkaFuture`, a blocking
// method returns a Promise): every parameter,
// return and property type of a translated member is the TypeScript type the
// table below gives its Java type (`return_type` and field `type` of the Java
// index; the TypeScript type as the checker resolves it). `null` and
// `undefined` are stripped first (a Java reference can be null). A return is
// a Promise of the mapped type only on a waiting member: one whose native
// path A1 resolves to a `_cb` twin, or one of `PURE_JS_WAITING`; a Promise
// elsewhere is a `type-mismatch`. A parameter translating a Java `Duration x`
// is named `x`, Java's name (`duration-name`), an options object's option is
// checked against the union of the Java types its forms give it.
//
// | Java                                         | TypeScript                              |
// |----------------------------------------------|-----------------------------------------|
// | byte, short, int, float, double, boxed       | number                                  |
// | long, Long, returned (a return, a field)     | bigint                                  |
// | long, Long, taken                            | bigint | number (a whole number up to 2^53) |
// | boolean, Boolean                             | boolean                                 |
// | String                                       | string                                  |
// | void, Void                                   | void                                    |
// | Object                                       | unknown                                 |
// | Duration; long x, TimeUnit unit (one Duration) | number (milliseconds) or a Temporal.Duration |
// | byte[], taken                                | Uint8Array (a Buffer is one), never a string |
// | byte[], returned (a return, a field)         | Buffer                                  |
// | Future<T>, KafkaFuture<T>                    | KafkaFuture<T>                          |
// | CompletionStage<T>                           | Promise<T>                              |
// | KafkaFuture.BaseFunction<A, B>, BiConsumer   | the alias of the same name: KafkaFuture.BaseFunction<A, B> |
// | Throwable                                    | unknown (anything a function may throw)  |
// | MetricValueProvider, MetricConfig, Measurable, Partitioner (placeholders) | unknown    |
// | Time (not a public Java type)                | unknown                                 |
// | ? (an unbounded wildcard)                    | any type                                 |
// | T, as a bare value's type                    | Bare<T> (Extract<T, BareValue>), at the bare parameter only |
// | List / Set / Collection / Iterable<T>, T[], taken | Iterable<T>, or an array of T      |
// | List / Collection<T>, T[], returned          | an array of T (a new array each call)   |
// | Iterable<T>, returned                        | Iterable<T>, or an array of T           |
// | Set<T>, returned (a constant: read-only)     | Set<T> or ReadonlySet<T>                |
// | Set<TopicPartition>, returned                | TopicPartitionSet                       |
// | Map<TopicPartition, V>                       | TopicPartitionMap<V>                    |
// | Map<MetricName, ? extends Metric>            | MetricNameMap<Metric>                   |
// | Map<String, String> (MetricName's tags)      | Map<string, string>, a ReadonlyMap taken |
// | Map, Properties (a client's configuration)   | the `...Configs` configuration type     |
// | a type variable                              | a type parameter                        |
// | a Java class translated in src/              | its TypeScript type                     |
//
// Any other Java type is `unmapped`, naming both types.

const { atNode } = require('../location');
const { mappedMembers } = require('./java_members');
const { liveForms, optionName, formOptionNames, signaturesOf } = require('./overload_shape');
const { waitingMembers } = require('./waiting_call_promise');

/**
 * The Java members that block in Java and wait in JavaScript without a native
 * call, so their Promise return is accepted as A1's waiting calls' is.
 */
const PURE_JS_WAITING = new Map([
  [
    'org.apache.kafka.common.KafkaFuture#get',
    "Java's get() blocks until the future completes; a JavaScript future with no native handle (allOf(), completedFuture(), " +
      "the dependants) settles in JavaScript, and a record's waits natively through its _cb twins",
  ],
]);
/**
 * The Java members a user implements whose JavaScript form may also return a
 * Promise of the mapped type, which the client awaits (root CLAUDE.md,
 * JavaScript Binding Conventions, Serialization: *(js-only)*).
 */
const AWAITED_RETURNS = new Map([
  [
    'org.apache.kafka.common.serialization.Serializer#serialize',
    'a serializer may return a Promise, which the producer awaits before it hands the record over',
  ],
]);
const { mappedItems, functionType, constantsOnly, formParameters, PLACEHOLDERS, NOT_PUBLIC_TYPES } = require('../java_ts');

const NUMBERS = new Set(['byte', 'short', 'int', 'float', 'double', 'Byte', 'Short', 'Integer', 'Float', 'Double', 'Number']);
const LONGS = new Set(['long', 'Long']);
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
  needs: ['program', 'java', 'native', 'header'],
  hint:
    'Give the member the TypeScript type the table of scripts/lint/rules/type_mapping.js gives its Java type ' +
    '(javascript/CLAUDE.md "Java shape, JS idiom").',
  parseJava,
  check(ctx) {
    const { ts, checker } = ctx.program();
    // The TypeScript name of each Java class src/ translates, by Java simple name.
    const tsNames = new Map();
    for (const { item, java } of mappedItems(ctx)) {
      if (java && java.cls) tsNames.set(java.cls.path[java.cls.path.length - 1], item.name);
    }
    // A bare value's type as Java's: KafkaFuture's `Bare<T>`, the values of T
    // the bare form of a one-parameter member takes (`Extract<T, BareValue>`),
    // is T. Read only at that parameter: everywhere else a narrowing of a Java
    // type is compared as written, so it is a mismatch.
    const unwrapBare = (t) => {
      // `Bare<T | null>` distributes to `Extract<T, BareValue> | null`.
      if (t.isUnion()) {
        const kept = t.types.filter((p) => !(p.flags & (ts.TypeFlags.Null | ts.TypeFlags.Undefined)));
        if (kept.length === 1 && kept[0] !== unwrapBare(kept[0])) return unwrapBare(kept[0]);
        return t;
      }
      const alias = t.aliasSymbol && t.aliasSymbol.name;
      const args = t.aliasTypeArguments || [];
      if (alias === 'Bare' && args.length === 1) return args[0];
      const second = args[1];
      if (alias === 'Extract' && args.length === 2 && second.aliasSymbol && second.aliasSymbol.name === 'BareValue') return args[0];
      return t;
    };
    // `null` / `undefined` stripped, a `Readonly<T>` read as `T`; `void`, a type parameter and
    // `unknown` kept (stripping would make them `never`, `T & {}` and `{}`).
    const strip = (t) => {
      if (t.flags & (ts.TypeFlags.Void | ts.TypeFlags.TypeParameter | ts.TypeFlags.Unknown | ts.TypeFlags.Any)) return t;
      // `T | null`: the type parameter itself (getNonNullableType would give `T & {}`).
      if (t.isUnion()) {
        const kept = t.types.filter((p) => !(p.flags & (ts.TypeFlags.Null | ts.TypeFlags.Undefined)));
        if (kept.length === 1) return strip(kept[0]);
      }
      const s = checker.getNonNullableType(t);
      if (s.aliasSymbol && s.aliasSymbol.name === 'Readonly' && s.aliasTypeArguments) return s.aliasTypeArguments[0];
      return s;
    };
    const parts = (t) => (t.isUnion() ? t.types : [t]);
    const symbolName = (t) => (t.aliasSymbol && t.aliasSymbol.name) || (t.symbol && t.symbol.name) || '';
    // The element type of an array (any direction) or, taken only, of an
    // Iterable: a Java array or collection the caller passes is any iterable
    // of its elements, and one returned is an array (root CLAUDE.md,
    // JavaScript Binding Conventions, Types; the plan's §5, item 1).
    const elementOf = (t, iterable = !outward) => {
      if (checker.isArrayType(t) || checker.isTupleType(t)) return checker.getTypeArguments(t)[0];
      const n = symbolName(t);
      if (n === 'ReadonlyArray' || n === 'Array' || (iterable && n === 'Iterable')) return (t.aliasTypeArguments || checker.getTypeArguments(t))[0];
      return undefined;
    };
    const typeArgs = (t) => t.aliasTypeArguments || checker.getTypeArguments(t);

    /** Whether TS type `t` (null stripped) is what Java type `j` maps to: true, false, or 'unmapped'. */
    const matches = (j, t0) => {
      const t = strip(t0);
      if (j.dims > 0) {
        // Java's byte[] is bytes only (a header value, Header#value): a
        // string is no byte[], unlike a key or value (K / V, Data). The
        // caller passes a Uint8Array (a Buffer is one), and the client
        // returns a Buffer (root CLAUDE.md, JavaScript Binding Conventions,
        // Types).
        if (j.name === 'byte' && j.dims === 1) {
          return parts(t).every((p) => (outward ? ['Buffer'] : ['Buffer', 'Uint8Array']).includes(symbolName(p)));
        }
        const e = elementOf(t);
        return e === undefined ? false : matches({ ...j, dims: j.dims - 1 }, e);
      }
      if (NUMBERS.has(j.name)) return (t.flags & ts.TypeFlags.NumberLike) !== 0;
      if (LONGS.has(j.name)) {
        // Java's long: a bigint returned; taken, a bigint or a whole number
        // (root CLAUDE.md, JavaScript Binding Conventions, Types).
        const ps = parts(t);
        const big = (p) => (p.flags & ts.TypeFlags.BigIntLike) !== 0;
        const number = (p) => (p.flags & ts.TypeFlags.NumberLike) !== 0;
        if (outward) return ps.every(big);
        return ps.some(big) && ps.some(number) && ps.every((p) => big(p) || number(p));
      }
      if (j.name === 'Duration') {
        // A number of milliseconds, and a Temporal.Duration typed by its
        // structure (its `total` method, src/internal/duration.ts).
        const ps = parts(t);
        const number = (p) => (p.flags & ts.TypeFlags.NumberLike) !== 0;
        return ps.some(number) && ps.every((p) => number(p) || checker.getPropertyOfType(p, 'total') !== undefined);
      }
      if (j.name === 'boolean' || j.name === 'Boolean') return (t.flags & ts.TypeFlags.BooleanLike) !== 0;
      if (j.name === 'String' || j.name === 'char' || j.name === 'Character') return (t.flags & ts.TypeFlags.StringLike) !== 0;
      if (j.name === 'void' || j.name === 'Void') return (t.flags & (ts.TypeFlags.Void | ts.TypeFlags.Undefined)) !== 0;
      if (j.name === 'Object') return (t.flags & (ts.TypeFlags.Unknown | ts.TypeFlags.Any)) !== 0;
      if (j.name === 'CompletionStage') {
        return symbolName(t) === 'Promise' && (j.args.length === 0 || matches(j.args[0], checker.getTypeArguments(t)[0]) === true);
      }
      if (j.name === 'BaseFunction' || j.name === 'BiConsumer') {
        const alias = t0.aliasSymbol || t.aliasSymbol;
        const args = t0.aliasTypeArguments || t.aliasTypeArguments || [];
        return (
          alias !== undefined &&
          alias.name === j.name &&
          (j.args.length === 0 || (args.length === j.args.length && j.args.every((a, i) => matches(a, args[i]) === true)))
        );
      }
      if (j.name === 'Throwable') return (t.flags & ts.TypeFlags.Unknown) !== 0;
      // The types the client does not translate, and those Java does not make
      // public, typed unknown (root CLAUDE.md, JavaScript Binding
      // Conventions, Scope). A placeholder is an alias of unknown.
      if (PLACEHOLDERS.has(j.name) || NOT_PUBLIC_TYPES.has(j.name)) return (t.flags & (ts.TypeFlags.Unknown | ts.TypeFlags.Any)) !== 0;
      if (j.name === '?') return true;
      if (j.name === 'Future' || j.name === 'KafkaFuture') {
        return symbolName(t) === 'KafkaFuture' && (j.args.length === 0 || matches(j.args[0], checker.getTypeArguments(t)[0]) === true);
      }
      if (j.name === 'Set' && outward && j.args[0] !== undefined && j.args[0].name === 'TopicPartition') {
        // A set of TopicPartition returned is a TopicPartitionSet, whose
        // members compare by value (root CLAUDE.md, JavaScript Binding
        // Conventions, Types).
        return symbolName(t) === 'TopicPartitionSet';
      }
      if (j.name === 'Set' && outward && ['ReadonlySet', 'Set'].includes(symbolName(t))) {
        // A Set returned is a Set; a constant one a read-only set (root
        // CLAUDE.md, JavaScript Binding Conventions, Names).
        const e = typeArgs(t)[0];
        const elem = j.args[0];
        return elem === undefined || e === undefined ? true : matches(elem, e);
      }
      if (j.name === 'Map' && j.args.length === 2 && j.args[0].name === 'TopicPartition') {
        return symbolName(t) === 'TopicPartitionMap' && matches(j.args[1], typeArgs(t)[0]) === true;
      }
      if (j.name === 'Map' && j.args.length === 2 && j.args.every((a) => a.name === 'String' && a.dims === 0)) {
        // MetricName's tags: a Map returned, a Map or ReadonlyMap taken.
        const n = symbolName(t);
        if (!(n === 'Map' || (!outward && n === 'ReadonlyMap'))) return (t0.aliasSymbol && t0.aliasSymbol.name.endsWith('Configs')) || symbolName(t).endsWith('Configs') ? true : false;
        const [k, v] = typeArgs(t);
        return matches(j.args[0], k) === true && matches(j.args[1], v) === true;
      }
      if (j.name === 'Map' && j.args.length === 2 && j.args[0].name === 'MetricName') {
        return symbolName(t) === 'MetricNameMap' && matches(j.args[1], typeArgs(t)[0]) === true;
      }
      if (j.name === 'Set' && outward) return false;
      if (LISTS.has(j.name) || j.name === 'Iterable') {
        // A Java Iterable returned stays an iterable.
        const e = elementOf(t, !outward || j.name === 'Iterable');
        if (e === undefined) return false;
        const elem = j.args[0];
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
    // Whether the type being checked is returned (a return, a field) rather than taken.
    let outward = false;
    const waiting = waitingMembers(ctx);
    // Where the member being checked is declared, for its findings.
    let at = null;
    /**
     * Checks TS type `tsType` against Java type(s) `javaTypes` (several: the
     * forms naming one option, their union); `promise` unwraps a Promise
     * return of a waiting member.
     */
    const check = (subject, where, javaTypes, tsType, { promise = false, awaited = false } = {}) => {
      const list = (Array.isArray(javaTypes) ? javaTypes : [javaTypes]).filter((x) => x);
      if (list.length === 0) return;
      checked += 1;
      // A return the user's implementation gives the client (AWAITED_RETURNS)
      // is taken, not returned.
      outward = (where === 'type' || where === 'return') && !awaited;
      const js = [...new Map(list.map((x) => [x, parseJava(x)])).values()];
      let t = tsType;
      if (promise) {
        const s = strip(t);
        if (symbolName(s) === 'Promise') t = checker.getTypeArguments(s)[0] || (s.aliasTypeArguments || [])[0] || t;
      }
      let ok;
      if (awaited && js.length === 1) {
        // Each part as itself, a Promise as what it resolves to.
        const ps = parts(strip(t)).map((p) => (symbolName(p) === 'Promise' ? checker.getTypeArguments(p)[0] || p : p));
        const each = ps.map((p) => matches(js[0], p));
        ok = each.includes('unmapped') ? 'unmapped' : each.every((x) => x === true);
      } else if (js.length === 1) {
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
    /** A parameter translating a Java `Duration x` is named `x`, Java's name. */
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
      const names = formOptionNames(forms);
      // The bare value's name: the one parameter of the one-parameter forms
      // (one name only; src/internal/args.ts gives no bare value otherwise).
      const bareNames = [...new Set(forms.filter((f) => f.params.length === 1).map((f) => names[forms.indexOf(f)][0]))];
      const bareName = bareNames.length === 1 ? bareNames[0] : null;
      // Every signature: the overload signatures, else the implementation.
      signaturesOf(ts, m.node).forEach((node, n) => {
        const where = n === 0 ? subject : `${subject}#${n + 1}`;
        at = atNode(ctx, node);
        const params = formParameters(ts, node);
        const sameArity = forms.find((f) => f.params.length === params.length);
        if (m.kind === 'method') {
          const sig = checker.getSignatureFromDeclaration(node);
          const javaReturn = (sameArity || forms[0] || jm.overloads[0]).return_type;
          // A Java CompletionStage is itself a Promise: a member returning
          // one (toCompletionStage, which waits on the future through its
          // _cb twin) is never unwrapped.
          const blocking =
            (waiting.has(subject) || PURE_JS_WAITING.has(`${jm.owner.fqn}#${jm.name}`)) && parseJava(javaReturn || 'void').name !== 'CompletionStage';
          const awaited = AWAITED_RETURNS.has(`${jm.owner.fqn}#${jm.name}`);
          if (sig) check(where, 'return', javaReturn, checker.getReturnTypeOfSignature(sig), { promise: blocking, awaited });
        }
        const p0 = params.length === 1 ? params[0] : null;
        if (p0 !== null && bareName !== null && ts.isIdentifier(p0.name) && p0.name.text === bareName) {
          // The bare value of the one-parameter forms: against their Java types.
          const types = forms.filter((f, k) => f.params.length === 1 && names[k][0] === bareName).map((f) => f.types[0]);
          check(where, `parameter ${bareName}`, types, unwrapBare(checker.getTypeAtLocation(p0)));
          durationName(where, p0, types[0], forms.find((f) => f.params.length === 1).param_names[0]);
          return;
        }
        const options = p0 === null ? null : strip(checker.getTypeAtLocation(p0));
        const isOptions =
          options !== null &&
          (forms.length > 1 || (ts.isIdentifier(p0.name) && p0.name.text === 'options')) &&
          (options.flags & ts.TypeFlags.Object) !== 0 &&
          !options.isUnion() &&
          !checker.isArrayType(options) &&
          options.getCallSignatures().length === 0 &&
          !(options.symbol && options.symbol.flags & ts.SymbolFlags.Class);
        if (isOptions) {
          // An options object (J6 checks its names): each option against the
          // union of the Java types the forms naming it give; a name closed
          // with `never` is no option of this signature.
          for (const prop of checker.getPropertiesOfType(options)) {
            const type = checker.getTypeOfSymbolAtLocation(prop, p0);
            if (checker.getNonNullableType(type).flags & ts.TypeFlags.Never) continue;
            const types = [];
            forms.forEach((f, k) => names[k].forEach((x, i) => x === prop.name && types.push(f.types[i])));
            if (types.length > 0) check(where, `option ${prop.name}`, types, type);
          }
        } else if (sameArity) {
          params.forEach((p, i) => {
            check(where, `parameter ${p.name.getText()}`, sameArity.types[i], checker.getTypeAtLocation(p));
            durationName(where, p, sameArity.types[i], sameArity.param_names[i]);
          });
        }
      });
    }
    return { findings, checked };
  },
};
