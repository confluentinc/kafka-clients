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

// check-type-mapping (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Types, and javascript/CLAUDE.md "Java shape, JS idiom": Java `null` is
// `null`, a `long` is a `bigint`, `send` returns a `KafkaFuture`, a blocking
// method returns a Promise): every parameter,
// return and property type of a translated member is the TypeScript type the
// table below gives its Java type (`return_type` and field `type` of the Java
// index; the TypeScript type as the checker resolves it). `null` and
// `undefined` are stripped first (a Java reference can be null). A return is
// a Promise of the mapped type only on a waiting member: one whose native
// path check-waiting-call-promise resolves to a `_cb` twin, or one of
// `PURE_JS_WAITING`; a Promise elsewhere is a `type-mismatch`. A parameter
// translating a Java `Duration x` is named `x`, Java's name
// (`duration-name`), an options object's option is checked against the
// union of the Java types its forms give it.
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
// | Optional<T>, OptionalInt, OptionalLong       | T (or number, bigint) | null            |
// | Duration; long x, TimeUnit unit (one Duration) | number (milliseconds) or a Temporal.Duration |
// | byte[], taken                                | Uint8Array (a Buffer is one), never a string |
// | byte[], returned (a return, a field)         | Buffer                                  |
// | Future<T>, KafkaFuture<T>                    | KafkaFuture<T>                          |
// | CompletionStage<T>                           | Promise<T>                              |
// | KafkaFuture.BaseFunction<A, B>, BiConsumer   | the alias of the same name: KafkaFuture.BaseFunction<A, B> |
// | Throwable                                    | unknown (anything a function may throw)  |
// | RuntimeException, Exception, taken           | Error                                    |
// | MetricValueProvider, MetricConfig, Measurable, Partitioner (placeholders) | unknown    |
// | Time (not a public Java type)                | unknown                                 |
// | ? (an unbounded wildcard)                    | any type                                 |
// | T, as a bare value's type                    | Bare<T> (Extract<T, BareValue>), at the bare parameter only |
// | Header, as a bare value's type               | RecordHeader (BARE_CLASSES), at the bare parameter only |
// | List / Set / Collection / Iterable<T>, T[], taken | Iterable<T>, or an array of T      |
// | Headers, Iterable<Header>, Header[], taken   | an iterable (an array) of Header | readonly [string, Uint8Array | null]; Headers also as itself |
// | List / Collection<T>, T[], returned          | an array of T (a new array each call)   |
// | Iterable<T>, returned                        | Iterable<T>, or an array of T           |
// | Set<T>, returned (a constant: read-only)     | Set<T> or ReadonlySet<T>                |
// | Set<TopicPartition>, returned                | TopicPartitionSet                       |
// | Map<TopicPartition, V>                       | TopicPartitionMap<V>                    |
// | Map<MetricName, ? extends Metric>            | MetricNameMap<Metric>                   |
// | Map<String, V>, returned                     | Map<string, V>                          |
// | Map<String, V>, taken                        | a Map or ReadonlyMap, or a plain object (Record) of string to V |
// | Map, Properties (a client's configuration)   | the `...Configs` configuration type (& its serde keys) |
// | a type variable                              | a type parameter                        |
// | a Java class translated in src/              | its TypeScript type                     |
//
// Any other Java type is `unmapped`, naming both types.

const { atNode } = require('../location');
const { mappedMembers } = require('./java_members');
const { LEFT_OUT, liveForms, optionName, formOptionNames, signaturesOf, formKey } = require('./overload_shape');
const { waitingMembers } = require('./waiting_call_promise');

/**
 * The Java members that block in Java and wait in JavaScript without a native
 * call, so their Promise return is accepted, as the Promise return of
 * check-waiting-call-promise's waiting calls is.
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
 * Promise of the mapped type, which the client awaits (javascript/CLAUDE.md,
 * JavaScript Binding Conventions, Serialization: *(js-only)*).
 */
const AWAITED_RETURNS = new Map([
  [
    'org.apache.kafka.common.serialization.Serializer#serialize',
    {
      taken: true,
      reason:
        'a serializer may return a Promise, which the producer awaits before it hands the record over; the bytes it returns are taken',
    },
  ],
  [
    'org.apache.kafka.common.serialization.Deserializer#deserialize',
    {
      taken: false,
      reason: 'a deserializer may return a Promise, which the consumer awaits; the value it returns is what poll() returns',
    },
  ],
]);

/**
 * The `AWAITED_RETURNS` entry of Java member `jm`: its own, or that of the
 * interface its class implements, as each built-in serde class's `serialize`
 * and `deserialize` implement the interface's (overload_shape.js
 * `isMergedForm`), or undefined.
 *
 * @param {{ name: string, owner: { fqn: string, supertype_fqns: Array<string | null> } }} jm
 * @returns {{ taken: boolean, reason: string } | undefined}
 */
function awaitedReturn(jm) {
  const own = AWAITED_RETURNS.get(`${jm.owner.fqn}#${jm.name}`);
  if (own !== undefined) return own;
  for (const fqn of jm.owner.supertype_fqns || []) {
    const entry = fqn === null ? undefined : AWAITED_RETURNS.get(`${fqn}#${jm.name}`);
    if (entry !== undefined) return entry;
  }
  return undefined;
}

/**
 * Java's `Void` is `null` in the Void serdes (javascript/CLAUDE.md,
 * JavaScript Binding Conventions, Serialization), and only there.
 */
const VOID_IS_NULL = new Set([
  'org.apache.kafka.common.serialization.VoidSerializer',
  'org.apache.kafka.common.serialization.VoidDeserializer',
]);

/**
 * Java's `UUID` is its canonical text, a string, in the UUID serdes
 * (javascript/CLAUDE.md, JavaScript Binding Conventions, Serialization), and
 * only there.
 */
const UUID_IS_STRING = new Set([
  'org.apache.kafka.common.serialization.UUIDSerializer',
  'org.apache.kafka.common.serialization.UUIDDeserializer',
]);
/**
 * The Java interfaces whose bare value takes one class of src/ that
 * implements it, at the bare parameter only (javascript/CLAUDE.md, The
 * binding's notes, "TypeScript first"): TypeScript cannot tell another
 * implementation from a plain object, which the run time reads as the options
 * object, so the bare form takes a class `tsc` tells from a plain object by
 * its private fields, and any implementation goes by name.
 */
const BARE_CLASSES = new Map([
  [
    'Header',
    {
      ts: 'RecordHeader',
      reason: 'Headers.add(header) takes a RecordHeader bare, and any Header by name, add({ header })',
    },
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
    // The TypeScript name of each Java class src/ translates, by Java simple
    // name: a nested type's own, as its type's symbol names it, an export's
    // kept where the two share one.
    const tsNames = new Map();
    for (const { item, java } of mappedItems(ctx)) {
      if (!java || !java.cls) continue;
      const simple = java.cls.path[java.cls.path.length - 1];
      if (!item.outer) tsNames.set(simple, item.name);
      else if (!tsNames.has(simple)) tsNames.set(simple, item.node.name.text);
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
    // of its elements, and one returned is an array (javascript/CLAUDE.md,
    // JavaScript Binding Conventions, Types; the plan's §5, item 1).
    const elementOf = (t, iterable = !outward) => {
      if (checker.isArrayType(t) || checker.isTupleType(t)) return checker.getTypeArguments(t)[0];
      const n = symbolName(t);
      if (n === 'ReadonlyArray' || n === 'Array' || (iterable && n === 'Iterable'))
        return (t.aliasTypeArguments || checker.getTypeArguments(t))[0];
      return undefined;
    };
    const typeArgs = (t) => t.aliasTypeArguments || checker.getTypeArguments(t);
    // The [key, value] pair the JS conventions take beside each Header where
    // Java takes Headers or Iterable<Header> (javascript/CLAUDE.md,
    // JavaScript Binding Conventions, Types and Serialization):
    // `readonly [string, Uint8Array | null]`.
    const isHeaderPair = (p) => {
      if (!checker.isTupleType(p) || !p.target || !p.target.readonly) return false;
      const elements = checker.getTypeArguments(p);
      if (elements.length !== 2) return false;
      const [k, v] = elements;
      const vs = parts(v);
      const bytes = vs.filter((x) => !(x.flags & ts.TypeFlags.Null));
      return (
        (k.flags & ts.TypeFlags.String) !== 0 &&
        vs.some((x) => x.flags & ts.TypeFlags.Null) &&
        bytes.length > 0 &&
        bytes.every((x) => symbolName(x) === 'Uint8Array')
      );
    };
    // An element of a collection Java takes: a Header element, of Java's
    // Iterable<Header> or Header[], is a Header or such a pair, the pair
    // included.
    const matchesElement = (elem, e) => {
      if (outward || elem.name !== 'Header' || elem.dims !== 0) return matches(elem, e);
      const ps = parts(strip(e));
      if (ps.some((p) => matches(elem, p) === 'unmapped')) return 'unmapped';
      return (
        ps.some(isHeaderPair) && ps.some((p) => matches(elem, p) === true) && ps.every((p) => isHeaderPair(p) || matches(elem, p) === true)
      );
    };

    /** Whether TS type `t` (null stripped) is what Java type `j` maps to: true, false, or 'unmapped'. */
    const matches = (j, t0) => {
      const t = strip(t0);
      if (j.dims > 0) {
        // Java's byte[] is bytes only (a header value, Header#value): a
        // string is no byte[], unlike a key or value (K / V, Data). The
        // caller passes a Uint8Array (a Buffer is one), and the client
        // returns a Buffer (javascript/CLAUDE.md, JavaScript Binding Conventions,
        // Types).
        if (j.name === 'byte' && j.dims === 1) {
          return parts(t).every((p) => (outward ? ['Buffer'] : ['Buffer', 'Uint8Array']).includes(symbolName(p)));
        }
        const e = elementOf(t);
        return e === undefined ? false : matchesElement({ ...j, dims: j.dims - 1 }, e);
      }
      // Java's ByteBuffer, which only a serde takes (a deserializer's
      // ByteBuffer form): a Buffer, as byte[] is.
      if (j.name === 'ByteBuffer') return parts(t).every((p) => (outward ? ['Buffer'] : ['Buffer', 'Uint8Array']).includes(symbolName(p)));
      // Java's Void in the Void serdes: null, and nothing else.
      if (voidIsNull && (j.name === 'Void' || j.name === 'void')) return parts(t0).every((p) => (p.flags & ts.TypeFlags.Null) !== 0);
      if (uuidIsString && j.name === 'UUID') return (t.flags & ts.TypeFlags.StringLike) !== 0;
      if (NUMBERS.has(j.name)) return (t.flags & ts.TypeFlags.NumberLike) !== 0;
      if (LONGS.has(j.name)) {
        // Java's long: a bigint returned; taken, a bigint or a whole number
        // (javascript/CLAUDE.md, JavaScript Binding Conventions, Types).
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
      // An error the user gives the client (MockProducer's error fields,
      // errorNext): a JavaScript Error.
      if (j.name === 'RuntimeException' || j.name === 'Exception') return symbolName(t) === 'Error';
      // The types the client does not translate, and those Java does not make
      // public, typed unknown (javascript/CLAUDE.md, JavaScript Binding
      // Conventions, Scope). A placeholder is an alias of unknown.
      if (PLACEHOLDERS.has(j.name) || NOT_PUBLIC_TYPES.has(j.name)) return (t.flags & (ts.TypeFlags.Unknown | ts.TypeFlags.Any)) !== 0;
      if (j.name === '?') return true;
      // Java's Optional<T> is `T | null` (javascript/CLAUDE.md, JavaScript Binding
      // Conventions, Types): null is stripped, T compared.
      if (j.name === 'Optional') return j.args.length === 0 ? true : matches(j.args[0], t);
      if (j.name === 'OptionalInt') return (t.flags & ts.TypeFlags.NumberLike) !== 0;
      if (j.name === 'OptionalLong') return (t.flags & ts.TypeFlags.BigIntLike) !== 0;
      if (j.name === 'Future' || j.name === 'KafkaFuture') {
        return symbolName(t) === 'KafkaFuture' && (j.args.length === 0 || matches(j.args[0], checker.getTypeArguments(t)[0]) === true);
      }
      if (j.name === 'Set' && outward && j.args[0] !== undefined && j.args[0].name === 'TopicPartition') {
        // A set of TopicPartition returned is a TopicPartitionSet, whose
        // members compare by value (javascript/CLAUDE.md, JavaScript Binding
        // Conventions, Types).
        return symbolName(t) === 'TopicPartitionSet';
      }
      if (j.name === 'Set' && outward && ['ReadonlySet', 'Set'].includes(symbolName(t))) {
        // A Set returned is a Set; a constant one a read-only set
        // (javascript/CLAUDE.md, JavaScript Binding Conventions, Names).
        const e = typeArgs(t)[0];
        const elem = j.args[0];
        return elem === undefined || e === undefined ? true : matches(elem, e);
      }
      if (j.name === 'Map' && j.args.length === 2 && j.args[0].name === 'TopicPartition') {
        return symbolName(t) === 'TopicPartitionMap' && matches(j.args[1], typeArgs(t)[0]) === true;
      }
      if (j.name === 'Map' && j.args.length === 2 && j.args[0].name === 'String' && j.args[0].dims === 0 && j.args[1].name !== 'Object') {
        // A map keyed by strings: a Map returned; taken, a Map, a ReadonlyMap
        // or a plain object of string to its values (javascript/CLAUDE.md,
        // JavaScript Binding Conventions, Types, "Other collections taken").
        if ((t0.aliasSymbol && t0.aliasSymbol.name.endsWith('Configs')) || symbolName(t).endsWith('Configs')) return true;
        const part = (p) => {
          const n = symbolName(p);
          if (n === 'Map' || (!outward && n === 'ReadonlyMap')) {
            const [k, v] = typeArgs(p);
            return matches(j.args[0], k) === true && matches(j.args[1], v) === true;
          }
          if (outward) return false;
          const index = checker.getIndexInfoOfType(p, ts.IndexKind.String);
          return index !== undefined && matches(j.args[1], index.type) === true;
        };
        return parts(t).every(part);
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
        return elem === undefined ? true : matchesElement(elem, e);
      }
      if (j.name === 'Map' || j.name === 'Properties') {
        const configsType = (x) => ((x.aliasSymbol && x.aliasSymbol.name) || symbolName(x)).endsWith('Configs');
        // The configuration type, or it intersected with the serde config
        // keys that bind the client's type arguments (`ProducerConfigs &
        // SerdeConfigs<K, V>`).
        if (configsType(t0)) return true;
        if (t.flags & ts.TypeFlags.Intersection) return t.types.some(configsType) ? true : 'unmapped';
        return configsType(t) ? true : 'unmapped';
      }
      if (/^[A-Z]$/.test(j.name)) return (t.flags & ts.TypeFlags.TypeParameter) !== 0;
      if (j.name === 'Headers' && !outward && j.dims === 0) {
        // Java's Headers taken: the Headers itself, or an iterable of Header
        // objects and [key, value] pairs.
        const header = { name: 'Header', args: [], dims: 0 };
        return parts(t).every((p) => {
          if (symbolName(p) === tsNames.get('Headers')) return true;
          const e = elementOf(p);
          return e !== undefined && matchesElement(header, e) === true;
        });
      }
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
    // Whether Java's Void is null (VOID_IS_NULL), and its UUID a string
    // (UUID_IS_STRING), for the member being checked.
    let voidIsNull = false;
    let uuidIsString = false;
    const waiting = waitingMembers(ctx);
    // Where the member being checked is declared, for its findings.
    let at = null;
    /**
     * Checks TS type `tsType` against Java type(s) `javaTypes` (several: the
     * forms naming one option, their union); `promise` unwraps a Promise
     * return of a waiting member.
     */
    const check = (subject, where, javaTypes, tsType, { promise = false, awaited = false, taken = false } = {}) => {
      const given = (Array.isArray(javaTypes) ? javaTypes : [javaTypes]).filter((x) => x);
      // A type variable beside a concrete type, a serde class's own type and
      // the interface's `T` it binds (LongSerializer's `Long | T`), is that
      // type.
      const list = given.some((x) => !/^[A-Z]$/.test(x)) ? given.filter((x) => !/^[A-Z]$/.test(x)) : given;
      if (list.length === 0) return;
      checked += 1;
      // A return the user's implementation gives the client
      // (AWAITED_RETURNS, `taken`) is taken, not returned.
      outward = (where === 'type' || where === 'return') && !taken;
      const js = [...new Map(list.map((x) => [x, parseJava(x)])).values()];
      let t = tsType;
      if (promise) {
        const s = strip(t);
        if (symbolName(s) === 'Promise') t = checker.getTypeArguments(s)[0] || (s.aliasTypeArguments || [])[0] || t;
      }
      let ok;
      if (awaited && js.length === 1) {
        // Each part as itself, a Promise as what it resolves to; null is
        // dropped part by part, as a type variable stays itself (strip()
        // would read `T | Promise<T> | null` as `(T & {}) | Promise<T>`).
        const ps = parts(t)
          .filter((p) => !(p.flags & (ts.TypeFlags.Null | ts.TypeFlags.Undefined)))
          .map((p) => (symbolName(p) === 'Promise' ? checker.getTypeArguments(p)[0] || p : p));
        // Nothing but null or undefined: the type as it is (a Void serde's null).
        const each = ps.length === 0 ? [matches(js[0], t)] : ps.map((p) => matches(js[0], p));
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
          ? {
              at,
              kind: 'unmapped',
              subject,
              message: `${where}: Java ${java} has no row in the table (TypeScript ${checker.typeToString(tsType)})`,
            }
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
      voidIsNull = VOID_IS_NULL.has(jm.owner.fqn);
      uuidIsString = UUID_IS_STRING.has(jm.owner.fqn);
      if (m.kind === 'property' || m.kind === 'get') {
        const javaType = jm.kind === 'field' ? jm.field.type : (liveForms(jm.overloads)[0] || jm.overloads[0]).return_type;
        const tsType =
          m.kind === 'get'
            ? checker.getReturnTypeOfSignature(checker.getSignatureFromDeclaration(m.node))
            : checker.getTypeAtLocation(m.node);
        check(subject, 'type', javaType, tsType);
        continue;
      }
      if (jm.kind === 'field') continue;
      // A form the JS conventions leave out is not asked for (overload_shape.js LEFT_OUT).
      const forms = liveForms(jm.overloads).filter((f) => !LEFT_OUT.has(`${jm.owner.fqn}#${formKey(f)}`));
      const names = formOptionNames(forms);
      // The bare values' names: the one parameter of each one-parameter form
      // (several names told apart by type, src/internal/args.ts `bareTypes`).
      const bareNames = [...new Set(forms.filter((f) => f.params.length === 1).map((f) => names[forms.indexOf(f)][0]))];
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
            (waiting.has(subject) || PURE_JS_WAITING.has(`${jm.owner.fqn}#${jm.name}`)) &&
            parseJava(javaReturn || 'void').name !== 'CompletionStage';
          const entry = awaitedReturn(jm);
          if (sig) {
            check(where, 'return', javaReturn, checker.getReturnTypeOfSignature(sig), {
              promise: blocking,
              awaited: entry !== undefined,
              taken: entry !== undefined && entry.taken,
            });
          }
        }
        const p0 = params.length === 1 ? params[0] : null;
        const bareName = p0 !== null && ts.isIdentifier(p0.name) && bareNames.includes(p0.name.text) ? p0.name.text : null;
        if (bareName !== null) {
          // The bare value of the one-parameter forms: against their Java types.
          const types = forms.filter((f, k) => f.params.length === 1 && names[k][0] === bareName).map((f) => f.types[0]);
          const bare = unwrapBare(checker.getTypeAtLocation(p0));
          // A Java interface whose bare value takes its class (BARE_CLASSES).
          const narrowed = types.length === 1 ? BARE_CLASSES.get(parseJava(types[0]).name) : undefined;
          if (narrowed === undefined || symbolName(strip(bare)) !== narrowed.ts) check(where, `parameter ${bareName}`, types, bare);
          // The name of the form the bare value translates, not of the first one-parameter form.
          const bareForm = forms.find((f, k) => f.params.length === 1 && names[k][0] === bareName);
          durationName(where, p0, bareForm.types[0], bareForm.param_names[0]);
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
          // An options object (check-overload-shape checks its names): each
          // option against the union of the Java types the forms naming it
          // give; a name closed with `never` is no option of this signature.
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
