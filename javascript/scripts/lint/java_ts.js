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

// How the TypeScript library maps to the Java client
// (design/current/javascript-rule-linter/PLAN.md §3.4), shared by the J rules.
// JavaScript keeps Java's names, so the mapping is derived, not marked:
//   - a public module `src/<dir>/<file>.ts` exports types of the Java package
//     of `<dir>` (`producer` -> `clients.producer`, `common/config` ->
//     `common.config`; the root is Java's `java.*`, which the index does not
//     hold), each named as its Java class (an `...Error` class for a Java
//     `...Exception`);
//   - a method has its Java method's name; a property `x` is Java's getter
//     `x()`, `getX()`, `isX()` or `hasX()` (no parameters), or Java's field
//     `x`; a `static readonly X` is Java's `public static final X`; the
//     constructor is Java's constructors.
// Two lists of lint-mapping.json cover the exceptions, not TSDoc tags (the
// root CLAUDE.md's JavaScript Binding Conventions allow only `@param`,
// `@returns`, `@throws`, `@defaultValue`, `@deprecated`, `@remarks` and, for
// library-only members, `@internal`): `invented`, the list of invented names,
// and `javaTargets`, where the convention does not find the Java one
// (check-invented-names reports an entry that matches nothing, is not needed
// or does not resolve).
//
// The error classes follow the generated hierarchy instead
// (javascript/CLAUDE.md "Errors"): one class per Java exception the FFI enum
// names, listed with its Java class in src/internal/error_registry.ts.

const { SCHEMA } = require('./java_index');
const path = require('node:path');

const PREFIX = 'org.apache.kafka.';

/**
 * The error classes javascript/CLAUDE.md "Errors" describes, which the Java
 * members rules do not map member by member: what they keep on every class
 * instead of Java's members (`getMessage()` and `getCause()` as `message` and
 * `cause`; the payload getters are not generated), and Java's `public static
 * final` instances (`INSTANCE`), which check-java-members asks of the Java
 * class.
 * No other public member: the FFI id is private, and an error is classified
 * with `instanceof`, so there is no `code`, `retriable` or `apiError`.
 */
const ERROR_ALLOWANCE = {
  members: new Set(['constructor', 'name', 'message', 'cause']),
  reason: 'an error class keeps the fields javascript/CLAUDE.md "Errors" lists (classify with instanceof), not Java\'s members',
};

/**
 * The client config classes, translated for their constants only
 * (javascript/CLAUDE.md "Generated files"; root CLAUDE.md, JavaScript Binding
 * Conventions, Scope, Q55, as the Python client's Scope: their `ConfigDef`
 * machinery is not translated). The key-name classes (`SslConfigs`,
 * `SaslConfigs`, `SecurityConfig`) are translated in full, but a method that
 * takes a `ConfigDef`.
 */
const CONSTANTS_ONLY = new Set([
  'org.apache.kafka.clients.producer.ProducerConfig',
  'org.apache.kafka.clients.consumer.ConsumerConfig',
  'org.apache.kafka.clients.admin.AdminClientConfig',
]);

/**
 * The data classes (javascript/CLAUDE.md "Java shape, JS idiom"): the classes
 * whose Java getters are public `readonly` fields set once in the
 * constructor, so spread, `JSON.stringify` and `structuredClone` keep them.
 * A class belongs here when reading a getter twice always gives the same
 * answer, costs no work and cannot fail; a client, a future and every other
 * class keep Java's getters as methods. check-data-objects enforces both.
 * The list is the root CLAUDE.md's (JavaScript Binding Conventions, Class
 * family: the records, `TopicPartition`, `OffsetAndMetadata`,
 * `RecordMetadata`, `Node` and the admin description classes), limited to the
 * classes the client translates: `Header` is not on it, so it keeps Java's
 * `key()` and `value()`.
 */
const DATA_CLASSES = new Set([
  'org.apache.kafka.clients.producer.ProducerRecord',
  'org.apache.kafka.clients.producer.RecordMetadata',
]);

/**
 * The Java functional interfaces translated as a function type
 * (javascript/CLAUDE.md "Java shape, JS idiom": `Callback` is a function):
 * mapped, but their members are not.
 */
const FUNCTION_TYPES = new Set(['org.apache.kafka.clients.producer.Callback']);

/** The members `java.lang.Enum` gives every Java enum. */
const ENUM_IMPLICIT = new Set(['values', 'valueOf', 'name', 'ordinal']);

/** `name` as snake_case, acronyms kept together (`KafkaProducer` -> `kafka_producer`). */
function snakeCase(name) {
  return name
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1_$2')
    .toLowerCase();
}

/** The Java index (the committed java-index.json, scripts/lint/java_index.js), with lookups. */
class JavaIndex {
  constructor(json) {
    if (json.schema !== SCHEMA) throw new Error(`the Java index has schema ${json.schema}, not ${SCHEMA}`);
    if (!Array.isArray(json.classes) || json.classes.length === 0) throw new Error('the Java index lists no class');
    this.classes = json.classes;
    this.byFqn = new Map(json.classes.map((c) => [c.fqn, c]));
    this.packages = new Set(json.classes.map((c) => c.package));
  }

  /** The class of `fqn` (`org.apache.kafka.<package>.<Outer>$<Inner>`, `.` accepted for `$`), or undefined. */
  resolve(fqn) {
    if (this.byFqn.has(fqn)) return this.byFqn.get(fqn);
    const rest = fqn.startsWith(PREFIX) ? fqn.slice(PREFIX.length) : null;
    if (rest === null) return undefined;
    const parts = rest.split('.');
    for (let i = parts.length - 1; i > 0; i--) {
      const candidate = `${PREFIX}${parts.slice(0, i).join('.')}.${parts.slice(i).join('$')}`;
      if (this.byFqn.has(candidate)) return this.byFqn.get(candidate);
    }
    return undefined;
  }

  /** The Java package of src/ directory `dir` (`producer` -> `clients.producer`), or null for the root. */
  packageOf(dir) {
    if (dir === '') return null;
    const dotted = dir.split('/').join('.');
    return this.packages.has(`clients.${dotted}`) ? `clients.${dotted}` : dotted;
  }

  /** The class supertype `written` of `cls` names: same package, qualified, or the only class of that name. */
  supertype(cls, written) {
    const base = written.replace(/<.*$/, '');
    const local = this.byFqn.get(`${PREFIX}${cls.package}.${base.split('.').join('$')}`);
    if (local) return local;
    const qualified = this.resolve(base.startsWith(PREFIX) ? base : `${PREFIX}${base}`);
    if (qualified) return qualified;
    const simple = base.split('.').pop();
    const named = this.classes.filter((c) => c.path[c.path.length - 1] === simple);
    return named.length === 1 ? named[0] : undefined;
  }

  /** `cls` and the classes of its supertype tree the index holds, `cls` first, each once. */
  chain(cls) {
    const out = [];
    const seen = new Set();
    const work = [cls];
    while (work.length > 0) {
      const c = work.shift();
      if (seen.has(c.fqn)) continue;
      seen.add(c.fqn);
      out.push(c);
      for (const s of c.supertypes) {
        const sup = this.supertype(c, s);
        if (sup) work.push(sup);
      }
    }
    return out;
  }
}

/** The class's name as the lint prints it: its fqn below `org.apache.kafka.`. */
function shortName(cls) {
  return cls.fqn.startsWith(PREFIX) ? cls.fqn.slice(PREFIX.length) : cls.fqn;
}

/** The file of the mapping lists, at the client's root. */
const MAPPING_FILE = 'lint-mapping.json';

/**
 * The mapping lists of `ctx`'s client, read once from `lint-mapping.json`
 * (both empty when the file is missing): `{invented, javaTargets}`, each a
 * `Map` from a key to its text, the order of the file kept.
 *   - `invented`: the names JavaScript invents (root CLAUDE.md, JavaScript
 *     Binding Conventions, Scope and "Layout and modules": every addition
 *     Java lacks is *(js-only)* and on the list of invented names, which is
 *     kept once released), for the exported items and public members the
 *     lint reads, each with the reason it has no Java counterpart. An
 *     invented options type is named `...Args` (check-invented-names).
 *   - `javaTargets`: the Java class, or member, of an item the convention
 *     does not find, `<fqn>` or `<fqn>#<member>`.
 * An export is keyed `src/<module>.ts:<Name>`, a member `<Class>.<member>`; a
 * member with a computed name (`[Symbol.asyncDispose]`) is not read, so not
 * listed.
 */
function mappingLists(ctx) {
  return ctx.once('mappingLists', () => {
    let text;
    try {
      text = ctx.read(MAPPING_FILE);
    } catch (e) {
      if (e.code === 'ENOENT') return { invented: new Map(), javaTargets: new Map() };
      throw e;
    }
    const json = JSON.parse(text);
    return {
      invented: new Map(Object.entries(json.invented || {})),
      javaTargets: new Map(Object.entries(json.javaTargets || {})),
    };
  });
}

/**
 * The mapping of the item or member keyed `key` (declared at `node`):
 * `{jsOnly, java}`, each `{text, node}` or null, `text` the reason or the
 * Java name, from the lists of `mappingLists`.
 */
function mappingOf(ctx, key, node) {
  const { invented, javaTargets } = mappingLists(ctx);
  return {
    jsOnly: invented.has(key) ? { text: invented.get(key), node } : null,
    java: javaTargets.has(key) ? { text: javaTargets.get(key), node } : null,
  };
}

/** The key of exported item `item` in the lists: `src/<module>.ts:<Name>`. */
function itemKey(item) {
  return `${item.rel}:${item.name}`;
}

/** The key of public member `m` of exported item `item` in the lists: `<Class>.<member>`. */
function memberKey(item, m) {
  return `${item.name}.${m.name}`;
}

/** The mapping of public member `m` of exported item `item` (`mappingOf`). */
function memberTags(ctx, item, m) {
  return mappingOf(ctx, memberKey(item, m), m.node);
}

/** Whether declaration `node`'s TSDoc carries `@internal`. */
function isInternal(ts, node) {
  return ts.getJSDocTags(node).some((t) => t.tagName.text === 'internal');
}

/** Whether `node` has modifier flag `flag`. */
function hasFlag(ts, node, flag) {
  return (ts.getCombinedModifierFlags(node) & flag) !== 0;
}

/**
 * The public members of class declaration `cls`: `{name, kind, isStatic,
 * isReadonly, node}`, `kind` one of `constructor`, `method`, `get`, `set` and
 * `property`. A `private` / `protected` member, an ES `#` member, a `_`
 * member and an `@internal` one (the library's own, which `stripInternal`
 * leaves out of the declarations) are not public.
 */
function publicMembers(ts, cls) {
  const out = [];
  for (const m of cls.members) {
    if (hasFlag(ts, m, ts.ModifierFlags.Private | ts.ModifierFlags.Protected)) continue;
    if (isInternal(ts, m)) continue;
    const isStatic = hasFlag(ts, m, ts.ModifierFlags.Static);
    const isReadonly = hasFlag(ts, m, ts.ModifierFlags.Readonly);
    if (ts.isConstructorDeclaration(m)) {
      // Overload signatures stand for the constructor: the first one (whose
      // TSDoc the declarations keep; an implementation's is not emitted), the
      // implementation only when there are none. The parameter properties are
      // the implementation's.
      const impl = m.body !== undefined;
      const overloads = cls.members.filter((o) => ts.isConstructorDeclaration(o) && o.body === undefined);
      if (overloads.length > 0 ? m !== overloads[0] && !impl : false) continue;
      if (overloads.length === 0 || m === overloads[0]) {
        out.push({ name: 'constructor', kind: 'constructor', isStatic: false, isReadonly: false, node: m });
      }
      for (const p of m.parameters) {
        const property = ts.getEffectiveModifierFlags(p) & ts.ModifierFlags.ParameterPropertyModifier;
        const hidden = hasFlag(ts, p, ts.ModifierFlags.Private | ts.ModifierFlags.Protected);
        if (property && !hidden && ts.isIdentifier(p.name) && !p.name.text.startsWith('_')) {
          out.push({
            name: p.name.text,
            kind: 'property',
            isStatic: false,
            isReadonly: hasFlag(ts, p, ts.ModifierFlags.Readonly),
            node: p,
          });
        }
      }
      continue;
    }
    if (!m.name || ts.isPrivateIdentifier(m.name)) continue;
    const name = ts.isIdentifier(m.name) || ts.isStringLiteral(m.name) ? m.name.text : null;
    if (name === null || name.startsWith('_')) continue;
    let kind = null;
    if (ts.isMethodDeclaration(m)) kind = 'method';
    else if (ts.isGetAccessorDeclaration(m)) kind = 'get';
    else if (ts.isSetAccessorDeclaration(m)) kind = 'set';
    else if (ts.isPropertyDeclaration(m)) kind = 'property';
    if (kind === null) continue;
    // An overload signature of a method stands with its implementation.
    if (kind === 'method' && m.body === undefined && out.some((o) => o.name === name && o.kind === 'method')) continue;
    out.push({ name, kind, isStatic, isReadonly, node: m });
  }
  // One entry per method name (TypeScript overload signatures are one function).
  const seen = new Set();
  return out.filter((m) => {
    const k = `${m.kind}:${m.isStatic}:${m.name}`;
    if (m.kind !== 'method') return true;
    if (seen.has(k)) return false;
    seen.add(k);
    return true;
  });
}

/**
 * The signatures of the function `node` is the first declaration of: its
 * overload signatures (an interface's method signatures), or the
 * implementation when it has none.
 */
function signaturesOf(ts, node) {
  const members = node.parent && node.parent.members ? [...node.parent.members] : [node];
  const same = members.filter((o) =>
    ts.isConstructorDeclaration(node)
      ? ts.isConstructorDeclaration(o)
      : (ts.isMethodDeclaration(o) || ts.isMethodSignature(o)) && o.name && node.name && o.name.getText() === node.name.getText() && (ts.getCombinedModifierFlags(o) & ts.ModifierFlags.Static) === (ts.getCombinedModifierFlags(node) & ts.ModifierFlags.Static),
  );
  const overloads = same.filter((o) => o.body === undefined);
  return overloads.length > 0 ? overloads : same.length > 0 ? same : [node];
}

/**
 * The parameters of signature `sig` that translate Java's: all of them but a
 * trailing rest parameter typed `BareCheck<...>`, the type-level check a bare-value
 * overload makes of its value's type (KafkaFuture's
 * `completedFuture(value, ...check: BareCheck<U>)`), which no call gives.
 */
function formParameters(ts, sig) {
  const ps = [...(sig.parameters || [])];
  const last = ps[ps.length - 1];
  // Only the type-level check: a rest parameter typed `BareCheck<...>`, whatever
  // its name; any other trailing rest parameter, a Java parameter named `check`
  // included, is compared as written.
  if (
    last &&
    last.dotDotDotToken &&
    last.type &&
    ts.isTypeReferenceNode(last.type) &&
    ts.isIdentifier(last.type.typeName) &&
    last.type.typeName.text === 'BareCheck'
  ) {
    ps.pop();
  }
  return ps;
}

/** `getX` -> `x`; null when `name` is not `get` + an upper-case letter. */
function unGet(name) {
  return /^get[A-Z]/.test(name) ? name[3].toLowerCase() + name.slice(4) : null;
}

/** The member `java.lang.Enum` gives every Java enum that the index does not list: `toString()`. */
const ENUM_TO_STRING = {
  kind: 'methods',
  name: 'toString',
  owner: {
    fqn: 'java.lang.Enum',
    package: 'java.lang',
    path: ['Enum'],
    deprecated: false,
  },
  overloads: [{ name: 'toString', params: [], visibility: 'public', static: false, deprecated: false }],
};

/**
 * The Java member TypeScript member `m` maps to in `cls`'s supertype chain:
 * `{kind: 'methods' | 'field' | 'constructors', name, owner, overloads?,
 * field?}`, or null. A member maps only to Java members of its own
 * static-ness (`anyStatic` lifts that, to tell a mismatch from an absence).
 * A method maps by name; a property `x` to Java's getter `x()` or `getX()`,
 * then to a field `x` (an `isX` / `hasX` getter keeps its name:
 * javascript/CLAUDE.md "`is` / `has` stay"); a static property to a field,
 * then to a zero-argument static method. On an enum, `toString` is
 * `java.lang.Enum`'s when the enum declares none.
 */
function javaMember(index, cls, m, { anyStatic = false } = {}) {
  const chain = index.chain(cls);
  const fits = (isStatic) => anyStatic || isStatic === m.isStatic;
  const methods = (name, arity) => {
    for (const c of chain) {
      const found = c.overloads.filter(
        (o) =>
          o.name === name &&
          o.name !== c.path[c.path.length - 1] &&
          (arity === undefined || o.params.length === arity) &&
          fits(o.static),
      );
      if (found.length > 0) return { kind: 'methods', name, owner: c, overloads: found };
    }
    return null;
  };
  const field = (name) => {
    for (const c of chain) {
      const f = c.fields.find((x) => x.name === name && fits(x.static));
      if (f) return { kind: 'field', name, owner: c, field: f };
    }
    return null;
  };
  if (m.kind === 'constructor') {
    const own = cls.overloads.filter((o) => o.name === cls.path[cls.path.length - 1]);
    return own.length > 0 ? { kind: 'constructors', name: cls.path[cls.path.length - 1], owner: cls, overloads: own } : null;
  }
  if (m.kind === 'method') {
    const found = methods(m.name);
    if (found) return found;
    return cls.kind === 'enum' && m.name === 'toString' && fits(false) ? ENUM_TO_STRING : null;
  }
  if (m.isStatic) return field(m.name) || methods(m.name, 0);
  const upper = m.name[0].toUpperCase() + m.name.slice(1);
  for (const getter of [m.name, `get${upper}`]) {
    const found = methods(getter, 0);
    if (found) return found;
  }
  return field(m.name);
}

/**
 * The members of an exported interface, or of a type alias of an object
 * type, in the shape `publicMembers` gives a class's: property and method
 * signatures (never static). Any other type alias has none.
 */
function shapeMembers(ts, node) {
  let elements = null;
  if (ts.isInterfaceDeclaration(node)) elements = node.members;
  else if (ts.isTypeAliasDeclaration(node) && ts.isTypeLiteralNode(node.type)) elements = node.type.members;
  if (elements === null) return [];
  const out = [];
  for (const e of elements) {
    if (!e.name || !(ts.isIdentifier(e.name) || ts.isStringLiteral(e.name))) continue;
    const name = e.name.text;
    if (name.startsWith('_')) continue;
    if (ts.isPropertySignature(e)) {
      out.push({ name, kind: 'property', isStatic: false, isReadonly: hasFlag(ts, e, ts.ModifierFlags.Readonly), node: e });
    } else if (ts.isMethodSignature(e) && !out.some((o) => o.name === name && o.kind === 'method')) {
      out.push({ name, kind: 'method', isStatic: false, isReadonly: false, node: e });
    }
  }
  return out;
}

/** The public members of exported item `item`: a class's, or an interface's or object type's. */
function itemMembers(ts, item) {
  return item.kind === 'class' ? publicMembers(ts, item.node) : shapeMembers(ts, item.node);
}

/** The kind of export declaration `d`, or null for one the rules do not read. */
function declarationKind(ts, d) {
  if (ts.isClassDeclaration(d)) return 'class';
  if (ts.isInterfaceDeclaration(d)) return 'interface';
  if (ts.isTypeAliasDeclaration(d)) return 'type';
  if (ts.isFunctionDeclaration(d)) return 'function';
  if (ts.isVariableDeclaration(d)) return 'variable';
  if (ts.isEnumDeclaration(d)) return 'enum';
  if (ts.isModuleDeclaration(d)) return 'namespace';
  return null;
}

/**
 * The exported items of the public modules of src/, as the type checker
 * lists each module's exports (an `export` declaration, an
 * `export { X }` / `export { X } from` re-export, an `export declare` one):
 * `{rel, dir, sf, name, node, kind, tags}` (`tags`: the lists' mapping,
 * `mappingOf`), `kind` one of `class`,
 * `interface`, `type`, `function`, `variable`, `enum` and `namespace`. A
 * public module is a src/ file outside src/internal/, other
 * than an `index.ts`. An exported symbol is one item (an overloaded
 * function's signatures included), listed under the public module that declares it when that
 * module exports it, else under the first public module exporting it.
 */
function exportedItems(ctx) {
  return ctx.once('exportedItems', () => {
    const { ts, checker, sourceFiles } = ctx.program();
    const modules = [];
    for (const sf of sourceFiles) {
      const rel = ctx.rel(sf.fileName);
      const inLib = rel.slice('src/'.length);
      if (inLib.split('/')[0] === 'internal' || path.posix.basename(rel) === 'index.ts') continue;
      const dir = path.posix.dirname(inLib) === '.' ? '' : path.posix.dirname(inLib);
      const symbol = checker.getSymbolAtLocation(sf);
      const exports = symbol ? checker.getExportsOfModule(symbol) : [];
      modules.push({ rel, dir, sf, exports });
    }
    // One item per exported symbol: its declarations (a function's overload
    // signatures and implementation) are one item, its mapping read from the
    // lists by the module and name it is listed under.
    const bySymbol = new Map();
    for (const { rel, dir, sf, exports } of modules) {
      for (const exported of exports) {
        const target = exported.flags & ts.SymbolFlags.Alias ? checker.getAliasedSymbol(exported) : exported;
        const declarations = (target.declarations || []).filter((d) => declarationKind(ts, d) !== null);
        if (declarations.length === 0) continue;
        const declaredHere = declarations.some((d) => d.getSourceFile() === sf);
        const known = bySymbol.get(target);
        if (known && (known.declaredHere || !declaredHere)) continue;
        const node = declarations.find((d) => !ts.isFunctionDeclaration(d) || d.body) || declarations[0];
        const kind = declarationKind(ts, node);
        const tags = mappingOf(ctx, `${rel}:${exported.name}`, node);
        bySymbol.set(target, { rel, dir, sf, name: exported.name, node, kind, tags, declaredHere });
      }
    }
    return [...bySymbol.values()].sort((a, b) =>
      a.rel < b.rel ? -1 : a.rel > b.rel ? 1 : a.node.getStart() - b.node.getStart(),
    );
  });
}

/** The rows of src/internal/error_registry.ts: a map from `src/<module>.ts` to `{name, java}`. */
function errorRegistry(ctx) {
  return ctx.once('errorRegistry', () => {
    const out = new Map();
    let text;
    try {
      text = ctx.read('src/internal/error_registry.ts');
    } catch (e) {
      if (e.code === 'ENOENT') return out;
      throw e;
    }
    for (const m of text.matchAll(/\[\s*'\.\.\/([\w/]+)',\s*'(\w+)',\s*'([\w.$]+)'\s*\]/g)) {
      out.set(`src/${m[1]}.ts`, { name: m[2], java: m[3] });
    }
    return out;
  });
}

/**
 * The Java class exported item `item` maps to by the convention: `{fqn, cls,
 * via}`, `cls` the index's class or undefined (a JDK class, or none), `via`
 * `registry` for an error class, `name` otherwise; null when the convention
 * names no Java class the index holds and no registry row covers it.
 */
function conventionClass(ctx, index, item) {
  const row = errorRegistry(ctx).get(item.rel);
  if (row && row.name === item.name) {
    return { fqn: row.java, cls: index.resolve(row.java), via: 'registry' };
  }
  const pkg = index.packageOf(item.dir);
  if (pkg === null) return null;
  const names = [item.name];
  if (item.name.endsWith('Error')) names.push(`${item.name.slice(0, -'Error'.length)}Exception`);
  for (const n of names) {
    const cls = index.byFqn.get(`${PREFIX}${pkg}.${n}`);
    if (cls) return { fqn: cls.fqn, cls, via: 'name' };
  }
  return null;
}

/**
 * The Java class of every exported class, interface and type alias, as the
 * J rules see it: `{item, java, isError, jsOnly}`, `java` the convention's
 * class or `javaTargets`' (`{fqn, cls, via: 'tag'}`), null when neither
 * finds one.
 */
function mappedItems(ctx) {
  return ctx.once('mappedItems', () => {
    const index = javaIndexOf(ctx);
    return exportedItems(ctx)
      .filter((item) => item.kind === 'class' || item.kind === 'interface' || item.kind === 'type')
      .map((item) => {
        const convention = conventionClass(ctx, index, item);
        let java = convention;
        if (item.tags.java) {
          const target = item.tags.java.text.split('#')[0];
          const cls = index.resolve(target);
          java = cls ? { fqn: cls.fqn, cls, via: 'tag' } : convention;
        }
        return { item, java, convention, isError: convention !== null && convention.via === 'registry' };
      });
  });
}

/** The Java index of `ctx`, built once. */
function javaIndexOf(ctx) {
  return ctx.once('javaIndex', () => new JavaIndex(ctx.javaIndex()));
}

/** Whether Java class `cls` is a config class translated for its constants only. */
function constantsOnly(cls) {
  return CONSTANTS_ONLY.has(cls.fqn);
}

/** Whether Java class `cls` is translated as a function type. */
function functionType(cls) {
  return FUNCTION_TYPES.has(cls.fqn);
}

module.exports = {
  PREFIX,
  ERROR_ALLOWANCE,
  DATA_CLASSES,
  ENUM_IMPLICIT,
  JavaIndex,
  javaIndexOf,
  snakeCase,
  shortName,
  MAPPING_FILE,
  mappingLists,
  mappingOf,
  itemKey,
  memberKey,
  memberTags,
  publicMembers,
  isInternal,
  signaturesOf,
  formParameters,
  shapeMembers,
  itemMembers,
  javaMember,
  unGet,
  exportedItems,
  errorRegistry,
  conventionClass,
  mappedItems,
  constantsOnly,
  functionType,
};
