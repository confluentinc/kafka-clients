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

// A hand-written Java index (java-index.json, schema 2, as `cargo xtask
// java-api` writes it, but every key given) for the Java rules' fixture
// projects.

const PREFIX = 'org.apache.kafka.';

/** An overload: `name` with parameter types `params`, public unless told. */
function method(name, params = [], extra = {}) {
  return {
    name,
    params,
    param_names: params.map((_, i) => `p${i}`),
    // As cargo xtask java-api writes them: `params` erased, `param_types`
    // as written (a test giving generic types sets both).
    param_types: params,
    // The type the tests of the type rule give it; none (as a constructor has) otherwise.
    return_type: null,
    visibility: 'public',
    static: false,
    default: false,
    deprecated: false,
    javadoc: null,
    ...extra,
  };
}

/** A field, public static final unless told. */
function field(name, extra = {}) {
  return {
    name,
    type: 'String',
    visibility: 'public',
    static: true,
    final: true,
    deprecated: false,
    javadoc: null,
    ...extra,
  };
}

/** A class of `<package>.<Name>` (a nested one as `Outer$Inner`), public and Public unless told. */
function javaClass(name, extra = {}) {
  const [pkg, cls] = [name.slice(0, name.lastIndexOf('.')), name.slice(name.lastIndexOf('.') + 1)];
  return {
    fqn: `${PREFIX}${name}`,
    package: pkg,
    path: cls.split('$'),
    kind: 'class',
    visibility: 'public',
    abstract: false,
    functional_interface: false,
    supertypes: [],
    public_audience: true,
    unsupported_package: false,
    deprecated: false,
    javadoc: null,
    overloads: [],
    fields: [],
    enum_constants: [],
    ...extra,
  };
}

/** The index file's text for `classes`. */
function javaIndex(classes) {
  return JSON.stringify({ schema: 2, audience_ref: '4.4.0-rc3', deprecation_refs: ['4.3.1', '4.4.0-rc3'], classes });
}

/**
 * The Java classes of the member rules' fixture: `Rec` (with its interface
 * `Base`), an enum, a config class, a function type, an error, and classes
 * the audience and deprecation rules refuse.
 */
const CLASSES = [
  javaClass('clients.producer.Rec', {
    supertypes: ['Base'],
    overloads: [
      method('Rec', ['String']),
      method('Rec', ['String', 'int']),
      method('topic'),
      method('offset'),
      method('getSize'),
      method('isOpen'),
      method('hasKey'),
      method('close'),
      method('close', ['Duration']),
      method('old', [], { deprecated: true }),
      method('hidden', [], { visibility: 'private' }),
      method('toString'),
      method('equals', ['Object']),
      method('hashCode'),
    ],
    fields: [
      field('UNKNOWN', { type: 'int' }),
      field('mode', { static: false, final: false }),
      field('counter', { type: 'int', final: false }),
      field('LEGACY', { deprecated: true }),
      field('SECRET', { visibility: 'private' }),
    ],
  }),
  javaClass('clients.producer.Base', { kind: 'interface', overloads: [method('flush'), method('extra')] }),
  javaClass('clients.producer.Mode', {
    kind: 'enum',
    overloads: [
      method('forName', ['String'], { static: true }),
      method('values', [], { static: true }),
      method('valueOf', ['String'], { static: true }),
      method('name'),
      method('ordinal'),
    ],
    fields: [field('ON', { type: 'Mode' }), field('OFF', { type: 'Mode' })],
    enum_constants: ['ON', 'OFF'],
  }),
  javaClass('clients.producer.ProducerConfig', {
    overloads: [method('ProducerConfig', ['Map']), method('configNames', [], { static: true })],
    fields: [field('A_CONFIG'), field('B_CONFIG', { deprecated: true })],
  }),
  javaClass('clients.producer.Callback', { kind: 'interface', overloads: [method('onCompletion', ['RecordMetadata'])] }),
  javaClass('clients.producer.Sealed', { public_audience: false, overloads: [method('go')] }),
  javaClass('clients.producer.Old', { deprecated: true }),
  javaClass('clients.producer.internals.Acc'),
  javaClass('common.x.Unsupp', { unsupported_package: true }),
  javaClass('clients.producer.Pkg', { overloads: [method('Pkg', [], { visibility: 'package-private' })] }),
  javaClass('common.errors.FooException'),
  javaClass('common.header.Header', { kind: 'interface', overloads: [method('key'), method('value')] }),
  javaClass('common.Future', { abstract: true, overloads: [method('Future'), method('get', [], { return_type: 'Object' })] }),
  javaClass('clients.producer.Legacy', { kind: 'interface', deprecated: true, overloads: [method('stop')] }),
  javaClass('clients.producer.Stopper', { supertypes: ['Legacy'], overloads: [method('start')] }),
  javaClass('clients.producer.Proto', {
    overloads: [method('name')],
    fields: [field('id', { type: 'short', static: false })],
  }),
];

/** The fixture's invented members, on the list of invented names. */
const INVENTED = { 'Rec.extraJs': 'A JavaScript addition.', 'Mode.toJSON': "JavaScript's JSON protocol." };

/** The fixture's src/: a translation of every class above that the rules accept. */
const LIB = {
  'src/producer/rec.ts': `export class Rec {
  static readonly UNKNOWN: number = -1;
  constructor(options: { p0: string; p1?: number });
  constructor(p0: string);
  constructor(options: unknown) { void options; }
  get topic(): string { return ''; }
  get offset(): number { return 0; }
  get size(): number { return 0; }
  get isOpen(): boolean { return true; }
  get hasKey(): boolean { return false; }
  close(options?: { p0?: number }): void;
  close(p0: number): void;
  close(options?: unknown): void { void options; }
  flush(): void {}
  extra(): void {}
  toString(): string { return ''; }
  /** A JavaScript addition, on the list of invented names. */
  extraJs(): void {}
  private _native(): void {}
  #state = 1;
}
`,
  'src/producer/mode.ts': `export class Mode {
  private constructor(readonly tag: string) {}
  static readonly ON: Mode = new Mode('ON');
  static readonly OFF: Mode = new Mode('OFF');
  static forName(n: string): Mode { void n; return Mode.ON; }
  static values(): Mode[] { return [Mode.ON, Mode.OFF]; }
  static valueOf(n: string): Mode { void n; return Mode.ON; }
  name(): string { return this.tag; }
  ordinal(): number { return 0; }
  toString(): string { return this.tag; }
  toJSON(): string { return this.tag; }
}
`.replace('private constructor(readonly tag: string) {}', 'private constructor(private readonly tag: string) {}'),
  'src/producer/producer_config.ts': `export class ProducerConfig {
  static readonly A_CONFIG: string = 'a';
  constructor(...args: unknown[]) { void args; throw new TypeError('not instantiable'); }
}
`,
  'src/producer/callback.ts': 'export type Callback = (metadata: unknown) => void;\n',
  'src/common/header/header.ts': 'export interface Header {\n  readonly key: string;\n  value(): unknown;\n}\n',
  'src/common/future.ts': 'export class Future {\n  private constructor() {}\n  get(): unknown { return null; }\n}\n',
  'src/common/errors/foo_error.ts': `export class FooError extends Error {
  declare readonly cause: unknown;
  constructor(message: string) { super(message); }
}
`,
  'src/internal/error_registry.ts': "export const ERRORS = [\n  ['../common/errors/foo_error', 'FooError', 'org.apache.kafka.common.errors.FooException'],\n];\n",
  // The mapping lists (scripts/lint/java_ts.js mappingLists): Rec.extraJs and Mode.toJSON are invented.
  'lint-mapping.json': mapping({ invented: INVENTED }),
};

/** The text of a lint-mapping.json holding `lists` (`invented`, `javaTargets`: key to text). */
function mapping(lists) {
  return `${JSON.stringify({ invented: {}, javaTargets: {}, ...lists }, null, 2)}\n`;
}

module.exports = { method, field, javaClass, javaIndex, mapping, INVENTED, CLASSES, LIB };
