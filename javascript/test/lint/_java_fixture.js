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

// A hand-written Java index (build/java-index.json, `cargo xtask java-index
// --json`'s schema 1) for the Java rules' fixture projects.

const PREFIX = 'org.apache.kafka.';

/** An overload: `name` with parameter types `params`, public unless told. */
function method(name, params = [], extra = {}) {
  return {
    name,
    params,
    param_names: params.map((_, i) => `p${i}`),
    return_type: 'void',
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
  return JSON.stringify({ schema: 1, audience_ref: '4.4.0-rc3', deprecation_refs: ['4.3.1', '4.4.0-rc3'], classes });
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
  javaClass('common.Future', { abstract: true, overloads: [method('Future'), method('get')] }),
  javaClass('clients.producer.Legacy', { kind: 'interface', deprecated: true, overloads: [method('stop')] }),
  javaClass('clients.producer.Stopper', { supertypes: ['Legacy'], overloads: [method('start')] }),
  javaClass('clients.producer.Proto', {
    overloads: [method('name')],
    fields: [field('id', { type: 'short', static: false })],
  }),
];

/** The fixture's lib/: a translation of every class above that the rules accept. */
const LIB = {
  'lib/producer/rec.ts': `export class Rec {
  static readonly UNKNOWN: number = -1;
  constructor(topic: string) { void topic; }
  get topic(): string { return ''; }
  get offset(): number { return 0; }
  get size(): number { return 0; }
  get isOpen(): boolean { return true; }
  get hasKey(): boolean { return false; }
  close(): void {}
  flush(): void {}
  extra(): void {}
  toString(): string { return ''; }
  /** @jsOnly A JavaScript addition. */
  extraJs(): void {}
  private _native(): void {}
  #state = 1;
}
`,
  'lib/producer/mode.ts': `export class Mode {
  private constructor(readonly tag: string) {}
  static readonly ON: Mode = new Mode('ON');
  static readonly OFF: Mode = new Mode('OFF');
  static forName(n: string): Mode { void n; return Mode.ON; }
  static values(): Mode[] { return [Mode.ON, Mode.OFF]; }
  get name(): string { return this.tag; }
  toString(): string { return this.tag; }
}
`.replace('private constructor(readonly tag: string) {}', 'private constructor(private readonly tag: string) {}'),
  'lib/producer/producer_config.ts': `export class ProducerConfig {
  static readonly A_CONFIG: string = 'a';
  constructor(...args: unknown[]) { void args; throw new TypeError('not instantiable'); }
}
`,
  'lib/producer/callback.ts': 'export type Callback = (metadata: unknown) => void;\n',
  'lib/common/header/header.ts': 'export interface Header {\n  readonly key: string;\n  value(): unknown;\n}\n',
  'lib/common/future.ts': 'export class Future {\n  private constructor() {}\n  get(): Promise<unknown> { return Promise.resolve(null); }\n}\n',
  'lib/common/errors/foo_error.ts': `export class FooError extends Error {
  readonly code!: number;
  readonly retriable!: boolean;
}
`,
  'lib/_error_registry.ts': "export const ERRORS = [\n  ['./common/errors/foo_error', 'FooError', 'org.apache.kafka.common.errors.FooException'],\n];\n",
};

module.exports = { method, field, javaClass, javaIndex, CLASSES, LIB };
