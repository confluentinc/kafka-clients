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

// The generated error hierarchy and the core -> JavaScript conversion
// (src/internal/errors.ts), as python/test/unit/test_errors.py checks the Python one:
// every class has the Java parent's class as its parent (both directions),
// lives in the folder of its Java package, carries its FFI id (private, as
// Python's `_ffi_id`) and no public field but `name`, `message` and `cause`,
// takes Java's message and cause constructor forms (both directions), and the
// conversion builds the class of the core's id with the fallback rules of
// python/confluent_kafka/_errors.py.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = require('@confluentinc/kafka-javascript');
/** The package of a src/ folder, by its own import path (no index re-exports another). */
const pkg = (folder) => require(folder === '' ? '@confluentinc/kafka-javascript' : `@confluentinc/kafka-javascript/${folder}`);
const common = {
  ...pkg('common'),
  config: pkg('common/config'),
  errors: pkg('common/errors'),
  metrics: pkg('common/metrics'),
  requests: pkg('common/requests'),
  protocol: { types: pkg('common/protocol/types') },
};
const producer = pkg('producer');
const consumer = pkg('consumer');
const { ERRORS, FFI_NAMES } = require('../../../dist/internal/error_registry');
const { LOCAL_NULL_POINTER, UNKNOWN_SERVER_ERROR, classForFfiId, errorClasses, errorFfiId, fromNative } = require('../../../dist/internal/errors');
const { create, ownFfiId, ownForms } = require('../../../dist/internal/throwable');

const REPO = path.join(__dirname, '..', '..', '..', '..');
const JAVA = path.join(REPO, 'kafka', 'clients', 'src', 'main', 'java');

const { KafkaError } = common;
const {
  ApiError,
  AuthenticationError,
  InvalidMetadataError,
  NotLeaderOrFollowerError,
  RetriableError,
  SaslAuthenticationError,
  TopicAuthorizationError,
  UnknownServerError,
} = common.errors;
const { ConfigError } = common.config;
const { IllegalArgumentError, IllegalStateError, NoSuchElementError, ExecutionError, IndexOutOfBoundsError } = root;

const BY_JAVA = new Map(ERRORS.map(([module, name, java]) => [java, require(path.join('../../../dist/internal', module))[name]]));
const JAVA_OF = new Map([...BY_JAVA].map(([java, cls]) => [cls, java]));

// The Java built-ins whose class extends JavaScript's RangeError.
const RANGE_ERRORS = new Set(['java.lang.IllegalArgumentException', 'java.lang.IndexOutOfBoundsException']);

// The Java built-ins' constructors (Java 11's; the JDK source is not in the
// tree), as the parameter sets of their message and cause forms.
const JDK_FOUR = [[], ['message'], ['message', 'cause'], ['cause']];
const JDK_FORMS = new Map([
  ['java.lang.IllegalStateException', JDK_FOUR],
  ['java.lang.IllegalArgumentException', JDK_FOUR],
  ['java.util.ConcurrentModificationException', JDK_FOUR],
  ['java.util.concurrent.TimeoutException', [[], ['message']]],
  ['java.lang.UnsupportedOperationException', JDK_FOUR],
  ['java.util.concurrent.ExecutionException', [['message', 'cause'], ['cause']]],
  ['java.util.NoSuchElementException', [[], ['message']]],
  ['java.lang.IndexOutOfBoundsException', [[], ['message']]],
  ['java.util.concurrent.CancellationException', [[], ['message']]],
]);
// The Java built-ins whose Java parent is another built-in with a class
// (Java 11's `CancellationException extends IllegalStateException`).
const JDK_PARENTS = new Map([['java.util.concurrent.CancellationException', 'java.lang.IllegalStateException']]);

/**
 * The message and cause forms of Java class `fqn`: its public constructors
 * (protected too, of an abstract class) that take only a String and a
 * Throwable, as parameter sets, read from its source. A constructor with any
 * other parameter takes a payload and has no form.
 */
function javaForms(fqn) {
  if (JDK_FORMS.has(fqn)) return JDK_FORMS.get(fqn);
  const source = fs.readFileSync(path.join(JAVA, ...fqn.split('.')) + '.java', 'utf8');
  const simple = fqn.split('.').pop();
  const abstract = new RegExp(`abstract\\s+class\\s+${simple}\\b`).test(source);
  const visibility = abstract ? '(?:public|protected)' : 'public';
  const forms = [];
  let any = false;
  for (const m of source.matchAll(new RegExp(`(@Deprecated\\s+)?${visibility}\\s+${simple}\\s*\\(([^)]*)\\)`, 'g'))) {
    any = true;
    if (m[1] !== undefined) continue;
    const types = m[2].trim() === '' ? [] : m[2].split(',').map((p) => p.trim().replace(/^final\s+/, '').split(/\s+/).slice(0, -1).join(' '));
    // A Throwable parameter is the cause, whatever its class (InterruptedException).
    const throwable = (t) => t === 'Throwable' || /Exception$/.test(t);
    const strings = types.filter((t) => t === 'String').length;
    const throwables = types.filter(throwable).length;
    if (strings + throwables !== types.length || strings > 1 || throwables > 1) continue;
    forms.push(types.map((t) => (t === 'String' ? 'message' : 'cause')));
  }
  // A class that declares no constructor has Java's implicit no-argument one.
  if (!any) forms.push([]);
  return forms;
}

/**
 * The fixed messages of Java class `fqn`'s constructors, read from its
 * source: the form's parameter set (`message,cause`) -> the text of the
 * string literals its `super(...)` / `this(...)` call starts with
 * (`super("Failing batch since transaction was aborted")`).
 */
function javaFixedTexts(fqn) {
  const texts = new Map();
  if (fqn.startsWith('java.')) return texts;
  const source = fs.readFileSync(path.join(JAVA, ...fqn.split('.')) + '.java', 'utf8');
  const simple = fqn.split('.').pop();
  const ctor = new RegExp(`(?:public|protected)\\s+${simple}\\s*\\(([^)]*)\\)\\s*\\{\\s*(?:super|this)\\(\\s*((?:"(?:[^"\\\\]|\\\\.)*"\\s*\\+?\\s*)+)`, 'g');
  for (const m of source.matchAll(ctor)) {
    const types = m[1].trim() === '' ? [] : m[1].split(',').map((p) => p.trim().replace(/^final\s+/, '').split(/\s+/).slice(0, -1).join(' '));
    // A constructor with a payload has no form (javaForms), so no fixed message.
    if (!types.every((t) => t === 'String' || t === 'Throwable' || /Exception$/.test(t))) continue;
    const params = types.map((t) => (t === 'String' ? 'message' : 'cause'));
    const text = [...m[2].matchAll(/"((?:[^"\\]|\\.)*)"/g)].map((l) => JSON.parse(`"${l[1]}"`)).join('');
    texts.set(params.join(','), text);
  }
  return texts;
}

/** Whether Java class `fqn` is abstract. */
function isAbstract(fqn) {
  if (fqn.startsWith('java.')) return false;
  const source = fs.readFileSync(path.join(JAVA, ...fqn.split('.')) + '.java', 'utf8');
  return new RegExp(`abstract\\s+class\\s+${fqn.split('.').pop()}\\b`).test(source);
}

/** An instance of `cls`, as the core reports one. */
function instance(cls) {
  return create(cls, 'm');
}

/** The FQN of Java's `extends` parent of `fqn`, read from its source. */
function javaParent(fqn) {
  const source = fs.readFileSync(path.join(JAVA, ...fqn.split('.')) + '.java', 'utf8');
  const simple = fqn.split('.').pop();
  const m = new RegExp(`class\\s+${simple}\\b[^{]*?\\bextends\\s+([\\w.]+)`).exec(source);
  assert.ok(m, `${fqn} has no extends clause`);
  const parent = m[1];
  if (parent.includes('.')) return parent;
  const imported = new RegExp(`^import\\s+([\\w.]+\\.${parent});`, 'm').exec(source);
  if (imported) return imported[1];
  if (['RuntimeException', 'Exception', 'IllegalStateException', 'IllegalArgumentException'].includes(parent)) {
    return `java.lang.${parent}`;
  }
  return `${fqn.slice(0, fqn.lastIndexOf('.'))}.${parent}`;
}

/** The src/ folder of a Java package: `clients` dropped, `java.*` at the root. */
function folderOf(fqn) {
  const pkg = fqn.slice(0, fqn.lastIndexOf('.'));
  if (pkg.startsWith('java.')) return '';
  return pkg.replace(/^org\.apache\.kafka\./, '').replace(/^clients\./, '').split('.').join('/');
}

test('every Java class has its class, whose parent is the Java parent\'s class', () => {
  assert.equal(BY_JAVA.size, 176);
  for (const [java, cls] of BY_JAVA) {
    const parent = Object.getPrototypeOf(cls);
    if (JDK_PARENTS.has(java)) {
      assert.equal(parent, BY_JAVA.get(JDK_PARENTS.get(java)), java);
      continue;
    }
    if (java.startsWith('java.') || java === 'org.apache.kafka.common.KafkaException') {
      // JavaScript has no RuntimeException: the chains start at Error, or at
      // RangeError for a value out of its range.
      assert.equal(parent, RANGE_ERRORS.has(java) ? RangeError : Error, java);
      continue;
    }
    const javaParentFqn = javaParent(java);
    assert.equal(parent, BY_JAVA.get(javaParentFqn), `${java} extends ${javaParentFqn}`);
  }
});

test('every error class the package defines is a generated one', () => {
  // Walk every import path of the package for Error subclasses: none is
  // invented. No index re-exports another, so each path is visited.
  const found = new Set();
  const visit = (ns) => {
    for (const value of Object.values(ns)) {
      if (typeof value === 'function' && value.prototype instanceof Error) found.add(value);
      else if (value !== null && typeof value === 'object' && !Array.isArray(value)) visit(value);
    }
  };
  const { exports } = require('../../../package.json');
  for (const entry of Object.keys(exports)) visit(pkg(entry === '.' ? '' : entry.slice('./'.length)));
  assert.deepEqual(new Set(errorClasses()), found);
});

test('a class keeps its Java name with the Exception suffix as Error, in the folder of its Java package', () => {
  for (const [module, name, java] of ERRORS) {
    const simple = java.split('.').pop();
    assert.equal(name, simple.endsWith('Exception') ? `${simple.slice(0, -'Exception'.length)}Error` : simple, java);
    const folder = folderOf(java);
    const stem = name.replace(/(?<=[a-z0-9])(?=[A-Z])/g, '_').toLowerCase();
    assert.equal(module, `../${folder === '' ? '' : `${folder}/`}${stem}`, java);
    // Exported by its folder's package.
    assert.equal(pkg(folder.split('/').filter(Boolean).join('/'))[name], BY_JAVA.get(java), `${name} is not exported by its package`);
    assert.equal(BY_JAVA.get(java).name, name);
  }
  assert.equal(producer.BufferExhaustedError.name, 'BufferExhaustedError');
  assert.equal(common.InvalidRecordError.name, 'InvalidRecordError');
  assert.equal(common.requests.CorrelationIdMismatchError.name, 'CorrelationIdMismatchError');
  assert.equal(common.protocol.types.SchemaError.name, 'SchemaError');
  assert.equal(consumer.CommitFailedError.name, 'CommitFailedError');
  // The two suffixless Java exceptions keep their names.
  assert.equal(common.errors.InvalidRegularExpression.name, 'InvalidRegularExpression');
});

test('the Java built-ins are at the root and are not KafkaErrors', () => {
  for (const cls of [IllegalStateError, NoSuchElementError, ExecutionError,
    root.ConcurrentModificationError, root.TimeoutError, root.UnsupportedOperationError]) {
    assert.equal(Object.getPrototypeOf(cls), Error, cls.name);
    assert.ok(!(instance(cls) instanceof KafkaError), cls.name);
  }
  // IllegalArgumentError and IndexOutOfBoundsError are JavaScript's RangeError.
  for (const cls of [IllegalArgumentError, IndexOutOfBoundsError]) {
    assert.equal(Object.getPrototypeOf(cls), RangeError, cls.name);
    const e = new cls('x');
    assert.ok(e instanceof RangeError && e instanceof Error && !(e instanceof KafkaError), cls.name);
    assert.equal(e.name, cls.name);
  }
  // The java.util.concurrent TimeoutException is not Kafka's.
  assert.notEqual(root.TimeoutError, common.errors.TimeoutError);
  assert.ok(new common.errors.TimeoutError('t') instanceof RetriableError);
  assert.ok(common.requests.CorrelationIdMismatchError.prototype instanceof IllegalStateError);
});

test("every class's own FFI id names its class, and the id table is a bijection", () => {
  const ids = new Map();
  for (const cls of errorClasses()) {
    const id = ownFfiId(cls);
    if (id === undefined || cls === KafkaError) continue;
    assert.ok(!ids.has(id), `${cls.name} and ${ids.get(id)?.name} share ${id}`);
    ids.set(id, cls);
    assert.equal(classForFfiId(id), cls);
    const e = fromNative({ code: id, message: 'm', apiError: true });
    assert.equal(e.constructor, cls, cls.name);
    assert.equal(e.name, cls.name);
    assert.equal(e.message, 'm');
    assert.equal(errorFfiId(e), id, cls.name);
    assert.equal(errorFfiId(instance(cls)), id, cls.name);
  }
  assert.equal(ids.size, 166);
  // Every FFI id but NONE and the Rust-only LOCAL_CALLBACK and LOCAL_NULL_POINTER has a class.
  const rustOnly = new Set(['NONE', 'LOCAL_CALLBACK', 'LOCAL_NULL_POINTER']);
  const named = [...FFI_NAMES].filter(([, n]) => !rustOnly.has(n)).map(([v]) => v);
  assert.deepEqual([...ids.keys()].sort((a, b) => a - b), named.sort((a, b) => a - b));
});

test('the base carries UNKNOWN_SERVER_ERROR, which UnknownServerError owns', () => {
  assert.equal(UNKNOWN_SERVER_ERROR, -1);
  assert.equal(ownFfiId(KafkaError), -1);
  assert.equal(classForFfiId(-1), UnknownServerError);
  assert.equal(FFI_NAMES.get(-1), 'UNKNOWN_SERVER_ERROR');
  assert.equal(errorFfiId(new KafkaError('x')), -1);
});

test('the built-ins the core does not model have no id', () => {
  for (const cls of [NoSuchElementError, IndexOutOfBoundsError, ExecutionError]) {
    assert.equal(ownFfiId(cls), undefined, cls.name);
    // The gRPC server reports them as UNKNOWN_SERVER_ERROR, as Python's to_ffi_id.
    assert.equal(errorFfiId(instance(cls)), -1);
  }
  assert.equal(errorFfiId(new IllegalStateError('s')), -4);
  assert.equal(FFI_NAMES.get(-4), 'LOCAL_ILLEGAL_STATE');
  assert.equal(errorFfiId(new IllegalArgumentError('a')), -3);
  // Anything that is not an error of the client's has none.
  for (const other of [new TypeError('t'), new Error('e'), { code: 7 }, null, 'x']) {
    assert.equal(errorFfiId(other), undefined);
  }
});

test('an error has no public field but name, message and cause: classify with instanceof', () => {
  for (const cls of errorClasses()) {
    const e = instance(cls);
    assert.deepEqual(Object.keys(e), [], cls.name);
    for (const gone of ['code', 'errorName', 'retriable', 'apiError']) {
      assert.ok(!(gone in e), `${cls.name}.${gone}`);
      assert.ok(!(gone in cls), `${cls.name}.${gone} (static)`);
    }
    assert.deepEqual(Object.getOwnPropertyNames(e).sort(), ['message', 'name', 'stack'], cls.name);
    assert.deepEqual(Object.getOwnPropertyNames(cls.prototype), ['constructor'], cls.name);
  }
  const withCause = create(KafkaError, 'm', null);
  assert.deepEqual(Object.getOwnPropertyNames(withCause).sort(), ['cause', 'message', 'name', 'stack']);
});

test("every class takes Java's message and cause forms, both ways", () => {
  for (const cls of errorClasses()) {
    const java = JAVA_OF.get(cls);
    const forms = (ownForms(cls) ?? []).map(([params]) => params);
    assert.deepEqual(
      forms.map((f) => f.join(',')).sort(),
      [...new Set(javaForms(java).map((f) => f.join(',')))].sort(),
      java,
    );
  }
});

test('each form gives Java\'s message and cause, and a call no form takes is refused with the exact message', () => {
  const cause = new TopicAuthorizationError('inner');
  for (const cls of errorClasses()) {
    const java = JAVA_OF.get(cls);
    // An abstract base refuses to be constructed (the test below), but its
    // forms are what a subclass takes: check them through a throwaway one.
    const target = isAbstract(java) ? class Sub extends cls {} : cls;
    const registered = ownForms(cls) ?? [];
    if (registered.length === 0) {
      assert.throws(() => new target({ message: 'm' }), {
        name: 'TypeError',
        message: `${cls.name} cannot be constructed: its Java constructors take a payload the client does not translate yet`,
      });
      continue;
    }
    const list = registered.map(([params]) => `(${params.join(', ')})`).join(', ');
    for (const [params, message] of registered) {
      const args = {};
      if (params.includes('message')) args.message = 'm';
      if (params.includes('cause')) args.cause = cause;
      const e = new target(args);
      assert.ok(e instanceof cls && e.name === target.name, cls.name);
      // Java's message, from Java's source, not from the generated table: a
      // fixed text where the constructor passes one, else the message, the
      // cause's toString() for (cause), Java's null ('') for ().
      const fixed = javaFixedTexts(java).get(params.join(','));
      const want = fixed !== undefined ? fixed
        : params.includes('message') ? 'm'
          : params.includes('cause') ? 'TopicAuthorizationError: inner' : '';
      assert.equal(e.message, want, `${cls.name}(${params})`);
      if (params.includes('cause')) assert.equal(e.cause, cause);
      else assert.ok(!Object.hasOwn(e, 'cause'), `${cls.name}(${params})`);
    }
    for (const missing of [['message', 'cause'], ['message'], ['cause'], []]) {
      if (registered.some(([params]) => params.join() === missing.join())) continue;
      const args = Object.fromEntries(missing.map((p) => [p, p === 'cause' ? cause : 'm']));
      assert.throws(() => (missing.length === 0 ? new target() : new target(args)), {
        name: 'IllegalArgumentError',
        message: `${cls.name}() takes one of ${list}; got (${missing.join(', ')})`,
      }, `${cls.name}(${missing})`);
    }
  }
});

test('the abstract bases are Java-abstract, have no id and cannot be constructed', () => {
  const abstract = [...BY_JAVA].filter(([java]) => isAbstract(java));
  assert.deepEqual(abstract.map(([, cls]) => cls.name).sort(), [
    'ApplicationRecoverableError',
    'InvalidMetadataError',
    'InvalidOffsetError',
    'RefreshRetriableError',
    'RetriableError',
  ]);
  for (const [, cls] of abstract) {
    assert.equal(ownFfiId(cls), undefined);
    assert.throws(() => new cls('x'), {
      name: 'TypeError',
      message: `${cls.name} is an abstract catch-only base; it is never thrown directly`,
    });
  }
  class Mine extends RetriableError {}
  const mine = new Mine('a subclass of an abstract base');
  assert.equal(mine.message, 'a subclass of an abstract base');
  assert.equal(mine.name, 'Mine');
  assert.ok(mine instanceof RetriableError && mine instanceof ApiError);
  // A user's subclass takes the forms of its nearest generated class, and
  // reports the id of its nearest class with one (ApiError's: an abstract
  // base has none).
  assert.equal(new Mine().message, '');
  assert.throws(() => new Mine({ reason: 'r' }), {
    name: 'IllegalArgumentError',
    message: 'RetriableError() takes one of (message, cause), (message), (cause), (); got (reason)',
  });
  assert.equal(errorFfiId(mine), ownFfiId(ApiError));
  class MyTimeout extends common.errors.TimeoutError {}
  assert.equal(errorFfiId(new MyTimeout('t')), 7);
});

test("the instanceof chain is Java's extends chain", () => {
  const e = new SaslAuthenticationError('bad credentials');
  for (const cls of [SaslAuthenticationError, AuthenticationError, ApiError, KafkaError, Error]) {
    assert.ok(e instanceof cls, cls.name);
  }
  assert.ok(!(e instanceof RetriableError));
  assert.equal(e.name, 'SaslAuthenticationError');
  assert.equal(errorFfiId(e), 58);
  const n = new NotLeaderOrFollowerError('no leader');
  assert.ok(n instanceof InvalidMetadataError && n instanceof RetriableError && n instanceof ApiError);
  // A ConfigException is a KafkaException, not an ApiException.
  const c = new ConfigError('c');
  assert.ok(c instanceof KafkaError && !(c instanceof ApiError));
  assert.equal(errorFfiId(c), -10);
});

test("the constructors are Java's message and cause, by name or bare", () => {
  assert.equal(new KafkaError('m').message, 'm');
  assert.equal(new KafkaError({ message: 'm' }).message, 'm');
  assert.equal(new KafkaError(null).message, '');
  assert.equal(new KafkaError({ message: null }).message, '');
  assert.equal(new KafkaError().message, '');
  // undefined is never "not given", where Java has a no-argument form too.
  assert.throws(() => new KafkaError(undefined), {
    name: 'TypeError',
    message: "message must not be undefined. Leave the argument out, or pass null for Java's null",
  });
  const cause = new TopicAuthorizationError('inner');
  const withCause = new KafkaError({ message: 'outer', cause });
  assert.equal(withCause.message, 'outer');
  assert.equal(withCause.cause, cause);
  // Java's (Throwable cause): the message is cause.toString(), JavaScript's String(cause).
  for (const causeOnly of [new KafkaError({ cause }), new KafkaError(cause)]) {
    assert.equal(causeOnly.message, 'TopicAuthorizationError: inner');
    assert.equal(causeOnly.cause, cause);
  }
  assert.equal(new KafkaError({ cause: null }).message, '');
  assert.equal(new KafkaError({ cause: null }).cause, null);
  assert.equal(new KafkaError({ message: 'm', cause: null }).cause, null);
  assert.ok(!Object.hasOwn(new KafkaError('m'), 'cause'));
  assert.equal(new KafkaError('m').cause, undefined);
  // A cause on its own is an Error; with no message form of its own, null too.
  assert.equal(new ExecutionError(cause).message, 'TopicAuthorizationError: inner');
  assert.equal(new ExecutionError(null).cause, null);
  assert.equal(new ExecutionError({ message: 'x', cause }).message, 'x');
  assert.throws(() => new ExecutionError('x'), {
    name: 'IllegalArgumentError',
    message: 'ExecutionError() takes one of (message, cause), (cause); got a string',
  });
  assert.throws(() => new ExecutionError(), {
    name: 'IllegalArgumentError',
    message: 'ExecutionError() takes one of (message, cause), (cause); got ()',
  });
  assert.throws(() => new KafkaError(5), {
    name: 'IllegalArgumentError',
    message: 'KafkaError() takes one of (message, cause), (message), (cause), (); got a number',
  });
  assert.throws(() => new KafkaError({ message: 5 }), { name: 'TypeError', message: 'message must be a string, not number' });
  assert.throws(() => new KafkaError({ message: undefined }), {
    name: 'TypeError',
    message: "message must not be undefined. Leave the key out, or pass null for Java's null",
  });
  assert.throws(() => new KafkaError({ message: 'm', reason: 'r' }), {
    name: 'IllegalArgumentError',
    message: 'KafkaError() takes one of (message, cause), (message), (cause), (); got (message, reason)',
  });
  // The ES2022 `new Error(message, { cause })` points to the one object.
  assert.throws(() => new KafkaError('m', { cause }), {
    name: 'IllegalArgumentError',
    message:
      'KafkaError() takes one object of named parameters, not 2 arguments: pass the message and the cause as one object, ' +
      'new KafkaError({ message, cause })',
  });
  // A form Java does not have is refused: TopicAuthorizationException has no
  // (String, Throwable) constructor.
  assert.throws(() => new TopicAuthorizationError({ message: 'm', cause }), {
    name: 'IllegalArgumentError',
    message: 'TopicAuthorizationError() takes one of (message); got (message, cause)',
  });
  // Java's own messages, pinned in full (python/test/unit/test_errors.py's
  // test_constructors_with_a_literal_message).
  assert.equal(new common.errors.TransactionAbortedError().message, 'Failing batch since transaction was aborted');
  assert.equal(
    new common.errors.CorruptRecordError().message,
    'This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.',
  );
  assert.equal(new common.errors.CorruptRecordError(cause).message, 'TopicAuthorizationError: inner');
  assert.equal(new ApiError(cause).message, 'TopicAuthorizationError: inner');
  assert.equal(
    new consumer.CommitFailedError().message,
    'Commit cannot be completed since the group has already rebalanced and assigned the partitions to another member. ' +
      'This means that the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms, ' +
      'which typically implies that the poll loop is spending too much time message processing. You can address this ' +
      'either by increasing max.poll.interval.ms or by reducing the maximum size of batches returned in poll() with ' +
      'max.poll.records.',
  );
  assert.equal(
    new consumer.RetriableCommitFailedError(cause).message,
    'Offset commit failed with a retriable exception. You should retry committing the latest consumed offsets.',
  );
  assert.equal(new consumer.RetriableCommitFailedError(cause).cause, cause);
  assert.ok(new consumer.CommitFailedError().message.startsWith('Commit cannot be completed since the group has already rebalanced'));
  assert.equal(new consumer.CommitFailedError('mine').message, 'mine');
  // A class whose every Java constructor takes a payload cannot be constructed yet.
  assert.throws(() => new common.metrics.QuotaViolationError('m'), {
    name: 'TypeError',
    message: 'QuotaViolationError cannot be constructed: its Java constructors take a payload the client does not translate yet',
  });
  assert.equal(Object.keys(new KafkaError('m')).includes('name'), false);
});

/** Whether `error`'s stack has a frame, and its first frame is not in throwable.ts (the caller's). */
function assertCallerFrames(error, what) {
  const frames = error.stack.split('\n').filter((line) => line.trimStart().startsWith('at '));
  assert.ok(frames.length > 0, `${what}: no stack frame in ${error.stack}`);
  assert.ok(!frames[0].includes('throwable'), `${what}: the first frame is create's own: ${frames[0]}`);
}

test('errors built without their constructor keep the caller\'s stack frames', async () => {
  // A core error, built by fromNative (as the addon does).
  const core = fromNative({ code: 29, message: 'denied', apiError: true });
  assertCallerFrames(core, 'fromNative');
  assert.match(core.stack, /errors\.test\.js/);
  // A form error of the matcher.
  let form;
  try {
    new producer.ProducerRecord({ topik: 't', value: 'v' });
  } catch (e) {
    form = e;
  }
  assert.ok(form instanceof IllegalArgumentError);
  assertCallerFrames(form, 'a form error');
  assert.match(form.stack, /errors\.test\.js/);
  // A closed producer's IllegalStateError.
  const p = new producer.KafkaProducer({ 'bootstrap.servers': 'localhost:1' });
  await p.close();
  const closed = await p.send(new producer.ProducerRecord({ topic: 't', value: 'v' })).then(() => null, (e) => e);
  assert.ok(closed instanceof IllegalStateError);
  assertCallerFrames(closed, 'a closed producer');
  assert.match(closed.stack, /kafka_producer/);
  // The own property names are unchanged.
  assert.deepEqual(Object.getOwnPropertyNames(core).sort(), ['message', 'name', 'stack']);
  assert.deepEqual(Object.getOwnPropertyNames(form).sort(), ['message', 'name', 'stack']);
});

test('fromNative: an unknown id constructs the base KafkaError', () => {
  const e = fromNative({ code: 999999, message: 'some message', apiError: false });
  assert.equal(e.constructor, KafkaError);
  assert.equal(e.message, 'some message');
  // The instance keeps the core's id, which its class does not have.
  assert.equal(errorFfiId(e), 999999);
  assert.ok(!('code' in e));
});

test("fromNative: LOCAL_NULL_POINTER is JavaScript's own TypeError, as Python's", () => {
  // Java's NullPointerException for a NULL mandatory argument has no class.
  assert.equal(FFI_NAMES.get(LOCAL_NULL_POINTER), 'LOCAL_NULL_POINTER');
  const cause = { code: 7, message: 'inner', apiError: true };
  const e = fromNative({ code: LOCAL_NULL_POINTER, message: 'topic must not be null', apiError: false, cause });
  assert.equal(e.constructor, TypeError);
  assert.ok(!(e instanceof KafkaError));
  assert.equal(e.message, 'topic must not be null');
  assert.equal(e.cause.constructor, common.errors.TimeoutError);
  assert.equal(e.cause.message, 'inner');
  assert.equal(fromNative({ code: LOCAL_NULL_POINTER, message: 'm' }).cause, undefined);
});

test('fromNative: -1 is UnknownServerError for an ApiException, else the bare KafkaError', () => {
  const api = fromNative({ code: -1, message: 'boom', apiError: true });
  assert.equal(api.constructor, UnknownServerError);
  assert.equal(errorFfiId(api), -1);
  const bare = fromNative({ code: -1, message: 'boom', apiError: false });
  assert.equal(bare.constructor, KafkaError);
  assert.equal(errorFfiId(bare), -1);
});

test('fromNative chains the core cause and keeps what the core reported', () => {
  const e = fromNative({ code: -1, message: 'outer', apiError: false, cause: { code: 29, message: 'inner', apiError: true } });
  assert.equal(e.message, 'outer');
  // TopicAuthorizationException has no (message, cause) form, yet the core's error keeps its cause.
  assert.ok(e.cause instanceof TopicAuthorizationError);
  assert.equal(e.cause.message, 'inner');
  assert.equal(errorFfiId(e.cause), 29);
  const t = fromNative({ code: 29, message: 'denied', apiError: true, cause: { code: 7, message: 'inner', apiError: true } });
  assert.equal(t.constructor, TopicAuthorizationError);
  assert.equal(t.cause.constructor, common.errors.TimeoutError);
  // A class with no form at all is built from the core too.
  const q = fromNative({ code: ownFfiId(common.metrics.QuotaViolationError), message: 'quota', apiError: false });
  assert.equal(q.constructor, common.metrics.QuotaViolationError);
  assert.equal(q.message, 'quota');
  const s = fromNative({ code: 58, message: 'Authentication failed', apiError: true });
  assert.ok(s instanceof SaslAuthenticationError && s instanceof AuthenticationError);
  // Not plain objects: returned as they are.
  const already = new Error('x');
  assert.equal(fromNative(already), already);
  assert.equal(fromNative(null), null);
});

test("the gRPC server maps the client's errors by their private FFI id, the Java built-ins included", () => {
  // grpc_translate.js (the multilanguage harness's JS server) tells the client's
  // errors from its own by errorFfiId: a binding-thrown IllegalArgumentError
  // crosses with its code -3 and the client's message unprefixed, as
  // python/grpc_translate.py does. Here, not in test/static: it loads the
  // addon through dist/producer.
  const { kafkaErrorToProto } = require('../../../grpc-server/grpc_translate');
  const message = 'Invalid timestamp: -5. Timestamp should always be non-negative or null.';
  assert.deepEqual(kafkaErrorToProto(new IllegalArgumentError(message)), { code: -3, message });
  assert.deepEqual(kafkaErrorToProto(new IllegalStateError('closed')), { code: -4, message: 'closed' });
  assert.deepEqual(kafkaErrorToProto(new SaslAuthenticationError('auth')), { code: 58, message: 'auth' });
  // An error the core reported crosses with the core's id, an unknown one included.
  assert.deepEqual(kafkaErrorToProto(fromNative({ code: 999999, message: 'odd', apiError: false })), { code: 999999, message: 'odd' });
  assert.deepEqual(kafkaErrorToProto(fromNative({ code: -1, message: 'bare', apiError: false })), { code: -1, message: 'bare' });
  assert.deepEqual(kafkaErrorToProto(new ExecutionError({ message: 'e', cause: null })), { code: -1, message: 'e' });
  // Anything else is the server's own failure.
  assert.deepEqual(kafkaErrorToProto(new TypeError('bad')), { code: -4, message: 'js server: TypeError: bad' });
});

test("the gRPC server sends a failed future's delivery error, the cause of get()'s ExecutionError", () => {
  const { futureError, kafkaErrorToProto } = require('../../../grpc-server/grpc_translate');
  const delivery = fromNative({ code: 7, message: 'Expiring 1 record(s)', apiError: true });
  assert.equal(futureError(new ExecutionError({ cause: delivery })), delivery);
  assert.deepEqual(kafkaErrorToProto(futureError(new ExecutionError(delivery))), { code: 7, message: 'Expiring 1 record(s)' });
  // An ExecutionError with no cause, and any other error, cross as they are.
  const bare = new ExecutionError({ message: 'e', cause: null });
  assert.equal(futureError(bare), bare);
  const other = new TypeError('bad');
  assert.equal(futureError(other), other);
});
