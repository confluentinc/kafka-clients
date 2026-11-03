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
// (lib/_errors.js), as python/test/unit/test_errors.py checks the Python one:
// every class has the Java parent's class as its parent (both directions),
// lives in the folder of its Java package, carries its FFI id, and the
// conversion constructs the class of the core's id with the fallback rules of
// python/confluent_kafka/_errors.py.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = require('@confluentinc/kafka-javascript');
const { common, producer, consumer } = root;
const { ERRORS, FFI_NAMES } = require('../../lib/_error_registry');
const { UNKNOWN_SERVER_ERROR, classForFfiId, errorClasses, fromNative } = require('../../lib/_errors');
const { ownFfiId } = require('../../lib/_throwable');

const REPO = path.join(__dirname, '..', '..', '..');
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

const BY_JAVA = new Map(ERRORS.map(([module, name, java]) => [java, require(path.join('../../lib', module))[name]]));

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

/** The lib/ folder of a Java package: `clients` dropped, `java.*` at the root. */
function folderOf(fqn) {
  const pkg = fqn.slice(0, fqn.lastIndexOf('.'));
  if (pkg.startsWith('java.')) return '';
  return pkg.replace(/^org\.apache\.kafka\./, '').replace(/^clients\./, '').split('.').join('/');
}

test('every Java class has its class, whose parent is the Java parent\'s class', () => {
  assert.equal(BY_JAVA.size, 175);
  for (const [java, cls] of BY_JAVA) {
    const parent = Object.getPrototypeOf(cls);
    if (java.startsWith('java.') || java === 'org.apache.kafka.common.KafkaException') {
      // JavaScript has no RuntimeException: the chains start at Error.
      assert.equal(parent, Error, java);
      continue;
    }
    const javaParentFqn = javaParent(java);
    assert.equal(parent, BY_JAVA.get(javaParentFqn), `${java} extends ${javaParentFqn}`);
  }
});

test('every error class the package defines is a generated one', () => {
  // Walk every namespace for Error subclasses: none is invented.
  const found = new Set();
  const visit = (ns) => {
    for (const value of Object.values(ns)) {
      if (typeof value === 'function' && value.prototype instanceof Error) found.add(value);
      else if (value !== null && typeof value === 'object' && !Array.isArray(value)) visit(value);
    }
  };
  visit(root);
  assert.deepEqual(new Set(errorClasses()), found);
});

test('a class keeps its Java name with the Exception suffix as Error, in the folder of its Java package', () => {
  for (const [module, name, java] of ERRORS) {
    const simple = java.split('.').pop();
    assert.equal(name, simple.endsWith('Exception') ? `${simple.slice(0, -'Exception'.length)}Error` : simple, java);
    const folder = folderOf(java);
    const stem = name.replace(/(?<=[a-z0-9])(?=[A-Z])/g, '_').toLowerCase();
    assert.equal(module, `./${folder === '' ? '' : `${folder}/`}${stem}`, java);
    // Exported by its folder's namespace.
    let ns = root;
    for (const segment of folder.split('/').filter(Boolean)) ns = ns[segment];
    assert.equal(ns[name], BY_JAVA.get(java), `${name} is not exported by its namespace`);
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
  for (const cls of [IllegalStateError, IllegalArgumentError, NoSuchElementError, ExecutionError, IndexOutOfBoundsError,
    root.ConcurrentModificationError, root.TimeoutError, root.UnsupportedOperationError]) {
    assert.equal(Object.getPrototypeOf(cls), Error, cls.name);
    assert.ok(!(new cls('x') instanceof KafkaError), cls.name);
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
    const e = new cls('m');
    assert.equal(e.code, id, cls.name);
    assert.equal(e.errorName, FFI_NAMES.get(id), cls.name);
  }
  assert.equal(ids.size, 166);
  // Every FFI id but NONE and LOCAL_CALLBACK (Rust-only) has a class.
  const named = [...FFI_NAMES].filter(([, n]) => n !== 'NONE' && n !== 'LOCAL_CALLBACK').map(([v]) => v);
  assert.deepEqual([...ids.keys()].sort((a, b) => a - b), named.sort((a, b) => a - b));
});

test('the base carries UNKNOWN_SERVER_ERROR, which UnknownServerError owns', () => {
  assert.equal(UNKNOWN_SERVER_ERROR, -1);
  assert.equal(ownFfiId(KafkaError), -1);
  assert.equal(classForFfiId(-1), UnknownServerError);
  const e = new KafkaError('x');
  assert.equal(e.code, -1);
  assert.equal(e.errorName, 'UNKNOWN_SERVER_ERROR');
});

test('the built-ins the core does not model have no id', () => {
  for (const cls of [NoSuchElementError, IndexOutOfBoundsError, ExecutionError]) {
    assert.equal(ownFfiId(cls), undefined, cls.name);
    const e = new cls('x');
    assert.equal(e.code, -1);
    assert.equal(e.errorName, null);
  }
  assert.equal(new IllegalStateError('s').errorName, 'LOCAL_ILLEGAL_STATE');
  assert.equal(new IllegalArgumentError('a').code, -3);
});

test('the abstract bases are Java-abstract, have no id and cannot be constructed', () => {
  const abstract = [...BY_JAVA].filter(([java]) => {
    if (java.startsWith('java.')) return false;
    const source = fs.readFileSync(path.join(JAVA, ...java.split('.')) + '.java', 'utf8');
    return new RegExp(`abstract\\s+class\\s+${java.split('.').pop()}\\b`).test(source);
  });
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
  assert.equal(mine.retriable, true);
});

test("the instanceof chain is Java's extends chain", () => {
  const e = new SaslAuthenticationError('bad credentials');
  for (const cls of [SaslAuthenticationError, AuthenticationError, ApiError, KafkaError, Error]) {
    assert.ok(e instanceof cls, cls.name);
  }
  assert.ok(!(e instanceof RetriableError));
  assert.equal(e.apiError, true);
  assert.equal(e.retriable, false);
  assert.equal(e.name, 'SaslAuthenticationError');
  assert.equal(e.code, 58);
  assert.equal(e.errorName, 'SASL_AUTHENTICATION_FAILED');
  const n = new NotLeaderOrFollowerError('no leader');
  assert.ok(n instanceof InvalidMetadataError && n instanceof RetriableError && n instanceof ApiError);
  assert.equal(n.retriable, true);
  // A ConfigException is a KafkaException, not an ApiException.
  const c = new ConfigError('c');
  assert.ok(c instanceof KafkaError && !(c instanceof ApiError));
  assert.equal(c.apiError, false);
  assert.equal(c.errorName, 'CONFIG');
});

test("the constructors are Java's message and cause", () => {
  assert.equal(new KafkaError('m').message, 'm');
  assert.equal(new KafkaError(null).message, '');
  assert.equal(new KafkaError().message, '');
  const cause = new TopicAuthorizationError('inner');
  const withCause = new KafkaError('outer', { cause });
  assert.equal(withCause.message, 'outer');
  assert.equal(withCause.cause, cause);
  // Java's (Throwable cause): the message is cause.toString().
  const causeOnly = new KafkaError(undefined, { cause });
  assert.equal(causeOnly.message, 'TopicAuthorizationError: inner');
  assert.equal(causeOnly.cause, cause);
  assert.equal(new KafkaError('m').cause, undefined);
  assert.throws(() => new KafkaError(5), { name: 'TypeError', message: 'message must be a string, not number' });
  assert.equal(Object.keys(new KafkaError('m')).includes('name'), false);
});

test('fromNative: an unknown id constructs the base KafkaError', () => {
  const e = fromNative({ code: 999999, name: null, message: 'some message', retriable: false, apiError: false });
  assert.equal(e.constructor, KafkaError);
  assert.equal(e.message, 'some message');
  assert.equal(e.code, 999999);
  assert.equal(e.errorName, null);
});

test('fromNative: -1 is UnknownServerError for an ApiException, else the bare KafkaError', () => {
  const api = fromNative({ code: -1, name: 'UNKNOWN_SERVER_ERROR', message: 'boom', retriable: false, apiError: true });
  assert.equal(api.constructor, UnknownServerError);
  const bare = fromNative({ code: -1, name: 'UNKNOWN_SERVER_ERROR', message: 'boom', retriable: false, apiError: false });
  assert.equal(bare.constructor, KafkaError);
  assert.equal(bare.errorName, 'UNKNOWN_SERVER_ERROR');
});

test('fromNative chains the core cause and keeps what the core reported', () => {
  const e = fromNative({
    code: -1,
    name: 'UNKNOWN_SERVER_ERROR',
    message: 'outer',
    retriable: false,
    apiError: false,
    cause: { code: 29, name: 'TOPIC_AUTHORIZATION_FAILED', message: 'inner', retriable: false, apiError: true },
  });
  assert.equal(e.message, 'outer');
  assert.ok(e.cause instanceof TopicAuthorizationError);
  assert.equal(e.cause.message, 'inner');
  assert.equal(e.cause.code, 29);
  const s = fromNative({ code: 58, name: 'SASL_AUTHENTICATION_FAILED', message: 'Authentication failed', retriable: false, apiError: true });
  assert.ok(s instanceof SaslAuthenticationError && s instanceof AuthenticationError);
  // Not plain objects: returned as they are.
  const already = new Error('x');
  assert.equal(fromNative(already), already);
  assert.equal(fromNative(null), null);
});
