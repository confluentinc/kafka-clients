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

// The package's entry points (src/index.ts and every module folder's
// index.ts, compiled to CommonJS in dist/): an ES module `import`s each with
// the same named exports `require` returns, through Node's CommonJS interop.

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const pkg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', '..', 'package.json'), 'utf8'));

test('every entry point imports from an ES module with the names require returns', async () => {
  for (const subpath of Object.keys(pkg.exports)) {
    const specifier = subpath === '.' ? pkg.name : `${pkg.name}/${subpath.slice(2)}`;
    const required = require(specifier);
    const imported = await import(specifier);
    const names = Object.keys(required).sort();
    // A Java package with no class of its own (common/protocol,
    // common/security) keeps its import path and exports nothing.
    for (const name of names) assert.equal(imported[name], required[name], `${specifier}: ${name}`);
    assert.equal(imported.default, required, `${specifier}: default`);
  }
});

test('the root exports only the Java built-in errors, and no index a namespace', () => {
  const root = require(pkg.name);
  assert.deepEqual(Object.keys(root).sort(), [
    'CancellationError',
    'ConcurrentModificationError',
    'ExecutionError',
    'IllegalArgumentError',
    'IllegalStateError',
    'IndexOutOfBoundsError',
    'NoSuchElementError',
    'TimeoutError',
    'UnsupportedOperationError',
  ]);
  for (const subpath of Object.keys(pkg.exports)) {
    const specifier = subpath === '.' ? pkg.name : `${pkg.name}/${subpath.slice(2)}`;
    for (const [name, value] of Object.entries(require(specifier))) {
      assert.equal(typeof value, 'function', `${specifier}: ${name} is a class, not a namespace`);
    }
  }
});

test('a deep import fails with ERR_PACKAGE_PATH_NOT_EXPORTED, from CommonJS and from an ES module', async () => {
  // Users import from a package's path, never from a file (root conventions,
  // Layout and modules): `exports` lists only the package paths.
  const deep = [
    `${pkg.name}/dist/index.js`,
    `${pkg.name}/dist/producer/kafka_producer`,
    `${pkg.name}/dist/internal/config`,
    `${pkg.name}/producer/kafka_producer`,
    `${pkg.name}/common/header/header`,
    `${pkg.name}/package.json`,
  ];
  for (const specifier of deep) {
    assert.throws(() => require(specifier), { code: 'ERR_PACKAGE_PATH_NOT_EXPORTED' }, specifier);
    await assert.rejects(import(specifier), { code: 'ERR_PACKAGE_PATH_NOT_EXPORTED' }, specifier);
  }
});

/**
 * A second copy of the package, as `node_modules` holds one when it has two
 * versions: the compiled package copied under build/second-copy/, its addon
 * the same file (native/ linked), so only the JavaScript classes differ.
 */
function secondCopy() {
  const root = path.join(__dirname, '..', '..');
  const dir = path.join(root, 'build', 'second-copy', 'node_modules', '@confluentinc', 'kafka-javascript');
  fs.rmSync(path.join(root, 'build', 'second-copy'), { recursive: true, force: true });
  fs.mkdirSync(dir, { recursive: true });
  fs.cpSync(path.join(root, 'dist'), path.join(dir, 'dist'), { recursive: true });
  fs.copyFileSync(path.join(root, 'package.json'), path.join(dir, 'package.json'));
  fs.symlinkSync(path.join(root, 'native'), path.join(dir, 'native'), 'junction');
  const { createRequire } = require('node:module');
  return createRequire(path.join(dir, '..', '..', 'index.js'));
}

test('two copies of the package: their classes are distinct, so calls taking the client\'s classes refuse the other copy\'s', async () => {
  // README.md, Limitations: "The client supports one copy of the package in
  // an app. If node_modules holds two versions, calls that take the client's
  // classes can fail." The classes are told apart with plain instanceof, with
  // no Symbol.for brand (root conventions, Signatures).
  const other = secondCopy();
  const ours = { root: require(pkg.name), errors: require(`${pkg.name}/common/errors`), common: require(`${pkg.name}/common`), header: require(`${pkg.name}/common/header`), producer: require(`${pkg.name}/producer`) };
  const theirs = { root: other(pkg.name), errors: other(`${pkg.name}/common/errors`), common: other(`${pkg.name}/common`), header: other(`${pkg.name}/common/header`), producer: other(`${pkg.name}/producer`) };
  assert.notEqual(theirs.producer.KafkaProducer, ours.producer.KafkaProducer, 'the second copy is the same module');

  // The error hierarchy holds within each copy, and not across them; the
  // name, and the FFI id the binding keeps, are the same in both.
  const error = new theirs.errors.TopicAuthorizationError('denied');
  assert.ok(error instanceof theirs.errors.AuthorizationError && error instanceof theirs.common.KafkaError);
  assert.ok(!(error instanceof ours.errors.TopicAuthorizationError));
  assert.ok(!(error instanceof ours.common.KafkaError));
  assert.ok(error instanceof Error, 'both copies extend the one Error');
  assert.equal(error.name, 'TopicAuthorizationError');
  assert.equal(error.name, new ours.errors.TopicAuthorizationError('denied').name);
  // _ffiId is a static of each class: the two copies' classes carry the same id.
  assert.equal(typeof ours.errors.TopicAuthorizationError._ffiId, 'number');
  assert.equal(ours.errors.TopicAuthorizationError._ffiId, 29);
  assert.equal(theirs.errors.TopicAuthorizationError._ffiId, ours.errors.TopicAuthorizationError._ffiId);
  const state = new theirs.root.IllegalStateError('closed');
  assert.ok(!(state instanceof ours.root.IllegalStateError));
  assert.equal(state.name, 'IllegalStateError');

  // A Header of the other copy is no Header here.
  const header = new theirs.header.Header({ key: 'h', value: null });
  assert.throws(() => new ours.header.Headers([header]), { name: 'TypeError', message: 'a header must be a Header' });
  assert.throws(() => new ours.producer.ProducerRecord({ topic: 't', partition: null, key: null, value: 'v', headers: [header] }), {
    name: 'TypeError',
    message: 'a header must be a Header',
  });

  // A ProducerRecord of the other copy is refused by this copy's producer.
  const record = new theirs.producer.ProducerRecord({ topic: 't', value: 'v' });
  const producer = new ours.producer.KafkaProducer({ 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 200 });
  try {
    // A bare foreign record fits no form of send(): IllegalArgumentError, in
    // the root conventions' format.
    await assert.rejects(producer.send(record), {
      name: 'IllegalArgumentError',
      message: 'send() takes one of (record), (record, callback); got a ProducerRecord',
    });
    // Named, it is the options form, whose record is checked by class.
    await assert.rejects(producer.send({ record }), { name: 'TypeError', message: 'record must be a ProducerRecord' });
  } finally {
    await producer.close({ timeout: 0 });
  }
});
