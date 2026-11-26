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

// Java's public static finals can't be reassigned: every public static data
// property of every exported class, nested classes included, is
// non-writable and non-configurable, and a static made on its first read is
// a non-configurable getter. An enum or a config class freezes itself, a
// generated one too; the other classes name theirs to
// src/internal/static_finals.ts.

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { finalStatics } = require('../../../dist/internal/static_finals');

const pkg = JSON.parse(fs.readFileSync(path.join(__dirname, '..', '..', '..', 'package.json'), 'utf8'));

// The statics every function has, and the binding's private ones (`_ffiId`).
/** @param {string} key */
const isPublicStatic = (key) => !['length', 'name', 'prototype'].includes(key) && !key.startsWith('_');

/**
 * Every exported class, nested classes included, by its dotted name.
 *
 * @returns {Map<Function, string>} each class and its name
 */
function exportedClasses() {
  /** @type {Map<Function, string>} */
  const out = new Map();
  /** @param {Function} cls @param {string} name */
  const walk = (cls, name) => {
    if (out.has(cls)) return;
    out.set(cls, name);
    for (const key of Reflect.ownKeys(cls)) {
      if (typeof key !== 'string' || !isPublicStatic(key)) continue;
      const descriptor = /** @type {PropertyDescriptor} */ (Object.getOwnPropertyDescriptor(cls, key));
      if (typeof descriptor.value === 'function' && descriptor.value.prototype !== undefined && /^[A-Z]/.test(key)) {
        walk(descriptor.value, `${name}.${key}`);
      }
    }
  };
  for (const subpath of Object.keys(pkg.exports)) {
    const specifier = subpath === '.' ? pkg.name : `${pkg.name}/${subpath.slice(2)}`;
    for (const [name, value] of Object.entries(require(specifier))) {
      if (typeof value === 'function' && value.prototype !== undefined) walk(value, name);
    }
  }
  return out;
}

test('every public static data property of an exported class is non-writable and non-configurable', () => {
  /** @type {string[]} */
  const reassignable = [];
  let checked = 0;
  for (const [cls, name] of exportedClasses()) {
    for (const key of Reflect.ownKeys(cls)) {
      if (typeof key !== 'string' || !isPublicStatic(key)) continue;
      const descriptor = /** @type {PropertyDescriptor} */ (Object.getOwnPropertyDescriptor(cls, key));
      // A static method or a nested class is no Java field.
      if (typeof descriptor.value === 'function') continue;
      checked += 1;
      if (descriptor.configurable || descriptor.writable) reassignable.push(`${name}.${key}`);
    }
  }
  assert.ok(checked > 200, `${checked} statics checked`);
  assert.deepEqual(reassignable, []);
});

test('a strict-mode assignment to a static final throws TypeError, and the class still works', () => {
  const { TimestampType } = require('@confluentinc/kafka-javascript/common/record');
  const { SecurityProtocol } = require('@confluentinc/kafka-javascript/common/security/auth');
  const { SslClientAuth, SslConfigs } = require('@confluentinc/kafka-javascript/common/config');
  const { ProducerConfig, RecordMetadata, KafkaProducer } = require('@confluentinc/kafka-javascript/producer');
  const { RecordDeserializationError } = require('@confluentinc/kafka-javascript/common/errors');
  const { AdminClientConfig, ConfigEntry, MockAdminClient } = require('@confluentinc/kafka-javascript/admin');
  const { AclOperation } = require('@confluentinc/kafka-javascript/common/acl');
  const { TopicConfig } = require('@confluentinc/kafka-javascript/common/config');
  const { KafkaPrincipal } = require('@confluentinc/kafka-javascript/common/security/auth');
  const { Uuid } = require('@confluentinc/kafka-javascript/common');
  const reserved = Uuid.RESERVED;
  const createTime = TimestampType.CREATE_TIME;
  /** @type {Array<[unknown, string]>} */
  const classes = [
    [TimestampType, 'CREATE_TIME'],
    [TimestampType, 'forName'],
    [SecurityProtocol, 'SSL'],
    [SslClientAuth, 'VALUES'],
    [SslConfigs, 'SSL_PROTOCOL_CONFIG'],
    [ProducerConfig, 'BOOTSTRAP_SERVERS_CONFIG'],
    [RecordMetadata, 'UNKNOWN_PARTITION'],
    [KafkaProducer, 'NETWORK_THREAD_PREFIX'],
    [RecordDeserializationError.DeserializationExceptionOrigin, 'KEY'],
    [ConfigEntry.ConfigSource, 'DEFAULT_CONFIG'],
    [AclOperation, 'READ'],
    [MockAdminClient, 'DEFAULT_CLUSTER_ID'],
    [MockAdminClient, 'DEFAULT_LOG_DIRS'],
    [AdminClientConfig, 'BOOTSTRAP_SERVERS_CONFIG'],
    [TopicConfig, 'CLEANUP_POLICY_CONFIG'],
    [KafkaPrincipal, 'ANONYMOUS'],
    [Uuid, 'ZERO_UUID'],
    [Uuid, 'RESERVED'],
  ];
  const finals = classes.map(([cls, key]) => /** @type {[Record<string, unknown>, string]} */ ([cls, key]));
  for (const [cls, key] of finals) {
    assert.throws(
      () => {
        cls[key] = null;
      },
      TypeError,
      key,
    );
    assert.throws(() => delete cls[key], TypeError, key);
  }
  // A new static can't be added to a frozen class either.
  assert.throws(() => {
    /** @type {Record<string, unknown>} */ (/** @type {unknown} */ (TimestampType)).EXTRA = 1;
  }, TypeError);
  assert.equal(TimestampType.CREATE_TIME, createTime);
  assert.equal(TimestampType.forName('CreateTime'), createTime);
  assert.equal(TimestampType.valueOf('LOG_APPEND_TIME'), TimestampType.LOG_APPEND_TIME);
  assert.equal(RecordMetadata.UNKNOWN_PARTITION, -1);
  // A static made on its first read keeps its getter, and gives the same set.
  assert.equal(Uuid.RESERVED, reserved);
  assert.equal(typeof Object.getOwnPropertyDescriptor(Uuid, 'RESERVED')?.get, 'function');
  assert.equal(ConfigEntry.ConfigSource.valueOf('DEFAULT_CONFIG'), ConfigEntry.ConfigSource.DEFAULT_CONFIG);
  assert.equal(MockAdminClient.DEFAULT_CLUSTER_ID, 'I4ZmrWqfT2e-upky_4fdPA');
});

test('finalStatics makes a data property final and a getter non-configurable, and refuses a name the class lacks', () => {
  /** A class with a constant and a static made on its first read. */
  class Lazy {
    static CONSTANT = 1;

    /** @returns {number} made on its first read */
    static get MADE() {
      return 2;
    }
  }
  finalStatics(Lazy, 'CONSTANT', 'MADE');
  const constant = /** @type {PropertyDescriptor} */ (Object.getOwnPropertyDescriptor(Lazy, 'CONSTANT'));
  assert.deepEqual([constant.writable, constant.configurable, constant.enumerable], [false, false, true]);
  const made = /** @type {PropertyDescriptor} */ (Object.getOwnPropertyDescriptor(Lazy, 'MADE'));
  assert.equal(typeof made.get, 'function');
  assert.equal(made.configurable, false);
  assert.equal(Lazy.MADE, 2);
  assert.throws(() => finalStatics(Lazy, /** @type {never} */ ('MISSING')), {
    constructor: TypeError,
    message: 'MISSING is not a static of the class',
  });
});
