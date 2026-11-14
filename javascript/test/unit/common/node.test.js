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

// Java has no NodeTest; these check the translation of Java's Node: its
// three constructors, its value methods as fields, noNode() and toString().

const test = require('node:test');
const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { Node } = require('@confluentinc/kafka-javascript/common');

const FIELDS = ['id', 'idString', 'host', 'port', 'hasRack', 'rack', 'isFenced', 'isEmpty'];

test("Java's three constructors, and what the shorter ones pass", () => {
  const plain = new Node({ id: 1, host: 'localhost', port: 9092 });
  assert.deepEqual({ ...plain }, { id: 1, idString: '1', host: 'localhost', port: 9092, hasRack: false, rack: null, isFenced: false, isEmpty: false });
  const racked = new Node({ id: 2, host: 'h', port: 1, rack: 'r1' });
  assert.equal(racked.hasRack, true);
  assert.equal(racked.rack, 'r1');
  assert.equal(racked.isFenced, false);
  const fenced = new Node({ id: 3, host: 'h', port: 1, rack: null, isFenced: true });
  assert.equal(fenced.hasRack, false);
  assert.equal(fenced.isFenced, true);
});

test('a data object: the fields are own data properties, kept by spread and JSON', () => {
  const node = new Node({ id: 0, host: 'localhost', port: 9092, rack: 'a', isFenced: false });
  assert.deepEqual(Object.keys(node), FIELDS);
  for (const name of FIELDS) {
    const d = Object.getOwnPropertyDescriptor(node, name);
    assert.ok('value' in d && d.enumerable, `${name} is an own enumerable data property`);
    assert.equal(Object.getOwnPropertyDescriptor(Node.prototype, name), undefined);
  }
  assert.deepEqual(JSON.parse(JSON.stringify(node)), { ...node });
  assert.equal(Object.isFrozen(node), false);
});

test('isEmpty, as Java computes it', () => {
  assert.equal(new Node({ id: 1, host: null, port: 1 }).isEmpty, true);
  assert.equal(new Node({ id: 1, host: '', port: 1 }).isEmpty, true);
  assert.equal(new Node({ id: 1, host: 'h', port: -1 }).isEmpty, true);
  assert.equal(new Node({ id: 1, host: 'h', port: 0 }).isEmpty, false);
});

test("noNode() is Java's placeholder, the same object every call", () => {
  const none = Node.noNode();
  assert.equal(none, Node.noNode());
  assert.equal(none.id, -1);
  assert.equal(none.host, '');
  assert.equal(none.port, -1);
  assert.equal(none.isEmpty, true);
  assert.throws(() => Node.noNode(1), { constructor: TypeError, message: 'noNode() only reads: it takes no argument' });
});

test("toString() is Java's", () => {
  assert.equal(new Node({ id: 0, host: 'localhost', port: 9092 }).toString(), 'localhost:9092 (id: 0 rack: null isFenced: false)');
  assert.equal(
    new Node({ id: 7, host: 'b', port: 1, rack: 'r', isFenced: true }).toString(),
    'b:1 (id: 7 rack: r isFenced: true)',
  );
});

test('the constructor takes one of Java forms, with run-time types', () => {
  const forms = 'Node() takes one of (id, host, port), (id, host, port, rack), (id, host, port, rack, isFenced)';
  assert.throws(() => new Node({ id: 1, host: 'h' }), { constructor: IllegalArgumentError, message: `${forms}; got (id, host)` });
  assert.throws(() => new Node({ id: 1, host: 'h', port: 1, isFenced: true }), {
    constructor: IllegalArgumentError,
    message: `${forms}; got (id, host, port, isFenced)`,
  });
  assert.throws(() => new Node({ id: 1, host: 'h', port: 1, idString: '1' }), {
    constructor: IllegalArgumentError,
    message: `${forms}; got (id, host, port, idString)`,
  });
  assert.throws(() => new Node({ id: '1', host: 'h', port: 1 }), { constructor: TypeError, message: 'id must be an integer' });
  assert.throws(() => new Node({ id: 1, host: 2, port: 1 }), { constructor: TypeError, message: 'host must be a string or null' });
  assert.throws(() => new Node({ id: 1, host: 'h', port: null }), { constructor: TypeError, message: 'port must be an integer' });
  assert.throws(() => new Node({ id: 1, host: 'h', port: 1, rack: 3 }), { constructor: TypeError, message: 'rack must be a string or null' });
  assert.throws(() => new Node({ id: 1, host: 'h', port: 1, rack: null, isFenced: null }), {
    constructor: TypeError,
    message: 'isFenced must be a boolean',
  });
  assert.throws(() => new Node({ id: 1, host: undefined, port: 1 }), {
    constructor: TypeError,
    message: "host must not be undefined. Leave the key out, or pass null for Java's null",
  });
});
