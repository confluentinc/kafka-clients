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

// AlterConfigOp and its OpType enum. Java has no test of its own for them:
// these check Java's equals, hashCode, toString, OpType.forId and the forms.

const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { AlterConfigOp, ConfigEntry } = require('@confluentinc/kafka-javascript/admin');

const { OpType } = AlterConfigOp;

test("OpType is Java's enum, forId answering null for an unknown byte", () => {
  assert.deepEqual(
    OpType.values().map((t) => [t.name(), t.ordinal(), t.id()]),
    [
      ['SET', 0, 0],
      ['DELETE', 1, 1],
      ['APPEND', 2, 2],
      ['SUBTRACT', 3, 3],
    ],
  );
  for (const t of OpType.values()) assert.equal(OpType.forId(t.id()), t);
  assert.equal(OpType.forId(4), null);
  assert.equal(OpType.valueOf('APPEND'), OpType.APPEND);
  assert.throws(() => OpType.valueOf('set'), {
    name: 'IllegalArgumentError',
    message: 'No enum constant org.apache.kafka.clients.admin.AlterConfigOp.OpType.set',
  });
  assert.equal(JSON.stringify(OpType.DELETE), '"DELETE"');
});

test("AlterConfigOp keeps Java's getters, equals, hashCode and toString; Java's constructor takes nulls", () => {
  const entry = new ConfigEntry({ name: 'retention.ms', value: '42' });
  const op = new AlterConfigOp({ configEntry: entry, operationType: OpType.SET });
  assert.equal(op.configEntry(), entry);
  assert.equal(op.opType(), OpType.SET);
  const same = new AlterConfigOp({ configEntry: new ConfigEntry({ name: 'retention.ms', value: '42' }), operationType: OpType.SET });
  assert.equal(op.equals(same), true);
  assert.equal(op.hashCode(), same.hashCode());
  assert.equal(op.equals(new AlterConfigOp({ configEntry: entry, operationType: OpType.DELETE })), false);
  assert.equal(op.toString(), `AlterConfigOp{opType=SET, configEntry=${entry.toString()}}`);
  assert.equal(JSON.stringify(op), JSON.stringify({ opType: 'SET', configEntry: entry }));
  const empty = new AlterConfigOp({ configEntry: null, operationType: null });
  assert.equal(empty.toString(), 'AlterConfigOp{opType=null, configEntry=null}');
  // @ts-expect-error a key no form has: the form check
  assert.throws(() => new AlterConfigOp({ configEntry: entry }), IllegalArgumentError);
  // @ts-expect-error a wrong type: a configEntry is a ConfigEntry
  assert.throws(() => new AlterConfigOp({ configEntry: 'x', operationType: OpType.SET }), {
    name: 'TypeError',
    message: 'configEntry must be a ConfigEntry or null, not a string',
  });
});
