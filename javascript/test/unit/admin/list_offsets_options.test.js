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

// ListOffsetsOptions: Java's two constructors, () and (isolationLevel), and
// the keyed constructor. Java has no test of it.

const assert = require('node:assert/strict');

const { ListOffsetsOptions } = require('@confluentinc/kafka-javascript/admin');
const { IsolationLevel } = require('@confluentinc/kafka-javascript/common');

test("ListOffsetsOptions: READ_UNCOMMITTED by default, or the level of Java's one-argument constructor, bare or by name", () => {
  assert.equal(new ListOffsetsOptions().isolationLevel(), IsolationLevel.READ_UNCOMMITTED);
  assert.equal(new ListOffsetsOptions(IsolationLevel.READ_COMMITTED).isolationLevel(), IsolationLevel.READ_COMMITTED);
  const keyed = new ListOffsetsOptions({ isolationLevel: IsolationLevel.READ_COMMITTED });
  assert.equal(keyed.isolationLevel(), IsolationLevel.READ_COMMITTED);
  // Java's constructors only: the timeout is set with its setter.
  // @ts-expect-error a wrong argument: the class takes no keyed constructor of its setters
  assert.throws(() => new ListOffsetsOptions({ isolationLevel: IsolationLevel.READ_COMMITTED, timeoutMs: 5 }), {
    name: 'IllegalArgumentError',
    message: 'ListOffsetsOptions() takes one of (), (isolationLevel); got (isolationLevel, timeoutMs)',
  });
  assert.equal(new ListOffsetsOptions(IsolationLevel.READ_COMMITTED).timeoutMs(5).timeoutMs(), 5);
  // Java stores a null level.
  assert.equal(new ListOffsetsOptions({ isolationLevel: null }).isolationLevel(), null);
  // @ts-expect-error a wrong argument: an isolation level is an IsolationLevel
  assert.throws(() => new ListOffsetsOptions({ isolationLevel: 'read_committed' }), {
    name: 'TypeError',
    message: 'isolationLevel must be an IsolationLevel, not a string',
  });
  // @ts-expect-error an argument given to a reader: it only reads
  assert.throws(() => keyed.isolationLevel(1), { name: 'TypeError', message: 'isolationLevel() only reads: it takes no argument' });
});
