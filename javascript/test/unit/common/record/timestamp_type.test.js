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

// Java's TimestampType. Java has no TimestampTypeTest: the enum's members are
// checked against its source.

const test = require('node:test');
const assert = require('node:assert/strict');

const { IllegalArgumentError, NoSuchElementError } = require('@confluentinc/kafka-javascript');
const { TimestampType } = require('@confluentinc/kafka-javascript/common/record');

test("the constants keep Java's id, name and order; toString is Java's name field", () => {
  assert.deepEqual(
    TimestampType.values().map((t) => [t.name(), t.ordinal(), t.id, t.toString(), t.toJSON()]),
    [
      ['NO_TIMESTAMP_TYPE', 0, -1, 'NoTimestampType', 'NO_TIMESTAMP_TYPE'],
      ['CREATE_TIME', 1, 0, 'CreateTime', 'CREATE_TIME'],
      ['LOG_APPEND_TIME', 2, 1, 'LogAppendTime', 'LOG_APPEND_TIME'],
    ],
  );
  assert.ok(Object.isFrozen(TimestampType.CREATE_TIME));
  assert.equal(JSON.stringify({ t: TimestampType.CREATE_TIME }), '{"t":"CREATE_TIME"}');
});

test("forName finds Java's name field, valueOf the constant's name", () => {
  assert.equal(TimestampType.forName('CreateTime'), TimestampType.CREATE_TIME);
  assert.equal(TimestampType.forName({ name: 'LogAppendTime' }), TimestampType.LOG_APPEND_TIME);
  assert.throws(() => TimestampType.forName('CREATE_TIME'), { constructor: NoSuchElementError, message: 'Invalid timestamp type CREATE_TIME' });
  assert.throws(() => TimestampType.forName(null), { constructor: NoSuchElementError, message: 'Invalid timestamp type null' });
  assert.equal(TimestampType.valueOf('NO_TIMESTAMP_TYPE'), TimestampType.NO_TIMESTAMP_TYPE);
  assert.throws(() => TimestampType.valueOf('CreateTime'), {
    constructor: IllegalArgumentError,
    message: 'No enum constant org.apache.kafka.common.record.TimestampType.CreateTime',
  });
  assert.throws(() => TimestampType.valueOf(null), { constructor: TypeError, message: 'Name is null' });
  assert.throws(() => TimestampType.values(1), { constructor: TypeError });
});
