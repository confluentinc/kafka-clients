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

// DeletedRecords and RecordsToDelete: Java ships no test of them; their
// JSON forms, a long as a decimal string.

const assert = require('node:assert/strict');

const { DeletedRecords, RecordsToDelete } = require('@confluentinc/kafka-javascript/admin');

test('the JSON forms write the offsets as decimal strings', () => {
  assert.deepEqual(JSON.parse(JSON.stringify(new DeletedRecords(9007199254740993n))), { lowWatermark: '9007199254740993' });
  assert.deepEqual(JSON.parse(JSON.stringify(RecordsToDelete.beforeOffset(5n))), { beforeOffset: '5' });
});
