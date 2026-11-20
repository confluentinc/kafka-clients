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

// TopicListing: Java ships no test of it; its constructor, getters,
// toString() and the JSON form.

const assert = require('node:assert/strict');

const { TopicListing } = require('@confluentinc/kafka-javascript/admin');
const { Uuid } = require('@confluentinc/kafka-javascript/common');

test("the constructor's fields, Java's toString() and the JSON form", () => {
  const id = new Uuid({ mostSigBits: 1n, leastSigBits: 2n });
  const listing = new TopicListing({ name: 't', topicId: id, internal: false });
  assert.equal(listing.name(), 't');
  assert.equal(listing.topicId(), id);
  assert.equal(listing.isInternal(), false);
  assert.equal(listing.toString(), `(name=t, topicId=${id}, internal=false)`);
  assert.deepEqual(JSON.parse(JSON.stringify(listing)), { name: 't', topicId: id.toString(), internal: false });
  assert.equal(new TopicListing({ name: null, topicId: null, internal: true }).toString(), '(name=null, topicId=null, internal=true)');
  // @ts-expect-error a missing field: the form check throws IllegalArgumentError
  assert.throws(() => new TopicListing({ name: 't', internal: false }), {
    name: 'IllegalArgumentError',
    message: 'TopicListing() takes one of (name, topicId, internal); got (name, internal)',
  });
});
