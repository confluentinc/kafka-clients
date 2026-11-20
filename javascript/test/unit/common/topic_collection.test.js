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

// Java's TopicCollectionTest, and the binding's checks of the factories.

const assert = require('node:assert/strict');

const { TopicCollection, Uuid } = require('@confluentinc/kafka-javascript/common');

test('testTopicCollection', () => {
  const topicIds = [Uuid.randomUuid(), Uuid.randomUuid(), Uuid.randomUuid()];
  const topicNames = ['foo', 'bar'];
  const idCollection = TopicCollection.ofTopicIds(topicIds);
  const nameCollection = TopicCollection.ofTopicNames(topicNames);
  assert.ok(topicIds.every((id) => idCollection.topicIds().includes(id)));
  assert.ok(topicNames.every((name) => nameCollection.topicNames().includes(name)));
  assert.ok(idCollection instanceof TopicCollection.TopicIdCollection);
  assert.ok(nameCollection instanceof TopicCollection.TopicNameCollection);
});

// ---- the binding's own ----------------------------------------------------------

test('the factories copy the collection, and refuse a bare string and null', () => {
  const names = ['foo'];
  const collection = TopicCollection.ofTopicNames({ topics: new Set(names) });
  names.push('bar');
  assert.deepEqual(collection.topicNames(), ['foo']);
  collection.topicNames().push('baz');
  assert.deepEqual(collection.topicNames(), ['foo']);
  // A string is an iterable of strings to tsc; Java's Collection<String> is
  // no string, which the run-time check refuses.
  assert.throws(() => TopicCollection.ofTopicNames('foo'), {
    name: 'TypeError',
    message: 'topics must be an iterable of strings, not string',
  });
  // @ts-expect-error null
  assert.throws(() => TopicCollection.ofTopicIds(null), { name: 'TypeError', message: 'topics must not be null' });
});

test('the JSON forms', () => {
  const id = Uuid.randomUuid();
  assert.deepEqual(JSON.parse(JSON.stringify(TopicCollection.ofTopicIds([id]))), { topicIds: [id.toString()] });
  assert.deepEqual(JSON.parse(JSON.stringify(TopicCollection.ofTopicNames(['a']))), { topicNames: ['a'] });
});
