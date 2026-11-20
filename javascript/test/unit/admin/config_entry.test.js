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

// ConfigEntry and Config: Java ships no test of them; ConfigEntry's
// List<ConfigSynonym> taken as any iterable, copied, and the JSON forms.

const assert = require('node:assert/strict');

const { Config, ConfigEntry } = require('@confluentinc/kafka-javascript/admin');

test('the synonyms are any iterable of ConfigSynonym, copied', () => {
  const entry = new ConfigEntry({
    name: 'a',
    value: 'b',
    source: ConfigEntry.ConfigSource.DEFAULT_CONFIG,
    isSensitive: false,
    isReadOnly: false,
    synonyms: new Set(),
    type: ConfigEntry.ConfigType.STRING,
    documentation: null,
  });
  assert.deepEqual(entry.synonyms(), []);
  assert.throws(
    () =>
      new ConfigEntry({
        name: 'a',
        value: 'b',
        source: null,
        isSensitive: false,
        isReadOnly: false,
        // @ts-expect-error a string is no ConfigSynonym
        synonyms: ['x'],
        type: null,
        documentation: null,
      }),
    { name: 'TypeError', message: 'synonyms must contain only ConfigSynonym, not a string' },
  );
});

test('the JSON forms of Config, ConfigEntry and ConfigSynonym: a sensitive value redacted, as toString() does', () => {
  const plain = new ConfigEntry({ name: 'a', value: 'b' });
  assert.deepEqual(JSON.parse(JSON.stringify(new Config([plain]))), {
    entries: [
      {
        name: 'a',
        value: 'b',
        source: 'UNKNOWN',
        isSensitive: false,
        isReadOnly: false,
        synonyms: [],
        type: 'UNKNOWN',
        documentation: null,
      },
    ],
  });
  const secret = new ConfigEntry({
    name: 'p',
    value: 's3cret',
    source: ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG,
    isSensitive: true,
    isReadOnly: false,
    synonyms: [],
    type: ConfigEntry.ConfigType.PASSWORD,
    documentation: null,
  });
  const json = JSON.stringify(secret);
  assert.ok(!json.includes('s3cret'), json);
  assert.equal(JSON.parse(json).value, 'Redacted');
});
