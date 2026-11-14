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

// check-encode-callers (scripts/lint/rules/encode_callers.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');

const RULE = 'check-encode-callers';

const ENCODE = 'export function encodeLikeJava(s: string): Uint8Array {\n  return new Uint8Array(s.length);\n}\n';
const USE = (from) => `import { encodeLikeJava } from '${from}';\nexport const b = encodeLikeJava('x');\n`;

const base = {
  'tsconfig.json': TSCONFIG,
  'src/internal/encode.ts': ENCODE,
  'src/internal/config.ts': USE('./encode'),
  'src/common/header/headers.ts': USE('../../internal/encode'),
  'src/common/serialization/string_serializer.ts': USE('../../internal/encode'),
};

test('the string serializer, the headers code and the config code pass', () => {
  const r = lint(project(base), [RULE]);
  assert.equal(r.status, 0, r.text);
});

test('another module, under any name, is a finding: the record encodes no string key or value', () => {
  const r = lint(
    project({
      ...base,
      'src/producer/producer_record.ts': USE('../internal/encode'),
      'src/producer/kafka_producer.ts':
        "import { encodeLikeJava as enc } from '../internal/encode';\nexport const k = enc('k');\nexport const f = [enc];\n",
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'src/producer/kafka_producer.ts:2: uses encodeLikeJava: only the string serializer, the headers code and the config code turn strings into bytes',
    'src/producer/kafka_producer.ts:3: uses encodeLikeJava: only the string serializer, the headers code and the config code turn strings into bytes',
    'src/producer/producer_record.ts:2: uses encodeLikeJava: only the string serializer, the headers code and the config code turn strings into bytes',
  ]);
});
