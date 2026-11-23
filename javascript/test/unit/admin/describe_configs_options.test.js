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

// DescribeConfigsOptions, an option class (javascript/CLAUDE.md, JavaScript
// Binding Conventions, Class family, Q17, Q19): the keyed constructor, Java's
// starting values and the getter / setter pairs. Java has no test of it.

const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { DescribeConfigsOptions } = require('@confluentinc/kafka-javascript/admin');

test("DescribeConfigsOptions: Java's starting values, the pairs, the keyed constructor", () => {
  const options = new DescribeConfigsOptions();
  assert.equal(options.includeSynonyms(), false);
  assert.equal(options.includeDocumentation(), false);
  assert.equal(options.timeoutMs(), null);
  assert.equal(options.includeSynonyms(true), options);
  assert.equal(options.includeDocumentation({ includeDocumentation: true }).includeDocumentation(), true);
  const keyed = new DescribeConfigsOptions({ timeoutMs: 10, includeSynonyms: true });
  assert.equal(keyed.timeoutMs(), 10);
  assert.equal(keyed.includeSynonyms(), true);
  assert.equal(keyed.includeDocumentation(), false);
  // @ts-expect-error a wrong type: Java's boolean, so a string fits no form
  assert.throws(() => options.includeSynonyms('yes'), {
    name: 'IllegalArgumentError',
    message: 'includeSynonyms() takes one of (), (includeSynonyms); got a string',
  });
  // @ts-expect-error a wrong type: Java's boolean, given by name
  assert.throws(() => options.includeSynonyms({ includeSynonyms: 'yes' }), {
    name: 'TypeError',
    message: 'includeSynonyms must be a boolean, not a string',
  });
  // @ts-expect-error a key the class has not
  assert.throws(() => new DescribeConfigsOptions({ validateOnly: true }), IllegalArgumentError);
});
