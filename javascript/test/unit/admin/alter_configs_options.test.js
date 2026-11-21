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

// AlterConfigsOptions, an option class (root CLAUDE.md, JavaScript Binding
// Conventions, Class family, Q17, Q19): Java's setter validateOnly, whose
// reader has its own name, shouldValidateOnly. Java has no test of it.

const assert = require('node:assert/strict');

const { AlterConfigsOptions } = require('@confluentinc/kafka-javascript/admin');

test('AlterConfigsOptions: validateOnly sets, shouldValidateOnly reads', () => {
  const options = new AlterConfigsOptions();
  assert.equal(options.shouldValidateOnly(), false);
  assert.equal(options.validateOnly(true), options);
  assert.equal(options.shouldValidateOnly(), true);
  assert.equal(new AlterConfigsOptions({ validateOnly: true }).shouldValidateOnly(), true);
  // @ts-expect-error a setter given no value: validateOnly() does not read
  assert.throws(() => options.validateOnly(), {
    name: 'TypeError',
    message: 'validateOnly() takes the value to set: read it with shouldValidateOnly()',
  });
  // @ts-expect-error an argument given to a reader: it only reads
  assert.throws(() => options.shouldValidateOnly(true), {
    name: 'TypeError',
    message: 'shouldValidateOnly() only reads: it takes no argument',
  });
});
