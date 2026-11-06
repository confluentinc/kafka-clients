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

// Reads the Java sources the generated modules come from (kafka/, Apache
// Kafka 4.3.1), for tests that check a generated value against Java's own.

const fs = require('node:fs');
const path = require('node:path');

const REPO = path.join(__dirname, '..', '..');
const JAVA = path.join(REPO, 'kafka', 'clients', 'src', 'main', 'java');
const DEPRECATED = path.join(REPO, 'design', 'current', 'java-deprecated.txt');

/**
 * Every `public static final String NAME = "literal";` of a Java class, as
 * `[name, value]`, and the names of its deprecated members
 * (design/current/java-deprecated.txt).
 *
 * @param {string} fqn
 * @returns {{literals: Array<[string, string]>, deprecated: Set<string>}}
 */
function javaStringConstants(fqn) {
  const source = fs.readFileSync(path.join(JAVA, ...fqn.split('.')) + '.java', 'utf8');
  const literals = [...source.matchAll(/public static final String (\w+)\s*=\s*"((?:[^"\\]|\\.)*)";/g)].map((m) => [
    m[1],
    JSON.parse(`"${m[2]}"`),
  ]);
  const deprecated = new Set(
    fs
      .readFileSync(DEPRECATED, 'utf8')
      .split('\n')
      .filter((line) => line.startsWith(`${fqn}#`))
      .map((line) => line.split('#')[1].trim()),
  );
  return { literals, deprecated };
}

module.exports = { javaStringConstants };
