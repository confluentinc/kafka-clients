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

// What the tests read from Java: the committed Java index (java-index.json,
// written by `cargo xtask java-api` from Apache Kafka's sources, read through
// scripts/lint/java_index.js), never the kafka submodule.

const { committedJavaIndex } = require('../scripts/lint/java_index');

/** The index's class `fqn` (`org.apache.kafka.….Outer$Inner`); fails when the index has none. */
function javaClass(fqn) {
  const cls = committedJavaIndex().classes.find((c) => c.fqn === fqn);
  if (cls === undefined) throw new Error(`the Java index has no class ${fqn}`);
  return cls;
}

/**
 * Every `public static final String NAME = "literal";` of a Java class, as
 * `[name, value]`, and the names of its deprecated members (the index's
 * deprecation: the source's and design/current/java-deprecated.txt's).
 *
 * @param {string} fqn
 * @returns {{literals: Array<[string, string]>, deprecated: Set<string>}}
 */
function javaStringConstants(fqn) {
  const cls = javaClass(fqn);
  const literals = cls.fields.filter((f) => f.value !== undefined).map((f) => [f.name, f.value]);
  const deprecated = new Set(cls.fields.filter((f) => f.deprecated).map((f) => f.name));
  return { literals, deprecated };
}

module.exports = { javaClass, javaStringConstants };
