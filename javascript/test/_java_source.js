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
// written by `cargo xtask java-index` from Apache Kafka's sources, read through
// scripts/lint/java_index.js), never the kafka submodule.

const { committedJavaIndex } = require('../scripts/lint/java_index');

/**
 * A field of a class in the index.
 *
 * @typedef {object} JavaField
 * @property {string} name
 * @property {string | null} type
 * @property {string} visibility
 * @property {boolean} static
 * @property {boolean} final
 * @property {boolean} deprecated
 * @property {string} [value] its literal, for a constant
 */

/**
 * A method or constructor of a class in the index.
 *
 * @typedef {object} JavaOverload
 * @property {string} name
 * @property {Array<string>} params
 * @property {Array<string>} param_names
 * @property {Array<string>} param_types
 * @property {string | null} return_type null for a constructor
 * @property {string} visibility
 * @property {boolean} static
 * @property {boolean} default
 * @property {boolean} deprecated
 * @property {boolean} [abstract]
 * @property {string} [fixed_message]
 */

/**
 * A class of the index.
 *
 * @typedef {object} JavaClass
 * @property {string} fqn
 * @property {string} package
 * @property {Array<string>} path
 * @property {string} kind
 * @property {string} visibility
 * @property {boolean} abstract
 * @property {boolean} deprecated
 * @property {boolean} public_audience
 * @property {boolean} unsupported_package
 * @property {Array<string>} supertypes
 * @property {Array<string | null>} supertype_fqns
 * @property {Array<JavaOverload>} overloads
 * @property {Array<JavaField>} fields
 * @property {Array<string>} enum_constants
 */

/**
 * The index's class `fqn` (`org.apache.kafka.….Outer$Inner`); fails when the index has none.
 *
 * @param {string} fqn
 * @returns {JavaClass}
 */
function javaClass(fqn) {
  /** @type {Array<JavaClass>} */
  const classes = committedJavaIndex().classes;
  const cls = classes.find((c) => c.fqn === fqn);
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
  /** @type {Array<[string, string]>} */
  const literals = [];
  for (const f of cls.fields) if (f.value !== undefined) literals.push([f.name, f.value]);
  const deprecated = new Set(cls.fields.filter((f) => f.deprecated).map((f) => f.name));
  return { literals, deprecated };
}

module.exports = { javaClass, javaStringConstants };
