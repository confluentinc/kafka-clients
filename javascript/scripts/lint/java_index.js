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

// The committed Java index, javascript/java-index.json: what the client reads
// from Java (the rule linter and the unit tests), written with the .NET
// binding's list by one scan, `cargo xtask java-api` (rust/xtask/src/
// java_index_json.rs, which documents the schema), so nothing in javascript/
// reads the kafka submodule. The file leaves out a key at its default; this
// loader puts every key back, so a reader sees each class, overload and field
// whole.

const fs = require('node:fs');
const path = require('node:path');

/** The schema this loader reads. */
const SCHEMA = 2;

/** The committed index, beside this client's package.json. */
const INDEX_PATH = path.join(__dirname, '..', '..', 'java-index.json');

/** The command that writes the index, from rust/. */
const COMMAND = 'cargo xtask java-api';

/** Each object's keys and their defaults: what the file leaves out. */
const CLASS_DEFAULTS = {
  kind: 'class',
  visibility: 'public',
  abstract: false,
  deprecated: false,
  public_audience: false,
  unsupported_package: false,
  supertypes: [],
  supertype_fqns: [],
  overloads: [],
  fields: [],
  enum_constants: [],
};
const OVERLOAD_DEFAULTS = { return_type: null, visibility: 'public', static: false, default: false, deprecated: false };
const FIELD_DEFAULTS = { type: null, visibility: 'public', static: false, final: false, deprecated: false };

/** `o` with each key of `defaults` it lacks set to a copy of its default. */
function fill(o, defaults) {
  for (const [k, v] of Object.entries(defaults)) {
    if (!(k in o)) o[k] = Array.isArray(v) ? [] : v;
  }
  return o;
}

/**
 * Index `index` (the parsed file) with every key put back: each class's
 * `fqn` (`org.apache.kafka.<package>.<path joined by $>`), each overload's
 * `param_names` (blank for a private one) and `param_types` (its `params`).
 */
function expand(index) {
  for (const c of index.classes || []) {
    fill(c, CLASS_DEFAULTS);
    if (c.fqn === undefined) c.fqn = `org.apache.kafka.${c.package}.${c.path.join('$')}`;
    for (const o of c.overloads) {
      fill(o, OVERLOAD_DEFAULTS);
      if (o.param_names === undefined) o.param_names = o.params.map(() => '');
      if (o.param_types === undefined) o.param_types = o.params;
    }
    for (const f of c.fields) fill(f, FIELD_DEFAULTS);
  }
  return index;
}

/** Reads and expands the index at `file` (default the committed one). */
function loadJavaIndex(file = INDEX_PATH) {
  return expand(JSON.parse(fs.readFileSync(file, 'utf8')));
}

let committed = null;

/** The committed index, read once: what the unit tests read from Java. */
function committedJavaIndex() {
  if (committed === null) committed = loadJavaIndex();
  return committed;
}

module.exports = { SCHEMA, INDEX_PATH, COMMAND, expand, loadJavaIndex, committedJavaIndex };
