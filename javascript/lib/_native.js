// Copyright 2025 Confluent Inc.
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

// Loads the Node-API addon (private) and does its one-time setup: the native
// layer builds its errors and metadata through the JS classes registered
// here, so every module that needs the addon requires this one, and no
// index.js does any setup.
//
// The addon is `confluent-kafka-javascript.node`. Until prebuilt per-platform
// packages exist, the only place it is looked for is the development build,
// build/Release/ (`make -C javascript build`).

const path = require('node:path');

const { fromNative } = require('./_errors');
const { RecordMetadata } = require('./producer/record_metadata');

const ADDON = path.join(__dirname, '..', 'build', 'Release', 'confluent-kafka-javascript.node');

let native;
try {
  native = require(ADDON);
} catch (e) {
  throw new Error(
    `cannot load the native addon ${ADDON}: build it with \`make -C javascript build\` ` +
      `after building libconfluent_kafka (\`make -C rust build-all-features\`)`,
    { cause: e },
  );
}

native.setFactories(
  (fields) => fromNative(fields),
  (fields) => new RecordMetadata(fields),
);

module.exports = native;
