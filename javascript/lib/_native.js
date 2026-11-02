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

// Loads the Node-API addon built by `npm run build` / `make -C javascript build`
// (private).

const path = require('node:path');

const ADDON = path.join(__dirname, '..', 'build', 'Release', 'confluent_kafka_js.node');

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

module.exports = native;
