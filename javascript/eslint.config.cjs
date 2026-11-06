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

// The rule linter's ESLint half (scripts/lint/eslint/config.js): `npx eslint .`
// from this directory, or `eslint javascript` from the repository: the
// `kafka/` rules read the client in this directory (`__dirname`), whatever
// ESLint's working directory. `--cache` is not supported for them.

module.exports = require('./scripts/lint/eslint/config').config(__dirname);
