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

// Random sequences of changes and reads on a record's Headers, against a
// plain-JS model of Java's RecordHeaders (test/memory/_workloads.js, the
// headers model). Each sequence comes from a seeded generator, so a failure
// is reproduced from its seed, which the failure prints: HEADERS_MODEL_SEED
// sets the first seed (default 1) and HEADERS_MODEL_SEEDS how many
// consecutive seeds run (default 40), e.g. `HEADERS_MODEL_SEED=17
// HEADERS_MODEL_SEEDS=1` for the one that failed.

const { runProbe } = require('./_probe');
const { HEADERS_MODEL_PROBE, checkHeadersModel } = require('./_workloads');

const FIRST_SEED = Number(process.env.HEADERS_MODEL_SEED ?? 1);
const SEEDS = Number(process.env.HEADERS_MODEL_SEEDS ?? 40);

test('random sequences on a record’s Headers read as the model of Java’s RecordHeaders, before and after the send seals them', () => {
  const env = { HEADERS_MODEL_SEED: String(FIRST_SEED), HEADERS_MODEL_SEEDS: String(SEEDS) };
  checkHeadersModel(runProbe(HEADERS_MODEL_PROBE, { env, timeout: 300000 }), SEEDS);
});
