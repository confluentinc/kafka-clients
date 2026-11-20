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

// The send, future and headers workloads (test/memory/_workloads.js) under
// the garbage collector's stress flags, in probe children with the
// allocator's poisoning: a JS value the addon holds without a reference, or
// uses outside its handle scope, or a pointer it keeps into memory V8 moves,
// fails only when a collection lands there, and these flags make collections
// land everywhere. Each workload must still give byte-exact records and free
// every native handle once.
//
//   --stress-compaction: every full collection compacts the heap, moving the
//     objects it keeps (a small Uint8Array's bytes among them);
//   --stress-scavenge=1: a scavenge at random points of the young generation's
//     filling, about 200 times as many as without;
//   --stress-incremental-marking: incremental marking, with its write
//     barriers, running for small heaps too.
//
// --gc-interval=50 is accepted but does nothing in Node's release builds (V8
// compiles its allocation timeouts only into debug and fuzzing builds: the
// same number of collections with and without it, checked on Node 22.22), so
// --stress-scavenge=1 stands in for a collection every few allocations.

const { runProbe } = require('./_probe');
const { HEADERS_MODEL_PROBE, checkHeadersModel, SEND_PROBE, checkSend, FUTURE_PROBE, checkFutures } = require('./_workloads');

const FLAGS = [['--stress-compaction'], ['--stress-scavenge=1'], ['--stress-incremental-marking']];

// Fewer headers sequences than headers_model.test.js runs, under each flag.
const SEEDS = 16;

for (const flags of FLAGS) {
  const named = flags.join(' ');

  test(`the send workload under ${named}: every record byte-exact, every native handle freed once`, () => {
    checkSend(runProbe(SEND_PROBE, { flags, timeout: 300000 }));
  });

  test(`the future workload under ${named}: every future answers its record, every native handle freed once`, () => {
    checkFutures(runProbe(FUTURE_PROBE, { flags, timeout: 300000 }));
  });

  test(`the headers model under ${named}: every read as the model has it, every native handle freed once`, () => {
    const env = { HEADERS_MODEL_SEED: '1', HEADERS_MODEL_SEEDS: String(SEEDS) };
    checkHeadersModel(runProbe(HEADERS_MODEL_PROBE, { flags, env, timeout: 300000 }), SEEDS);
  });
}
