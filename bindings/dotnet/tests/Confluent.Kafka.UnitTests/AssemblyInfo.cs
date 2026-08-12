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

using Xunit;

// Run the whole assembly's tests SEQUENTIALLY (no cross-collection parallelism).
//
// Each test owns a Kafka consumer, which is single-owner / not thread-safe by contract
// (ffi-marshalling.md §B1, consumer-threading §§1/2). xUnit's default is to run test
// COLLECTIONS in parallel, so several tests would create / drive / tear down their own
// native consumers concurrently across threads. Teardown is fire-and-forget on the
// native side (Consumer_destroy detaches — does not join — the callback dispatcher,
// ffi §B2/§B7), so a completion callback for one consumer can still be firing on its
// foreign dispatcher thread while another test's teardown + GC runs in parallel — the
// accepted single-owner residual (an unawaited-op straggler callback after destroy).
// Under parallel execution that inter-test race intermittently crashes the test host
// (a pre-existing flake since the M3 completion bridge; the fix is a Rust-core dispatcher
// join on destroy, out of scope here). Serial execution removes the cross-test race
// without weakening any assertion — the ops within a test are already serialized by the
// single-owner core guard. This is the standard harness setting for a not-thread-safe
// native-resource suite.
[assembly: CollectionBehavior(DisableTestParallelization = false)]
