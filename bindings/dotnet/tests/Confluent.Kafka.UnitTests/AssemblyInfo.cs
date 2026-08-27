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

// Run the whole assembly's tests in PARALLEL — xUnit's default cross-collection parallelism
// is deliberately left enabled (DisableTestParallelization = false).
//
// HISTORY, because this setting has flipped once already and the comment above it did not.
// Each test owns a Kafka consumer, which is single-owner / not thread-safe by contract
// (ffi-marshalling.md §B1, consumer-threading §§1/2). Parallel collections mean several
// tests create / drive / tear down their own native consumers concurrently across threads.
// That used to intermittently crash the TEST HOST, and the mechanism was a genuine
// use-after-free, not a test-harness quirk: ~34 synchronous native call sites passed a raw
// `_handle.DangerousGetHandle()` to native, guarded only by a flag read a few instructions
// earlier, so a teardown on one thread could free a consumer while a call on another thread
// was still executing on it. Serial execution hid the race; it did not fix it.
//
// WHY PARALLEL IS SAFE NOW (M9/P4 H1, decision Q5). Every synchronous consumer P/Invoke
// declares its handle parameter as the SafeConsumerHandle, so the interop marshaller takes a
// DangerousAddRef before the native call and releases it in a finally after — the consumer
// cannot be destroyed out from under a call in progress (ffi §A2). The async submit helpers
// already held a span-the-op reference (M9/P3 `073252f3`), which is what made parallel
// execution defensible enough to enable in the first place; H1 completes the other half.
// A straggler completion callback firing after another test's teardown can therefore no
// longer touch freed memory.
//
// The remaining parallel-execution obligation is in the ALLOCATION tests, and it is
// load-bearing rather than defensive: every allocation assertion MUST use the per-thread
// GC.GetAllocatedBytesForCurrentThread(), never the process-wide GC.GetTotalAllocatedBytes,
// or it will pick up concurrently-running tests' allocations and flake. For the same reason
// the measured region must not `await`. Preserve both in any new allocation test.
//
// Note this comment previously claimed the opposite ("Run the whole assembly's tests
// SEQUENTIALLY") while sitting directly above `= false`, and pointed at "a Rust-core
// dispatcher join on destroy" as the fix. `073252f3` flipped the value and left the comment
// untouched. The dispatcher join is NOT being pursued and is NOT tracked (M9/P4 decision Q3);
// the residuals it would have addressed are accepted permanently — see
// NativeConsumer.Dispose.
[assembly: CollectionBehavior(DisableTestParallelization = false)]
