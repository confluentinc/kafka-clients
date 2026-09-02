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

//! Performance integration tests against a real Kafka broker in Docker.
//!
//! Split out of the `integration` target on purpose. These tests assert
//! **latency and throughput budgets**, so they are only meaningful on an
//! otherwise-idle machine: run concurrently with the ~138 functional
//! integration tests they contend for CPU, memory and Docker, and their p99
//! budgets fail for reasons that have nothing to do with the code under test.
//! Keeping them in a separate binary means a plain
//! `cargo test --features integration-tests --test integration` can never
//! schedule them alongside functional tests.
//!
//! Requires: `cargo test --features integration-tests --test performance`
//! (or `make test-integration-perf-rust`, which runs them after the
//! functional suite has finished and released its clusters).
//!
//! The Python counterparts live in `bindings/python/test/performance` and are
//! likewise excluded from the normal `pytest test/unit` run; see
//! `make test-integration-perf-python`.

#[path = "../common/mod.rs"]
mod common;

mod producer_perf_test;
mod transactional_producer_perf_test;
