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

//! Integration tests against a real Kafka broker in Docker.
//!
//! Mirrors Java integration test packages.
//! Requires: `cargo test --features integration-tests`

#![cfg(feature = "integration-tests")]

#[path = "../common/mod.rs"]
mod common;

// Phase 8a — producer-side smoke test (PLAINTEXT, 1000 records, 3 partitions).
mod producer_smoke_test;

// Performance tests — re-enabled in Phase 8a.1 against the Phase 7g
// `Result<KafkaFuture<RecordMetadata>, KafkaError>` send shape.
// `producer_perf_test` stays muted until its Java source is translated.
mod performance_test;
// mod producer_perf_test;

// Phase 9i — CCloud-style external-broker smoke test. Skips when
// `SASL_USERNAME` is unset, so it's safe to include in the default
// integration test set.
mod ccloud_smoke_test;
