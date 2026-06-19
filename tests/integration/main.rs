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

mod api_versions_test;
mod connection_test;
mod consumer_test;
mod consumer_topic_creation_test;
mod metadata_test;
mod performance_test;
mod plaintext_consumer_assign_test;
mod plaintext_consumer_callback_test;
mod plaintext_consumer_commit_test;
mod plaintext_consumer_fetch_test;
mod plaintext_consumer_poll_test;
mod plaintext_consumer_subscription_test;
mod plaintext_consumer_test;
mod producer_perf_test;
mod producer_test;
mod sasl_ssl_consumer_test;
mod ssl_sasl_test;
