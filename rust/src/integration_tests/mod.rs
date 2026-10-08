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

//! Integration tests against a real Kafka broker in Docker that drive the
//! crate's internals directly (`Selector`, the request/response wrappers,
//! `SslFactory`). They live in the crate rather than under `tests/`, because
//! those types are not public API (`org.apache.kafka.common.{network,
//! requests, protocol, security.ssl}` carry the "not a supported API"
//! disclaimer). The public-API integration suites stay in `tests/integration`.
//!
//! Requires: `cargo test --lib --features integration-tests integration_tests::`

mod common;

mod api_versions_test;
mod cluster_check_test;
mod connection_test;
mod describe_features_test;
mod metadata_test;
mod ssl_sasl_test;
