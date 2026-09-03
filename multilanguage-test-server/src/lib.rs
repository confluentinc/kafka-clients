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

//! Generated tonic stubs for the multilanguage integration test harness.
//!
//! See `proto/producer_service.proto`, `proto/consumer_service.proto` and
//! `proto/admin_service.proto` for the wire schema. All three share the
//! `confluent.kafka.test` package, so a single `include_proto!` pulls in every
//! service. The Python and C++ servers regenerate their own stubs from the same
//! `.proto` files, so this crate is the single source of truth.

/// Generated proto types and gRPC service stubs.
///
/// `producer_service_client::ProducerServiceClient`,
/// `consumer_service_client::ConsumerServiceClient` and
/// `admin_service_client::AdminServiceClient` are the clients used by the Rust
/// `MultilanguageProducer` / `MultilanguageConsumer` / `MultilanguageAdmin`; the
/// matching `*_server` modules are available for in-process Rust reference
/// servers (not currently used).
pub mod proto {
    tonic::include_proto!("confluent.kafka.test");
}
