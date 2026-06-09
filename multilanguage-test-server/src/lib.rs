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
//! See `proto/producer_service.proto` for the wire schema. The Python
//! and C++ servers regenerate their own stubs from the same `.proto`
//! file, so this crate is the single source of truth.

/// Generated proto types and gRPC service stubs.
///
/// `producer_service_client::ProducerServiceClient` is the client used by
/// the Rust `MultilanguageProducer`; `producer_service_server::ProducerServiceServer`
/// is available for an in-process Rust reference server (not currently used).
pub mod proto {
    tonic::include_proto!("confluent.kafka.test");
}
