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

//! Shared test helper module providing access to test-only generated message types
//! (SimpleExampleMessage, NullableStructMessage, SimpleArraysMessage).
//!
//! The generated code uses `crate::common::protocol::*` and `crate::common::Uuid`.
//! When included from integration tests, `crate` refers to the test binary crate,
//! so we re-export the library's `common` module here to satisfy those paths.

// Re-export library types that the generated code references via `crate::common::*`.
// In integration tests, `crate::common` resolves to this module, so we must
// provide `protocol` and `Uuid` here to satisfy those paths.
pub use confluent_kafka_rust::common::Uuid;
pub use confluent_kafka_rust::common::protocol;

#[allow(dead_code, clippy::all)]
mod test_generated {
    include!(concat!(env!("OUT_DIR"), "/test_generated/mod.rs"));
}
pub use test_generated::*;
