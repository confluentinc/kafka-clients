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

//! Configuration value types (org.apache.kafka.common.config.types).
//!
//! Java marks this package "not a supported Kafka API" (its `package-info.java`),
//! so it is crate-private (CLAUDE.md §2). No public signature names a type from
//! it, so a user meets a [`Password`] only as the `[hidden]` that a config's
//! `Debug` renders in its place.

mod password;

pub use password::Password;
