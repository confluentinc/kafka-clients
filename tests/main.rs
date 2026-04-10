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

//! Tests for `org.apache.kafka.common` — generated message types and wire protocol.
//!
//! Mirrors the Java test packages `org.apache.kafka.common.message` and
//! `org.apache.kafka.common.protocol`.

mod common;

#[path = "common/message/mod.rs"]
mod message;
#[path = "common/protocol/mod.rs"]
mod protocol;
