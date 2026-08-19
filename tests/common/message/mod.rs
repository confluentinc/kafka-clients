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

//! Tests for `org.apache.kafka.common.message` — generated message types.
//!
//! Mirrors the Java test package `org.apache.kafka.common.message`.

mod generated_messages_test;
mod message_round_trip_test;
mod message_serialization_test;
mod message_test;
mod nullable_struct_message_test;
mod records_serde_test;
mod simple_arrays_message_test;
mod simple_example_message_test;
