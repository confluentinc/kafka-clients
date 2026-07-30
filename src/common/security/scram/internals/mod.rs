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

//! Internal SASL/SCRAM helpers
//! (org.apache.kafka.common.security.scram.internals).
//!
//! Package name contains `internals`, so everything here is `pub(crate)`
//! (CLAUDE.md naming conventions).

pub(crate) mod scram_formatter;
pub(crate) mod scram_mechanism;

pub(crate) use scram_formatter::ScramFormatter;
pub(crate) use scram_mechanism::ScramMechanism;
