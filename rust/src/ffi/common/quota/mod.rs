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

//! `kafka_common_quota_*`: `org.apache.kafka.common.quota` (CLAUDE.md §4):
//! the four quota classes as owned handles, `ClientQuotaAlteration.Op` as a
//! nested handle and the Rust-only `ClientQuotaMatch` as an enum with two
//! singletons and one owned data variant.

pub(crate) mod client_quota_alteration;
pub(crate) mod client_quota_entity;
pub(crate) mod client_quota_filter;
pub(crate) mod client_quota_filter_component;
pub(crate) mod client_quota_match;
