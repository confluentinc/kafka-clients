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

//! Client quota types.
//!
//! Corresponds to the `org.apache.kafka.common.quota` package.

pub mod client_quota_alteration;
pub mod client_quota_entity;
pub mod client_quota_filter;
pub mod client_quota_filter_component;

pub use client_quota_alteration::{ClientQuotaAlteration, Op};
pub use client_quota_entity::ClientQuotaEntity;
pub use client_quota_filter::ClientQuotaFilter;
pub use client_quota_filter_component::{ClientQuotaFilterComponent, ClientQuotaMatch};
