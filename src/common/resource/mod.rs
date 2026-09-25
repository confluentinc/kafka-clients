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

//! ACL resource types.
//!
//! Corresponds to the `org.apache.kafka.common.resource` package.

mod pattern_type;
// The `Resource` class lives in its own `resource.rs` file per CLAUDE.md's
// "one Java class per file" rule, nested under the `resource` module that
// mirrors the `org.apache.kafka.common.resource` package.
#[allow(clippy::module_inception)]
mod resource;
mod resource_pattern;
mod resource_pattern_filter;
mod resource_type;

pub use pattern_type::PatternType;
pub use resource::Resource;
pub use resource_pattern::ResourcePattern;
pub use resource_pattern_filter::ResourcePatternFilter;
pub use resource_type::ResourceType;
