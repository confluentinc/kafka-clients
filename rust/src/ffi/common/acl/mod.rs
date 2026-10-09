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

//! `kafka_common_acl_*`: `org.apache.kafka.common.acl` (CLAUDE.md §4): the two
//! enums as borrowed singletons and the four ACL classes as owned handles whose
//! string getters borrow from the handle.

pub(crate) mod access_control_entry;
pub(crate) mod access_control_entry_filter;
pub(crate) mod acl_binding;
pub(crate) mod acl_binding_filter;
pub(crate) mod acl_operation;
pub(crate) mod acl_permission_type;
