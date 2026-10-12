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

//! Access control list (ACL) types.
//!
//! Corresponds to the `org.apache.kafka.common.acl` package.

mod access_control_entry;
mod access_control_entry_data;
mod access_control_entry_filter;
mod acl_binding;
mod acl_binding_filter;
mod acl_operation;
mod acl_permission_type;

pub use access_control_entry::AccessControlEntry;
pub use access_control_entry_filter::AccessControlEntryFilter;
pub use acl_binding::AclBinding;
pub use acl_binding_filter::AclBindingFilter;
pub use acl_operation::AclOperation;
pub use acl_permission_type::AclPermissionType;
