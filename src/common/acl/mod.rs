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
//! Corresponds to the `org.apache.kafka.common.acl` package. Only the
//! [`AclOperation`] and [`AclPermissionType`] enums are translated so far
//! (Milestone 11 Tier 1 dependency for `TopicDescription.authorized_operations`
//! and `DescribeClusterResult`); the rest of `common.acl` (`AclBinding`,
//! `AclBindingFilter`, etc.) arrives with the ACL admin RPCs.

pub mod acl_operation;
pub mod acl_permission_type;

pub use acl_operation::AclOperation;
pub use acl_permission_type::AclPermissionType;
