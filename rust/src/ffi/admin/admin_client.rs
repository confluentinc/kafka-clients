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

//! `org.apache.kafka.clients.admin.AdminClient` (CLAUDE.md §4): the static
//! factory. Nothing constructs an `AdminClient` instance, so there is no
//! `kafka_admin_AdminClient_t`; `create` returns the `kafka_admin_Admin_t`
//! Rust's `Box<dyn Admin>` stands for.

use crate::admin::AdminClient;
use crate::ffi::admin::admin_client_config::{admin_client_config_ref, kafka_admin_AdminClientConfig_t};
use crate::ffi::admin::{AdminClassHandle, kafka_admin_Admin_t, out_slot};
use crate::ffi::common::kafka_common_Error_t;

/// `AdminClient.create(AdminClientConfig)`: builds a `KafkaAdminClient` and
/// delivers it as an owned `Admin_t`, freed with `kafka_admin_Admin_destroy`.
/// The configuration stays the caller's and can build further clients.
///
/// # Safety
///
/// `config` must be a live handle and `out_create` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_create(
    config: *const kafka_admin_AdminClientConfig_t,
    out_create: *mut *mut kafka_admin_Admin_t,
) -> *mut kafka_common_Error_t {
    let config = unsafe { admin_client_config_ref(config) };
    let result = AdminClassHandle::new(|| AdminClient::create(config.build()?));
    unsafe { out_slot(result, out_create, |handle| Box::into_raw(handle) as *mut kafka_admin_Admin_t) }
}
