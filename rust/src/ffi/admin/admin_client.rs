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
use crate::ffi::admin::{AdminClassHandle, kafka_admin_Admin_t};
use crate::ffi::common::deliver_value;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::ffi_guard;

/// `AdminClient.create(AdminClientConfig)`: builds a `KafkaAdminClient` and
/// delivers it as an owned `Admin_t`, freed with `kafka_admin_Admin_destroy`.
/// The configuration stays the caller's and can build further clients.
///
/// # Errors
///
/// Returns NULL and sets `out_err` to a Kafka error, `Failed to create new
/// KafkaAdminClient`, whose cause is the failure: the bootstrap addresses
/// cannot be resolved, the channel builder cannot be created, or
/// `default.api.timeout.ms` is set explicitly below `request.timeout.ms` (a
/// config error, as Java's `KafkaAdminClient.configureDefaultApiTimeoutMs`
/// throws).
///
/// # Safety
///
/// `config` must be a live handle and `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClient_create(
    config: *const kafka_admin_AdminClientConfig_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_admin_Admin_t {
    // SAFETY: Per this function's `# Safety`, `config` is a live handle, kept alive for this call,
    // which is as long as the reference is used (`AdminClassHandle::new` runs the closure before
    // returning), as `admin_client_config_ref` requires.
    let config = unsafe { admin_client_config_ref(config) };
    let result = AdminClassHandle::new(|| AdminClient::create(config.build()?));
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid slot, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |handle| Box::into_raw(handle) as *mut kafka_admin_Admin_t) }
}
