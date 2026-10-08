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

//! `kafka_admin_AdminClientConfig_t`:
//! `org.apache.kafka.clients.admin.AdminClientConfig` (CLAUDE.md §4).
//!
//! Built from a `kafka_Map_t` of C strings, the way Rust's
//! `AdminClientConfig::new` takes a `HashMap<String, String>`; validation
//! happens here, so `kafka_admin_AdminClient_create` only sees a valid
//! configuration.
//!
//! An `AdminClientConfig` is not `Clone` and `AdminClient::create` takes it
//! by value, while the C constructor borrows the handle; the handle therefore
//! keeps the validated properties and rebuilds the Rust value for every
//! client created from it, so one configuration can build several clients
//! (Java's `Properties` can be reused the same way).

use std::collections::HashMap;

use crate::admin::AdminClientConfig;
use crate::common::Error;
use crate::ffi::common::{box_error, kafka_common_Error_t};
use crate::ffi::util::{kafka_Map_t, map_strings};

/// Opaque handle to a validated [`AdminClientConfig`].
#[repr(C)]
pub struct kafka_admin_AdminClientConfig_t {
    _private: [u8; 0],
}

pub(crate) struct AdminClientConfigInner {
    props: HashMap<String, String>,
}

impl AdminClientConfigInner {
    /// The Rust configuration this handle stands for.
    pub(crate) fn build(&self) -> Result<AdminClientConfig, Error> {
        AdminClientConfig::new(&self.props)
    }
}

/// The configuration behind a handle.
///
/// # Safety
///
/// `config` must be a live handle.
pub(crate) unsafe fn admin_client_config_ref<'a>(
    config: *const kafka_admin_AdminClientConfig_t,
) -> &'a AdminClientConfigInner {
    unsafe { &*(config as *const AdminClientConfigInner) }
}

/// `new AdminClientConfig(Map<String, String> props)`: validates the
/// properties (the map stays the caller's) and delivers the configuration,
/// owned by the caller ([`kafka_admin_AdminClientConfig_destroy`]).
///
/// # Safety
///
/// `props` must be a live map of C strings and `out_new` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClientConfig_new(
    props: *const kafka_Map_t,
    out_new: *mut *mut kafka_admin_AdminClientConfig_t,
) -> *mut kafka_common_Error_t {
    let props: HashMap<String, String> = unsafe { map_strings(props) }.into_iter().collect();
    if let Err(error) = AdminClientConfig::new(&props) {
        return box_error(error);
    }
    let inner = AdminClientConfigInner { props };
    unsafe { *out_new = Box::into_raw(Box::new(inner)) as *mut kafka_admin_AdminClientConfig_t };
    std::ptr::null_mut()
}

/// Frees the handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or a handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClientConfig_destroy(self_: *mut kafka_admin_AdminClientConfig_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut AdminClientConfigInner) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::common::{kafka_common_Error_destroy, kafka_common_Error_message};
    use crate::ffi::util::{box_string_map, c_str_to_string, kafka_Map_destroy};

    #[test]
    fn new_validates_and_rebuilds_the_configuration() {
        let props = box_string_map([("bootstrap.servers", "localhost:9092"), ("request.timeout.ms", "1234")]);
        let mut config: *mut kafka_admin_AdminClientConfig_t = std::ptr::null_mut();
        let error = unsafe { kafka_admin_AdminClientConfig_new(props, &mut config) };
        assert!(error.is_null());
        unsafe { kafka_Map_destroy(props) };

        let built = unsafe { admin_client_config_ref(config) }.build().expect("rebuilt");
        assert_eq!(built.request_timeout_ms(), 1234);
        // The handle rebuilds a fresh value for every client created from it.
        unsafe { admin_client_config_ref(config) }.build().expect("rebuilt twice");
        unsafe { kafka_admin_AdminClientConfig_destroy(config) };
    }

    #[test]
    fn new_reports_an_invalid_property_through_the_error_slot() {
        let props = box_string_map([
            ("bootstrap.servers", "localhost:9092"),
            ("request.timeout.ms", "not-a-number"),
        ]);
        let mut config: *mut kafka_admin_AdminClientConfig_t = std::ptr::null_mut();
        let error = unsafe { kafka_admin_AdminClientConfig_new(props, &mut config) };
        assert!(!error.is_null());
        assert!(config.is_null());
        let message = unsafe { c_str_to_string(kafka_common_Error_message(error)) };
        assert!(message.contains("request.timeout.ms"), "{message}");
        unsafe {
            kafka_common_Error_destroy(error);
            kafka_Map_destroy(props);
            // Null is a no-op.
            kafka_admin_AdminClientConfig_destroy(config);
        }
    }
}
