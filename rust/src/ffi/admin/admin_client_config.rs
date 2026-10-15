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
use crate::ffi::common::fail_with;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::ffi_guard;
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
/// `config` must be a live handle, kept alive for `'a`.
pub(crate) unsafe fn admin_client_config_ref<'a>(
    config: *const kafka_admin_AdminClientConfig_t,
) -> &'a AdminClientConfigInner {
    // SAFETY: Per this function's `# Safety`, `config` is a live handle kept alive for `'a`: a
    // `Box<AdminClientConfigInner>` from `Box::into_raw` in `kafka_admin_AdminClientConfig_new`, the
    // only constructor. The handle has no setters and only its destroy takes the box back, so the
    // shared reference is to a live value no `&mut` aliases.
    unsafe { &*(config as *const AdminClientConfigInner) }
}

/// `new AdminClientConfig(Map<String, String> props)`: validates the
/// properties (the map stays the caller's) and delivers the configuration,
/// owned by the caller ([`kafka_admin_AdminClientConfig_destroy`]).
///
/// # Safety
///
/// `props` must be a live map of C strings and `out_err` a valid slot pointing
/// at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClientConfig_new(
    props: *const kafka_Map_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_admin_AdminClientConfig_t {
    // SAFETY: Per this function's `# Safety`, `props` is a live map of C strings, i.e. a valid map
    // whose keys and values are NUL-terminated strings, as `map_strings` requires; they are copied
    // into owned strings.
    let props: HashMap<String, String> = unsafe { map_strings(props) }.into_iter().collect();
    if let Err(error) = AdminClientConfig::new(&props) {
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        return unsafe { fail_with(out_err, error) };
    }
    let inner = AdminClientConfigInner { props };
    Box::into_raw(Box::new(inner)) as *mut kafka_admin_AdminClientConfig_t
}

/// Frees the handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or a handle not yet destroyed, that no other call
/// uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClientConfig_destroy(self_: *mut kafka_admin_AdminClientConfig_t) {
    if !self_.is_null() {
        // SAFETY: Per this function's `# Safety`, `self_` (non-null, checked above) is a handle not
        // yet destroyed, i.e. the `Box<AdminClientConfigInner>` from `Box::into_raw` in
        // `kafka_admin_AdminClientConfig_new`, that no other call uses at the same time and that is
        // not used afterwards, so taking it back and dropping it frees it exactly once.
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
        let mut error = std::ptr::null_mut();
        // SAFETY: Test code: `props` is the live map of C strings just built, destroyed once after
        // `new` copied it; `error` is a valid slot pointing at null, left null on success; `config`
        // is the live handle `new` returned, read only while alive and destroyed once at the end.
        unsafe {
            let config = kafka_admin_AdminClientConfig_new(props, &mut error);
            assert!(error.is_null());
            kafka_Map_destroy(props);

            let built = admin_client_config_ref(config).build().expect("rebuilt");
            assert_eq!(built.request_timeout_ms(), 1234);
            // The handle rebuilds a fresh value for every client created from it.
            admin_client_config_ref(config).build().expect("rebuilt twice");
            kafka_admin_AdminClientConfig_destroy(config);
        }
    }

    #[test]
    fn new_reports_an_invalid_property_through_the_error_slot() {
        let props = box_string_map([
            ("bootstrap.servers", "localhost:9092"),
            ("request.timeout.ms", "not-a-number"),
        ]);
        let mut error = std::ptr::null_mut();
        // SAFETY: Test code: `props` is the live map of C strings just built, destroyed once at the
        // end; `error` is a valid slot pointing at null, then the owned error `new` stored, its
        // message read while it is alive and it is destroyed once; `config` is the null `new`
        // returned on failure.
        unsafe {
            let config = kafka_admin_AdminClientConfig_new(props, &mut error);
            assert!(!error.is_null());
            assert!(config.is_null());
            let message = c_str_to_string(kafka_common_Error_message(error));
            assert!(message.contains("request.timeout.ms"), "{message}");
            kafka_common_Error_destroy(error);
            kafka_Map_destroy(props);
            // Null is a no-op.
            kafka_admin_AdminClientConfig_destroy(config);
        }
    }
}
