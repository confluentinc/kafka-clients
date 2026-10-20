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
//! `AdminClient::create` takes an `AdminClientConfig` by value, while the C
//! constructor borrows the handle; the handle therefore keeps the validated
//! properties and rebuilds the Rust value for every client created from it,
//! so one configuration can build several clients (Java's `Properties` can be
//! reused the same way), each with its own record of the keys it used. The
//! handle also keeps the configuration validated in
//! [`kafka_admin_AdminClientConfig_new`], which answers `AbstractConfig`'s
//! getters (`kafka_admin_AdminClientConfig_get_int`, ...).

use std::collections::HashMap;
use std::ffi::c_char;

use crate::admin::AdminClientConfig;
use crate::common::Error;
use crate::ffi::common::config::config_getters::{self, LentStrings};
use crate::ffi::common::fail_with;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::ffi_guard;
use crate::ffi::util::{kafka_List_t, kafka_Map_t, map_strings, sorted_string_list};

/// Opaque handle to a validated [`AdminClientConfig`].
#[repr(C)]
pub struct kafka_admin_AdminClientConfig_t {
    _private: [u8; 0],
}

pub(crate) struct AdminClientConfigInner {
    props: HashMap<String, String>,
    /// The configuration the getters read.
    config: AdminClientConfig,
    /// The values [`kafka_admin_AdminClientConfig_get_string`] lent out.
    strings: LentStrings,
    /// The documentation [`kafka_admin_AdminClientConfig_documentation_of`]
    /// lent out.
    docs: LentStrings,
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
    // only constructor. The handle has no setters (the lent strings and the used keys mutate behind
    // their `Mutex`es) and only its destroy takes the box back, so the shared reference is to a
    // live value no `&mut` aliases.
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
    let config = match AdminClientConfig::new(&props) {
        Ok(config) => config,
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => return unsafe { fail_with(out_err, error) },
    };
    let inner = AdminClientConfigInner { props, config, strings: LentStrings::default(), docs: LentStrings::default() };
    Box::into_raw(Box::new(inner)) as *mut kafka_admin_AdminClientConfig_t
}

/// `getShort(String key)`: the `SHORT` configuration `key`. Returns `1` with the
/// value in `*out_value`, or `0` when the key has no value (Java's `null`),
/// `*out_value` then left unwritten. A key of another type fails with an
/// `IllegalArgumentError`, as Java's cast does, and a key the configuration
/// does not define with Java's `ConfigException`
/// (`Unknown configuration '<key>'`), both through `out_err`. Reading a key
/// marks it used ([`kafka_admin_AdminClientConfig_unused`]).
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string;
/// `out_value` valid for a write; `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClientConfig_get_short(
    self_: *const kafka_admin_AdminClientConfig_t,
    key: *const c_char,
    out_value: *mut i16,
    out_err: *mut *mut kafka_common_Error_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let inner = unsafe { admin_client_config_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `key`, `out_value` and `out_err` are as
    // `get_scalar` requires.
    unsafe { config_getters::get_scalar(key, out_value, out_err, |key| inner.config.get_short(key)) }
}

/// `getInt(String key)`: the `INT` configuration `key`. Returns `1` with the
/// value in `*out_value`, or `0` when the key has no value (Java's `null`),
/// `*out_value` then left unwritten. A key of another type fails with an
/// `IllegalArgumentError`, as Java's cast does, and a key the configuration
/// does not define with Java's `ConfigException`
/// (`Unknown configuration '<key>'`), both through `out_err`. Reading a key
/// marks it used ([`kafka_admin_AdminClientConfig_unused`]).
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string;
/// `out_value` valid for a write; `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClientConfig_get_int(
    self_: *const kafka_admin_AdminClientConfig_t,
    key: *const c_char,
    out_value: *mut i32,
    out_err: *mut *mut kafka_common_Error_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let inner = unsafe { admin_client_config_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `key`, `out_value` and `out_err` are as
    // `get_scalar` requires.
    unsafe { config_getters::get_scalar(key, out_value, out_err, |key| inner.config.get_int(key)) }
}

/// `getLong(String key)`: the `LONG` configuration `key`. Returns `1` with the
/// value in `*out_value`, or `0` when the key has no value (Java's `null`),
/// `*out_value` then left unwritten. A key of another type fails with an
/// `IllegalArgumentError`, as Java's cast does, and a key the configuration
/// does not define with Java's `ConfigException`
/// (`Unknown configuration '<key>'`), both through `out_err`. Reading a key
/// marks it used ([`kafka_admin_AdminClientConfig_unused`]).
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string;
/// `out_value` valid for a write; `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClientConfig_get_long(
    self_: *const kafka_admin_AdminClientConfig_t,
    key: *const c_char,
    out_value: *mut i64,
    out_err: *mut *mut kafka_common_Error_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let inner = unsafe { admin_client_config_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `key`, `out_value` and `out_err` are as
    // `get_scalar` requires.
    unsafe { config_getters::get_scalar(key, out_value, out_err, |key| inner.config.get_long(key)) }
}

/// `getDouble(String key)`: the `DOUBLE` configuration `key`. Returns `1` with the
/// value in `*out_value`, or `0` when the key has no value (Java's `null`),
/// `*out_value` then left unwritten. A key of another type fails with an
/// `IllegalArgumentError`, as Java's cast does, and a key the configuration
/// does not define with Java's `ConfigException`
/// (`Unknown configuration '<key>'`), both through `out_err`. Reading a key
/// marks it used ([`kafka_admin_AdminClientConfig_unused`]).
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string;
/// `out_value` valid for a write; `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClientConfig_get_double(
    self_: *const kafka_admin_AdminClientConfig_t,
    key: *const c_char,
    out_value: *mut f64,
    out_err: *mut *mut kafka_common_Error_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let inner = unsafe { admin_client_config_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `key`, `out_value` and `out_err` are as
    // `get_scalar` requires.
    unsafe { config_getters::get_scalar(key, out_value, out_err, |key| inner.config.get_double(key)) }
}

/// `getBoolean(String key)`: the `BOOLEAN` configuration `key`. Returns `1` with the
/// value in `*out_value`, or `0` when the key has no value (Java's `null`),
/// `*out_value` then left unwritten. A key of another type fails with an
/// `IllegalArgumentError`, as Java's cast does, and a key the configuration
/// does not define with Java's `ConfigException`
/// (`Unknown configuration '<key>'`), both through `out_err`. Reading a key
/// marks it used ([`kafka_admin_AdminClientConfig_unused`]).
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string;
/// `out_value` valid for a write; `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClientConfig_get_boolean(
    self_: *const kafka_admin_AdminClientConfig_t,
    key: *const c_char,
    out_value: *mut i8,
    out_err: *mut *mut kafka_common_Error_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let inner = unsafe { admin_client_config_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `key`, `out_value` and `out_err` are as
    // `get_scalar` requires.
    unsafe {
        config_getters::get_scalar(key, out_value, out_err, |key| {
            inner.config.get_boolean(key).map(|value| value.map(i8::from))
        })
    }
}

/// `getString(String key)`: the `STRING` configuration `key`, borrowed from
/// the handle and valid until it is destroyed, or `NULL` with `*out_err`
/// still `NULL` when the key has no value (Java's `null`, which is also what
/// an unset `PASSWORD` key such as `ssl.key.password` answers). Fails as
/// [`kafka_admin_AdminClientConfig_get_int`] does.
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string;
/// `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClientConfig_get_string(
    self_: *const kafka_admin_AdminClientConfig_t,
    key: *const c_char,
    out_err: *mut *mut kafka_common_Error_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let inner = unsafe { admin_client_config_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `key` and `out_err` are as `get_string` requires.
    unsafe { config_getters::get_string(&inner.strings, key, out_err, |key| inner.config.get_string(key)) }
}

/// `getList(String key)`: the `LIST` configuration `key`, as an owned list of
/// `char *` the caller frees with `kafka_List_destroy`, or `NULL` with
/// `*out_err` still `NULL` when the key has no value (Java's `null`). Fails
/// as [`kafka_admin_AdminClientConfig_get_int`] does.
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string;
/// `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClientConfig_get_list(
    self_: *const kafka_admin_AdminClientConfig_t,
    key: *const c_char,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let inner = unsafe { admin_client_config_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `key` and `out_err` are as `get_list` requires.
    unsafe { config_getters::get_list(key, out_err, |key| inner.config.get_list(key)) }
}

/// `documentationOf(String key)`: the documentation of the configuration
/// `key`, borrowed from the handle and valid until it is destroyed, or `NULL`
/// when the configuration does not define `key`.
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClientConfig_documentation_of(
    self_: *const kafka_admin_AdminClientConfig_t,
    key: *const c_char,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let inner = unsafe { admin_client_config_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `key` is as `documentation_of` requires.
    unsafe { config_getters::documentation_of(&inner.docs, key, |key| inner.config.documentation_of(key)) }
}

/// `unused()`: the keys of the properties never read through this handle's
/// getters nor marked with [`kafka_admin_AdminClientConfig_ignore`], as an
/// owned, sorted list of `char *` the caller frees with `kafka_List_destroy`.
/// A client created from the handle keeps its own record, starting empty.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClientConfig_unused(
    self_: *const kafka_admin_AdminClientConfig_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    sorted_string_list(unsafe { admin_client_config_ref(self_) }.config.unused())
}

/// `ignore(String key)`: marks the configuration `key` used without reading
/// it.
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClientConfig_ignore(
    self_: *const kafka_admin_AdminClientConfig_t,
    key: *const c_char,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let inner = unsafe { admin_client_config_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `key` is as `ignore` requires.
    unsafe { config_getters::ignore(key, |key| inner.config.ignore(key)) }
}

/// `logUnused()`: logs at INFO, in one line, the keys
/// [`kafka_admin_AdminClientConfig_unused`] answers, and nothing when there
/// are none.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AdminClientConfig_log_unused(self_: *const kafka_admin_AdminClientConfig_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    unsafe { admin_client_config_ref(self_) }.config.log_unused();
}

/// `configNames()`: the names of the configurations `AdminClientConfig`
/// defines, as an owned, sorted list of `char *` the caller frees with
/// `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_AdminClientConfig_config_names() -> *mut kafka_List_t {
    sorted_string_list(AdminClientConfig::config_names())
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
    use crate::ffi::util::{box_string_map, c_str_to_string, kafka_List_destroy, kafka_Map_destroy, list_strings};
    use std::ffi::CString;

    /// A handle over `bootstrap.servers` plus `extra`, destroyed by the caller.
    fn config_with(extra: &[(&str, &str)]) -> *mut kafka_admin_AdminClientConfig_t {
        let mut entries = vec![("bootstrap.servers", "localhost:9092")];
        entries.extend_from_slice(extra);
        let props = box_string_map(entries);
        let mut error = std::ptr::null_mut();
        // SAFETY: Test code: `props` is the live map just built, destroyed once after `new` copied
        // it; `error` is a valid slot pointing at null.
        unsafe {
            let config = kafka_admin_AdminClientConfig_new(props, &mut error);
            assert!(error.is_null());
            kafka_Map_destroy(props);
            config
        }
    }

    /// Takes the owned list `list`, returning its strings.
    fn take_list(list: *mut kafka_List_t) -> Vec<String> {
        assert!(!list.is_null());
        // SAFETY: Test code: `list` is an owned list of C strings just returned, read once and
        // destroyed once.
        unsafe {
            let strings = list_strings(list);
            kafka_List_destroy(list);
            strings
        }
    }

    /// Takes the owned error in `error`, returning its message.
    fn take_error(error: &mut *mut kafka_common_Error_t) -> String {
        assert!(!error.is_null());
        // SAFETY: Test code: `*error` is the owned error just stored, read once and destroyed once.
        unsafe {
            let message = c_str_to_string(kafka_common_Error_message(*error));
            kafka_common_Error_destroy(*error);
            *error = std::ptr::null_mut();
            message
        }
    }

    #[test]
    fn scalar_getters_report_value_null_and_errors() {
        let config = config_with(&[("request.timeout.ms", "1234"), ("enable.metrics.push", "true")]);
        let mut error = std::ptr::null_mut();
        let key = |k: &str| CString::new(k).unwrap();
        // SAFETY: Test code: `config` is the live handle built above, destroyed once at the end;
        // the keys are live C strings; the out values are valid locals; `error` is a valid slot,
        // reset to null by `take_error` after each failure.
        unsafe {
            let mut int = 0_i32;
            assert_eq!(
                kafka_admin_AdminClientConfig_get_int(config, key("request.timeout.ms").as_ptr(), &mut int, &mut error),
                1
            );
            assert!(error.is_null());
            assert_eq!(int, 1234);

            let mut long = 0_i64;
            assert_eq!(
                kafka_admin_AdminClientConfig_get_long(
                    config,
                    key("metadata.max.age.ms").as_ptr(),
                    &mut long,
                    &mut error
                ),
                1
            );
            assert_eq!(long, 300_000);

            let mut boolean = 0_i8;
            assert_eq!(
                kafka_admin_AdminClientConfig_get_boolean(
                    config,
                    key("enable.metrics.push").as_ptr(),
                    &mut boolean,
                    &mut error
                ),
                1
            );
            assert_eq!(boolean, 1);

            // A type mismatch and an unknown key fail through `out_err`.
            let mut short = 7_i16;
            assert_eq!(
                kafka_admin_AdminClientConfig_get_short(
                    config,
                    key("request.timeout.ms").as_ptr(),
                    &mut short,
                    &mut error
                ),
                0
            );
            assert_eq!(
                take_error(&mut error),
                "Configuration 'request.timeout.ms' is of type INT, not SHORT"
            );
            assert_eq!(short, 7);
            let mut double = 0.0_f64;
            assert_eq!(
                kafka_admin_AdminClientConfig_get_double(config, key("no.such.key").as_ptr(), &mut double, &mut error),
                0
            );
            assert_eq!(take_error(&mut error), "Unknown configuration 'no.such.key'");

            kafka_admin_AdminClientConfig_destroy(config);
        }
    }

    #[test]
    fn string_list_and_documentation_getters() {
        let config = config_with(&[("client.id", "admin-1")]);
        let mut error = std::ptr::null_mut();
        let key = |k: &str| CString::new(k).unwrap();
        // SAFETY: Test code: `config` is the live handle built above, destroyed once at the end;
        // the borrowed strings are read while it is alive; `error` is a valid slot.
        unsafe {
            let client_id = kafka_admin_AdminClientConfig_get_string(config, key("client.id").as_ptr(), &mut error);
            assert!(error.is_null());
            assert_eq!(c_str_to_string(client_id), "admin-1");
            // The same key lends the same pointer.
            assert_eq!(
                kafka_admin_AdminClientConfig_get_string(config, key("client.id").as_ptr(), &mut error),
                client_id
            );
            // Java's `null`: no value and no error.
            assert!(
                kafka_admin_AdminClientConfig_get_string(config, key("sasl.jaas.config").as_ptr(), &mut error)
                    .is_null()
            );
            assert!(error.is_null());
            assert!(kafka_admin_AdminClientConfig_get_string(config, key("retries").as_ptr(), &mut error).is_null());
            assert_eq!(take_error(&mut error), "Configuration 'retries' is of type INT, not STRING");

            let servers = kafka_admin_AdminClientConfig_get_list(config, key("bootstrap.servers").as_ptr(), &mut error);
            assert!(error.is_null());
            assert_eq!(take_list(servers), ["localhost:9092"]);
            assert!(kafka_admin_AdminClientConfig_get_list(config, key("client.id").as_ptr(), &mut error).is_null());
            assert_eq!(take_error(&mut error), "Configuration 'client.id' is of type STRING, not LIST");

            let doc = kafka_admin_AdminClientConfig_documentation_of(config, key("client.id").as_ptr());
            assert_eq!(c_str_to_string(doc), crate::CommonClientConfigs::CLIENT_ID_DOC);
            assert!(kafka_admin_AdminClientConfig_documentation_of(config, key("no.such.key").as_ptr()).is_null());

            kafka_admin_AdminClientConfig_destroy(config);
        }
    }

    #[test]
    fn unused_ignore_log_unused_and_config_names() {
        let config = config_with(&[("request.timeout.ms", "1234"), ("some.unknown.key", "x")]);
        let mut error = std::ptr::null_mut();
        let key = |k: &str| CString::new(k).unwrap();
        // SAFETY: Test code: `config` is the live handle built above, destroyed once at the end;
        // `error` is a valid slot; the lists are owned and taken once.
        unsafe {
            assert_eq!(
                take_list(kafka_admin_AdminClientConfig_unused(config)),
                ["bootstrap.servers", "request.timeout.ms", "some.unknown.key"]
            );
            let mut int = 0_i32;
            kafka_admin_AdminClientConfig_get_int(config, key("request.timeout.ms").as_ptr(), &mut int, &mut error);
            kafka_admin_AdminClientConfig_ignore(config, key("some.unknown.key").as_ptr());
            assert_eq!(take_list(kafka_admin_AdminClientConfig_unused(config)), ["bootstrap.servers"]);
            kafka_admin_AdminClientConfig_log_unused(config);

            // A configuration rebuilt for a client starts with nothing used.
            let built = admin_client_config_ref(config).build().expect("rebuilt");
            assert_eq!(built.unused().len(), 3);

            kafka_admin_AdminClientConfig_destroy(config);
        }
        let names = take_list(kafka_admin_AdminClientConfig_config_names());
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
        let expected: Vec<String> = {
            let mut v: Vec<String> = AdminClientConfig::config_names().into_iter().collect();
            v.sort();
            v
        };
        assert_eq!(names, expected);
        assert!(names.iter().any(|n| n == "bootstrap.controllers"));
    }

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
