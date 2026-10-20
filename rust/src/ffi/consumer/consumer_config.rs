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

//! `kafka_consumer_ConsumerConfig_t`:
//! `org.apache.kafka.clients.consumer.ConsumerConfig`.
//!
//! Built from a `kafka_Map_t` of C strings, the way Rust's
//! `ConsumerConfig::new` takes a `HashMap<String, String>`; validation
//! happens here, so `kafka_consumer_KafkaConsumer_new` only sees a valid
//! configuration, which it clones (one configuration can build several
//! consumers, as Java's `Properties` can).
//!
//! The map is the only input, as in Rust: a typed setter would skip the
//! `ConfigDef` validators `new` runs. Values are read back by key through
//! `AbstractConfig`'s getters (`kafka_consumer_ConsumerConfig_get_int`,
//! `kafka_consumer_ConsumerConfig_get_string`, ...), not through per-key
//! getters, which would freeze a key's Java type.

use std::collections::HashMap;
use std::os::raw::c_char;

use crate::consumer::ConsumerConfig;
use crate::ffi::common::config::config_getters::{self, LentStrings};
use crate::ffi::common::fail_with;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::ffi_guard;
use crate::ffi::util::{kafka_List_t, kafka_Map_t, map_strings, sorted_string_list};

/// Opaque handle to a validated [`ConsumerConfig`].
#[repr(C)]
pub struct kafka_consumer_ConsumerConfig_t {
    _private: [u8; 0],
}

/// What a handle points at: the configuration, and the strings its getters
/// lent out, kept so the pointers they return live as long as the handle, as a
/// Rust `&str` borrowed from the configuration does.
struct ConsumerConfigHandle {
    config: ConsumerConfig,
    /// The values [`kafka_consumer_ConsumerConfig_get_string`] lent out.
    strings: LentStrings,
}

/// The handle's state.
///
/// # Safety
///
/// `config` must be a live handle, kept alive for `'a`.
unsafe fn handle_ref<'a>(config: *const kafka_consumer_ConsumerConfig_t) -> &'a ConsumerConfigHandle {
    // SAFETY: Per this function's `# Safety`, `config` is a live handle kept alive for `'a`: a
    // `Box<ConsumerConfigHandle>` from `Box::into_raw` in `kafka_consumer_ConsumerConfig_new`, the
    // only constructor, never reached through a `&mut` afterwards (the lent strings and the used keys
    // mutate behind their `Mutex`es), so the shared reference is to a live value no `&mut` aliases.
    unsafe { &*(config as *const ConsumerConfigHandle) }
}

/// The configuration behind a handle.
///
/// # Safety
///
/// `config` must be a live handle, kept alive for `'a`.
pub(crate) unsafe fn consumer_config_ref<'a>(config: *const kafka_consumer_ConsumerConfig_t) -> &'a ConsumerConfig {
    // SAFETY: Per this function's `# Safety`, `config` is a live handle kept alive for `'a`, as
    // `handle_ref` requires.
    &unsafe { handle_ref(config) }.config
}

/// `new ConsumerConfig(Map<String, String> props)`: validates the
/// properties (the map stays the caller's) and delivers the configuration,
/// owned by the caller ([`kafka_consumer_ConsumerConfig_destroy`]), or
/// returns the `ConfigException` translation.
///
/// # Safety
///
/// `props` must be null or a valid map of `char *` keys and values; `out_err` a
/// valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_new(
    props: *const kafka_Map_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_consumer_ConsumerConfig_t {
    // SAFETY: Per this function's `# Safety`, `props` is null or a valid map of `char *` keys and
    // values, as `map_strings` requires; they are copied into owned strings.
    let props: HashMap<String, String> = unsafe { map_strings(props) }.into_iter().collect();
    match ConsumerConfig::new(&props) {
        Ok(config) => Box::into_raw(Box::new(ConsumerConfigHandle { config, strings: LentStrings::default() }))
            as *mut kafka_consumer_ConsumerConfig_t,
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// `getShort(String key)`: the `SHORT` configuration `key`. Returns `1` with the
/// value in `*out_value`, or `0` when the key has no value (Java's `null`),
/// `*out_value` then left unwritten. A key of another type fails with an
/// `IllegalArgumentError`, as Java's cast does, and a key the configuration
/// does not define with Java's `ConfigException`
/// (`Unknown configuration '<key>'`), both through `out_err`. Reading a key
/// marks it used ([`kafka_consumer_ConsumerConfig_unused`]).
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string;
/// `out_value` valid for a write; `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_get_short(
    self_: *const kafka_consumer_ConsumerConfig_t,
    key: *const c_char,
    out_value: *mut i16,
    out_err: *mut *mut kafka_common_Error_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let handle = unsafe { handle_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `key`, `out_value` and `out_err` are as
    // `get_scalar` requires.
    unsafe { config_getters::get_scalar(key, out_value, out_err, |key| handle.config.get_short(key)) }
}

/// `getInt(String key)`: the `INT` configuration `key`. Returns `1` with the
/// value in `*out_value`, or `0` when the key has no value (Java's `null`),
/// `*out_value` then left unwritten. A key of another type fails with an
/// `IllegalArgumentError`, as Java's cast does, and a key the configuration
/// does not define with Java's `ConfigException`
/// (`Unknown configuration '<key>'`), both through `out_err`. Reading a key
/// marks it used ([`kafka_consumer_ConsumerConfig_unused`]).
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string;
/// `out_value` valid for a write; `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_get_int(
    self_: *const kafka_consumer_ConsumerConfig_t,
    key: *const c_char,
    out_value: *mut i32,
    out_err: *mut *mut kafka_common_Error_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let handle = unsafe { handle_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `key`, `out_value` and `out_err` are as
    // `get_scalar` requires.
    unsafe { config_getters::get_scalar(key, out_value, out_err, |key| handle.config.get_int(key)) }
}

/// `getLong(String key)`: the `LONG` configuration `key`. Returns `1` with the
/// value in `*out_value`, or `0` when the key has no value (Java's `null`),
/// `*out_value` then left unwritten. A key of another type fails with an
/// `IllegalArgumentError`, as Java's cast does, and a key the configuration
/// does not define with Java's `ConfigException`
/// (`Unknown configuration '<key>'`), both through `out_err`. Reading a key
/// marks it used ([`kafka_consumer_ConsumerConfig_unused`]).
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string;
/// `out_value` valid for a write; `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_get_long(
    self_: *const kafka_consumer_ConsumerConfig_t,
    key: *const c_char,
    out_value: *mut i64,
    out_err: *mut *mut kafka_common_Error_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let handle = unsafe { handle_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `key`, `out_value` and `out_err` are as
    // `get_scalar` requires.
    unsafe { config_getters::get_scalar(key, out_value, out_err, |key| handle.config.get_long(key)) }
}

/// `getDouble(String key)`: the `DOUBLE` configuration `key`. Returns `1` with the
/// value in `*out_value`, or `0` when the key has no value (Java's `null`),
/// `*out_value` then left unwritten. A key of another type fails with an
/// `IllegalArgumentError`, as Java's cast does, and a key the configuration
/// does not define with Java's `ConfigException`
/// (`Unknown configuration '<key>'`), both through `out_err`. Reading a key
/// marks it used ([`kafka_consumer_ConsumerConfig_unused`]).
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string;
/// `out_value` valid for a write; `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_get_double(
    self_: *const kafka_consumer_ConsumerConfig_t,
    key: *const c_char,
    out_value: *mut f64,
    out_err: *mut *mut kafka_common_Error_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let handle = unsafe { handle_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `key`, `out_value` and `out_err` are as
    // `get_scalar` requires.
    unsafe { config_getters::get_scalar(key, out_value, out_err, |key| handle.config.get_double(key)) }
}

/// `getBoolean(String key)`: the `BOOLEAN` configuration `key`. Returns `1` with the
/// value in `*out_value`, or `0` when the key has no value (Java's `null`),
/// `*out_value` then left unwritten. A key of another type fails with an
/// `IllegalArgumentError`, as Java's cast does, and a key the configuration
/// does not define with Java's `ConfigException`
/// (`Unknown configuration '<key>'`), both through `out_err`. Reading a key
/// marks it used ([`kafka_consumer_ConsumerConfig_unused`]).
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string;
/// `out_value` valid for a write; `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_get_boolean(
    self_: *const kafka_consumer_ConsumerConfig_t,
    key: *const c_char,
    out_value: *mut i8,
    out_err: *mut *mut kafka_common_Error_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let handle = unsafe { handle_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `key`, `out_value` and `out_err` are as
    // `get_scalar` requires.
    unsafe {
        config_getters::get_scalar(key, out_value, out_err, |key| {
            handle.config.get_boolean(key).map(|value| value.map(i8::from))
        })
    }
}

/// `getList(String key)`: the `LIST` configuration `key`, as an owned list of
/// `char *` the caller frees with `kafka_List_destroy`, or `NULL` with
/// `*out_err` still `NULL` when the key has no value (Java's `null`). Fails
/// as [`kafka_consumer_ConsumerConfig_get_int`] does.
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string;
/// `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_get_list(
    self_: *const kafka_consumer_ConsumerConfig_t,
    key: *const c_char,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let handle = unsafe { handle_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `key` and `out_err` are as `get_list` requires.
    unsafe { config_getters::get_list(key, out_err, |key| handle.config.get_list(key)) }
}

/// `unused()`: the keys of the properties never read through this handle's
/// getters nor marked with [`kafka_consumer_ConsumerConfig_ignore`], as an
/// owned, sorted list of `char *` the caller frees with `kafka_List_destroy`.
/// A consumer created from the handle starts from a copy of this record and
/// keeps its own afterwards.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_unused(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    sorted_string_list(unsafe { handle_ref(self_) }.config.unused())
}

/// `ignore(String key)`: marks the configuration `key` used without reading
/// it.
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_ignore(
    self_: *const kafka_consumer_ConsumerConfig_t,
    key: *const c_char,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let handle = unsafe { handle_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `key` is as `ignore` requires.
    unsafe { config_getters::ignore(key, |key| handle.config.ignore(key)) }
}

/// `logUnused()`: logs at INFO, in one line, the keys
/// [`kafka_consumer_ConsumerConfig_unused`] answers, and nothing when there
/// are none.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_log_unused(self_: *const kafka_consumer_ConsumerConfig_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    unsafe { handle_ref(self_) }.config.log_unused();
}

/// `configNames()`: the names of the configurations `ConsumerConfig`
/// defines, as an owned, sorted list of `char *` the caller frees with
/// `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_ConsumerConfig_config_names() -> *mut kafka_List_t {
    sorted_string_list(ConsumerConfig::config_names())
}

/// `getString(String key)`: the value of the `STRING` key `key`, borrowed
/// from the handle and valid until it is destroyed, or `NULL` with `*out_err` still
/// `NULL` when the key has no value (Java's `null`, which is also what an unset
/// `PASSWORD` key such as `ssl.truststore.password` answers). `client.id` is the
/// resolved id, generated when the property is unset. A key of another type
/// (`max.poll.records`, a set `PASSWORD` key) fails as Java's cast does, with an
/// `IllegalArgumentError`, and a key the configuration does not hold is Java's
/// `ConfigException` (`Unknown configuration '<key>'`), both delivered through
/// `out_err`.
///
/// # Safety
///
/// `self_` must be a valid handle; `key` a valid NUL-terminated string;
/// `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_get_string(
    self_: *const kafka_consumer_ConsumerConfig_t,
    key: *const c_char,
    out_err: *mut *mut kafka_common_Error_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this call, as
    // `handle_ref` requires; the reference does not outlive the call.
    let handle = unsafe { handle_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `key` and `out_err` are as `get_string` requires.
    unsafe { config_getters::get_string(&handle.strings, key, out_err, |key| handle.config.get_string(key)) }
}

/// Frees a handle; a null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or a valid handle not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_destroy(self_: *mut kafka_consumer_ConsumerConfig_t) {
    if !self_.is_null() {
        // SAFETY: Per this function's `# Safety`, `self_` (non-null, checked above) is a valid
        // handle not used afterwards. Every config handle is a `Box<ConsumerConfigHandle>` from
        // `Box::into_raw` in `kafka_consumer_ConsumerConfig_new`, owned by the caller, so taking it
        // back and dropping it frees it exactly once.
        unsafe { drop(Box::from_raw(self_ as *mut ConsumerConfigHandle)) };
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::ffi::common::{kafka_common_Error_destroy, kafka_common_Error_message};
    use crate::ffi::error_predicates::{
        kafka_common_Error_is_config_error, kafka_common_Error_is_local_illegal_argument_error,
    };
    use crate::ffi::util::{
        box_string_map, c_str_to_string, kafka_List_destroy, kafka_Map_destroy, kafka_Map_new, kafka_Map_put,
        list_strings,
    };
    use std::ffi::CString;

    /// A map of C string literals, which outlive it; the map does not own them.
    fn props(entries: &[(&'static CStr, &'static CStr)]) -> *mut kafka_Map_t {
        let props = kafka_Map_new();
        for (key, value) in entries {
            // SAFETY: Test code: `props` is the live map just built, and the key and value are C
            // string literals that outlive it; the map does not own them.
            unsafe { kafka_Map_put(props, key.as_ptr() as *mut _, value.as_ptr() as *mut _) };
        }
        props
    }

    /// The map is validated and parsed as `ConsumerConfig::new` parses it. The
    /// crate reads the result through its typed getters; C reads it by key
    /// (module docs).
    #[test]
    fn new_validates_and_parses_the_map() {
        let props = props(&[
            (c"bootstrap.servers", c"localhost:9092"),
            (c"group.id", c"g"),
            (c"bootstrap.resolve.timeout.ms", c"5000"),
        ]);
        let mut err = std::ptr::null_mut();
        // SAFETY: Test code: `props` is a live map of `char *` keys and values and `err` a valid
        // slot pointing at null; `props` and `config` are destroyed once, after their last use.
        unsafe {
            let config = kafka_consumer_ConsumerConfig_new(props, &mut err);
            assert!(err.is_null());
            kafka_Map_destroy(props);
            let parsed = consumer_config_ref(config);
            assert_eq!(parsed.bootstrap_servers(), ["localhost:9092".to_string()]);
            assert_eq!(parsed.group_id(), Some("g"));
            assert!(parsed.enable_auto_commit());
            assert_eq!(parsed.bootstrap_resolve_timeout_ms(), 5000);
            kafka_consumer_ConsumerConfig_destroy(config);
        }
    }

    /// `getString` lends a set value out of the handle, the same pointer on
    /// every call, answers `NULL` with no error for an unset nullable key,
    /// and for an unset `PASSWORD` key, reads `security.protocol` as written,
    /// fails a key of another type as Java's cast does,
    /// and delivers `Unknown configuration` for a key it does not hold.
    #[test]
    fn get_string_reads_a_string_key() {
        let props = props(&[
            (c"bootstrap.servers", c"localhost:9092"),
            (c"client.id", c"c1"),
            (c"security.protocol", c"sasl_ssl"),
            (c"sasl.mechanism", c"PLAIN"),
        ]);
        let mut err = std::ptr::null_mut();
        // SAFETY: Test code: `props` is a live map of `char *` keys and values, `err` a valid slot
        // pointing at null and the keys C string literals; every returned string is read before
        // `config`, which owns it, is destroyed, and `props`, `config` and the error are destroyed
        // once.
        unsafe {
            let config = kafka_consumer_ConsumerConfig_new(props, &mut err);
            assert!(err.is_null());
            kafka_Map_destroy(props);
            let client_id = kafka_consumer_ConsumerConfig_get_string(config, c"client.id".as_ptr(), &mut err);
            assert!(err.is_null());
            assert_eq!(CStr::from_ptr(client_id).to_str().unwrap(), "c1");
            let again = kafka_consumer_ConsumerConfig_get_string(config, c"client.id".as_ptr(), &mut err);
            assert_eq!(again, client_id);
            let group_id = kafka_consumer_ConsumerConfig_get_string(config, c"group.id".as_ptr(), &mut err);
            assert!(group_id.is_null());
            assert!(err.is_null());
            let protocol = kafka_consumer_ConsumerConfig_get_string(config, c"security.protocol".as_ptr(), &mut err);
            assert!(err.is_null());
            assert_eq!(CStr::from_ptr(protocol).to_str().unwrap(), "sasl_ssl");
            let password = kafka_consumer_ConsumerConfig_get_string(config, c"ssl.key.password".as_ptr(), &mut err);
            assert!(password.is_null());
            assert!(err.is_null());
            let mechanism = kafka_consumer_ConsumerConfig_get_string(config, c"sasl.mechanism".as_ptr(), &mut err);
            assert!(err.is_null());
            assert_eq!(CStr::from_ptr(mechanism).to_str().unwrap(), "PLAIN");
            let other_type = kafka_consumer_ConsumerConfig_get_string(config, c"max.poll.records".as_ptr(), &mut err);
            assert!(other_type.is_null());
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(err), 1);
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(err)).to_str().unwrap(),
                "Configuration 'max.poll.records' is of type INT, not STRING"
            );
            kafka_common_Error_destroy(err);
            err = std::ptr::null_mut();
            let unknown = kafka_consumer_ConsumerConfig_get_string(config, c"no.such.key".as_ptr(), &mut err);
            assert!(unknown.is_null());
            assert_eq!(kafka_common_Error_is_config_error(err), 1);
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(err)).to_str().unwrap(),
                "Unknown configuration 'no.such.key'"
            );
            kafka_common_Error_destroy(err);
            kafka_consumer_ConsumerConfig_destroy(config);
        }
    }

    /// A map without `bootstrap.servers` is Java's `ConfigException`, delivered
    /// through `out_err` with no handle.
    #[test]
    fn new_rejects_a_map_without_bootstrap_servers() {
        let props = props(&[(c"group.id", c"g")]);
        let mut err = std::ptr::null_mut();
        // SAFETY: Test code: `props` is a live map of `char *` keys and values and `err` a valid
        // slot pointing at null; the error's message is read before the error is destroyed, once,
        // and `props` is destroyed once.
        unsafe {
            let config = kafka_consumer_ConsumerConfig_new(props, &mut err);
            assert!(config.is_null());
            assert!(!err.is_null());
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(err)).to_str().unwrap(),
                "Missing required configuration \"bootstrap.servers\" which has no default value."
            );
            kafka_common_Error_destroy(err);
            kafka_Map_destroy(props);
        }
    }

    /// A handle over `bootstrap.servers` plus `extra`, destroyed by the caller.
    fn config_with(extra: &[(&str, &str)]) -> *mut kafka_consumer_ConsumerConfig_t {
        let mut entries = vec![("bootstrap.servers", "localhost:9092")];
        entries.extend_from_slice(extra);
        let props = box_string_map(entries);
        let mut error = std::ptr::null_mut();
        // SAFETY: Test code: `props` is the live map just built, destroyed once after `new` copied
        // it; `error` is a valid slot pointing at null.
        unsafe {
            let config = kafka_consumer_ConsumerConfig_new(props, &mut error);
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

    /// The scalar getters answer `1` with the value, and `0` with the error in
    /// `out_err` for a key of another type or one the configuration does not
    /// define, leaving `*out_value` unwritten.
    #[test]
    fn scalar_getters_report_value_null_and_errors() {
        let config = config_with(&[("group.id", "g"), ("max.poll.records", "123")]);
        let mut error = std::ptr::null_mut();
        let key = |k: &str| CString::new(k).unwrap();
        // SAFETY: Test code: `config` is the live handle built above, destroyed once at the end;
        // the keys are live C strings; the out values are valid locals; `error` is a valid slot,
        // reset to null by `take_error` after each failure.
        unsafe {
            let mut int = 0_i32;
            assert_eq!(
                kafka_consumer_ConsumerConfig_get_int(config, key("max.poll.records").as_ptr(), &mut int, &mut error),
                1
            );
            assert!(error.is_null());
            assert_eq!(int, 123);

            let mut long = 0_i64;
            assert_eq!(
                kafka_consumer_ConsumerConfig_get_long(
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
                kafka_consumer_ConsumerConfig_get_boolean(
                    config,
                    key("enable.auto.commit").as_ptr(),
                    &mut boolean,
                    &mut error
                ),
                1
            );
            assert_eq!(boolean, 1);

            let mut short = 7_i16;
            assert_eq!(
                kafka_consumer_ConsumerConfig_get_short(
                    config,
                    key("max.poll.records").as_ptr(),
                    &mut short,
                    &mut error
                ),
                0
            );
            assert_eq!(
                take_error(&mut error),
                "Configuration 'max.poll.records' is of type INT, not SHORT"
            );
            assert_eq!(short, 7);
            let mut double = 0.0_f64;
            assert_eq!(
                kafka_consumer_ConsumerConfig_get_double(config, key("no.such.key").as_ptr(), &mut double, &mut error),
                0
            );
            assert_eq!(take_error(&mut error), "Unknown configuration 'no.such.key'");

            kafka_consumer_ConsumerConfig_destroy(config);
        }
    }

    /// `getList` hands out an owned list, or fails a key of another type.
    #[test]
    fn list_getter() {
        let config = config_with(&[("client.id", "c1")]);
        let mut error = std::ptr::null_mut();
        let key = |k: &str| CString::new(k).unwrap();
        // SAFETY: Test code: `config` is the live handle built above, destroyed once at the end;
        // the borrowed strings are read while it is alive; `error` is a valid slot.
        unsafe {
            let servers = kafka_consumer_ConsumerConfig_get_list(config, key("bootstrap.servers").as_ptr(), &mut error);
            assert!(error.is_null());
            assert_eq!(take_list(servers), ["localhost:9092"]);
            assert!(kafka_consumer_ConsumerConfig_get_list(config, key("client.id").as_ptr(), &mut error).is_null());
            assert_eq!(take_error(&mut error), "Configuration 'client.id' is of type STRING, not LIST");

            kafka_consumer_ConsumerConfig_destroy(config);
        }
    }

    /// `unused` lists, sorted, the keys neither read nor ignored; `configNames`
    /// lists, sorted, every key `ConsumerConfig` defines.
    #[test]
    fn unused_ignore_log_unused_and_config_names() {
        let config = config_with(&[("max.poll.records", "123"), ("some.unknown.key", "x")]);
        let mut error = std::ptr::null_mut();
        let key = |k: &str| CString::new(k).unwrap();
        // SAFETY: Test code: `config` is the live handle built above, destroyed once at the end;
        // `error` is a valid slot; the lists are owned and taken once.
        unsafe {
            assert_eq!(
                take_list(kafka_consumer_ConsumerConfig_unused(config)),
                ["bootstrap.servers", "max.poll.records", "some.unknown.key"]
            );
            let mut int = 0_i32;
            kafka_consumer_ConsumerConfig_get_int(config, key("max.poll.records").as_ptr(), &mut int, &mut error);
            assert!(error.is_null());
            kafka_consumer_ConsumerConfig_ignore(config, key("some.unknown.key").as_ptr());
            assert_eq!(take_list(kafka_consumer_ConsumerConfig_unused(config)), ["bootstrap.servers"]);
            kafka_consumer_ConsumerConfig_log_unused(config);
            kafka_consumer_ConsumerConfig_destroy(config);
        }
        let names = take_list(kafka_consumer_ConsumerConfig_config_names());
        let mut expected: Vec<String> = ConsumerConfig::config_names().into_iter().collect();
        expected.sort();
        assert_eq!(names, expected);
        assert!(names.iter().any(|n| n == "group.protocol"));
    }
}
