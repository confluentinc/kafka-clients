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
//! `kafka_consumer_ConsumerConfig_get_string`, Java's `AbstractConfig.getString`,
//! not through per-key getters, which would freeze a key's Java type.

use std::collections::HashMap;
use std::ffi::CString;
use std::os::raw::c_char;
use std::sync::{Mutex, PoisonError};

use crate::consumer::ConsumerConfig;
use crate::ffi::common::fail_with;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::ffi_guard;
use crate::ffi::util::{c_str_to_string, kafka_Map_t, map_strings};

/// Opaque handle to a validated [`ConsumerConfig`].
#[repr(C)]
pub struct kafka_consumer_ConsumerConfig_t {
    _private: [u8; 0],
}

/// What a handle points at: the configuration, and the NUL-terminated copies
/// of the values [`kafka_consumer_ConsumerConfig_get_string`] lent out, one
/// per key, kept so the pointers it returns live as long as the handle, as a
/// Rust `&str` borrowed from the configuration does. An entry is never
/// replaced or removed, and a `CString`'s bytes stay put when the map grows.
struct ConsumerConfigHandle {
    config: ConsumerConfig,
    strings: Mutex<HashMap<String, CString>>,
}

/// The handle's state.
///
/// # Safety
///
/// `config` must be a live handle, kept alive for `'a`.
unsafe fn handle_ref<'a>(config: *const kafka_consumer_ConsumerConfig_t) -> &'a ConsumerConfigHandle {
    // SAFETY: Per this function's `# Safety`, `config` is a live handle kept alive for `'a`: a
    // `Box<ConsumerConfigHandle>` from `Box::into_raw` in `kafka_consumer_ConsumerConfig_new`, the
    // only constructor, never reached through a `&mut` afterwards (the string cache mutates behind
    // its `Mutex`), so the shared reference is to a live value no `&mut` aliases.
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
        Ok(config) => Box::into_raw(Box::new(ConsumerConfigHandle { config, strings: Mutex::default() }))
            as *mut kafka_consumer_ConsumerConfig_t,
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// `getString(String key)`: the value of the `STRING` key `key`, borrowed
/// from the handle and valid until it is destroyed, or `NULL` with `*out_err` still
/// `NULL` when the key has no value (Java's `null`). `client.id` is the
/// resolved id, generated when the property is unset. A key of another type
/// (`max.poll.records`) fails as Java's cast does, with an
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
    // SAFETY: Per this function's `# Safety`, `key` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`.
    let key = unsafe { c_str_to_string(key) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this call, as
    // `handle_ref` requires; the reference does not outlive the call.
    let handle = unsafe { handle_ref(self_) };
    match handle.config.get_string(&key) {
        // The pointer outlives the guard: the entry stays in the map, unchanged, until the handle
        // is destroyed (`ConsumerConfigHandle`).
        Ok(Some(value)) => handle
            .strings
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(key)
            .or_insert_with(|| {
                CString::new(value)
                    .expect("a value parsed from NUL-terminated C strings, or a constant name, has no NUL")
            })
            .as_ptr(),
        Ok(None) => std::ptr::null(),
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
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
    use crate::ffi::util::{kafka_Map_destroy, kafka_Map_new, kafka_Map_put};

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
    /// reads a key the configuration holds as a parsed value
    /// (`security.protocol`), fails a key of another type as Java's cast does,
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
            assert_eq!(CStr::from_ptr(protocol).to_str().unwrap(), "SASL_SSL");
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
}
