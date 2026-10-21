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

//! `kafka_producer_ProducerConfig_t`:
//! `org.apache.kafka.clients.producer.ProducerConfig` (CLAUDE.md §4).
//!
//! Built from a `kafka_Map_t` of C strings, the way Rust's
//! `ProducerConfig::new` takes a `HashMap<String, String>`; validation
//! happens here, so `kafka_producer_KafkaProducer_new` only sees a valid
//! configuration. The partitioner is not a configuration value (CLAUDE.md §2:
//! no classes in properties) but set through
//! [`kafka_producer_ProducerConfig_set_partitioner`], as in Rust.
//!
//! A `ProducerConfig` is not `Clone` and `KafkaProducer::new` takes it by
//! value, while the C constructor borrows the handle; the handle therefore
//! keeps the configuration validated in [`kafka_producer_ProducerConfig_new`],
//! which answers `AbstractConfig`'s getters
//! (`kafka_producer_ProducerConfig_get_int`, ...), and each producer created
//! from it gets a copy of that configuration, so one configuration can build
//! several producers. Like Java's producers given one `ProducerConfig`, they
//! share its resolved `client.id`, the one
//! `kafka_producer_ProducerConfig_get_string` answers, and each keeps its own
//! record of the keys it used.

use std::collections::HashMap;
use std::ffi::c_char;
use std::sync::Arc;

use crate::common::Error;
use crate::ffi::common::config::config_getters::{self, LentStrings};
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::common::{fail_with, init_default_logger};
use crate::ffi::ffi_guard;
use crate::ffi::producer::partitioner::{
    SharedPartitioner, SharedPartitionerAdapter, kafka_producer_Partitioner_t, take_partitioner,
};
use crate::ffi::util::{kafka_List_t, kafka_Map_t, map_strings, sorted_string_list};
use crate::producer::ProducerConfig;

/// Opaque handle to a validated [`ProducerConfig`].
#[repr(C)]
pub struct kafka_producer_ProducerConfig_t {
    _private: [u8; 0],
}

pub(crate) struct ProducerConfigInner {
    /// The configuration the getters read, copied for each producer
    /// ([`Self::build`]).
    config: ProducerConfig,
    partitioner: Option<Arc<SharedPartitioner>>,
    /// The values [`kafka_producer_ProducerConfig_get_string`] lent out.
    strings: LentStrings,
    /// Test-only: properties that [`Self::build`] parses with
    /// `ProducerConfig::new` in place of the copy, so that it can fail
    /// ([`Self::unvalidated_for_test`]).
    #[cfg(test)]
    unvalidated_props: Option<HashMap<String, String>>,
}

impl ProducerConfigInner {
    /// A handle over `props` that skipped `kafka_producer_ProducerConfig_new`'s
    /// validation, so that [`Self::build`] fails on invalid ones: a test-only
    /// way to reach the build error path of the constructors that take it.
    #[cfg(test)]
    pub(crate) fn unvalidated_for_test(props: HashMap<String, String>) -> *mut kafka_producer_ProducerConfig_t {
        let valid = HashMap::from([("bootstrap.servers".to_string(), "localhost:9092".to_string())]);
        let config = ProducerConfig::new(&valid).expect("a valid configuration");
        Box::into_raw(Box::new(Self {
            config,
            partitioner: None,
            strings: LentStrings::default(),
            unvalidated_props: Some(props),
        })) as *mut kafka_producer_ProducerConfig_t
    }

    /// The configuration of a producer created from the handle: a copy of the
    /// handle's ([`ProducerConfig::copy_for_client`]), with its `client.id`,
    /// its own record of the keys used and the handle's partitioner.
    ///
    /// # Errors
    ///
    /// Fails as [`ProducerConfig::copy_for_client`] does, which it does not in
    /// practice.
    pub(crate) fn build(&self) -> Result<ProducerConfig, Error> {
        #[cfg(test)]
        if let Some(props) = &self.unvalidated_props {
            ProducerConfig::new(props)?;
        }
        let config = self.config.copy_for_client()?;
        Ok(match &self.partitioner {
            Some(partitioner) => config.set_partitioner(Box::new(SharedPartitionerAdapter(Arc::clone(partitioner)))),
            None => config,
        })
    }
}

/// The configuration behind a handle.
///
/// # Safety
///
/// `config` must be a live handle, kept alive for `'a`, during which no
/// [`kafka_producer_ProducerConfig_set_partitioner`] call runs on it.
pub(crate) unsafe fn producer_config_ref<'a>(
    config: *const kafka_producer_ProducerConfig_t,
) -> &'a ProducerConfigInner {
    // SAFETY: Per this function's `# Safety`, `config` is a live handle kept alive for `'a`, i.e.
    // the `Box::into_raw` of a `ProducerConfigInner` in `kafka_producer_ProducerConfig_new`, the
    // only constructor, and no `kafka_producer_ProducerConfig_set_partitioner` call, the only
    // writer (the lent strings and the used keys mutate behind their `Mutex`es), runs on it for
    // `'a`, so the shared reference is sound.
    unsafe { &*(config as *const ProducerConfigInner) }
}

/// `new ProducerConfig(Map<String, String> props)`: validates the
/// properties (the map stays the caller's) and delivers the configuration,
/// owned by the caller ([`kafka_producer_ProducerConfig_destroy`]).
///
/// It first sets up the default stderr logger, as creating a client does, so
/// the warnings the configuration logs while it is built are not lost when it
/// is created before any client; its clients do not log them again.
///
/// # Safety
///
/// `props` must be a live map of C strings and `out_err` a valid slot pointing
/// at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerConfig_new(
    props: *const kafka_Map_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_producer_ProducerConfig_t {
    init_default_logger();
    // SAFETY: Per this function's `# Safety`, `props` is a live map of C strings, as `map_strings`
    // requires; its entries are copied into owned `String`s.
    let props: HashMap<String, String> = unsafe { map_strings(props) }.into_iter().collect();
    let config = match ProducerConfig::new(&props) {
        Ok(config) => config,
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => return unsafe { fail_with(out_err, error) },
    };
    let inner = ProducerConfigInner {
        config,
        partitioner: None,
        strings: LentStrings::default(),
        #[cfg(test)]
        unvalidated_props: None,
    };
    Box::into_raw(Box::new(inner)) as *mut kafka_producer_ProducerConfig_t
}

/// `ProducerConfig::set_partitioner`: the `*mut` parameter consumes an owned
/// `kafka_producer_Partitioner_t` or shares the implementation of an
/// `__as_Partitioner` view (whose class handle the caller keeps alive until
/// the producers built from this configuration are destroyed).
///
/// # Safety
///
/// `self_` and `partitioner` must be live handles, and no other call may use
/// `self_` at the same time.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerConfig_set_partitioner(
    self_: *mut kafka_producer_ProducerConfig_t,
    partitioner: *mut kafka_producer_Partitioner_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, the `Box::into_raw` of a
    // `ProducerConfigInner` in `kafka_producer_ProducerConfig_new`, that no other call uses at the
    // same time, so the exclusive reference, used only within this call, aliases nothing; no view
    // into the handle is ever handed out.
    let inner = unsafe { &mut *(self_ as *mut ProducerConfigInner) };
    // SAFETY: Per this function's `# Safety`, `partitioner` is a live handle, as `take_partitioner`
    // requires; an owned one is freed by that call and not used again by the caller, as the rustdoc
    // states.
    inner.partitioner = Some(unsafe { take_partitioner(partitioner) });
}

/// `getShort(String key)`: the `SHORT` configuration `key`. Returns `1` with the
/// value in `*out_value`, or `0` when the key has no value (Java's `null`),
/// `*out_value` then left unwritten. A key of another type fails with an
/// `IllegalArgumentError`, as Java's cast does, and a key the configuration
/// does not define with Java's `ConfigException`
/// (`Unknown configuration '<key>'`), both through `out_err`. Reading a key
/// marks it used ([`kafka_producer_ProducerConfig_unused`]).
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string;
/// `out_value` valid for a write; `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerConfig_get_short(
    self_: *const kafka_producer_ProducerConfig_t,
    key: *const c_char,
    out_value: *mut i16,
    out_err: *mut *mut kafka_common_Error_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let inner = unsafe { producer_config_ref(self_) };
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
/// marks it used ([`kafka_producer_ProducerConfig_unused`]).
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string;
/// `out_value` valid for a write; `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerConfig_get_int(
    self_: *const kafka_producer_ProducerConfig_t,
    key: *const c_char,
    out_value: *mut i32,
    out_err: *mut *mut kafka_common_Error_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let inner = unsafe { producer_config_ref(self_) };
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
/// marks it used ([`kafka_producer_ProducerConfig_unused`]).
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string;
/// `out_value` valid for a write; `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerConfig_get_long(
    self_: *const kafka_producer_ProducerConfig_t,
    key: *const c_char,
    out_value: *mut i64,
    out_err: *mut *mut kafka_common_Error_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let inner = unsafe { producer_config_ref(self_) };
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
/// marks it used ([`kafka_producer_ProducerConfig_unused`]).
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string;
/// `out_value` valid for a write; `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerConfig_get_double(
    self_: *const kafka_producer_ProducerConfig_t,
    key: *const c_char,
    out_value: *mut f64,
    out_err: *mut *mut kafka_common_Error_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let inner = unsafe { producer_config_ref(self_) };
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
/// marks it used ([`kafka_producer_ProducerConfig_unused`]).
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string;
/// `out_value` valid for a write; `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerConfig_get_boolean(
    self_: *const kafka_producer_ProducerConfig_t,
    key: *const c_char,
    out_value: *mut i8,
    out_err: *mut *mut kafka_common_Error_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let inner = unsafe { producer_config_ref(self_) };
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
/// an unset `PASSWORD` key such as `ssl.keystore.key` answers). `client.id`
/// is the resolved id, generated when the property is unset, under which
/// every producer created from the handle runs. Fails as
/// [`kafka_producer_ProducerConfig_get_int`] does.
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string;
/// `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerConfig_get_string(
    self_: *const kafka_producer_ProducerConfig_t,
    key: *const c_char,
    out_err: *mut *mut kafka_common_Error_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let inner = unsafe { producer_config_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `key` and `out_err` are as `get_string` requires.
    unsafe { config_getters::get_string(&inner.strings, key, out_err, |key| inner.config.get_string(key)) }
}

/// `getList(String key)`: the `LIST` configuration `key`, as an owned list of
/// `char *` the caller frees with `kafka_List_destroy`, or `NULL` with
/// `*out_err` still `NULL` when the key has no value (Java's `null`). Fails
/// as [`kafka_producer_ProducerConfig_get_int`] does.
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string;
/// `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerConfig_get_list(
    self_: *const kafka_producer_ProducerConfig_t,
    key: *const c_char,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let inner = unsafe { producer_config_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `key` and `out_err` are as `get_list` requires.
    unsafe { config_getters::get_list(key, out_err, |key| inner.config.get_list(key)) }
}

/// `unused()`: the keys of the properties never read through this handle's
/// getters nor marked with [`kafka_producer_ProducerConfig_ignore`], as an
/// owned, sorted list of `char *` the caller frees with `kafka_List_destroy`.
/// A producer created from the handle keeps its own record, starting empty.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerConfig_unused(
    self_: *const kafka_producer_ProducerConfig_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    sorted_string_list(unsafe { producer_config_ref(self_) }.config.unused())
}

/// `ignore(String key)`: marks the configuration `key` used without reading
/// it.
///
/// # Safety
///
/// `self_` must be a live handle; `key` a valid NUL-terminated string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerConfig_ignore(
    self_: *const kafka_producer_ProducerConfig_t,
    key: *const c_char,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    let inner = unsafe { producer_config_ref(self_) };
    // SAFETY: Per this function's `# Safety`, `key` is as `ignore` requires.
    unsafe { config_getters::ignore(key, |key| inner.config.ignore(key)) }
}

/// `logUnused()`: logs at INFO, in one line, the keys
/// [`kafka_producer_ProducerConfig_unused`] answers, and nothing when there
/// are none.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerConfig_log_unused(self_: *const kafka_producer_ProducerConfig_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this call.
    unsafe { producer_config_ref(self_) }.config.log_unused();
}

/// `configNames()`: the names of the configurations `ProducerConfig`
/// defines, as an owned, sorted list of `char *` the caller frees with
/// `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_producer_ProducerConfig_config_names() -> *mut kafka_List_t {
    sorted_string_list(ProducerConfig::config_names())
}

/// Frees the handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or a handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerConfig_destroy(self_: *mut kafka_producer_ProducerConfig_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, a handle
        // not yet destroyed, i.e. the `Box::into_raw` of `kafka_producer_ProducerConfig_new`, so
        // `Box::from_raw` reclaims it exactly once.
        drop(unsafe { Box::from_raw(self_ as *mut ProducerConfigInner) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::common::{
        kafka_common_Error__enum, kafka_common_Error_destroy, kafka_common_Error_e, kafka_common_Error_message,
    };
    use crate::ffi::producer::round_robin_partitioner::{
        kafka_producer_RoundRobinPartitioner__as_Partitioner, kafka_producer_RoundRobinPartitioner_destroy,
        kafka_producer_RoundRobinPartitioner_new,
    };
    use crate::ffi::util::{box_string_map, c_str_to_string, kafka_List_destroy, kafka_Map_destroy, list_strings};
    use crate::producer::ProducerConfig;
    use std::ffi::CString;

    /// A handle over `bootstrap.servers` plus `extra`, destroyed by the caller.
    fn config_with(extra: &[(&str, &str)]) -> *mut kafka_producer_ProducerConfig_t {
        let mut entries = vec![("bootstrap.servers", "localhost:9092")];
        entries.extend_from_slice(extra);
        let props = box_string_map(entries);
        let mut error = std::ptr::null_mut();
        // SAFETY: Test code: `props` is the live map just built, destroyed once after `new` copied
        // it; `error` is a valid slot pointing at null.
        unsafe {
            let config = kafka_producer_ProducerConfig_new(props, &mut error);
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
        let config = config_with(&[
            ("request.timeout.ms", "1234"),
            ("enable.idempotence", "false"),
            ("acks", "1"),
        ]);
        let mut error = std::ptr::null_mut();
        let key = |k: &str| CString::new(k).unwrap();
        // SAFETY: Test code: `config` is the live handle built above, destroyed once at the end;
        // the keys are live C strings; the out values are valid locals; `error` is a valid slot,
        // reset to null by `take_error` after each failure.
        unsafe {
            let mut int = 0_i32;
            assert_eq!(
                kafka_producer_ProducerConfig_get_int(config, key("request.timeout.ms").as_ptr(), &mut int, &mut error),
                1
            );
            assert!(error.is_null());
            assert_eq!(int, 1234);

            let mut long = 0_i64;
            assert_eq!(
                kafka_producer_ProducerConfig_get_long(
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
                kafka_producer_ProducerConfig_get_boolean(
                    config,
                    key("enable.idempotence").as_ptr(),
                    &mut boolean,
                    &mut error
                ),
                1
            );
            assert_eq!(boolean, 0);

            // A type mismatch and an unknown key fail through `out_err`.
            let mut short = 7_i16;
            assert_eq!(
                kafka_producer_ProducerConfig_get_short(
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
                kafka_producer_ProducerConfig_get_double(config, key("no.such.key").as_ptr(), &mut double, &mut error),
                0
            );
            assert_eq!(take_error(&mut error), "Unknown configuration 'no.such.key'");

            kafka_producer_ProducerConfig_destroy(config);
        }
    }

    #[test]
    fn string_and_list_getters() {
        let config = config_with(&[("client.id", "producer-a")]);
        let mut error = std::ptr::null_mut();
        let key = |k: &str| CString::new(k).unwrap();
        // SAFETY: Test code: `config` is the live handle built above, destroyed once at the end;
        // the borrowed strings are read while it is alive; `error` is a valid slot.
        unsafe {
            let client_id = kafka_producer_ProducerConfig_get_string(config, key("client.id").as_ptr(), &mut error);
            assert!(error.is_null());
            assert_eq!(c_str_to_string(client_id), "producer-a");
            // The same key lends the same pointer.
            assert_eq!(
                kafka_producer_ProducerConfig_get_string(config, key("client.id").as_ptr(), &mut error),
                client_id
            );
            // Java's `null`: no value and no error.
            assert!(
                kafka_producer_ProducerConfig_get_string(config, key("sasl.jaas.config").as_ptr(), &mut error)
                    .is_null()
            );
            assert!(error.is_null());
            assert!(kafka_producer_ProducerConfig_get_string(config, key("batch.size").as_ptr(), &mut error).is_null());
            assert_eq!(take_error(&mut error), "Configuration 'batch.size' is of type INT, not STRING");

            let servers = kafka_producer_ProducerConfig_get_list(config, key("bootstrap.servers").as_ptr(), &mut error);
            assert!(error.is_null());
            assert_eq!(take_list(servers), ["localhost:9092"]);
            assert!(kafka_producer_ProducerConfig_get_list(config, key("client.id").as_ptr(), &mut error).is_null());
            assert_eq!(take_error(&mut error), "Configuration 'client.id' is of type STRING, not LIST");

            kafka_producer_ProducerConfig_destroy(config);
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
                take_list(kafka_producer_ProducerConfig_unused(config)),
                ["bootstrap.servers", "request.timeout.ms", "some.unknown.key"]
            );
            let mut int = 0_i32;
            kafka_producer_ProducerConfig_get_int(config, key("request.timeout.ms").as_ptr(), &mut int, &mut error);
            kafka_producer_ProducerConfig_ignore(config, key("some.unknown.key").as_ptr());
            assert_eq!(take_list(kafka_producer_ProducerConfig_unused(config)), ["bootstrap.servers"]);
            kafka_producer_ProducerConfig_log_unused(config);

            // A configuration copied for a client starts with nothing used.
            let built = producer_config_ref(config).build().expect("copied");
            assert_eq!(built.unused().len(), 3);

            kafka_producer_ProducerConfig_destroy(config);
        }
        let names = take_list(kafka_producer_ProducerConfig_config_names());
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
        let expected: Vec<String> = {
            let mut v: Vec<String> = ProducerConfig::config_names().into_iter().collect();
            v.sort();
            v
        };
        assert_eq!(names, expected);
        assert!(names.iter().any(|n| n == "partitioner.type"));
    }

    #[test]
    fn new_validates_and_the_handle_rebuilds_the_config() {
        let bad = box_string_map([("bootstrap.servers", "localhost:9092"), ("acks", "many")]);
        let mut err = std::ptr::null_mut();
        // SAFETY: Test code: `err` is a valid slot pointing at null before each call (reset after
        // the error is destroyed), each map and error handle is destroyed once after its last use,
        // the round-robin handle outlives its `__as_Partitioner` view, which the configuration
        // shares, and the configuration handle is destroyed once after its last
        // `producer_config_ref`.
        unsafe {
            let out = kafka_producer_ProducerConfig_new(bad, &raw mut err);
            assert!(!err.is_null());
            assert!(out.is_null());
            assert!(c_str_to_string(kafka_common_Error_message(err)).contains("acks"));
            kafka_common_Error_destroy(err);
            err = std::ptr::null_mut();
            kafka_Map_destroy(bad);

            let good = box_string_map([("bootstrap.servers", "localhost:9092"), ("linger.ms", "5")]);
            let out = kafka_producer_ProducerConfig_new(good, &raw mut err);
            assert!(err.is_null());
            kafka_Map_destroy(good);

            let partitioner = kafka_producer_RoundRobinPartitioner_new();
            kafka_producer_ProducerConfig_set_partitioner(
                out,
                kafka_producer_RoundRobinPartitioner__as_Partitioner(partitioner),
            );
            let config = producer_config_ref(out).build().unwrap();
            assert_eq!(config.linger_ms, 5);
            assert!(config.partitioner.is_some());
            // A second build from the same handle works too.
            assert!(producer_config_ref(out).build().is_ok());
            kafka_producer_ProducerConfig_destroy(out);
            kafka_producer_RoundRobinPartitioner_destroy(partitioner);
            kafka_producer_ProducerConfig_destroy(std::ptr::null_mut());
        }
    }

    /// A SASL `security.protocol` with a mechanism other than `PLAIN` fails
    /// `new` with `LOCAL_UNSUPPORTED_OPERATION`, Java's
    /// `UnsupportedOperationException`.
    #[test]
    fn new_rejects_an_unsupported_sasl_mechanism() {
        let props = box_string_map([
            ("bootstrap.servers", "localhost:9092"),
            ("security.protocol", "SASL_PLAINTEXT"),
            ("sasl.mechanism", "SCRAM-SHA-256"),
        ]);
        let mut err = std::ptr::null_mut();
        // SAFETY: Test code: `err` is a valid slot pointing at null, the map is destroyed once
        // after `new` copied it, and the error is read and then destroyed once by `take_error`.
        unsafe {
            let out = kafka_producer_ProducerConfig_new(props, &raw mut err);
            kafka_Map_destroy(props);
            assert!(out.is_null());
            assert!(!err.is_null());
            assert_eq!(
                kafka_common_Error__enum(err),
                kafka_common_Error_e::kafka_common_Error_LOCAL_UNSUPPORTED_OPERATION
            );
        }
        assert_eq!(
            take_error(&mut err),
            "SASL mechanism 'SCRAM-SHA-256' is not supported by this client; only PLAIN is"
        );
    }
}
