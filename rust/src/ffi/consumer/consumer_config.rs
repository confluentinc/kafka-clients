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
//! Rust's setters take `self` by value and return it; C applies them in
//! place. The string getters return pointers borrowed from the handle that
//! stay valid until the next setter call or the handle's destruction.

use std::collections::HashMap;
use std::ffi::{CString, c_char};
use std::sync::Mutex;

use crate::consumer::ConsumerConfig;
use crate::ffi::common::{box_error, kafka_common_Error_t};
use crate::ffi::util::{
    box_string_list, c_str_to_string, kafka_List_t, kafka_Map_t, list_strings, map_strings, owned_c_string,
};

/// Opaque handle to a validated [`ConsumerConfig`].
#[repr(C)]
pub struct kafka_consumer_ConsumerConfig_t {
    _private: [u8; 0],
}

/// What the handle points at: the configuration (`None` only transiently,
/// while a by-value setter runs) and the NUL-terminated copies the string
/// getters hand out, keyed by getter.
pub(crate) struct ConsumerConfigInner {
    config: Option<ConsumerConfig>,
    strings: Mutex<HashMap<&'static str, CString>>,
}

impl ConsumerConfigInner {
    /// The configuration this handle stands for.
    pub(crate) fn config(&self) -> &ConsumerConfig {
        self.config.as_ref().expect("configuration present outside a setter")
    }

    /// A borrowed NUL-terminated copy of `value`, cached under `key` until
    /// the next setter; the heap buffer of a `CString` does not move when
    /// the map rehashes, so the pointer stays valid.
    fn cached(&self, key: &'static str, value: Option<&str>) -> *const c_char {
        let Some(value) = value else {
            return std::ptr::null();
        };
        let mut strings = self.strings.lock().unwrap();
        strings.entry(key).or_insert_with(|| owned_c_string(value)).as_ptr()
    }

    fn set(&mut self, f: impl FnOnce(ConsumerConfig) -> ConsumerConfig) {
        if let Some(config) = self.config.take() {
            self.config = Some(f(config));
        }
        self.strings.lock().unwrap().clear();
    }
}

/// The configuration behind a handle.
///
/// # Safety
///
/// `config` must be a live handle.
pub(crate) unsafe fn consumer_config_ref<'a>(
    config: *const kafka_consumer_ConsumerConfig_t,
) -> &'a ConsumerConfigInner {
    unsafe { &*(config as *const ConsumerConfigInner) }
}

unsafe fn inner_mut<'a>(config: *mut kafka_consumer_ConsumerConfig_t) -> &'a mut ConsumerConfigInner {
    unsafe { &mut *(config as *mut ConsumerConfigInner) }
}

/// `new ConsumerConfig(Map<String, String> props)`: validates the
/// properties (the map stays the caller's) and delivers the configuration,
/// owned by the caller ([`kafka_consumer_ConsumerConfig_destroy`]), or
/// returns the `ConfigException` translation.
///
/// # Safety
///
/// `props` must be null or a valid map of `char *` keys and values;
/// `out_new` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_new(
    props: *const kafka_Map_t,
    out_new: *mut *mut kafka_consumer_ConsumerConfig_t,
) -> *mut kafka_common_Error_t {
    let props: HashMap<String, String> = unsafe { map_strings(props) }.into_iter().collect();
    match ConsumerConfig::new(&props) {
        Ok(config) => {
            let inner = ConsumerConfigInner { config: Some(config), strings: Mutex::new(HashMap::new()) };
            unsafe { *out_new = Box::into_raw(Box::new(inner)) as *mut kafka_consumer_ConsumerConfig_t };
            std::ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// Frees a handle; a null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or a valid handle not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_destroy(self_: *mut kafka_consumer_ConsumerConfig_t) {
    if !self_.is_null() {
        unsafe { drop(Box::from_raw(self_ as *mut ConsumerConfigInner)) };
    }
}

// ---------------------------------------------------------------------------
// Getters (borrowed strings, see the module docs)
// ---------------------------------------------------------------------------

/// `auto.commit.interval.ms`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_auto_commit_interval_ms(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> i32 {
    unsafe { consumer_config_ref(self_) }.config().auto_commit_interval_ms()
}

/// `enable.auto.commit`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_enable_auto_commit(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> i8 {
    i8::from(unsafe { consumer_config_ref(self_) }.config().enable_auto_commit())
}

/// `heartbeat.interval.ms`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_heartbeat_interval_ms(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> i32 {
    unsafe { consumer_config_ref(self_) }.config().heartbeat_interval_ms()
}

/// `max.poll.interval.ms`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_max_poll_interval_ms(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> i32 {
    unsafe { consumer_config_ref(self_) }.config().max_poll_interval_ms()
}

/// `max.poll.records`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_max_poll_records(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> i32 {
    unsafe { consumer_config_ref(self_) }.config().max_poll_records()
}

/// `request.timeout.ms`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_request_timeout_ms(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> i32 {
    unsafe { consumer_config_ref(self_) }.config().request_timeout_ms()
}

/// `retry.backoff.max.ms`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_retry_backoff_max_ms(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> i64 {
    unsafe { consumer_config_ref(self_) }.config().retry_backoff_max_ms()
}

/// `retry.backoff.ms`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_retry_backoff_ms(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> i64 {
    unsafe { consumer_config_ref(self_) }.config().retry_backoff_ms()
}

/// `session.timeout.ms`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_session_timeout_ms(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> i32 {
    unsafe { consumer_config_ref(self_) }.config().session_timeout_ms()
}

/// `internal.throw.on.fetch.stable.offset.unsupported`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_throw_on_fetch_stable_offset_unsupported(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> i8 {
    i8::from(
        unsafe { consumer_config_ref(self_) }
            .config()
            .throw_on_fetch_stable_offset_unsupported(),
    )
}

/// `auto.offset.reset`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_auto_offset_reset(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> *const c_char {
    let inner = unsafe { consumer_config_ref(self_) };
    let value = inner.config().auto_offset_reset();
    inner.cached("auto_offset_reset", Some(value))
}

/// `client.id`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_client_id(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> *const c_char {
    let inner = unsafe { consumer_config_ref(self_) };
    let value = inner.config().client_id();
    inner.cached("client_id", Some(value))
}

/// `group.id`, or `NULL` when unset.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_group_id(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> *const c_char {
    let inner = unsafe { consumer_config_ref(self_) };
    let value = inner.config().group_id();
    inner.cached("group_id", value)
}

/// `group.instance.id`, or `NULL` when unset.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_group_instance_id(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> *const c_char {
    let inner = unsafe { consumer_config_ref(self_) };
    let value = inner.config().group_instance_id();
    inner.cached("group_instance_id", value)
}

/// `group.protocol`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_group_protocol(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> *const c_char {
    let inner = unsafe { consumer_config_ref(self_) };
    let value = inner.config().group_protocol();
    inner.cached("group_protocol", Some(value))
}

/// `group.remote.assignor`, or `NULL` when unset.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_group_remote_assignor(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> *const c_char {
    let inner = unsafe { consumer_config_ref(self_) };
    let value = inner.config().group_remote_assignor();
    inner.cached("group_remote_assignor", value)
}

/// `metadata.recovery.strategy`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_metadata_recovery_strategy(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> *const c_char {
    let inner = unsafe { consumer_config_ref(self_) };
    let value = inner.config().metadata_recovery_strategy();
    inner.cached("metadata_recovery_strategy", Some(value))
}

/// `security.protocol`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_security_protocol(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> *const c_char {
    let inner = unsafe { consumer_config_ref(self_) };
    let value = inner.config().security_protocol();
    inner.cached("security_protocol", Some(value))
}

/// `bootstrap.servers`: an owned list of owned `char *`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_bootstrap_servers(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> *mut kafka_List_t {
    box_string_list(unsafe { consumer_config_ref(self_) }.config().bootstrap_servers())
}

/// `partition.assignment.strategy`: an owned list of owned `char *`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_partition_assignment_strategy(
    self_: *const kafka_consumer_ConsumerConfig_t,
) -> *mut kafka_List_t {
    box_string_list(unsafe { consumer_config_ref(self_) }.config().partition_assignment_strategy())
}

// ---------------------------------------------------------------------------
// Setters (applied in place)
// ---------------------------------------------------------------------------

/// `set_auto_offset_reset`.
///
/// # Safety
///
/// `self_` must be a valid handle and `value` a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_set_auto_offset_reset(
    self_: *mut kafka_consumer_ConsumerConfig_t,
    value: *const c_char,
) {
    let value = unsafe { c_str_to_string(value) };
    unsafe { inner_mut(self_) }.set(|c| c.set_auto_offset_reset(value));
}

/// `set_bootstrap_servers`: a list of `char *`, copied during the call.
///
/// # Safety
///
/// `self_` must be a valid handle and `bootstrap_servers` null or a valid
/// list of strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_set_bootstrap_servers(
    self_: *mut kafka_consumer_ConsumerConfig_t,
    bootstrap_servers: *const kafka_List_t,
) {
    let servers = unsafe { list_strings(bootstrap_servers) };
    unsafe { inner_mut(self_) }.set(|c| c.set_bootstrap_servers(servers));
}

/// `set_client_id`.
///
/// # Safety
///
/// `self_` must be a valid handle and `client_id` a valid NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_set_client_id(
    self_: *mut kafka_consumer_ConsumerConfig_t,
    client_id: *const c_char,
) {
    let client_id = unsafe { c_str_to_string(client_id) };
    unsafe { inner_mut(self_) }.set(|c| c.set_client_id(client_id));
}

/// `set_enable_auto_commit`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_set_enable_auto_commit(
    self_: *mut kafka_consumer_ConsumerConfig_t,
    value: i8,
) {
    unsafe { inner_mut(self_) }.set(|c| c.set_enable_auto_commit(value != 0));
}

/// `set_group_id`.
///
/// # Safety
///
/// `self_` must be a valid handle and `group_id` a valid NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_set_group_id(
    self_: *mut kafka_consumer_ConsumerConfig_t,
    group_id: *const c_char,
) {
    let group_id = unsafe { c_str_to_string(group_id) };
    unsafe { inner_mut(self_) }.set(|c| c.set_group_id(group_id));
}

/// `set_group_protocol`.
///
/// # Safety
///
/// `self_` must be a valid handle and `protocol` a valid NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_set_group_protocol(
    self_: *mut kafka_consumer_ConsumerConfig_t,
    protocol: *const c_char,
) {
    let protocol = unsafe { c_str_to_string(protocol) };
    unsafe { inner_mut(self_) }.set(|c| c.set_group_protocol(protocol));
}

/// `set_request_timeout_ms`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_set_request_timeout_ms(
    self_: *mut kafka_consumer_ConsumerConfig_t,
    value: i32,
) {
    unsafe { inner_mut(self_) }.set(|c| c.set_request_timeout_ms(value));
}

/// `set_retry_backoff_ms`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerConfig_set_retry_backoff_ms(
    self_: *mut kafka_consumer_ConsumerConfig_t,
    value: i64,
) {
    unsafe { inner_mut(self_) }.set(|c| c.set_retry_backoff_ms(value));
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::ffi::util::{kafka_List_destroy, kafka_List_size, kafka_Map_destroy, kafka_Map_new, kafka_Map_put};

    fn config() -> *mut kafka_consumer_ConsumerConfig_t {
        let props = kafka_Map_new();
        let key = c"bootstrap.servers";
        let value = c"localhost:9092";
        unsafe { kafka_Map_put(props, key.as_ptr() as *mut _, value.as_ptr() as *mut _) };
        let mut config = std::ptr::null_mut();
        assert!(unsafe { kafka_consumer_ConsumerConfig_new(props, &mut config) }.is_null());
        unsafe { kafka_Map_destroy(props) };
        config
    }

    #[test]
    fn getters_and_in_place_setters() {
        let config = config();
        unsafe {
            assert!(kafka_consumer_ConsumerConfig_group_id(config).is_null());
            assert_eq!(kafka_consumer_ConsumerConfig_enable_auto_commit(config), 1);
            let servers = kafka_consumer_ConsumerConfig_bootstrap_servers(config);
            assert_eq!(kafka_List_size(servers), 1);
            kafka_List_destroy(servers);

            kafka_consumer_ConsumerConfig_set_group_id(config, c"g".as_ptr());
            kafka_consumer_ConsumerConfig_set_enable_auto_commit(config, 0);
            kafka_consumer_ConsumerConfig_set_request_timeout_ms(config, 1234);
            assert_eq!(
                CStr::from_ptr(kafka_consumer_ConsumerConfig_group_id(config)).to_str().unwrap(),
                "g"
            );
            assert_eq!(kafka_consumer_ConsumerConfig_enable_auto_commit(config), 0);
            assert_eq!(kafka_consumer_ConsumerConfig_request_timeout_ms(config), 1234);
            assert_eq!(consumer_config_ref(config).config().group_id(), Some("g"));
            // The cache is invalidated by the setter.
            kafka_consumer_ConsumerConfig_set_group_id(config, c"h".as_ptr());
            assert_eq!(
                CStr::from_ptr(kafka_consumer_ConsumerConfig_group_id(config)).to_str().unwrap(),
                "h"
            );
            kafka_consumer_ConsumerConfig_destroy(config);
        }
    }
}
