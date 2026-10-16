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
//! The map is the only input and the handle is opaque, as in Rust: a typed
//! getter would freeze a key's Java type, which Java changes in minors, and a
//! typed setter would skip the `ConfigDef` validators `new` runs.

use std::collections::HashMap;

use crate::consumer::ConsumerConfig;
use crate::ffi::common::fail_with;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::ffi_guard;
use crate::ffi::util::{kafka_Map_t, map_strings};

/// Opaque handle to a validated [`ConsumerConfig`].
#[repr(C)]
pub struct kafka_consumer_ConsumerConfig_t {
    _private: [u8; 0],
}

/// The configuration behind a handle.
///
/// # Safety
///
/// `config` must be a live handle, kept alive for `'a`.
pub(crate) unsafe fn consumer_config_ref<'a>(config: *const kafka_consumer_ConsumerConfig_t) -> &'a ConsumerConfig {
    // SAFETY: Per this function's `# Safety`, `config` is a live handle kept alive for `'a`: a
    // `Box<ConsumerConfig>` from `Box::into_raw` in `kafka_consumer_ConsumerConfig_new`, the only
    // constructor, which nothing mutates afterwards, so the shared reference is to a live value no
    // `&mut` aliases.
    unsafe { &*(config as *const ConsumerConfig) }
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
        Ok(config) => Box::into_raw(Box::new(config)) as *mut kafka_consumer_ConsumerConfig_t,
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
        // handle not used afterwards. Every config handle is a `Box<ConsumerConfig>` from
        // `Box::into_raw` in `kafka_consumer_ConsumerConfig_new`, owned by the caller, so taking it
        // back and dropping it frees it exactly once.
        unsafe { drop(Box::from_raw(self_ as *mut ConsumerConfig)) };
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::ffi::common::{kafka_common_Error_destroy, kafka_common_Error_message};
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
    /// crate reads the result through its typed getters; C gets none (module
    /// docs).
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
