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
//! keeps the validated properties and rebuilds the Rust value for every
//! producer created from it, so one configuration can build several
//! producers (Java's `Properties` can be reused the same way).

use std::collections::HashMap;
use std::sync::Arc;

use crate::common::Error;
use crate::ffi::common::{box_error, kafka_common_Error_t};
use crate::ffi::producer::partitioner::{
    SharedPartitioner, SharedPartitionerAdapter, kafka_producer_Partitioner_t, take_partitioner,
};
use crate::ffi::util::{kafka_Map_t, map_strings};
use crate::producer::ProducerConfig;

/// Opaque handle to a validated [`ProducerConfig`].
#[repr(C)]
pub struct kafka_producer_ProducerConfig_t {
    _private: [u8; 0],
}

pub(crate) struct ProducerConfigInner {
    props: HashMap<String, String>,
    partitioner: Option<Arc<SharedPartitioner>>,
}

impl ProducerConfigInner {
    /// The Rust configuration this handle stands for.
    pub(crate) fn build(&self) -> Result<ProducerConfig, Error> {
        let config = ProducerConfig::new(&self.props)?;
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
/// `config` must be a live handle.
pub(crate) unsafe fn producer_config_ref<'a>(
    config: *const kafka_producer_ProducerConfig_t,
) -> &'a ProducerConfigInner {
    unsafe { &*(config as *const ProducerConfigInner) }
}

/// `new ProducerConfig(Map<String, String> props)`: validates the
/// properties (the map stays the caller's) and delivers the configuration,
/// owned by the caller ([`kafka_producer_ProducerConfig_destroy`]).
///
/// # Safety
///
/// `props` must be a live map of C strings and `out_new` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerConfig_new(
    props: *const kafka_Map_t,
    out_new: *mut *mut kafka_producer_ProducerConfig_t,
) -> *mut kafka_common_Error_t {
    let props: HashMap<String, String> = unsafe { map_strings(props) }.into_iter().collect();
    if let Err(error) = ProducerConfig::new(&props) {
        return box_error(error);
    }
    let inner = ProducerConfigInner { props, partitioner: None };
    unsafe { *out_new = Box::into_raw(Box::new(inner)) as *mut kafka_producer_ProducerConfig_t };
    std::ptr::null_mut()
}

/// `ProducerConfig::set_partitioner`: the `*mut` parameter consumes an owned
/// `kafka_producer_Partitioner_t` or shares the implementation of an
/// `__as_Partitioner` view (whose class handle the caller keeps alive until
/// the producers built from this configuration are destroyed).
///
/// # Safety
///
/// `self_` and `partitioner` must be live handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerConfig_set_partitioner(
    self_: *mut kafka_producer_ProducerConfig_t,
    partitioner: *mut kafka_producer_Partitioner_t,
) {
    let inner = unsafe { &mut *(self_ as *mut ProducerConfigInner) };
    inner.partitioner = Some(unsafe { take_partitioner(partitioner) });
}

/// Frees the handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or a handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_ProducerConfig_destroy(self_: *mut kafka_producer_ProducerConfig_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ProducerConfigInner) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::common::{kafka_common_Error_destroy, kafka_common_Error_message};
    use crate::ffi::producer::round_robin_partitioner::{
        kafka_producer_RoundRobinPartitioner__as_Partitioner, kafka_producer_RoundRobinPartitioner_destroy,
        kafka_producer_RoundRobinPartitioner_new,
    };
    use crate::ffi::util::{box_string_map, c_str_to_string, kafka_Map_destroy};

    #[test]
    fn new_validates_and_the_handle_rebuilds_the_config() {
        let bad = box_string_map([("bootstrap.servers", "localhost:9092"), ("acks", "many")]);
        let mut out = std::ptr::null_mut();
        unsafe {
            let error = kafka_producer_ProducerConfig_new(bad, &raw mut out);
            assert!(!error.is_null());
            assert!(c_str_to_string(kafka_common_Error_message(error)).contains("acks"));
            kafka_common_Error_destroy(error);
            kafka_Map_destroy(bad);

            let good = box_string_map([("bootstrap.servers", "localhost:9092"), ("linger.ms", "5")]);
            assert!(kafka_producer_ProducerConfig_new(good, &raw mut out).is_null());
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
}
