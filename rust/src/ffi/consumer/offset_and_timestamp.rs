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

//! `kafka_consumer_OffsetAndTimestamp_t`:
//! `org.apache.kafka.clients.consumer.OffsetAndTimestamp`, the value of
//! `offsetsForTimes`.

use std::collections::HashMap;
use std::ffi::{c_char, c_void};

use crate::common::{Error, TopicPartition};
use crate::consumer::OffsetAndTimestamp;
use crate::ffi::common::topic_partition::{TopicPartitionInner, box_topic_partition, topic_partition_key_eq};
use crate::ffi::common::{box_error, kafka_common_Error_t};
use crate::ffi::util::{box_map, destroy_boxed, into_c_string, kafka_Map_t};

/// Opaque handle to an [`OffsetAndTimestamp`].
#[repr(C)]
pub struct kafka_consumer_OffsetAndTimestamp_t {
    _private: [u8; 0],
}

/// Hands `oat` to C as an owned handle, freed with
/// [`kafka_consumer_OffsetAndTimestamp_destroy`] or by the map owning it.
pub(crate) fn box_offset_and_timestamp(oat: OffsetAndTimestamp) -> *mut kafka_consumer_OffsetAndTimestamp_t {
    Box::into_raw(Box::new(oat)) as *mut kafka_consumer_OffsetAndTimestamp_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `oat` must be a valid offset-and-timestamp handle.
pub(crate) unsafe fn offset_and_timestamp_ref<'a>(
    oat: *const kafka_consumer_OffsetAndTimestamp_t,
) -> &'a OffsetAndTimestamp {
    unsafe { &*(oat as *const OffsetAndTimestamp) }
}

/// Hands a Java `Map<TopicPartition, OffsetAndTimestamp>` to C as an owned
/// map: keys are owned `kafka_common_TopicPartition_t *`, values owned
/// `kafka_consumer_OffsetAndTimestamp_t *`, entries ordered by topic then
/// partition, and `kafka_Map_get` compares keys by value. An unresolved
/// partition is absent (see the Rust `offsets_for_times` contract note).
pub(crate) fn offset_and_timestamp_map(map: &HashMap<TopicPartition, OffsetAndTimestamp>) -> *mut kafka_Map_t {
    let mut entries: Vec<(&TopicPartition, &OffsetAndTimestamp)> = map.iter().collect();
    entries.sort_by(|(a, _), (b, _)| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
    let entries = entries
        .into_iter()
        .map(|(tp, oat)| {
            (
                box_topic_partition(tp.clone()) as *mut c_void,
                box_offset_and_timestamp(oat.clone()) as *mut c_void,
            )
        })
        .collect();
    box_map(
        entries,
        Some(destroy_boxed::<TopicPartitionInner>),
        Some(destroy_boxed::<OffsetAndTimestamp>),
        Some(topic_partition_key_eq),
    )
}

unsafe fn deliver(
    oat: Result<OffsetAndTimestamp, Error>,
    out: *mut *mut kafka_consumer_OffsetAndTimestamp_t,
) -> *mut kafka_common_Error_t {
    match oat {
        Ok(oat) => {
            unsafe { *out = box_offset_and_timestamp(oat) };
            std::ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// `new OffsetAndTimestamp(long offset, long timestamp)`: an owned handle
/// delivered through `out_new`. A negative offset fails with the Java
/// `IllegalArgumentException`.
///
/// # Safety
///
/// `out_new` must be a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_new(
    offset: i64,
    timestamp: i64,
    out_new: *mut *mut kafka_consumer_OffsetAndTimestamp_t,
) -> *mut kafka_common_Error_t {
    unsafe { deliver(OffsetAndTimestamp::new(offset, timestamp), out_new) }
}

/// `new OffsetAndTimestamp(long offset, long timestamp, Optional<Integer> leaderEpoch)`;
/// a negative `leader_epoch` is `Optional.empty()`.
///
/// # Safety
///
/// `out_with_leader_epoch` must be a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_with_leader_epoch(
    offset: i64,
    timestamp: i64,
    leader_epoch: i32,
    out_with_leader_epoch: *mut *mut kafka_consumer_OffsetAndTimestamp_t,
) -> *mut kafka_common_Error_t {
    let leader_epoch = (leader_epoch >= 0).then_some(leader_epoch);
    unsafe {
        deliver(
            OffsetAndTimestamp::with_leader_epoch(offset, timestamp, leader_epoch),
            out_with_leader_epoch,
        )
    }
}

/// Java `toString()`: an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_to_string(
    self_: *const kafka_consumer_OffsetAndTimestamp_t,
) -> *mut c_char {
    into_c_string(&unsafe { offset_and_timestamp_ref(self_) }.to_string())
}

/// `offset()`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_offset(
    self_: *const kafka_consumer_OffsetAndTimestamp_t,
) -> i64 {
    unsafe { offset_and_timestamp_ref(self_) }.offset()
}

/// `timestamp()`: milliseconds since the epoch.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_timestamp(
    self_: *const kafka_consumer_OffsetAndTimestamp_t,
) -> i64 {
    unsafe { offset_and_timestamp_ref(self_) }.timestamp()
}

/// `leaderEpoch()`: the epoch, or `-1` for `Optional.empty()`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_leader_epoch(
    self_: *const kafka_consumer_OffsetAndTimestamp_t,
) -> i32 {
    unsafe { offset_and_timestamp_ref(self_) }.leader_epoch().unwrap_or(-1)
}

/// Frees a handle; a null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or a valid handle not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_destroy(self_: *mut kafka_consumer_OffsetAndTimestamp_t) {
    if !self_.is_null() {
        unsafe { drop(Box::from_raw(self_ as *mut OffsetAndTimestamp)) };
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::ffi::common::{error_ref, kafka_common_Error_destroy};
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn constructors_and_getters() {
        let mut oat = std::ptr::null_mut();
        assert!(unsafe { kafka_consumer_OffsetAndTimestamp_with_leader_epoch(5, 100, 2, &mut oat) }.is_null());
        unsafe {
            assert_eq!(kafka_consumer_OffsetAndTimestamp_offset(oat), 5);
            assert_eq!(kafka_consumer_OffsetAndTimestamp_timestamp(oat), 100);
            assert_eq!(kafka_consumer_OffsetAndTimestamp_leader_epoch(oat), 2);
            let s = kafka_consumer_OffsetAndTimestamp_to_string(oat);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), "(timestamp=100, leaderEpoch=2, offset=5)");
            kafka_string_destroy(s);
            kafka_consumer_OffsetAndTimestamp_destroy(oat);
        }
        let mut plain = std::ptr::null_mut();
        assert!(unsafe { kafka_consumer_OffsetAndTimestamp_new(5, 100, &mut plain) }.is_null());
        assert_eq!(unsafe { kafka_consumer_OffsetAndTimestamp_leader_epoch(plain) }, -1);
        unsafe { kafka_consumer_OffsetAndTimestamp_destroy(plain) };
    }

    #[test]
    fn negative_offset_is_the_java_illegal_argument() {
        let mut oat = std::ptr::null_mut();
        let error = unsafe { kafka_consumer_OffsetAndTimestamp_new(-1, 0, &mut oat) };
        assert!(!error.is_null());
        assert!(unsafe { error_ref(error) }.error.is_local_illegal_argument_error());
        unsafe { kafka_common_Error_destroy(error) };
    }
}
