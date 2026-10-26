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
use crate::ffi::common::deliver_value;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::common::topic_partition::{TopicPartitionInner, box_topic_partition, topic_partition_key_eq};
use crate::ffi::ffi_guard;
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
/// `oat` must be a valid offset-and-timestamp handle, kept alive for `'a`.
pub(crate) unsafe fn offset_and_timestamp_ref<'a>(
    oat: *const kafka_consumer_OffsetAndTimestamp_t,
) -> &'a OffsetAndTimestamp {
    // SAFETY: Per this function's `# Safety`, `oat` is a valid offset-and-timestamp handle kept
    // alive for `'a`: a `Box<OffsetAndTimestamp>` from `box_offset_and_timestamp`, the only place
    // handles are made, owned by the caller or by a map, and never mutated, so the shared reference
    // is to a live value.
    unsafe { &*(oat as *const OffsetAndTimestamp) }
}

/// Hands a Java `Map<TopicPartition, OffsetAndTimestamp>` to C as an owned
/// map: keys are owned `kafka_common_TopicPartition_t *`, values owned
/// `kafka_consumer_OffsetAndTimestamp_t *`, entries ordered by topic then
/// partition, and `kafka_Map_get` compares keys by value. A partition
/// without an offset (Java's `null` value) has a `NULL` value.
pub(crate) fn offset_and_timestamp_map(map: &HashMap<TopicPartition, Option<OffsetAndTimestamp>>) -> *mut kafka_Map_t {
    let mut entries: Vec<(&TopicPartition, &Option<OffsetAndTimestamp>)> = map.iter().collect();
    entries.sort_by(|(a, _), (b, _)| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
    let entries = entries
        .into_iter()
        .map(|(tp, oat)| {
            (
                box_topic_partition(tp.clone()) as *mut c_void,
                oat.as_ref()
                    .map_or(std::ptr::null_mut(), |oat| box_offset_and_timestamp(oat.clone()) as *mut c_void),
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

/// Returns a constructor's value as an owned handle, or stores its error in
/// `*out_err`.
///
/// # Safety
///
/// `out_err` must be valid for one write and point at null.
unsafe fn deliver(
    oat: Result<OffsetAndTimestamp, Error>,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_consumer_OffsetAndTimestamp_t {
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_value` requires. The
    // handle returned is the fresh box the caller owns, freed with
    // `kafka_consumer_OffsetAndTimestamp_destroy`.
    unsafe { deliver_value(oat, out_err, box_offset_and_timestamp) }
}

/// `new OffsetAndTimestamp(long offset, long timestamp)`. A negative offset stores the Java `IllegalArgumentException` in
/// `*out_err` and returns `NULL`.
///
/// # Parameters
///
/// - `offset` - The offset
/// - `timestamp` - The timestamp
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_argument_error` If the offset or timestamp is negative
///
/// # Safety
///
/// `out_err` must be a valid slot pointing at null.
///
/// The returned handle is owned by the caller, freed with
/// `kafka_consumer_OffsetAndTimestamp_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_new(
    offset: i64,
    timestamp: i64,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_consumer_OffsetAndTimestamp_t {
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer, as `deliver` requires.
    unsafe { deliver(OffsetAndTimestamp::new(offset, timestamp), out_err) }
}

/// `new OffsetAndTimestamp(long offset, long timestamp, Optional<Integer> leaderEpoch)`;
/// a negative `leader_epoch` is `Optional.empty()`.
///
/// # Parameters
///
/// - `offset` - The offset
/// - `timestamp` - The timestamp
/// - `leader_epoch` - The leader epoch
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_argument_error` If the offset or timestamp is negative
///
/// # Safety
///
/// `out_err` must be a valid slot pointing at null.
///
/// The returned handle is owned by the caller, freed with
/// `kafka_consumer_OffsetAndTimestamp_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_with_leader_epoch(
    offset: i64,
    timestamp: i64,
    leader_epoch: i32,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_consumer_OffsetAndTimestamp_t {
    let leader_epoch = (leader_epoch >= 0).then_some(leader_epoch);
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer, as
    // `deliver` requires.
    unsafe { deliver(OffsetAndTimestamp::with_leader_epoch(offset, timestamp, leader_epoch), out_err) }
}

/// Java `toString()`.
///
/// # Returns
///
/// The string form.
///
/// # Safety
///
/// `self_` must be a valid handle.
///
/// The returned string is owned by the caller, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_to_string(
    self_: *const kafka_consumer_OffsetAndTimestamp_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this expression,
    // the only use of the reference, as `offset_and_timestamp_ref` requires.
    into_c_string(&unsafe { offset_and_timestamp_ref(self_) }.to_string())
}

/// `offset()`.
///
/// # Returns
///
/// The offset
///
/// # Safety
///
/// `self_` must be a valid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_offset(
    self_: *const kafka_consumer_OffsetAndTimestamp_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this expression,
    // the only use of the reference, as `offset_and_timestamp_ref` requires.
    unsafe { offset_and_timestamp_ref(self_) }.offset()
}

/// `timestamp()`: milliseconds since the epoch.
///
/// # Returns
///
/// The timestamp
///
/// # Safety
///
/// `self_` must be a valid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_timestamp(
    self_: *const kafka_consumer_OffsetAndTimestamp_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this expression,
    // the only use of the reference, as `offset_and_timestamp_ref` requires.
    unsafe { offset_and_timestamp_ref(self_) }.timestamp()
}

/// `leaderEpoch()`: the epoch, or `-1` for `Optional.empty()`.
///
/// A caught Rust panic returns -2, which is no leader epoch.
///
/// # Returns
///
/// The leader epoch or empty if it is not known
///
/// # Safety
///
/// `self_` must be a valid handle.
#[ffi_guard(fallback = -2)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_leader_epoch(
    self_: *const kafka_consumer_OffsetAndTimestamp_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this expression,
    // the only use of the reference, as `offset_and_timestamp_ref` requires.
    unsafe { offset_and_timestamp_ref(self_) }.leader_epoch().unwrap_or(-1)
}

/// Frees an owned handle; a null pointer is a no-op. A handle borrowed from
/// a map Rust returned is freed by that map's `kafka_Map_destroy`, never
/// here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndTimestamp_destroy(self_: *mut kafka_consumer_OffsetAndTimestamp_t) {
    if !self_.is_null() {
        // SAFETY: Per this function's `# Safety`, `self_` (non-null, checked above) is an owned
        // handle not yet destroyed, i.e. a `Box<OffsetAndTimestamp>` from
        // `box_offset_and_timestamp` that no map owns, so taking it back and dropping it frees it
        // exactly once.
        unsafe { drop(Box::from_raw(self_ as *mut OffsetAndTimestamp)) };
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::ffi::common::topic_partition::topic_partition_ref;
    use crate::ffi::common::{error_ref, kafka_common_Error_destroy};
    use crate::ffi::util::{kafka_Map_destroy, kafka_string_destroy, map_entries};

    #[test]
    fn constructors_and_getters() {
        let mut err = std::ptr::null_mut();
        // SAFETY: Test code: `err` is a valid slot pointing at null.
        let oat = unsafe { kafka_consumer_OffsetAndTimestamp_with_leader_epoch(5, 100, 2, &mut err) };
        assert!(err.is_null());
        // SAFETY: Test code: `oat` is the owned handle just returned (`err` is null, asserted
        // above), destroyed once at the end;
        // the string from `to_string` is destroyed once, after it is read.
        unsafe {
            assert_eq!(kafka_consumer_OffsetAndTimestamp_offset(oat), 5);
            assert_eq!(kafka_consumer_OffsetAndTimestamp_timestamp(oat), 100);
            assert_eq!(kafka_consumer_OffsetAndTimestamp_leader_epoch(oat), 2);
            let s = kafka_consumer_OffsetAndTimestamp_to_string(oat);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), "(timestamp=100, leaderEpoch=2, offset=5)");
            kafka_string_destroy(s);
            kafka_consumer_OffsetAndTimestamp_destroy(oat);
        }
        // SAFETY: Test code: `err` is a valid slot, still pointing at null.
        let plain = unsafe { kafka_consumer_OffsetAndTimestamp_new(5, 100, &mut err) };
        assert!(err.is_null());
        // SAFETY: Test code: `plain` is the owned handle just returned (`err` is null, asserted
        // above).
        assert_eq!(unsafe { kafka_consumer_OffsetAndTimestamp_leader_epoch(plain) }, -1);
        // SAFETY: Test code: `plain` is the owned handle returned above, destroyed once.
        unsafe { kafka_consumer_OffsetAndTimestamp_destroy(plain) };
    }

    #[test]
    fn negative_offset_is_the_java_illegal_argument() {
        let mut error = std::ptr::null_mut();
        // SAFETY: Test code: `error` is a valid slot pointing at null.
        let oat = unsafe { kafka_consumer_OffsetAndTimestamp_new(-1, 0, &mut error) };
        assert!(!error.is_null());
        assert!(oat.is_null());
        // SAFETY: Test code: `error` is the owned error just stored (non-null, asserted above).
        assert!(unsafe { error_ref(error) }.error.is_local_illegal_argument_error());
        // SAFETY: Test code: `error` is the owned error stored above, destroyed once.
        unsafe { kafka_common_Error_destroy(error) };
    }

    /// Java's `offsetsForTimes` maps a partition without an offset for its timestamp to `null`:
    /// the C map keeps the partition as a key, with a `NULL` value, and frees only the non-`NULL`
    /// values.
    #[test]
    fn map_keeps_a_partition_without_an_offset_as_a_null_value() {
        let map = HashMap::from([
            (TopicPartition::new("a", 0), Some(OffsetAndTimestamp::new(5, 100).unwrap())),
            (TopicPartition::new("a", 1), None),
        ]);
        let c_map = offset_and_timestamp_map(&map);
        // SAFETY: Test code: `c_map` is the live map just built, not mutated while its entries
        // are read; every key is a live topic-partition handle and every non-null value a live
        // offset-and-timestamp handle the map owns.
        let entries: Vec<_> = unsafe { map_entries(c_map) }
            .iter()
            .map(|&(k, v)| unsafe {
                (
                    topic_partition_ref(k as *const _).clone(),
                    (!v.is_null()).then(|| offset_and_timestamp_ref(v as *const _).offset()),
                )
            })
            .collect();
        assert_eq!(
            entries,
            vec![
                (TopicPartition::new("a", 0), Some(5)),
                (TopicPartition::new("a", 1), None)
            ]
        );
        // SAFETY: Test code: `c_map` is the map built above, destroyed once; its destructor skips
        // the `NULL` value.
        unsafe { kafka_Map_destroy(c_map) };
    }
}
