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

//! `kafka_consumer_OffsetAndMetadata_t`:
//! `org.apache.kafka.clients.consumer.OffsetAndMetadata`, plus the
//! `Map<TopicPartition, OffsetAndMetadata>` conversions the commit and
//! `committed` paths share.

use std::collections::HashMap;
use std::ffi::{CString, c_char, c_void};

use crate::common::{Error, TopicPartition};
use crate::consumer::OffsetAndMetadata;
use crate::ffi::common::topic_partition::{
    TopicPartitionInner, box_topic_partition, kafka_common_TopicPartition_t, topic_partition_key_eq,
    topic_partition_ref,
};
use crate::ffi::common::{box_error, kafka_common_Error_t};
use crate::ffi::ffi_guard;
use crate::ffi::util::{box_map, c_str_to_string, destroy_boxed, into_c_string, kafka_Map_t, map_entries};

/// Opaque handle to an [`OffsetAndMetadata`].
#[repr(C)]
pub struct kafka_consumer_OffsetAndMetadata_t {
    _private: [u8; 0],
}

/// What the handle points at: the value and a NUL-terminated copy of its
/// metadata for the borrowed getter.
pub(crate) struct OffsetAndMetadataInner {
    oam: OffsetAndMetadata,
    metadata_c: CString,
}

impl OffsetAndMetadataInner {
    pub(crate) fn new(oam: OffsetAndMetadata) -> Self {
        let metadata_c = CString::new(oam.metadata().as_bytes()).unwrap_or_default();
        Self { oam, metadata_c }
    }
}

/// Hands `oam` to C as an owned handle, freed with
/// [`kafka_consumer_OffsetAndMetadata_destroy`] or by the container owning it.
pub(crate) fn box_offset_and_metadata(oam: OffsetAndMetadata) -> *mut kafka_consumer_OffsetAndMetadata_t {
    Box::into_raw(Box::new(OffsetAndMetadataInner::new(oam))) as *mut kafka_consumer_OffsetAndMetadata_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `oam` must be a valid offset-and-metadata handle, kept alive for `'a`.
pub(crate) unsafe fn offset_and_metadata_ref<'a>(
    oam: *const kafka_consumer_OffsetAndMetadata_t,
) -> &'a OffsetAndMetadata {
    // SAFETY: Per this function's `# Safety`, `oam` is a valid offset-and-metadata handle kept
    // alive for `'a`: a `Box<OffsetAndMetadataInner>` from `box_offset_and_metadata`, the only
    // place handles are made, owned by the caller or by a container, and never mutated, so the
    // shared reference is to a live value.
    &unsafe { &*(oam as *const OffsetAndMetadataInner) }.oam
}

/// Hands a Java `Map<TopicPartition, OffsetAndMetadata>` to C as an owned
/// map: keys are owned `kafka_common_TopicPartition_t *`, values owned
/// `kafka_consumer_OffsetAndMetadata_t *`, entries ordered by topic then
/// partition (a deterministic order for a map Java leaves unordered), and
/// `kafka_Map_get` compares keys by value.
pub(crate) fn offset_and_metadata_map(map: &HashMap<TopicPartition, OffsetAndMetadata>) -> *mut kafka_Map_t {
    let mut entries: Vec<(&TopicPartition, &OffsetAndMetadata)> = map.iter().collect();
    entries.sort_by(|(a, _), (b, _)| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
    let entries = entries
        .into_iter()
        .map(|(tp, oam)| {
            (
                box_topic_partition(tp.clone()) as *mut c_void,
                box_offset_and_metadata(oam.clone()) as *mut c_void,
            )
        })
        .collect();
    box_map(
        entries,
        Some(destroy_boxed::<TopicPartitionInner>),
        Some(destroy_boxed::<OffsetAndMetadataInner>),
        Some(topic_partition_key_eq),
    )
}

/// Reads a map of `const kafka_common_TopicPartition_t *` keys and
/// `const kafka_consumer_OffsetAndMetadata_t *` values; null reads as empty.
///
/// # Safety
///
/// `map` must be null or a valid map whose keys are topic-partition handles
/// and whose values are offset-and-metadata handles, and no other call may
/// mutate it during this one.
pub(crate) unsafe fn map_offset_and_metadata(map: *const kafka_Map_t) -> HashMap<TopicPartition, OffsetAndMetadata> {
    // SAFETY: Per this function's `# Safety`, `map` is null or a valid map that no other call
    // mutates during this one, as `map_entries` requires; the entries are read only within this
    // expression.
    unsafe { map_entries(map) }
        .iter()
        .map(|&(k, v)| {
            // SAFETY: Per this function's `# Safety`, every key of `map` is a topic-partition
            // handle, live during the call, as `topic_partition_ref` requires; the reference is
            // only cloned.
            let tp = unsafe { topic_partition_ref(k as *const kafka_common_TopicPartition_t) }.clone();
            // SAFETY: Per this function's `# Safety`, every value of `map` is an
            // offset-and-metadata handle, live during the call, as `offset_and_metadata_ref`
            // requires; the reference is only cloned.
            let oam = unsafe { offset_and_metadata_ref(v as *const kafka_consumer_OffsetAndMetadata_t) }.clone();
            (tp, oam)
        })
        .collect()
}

/// Delivers a freshly constructed value through `out`, or returns the
/// construction error (CLAUDE.md §4 error slot).
///
/// # Safety
///
/// `out` must be a valid pointer.
unsafe fn deliver(
    oam: Result<OffsetAndMetadata, Error>,
    out: *mut *mut kafka_consumer_OffsetAndMetadata_t,
) -> *mut kafka_common_Error_t {
    match oam {
        Ok(oam) => {
            // SAFETY: Per this function's `# Safety`, `out` is a valid pointer, so the write is
            // sound; the handle written is the fresh box the caller owns, freed with
            // `kafka_consumer_OffsetAndMetadata_destroy`.
            unsafe { *out = box_offset_and_metadata(oam) };
            std::ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// `new OffsetAndMetadata(long offset)`: an owned handle delivered through
/// `out_new`, freed with [`kafka_consumer_OffsetAndMetadata_destroy`]. A
/// negative offset fails with the Java `IllegalArgumentException`
/// (`LocalIllegalArgument`, "Invalid negative offset").
///
/// # Safety
///
/// `out_new` must be a valid pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndMetadata_new(
    offset: i64,
    out_new: *mut *mut kafka_consumer_OffsetAndMetadata_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `out_new` is a valid pointer, as `deliver` requires.
    unsafe { deliver(OffsetAndMetadata::new(offset), out_new) }
}

/// `new OffsetAndMetadata(long offset, String metadata)`; a null `metadata`
/// is Java `null`, which the class stores as the empty string.
///
/// # Safety
///
/// `metadata` must be null or a valid NUL-terminated string; `out_with_metadata`
/// a valid pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndMetadata_with_metadata(
    offset: i64,
    metadata: *const c_char,
    out_with_metadata: *mut *mut kafka_consumer_OffsetAndMetadata_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `metadata` is null or a valid NUL-terminated string,
    // as `c_str_to_string` requires; it is copied into an owned `String`.
    let metadata = unsafe { c_str_to_string(metadata) };
    // SAFETY: Per this function's `# Safety`, `out_with_metadata` is a valid pointer, as `deliver`
    // requires.
    unsafe { deliver(OffsetAndMetadata::with_metadata(offset, metadata), out_with_metadata) }
}

/// `new OffsetAndMetadata(long offset, Optional<Integer> leaderEpoch, String metadata)`;
/// a negative `leader_epoch` is `Optional.empty()`.
///
/// # Safety
///
/// `metadata` must be null or a valid NUL-terminated string;
/// `out_with_leader_epoch_metadata` a valid pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata(
    offset: i64,
    leader_epoch: i32,
    metadata: *const c_char,
    out_with_leader_epoch_metadata: *mut *mut kafka_consumer_OffsetAndMetadata_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `metadata` is null or a valid NUL-terminated string,
    // as `c_str_to_string` requires; it is copied into an owned `String`.
    let metadata = unsafe { c_str_to_string(metadata) };
    let leader_epoch = (leader_epoch >= 0).then_some(leader_epoch);
    // SAFETY: Per this function's `# Safety`, `out_with_leader_epoch_metadata` is a valid pointer,
    // as `deliver` requires.
    unsafe {
        deliver(
            OffsetAndMetadata::with_leader_epoch_metadata(offset, leader_epoch, metadata),
            out_with_leader_epoch_metadata,
        )
    }
}

/// Java `toString()`: an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndMetadata_to_string(
    self_: *const kafka_consumer_OffsetAndMetadata_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this expression,
    // the only use of the reference, as `offset_and_metadata_ref` requires.
    into_c_string(&unsafe { offset_and_metadata_ref(self_) }.to_string())
}

/// `offset()`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndMetadata_offset(
    self_: *const kafka_consumer_OffsetAndMetadata_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this expression,
    // the only use of the reference, as `offset_and_metadata_ref` requires.
    unsafe { offset_and_metadata_ref(self_) }.offset()
}

/// `metadata()`: the commit metadata, empty when none, borrowed from the
/// handle.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndMetadata_metadata(
    self_: *const kafka_consumer_OffsetAndMetadata_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, i.e. a
    // `Box<OffsetAndMetadataInner>` from `box_offset_and_metadata`, never mutated; the pointer
    // returned borrows its `metadata_c`, valid until the handle is freed, as the rustdoc states.
    unsafe { &*(self_ as *const OffsetAndMetadataInner) }.metadata_c.as_ptr()
}

/// `leaderEpoch()`: the epoch, or `-1` for `Optional.empty()`.
///
/// A caught Rust panic returns -2, which is no leader epoch: Java's `leaderEpoch()` answers a negative one as `Optional.empty()`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[ffi_guard(fallback = -2)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetAndMetadata_leader_epoch(
    self_: *const kafka_consumer_OffsetAndMetadata_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this expression,
    // the only use of the reference, as `offset_and_metadata_ref` requires.
    unsafe { offset_and_metadata_ref(self_) }.leader_epoch().unwrap_or(-1)
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
pub unsafe extern "C" fn kafka_consumer_OffsetAndMetadata_destroy(self_: *mut kafka_consumer_OffsetAndMetadata_t) {
    if !self_.is_null() {
        // SAFETY: Per this function's `# Safety`, `self_` (non-null, checked above) is an owned
        // handle not yet destroyed, i.e. a `Box<OffsetAndMetadataInner>` from
        // `box_offset_and_metadata` that no container owns, so taking it back and dropping it frees
        // it exactly once.
        unsafe { drop(Box::from_raw(self_ as *mut OffsetAndMetadataInner)) };
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::ffi::common::{error_ref, kafka_common_Error_destroy};
    use crate::ffi::util::{kafka_Map_destroy, kafka_Map_size, kafka_string_destroy};

    #[test]
    fn constructors_and_getters() {
        let mut oam = std::ptr::null_mut();
        let error =
            // SAFETY: Test code: the metadata is a C string literal and `oam` a valid out pointer.
            unsafe { kafka_consumer_OffsetAndMetadata_with_leader_epoch_metadata(42, 7, c"m".as_ptr(), &mut oam) };
        assert!(error.is_null());
        // SAFETY: Test code: `oam` is the owned handle just delivered (the error is null, asserted
        // above), destroyed once at the end, after the borrowed metadata string is read; the string
        // from `to_string` is destroyed once, after it is read.
        unsafe {
            assert_eq!(kafka_consumer_OffsetAndMetadata_offset(oam), 42);
            assert_eq!(kafka_consumer_OffsetAndMetadata_leader_epoch(oam), 7);
            assert_eq!(
                CStr::from_ptr(kafka_consumer_OffsetAndMetadata_metadata(oam)).to_str().unwrap(),
                "m"
            );
            let s = kafka_consumer_OffsetAndMetadata_to_string(oam);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                "OffsetAndMetadata{offset=42, leaderEpoch=7, metadata='m'}"
            );
            kafka_string_destroy(s);
            kafka_consumer_OffsetAndMetadata_destroy(oam);
        }

        let mut plain = std::ptr::null_mut();
        // SAFETY: Test code: `plain` is a valid out pointer.
        assert!(unsafe { kafka_consumer_OffsetAndMetadata_new(1, &mut plain) }.is_null());
        // SAFETY: Test code: `plain` is the owned handle just delivered, destroyed once at the end,
        // after the borrowed metadata string is read.
        unsafe {
            assert_eq!(kafka_consumer_OffsetAndMetadata_leader_epoch(plain), -1);
            assert_eq!(
                CStr::from_ptr(kafka_consumer_OffsetAndMetadata_metadata(plain))
                    .to_str()
                    .unwrap(),
                ""
            );
            kafka_consumer_OffsetAndMetadata_destroy(plain);
        }
    }

    #[test]
    fn negative_offset_is_the_java_illegal_argument() {
        let mut oam = std::ptr::null_mut();
        // SAFETY: Test code: `oam` is a valid out pointer.
        let error = unsafe { kafka_consumer_OffsetAndMetadata_new(-1, &mut oam) };
        assert!(!error.is_null());
        // SAFETY: Test code: `error` is the owned error just returned (non-null, asserted above).
        assert!(unsafe { error_ref(error) }.error.is_local_illegal_argument_error());
        // SAFETY: Test code: `error` is the owned error returned above, destroyed once.
        unsafe { kafka_common_Error_destroy(error) };
    }

    #[test]
    fn map_round_trip_is_sorted() {
        let mut map = HashMap::new();
        map.insert(TopicPartition::new("b", 0), OffsetAndMetadata::new(1).unwrap());
        map.insert(TopicPartition::new("a", 1), OffsetAndMetadata::new(2).unwrap());
        map.insert(TopicPartition::new("a", 0), OffsetAndMetadata::new(3).unwrap());
        let c_map = offset_and_metadata_map(&map);
        // SAFETY: Test code: `c_map` is the live map `offset_and_metadata_map` just built.
        assert_eq!(unsafe { kafka_Map_size(c_map) }, 3);
        // SAFETY: Test code: `c_map` is live and not mutated while its entries are read.
        let keys: Vec<_> = unsafe { map_entries(c_map) }
            .iter()
            // SAFETY: Test code: every key of `c_map` is a live topic-partition handle the map
            // owns.
            .map(|&(k, _)| unsafe { topic_partition_ref(k as *const _) }.clone())
            .collect();
        assert_eq!(
            keys,
            vec![
                TopicPartition::new("a", 0),
                TopicPartition::new("a", 1),
                TopicPartition::new("b", 0)
            ]
        );
        // SAFETY: Test code: `c_map` is a live map of topic-partition to offset-and-metadata
        // handles, not mutated during the call.
        assert_eq!(unsafe { map_offset_and_metadata(c_map) }, map);
        // SAFETY: Test code: `c_map` is the map built above, destroyed once.
        unsafe { kafka_Map_destroy(c_map) };
    }
}
