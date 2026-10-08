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

//! `kafka_admin_OffsetSpec_t`: `org.apache.kafka.clients.admin.OffsetSpec`
//! (CLAUDE.md §4, "Enums"). Java's `OffsetSpec` is a class with one
//! subclass per kind of offset request, translated to a Rust enum: the
//! kinds without data (`EARLIEST`, `LATEST`, ...) are borrowed singletons
//! never freed, and `TimestampSpec`, which carries the timestamp, is built
//! owned by `kafka_admin_OffsetSpec_for_timestamp` and freed with
//! [`kafka_admin_OffsetSpec_destroy`], which is a no-op on a singleton so a
//! caller may destroy every spec it receives without telling them apart.

#![expect(non_camel_case_types)]

use crate::admin::OffsetSpec;

/// Opaque handle to an [`OffsetSpec`]: a singleton for the kinds without
/// data, an owned handle for a timestamp spec.
#[repr(C)]
pub struct kafka_admin_OffsetSpec_t {
    _private: [u8; 0],
}

/// The kinds of [`OffsetSpec`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_admin_OffsetSpec_e {
    earliest,
    latest,
    max_timestamp,
    earliest_local,
    latest_tiered,
    earliest_pending_upload,
    /// `OffsetSpec.TimestampSpec`: the one kind carrying data.
    timestamp,
}

/// One static instance per kind without data, indexed by
/// [`kafka_admin_OffsetSpec_e`] (the `timestamp` kind has no singleton).
static VARIANTS: [OffsetSpec; 6] = [
    OffsetSpec::Earliest,
    OffsetSpec::Latest,
    OffsetSpec::MaxTimestamp,
    OffsetSpec::EarliestLocal,
    OffsetSpec::LatestTiered,
    OffsetSpec::EarliestPendingUpload,
];

/// Exhaustive, so a kind Java adds fails to compile until it has its C
/// enumerator and singleton.
fn enum_of(spec: OffsetSpec) -> kafka_admin_OffsetSpec_e {
    match spec {
        OffsetSpec::Earliest => kafka_admin_OffsetSpec_e::earliest,
        OffsetSpec::Latest => kafka_admin_OffsetSpec_e::latest,
        OffsetSpec::MaxTimestamp => kafka_admin_OffsetSpec_e::max_timestamp,
        OffsetSpec::EarliestLocal => kafka_admin_OffsetSpec_e::earliest_local,
        OffsetSpec::LatestTiered => kafka_admin_OffsetSpec_e::latest_tiered,
        OffsetSpec::EarliestPendingUpload => kafka_admin_OffsetSpec_e::earliest_pending_upload,
        OffsetSpec::Timestamp(_) => kafka_admin_OffsetSpec_e::timestamp,
    }
}

/// The borrowed singleton standing for a kind without data.
fn singleton(index: kafka_admin_OffsetSpec_e) -> *const kafka_admin_OffsetSpec_t {
    &VARIANTS[index as usize] as *const OffsetSpec as *const kafka_admin_OffsetSpec_t
}

/// Whether `spec` points into the singleton table.
fn is_singleton(spec: *const kafka_admin_OffsetSpec_t) -> bool {
    VARIANTS
        .iter()
        .any(|v| std::ptr::eq(v as *const OffsetSpec as *const kafka_admin_OffsetSpec_t, spec))
}

/// Hands `spec` to C: the borrowed singleton for a kind without data, an
/// owned handle for a timestamp spec. Either way the result may be passed
/// to [`kafka_admin_OffsetSpec_destroy`], a no-op on a singleton.
pub(crate) fn box_offset_spec(spec: OffsetSpec) -> *mut kafka_admin_OffsetSpec_t {
    match spec {
        OffsetSpec::Timestamp(_) => Box::into_raw(Box::new(spec)) as *mut kafka_admin_OffsetSpec_t,
        _ => singleton(enum_of(spec)) as *mut kafka_admin_OffsetSpec_t,
    }
}

/// The value behind a handle, singleton or owned.
///
/// # Safety
///
/// `spec` must be a handle returned by this module and, if owned, not yet
/// destroyed.
pub(crate) unsafe fn offset_spec_value_of(spec: *const kafka_admin_OffsetSpec_t) -> OffsetSpec {
    unsafe { *(spec as *const OffsetSpec) }
}

/// `OffsetSpec.earliest()`: a borrowed singleton.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_OffsetSpec_earliest() -> *const kafka_admin_OffsetSpec_t {
    singleton(kafka_admin_OffsetSpec_e::earliest)
}

/// `OffsetSpec.latest()`: a borrowed singleton.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_OffsetSpec_latest() -> *const kafka_admin_OffsetSpec_t {
    singleton(kafka_admin_OffsetSpec_e::latest)
}

/// `OffsetSpec.maxTimestamp()`: a borrowed singleton.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_OffsetSpec_max_timestamp() -> *const kafka_admin_OffsetSpec_t {
    singleton(kafka_admin_OffsetSpec_e::max_timestamp)
}

/// `OffsetSpec.earliestLocal()`: a borrowed singleton.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_OffsetSpec_earliest_local() -> *const kafka_admin_OffsetSpec_t {
    singleton(kafka_admin_OffsetSpec_e::earliest_local)
}

/// `OffsetSpec.latestTiered()`: a borrowed singleton.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_OffsetSpec_latest_tiered() -> *const kafka_admin_OffsetSpec_t {
    singleton(kafka_admin_OffsetSpec_e::latest_tiered)
}

/// `OffsetSpec.earliestPendingUpload()`: a borrowed singleton.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_OffsetSpec_earliest_pending_upload() -> *const kafka_admin_OffsetSpec_t {
    singleton(kafka_admin_OffsetSpec_e::earliest_pending_upload)
}

/// `OffsetSpec.forTimestamp(long timestamp)`: an owned `TimestampSpec`
/// handle, freed with [`kafka_admin_OffsetSpec_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_OffsetSpec_for_timestamp(timestamp: i64) -> *mut kafka_admin_OffsetSpec_t {
    box_offset_spec(OffsetSpec::for_timestamp(timestamp))
}

/// The `timestamp` kind built from its data (CLAUDE.md §4, "Enums": the
/// value carrying data is built by the Java static factory), the same as
/// [`kafka_admin_OffsetSpec_for_timestamp`]: an owned handle freed with
/// [`kafka_admin_OffsetSpec_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_OffsetSpec_timestamp(value: i64) -> *mut kafka_admin_OffsetSpec_t {
    box_offset_spec(OffsetSpec::Timestamp(value))
}

/// The C enumerator of a handle, for a `switch`.
///
/// # Safety
///
/// `self_` must be a handle returned by this module and, if owned, not yet
/// destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_OffsetSpec__enum(
    self_: *const kafka_admin_OffsetSpec_t,
) -> kafka_admin_OffsetSpec_e {
    enum_of(unsafe { offset_spec_value_of(self_) })
}

/// Frees an owned timestamp spec; a singleton or null is a no-op.
///
/// # Safety
///
/// `self_` must be null, a singleton, or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_OffsetSpec_destroy(self_: *mut kafka_admin_OffsetSpec_t) {
    if !self_.is_null() && !is_singleton(self_) {
        drop(unsafe { Box::from_raw(self_ as *mut OffsetSpec) });
    }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;

    #[test]
    fn singletons_match_their_enumerator_and_survive_destroy() {
        for (index, &spec) in VARIANTS.iter().enumerate() {
            let handle = box_offset_spec(spec);
            assert!(is_singleton(handle));
            unsafe {
                assert_eq!(offset_spec_value_of(handle), spec);
                assert_eq!(kafka_admin_OffsetSpec__enum(handle) as usize, index);
                kafka_admin_OffsetSpec_destroy(handle);
                assert_eq!(offset_spec_value_of(handle), spec);
            }
        }
        assert_eq!(kafka_admin_OffsetSpec_earliest(), box_offset_spec(OffsetSpec::Earliest));
        assert_eq!(kafka_admin_OffsetSpec_latest(), box_offset_spec(OffsetSpec::Latest));
        assert_eq!(
            kafka_admin_OffsetSpec_max_timestamp(),
            box_offset_spec(OffsetSpec::MaxTimestamp)
        );
        assert_eq!(
            kafka_admin_OffsetSpec_earliest_local(),
            box_offset_spec(OffsetSpec::EarliestLocal)
        );
        assert_eq!(
            kafka_admin_OffsetSpec_latest_tiered(),
            box_offset_spec(OffsetSpec::LatestTiered)
        );
        assert_eq!(
            kafka_admin_OffsetSpec_earliest_pending_upload(),
            box_offset_spec(OffsetSpec::EarliestPendingUpload)
        );
    }

    #[test]
    fn timestamp_spec_is_owned() {
        let handle = kafka_admin_OffsetSpec_for_timestamp(42);
        let other = kafka_admin_OffsetSpec_timestamp(42);
        assert_ne!(handle, other);
        assert!(!is_singleton(handle));
        unsafe {
            assert_eq!(offset_spec_value_of(handle), OffsetSpec::Timestamp(42));
            assert_eq!(offset_spec_value_of(other), OffsetSpec::for_timestamp(42));
            assert_eq!(kafka_admin_OffsetSpec__enum(handle), kafka_admin_OffsetSpec_e::timestamp);
            kafka_admin_OffsetSpec_destroy(handle);
            kafka_admin_OffsetSpec_destroy(other);
            kafka_admin_OffsetSpec_destroy(ptr::null_mut());
        }
    }
}
