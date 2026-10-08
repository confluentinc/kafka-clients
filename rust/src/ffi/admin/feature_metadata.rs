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

//! `kafka_admin_FeatureMetadata_t`:
//! `org.apache.kafka.clients.admin.FeatureMetadata` (CLAUDE.md §4). Java's
//! constructor is package-private (the class is built from a
//! `describeFeatures` response), so there is no `_new`; the maps come out as
//! owned copies sorted by feature name.

use std::ffi::{c_char, c_void};

use crate::admin::FeatureMetadata;
use crate::ffi::admin::finalized_version_range::{
    box_finalized_version_range, destroy_finalized_version_range_element,
};
use crate::ffi::admin::supported_version_range::{
    box_supported_version_range, destroy_supported_version_range_element,
};
use crate::ffi::util::{ElementDestroy, box_string_keyed_map, into_c_string, kafka_Map_t};

/// Opaque handle to a [`FeatureMetadata`].
#[repr(C)]
pub struct kafka_admin_FeatureMetadata_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_FeatureMetadata_t`] points at.
pub(crate) struct FeatureMetadataInner {
    metadata: FeatureMetadata,
}

/// The metadata behind a handle.
///
/// # Safety
///
/// `metadata` must be a valid feature-metadata handle.
pub(crate) unsafe fn feature_metadata_ref<'a>(metadata: *const kafka_admin_FeatureMetadata_t) -> &'a FeatureMetadata {
    &unsafe { &*(metadata as *const FeatureMetadataInner) }.metadata
}

/// Hands `metadata` to C as an owned handle, freed with
/// [`kafka_admin_FeatureMetadata_destroy`].
pub(crate) fn box_feature_metadata(metadata: FeatureMetadata) -> *mut kafka_admin_FeatureMetadata_t {
    Box::into_raw(Box::new(FeatureMetadataInner { metadata })) as *mut kafka_admin_FeatureMetadata_t
}

/// Frees a `kafka_admin_FeatureMetadata_t *` element of an owned container.
///
/// # Safety
///
/// `element` must be an owned feature-metadata handle not yet destroyed.
pub(crate) unsafe fn destroy_feature_metadata_element(element: *mut c_void) {
    unsafe { kafka_admin_FeatureMetadata_destroy(element as *mut kafka_admin_FeatureMetadata_t) };
}

/// Hands a `Map<String, V>` to C as an owned map of owned `char *` keys in
/// ascending order to owned handles built by `box_value`.
fn sorted_feature_map<'a, V: 'a>(
    map: impl IntoIterator<Item = (&'a String, &'a V)>,
    box_value: impl Fn(&V) -> *mut c_void,
    value_destroy: ElementDestroy,
) -> *mut kafka_Map_t {
    let mut entries: Vec<(&String, &V)> = map.into_iter().collect();
    entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
    box_string_keyed_map(entries.into_iter().map(|(k, v)| (k, box_value(v))), Some(value_destroy))
}

/// `finalizedFeatures()`: an owned map of owned `char *` feature names in
/// ascending order to owned `kafka_admin_FinalizedVersionRange_t *`, freed
/// together with `kafka_Map_destroy`.
///
/// # Safety
///
/// `self_` must be a valid feature-metadata handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FeatureMetadata_finalized_features(
    self_: *const kafka_admin_FeatureMetadata_t,
) -> *mut kafka_Map_t {
    sorted_feature_map(
        unsafe { feature_metadata_ref(self_) }.finalized_features(),
        |range| box_finalized_version_range(*range) as *mut c_void,
        destroy_finalized_version_range_element,
    )
}

/// `finalizedFeaturesEpoch()`: the epoch, or `-1` when the broker reported
/// none (Java's empty `Optional<Long>`).
///
/// # Safety
///
/// `self_` must be a valid feature-metadata handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FeatureMetadata_finalized_features_epoch(
    self_: *const kafka_admin_FeatureMetadata_t,
) -> i64 {
    unsafe { feature_metadata_ref(self_) }.finalized_features_epoch().unwrap_or(-1)
}

/// `supportedFeatures()`: an owned map of owned `char *` feature names in
/// ascending order to owned `kafka_admin_SupportedVersionRange_t *`, freed
/// together with `kafka_Map_destroy`.
///
/// # Safety
///
/// `self_` must be a valid feature-metadata handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FeatureMetadata_supported_features(
    self_: *const kafka_admin_FeatureMetadata_t,
) -> *mut kafka_Map_t {
    sorted_feature_map(
        unsafe { feature_metadata_ref(self_) }.supported_features(),
        |range| box_supported_version_range(*range) as *mut c_void,
        destroy_supported_version_range_element,
    )
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid feature-metadata handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FeatureMetadata_to_string(
    self_: *const kafka_admin_FeatureMetadata_t,
) -> *mut c_char {
    into_c_string(&unsafe { feature_metadata_ref(self_) }.to_string())
}

/// Frees an owned metadata handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned metadata handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_FeatureMetadata_destroy(self_: *mut kafka_admin_FeatureMetadata_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut FeatureMetadataInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::admin::{FinalizedVersionRange, SupportedVersionRange};
    use crate::ffi::admin::finalized_version_range::{
        finalized_version_range_ref, kafka_admin_FinalizedVersionRange_t,
    };
    use crate::ffi::admin::supported_version_range::{
        kafka_admin_SupportedVersionRange_t, supported_version_range_ref,
    };
    use crate::ffi::util::{kafka_Map_destroy, kafka_Map_get, kafka_Map_key, kafka_Map_size, kafka_string_destroy};

    #[test]
    fn maps_come_out_sorted_and_the_epoch_defaults_to_minus_one() {
        let finalized = HashMap::from([
            ("b".to_string(), FinalizedVersionRange::new(1, 2).unwrap()),
            ("a".to_string(), FinalizedVersionRange::new(0, 1).unwrap()),
        ]);
        let supported = HashMap::from([("a".to_string(), SupportedVersionRange::new(0, 3).unwrap())]);
        let metadata = FeatureMetadata::new(finalized.clone(), None, supported.clone());
        let handle = box_feature_metadata(metadata.clone());
        unsafe {
            assert_eq!(*feature_metadata_ref(handle), metadata);
            assert_eq!(kafka_admin_FeatureMetadata_finalized_features_epoch(handle), -1);

            let map = kafka_admin_FeatureMetadata_finalized_features(handle);
            assert_eq!(kafka_Map_size(map), 2);
            assert_eq!(CStr::from_ptr(kafka_Map_key(map, 0) as *const c_char).to_str().unwrap(), "a");
            let b = kafka_Map_get(map, c"b".as_ptr() as *mut c_void) as *const kafka_admin_FinalizedVersionRange_t;
            assert_eq!(*finalized_version_range_ref(b), finalized["b"]);
            kafka_Map_destroy(map);

            let map = kafka_admin_FeatureMetadata_supported_features(handle);
            assert_eq!(kafka_Map_size(map), 1);
            let a = kafka_Map_get(map, c"a".as_ptr() as *mut c_void) as *const kafka_admin_SupportedVersionRange_t;
            assert_eq!(*supported_version_range_ref(a), supported["a"]);
            kafka_Map_destroy(map);

            let s = kafka_admin_FeatureMetadata_to_string(handle);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), metadata.to_string());
            kafka_string_destroy(s);
            kafka_admin_FeatureMetadata_destroy(handle);
            kafka_admin_FeatureMetadata_destroy(ptr::null_mut());
        }

        let with_epoch = box_feature_metadata(FeatureMetadata::new(HashMap::new(), Some(9), HashMap::new()));
        unsafe {
            assert_eq!(kafka_admin_FeatureMetadata_finalized_features_epoch(with_epoch), 9);
            kafka_admin_FeatureMetadata_destroy(with_epoch);
        }
    }
}
