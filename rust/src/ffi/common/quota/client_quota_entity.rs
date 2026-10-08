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

//! `kafka_common_quota_ClientQuotaEntity_t`:
//! `org.apache.kafka.common.quota.ClientQuotaEntity` (CLAUDE.md §4).
//!
//! Java's entity is a `Map<String, String>` from entity type (`"user"`,
//! `"client-id"`, `"ip"`) to entity name, with a **null value meaning the
//! built-in default entity** for that type — the `--entity-default` of the
//! command-line tools — which is not the same as the type being absent from the
//! map, and not the same as the name `""`. The C map keeps all three apart:
//! absence is the key not being present, the default entity is a `NULL` value,
//! and `""` is a pointer to an empty string.

use std::ffi::{c_char, c_void};
use std::ptr;

use crate::common::quota::ClientQuotaEntity;
use crate::ffi::util::{
    box_string_keyed_map, c_str_to_option, c_str_to_string, destroy_string_element, into_c_string, kafka_Map_t,
    map_entries,
};

/// Opaque handle to a [`ClientQuotaEntity`].
///
/// Points at the entity itself: every getter returns an owned value, so no
/// NUL-terminated cache is needed.
#[repr(C)]
pub struct kafka_common_quota_ClientQuotaEntity_t {
    _private: [u8; 0],
}

/// The entity behind a handle.
///
/// # Safety
///
/// `entity` must be a valid client-quota-entity handle.
pub(crate) unsafe fn client_quota_entity_ref<'a>(
    entity: *const kafka_common_quota_ClientQuotaEntity_t,
) -> &'a ClientQuotaEntity {
    unsafe { &*(entity as *const ClientQuotaEntity) }
}

/// A borrowed handle on `entity`, valid as long as `entity`.
pub(crate) fn client_quota_entity_ptr(entity: &ClientQuotaEntity) -> *const kafka_common_quota_ClientQuotaEntity_t {
    entity as *const ClientQuotaEntity as *const kafka_common_quota_ClientQuotaEntity_t
}

/// Hands `entity` to C as an owned handle, freed with
/// [`kafka_common_quota_ClientQuotaEntity_destroy`].
pub(crate) fn box_client_quota_entity(entity: ClientQuotaEntity) -> *mut kafka_common_quota_ClientQuotaEntity_t {
    Box::into_raw(Box::new(entity)) as *mut kafka_common_quota_ClientQuotaEntity_t
}

/// The entries sorted by entity type, so that index addressing through the C
/// map and the ordering of entities inside a result are reproducible; Java's
/// map is unordered.
pub(crate) fn sorted_entries(entity: &ClientQuotaEntity) -> Vec<(&str, Option<&str>)> {
    let mut entries: Vec<(&str, Option<&str>)> =
        entity.entries().iter().map(|(t, n)| (t.as_str(), n.as_deref())).collect();
    entries.sort_unstable();
    entries
}

/// `new ClientQuotaEntity(Map<String, String> entries)`: `entries` maps
/// entity-type strings to entity-name strings, a `NULL` value naming the
/// built-in default entity for its type. The map and its strings are copied
/// during the call. The owned entity is freed with
/// [`kafka_common_quota_ClientQuotaEntity_destroy`].
///
/// # Safety
///
/// `entries` must be a valid map of NUL-terminated keys and NUL-terminated or
/// `NULL` values.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaEntity_new(
    entries: *const kafka_Map_t,
) -> *mut kafka_common_quota_ClientQuotaEntity_t {
    let entries = unsafe { map_entries(entries) }
        .iter()
        .map(|&(k, v)| {
            (unsafe { c_str_to_string(k as *const c_char) }, unsafe {
                c_str_to_option(v as *const c_char)
            })
        })
        .collect();
    box_client_quota_entity(ClientQuotaEntity::new(entries))
}

/// `isValidEntityType(String entityType)`: whether `entity_type` is one of
/// `user`, `client-id` or `ip`.
///
/// # Safety
///
/// `entity_type` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaEntity_is_valid_entity_type(entity_type: *const c_char) -> i8 {
    i8::from(ClientQuotaEntity::is_valid_entity_type(&unsafe {
        c_str_to_string(entity_type)
    }))
}

/// `entries()`: an owned map from entity type to entity name, both
/// NUL-terminated strings, a `NULL` value meaning the built-in default entity
/// for its type. Entries are sorted by entity type. Freed with
/// `kafka_Map_destroy`, which also frees the strings.
///
/// # Safety
///
/// `self_` must be a valid client-quota-entity handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaEntity_entries(
    self_: *const kafka_common_quota_ClientQuotaEntity_t,
) -> *mut kafka_Map_t {
    let entries = sorted_entries(unsafe { client_quota_entity_ref(self_) })
        .into_iter()
        .map(|(t, n)| (t, n.map_or(ptr::null_mut(), |n| into_c_string(n) as *mut c_void)));
    box_string_keyed_map(entries, Some(destroy_string_element))
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid client-quota-entity handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaEntity_to_string(
    self_: *const kafka_common_quota_ClientQuotaEntity_t,
) -> *mut c_char {
    into_c_string(&unsafe { client_quota_entity_ref(self_) }.to_string())
}

/// Frees an owned entity handle. Null is a no-op; an entity borrowed from a
/// result or an alteration is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned entity handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaEntity_destroy(
    self_: *mut kafka_common_quota_ClientQuotaEntity_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ClientQuotaEntity) });
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ffi::{CStr, CString};

    use super::*;
    use crate::ffi::util::{
        kafka_Map_destroy, kafka_Map_key, kafka_Map_new, kafka_Map_put, kafka_Map_size, kafka_Map_value,
        kafka_string_destroy,
    };

    pub(crate) fn quota_entity(entries: &[(&str, Option<&str>)]) -> ClientQuotaEntity {
        ClientQuotaEntity::new(entries.iter().map(|(t, n)| (t.to_string(), n.map(str::to_string))).collect())
    }

    unsafe fn entry_at(map: *const kafka_Map_t, index: i32) -> (String, Option<String>) {
        let key = unsafe { CStr::from_ptr(kafka_Map_key(map, index) as *const c_char) };
        let value = unsafe { kafka_Map_value(map, index) } as *const c_char;
        let value = (!value.is_null()).then(|| unsafe { CStr::from_ptr(value) }.to_str().unwrap().to_string());
        (key.to_str().unwrap().to_string(), value)
    }

    #[test]
    fn entries_distinguish_the_default_entity_from_the_empty_name() {
        let entity = quota_entity(&[("user", None), ("client-id", Some("")), ("ip", Some("10.0.0.1"))]);
        unsafe {
            let map = kafka_common_quota_ClientQuotaEntity_entries(client_quota_entity_ptr(&entity));
            assert_eq!(kafka_Map_size(map), 3);
            // Sorted by entity type: client-id, ip, user. Present but empty is
            // a pointer to "", not null; the default entity is null.
            assert_eq!(entry_at(map, 0), ("client-id".to_string(), Some(String::new())));
            assert_eq!(entry_at(map, 1), ("ip".to_string(), Some("10.0.0.1".to_string())));
            assert_eq!(entry_at(map, 2), ("user".to_string(), None));
            kafka_Map_destroy(map);
        }
    }

    #[test]
    fn constructor_round_trips_through_the_c_map() {
        let user = CString::new("user").unwrap();
        let alice = CString::new("alice").unwrap();
        let client_id = CString::new("client-id").unwrap();
        unsafe {
            let map = kafka_Map_new();
            kafka_Map_put(map, user.as_ptr() as *mut c_void, alice.as_ptr() as *mut c_void);
            kafka_Map_put(map, client_id.as_ptr() as *mut c_void, ptr::null_mut());
            let entity = kafka_common_quota_ClientQuotaEntity_new(map);
            kafka_Map_destroy(map);

            let expected = ClientQuotaEntity::new(HashMap::from([
                ("user".to_string(), Some("alice".to_string())),
                ("client-id".to_string(), None),
            ]));
            assert_eq!(*client_quota_entity_ref(entity), expected);
            // Compared against the same instance: `Display` walks the entity's
            // `HashMap`, whose order differs between two equal maps.
            let s = kafka_common_quota_ClientQuotaEntity_to_string(entity);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), client_quota_entity_ref(entity).to_string());
            kafka_string_destroy(s);
            kafka_common_quota_ClientQuotaEntity_destroy(entity);
            kafka_common_quota_ClientQuotaEntity_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn is_valid_entity_type_follows_java() {
        for (name, valid) in [("user", 1), ("client-id", 1), ("ip", 1), ("group", 0), ("", 0)] {
            let c = CString::new(name).unwrap();
            assert_eq!(
                unsafe { kafka_common_quota_ClientQuotaEntity_is_valid_entity_type(c.as_ptr()) },
                valid,
                "{name}"
            );
        }
    }

    #[test]
    fn sorted_entries_order_entities_reproducibly() {
        let a = quota_entity(&[("user", Some("alice")), ("client-id", None)]);
        let b = quota_entity(&[("ip", Some("10.0.0.1"))]);
        assert_eq!(sorted_entries(&a), vec![("client-id", None), ("user", Some("alice"))]);
        assert!(sorted_entries(&a) < sorted_entries(&b));
    }
}
