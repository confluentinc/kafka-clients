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

//! `kafka_common_quota_ClientQuotaFilterComponent_t`:
//! `org.apache.kafka.common.quota.ClientQuotaFilterComponent` (CLAUDE.md §4).

use std::ffi::{CString, c_char};

use crate::common::quota::ClientQuotaFilterComponent;
use crate::ffi::common::quota::client_quota_match::{client_quota_match_ptr, kafka_common_quota_ClientQuotaMatch_t};
use crate::ffi::util::{c_str_to_string, into_c_string, kafka_List_t, list_elements, owned_c_string};

/// Opaque handle to a [`ClientQuotaFilterComponent`].
#[repr(C)]
pub struct kafka_common_quota_ClientQuotaFilterComponent_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_quota_ClientQuotaFilterComponent_t`] points at: the
/// component plus the NUL-terminated entity type its getter borrows out.
pub(crate) struct ClientQuotaFilterComponentInner {
    component: ClientQuotaFilterComponent,
    entity_type_c: CString,
}

impl ClientQuotaFilterComponentInner {
    pub(crate) fn new(component: ClientQuotaFilterComponent) -> Self {
        let entity_type_c = owned_c_string(component.entity_type());
        Self { component, entity_type_c }
    }
}

unsafe fn inner_ref<'a>(
    component: *const kafka_common_quota_ClientQuotaFilterComponent_t,
) -> &'a ClientQuotaFilterComponentInner {
    unsafe { &*(component as *const ClientQuotaFilterComponentInner) }
}

/// The component behind a handle.
///
/// # Safety
///
/// `component` must be a valid filter-component handle.
pub(crate) unsafe fn client_quota_filter_component_ref<'a>(
    component: *const kafka_common_quota_ClientQuotaFilterComponent_t,
) -> &'a ClientQuotaFilterComponent {
    &unsafe { inner_ref(component) }.component
}

/// Hands `component` to C as an owned handle, freed with
/// [`kafka_common_quota_ClientQuotaFilterComponent_destroy`].
pub(crate) fn box_client_quota_filter_component(
    component: ClientQuotaFilterComponent,
) -> *mut kafka_common_quota_ClientQuotaFilterComponent_t {
    Box::into_raw(Box::new(ClientQuotaFilterComponentInner::new(component)))
        as *mut kafka_common_quota_ClientQuotaFilterComponent_t
}

/// Copies the components out of a C list of borrowed component handles.
///
/// # Safety
///
/// `list` must be null or a valid list whose elements are filter-component
/// handles.
pub(crate) unsafe fn list_client_quota_filter_components(list: *const kafka_List_t) -> Vec<ClientQuotaFilterComponent> {
    unsafe { list_elements(list) }
        .iter()
        .map(|&element| {
            unsafe {
                client_quota_filter_component_ref(element as *const kafka_common_quota_ClientQuotaFilterComponent_t)
            }
            .clone()
        })
        .collect()
}

/// `ofEntity(String entityType, String entityName)`: a component matching
/// the entity named `entity_name` exactly. Owned, freed with
/// [`kafka_common_quota_ClientQuotaFilterComponent_destroy`].
///
/// # Safety
///
/// `entity_type` and `entity_name` must be valid NUL-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaFilterComponent_of_entity(
    entity_type: *const c_char,
    entity_name: *const c_char,
) -> *mut kafka_common_quota_ClientQuotaFilterComponent_t {
    box_client_quota_filter_component(ClientQuotaFilterComponent::of_entity(
        unsafe { c_str_to_string(entity_type) },
        unsafe { c_str_to_string(entity_name) },
    ))
}

/// `ofDefaultEntity(String entityType)`: a component matching the built-in
/// default entity of `entity_type`. Owned.
///
/// # Safety
///
/// `entity_type` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaFilterComponent_of_default_entity(
    entity_type: *const c_char,
) -> *mut kafka_common_quota_ClientQuotaFilterComponent_t {
    box_client_quota_filter_component(ClientQuotaFilterComponent::of_default_entity(unsafe {
        c_str_to_string(entity_type)
    }))
}

/// `ofEntityType(String entityType)`: a component matching any specified
/// entity of `entity_type`. Owned.
///
/// # Safety
///
/// `entity_type` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaFilterComponent_of_entity_type(
    entity_type: *const c_char,
) -> *mut kafka_common_quota_ClientQuotaFilterComponent_t {
    box_client_quota_filter_component(ClientQuotaFilterComponent::of_entity_type(unsafe {
        c_str_to_string(entity_type)
    }))
}

/// `entityType()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid filter-component handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaFilterComponent_entity_type(
    self_: *const kafka_common_quota_ClientQuotaFilterComponent_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.entity_type_c.as_ptr()
}

/// `match()`: borrowed from the handle. A data-less match is the singleton
/// returned by `kafka_common_quota_ClientQuotaMatch_default` /
/// `kafka_common_quota_ClientQuotaMatch_any`, so it compares with `==`; an
/// exact match dies with the component and is never destroyed.
///
/// # Safety
///
/// `self_` must be a valid filter-component handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaFilterComponent_match(
    self_: *const kafka_common_quota_ClientQuotaFilterComponent_t,
) -> *const kafka_common_quota_ClientQuotaMatch_t {
    client_quota_match_ptr(unsafe { client_quota_filter_component_ref(self_) }.r#match())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid filter-component handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaFilterComponent_to_string(
    self_: *const kafka_common_quota_ClientQuotaFilterComponent_t,
) -> *mut c_char {
    into_c_string(&unsafe { client_quota_filter_component_ref(self_) }.to_string())
}

/// Frees an owned filter-component handle. Null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned filter-component handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaFilterComponent_destroy(
    self_: *mut kafka_common_quota_ClientQuotaFilterComponent_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ClientQuotaFilterComponentInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::common::quota::ClientQuotaMatch;
    use crate::ffi::common::quota::client_quota_match::{
        client_quota_match_ref, kafka_common_quota_ClientQuotaMatch__enum, kafka_common_quota_ClientQuotaMatch_any,
        kafka_common_quota_ClientQuotaMatch_default, kafka_common_quota_ClientQuotaMatch_e,
    };
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn the_three_factories_build_the_three_match_states() {
        let user = CString::new("user").unwrap();
        let alice = CString::new("alice").unwrap();
        unsafe {
            let exact = kafka_common_quota_ClientQuotaFilterComponent_of_entity(user.as_ptr(), alice.as_ptr());
            assert_eq!(
                *client_quota_filter_component_ref(exact),
                ClientQuotaFilterComponent::of_entity("user", "alice")
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_quota_ClientQuotaFilterComponent_entity_type(exact)).to_str(),
                Ok("user")
            );
            let m = kafka_common_quota_ClientQuotaFilterComponent_match(exact);
            assert_eq!(
                kafka_common_quota_ClientQuotaMatch__enum(m),
                kafka_common_quota_ClientQuotaMatch_e::exact
            );
            assert_eq!(*client_quota_match_ref(m), ClientQuotaMatch::Exact("alice".to_string()));
            let s = kafka_common_quota_ClientQuotaFilterComponent_to_string(exact);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                ClientQuotaFilterComponent::of_entity("user", "alice").to_string()
            );
            kafka_string_destroy(s);
            kafka_common_quota_ClientQuotaFilterComponent_destroy(exact);

            let default = kafka_common_quota_ClientQuotaFilterComponent_of_default_entity(user.as_ptr());
            assert_eq!(
                kafka_common_quota_ClientQuotaFilterComponent_match(default),
                kafka_common_quota_ClientQuotaMatch_default()
            );
            kafka_common_quota_ClientQuotaFilterComponent_destroy(default);

            let any = kafka_common_quota_ClientQuotaFilterComponent_of_entity_type(user.as_ptr());
            assert_eq!(
                kafka_common_quota_ClientQuotaFilterComponent_match(any),
                kafka_common_quota_ClientQuotaMatch_any()
            );
            kafka_common_quota_ClientQuotaFilterComponent_destroy(any);
            kafka_common_quota_ClientQuotaFilterComponent_destroy(ptr::null_mut());
        }
    }
}
