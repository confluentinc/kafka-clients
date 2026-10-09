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

//! `kafka_common_quota_ClientQuotaFilter_t`:
//! `org.apache.kafka.common.quota.ClientQuotaFilter` (CLAUDE.md §4).

use std::ffi::{c_char, c_void};

use crate::common::quota::ClientQuotaFilter;
use crate::ffi::common::quota::client_quota_filter_component::{
    ClientQuotaFilterComponentInner, box_client_quota_filter_component, list_client_quota_filter_components,
};
use crate::ffi::util::{box_list, destroy_boxed, into_c_string, kafka_List_t};

/// Opaque handle to a [`ClientQuotaFilter`].
///
/// Points at the filter itself: every getter returns an owned value, so no
/// NUL-terminated cache is needed.
#[repr(C)]
pub struct kafka_common_quota_ClientQuotaFilter_t {
    _private: [u8; 0],
}

/// The filter behind a handle.
///
/// # Safety
///
/// `filter` must be a valid client-quota-filter handle.
pub(crate) unsafe fn client_quota_filter_ref<'a>(
    filter: *const kafka_common_quota_ClientQuotaFilter_t,
) -> &'a ClientQuotaFilter {
    unsafe { &*(filter as *const ClientQuotaFilter) }
}

/// Hands `filter` to C as an owned handle, freed with
/// [`kafka_common_quota_ClientQuotaFilter_destroy`].
pub(crate) fn box_client_quota_filter(filter: ClientQuotaFilter) -> *mut kafka_common_quota_ClientQuotaFilter_t {
    Box::into_raw(Box::new(filter)) as *mut kafka_common_quota_ClientQuotaFilter_t
}

/// `contains(Collection<ClientQuotaFilterComponent> components)`: a filter
/// matching every entity that contains the given components, and possibly
/// others. `components` is a list of borrowed
/// `kafka_common_quota_ClientQuotaFilterComponent_t` handles, copied during
/// the call. Owned, freed with [`kafka_common_quota_ClientQuotaFilter_destroy`].
///
/// # Safety
///
/// `components` must be null or a valid list of filter-component handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaFilter_contains(
    components: *const kafka_List_t,
) -> *mut kafka_common_quota_ClientQuotaFilter_t {
    box_client_quota_filter(ClientQuotaFilter::contains(unsafe {
        list_client_quota_filter_components(components)
    }))
}

/// `containsOnly(Collection<ClientQuotaFilterComponent> components)`: a
/// filter matching every entity that contains exactly the given components.
/// Same element type and ownership as
/// [`kafka_common_quota_ClientQuotaFilter_contains`].
///
/// # Safety
///
/// `components` must be null or a valid list of filter-component handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaFilter_contains_only(
    components: *const kafka_List_t,
) -> *mut kafka_common_quota_ClientQuotaFilter_t {
    box_client_quota_filter(ClientQuotaFilter::contains_only(unsafe {
        list_client_quota_filter_components(components)
    }))
}

/// `all()`: a filter matching every configured entity. Owned.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_quota_ClientQuotaFilter_all() -> *mut kafka_common_quota_ClientQuotaFilter_t {
    box_client_quota_filter(ClientQuotaFilter::all())
}

/// `components()`: an owned list of owned
/// `kafka_common_quota_ClientQuotaFilterComponent_t` copies, in the filter's
/// order. Freed with `kafka_List_destroy`, which also frees the components.
///
/// # Safety
///
/// `self_` must be a valid client-quota-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaFilter_components(
    self_: *const kafka_common_quota_ClientQuotaFilter_t,
) -> *mut kafka_List_t {
    let elements = unsafe { client_quota_filter_ref(self_) }
        .components()
        .iter()
        .map(|component| box_client_quota_filter_component(component.clone()) as *mut c_void)
        .collect();
    box_list(elements, Some(destroy_boxed::<ClientQuotaFilterComponentInner>))
}

/// `strict()`: whether only entities with exactly the given components match.
///
/// # Safety
///
/// `self_` must be a valid client-quota-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaFilter_strict(
    self_: *const kafka_common_quota_ClientQuotaFilter_t,
) -> i8 {
    i8::from(unsafe { client_quota_filter_ref(self_) }.strict())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid client-quota-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaFilter_to_string(
    self_: *const kafka_common_quota_ClientQuotaFilter_t,
) -> *mut c_char {
    into_c_string(&unsafe { client_quota_filter_ref(self_) }.to_string())
}

/// Frees an owned filter handle. Null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned filter handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_quota_ClientQuotaFilter_destroy(
    self_: *mut kafka_common_quota_ClientQuotaFilter_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ClientQuotaFilter) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};
    use std::ptr;

    use super::*;
    use crate::common::quota::ClientQuotaFilterComponent;
    use crate::ffi::common::quota::client_quota_filter_component::{
        client_quota_filter_component_ref, kafka_common_quota_ClientQuotaFilterComponent_destroy,
        kafka_common_quota_ClientQuotaFilterComponent_of_default_entity,
        kafka_common_quota_ClientQuotaFilterComponent_of_entity, kafka_common_quota_ClientQuotaFilterComponent_t,
    };
    use crate::ffi::util::{kafka_List_add, kafka_List_destroy, kafka_List_get, kafka_List_new, kafka_string_destroy};

    #[test]
    fn contains_and_contains_only_copy_the_components_and_set_strict() {
        let user = CString::new("user").unwrap();
        let alice = CString::new("alice").unwrap();
        let client_id = CString::new("client-id").unwrap();
        let expected = vec![
            ClientQuotaFilterComponent::of_entity("user", "alice"),
            ClientQuotaFilterComponent::of_default_entity("client-id"),
        ];
        unsafe {
            let c0 = kafka_common_quota_ClientQuotaFilterComponent_of_entity(user.as_ptr(), alice.as_ptr());
            let c1 = kafka_common_quota_ClientQuotaFilterComponent_of_default_entity(client_id.as_ptr());
            let list = kafka_List_new();
            kafka_List_add(list, c0 as *mut c_void);
            kafka_List_add(list, c1 as *mut c_void);

            let loose = kafka_common_quota_ClientQuotaFilter_contains(list);
            let strict = kafka_common_quota_ClientQuotaFilter_contains_only(list);
            // The inputs were copied: the caller frees them first.
            kafka_List_destroy(list);
            kafka_common_quota_ClientQuotaFilterComponent_destroy(c0);
            kafka_common_quota_ClientQuotaFilterComponent_destroy(c1);

            assert_eq!(*client_quota_filter_ref(loose), ClientQuotaFilter::contains(expected.clone()));
            assert_eq!(
                *client_quota_filter_ref(strict),
                ClientQuotaFilter::contains_only(expected.clone())
            );
            assert_eq!(kafka_common_quota_ClientQuotaFilter_strict(loose), 0);
            assert_eq!(kafka_common_quota_ClientQuotaFilter_strict(strict), 1);

            let components = kafka_common_quota_ClientQuotaFilter_components(strict);
            let copies: Vec<ClientQuotaFilterComponent> = (0..2)
                .map(|i| {
                    client_quota_filter_component_ref(
                        kafka_List_get(components, i) as *const kafka_common_quota_ClientQuotaFilterComponent_t
                    )
                    .clone()
                })
                .collect();
            assert_eq!(copies, expected);
            kafka_List_destroy(components);

            let s = kafka_common_quota_ClientQuotaFilter_to_string(strict);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                ClientQuotaFilter::contains_only(expected).to_string()
            );
            kafka_string_destroy(s);
            kafka_common_quota_ClientQuotaFilter_destroy(loose);
            kafka_common_quota_ClientQuotaFilter_destroy(strict);
            kafka_common_quota_ClientQuotaFilter_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn all_matches_every_entity() {
        unsafe {
            let all = kafka_common_quota_ClientQuotaFilter_all();
            assert_eq!(*client_quota_filter_ref(all), ClientQuotaFilter::all());
            let components = kafka_common_quota_ClientQuotaFilter_components(all);
            assert_eq!(crate::ffi::util::kafka_List_size(components), 0);
            kafka_List_destroy(components);
            kafka_common_quota_ClientQuotaFilter_destroy(all);
        }
    }
}
