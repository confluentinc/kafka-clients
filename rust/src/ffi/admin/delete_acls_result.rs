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

//! C bindings for `org.apache.kafka.clients.admin.DeleteAclsResult` and its
//! nested `DeleteAclsResult.FilterResult` / `DeleteAclsResult.FilterResults`.

use std::ffi::c_void;
use std::ptr;

use crate::admin::{DeleteAclsResult, FilterResult, FilterResults};
use crate::common::KafkaFuture;
use crate::common::acl::{AclBinding, AclBindingFilter};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_list_element, destroy_result, result_ref};
use crate::ffi::common::acl::acl_binding::{
    AclBindingInner, box_acl_binding, kafka_common_acl_AclBinding_destroy, kafka_common_acl_AclBinding_t,
};
use crate::ffi::common::acl::acl_binding_filter::{
    acl_binding_filter_ref, box_acl_binding_filter, kafka_common_acl_AclBindingFilter_destroy,
    kafka_common_acl_AclBindingFilter_t,
};
use crate::ffi::common::{ErrorInner, kafka_common_Error_t};
use crate::ffi::ffi_guard;
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::{box_list, kafka_List_t, kafka_Map_t};

// ---------------------------------------------------------------------------
// DeleteAclsResult.FilterResult
// ---------------------------------------------------------------------------

/// Opaque handle to a [`FilterResult`] (`DeleteAclsResult.FilterResult`),
/// owned by the caller and freed with
/// [`kafka_admin_DeleteAclsResult_FilterResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DeleteAclsResult_FilterResult_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_DeleteAclsResult_FilterResult_t`] points at: the
/// handles of the value's binding and error, which the getters borrow out
/// (Java's `FilterResult` has no other state).
pub(crate) struct FilterResultInner {
    binding: Option<AclBindingInner>,
    error: Option<ErrorInner>,
}

impl FilterResultInner {
    fn new(value: FilterResult) -> Self {
        let binding = value.binding().cloned().map(AclBindingInner::new);
        let error = value.error().cloned().map(ErrorInner::new);
        Self { binding, error }
    }
}

/// What a filter-result handle points at.
///
/// # Safety
///
/// `value` must be a valid filter-result handle, kept alive for `'a`.
unsafe fn filter_result_inner<'a>(value: *const kafka_admin_DeleteAclsResult_FilterResult_t) -> &'a FilterResultInner {
    // SAFETY: Per this function's `# Safety`, `value` is a valid filter-result handle kept alive
    // for `'a`. Every `kafka_admin_DeleteAclsResult_FilterResult_t` is the `Box::into_raw` of a
    // `Box<FilterResultInner>` (`box_filter_result`, the only place one is made), so it points at a
    // live `FilterResultInner`. Nothing takes a `&mut` to it: only the destroy takes the box back,
    // and its `# Safety` excludes any other use at the same time or afterwards.
    unsafe { &*(value as *const FilterResultInner) }
}

/// Hands `value` to C as an owned handle.
pub(crate) fn box_filter_result(value: FilterResult) -> *mut kafka_admin_DeleteAclsResult_FilterResult_t {
    Box::into_raw(Box::new(FilterResultInner::new(value))) as *mut kafka_admin_DeleteAclsResult_FilterResult_t
}

/// Frees a `kafka_admin_DeleteAclsResult_FilterResult_t *` element of an
/// owned container.
///
/// # Safety
///
/// `element` must be an owned filter-result handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
pub(crate) unsafe fn destroy_filter_result_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned filter-result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards, as
    // `kafka_admin_DeleteAclsResult_FilterResult_destroy` requires.
    unsafe {
        kafka_admin_DeleteAclsResult_FilterResult_destroy(element as *mut kafka_admin_DeleteAclsResult_FilterResult_t)
    }
}

/// Return the deleted ACL binding or `NULL` if there was an error.
///
/// # Returns
///
/// The deleted ACL binding, or `NULL` when the deletion failed.
///
/// # Safety
///
/// `self_` must be a valid filter-result handle.
///
/// The returned binding is borrowed from `self_` and is never passed to
/// `kafka_common_acl_AclBinding_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteAclsResult_FilterResult_binding(
    self_: *const kafka_admin_DeleteAclsResult_FilterResult_t,
) -> *const kafka_common_acl_AclBinding_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid filter-result handle, kept alive
    // for this call, as `filter_result_inner` requires. The pointer handed out borrows the handle's
    // own binding, which lives as long as the handle.
    unsafe { filter_result_inner(self_) }
        .binding
        .as_ref()
        .map_or(ptr::null(), |binding| {
            binding as *const AclBindingInner as *const kafka_common_acl_AclBinding_t
        })
}

/// Return an error if the ACL delete was not successful or `NULL` if it was.
///
/// # Returns
///
/// The error the deletion failed with, or `NULL` when it succeeded.
///
/// # Safety
///
/// `self_` must be a valid filter-result handle.
///
/// The returned error is borrowed from `self_` and is never passed to `kafka_common_Error_destroy`.
#[ffi_guard(fallback = ::core::ptr::null())]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteAclsResult_FilterResult_error(
    self_: *const kafka_admin_DeleteAclsResult_FilterResult_t,
) -> *const kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid filter-result handle, kept alive
    // for this call, as `filter_result_inner` requires. The pointer handed out borrows the handle's
    // own error, which lives as long as the handle.
    unsafe { filter_result_inner(self_) }
        .error
        .as_ref()
        .map_or(ptr::null(), |error| error as *const ErrorInner as *const kafka_common_Error_t)
}

/// Frees a handle; `NULL` is a no-op.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned handle not yet destroyed, that no other
/// call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteAclsResult_FilterResult_destroy(
    self_: *mut kafka_admin_DeleteAclsResult_FilterResult_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards. Every such handle is the `Box::into_raw` of a `Box<FilterResultInner>`
        // (`box_filter_result`), so `from_raw` reclaims that allocation, once.
        drop(unsafe { Box::from_raw(self_ as *mut FilterResultInner) });
    }
}

// ---------------------------------------------------------------------------
// DeleteAclsResult.FilterResults
// ---------------------------------------------------------------------------

/// Opaque handle to a [`FilterResults`] (`DeleteAclsResult.FilterResults`),
/// owned by the caller and freed with
/// [`kafka_admin_DeleteAclsResult_FilterResults_destroy`].
#[repr(C)]
pub struct kafka_admin_DeleteAclsResult_FilterResults_t {
    _private: [u8; 0],
}

/// Hands `value` to C as an owned handle.
pub(crate) fn box_filter_results(value: FilterResults) -> *mut kafka_admin_DeleteAclsResult_FilterResults_t {
    Box::into_raw(Box::new(value)) as *mut kafka_admin_DeleteAclsResult_FilterResults_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `value` must be a valid filter-results handle, kept alive for `'a`.
pub(crate) unsafe fn filter_results_ref<'a>(
    value: *const kafka_admin_DeleteAclsResult_FilterResults_t,
) -> &'a FilterResults {
    // SAFETY: Per this function's `# Safety`, `value` is a valid filter-results handle kept alive
    // for `'a`. Every `kafka_admin_DeleteAclsResult_FilterResults_t` is the `Box::into_raw` of a
    // `Box<FilterResults>` (`box_filter_results`, the only place one is made, for an owned handle
    // or the value a future delivers), so it points at a live `FilterResults`. Nothing takes a
    // `&mut` to it: only the destroy takes the box back, and its `# Safety` excludes any other use
    // at the same time or afterwards.
    unsafe { &*(value as *const FilterResults) }
}

/// Frees a `kafka_admin_DeleteAclsResult_FilterResults_t *` element of an
/// owned container or future.
///
/// # Safety
///
/// `element` must be an owned filter-results handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
pub(crate) unsafe fn destroy_filter_results_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned filter-results handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards, as
    // `kafka_admin_DeleteAclsResult_FilterResults_destroy` requires.
    unsafe {
        kafka_admin_DeleteAclsResult_FilterResults_destroy(element as *mut kafka_admin_DeleteAclsResult_FilterResults_t)
    }
}

/// Return a list of delete ACLs results for a given filter.
///
/// # Returns
///
/// A list, in response order, of owned `kafka_admin_DeleteAclsResult_FilterResult_t *` copies.
///
/// # Safety
///
/// `self_` must be a valid filter-results handle.
///
/// The returned list is owned by the caller, freed with `kafka_List_destroy` (which frees the
/// elements).
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteAclsResult_FilterResults_values(
    self_: *const kafka_admin_DeleteAclsResult_FilterResults_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid filter-results handle, kept alive
    // for this call, as `filter_results_ref` requires; the borrow ends once the copies are made.
    let elements = unsafe { filter_results_ref(self_) }
        .values()
        .iter()
        .map(|value| box_filter_result(value.clone()) as *mut c_void)
        .collect();
    box_list(elements, Some(destroy_filter_result_element))
}

/// Frees a handle; `NULL` is a no-op.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned handle not yet destroyed, that no other
/// call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteAclsResult_FilterResults_destroy(
    self_: *mut kafka_admin_DeleteAclsResult_FilterResults_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards. Every such handle is the `Box::into_raw` of a `Box<FilterResults>`
        // (`box_filter_results`), so `from_raw` reclaims that allocation, once.
        drop(unsafe { Box::from_raw(self_ as *mut FilterResults) });
    }
}

// ---------------------------------------------------------------------------
// DeleteAclsResult
// ---------------------------------------------------------------------------

/// Opaque handle to a [`DeleteAclsResult`], owned by the caller and freed
/// with [`kafka_admin_DeleteAclsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DeleteAclsResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C, binding the futures it exposes to `ctx`.
pub(crate) fn box_delete_acls_result(result: DeleteAclsResult, ctx: &FutureCtx) -> *mut kafka_admin_DeleteAclsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle, kept alive for `'a`.
unsafe fn handle<'a>(self_: *const kafka_admin_DeleteAclsResult_t) -> &'a ResultHandle<DeleteAclsResult> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle kept alive for `'a`,
    // and every `kafka_admin_DeleteAclsResult_t` is built by `box_result::<DeleteAclsResult, _>`
    // (in `box_delete_acls_result`), as `result_ref` requires.
    unsafe { result_ref(self_) }
}

/// Frees a `kafka_common_acl_AclBindingFilter_t *` key of an owned map.
///
/// # Safety
///
/// `element` must be an owned filter handle not yet destroyed.
unsafe fn destroy_acl_binding_filter_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned filter handle not yet
    // destroyed, as `kafka_common_acl_AclBindingFilter_destroy` requires.
    unsafe { kafka_common_acl_AclBindingFilter_destroy(element as *mut kafka_common_acl_AclBindingFilter_t) }
}

/// Compares two `kafka_common_acl_AclBindingFilter_t *` keys by value.
///
/// # Safety
///
/// `a` and `b` must be valid ACL-binding-filter handles, alive for this
/// call.
unsafe fn acl_binding_filter_key_eq(a: *mut c_void, b: *mut c_void) -> bool {
    // SAFETY: Per this function's `# Safety`, `a` and `b` are valid ACL-binding-filter handles,
    // alive for this call, as `acl_binding_filter_ref` requires; both borrows end with the
    // comparison.
    unsafe {
        acl_binding_filter_ref(a as *const kafka_common_acl_AclBindingFilter_t)
            == acl_binding_filter_ref(b as *const kafka_common_acl_AclBindingFilter_t)
    }
}

/// Frees a `kafka_common_acl_AclBinding_t *` element of an owned list.
///
/// # Safety
///
/// `element` must be an owned binding handle not yet destroyed.
unsafe fn destroy_acl_binding_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned binding handle not yet
    // destroyed, as `kafka_common_acl_AclBinding_destroy` requires.
    unsafe { kafka_common_acl_AclBinding_destroy(element as *mut kafka_common_acl_AclBinding_t) }
}

/// An owned list of owned `kafka_common_acl_AclBinding_t *`, in the
/// response's order.
fn acl_binding_list(bindings: Vec<AclBinding>) -> *mut kafka_List_t {
    let elements = bindings
        .into_iter()
        .map(|binding| box_acl_binding(binding) as *mut c_void)
        .collect();
    box_list(elements, Some(destroy_acl_binding_element))
}

/// Return a map from acl filters to futures which can be used to check the status of the deletions
/// by each filter.
///
/// # Returns
///
/// A map, sorted by the filter's `toString()`, of owned `kafka_common_acl_AclBindingFilter_t *`
/// keys (compared by value in `kafka_Map_get`) to owned `kafka_common_KafkaFuture_t *` whose `get`
/// delivers a `kafka_admin_DeleteAclsResult_FilterResults_t *` owned by the future.
///
/// # Safety
///
/// `self_` must be a live result handle.
///
/// The returned map is owned by the caller, freed with `kafka_Map_destroy`, which frees the keys
/// and the futures.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteAclsResult_values(
    self_: *const kafka_admin_DeleteAclsResult_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    let mut entries: Vec<(&AclBindingFilter, &KafkaFuture<FilterResults>)> = h.result.values().iter().collect();
    entries.sort_by_cached_key(|(filter, _)| filter.to_string());
    h.ctx.keyed_future_map(
        entries,
        |filter| box_acl_binding_filter(filter.clone()) as *mut c_void,
        destroy_acl_binding_filter_element,
        acl_binding_filter_key_eq,
        |ctx, future| {
            // SAFETY: `destroy_filter_results_element` frees exactly the handle
            // `box_filter_results` makes, and the handle holds only owned data and no thread-bound
            // state, so it may be freed from any thread and read from several at once, as
            // `handle_future` requires.
            unsafe {
                ctx.handle_future(
                    future,
                    |results| box_filter_results(results) as *mut c_void,
                    destroy_filter_results_element,
                )
            }
        },
    )
}

/// Return a future which succeeds only if all the ACLs deletions succeed, and which contains all
/// the deleted ACLs. Note that it if the filters don't match any ACLs, this is not considered an
/// error.
///
/// # Returns
///
/// A future whose `get` delivers a `kafka_List_t *` owned by the future, of
/// `kafka_common_acl_AclBinding_t *` (every ACL binding deleted, across all filters); the future
/// fails when any filter failed.
///
/// # Safety
///
/// `self_` must be a live result handle.
///
/// The returned future is owned by the caller, freed with `kafka_common_KafkaFuture_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteAclsResult_all(
    self_: *const kafka_admin_DeleteAclsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    // SAFETY: `destroy_list_element` frees exactly the list `acl_binding_list` makes through
    // `box_list`, and the handle holds only owned data and no thread-bound state, so it may be
    // freed from any thread and read from several at once, as `handle_future` requires.
    unsafe {
        h.ctx.handle_future(
            &h.result.all(),
            |bindings| acl_binding_list(bindings) as *mut c_void,
            destroy_list_element,
        )
    }
}

/// Frees a result handle; `NULL` is a no-op. Maps and futures taken from the
/// result stay valid until they are destroyed themselves.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned result handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeleteAclsResult_destroy(self_: *mut kafka_admin_DeleteAclsResult_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards; every
    // `kafka_admin_DeleteAclsResult_t` is built by `box_result::<DeleteAclsResult, _>`, as
    // `destroy_result` requires.
    unsafe { destroy_result::<DeleteAclsResult, _>(self_) }
}
