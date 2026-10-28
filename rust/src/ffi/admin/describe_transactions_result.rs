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

//! `kafka_admin_DescribeTransactionsResult_t`:
//! `org.apache.kafka.clients.admin.DescribeTransactionsResult` (CLAUDE.md §4).

use std::collections::{BTreeMap, HashMap};
use std::ffi::{c_char, c_void};

use crate::admin::{DescribeTransactionsResult, TransactionDescription};
use crate::ffi::admin::transaction_description::{
    box_transaction_description, destroy_transaction_description_element,
};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_map_element, destroy_result, result_ref};
use crate::ffi::common::deliver_value;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::ffi_guard;
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::{box_string_keyed_map, c_str_to_string};

/// Opaque handle to a [`DescribeTransactionsResult`], owned and freed with
/// [`kafka_admin_DescribeTransactionsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DescribeTransactionsResult_t {
    _private: [u8; 0],
}

/// Java's `Map<String, TransactionDescription>` as an owned map of owned
/// `char *` keys, sorted, to owned `kafka_admin_TransactionDescription_t *`.
fn descriptions_map(map: HashMap<String, TransactionDescription>) -> *mut c_void {
    let sorted: BTreeMap<String, TransactionDescription> = map.into_iter().collect();
    box_string_keyed_map(
        sorted
            .into_iter()
            .map(|(id, d)| (id, box_transaction_description(d) as *mut c_void)),
        Some(destroy_transaction_description_element),
    ) as *mut c_void
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_describe_transactions_result(
    result: DescribeTransactionsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_DescribeTransactionsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle, kept alive for `'a`.
unsafe fn handle<'a>(
    self_: *const kafka_admin_DescribeTransactionsResult_t,
) -> &'a ResultHandle<DescribeTransactionsResult> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle kept alive for `'a`,
    // and every `kafka_admin_DescribeTransactionsResult_t` is built by
    // `box_result::<DescribeTransactionsResult, _>` (in `box_describe_transactions_result`), as
    // `result_ref` requires.
    unsafe { result_ref(self_) }
}

/// Get the description of a specific transactional ID.
///
/// # Parameters
///
/// - `transactional_id` - the transactional ID to describe
///
/// # Returns
///
/// A future which completes when the transaction description of a particular transactional ID is
/// available.
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_argument_error` if the `transactional_id` was not included
///   in the respective call to `kafka_admin_Admin_describe_transactions`.
///
/// # Safety
///
/// `self_` must be a live result handle, `transactional_id` a valid
/// NUL-terminated string and `out_err` a valid slot pointing at `NULL`.
///
/// The returned future is owned and freed with `kafka_common_KafkaFuture_destroy`; its `get` yields
/// a `kafka_admin_TransactionDescription_t *` owned by the future. `NULL` is returned on failure.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTransactionsResult_description(
    self_: *const kafka_admin_DescribeTransactionsResult_t,
    transactional_id: *const c_char,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    // SAFETY: Per this function's `# Safety`, `transactional_id` is a valid NUL-terminated string,
    // as `c_str_to_string` requires; it is copied into a `String` during the call.
    let transactional_id = unsafe { c_str_to_string(transactional_id) };
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_value` requires. For `handle_future`, `destroy_transaction_description_element` frees exactly the
    // handle `box_transaction_description` makes, and the handle holds only owned data and no
    // thread-bound state, so it may be freed from any thread and read from several at once, as
    // `handle_future` requires.
    unsafe {
        deliver_value(h.result.description(&transactional_id), out_err, |f| {
            h.ctx.handle_future(
                &f,
                |d| box_transaction_description(d) as *mut c_void,
                destroy_transaction_description_element,
            )
        })
    }
}

/// `all()`: a future; its `get` delivers a `kafka_Map_t *` (owned by the future) of `char *`
/// transactional ids, sorted, to `kafka_admin_TransactionDescription_t *`.
///
/// # Returns
///
/// A future which either completes when all transaction descriptions complete or fails if any of
/// the descriptions cannot be obtained
///
/// # Safety
///
/// `self_` must be a live result handle.
///
/// The returned future is owned by the caller, freed with `kafka_common_KafkaFuture_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTransactionsResult_all(
    self_: *const kafka_admin_DescribeTransactionsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    // SAFETY: `destroy_map_element` frees exactly the map `descriptions_map` makes through
    // `box_string_keyed_map`, and the handle holds only owned data and no thread-bound state, so it
    // may be freed from any thread and read from several at once, as `handle_future` requires.
    unsafe { h.ctx.handle_future(&h.result.all(), descriptions_map, destroy_map_element) }
}

/// Frees a result handle; `NULL` is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned result handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTransactionsResult_destroy(
    self_: *mut kafka_admin_DescribeTransactionsResult_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards; every
    // `kafka_admin_DescribeTransactionsResult_t` is built by
    // `box_result::<DescribeTransactionsResult, _>`, as `destroy_result` requires.
    unsafe { destroy_result::<DescribeTransactionsResult, _>(self_) }
}
