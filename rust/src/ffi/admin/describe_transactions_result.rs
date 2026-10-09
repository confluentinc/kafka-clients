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
use crate::ffi::admin::{
    FutureCtx, ResultHandle, box_result, destroy_map_element, destroy_result, out_slot, result_ref,
};
use crate::ffi::common::kafka_common_Error_t;
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

unsafe fn handle<'a>(
    self_: *const kafka_admin_DescribeTransactionsResult_t,
) -> &'a ResultHandle<DescribeTransactionsResult> {
    unsafe { result_ref(self_) }
}

/// `description(String transactionalId)`: delivers through `out_description`
/// an owned future (freed with `kafka_common_KafkaFuture_destroy`) whose
/// `get` yields a `kafka_admin_TransactionDescription_t *` owned by the
/// future, or returns the owned `IllegalArgumentError` when
/// `transactional_id` was not part of the request.
///
/// # Safety
///
/// `self_` must be a live result handle, `transactional_id` a valid
/// NUL-terminated string and `out_description` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTransactionsResult_description(
    self_: *const kafka_admin_DescribeTransactionsResult_t,
    transactional_id: *const c_char,
    out_description: *mut *mut kafka_common_KafkaFuture_t,
) -> *mut kafka_common_Error_t {
    let h = unsafe { handle(self_) };
    let transactional_id = unsafe { c_str_to_string(transactional_id) };
    unsafe {
        out_slot(h.result.description(&transactional_id), out_description, |f| {
            h.ctx.handle_future(
                &f,
                |d| box_transaction_description(d) as *mut c_void,
                destroy_transaction_description_element,
            )
        })
    }
}

/// `all()`: an owned future freed with `kafka_common_KafkaFuture_destroy`;
/// its `get` delivers a `kafka_Map_t *` (owned by the future) of `char *`
/// transactional ids, sorted, to `kafka_admin_TransactionDescription_t *`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTransactionsResult_all(
    self_: *const kafka_admin_DescribeTransactionsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.handle_future(&h.result.all(), descriptions_map, destroy_map_element)
}

/// Frees a result handle; null is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeTransactionsResult_destroy(
    self_: *mut kafka_admin_DescribeTransactionsResult_t,
) {
    unsafe { destroy_result::<DescribeTransactionsResult, _>(self_) }
}
