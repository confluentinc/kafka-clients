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

//! `kafka_admin_DescribeFeaturesResult_t`:
//! `org.apache.kafka.clients.admin.DescribeFeaturesResult` (CLAUDE.md §4).

use std::ffi::c_void;

use crate::admin::DescribeFeaturesResult;
use crate::ffi::admin::feature_metadata::{box_feature_metadata, destroy_feature_metadata_element};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, result_ref};
use crate::ffi::ffi_guard;
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;

/// Opaque handle to a [`DescribeFeaturesResult`], owned and freed with
/// [`kafka_admin_DescribeFeaturesResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DescribeFeaturesResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_describe_features_result(
    result: DescribeFeaturesResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_DescribeFeaturesResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle, kept alive for `'a`.
unsafe fn handle<'a>(self_: *const kafka_admin_DescribeFeaturesResult_t) -> &'a ResultHandle<DescribeFeaturesResult> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle kept alive for `'a`,
    // and every `kafka_admin_DescribeFeaturesResult_t` is built by
    // `box_result::<DescribeFeaturesResult, _>` (in `box_describe_features_result`), as
    // `result_ref` requires.
    unsafe { result_ref(self_) }
}

/// `featureMetadata()`: an owned future freed with
/// `kafka_common_KafkaFuture_destroy`; its `get` delivers a
/// `kafka_admin_FeatureMetadata_t *` owned by the future.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeFeaturesResult_feature_metadata(
    self_: *const kafka_admin_DescribeFeaturesResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    // SAFETY: `destroy_feature_metadata_element` frees exactly the handle `box_feature_metadata`
    // makes, and the handle holds only owned data and no thread-bound state, so it may be freed
    // from any thread and read from several at once, as `handle_future` requires.
    unsafe {
        h.ctx.handle_future(
            &h.result.feature_metadata(),
            |metadata| box_feature_metadata(metadata) as *mut c_void,
            destroy_feature_metadata_element,
        )
    }
}

/// Frees a result handle; null is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeFeaturesResult_destroy(self_: *mut kafka_admin_DescribeFeaturesResult_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards; every
    // `kafka_admin_DescribeFeaturesResult_t` is built by `box_result::<DescribeFeaturesResult, _>`,
    // as `destroy_result` requires.
    unsafe { destroy_result::<DescribeFeaturesResult, _>(self_) }
}
