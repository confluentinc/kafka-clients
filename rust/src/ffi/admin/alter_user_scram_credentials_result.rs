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

//! C bindings for `org.apache.kafka.clients.admin.AlterUserScramCredentialsResult`.

use std::collections::HashMap;
use std::ffi::{c_char, c_void};
use std::ptr;

use crate::admin::AlterUserScramCredentialsResult;
use crate::common::{Error, KafkaFuture};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, result_ref};
use crate::ffi::common::take_error;
use crate::ffi::kafka_future::{
    kafka_common_KafkaFuture_get, kafka_common_KafkaFuture_is_done, kafka_common_KafkaFuture_t,
};
use crate::ffi::util::{c_str_to_string, kafka_Map_t, map_entries};

/// Opaque handle to an [`AlterUserScramCredentialsResult`], owned by the
/// caller and freed with [`kafka_admin_AlterUserScramCredentialsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_AlterUserScramCredentialsResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C, binding the futures it exposes to `ctx`.
pub(crate) fn box_alter_user_scram_credentials_result(
    result: AlterUserScramCredentialsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_AlterUserScramCredentialsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle.
unsafe fn handle<'a>(
    self_: *const kafka_admin_AlterUserScramCredentialsResult_t,
) -> &'a ResultHandle<AlterUserScramCredentialsResult> {
    unsafe { result_ref(self_) }
}

/// The Rust `KafkaFuture<Void>` standing for a C future that is already
/// complete (see [`kafka_admin_AlterUserScramCredentialsResult_new`]): its
/// outcome is copied; a future that is not yet complete yields one failed
/// with the translation of `IllegalArgumentException`.
///
/// # Safety
///
/// `future` must be a valid future handle.
unsafe fn resolved_void_future(future: *const kafka_common_KafkaFuture_t) -> KafkaFuture<()> {
    if unsafe { kafka_common_KafkaFuture_is_done(future) } == 0 {
        return KafkaFuture::completed_future(Err(Error::local_illegal_argument(
            "The futures passed to AlterUserScramCredentialsResult must already be complete",
        )));
    }
    let mut value: *mut c_void = ptr::null_mut();
    let error = unsafe { kafka_common_KafkaFuture_get(future, &mut value) };
    KafkaFuture::completed_future(unsafe { take_error(error) }.map_or(Ok(()), Err))
}

/// `new AlterUserScramCredentialsResult(Map<String, KafkaFuture<Void>> futures)`:
/// `futures` is a borrowed map of `const char *` user names to
/// `const kafka_common_KafkaFuture_t *`, copied during the call; the caller
/// keeps ownership of the map, its keys and its futures. Only futures that
/// are already complete (`kafka_common_KafkaFuture_is_done`) can be copied
/// across the boundary: the outcome of each is taken as it stands, and a
/// future still pending stands as one failed with the translation of
/// `IllegalArgumentException`. Owned, freed with
/// [`kafka_admin_AlterUserScramCredentialsResult_destroy`].
///
/// # Safety
///
/// `futures` must be null or a valid map whose keys are NUL-terminated
/// strings and whose values are future handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterUserScramCredentialsResult_new(
    futures: *const kafka_Map_t,
) -> *mut kafka_admin_AlterUserScramCredentialsResult_t {
    let futures: HashMap<String, KafkaFuture<()>> = unsafe { map_entries(futures) }
        .iter()
        .map(|&(k, v)| {
            (unsafe { c_str_to_string(k as *const c_char) }, unsafe {
                resolved_void_future(v as *const kafka_common_KafkaFuture_t)
            })
        })
        .collect();
    box_alter_user_scram_credentials_result(AlterUserScramCredentialsResult::new(futures), &FutureCtx::detached())
}

/// `AlterUserScramCredentialsResult.values()`: an owned map, sorted by user
/// name, of owned `char *` user names to owned
/// `kafka_common_KafkaFuture_t *` (`KafkaFuture<Void>`: `get` delivers
/// `NULL`). Freed with `kafka_Map_destroy`, which frees the keys and the
/// futures.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterUserScramCredentialsResult_values(
    self_: *const kafka_admin_AlterUserScramCredentialsResult_t,
) -> *mut kafka_Map_t {
    let h = unsafe { handle(self_) };
    h.ctx.string_keyed_future_map(h.result.values().iter(), FutureCtx::void_future)
}

/// `AlterUserScramCredentialsResult.all()`: an owned `KafkaFuture<Void>`
/// (its `get` delivers `NULL`) that succeeds once every alteration
/// succeeded, freed with `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterUserScramCredentialsResult_all(
    self_: *const kafka_admin_AlterUserScramCredentialsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.void_future(&h.result.all())
}

/// Frees a result handle; null is a no-op. Maps and futures taken from the
/// result stay valid until they are destroyed themselves.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterUserScramCredentialsResult_destroy(
    self_: *mut kafka_admin_AlterUserScramCredentialsResult_t,
) {
    unsafe { destroy_result::<AlterUserScramCredentialsResult, _>(self_) }
}
