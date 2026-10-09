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

//! C bindings for `org.apache.kafka.clients.admin.CreateTopicsResult` and
//! its nested `CreateTopicsResult.TopicMetadataAndConfig`.

use std::ffi::{c_char, c_void};

use crate::admin::{CreateTopicsResult, TopicMetadataAndConfig};
use crate::common::{Error, KafkaFuture, Uuid};
use crate::ffi::admin::config::{box_config, config_ref, destroy_config_element, kafka_admin_Config_t};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, out_slot, result_ref};
use crate::ffi::common::uuid::{box_uuid, kafka_common_Uuid_destroy, kafka_common_Uuid_t, uuid_of};
use crate::ffi::common::{kafka_common_Error_t, take_error};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::{c_str_to_string, kafka_Map_t};

// ---------------------------------------------------------------------------
// CreateTopicsResult.TopicMetadataAndConfig
// ---------------------------------------------------------------------------

/// Opaque handle to a [`TopicMetadataAndConfig`]
/// (`CreateTopicsResult.TopicMetadataAndConfig`), owned by the caller and
/// freed with [`kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_destroy`].
#[repr(C)]
pub struct kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_t {
    _private: [u8; 0],
}

/// Hands `value` to C as an owned handle.
pub(crate) fn box_topic_metadata_and_config(
    value: TopicMetadataAndConfig,
) -> *mut kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_t {
    Box::into_raw(Box::new(value)) as *mut kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `value` must be a valid topic-metadata-and-config handle.
pub(crate) unsafe fn topic_metadata_and_config_ref<'a>(
    value: *const kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_t,
) -> &'a TopicMetadataAndConfig {
    unsafe { &*(value as *const TopicMetadataAndConfig) }
}

/// `new TopicMetadataAndConfig(Uuid topicId, int numPartitions, int replicationFactor, Config config)`:
/// `topic_id` and `config` are borrowed and copied during the call. Owned,
/// freed with [`kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_destroy`].
///
/// # Safety
///
/// `topic_id` must be a valid uuid handle and `config` a valid config
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_new(
    topic_id: *const kafka_common_Uuid_t,
    num_partitions: i32,
    replication_factor: i32,
    config: *const kafka_admin_Config_t,
) -> *mut kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_t {
    box_topic_metadata_and_config(TopicMetadataAndConfig::new(
        unsafe { uuid_of(topic_id) },
        num_partitions,
        replication_factor,
        unsafe { config_ref(config) }.clone(),
    ))
}

/// `new TopicMetadataAndConfig(ApiException exception)`: a holder whose
/// every accessor returns `error`. `error` is consumed: the caller must not
/// destroy it. A null `error` (which Java's constructor does not accept)
/// stands for the translation of `IllegalArgumentException`. Owned, freed
/// with [`kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_destroy`].
///
/// # Safety
///
/// `error` must be null or an owned error handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_with_error(
    error: *mut kafka_common_Error_t,
) -> *mut kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_t {
    let error = unsafe { take_error(error) }
        .unwrap_or_else(|| Error::local_illegal_argument("TopicMetadataAndConfig requires the error it stands for"));
    box_topic_metadata_and_config(TopicMetadataAndConfig::with_error(error))
}

/// `topicId()`: stores in `*out_topic_id` an owned `kafka_common_Uuid_t *`
/// (freed with `kafka_common_Uuid_destroy`) and returns `NULL`, or returns
/// the owned error this holder stands for.
///
/// # Safety
///
/// `self_` must be a valid handle and `out_topic_id` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_topic_id(
    self_: *const kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_t,
    out_topic_id: *mut *mut kafka_common_Uuid_t,
) -> *mut kafka_common_Error_t {
    unsafe { out_slot(topic_metadata_and_config_ref(self_).topic_id(), out_topic_id, box_uuid) }
}

/// `numPartitions()`: stores the partition count in `*out_num_partitions`
/// and returns `NULL`, or returns the owned error this holder stands for.
///
/// # Safety
///
/// `self_` must be a valid handle and `out_num_partitions` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_num_partitions(
    self_: *const kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_t,
    out_num_partitions: *mut i32,
) -> *mut kafka_common_Error_t {
    unsafe { out_slot(topic_metadata_and_config_ref(self_).num_partitions(), out_num_partitions, |n| n) }
}

/// `replicationFactor()`: stores the replication factor in
/// `*out_replication_factor` and returns `NULL`, or returns the owned error
/// this holder stands for.
///
/// # Safety
///
/// `self_` must be a valid handle and `out_replication_factor` a valid
/// slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_replication_factor(
    self_: *const kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_t,
    out_replication_factor: *mut i32,
) -> *mut kafka_common_Error_t {
    unsafe {
        out_slot(
            topic_metadata_and_config_ref(self_).replication_factor(),
            out_replication_factor,
            |n| n,
        )
    }
}

/// `config()`: stores in `*out_config` an owned `kafka_admin_Config_t *`
/// (freed with `kafka_admin_Config_destroy`) and returns `NULL`, or returns
/// the owned error this holder stands for.
///
/// # Safety
///
/// `self_` must be a valid handle and `out_config` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_config(
    self_: *const kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_t,
    out_config: *mut *mut kafka_admin_Config_t,
) -> *mut kafka_common_Error_t {
    unsafe { out_slot(topic_metadata_and_config_ref(self_).config(), out_config, box_config) }
}

/// Frees a handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_destroy(
    self_: *mut kafka_admin_CreateTopicsResult_TopicMetadataAndConfig_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut TopicMetadataAndConfig) });
    }
}

// ---------------------------------------------------------------------------
// CreateTopicsResult
// ---------------------------------------------------------------------------

/// Opaque handle to a [`CreateTopicsResult`], owned by the caller and freed
/// with [`kafka_admin_CreateTopicsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_CreateTopicsResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C, binding the futures it exposes to `ctx`.
pub(crate) fn box_create_topics_result(
    result: CreateTopicsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_CreateTopicsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle.
unsafe fn handle<'a>(self_: *const kafka_admin_CreateTopicsResult_t) -> &'a ResultHandle<CreateTopicsResult> {
    unsafe { result_ref(self_) }
}

/// Frees a `kafka_common_Uuid_t *` a future delivered.
///
/// # Safety
///
/// `element` must be an owned `kafka_common_Uuid_t *`.
unsafe fn destroy_uuid_element(element: *mut c_void) {
    unsafe { kafka_common_Uuid_destroy(element as *mut kafka_common_Uuid_t) }
}

/// The per-topic refinement `get` builds, or — when `topic` was not part of
/// the request — a future failed with the translation of the
/// `IllegalArgumentException` Java throws (the Rust accessor panics there,
/// which a C caller cannot be exposed to).
///
/// # Safety
///
/// `h` must be a live result handle and `topic` a valid NUL-terminated string.
unsafe fn topic_future<T>(
    h: &ResultHandle<CreateTopicsResult>,
    topic: *const c_char,
    get: impl FnOnce(&CreateTopicsResult, &str) -> KafkaFuture<T>,
) -> KafkaFuture<T>
where
    T: Clone + Send + Sync + 'static,
{
    let topic = unsafe { c_str_to_string(topic) };
    if h.result.values().contains_key(&topic) {
        get(&h.result, &topic)
    } else {
        KafkaFuture::completed_future(Err(Error::local_illegal_argument(format!(
            "Topic {topic} was not part of the createTopics request"
        ))))
    }
}

/// `CreateTopicsResult.values()`: an owned map, sorted by topic name, of
/// owned `char *` topic names to owned `kafka_common_KafkaFuture_t *`
/// (`KafkaFuture<Void>`: `get` delivers `NULL`). Freed with
/// `kafka_Map_destroy`, which frees the keys and the futures.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateTopicsResult_values(
    self_: *const kafka_admin_CreateTopicsResult_t,
) -> *mut kafka_Map_t {
    let h = unsafe { handle(self_) };
    let values = h.result.values();
    h.ctx.string_keyed_future_map(values.iter(), FutureCtx::void_future)
}

/// `CreateTopicsResult.all()`: an owned `KafkaFuture<Void>` (its `get`
/// delivers `NULL`) that succeeds once every topic was created, freed with
/// `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateTopicsResult_all(
    self_: *const kafka_admin_CreateTopicsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.void_future(&h.result.all())
}

/// `CreateTopicsResult.config(String topic)`: an owned future whose `get`
/// delivers a `kafka_admin_Config_t *` owned by the future, freed with
/// `kafka_common_KafkaFuture_destroy`. The future fails with the
/// translation of `IllegalArgumentException` when `topic` was not part of
/// the request.
///
/// # Safety
///
/// `self_` must be a live result handle and `topic` a valid NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateTopicsResult_config(
    self_: *const kafka_admin_CreateTopicsResult_t,
    topic: *const c_char,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    let future = unsafe { topic_future(h, topic, CreateTopicsResult::config) };
    h.ctx
        .handle_future(&future, |config| box_config(config) as *mut c_void, destroy_config_element)
}

/// `CreateTopicsResult.topicId(String topic)`: an owned future whose `get`
/// delivers a `kafka_common_Uuid_t *` owned by the future, freed with
/// `kafka_common_KafkaFuture_destroy`. The future fails with the
/// translation of `IllegalArgumentException` when `topic` was not part of
/// the request.
///
/// # Safety
///
/// `self_` must be a live result handle and `topic` a valid NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateTopicsResult_topic_id(
    self_: *const kafka_admin_CreateTopicsResult_t,
    topic: *const c_char,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    let future: KafkaFuture<Uuid> = unsafe { topic_future(h, topic, CreateTopicsResult::topic_id) };
    h.ctx
        .handle_future(&future, |uuid| box_uuid(uuid) as *mut c_void, destroy_uuid_element)
}

/// `CreateTopicsResult.numPartitions(String topic)`: an owned future whose
/// `get` delivers an `int32_t *` owned by the future, freed with
/// `kafka_common_KafkaFuture_destroy`. The future fails with the
/// translation of `IllegalArgumentException` when `topic` was not part of
/// the request.
///
/// # Safety
///
/// `self_` must be a live result handle and `topic` a valid NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateTopicsResult_num_partitions(
    self_: *const kafka_admin_CreateTopicsResult_t,
    topic: *const c_char,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx
        .value_future(&unsafe { topic_future(h, topic, CreateTopicsResult::num_partitions) })
}

/// `CreateTopicsResult.replicationFactor(String topic)`: an owned future
/// whose `get` delivers an `int32_t *` owned by the future, freed with
/// `kafka_common_KafkaFuture_destroy`. The future fails with the
/// translation of `IllegalArgumentException` when `topic` was not part of
/// the request.
///
/// # Safety
///
/// `self_` must be a live result handle and `topic` a valid NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateTopicsResult_replication_factor(
    self_: *const kafka_admin_CreateTopicsResult_t,
    topic: *const c_char,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx
        .value_future(&unsafe { topic_future(h, topic, CreateTopicsResult::replication_factor) })
}

/// Frees a result handle; null is a no-op. Maps and futures taken from the
/// result stay valid until they are destroyed themselves.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_CreateTopicsResult_destroy(self_: *mut kafka_admin_CreateTopicsResult_t) {
    unsafe { destroy_result::<CreateTopicsResult, _>(self_) }
}
