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

//! C bindings for `org.apache.kafka.clients.admin.DescribeConfigsResult`.

use std::collections::HashMap;
use std::ffi::c_void;

use crate::admin::{Config, DescribeConfigsResult};
use crate::common::KafkaFuture;
use crate::common::config::ConfigResource;
use crate::ffi::admin::config::{box_config, destroy_config_element};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_map_element, destroy_result, result_ref};
use crate::ffi::common::config::config_resource::{
    box_config_resource, config_resource_key_eq, destroy_config_resource_element,
};
use crate::ffi::ffi_guard;
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::{box_map, kafka_Map_t};

/// Opaque handle to a [`DescribeConfigsResult`], owned by the caller and
/// freed with [`kafka_admin_DescribeConfigsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DescribeConfigsResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C, binding the futures it exposes to `ctx`.
pub(crate) fn box_describe_configs_result(
    result: DescribeConfigsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_DescribeConfigsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle, kept alive for `'a`.
unsafe fn handle<'a>(self_: *const kafka_admin_DescribeConfigsResult_t) -> &'a ResultHandle<DescribeConfigsResult> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle kept alive for `'a`,
    // and every `kafka_admin_DescribeConfigsResult_t` is built by
    // `box_result::<DescribeConfigsResult, _>` (in `box_describe_configs_result`), as `result_ref`
    // requires.
    unsafe { result_ref(self_) }
}

/// The C shape of `all()`'s value: an owned map, sorted by the resource's
/// `toString()`, of owned `kafka_common_config_ConfigResource_t *`
/// (compared by value in `kafka_Map_get`) to owned `kafka_admin_Config_t *`.
fn box_configs(configs: HashMap<ConfigResource, Config>) -> *mut kafka_Map_t {
    let mut entries: Vec<(ConfigResource, Config)> = configs.into_iter().collect();
    entries.sort_by_cached_key(|(resource, _)| resource.to_string());
    let entries = entries
        .into_iter()
        .map(|(resource, config)| (box_config_resource(resource) as *mut c_void, box_config(config) as *mut c_void))
        .collect();
    box_map(
        entries,
        Some(destroy_config_resource_element),
        Some(destroy_config_element),
        Some(config_resource_key_eq),
    )
}

/// Return a map from resources to futures which can be used to check the status of the
/// configuration for each resource.
///
/// # Returns
///
/// A map, sorted by the resource's `toString()`, of owned `kafka_common_config_ConfigResource_t *`
/// keys (compared by value in `kafka_Map_get`) to owned `kafka_common_KafkaFuture_t *` whose `get`
/// delivers a `kafka_admin_Config_t *` owned by the future.
///
/// # Safety
///
/// `self_` must be a live result handle.
///
/// The returned map is owned by the caller, freed with `kafka_Map_destroy`, which frees the keys
/// and the futures.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeConfigsResult_values(
    self_: *const kafka_admin_DescribeConfigsResult_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    let mut entries: Vec<(&ConfigResource, &KafkaFuture<Config>)> = h.result.values().iter().collect();
    entries.sort_by_cached_key(|(resource, _)| resource.to_string());
    h.ctx.keyed_future_map(
        entries,
        |resource| box_config_resource(resource.clone()) as *mut c_void,
        destroy_config_resource_element,
        config_resource_key_eq,
        // SAFETY: `destroy_config_element` frees exactly the handle `box_config` makes, and the
        // handle holds only owned data and no thread-bound state, so it may be freed from any
        // thread and read from several at once, as `handle_future` requires.
        |ctx, future| unsafe {
            ctx.handle_future(future, |config| box_config(config) as *mut c_void, destroy_config_element)
        },
    )
}

/// Return a future which succeeds only if all the config descriptions succeed.
///
/// # Returns
///
/// A future whose `get` delivers a `kafka_Map_t *` owned by the future, of
/// `kafka_common_config_ConfigResource_t *` keys (sorted by `toString()`, compared by value in
/// `kafka_Map_get`) to `kafka_admin_Config_t *`; the future fails when any description failed.
///
/// # Safety
///
/// `self_` must be a live result handle.
///
/// The returned future is owned by the caller, freed with `kafka_common_KafkaFuture_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeConfigsResult_all(
    self_: *const kafka_admin_DescribeConfigsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    // SAFETY: `destroy_map_element` frees exactly the map `box_configs` makes through `box_map`,
    // and the handle holds only owned data and no thread-bound state, so it may be freed from any
    // thread and read from several at once, as `handle_future` requires.
    unsafe {
        h.ctx.handle_future(
            &h.result.all(),
            |configs| box_configs(configs) as *mut c_void,
            destroy_map_element,
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
pub unsafe extern "C" fn kafka_admin_DescribeConfigsResult_destroy(self_: *mut kafka_admin_DescribeConfigsResult_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards; every
    // `kafka_admin_DescribeConfigsResult_t` is built by `box_result::<DescribeConfigsResult, _>`,
    // as `destroy_result` requires.
    unsafe { destroy_result::<DescribeConfigsResult, _>(self_) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::config::config_resource::Type;
    use crate::ffi::common::config::config_resource::{
        kafka_common_config_ConfigResource_destroy, kafka_common_config_ConfigResource_t,
    };
    use crate::ffi::util::{kafka_Map_destroy, kafka_Map_get, kafka_Map_put, kafka_Map_size};

    /// A `NULL` key in a map keyed by value equals only a `NULL` key: the
    /// config-resource comparison, which reads both keys, never sees it.
    #[test]
    fn a_null_key_beside_a_config_resource_key_is_never_compared() {
        let map = box_configs(HashMap::new());
        let key = box_config_resource(ConfigResource::new(Type::Topic, "t".to_string())) as *mut c_void;
        let probe = box_config_resource(ConfigResource::new(Type::Topic, "t".to_string())) as *mut c_void;
        let config = box_config(Config::new(Vec::new())) as *mut c_void;
        // SAFETY: Test code: `map` is live until its destroy below, which frees `key` and `config`,
        // the map owning both once put; `probe` is live until its own destroy.
        unsafe {
            kafka_Map_put(map, std::ptr::null_mut(), std::ptr::null_mut());
            kafka_Map_put(map, key, config);
            assert_eq!(kafka_Map_size(map), 2);
            assert!(kafka_Map_get(map, std::ptr::null_mut()).is_null());
            assert_eq!(kafka_Map_get(map, probe), config);
            kafka_Map_destroy(map);
            kafka_common_config_ConfigResource_destroy(probe as *mut kafka_common_config_ConfigResource_t);
        }
    }
}
