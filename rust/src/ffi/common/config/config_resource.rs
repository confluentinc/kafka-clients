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

//! `kafka_common_config_ConfigResource_t`:
//! `org.apache.kafka.common.config.ConfigResource` (CLAUDE.md §4), with its
//! nested `Type` enum as `kafka_common_config_ConfigResource_Type_t`
//! (CLAUDE.md §4, "Nested types" and "Enums"): borrowed per-value singletons
//! plus the C enum `kafka_common_config_ConfigResource_Type_e`.

#![expect(non_camel_case_types)]

use std::ffi::{CString, c_char, c_void};

use crate::common::config::ConfigResource;
use crate::common::config::config_resource::Type;
use crate::ffi::ffi_guard;
use crate::ffi::util::{c_str_to_string, into_c_string, owned_c_string};

// ---------------------------------------------------------------------------
// ConfigResource.Type
// ---------------------------------------------------------------------------

/// Opaque handle to a [`Type`] singleton.
#[repr(C)]
pub struct kafka_common_config_ConfigResource_Type_t {
    _private: [u8; 0],
}

/// The values of [`Type`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_config_ConfigResource_Type_e {
    /// `Type::Group`: Java's `GROUP`.
    kafka_common_config_ConfigResource_Type_GROUP,
    /// `Type::ClientMetrics`: Java's `CLIENT_METRICS`.
    kafka_common_config_ConfigResource_Type_CLIENT_METRICS,
    /// `Type::BrokerLogger`: Java's `BROKER_LOGGER`.
    kafka_common_config_ConfigResource_Type_BROKER_LOGGER,
    /// `Type::Broker`: Java's `BROKER`.
    kafka_common_config_ConfigResource_Type_BROKER,
    /// `Type::Topic`: Java's `TOPIC`.
    kafka_common_config_ConfigResource_Type_TOPIC,
    /// `Type::Unknown`: Java's `UNKNOWN`.
    kafka_common_config_ConfigResource_Type_UNKNOWN,
}

/// One static instance per value, indexed by
/// [`kafka_common_config_ConfigResource_Type_e`].
static TYPES: [Type; 6] = [
    Type::Group,
    Type::ClientMetrics,
    Type::BrokerLogger,
    Type::Broker,
    Type::Topic,
    Type::Unknown,
];

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn type_enum_of(value: Type) -> kafka_common_config_ConfigResource_Type_e {
    match value {
        Type::Group => kafka_common_config_ConfigResource_Type_e::kafka_common_config_ConfigResource_Type_GROUP,
        Type::ClientMetrics => {
            kafka_common_config_ConfigResource_Type_e::kafka_common_config_ConfigResource_Type_CLIENT_METRICS
        },
        Type::BrokerLogger => {
            kafka_common_config_ConfigResource_Type_e::kafka_common_config_ConfigResource_Type_BROKER_LOGGER
        },
        Type::Broker => kafka_common_config_ConfigResource_Type_e::kafka_common_config_ConfigResource_Type_BROKER,
        Type::Topic => kafka_common_config_ConfigResource_Type_e::kafka_common_config_ConfigResource_Type_TOPIC,
        Type::Unknown => kafka_common_config_ConfigResource_Type_e::kafka_common_config_ConfigResource_Type_UNKNOWN,
    }
}

/// The borrowed singleton standing for `value`.
pub(crate) fn type_singleton(value: Type) -> *const kafka_common_config_ConfigResource_Type_t {
    &TYPES[type_enum_of(value) as usize] as *const Type as *const kafka_common_config_ConfigResource_Type_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `value` must be a singleton returned by this module.
pub(crate) unsafe fn type_of(value: *const kafka_common_config_ConfigResource_Type_t) -> Type {
    // SAFETY: Per this function's `# Safety`, `value` is a singleton returned by this module, i.e.
    // a pointer to an element of the `static` `TYPES` array (`type_singleton`), so it points at a
    // live, initialised `Type`, which is `Copy` and read out here.
    unsafe { *(value as *const Type) }
}

/// `ConfigResource.Type.GROUP`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_config_ConfigResource_Type_group() -> *const kafka_common_config_ConfigResource_Type_t {
    type_singleton(Type::Group)
}

/// `ConfigResource.Type.CLIENT_METRICS`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_config_ConfigResource_Type_client_metrics()
-> *const kafka_common_config_ConfigResource_Type_t {
    type_singleton(Type::ClientMetrics)
}

/// `ConfigResource.Type.BROKER_LOGGER`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_config_ConfigResource_Type_broker_logger()
-> *const kafka_common_config_ConfigResource_Type_t {
    type_singleton(Type::BrokerLogger)
}

/// `ConfigResource.Type.BROKER`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_config_ConfigResource_Type_broker() -> *const kafka_common_config_ConfigResource_Type_t {
    type_singleton(Type::Broker)
}

/// `ConfigResource.Type.TOPIC`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_config_ConfigResource_Type_topic() -> *const kafka_common_config_ConfigResource_Type_t {
    type_singleton(Type::Topic)
}

/// `ConfigResource.Type.UNKNOWN`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_config_ConfigResource_Type_unknown() -> *const kafka_common_config_ConfigResource_Type_t
{
    type_singleton(Type::Unknown)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// A caught Rust panic returns `kafka_common_config_ConfigResource_Type_UNKNOWN`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard(fallback = kafka_common_config_ConfigResource_Type_e::kafka_common_config_ConfigResource_Type_UNKNOWN)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_config_ConfigResource_Type__enum(
    self_: *const kafka_common_config_ConfigResource_Type_t,
) -> kafka_common_config_ConfigResource_Type_e {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `type_of` requires; the value is copied out.
    type_enum_of(unsafe { type_of(self_) })
}

/// `id()`: the wire-protocol byte.
///
/// A caught Rust panic returns -1, which is no code of this Java `byte`
/// enum: its 0 is the valid `UNKNOWN`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard(fallback = -1)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_config_ConfigResource_Type_id(
    self_: *const kafka_common_config_ConfigResource_Type_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `type_of` requires; the value is copied out.
    unsafe { type_of(self_) }.id()
}

/// `forId(byte id)`: the singleton for a wire-protocol byte, `UNKNOWN` for
/// one this client does not know.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_config_ConfigResource_Type_for_id(
    id: i8,
) -> *const kafka_common_config_ConfigResource_Type_t {
    type_singleton(Type::for_id(id))
}

// ---------------------------------------------------------------------------
// ConfigResource
// ---------------------------------------------------------------------------

/// Opaque handle to a [`ConfigResource`].
#[repr(C)]
pub struct kafka_common_config_ConfigResource_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_config_ConfigResource_t`] points at: the resource
/// plus the NUL-terminated name its getter borrows out.
pub(crate) struct ConfigResourceInner {
    resource: ConfigResource,
    name_c: CString,
}

impl ConfigResourceInner {
    pub(crate) fn new(resource: ConfigResource) -> Self {
        let name_c = owned_c_string(resource.name());
        Self { resource, name_c }
    }
}

/// What a handle points at.
///
/// # Safety
///
/// `resource` must be a valid config-resource handle, alive for `'a`.
unsafe fn inner_ref<'a>(resource: *const kafka_common_config_ConfigResource_t) -> &'a ConfigResourceInner {
    // SAFETY: Per this function's `# Safety`, `resource` is a valid config-resource handle alive
    // for `'a`, so it points at a live `ConfigResourceInner`, the `Box::into_raw` of
    // `box_config_resource` (no view of this type is handed out). Nothing mutates a
    // `ConfigResourceInner` while a handle to it exists, so the shared reference is sound for `'a`.
    unsafe { &*(resource as *const ConfigResourceInner) }
}

/// The resource behind a handle.
///
/// # Safety
///
/// `resource` must be a valid config-resource handle, alive for `'a`.
pub(crate) unsafe fn config_resource_ref<'a>(
    resource: *const kafka_common_config_ConfigResource_t,
) -> &'a ConfigResource {
    // SAFETY: Per this function's `# Safety`, `resource` is a valid config-resource handle, as
    // `inner_ref` requires; the reference returned lives for the same `'a`.
    &unsafe { inner_ref(resource) }.resource
}

/// Frees a `kafka_common_config_ConfigResource_t *` element of an owned
/// list, or key of an owned map.
///
/// # Safety
///
/// `element` must be an owned config-resource handle not yet destroyed.
pub(crate) unsafe fn destroy_config_resource_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned config-resource handle not yet
    // destroyed, as `kafka_common_config_ConfigResource_destroy` requires.
    unsafe { kafka_common_config_ConfigResource_destroy(element as *mut kafka_common_config_ConfigResource_t) }
}

/// Compares two `kafka_common_config_ConfigResource_t *` keys by value, for
/// `kafka_Map_get` on a Rust-built map keyed by config resource.
///
/// # Safety
///
/// `a` and `b` must be valid config-resource handles, alive for this call.
pub(crate) unsafe fn config_resource_key_eq(a: *mut c_void, b: *mut c_void) -> bool {
    // SAFETY: Per this function's `# Safety`, `a` and `b` are valid config-resource handles, alive
    // for this call, as `config_resource_ref` requires; both borrows end with the comparison.
    unsafe {
        config_resource_ref(a as *const kafka_common_config_ConfigResource_t)
            == config_resource_ref(b as *const kafka_common_config_ConfigResource_t)
    }
}

/// Hands `resource` to C as an owned handle, freed with
/// [`kafka_common_config_ConfigResource_destroy`].
pub(crate) fn box_config_resource(resource: ConfigResource) -> *mut kafka_common_config_ConfigResource_t {
    Box::into_raw(Box::new(ConfigResourceInner::new(resource))) as *mut kafka_common_config_ConfigResource_t
}

/// `new ConfigResource(Type type, String name)`: the name is copied. Owned,
/// freed with [`kafka_common_config_ConfigResource_destroy`].
///
/// # Safety
///
/// `resource_type` must be a type singleton and `name` a valid
/// NUL-terminated string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_config_ConfigResource_new(
    resource_type: *const kafka_common_config_ConfigResource_Type_t,
    name: *const c_char,
) -> *mut kafka_common_config_ConfigResource_t {
    // SAFETY: Per this function's `# Safety`, `resource_type` is a type singleton, as `type_of`
    // requires, and its value is copied out; `name` is a valid NUL-terminated string, as
    // `c_str_to_string` requires, and is copied into an owned `String`. Nothing borrows either
    // argument after this call.
    box_config_resource(ConfigResource::new(unsafe { type_of(resource_type) }, unsafe {
        c_str_to_string(name)
    }))
}

/// `type()`: the borrowed `Type` singleton.
///
/// # Safety
///
/// `self_` must be a valid config-resource handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_config_ConfigResource_type(
    self_: *const kafka_common_config_ConfigResource_t,
) -> *const kafka_common_config_ConfigResource_Type_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid config-resource handle, as
    // `config_resource_ref` requires; the value is copied out and mapped to its static singleton,
    // so the borrow lasts for this call only.
    type_singleton(unsafe { config_resource_ref(self_) }.r#type())
}

/// `name()`: borrowed from the handle; empty for the default resource.
///
/// # Safety
///
/// `self_` must be a valid config-resource handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_config_ConfigResource_name(
    self_: *const kafka_common_config_ConfigResource_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid config-resource handle, as
    // `inner_ref` requires; the string returned is the handle's own C string, so it lives as long
    // as the handle; that is the rustdoc's "borrowed from the handle".
    unsafe { inner_ref(self_) }.name_c.as_ptr()
}

/// `isDefault()`: whether the name is empty, naming the default resource of
/// the type.
///
/// # Safety
///
/// `self_` must be a valid config-resource handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_config_ConfigResource_is_default(
    self_: *const kafka_common_config_ConfigResource_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid config-resource handle, as
    // `config_resource_ref` requires; the reference is used only within this call.
    i8::from(unsafe { config_resource_ref(self_) }.is_default())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid config-resource handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_config_ConfigResource_to_string(
    self_: *const kafka_common_config_ConfigResource_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid config-resource handle, as
    // `config_resource_ref` requires; the result is copied into an owned value before return, so
    // the borrow lasts for this call only.
    into_c_string(&unsafe { config_resource_ref(self_) }.to_string())
}

/// Frees an owned config-resource handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned config-resource handle not yet
/// destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_config_ConfigResource_destroy(self_: *mut kafka_common_config_ConfigResource_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, i.e. the `Box::into_raw` of a `ConfigResourceInner` in
        // `box_config_resource`, so `from_raw` reclaims that allocation, once.
        drop(unsafe { Box::from_raw(self_ as *mut ConfigResourceInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn type_singletons_round_trip_and_follow_the_rust_enum() {
        for (index, &value) in TYPES.iter().enumerate() {
            let handle = type_singleton(value);
            // SAFETY: Test code: every handle passed is a singleton from `type_singleton`, which
            // points into the static `TYPES` array and is never freed.
            unsafe {
                assert_eq!(type_of(handle), value);
                assert_eq!(kafka_common_config_ConfigResource_Type__enum(handle) as usize, index);
                assert_eq!(kafka_common_config_ConfigResource_Type_id(handle), value.id());
                assert_eq!(kafka_common_config_ConfigResource_Type_for_id(value.id()), handle);
            }
        }
        assert_eq!(kafka_common_config_ConfigResource_Type_group(), type_singleton(Type::Group));
        assert_eq!(
            kafka_common_config_ConfigResource_Type_client_metrics(),
            type_singleton(Type::ClientMetrics)
        );
        assert_eq!(
            kafka_common_config_ConfigResource_Type_broker_logger(),
            type_singleton(Type::BrokerLogger)
        );
        assert_eq!(kafka_common_config_ConfigResource_Type_broker(), type_singleton(Type::Broker));
        assert_eq!(kafka_common_config_ConfigResource_Type_topic(), type_singleton(Type::Topic));
        assert_eq!(kafka_common_config_ConfigResource_Type_unknown(), type_singleton(Type::Unknown));
        // SAFETY: Test code: the handle passed is a singleton from this module's getter, which
        // points into the static `TYPES` array and is never freed.
        unsafe {
            assert_eq!(
                kafka_common_config_ConfigResource_Type_id(kafka_common_config_ConfigResource_Type_topic()),
                2
            );
        }
        assert_eq!(
            kafka_common_config_ConfigResource_Type_for_id(-1),
            kafka_common_config_ConfigResource_Type_unknown()
        );
    }

    #[test]
    fn resource_getters_and_default_follow_java() {
        let name = CString::new("orders").unwrap();
        let empty = CString::new("").unwrap();
        // SAFETY: Test code: the type arguments are singletons from this module's getters, `name`
        // and `empty` are `CString`s that outlive the constructors copying them, the name view is
        // read only while `topic` is alive, and every owned handle (the resources, the string) is
        // destroyed once.
        unsafe {
            let topic =
                kafka_common_config_ConfigResource_new(kafka_common_config_ConfigResource_Type_topic(), name.as_ptr());
            assert_eq!(
                *config_resource_ref(topic),
                ConfigResource::new(Type::Topic, "orders".to_string())
            );
            assert_eq!(
                kafka_common_config_ConfigResource_type(topic),
                kafka_common_config_ConfigResource_Type_topic()
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_config_ConfigResource_name(topic)).to_str().unwrap(),
                "orders"
            );
            assert_eq!(kafka_common_config_ConfigResource_is_default(topic), 0);
            let s = kafka_common_config_ConfigResource_to_string(topic);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                ConfigResource::new(Type::Topic, "orders".to_string()).to_string()
            );
            kafka_string_destroy(s);
            kafka_common_config_ConfigResource_destroy(topic);

            let default_broker = kafka_common_config_ConfigResource_new(
                kafka_common_config_ConfigResource_Type_broker(),
                empty.as_ptr(),
            );
            assert_eq!(kafka_common_config_ConfigResource_is_default(default_broker), 1);
            kafka_common_config_ConfigResource_destroy(default_broker);
            kafka_common_config_ConfigResource_destroy(ptr::null_mut());
        }
    }
}
