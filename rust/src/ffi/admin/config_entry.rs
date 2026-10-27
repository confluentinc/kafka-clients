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

//! `kafka_admin_ConfigEntry_t`: `org.apache.kafka.clients.admin.ConfigEntry`
//! with its nested types (CLAUDE.md §4, "Nested types" and "Enums"): the
//! enums `kafka_admin_ConfigEntry_ConfigSource_t` and
//! `kafka_admin_ConfigEntry_ConfigType_t` as borrowed singletons, the class
//! `kafka_admin_ConfigEntry_ConfigSynonym_t`, and the Rust-only
//! `kafka_admin_ConfigEntryOptions_t` / `kafka_admin_ConfigEntryOptionsBuilder_t`
//! standing for Java's widest `ConfigEntry` constructor (CLAUDE.md §2).
//!
//! The string getters borrow NUL-terminated copies kept beside the value, so
//! they stay valid as long as the handle they were read from.

#![expect(non_camel_case_types)]

use std::ffi::{CString, c_char, c_void};

use crate::admin::config_entry::{ConfigSource, ConfigSynonym, ConfigType};
use crate::admin::{ConfigEntry, ConfigEntryOptions, ConfigEntryOptionsBuilder};
use crate::ffi::common::deliver_value;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::ffi_guard;
use crate::ffi::util::{
    box_list, c_str_to_option, c_str_to_string, into_c_string, kafka_List_t, list_elements, owned_c_string,
};

// ---------------------------------------------------------------------------
// ConfigEntry.ConfigSource
// ---------------------------------------------------------------------------

/// Opaque handle to a [`ConfigSource`] singleton.
#[repr(C)]
pub struct kafka_admin_ConfigEntry_ConfigSource_t {
    _private: [u8; 0],
}

/// The values of [`ConfigSource`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_admin_ConfigEntry_ConfigSource_e {
    kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_TOPIC_CONFIG,
    kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_BROKER_LOGGER_CONFIG,
    kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_BROKER_CONFIG,
    kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_DEFAULT_BROKER_CONFIG,
    kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_CLIENT_METRICS_CONFIG,
    kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_GROUP_CONFIG,
    kafka_admin_ConfigEntry_ConfigSource_STATIC_BROKER_CONFIG,
    kafka_admin_ConfigEntry_ConfigSource_DEFAULT_CONFIG,
    kafka_admin_ConfigEntry_ConfigSource_UNKNOWN,
}

/// One static instance per value, indexed by
/// [`kafka_admin_ConfigEntry_ConfigSource_e`].
static SOURCES: [ConfigSource; 9] = [
    ConfigSource::DynamicTopicConfig,
    ConfigSource::DynamicBrokerLoggerConfig,
    ConfigSource::DynamicBrokerConfig,
    ConfigSource::DynamicDefaultBrokerConfig,
    ConfigSource::DynamicClientMetricsConfig,
    ConfigSource::DynamicGroupConfig,
    ConfigSource::StaticBrokerConfig,
    ConfigSource::DefaultConfig,
    ConfigSource::Unknown,
];

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn source_enum_of(source: ConfigSource) -> kafka_admin_ConfigEntry_ConfigSource_e {
    match source {
        ConfigSource::DynamicTopicConfig => {
            kafka_admin_ConfigEntry_ConfigSource_e::kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_TOPIC_CONFIG
        },
        ConfigSource::DynamicBrokerLoggerConfig => {
            kafka_admin_ConfigEntry_ConfigSource_e::kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_BROKER_LOGGER_CONFIG
        },
        ConfigSource::DynamicBrokerConfig => {
            kafka_admin_ConfigEntry_ConfigSource_e::kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_BROKER_CONFIG
        },
        ConfigSource::DynamicDefaultBrokerConfig => {
            kafka_admin_ConfigEntry_ConfigSource_e::kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_DEFAULT_BROKER_CONFIG
        },
        ConfigSource::DynamicClientMetricsConfig => {
            kafka_admin_ConfigEntry_ConfigSource_e::kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_CLIENT_METRICS_CONFIG
        },
        ConfigSource::DynamicGroupConfig => {
            kafka_admin_ConfigEntry_ConfigSource_e::kafka_admin_ConfigEntry_ConfigSource_DYNAMIC_GROUP_CONFIG
        },
        ConfigSource::StaticBrokerConfig => {
            kafka_admin_ConfigEntry_ConfigSource_e::kafka_admin_ConfigEntry_ConfigSource_STATIC_BROKER_CONFIG
        },
        ConfigSource::DefaultConfig => {
            kafka_admin_ConfigEntry_ConfigSource_e::kafka_admin_ConfigEntry_ConfigSource_DEFAULT_CONFIG
        },
        ConfigSource::Unknown => kafka_admin_ConfigEntry_ConfigSource_e::kafka_admin_ConfigEntry_ConfigSource_UNKNOWN,
    }
}

/// The borrowed singleton standing for `source`.
pub(crate) fn config_source_singleton(source: ConfigSource) -> *const kafka_admin_ConfigEntry_ConfigSource_t {
    &SOURCES[source_enum_of(source) as usize] as *const ConfigSource as *const kafka_admin_ConfigEntry_ConfigSource_t
}

/// The value behind a source singleton.
///
/// # Safety
///
/// `source` must be a singleton returned by this module.
pub(crate) unsafe fn config_source_value_of(source: *const kafka_admin_ConfigEntry_ConfigSource_t) -> ConfigSource {
    // SAFETY: Per this function's `# Safety`, `source` is a singleton returned by this module:
    // `config_source_singleton`, the only producer of `kafka_admin_ConfigEntry_ConfigSource_t`
    // pointers, casts `&SOURCES[i]` from `*const ConfigSource`, so it points at a live
    // `ConfigSource` in a `static`, which is `Copy` and immutable.
    unsafe { *(source as *const ConfigSource) }
}

/// `ConfigSource.DYNAMIC_TOPIC_CONFIG`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ConfigEntry_ConfigSource_dynamic_topic_config()
-> *const kafka_admin_ConfigEntry_ConfigSource_t {
    config_source_singleton(ConfigSource::DynamicTopicConfig)
}

/// `ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ConfigEntry_ConfigSource_dynamic_broker_logger_config()
-> *const kafka_admin_ConfigEntry_ConfigSource_t {
    config_source_singleton(ConfigSource::DynamicBrokerLoggerConfig)
}

/// `ConfigSource.DYNAMIC_BROKER_CONFIG`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ConfigEntry_ConfigSource_dynamic_broker_config()
-> *const kafka_admin_ConfigEntry_ConfigSource_t {
    config_source_singleton(ConfigSource::DynamicBrokerConfig)
}

/// `ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ConfigEntry_ConfigSource_dynamic_default_broker_config()
-> *const kafka_admin_ConfigEntry_ConfigSource_t {
    config_source_singleton(ConfigSource::DynamicDefaultBrokerConfig)
}

/// `ConfigSource.DYNAMIC_CLIENT_METRICS_CONFIG`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ConfigEntry_ConfigSource_dynamic_client_metrics_config()
-> *const kafka_admin_ConfigEntry_ConfigSource_t {
    config_source_singleton(ConfigSource::DynamicClientMetricsConfig)
}

/// `ConfigSource.DYNAMIC_GROUP_CONFIG`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ConfigEntry_ConfigSource_dynamic_group_config()
-> *const kafka_admin_ConfigEntry_ConfigSource_t {
    config_source_singleton(ConfigSource::DynamicGroupConfig)
}

/// `ConfigSource.STATIC_BROKER_CONFIG`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ConfigEntry_ConfigSource_static_broker_config()
-> *const kafka_admin_ConfigEntry_ConfigSource_t {
    config_source_singleton(ConfigSource::StaticBrokerConfig)
}

/// `ConfigSource.DEFAULT_CONFIG`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ConfigEntry_ConfigSource_default_config() -> *const kafka_admin_ConfigEntry_ConfigSource_t
{
    config_source_singleton(ConfigSource::DefaultConfig)
}

/// `ConfigSource.UNKNOWN`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ConfigEntry_ConfigSource_unknown() -> *const kafka_admin_ConfigEntry_ConfigSource_t {
    config_source_singleton(ConfigSource::Unknown)
}

/// The C enumerator of a source singleton, for a `switch`.
///
/// A caught Rust panic returns `kafka_admin_ConfigEntry_ConfigSource_UNKNOWN`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard(fallback = kafka_admin_ConfigEntry_ConfigSource_e::kafka_admin_ConfigEntry_ConfigSource_UNKNOWN)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_ConfigSource__enum(
    self_: *const kafka_admin_ConfigEntry_ConfigSource_t,
) -> kafka_admin_ConfigEntry_ConfigSource_e {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `config_source_value_of` requires.
    source_enum_of(unsafe { config_source_value_of(self_) })
}

/// `ConfigSource.toString()`: the Java constant name, as an owned string freed with
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_ConfigSource_to_string(
    self_: *const kafka_admin_ConfigEntry_ConfigSource_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `config_source_value_of` requires.
    into_c_string(&unsafe { config_source_value_of(self_) }.to_string())
}

// ---------------------------------------------------------------------------
// ConfigEntry.ConfigType
// ---------------------------------------------------------------------------

/// Opaque handle to a [`ConfigType`] singleton.
#[repr(C)]
pub struct kafka_admin_ConfigEntry_ConfigType_t {
    _private: [u8; 0],
}

/// The values of [`ConfigType`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_admin_ConfigEntry_ConfigType_e {
    kafka_admin_ConfigEntry_ConfigType_UNKNOWN,
    kafka_admin_ConfigEntry_ConfigType_BOOLEAN,
    kafka_admin_ConfigEntry_ConfigType_STRING,
    kafka_admin_ConfigEntry_ConfigType_INT,
    kafka_admin_ConfigEntry_ConfigType_SHORT,
    kafka_admin_ConfigEntry_ConfigType_LONG,
    kafka_admin_ConfigEntry_ConfigType_DOUBLE,
    kafka_admin_ConfigEntry_ConfigType_LIST,
    kafka_admin_ConfigEntry_ConfigType_CLASS,
    kafka_admin_ConfigEntry_ConfigType_PASSWORD,
}

/// One static instance per value, indexed by
/// [`kafka_admin_ConfigEntry_ConfigType_e`].
static TYPES: [ConfigType; 10] = [
    ConfigType::Unknown,
    ConfigType::Boolean,
    ConfigType::String,
    ConfigType::Int,
    ConfigType::Short,
    ConfigType::Long,
    ConfigType::Double,
    ConfigType::List,
    ConfigType::Class,
    ConfigType::Password,
];

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn type_enum_of(config_type: ConfigType) -> kafka_admin_ConfigEntry_ConfigType_e {
    match config_type {
        ConfigType::Unknown => kafka_admin_ConfigEntry_ConfigType_e::kafka_admin_ConfigEntry_ConfigType_UNKNOWN,
        ConfigType::Boolean => kafka_admin_ConfigEntry_ConfigType_e::kafka_admin_ConfigEntry_ConfigType_BOOLEAN,
        ConfigType::String => kafka_admin_ConfigEntry_ConfigType_e::kafka_admin_ConfigEntry_ConfigType_STRING,
        ConfigType::Int => kafka_admin_ConfigEntry_ConfigType_e::kafka_admin_ConfigEntry_ConfigType_INT,
        ConfigType::Short => kafka_admin_ConfigEntry_ConfigType_e::kafka_admin_ConfigEntry_ConfigType_SHORT,
        ConfigType::Long => kafka_admin_ConfigEntry_ConfigType_e::kafka_admin_ConfigEntry_ConfigType_LONG,
        ConfigType::Double => kafka_admin_ConfigEntry_ConfigType_e::kafka_admin_ConfigEntry_ConfigType_DOUBLE,
        ConfigType::List => kafka_admin_ConfigEntry_ConfigType_e::kafka_admin_ConfigEntry_ConfigType_LIST,
        ConfigType::Class => kafka_admin_ConfigEntry_ConfigType_e::kafka_admin_ConfigEntry_ConfigType_CLASS,
        ConfigType::Password => kafka_admin_ConfigEntry_ConfigType_e::kafka_admin_ConfigEntry_ConfigType_PASSWORD,
    }
}

/// The borrowed singleton standing for `config_type`.
pub(crate) fn config_type_singleton(config_type: ConfigType) -> *const kafka_admin_ConfigEntry_ConfigType_t {
    &TYPES[type_enum_of(config_type) as usize] as *const ConfigType as *const kafka_admin_ConfigEntry_ConfigType_t
}

/// The value behind a type singleton.
///
/// # Safety
///
/// `config_type` must be a singleton returned by this module.
pub(crate) unsafe fn config_type_value_of(config_type: *const kafka_admin_ConfigEntry_ConfigType_t) -> ConfigType {
    // SAFETY: Per this function's `# Safety`, `config_type` is a singleton returned by this module:
    // `config_type_singleton`, the only producer of `kafka_admin_ConfigEntry_ConfigType_t`
    // pointers, casts `&TYPES[i]` from `*const ConfigType`, so it points at a live `ConfigType` in
    // a `static`, which is `Copy` and immutable.
    unsafe { *(config_type as *const ConfigType) }
}

/// `ConfigType.UNKNOWN`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ConfigEntry_ConfigType_unknown() -> *const kafka_admin_ConfigEntry_ConfigType_t {
    config_type_singleton(ConfigType::Unknown)
}

/// `ConfigType.BOOLEAN`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ConfigEntry_ConfigType_boolean() -> *const kafka_admin_ConfigEntry_ConfigType_t {
    config_type_singleton(ConfigType::Boolean)
}

/// `ConfigType.STRING`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ConfigEntry_ConfigType_string() -> *const kafka_admin_ConfigEntry_ConfigType_t {
    config_type_singleton(ConfigType::String)
}

/// `ConfigType.INT`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ConfigEntry_ConfigType_int() -> *const kafka_admin_ConfigEntry_ConfigType_t {
    config_type_singleton(ConfigType::Int)
}

/// `ConfigType.SHORT`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ConfigEntry_ConfigType_short() -> *const kafka_admin_ConfigEntry_ConfigType_t {
    config_type_singleton(ConfigType::Short)
}

/// `ConfigType.LONG`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ConfigEntry_ConfigType_long() -> *const kafka_admin_ConfigEntry_ConfigType_t {
    config_type_singleton(ConfigType::Long)
}

/// `ConfigType.DOUBLE`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ConfigEntry_ConfigType_double() -> *const kafka_admin_ConfigEntry_ConfigType_t {
    config_type_singleton(ConfigType::Double)
}

/// `ConfigType.LIST`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ConfigEntry_ConfigType_list() -> *const kafka_admin_ConfigEntry_ConfigType_t {
    config_type_singleton(ConfigType::List)
}

/// `ConfigType.CLASS`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ConfigEntry_ConfigType_class() -> *const kafka_admin_ConfigEntry_ConfigType_t {
    config_type_singleton(ConfigType::Class)
}

/// `ConfigType.PASSWORD`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ConfigEntry_ConfigType_password() -> *const kafka_admin_ConfigEntry_ConfigType_t {
    config_type_singleton(ConfigType::Password)
}

/// The C enumerator of a type singleton, for a `switch`.
///
/// A caught Rust panic returns `kafka_admin_ConfigEntry_ConfigType_UNKNOWN`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard(fallback = kafka_admin_ConfigEntry_ConfigType_e::kafka_admin_ConfigEntry_ConfigType_UNKNOWN)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_ConfigType__enum(
    self_: *const kafka_admin_ConfigEntry_ConfigType_t,
) -> kafka_admin_ConfigEntry_ConfigType_e {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `config_type_value_of` requires.
    type_enum_of(unsafe { config_type_value_of(self_) })
}

/// `ConfigType.toString()`: the Java constant name, as an owned string freed with
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_ConfigType_to_string(
    self_: *const kafka_admin_ConfigEntry_ConfigType_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `config_type_value_of` requires.
    into_c_string(&unsafe { config_type_value_of(self_) }.to_string())
}

// ---------------------------------------------------------------------------
// ConfigEntry.ConfigSynonym
// ---------------------------------------------------------------------------

/// Opaque handle to a [`ConfigSynonym`].
#[repr(C)]
pub struct kafka_admin_ConfigEntry_ConfigSynonym_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_ConfigEntry_ConfigSynonym_t`] points at: the synonym
/// plus the NUL-terminated copies its string getters borrow out.
pub(crate) struct ConfigSynonymInner {
    synonym: ConfigSynonym,
    name_c: CString,
    value_c: Option<CString>,
}

impl ConfigSynonymInner {
    pub(crate) fn new(synonym: ConfigSynonym) -> Self {
        let name_c = owned_c_string(synonym.name());
        let value_c = synonym.value().map(owned_c_string);
        Self { synonym, name_c, value_c }
    }
}

/// What a synonym handle points at.
///
/// # Safety
///
/// `synonym` must be a valid config-synonym handle, kept alive for `'a`.
unsafe fn synonym_inner_ref<'a>(synonym: *const kafka_admin_ConfigEntry_ConfigSynonym_t) -> &'a ConfigSynonymInner {
    // SAFETY: Per this function's `# Safety`, `synonym` is a valid config-synonym handle kept alive
    // for `'a`; every `kafka_admin_ConfigEntry_ConfigSynonym_t` is the `Box::into_raw` of a
    // `Box<ConfigSynonymInner>` (`box_config_synonym`, the only producer), so the shared reference
    // is to a live `ConfigSynonymInner`, which no entry point mutates.
    unsafe { &*(synonym as *const ConfigSynonymInner) }
}

/// The synonym behind a handle.
///
/// # Safety
///
/// `synonym` must be a valid config-synonym handle, kept alive for `'a`.
pub(crate) unsafe fn config_synonym_ref<'a>(
    synonym: *const kafka_admin_ConfigEntry_ConfigSynonym_t,
) -> &'a ConfigSynonym {
    // SAFETY: Per this function's `# Safety`, `synonym` is a valid config-synonym handle kept alive
    // for `'a`, as `synonym_inner_ref` requires.
    &unsafe { synonym_inner_ref(synonym) }.synonym
}

/// Hands `synonym` to C as an owned handle, freed with
/// [`kafka_admin_ConfigEntry_ConfigSynonym_destroy`].
pub(crate) fn box_config_synonym(synonym: ConfigSynonym) -> *mut kafka_admin_ConfigEntry_ConfigSynonym_t {
    Box::into_raw(Box::new(ConfigSynonymInner::new(synonym))) as *mut kafka_admin_ConfigEntry_ConfigSynonym_t
}

/// Frees a `kafka_admin_ConfigEntry_ConfigSynonym_t *` element of an owned
/// container.
///
/// # Safety
///
/// `element` must be an owned synonym handle not yet destroyed, that no other
/// call uses at the same time; it is not used afterwards.
pub(crate) unsafe fn destroy_config_synonym_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned synonym handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards, as
    // `kafka_admin_ConfigEntry_ConfigSynonym_destroy` requires.
    unsafe { kafka_admin_ConfigEntry_ConfigSynonym_destroy(element as *mut kafka_admin_ConfigEntry_ConfigSynonym_t) };
}

/// Reads a borrowed list of `const kafka_admin_ConfigEntry_ConfigSynonym_t *`
/// into clones; null reads as empty.
///
/// # Safety
///
/// `list` must be null or a valid list whose elements are valid
/// config-synonym handles, all kept alive and not mutated for this call.
unsafe fn list_config_synonyms(list: *const kafka_List_t) -> Vec<ConfigSynonym> {
    // SAFETY: Per this function's `# Safety`, `list` is null or a valid list, alive and not mutated
    // for this call, as `list_elements` requires; the borrowed slice is consumed before the call
    // returns.
    unsafe { list_elements(list) }
        .iter()
        .map(|&element| {
            // SAFETY: Per this function's `# Safety`, each element of `list` is a valid
            // config-synonym handle, kept alive for this call, as `config_synonym_ref` requires;
            // the value is cloned at once.
            unsafe { config_synonym_ref(element as *const kafka_admin_ConfigEntry_ConfigSynonym_t) }.clone()
        })
        .collect()
}

/// `ConfigSynonym.name()`: a borrowed string valid as long as the synonym,
/// never passed to `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid config-synonym handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_ConfigSynonym_name(
    self_: *const kafka_admin_ConfigEntry_ConfigSynonym_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid config-synonym handle, kept alive
    // for this call, as `synonym_inner_ref` requires; the returned pointer borrows `name_c`, which
    // lives as long as the handle.
    unsafe { synonym_inner_ref(self_) }.name_c.as_ptr()
}

/// `ConfigSynonym.value()`: a borrowed string valid as long as the synonym,
/// never passed to `kafka_string_destroy`; `NULL` when the configuration is
/// sensitive and the value was withheld.
///
/// # Safety
///
/// `self_` must be a valid config-synonym handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_ConfigSynonym_value(
    self_: *const kafka_admin_ConfigEntry_ConfigSynonym_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid config-synonym handle, kept alive
    // for this call, as `synonym_inner_ref` requires; the returned pointer borrows `value_c`, which
    // lives as long as the handle.
    unsafe { synonym_inner_ref(self_) }
        .value_c
        .as_ref()
        .map_or(std::ptr::null(), |v| v.as_ptr())
}

/// `ConfigSynonym.source()`: a borrowed source singleton.
///
/// # Safety
///
/// `self_` must be a valid config-synonym handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_ConfigSynonym_source(
    self_: *const kafka_admin_ConfigEntry_ConfigSynonym_t,
) -> *const kafka_admin_ConfigEntry_ConfigSource_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid config-synonym handle, kept alive
    // for this call, as `config_synonym_ref` requires.
    config_source_singleton(unsafe { config_synonym_ref(self_) }.source())
}

/// `ConfigSynonym.toString()`, as an owned string freed with
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid config-synonym handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_ConfigSynonym_to_string(
    self_: *const kafka_admin_ConfigEntry_ConfigSynonym_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid config-synonym handle, kept alive
    // for this call, as `config_synonym_ref` requires.
    into_c_string(&unsafe { config_synonym_ref(self_) }.to_string())
}

/// Frees an owned synonym handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned synonym handle not yet destroyed, that no
/// other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_ConfigSynonym_destroy(
    self_: *mut kafka_admin_ConfigEntry_ConfigSynonym_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // synonym handle not yet destroyed, that no other call uses at the same time and that is
        // not used afterwards; every handle is the `Box::into_raw` of a `Box<ConfigSynonymInner>`
        // (`box_config_synonym`), so `from_raw` reclaims it exactly once.
        drop(unsafe { Box::from_raw(self_ as *mut ConfigSynonymInner) });
    }
}

// ---------------------------------------------------------------------------
// ConfigEntryOptions and its builder
// ---------------------------------------------------------------------------

/// Opaque handle to a [`ConfigEntryOptions`]: every parameter of Java's
/// widest `ConfigEntry` constructor, built by
/// [`kafka_admin_ConfigEntryOptionsBuilder_build`].
#[repr(C)]
// the Options struct standing for the constructor overloads with more than three parameters (CLAUDE.md §2)
#[doc(alias = "rust-only")]
pub struct kafka_admin_ConfigEntryOptions_t {
    _private: [u8; 0],
}

/// The options behind a handle.
///
/// # Safety
///
/// `options` must be a valid config-entry-options handle, kept alive for
/// `'a`.
pub(crate) unsafe fn config_entry_options_ref<'a>(
    options: *const kafka_admin_ConfigEntryOptions_t,
) -> &'a ConfigEntryOptions {
    // SAFETY: Per this function's `# Safety`, `options` is a valid config-entry-options handle kept
    // alive for `'a`; every `kafka_admin_ConfigEntryOptions_t` is the `Box::into_raw` of a
    // `Box<ConfigEntryOptions>` (the closure in `kafka_admin_ConfigEntryOptionsBuilder_build`, the
    // only producer), so the shared reference is to a live `ConfigEntryOptions`, which no entry
    // point mutates.
    unsafe { &*(options as *const ConfigEntryOptions) }
}

/// Frees an owned options handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned options handle not yet destroyed, that no
/// other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntryOptions_destroy(self_: *mut kafka_admin_ConfigEntryOptions_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // options handle not yet destroyed, that no other call uses at the same time and that is
        // not used afterwards; every handle is the `Box::into_raw` of a `Box<ConfigEntryOptions>`
        // (`kafka_admin_ConfigEntryOptionsBuilder_build`), so `from_raw` reclaims it exactly once.
        drop(unsafe { Box::from_raw(self_ as *mut ConfigEntryOptions) });
    }
}

/// Opaque handle to a [`ConfigEntryOptionsBuilder`].
#[repr(C)]
// the builder of the Options struct standing for the constructor overloads (CLAUDE.md §2)
#[doc(alias = "rust-only")]
pub struct kafka_admin_ConfigEntryOptionsBuilder_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_ConfigEntryOptionsBuilder_t`] points at: the setter
/// calls made so far. Rust's builder is consumed by `build`, so the handle
/// records the calls and replays them on a fresh builder at each `build`,
/// which leaves the handle reusable the way Java's reference stays usable.
#[derive(Default)]
struct ConfigEntryOptionsBuilderInner {
    name: Option<String>,
    value: Option<Option<String>>,
    source: Option<ConfigSource>,
    is_sensitive: Option<bool>,
    is_read_only: Option<bool>,
    synonyms: Option<Vec<ConfigSynonym>>,
    config_type: Option<Option<ConfigType>>,
    documentation: Option<Option<String>>,
}

impl ConfigEntryOptionsBuilderInner {
    /// Replays the recorded setter calls on a fresh [`ConfigEntryOptionsBuilder`].
    fn builder(&self) -> ConfigEntryOptionsBuilder {
        let mut builder = ConfigEntryOptionsBuilder::new();
        if let Some(name) = &self.name {
            builder = builder.set_name(name.clone());
        }
        if let Some(value) = &self.value {
            builder = builder.set_value(value.clone());
        }
        if let Some(source) = self.source {
            builder = builder.set_source(source);
        }
        if let Some(is_sensitive) = self.is_sensitive {
            builder = builder.set_is_sensitive(is_sensitive);
        }
        if let Some(is_read_only) = self.is_read_only {
            builder = builder.set_is_read_only(is_read_only);
        }
        if let Some(synonyms) = &self.synonyms {
            builder = builder.set_synonyms(synonyms.clone());
        }
        if let Some(config_type) = self.config_type {
            builder = builder.set_config_type(config_type);
        }
        if let Some(documentation) = &self.documentation {
            builder = builder.set_documentation(documentation.clone());
        }
        builder
    }
}

/// What a builder handle points at, for a setter or `build`.
///
/// # Safety
///
/// `builder` must be a valid builder handle, kept alive for `'a`, that no
/// other call uses during `'a`.
unsafe fn builder_mut<'a>(
    builder: *mut kafka_admin_ConfigEntryOptionsBuilder_t,
) -> &'a mut ConfigEntryOptionsBuilderInner {
    // SAFETY: Per this function's `# Safety`, `builder` is a valid builder handle kept alive for
    // `'a` that no other call uses during `'a`; every `kafka_admin_ConfigEntryOptionsBuilder_t` is
    // the `Box::into_raw` of a `Box<ConfigEntryOptionsBuilderInner>`
    // (`kafka_admin_ConfigEntryOptionsBuilder_new`, the only producer), so the exclusive reference
    // is to a live `ConfigEntryOptionsBuilderInner` nothing else reads or writes while it is held.
    unsafe { &mut *(builder as *mut ConfigEntryOptionsBuilderInner) }
}

/// `ConfigEntryOptionsBuilder::new()`: a builder with every mandatory
/// parameter unset and every other parameter at the value Java's narrow
/// constructor passes. Owned, freed with
/// [`kafka_admin_ConfigEntryOptionsBuilder_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ConfigEntryOptionsBuilder_new() -> *mut kafka_admin_ConfigEntryOptionsBuilder_t {
    Box::into_raw(Box::new(ConfigEntryOptionsBuilderInner::default())) as *mut kafka_admin_ConfigEntryOptionsBuilder_t
}

/// `set_name(name)`: Java's `name`, a mandatory parameter; copied.
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the same
/// time, and `name` a valid NUL-terminated string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntryOptionsBuilder_set_name(
    self_: *mut kafka_admin_ConfigEntryOptionsBuilder_t,
    name: *const c_char,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle that no other call
    // uses at the same time, kept alive for this call, as `builder_mut` requires, and `name` a
    // valid NUL-terminated string, as `c_str_to_string` requires.
    unsafe { builder_mut(self_) }.name = Some(unsafe { c_str_to_string(name) });
}

/// `set_value(value)`: Java's `value`, a mandatory parameter that may be
/// `NULL` (Java's null value); copied.
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the same
/// time, and `value` null or a valid NUL-terminated string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntryOptionsBuilder_set_value(
    self_: *mut kafka_admin_ConfigEntryOptionsBuilder_t,
    value: *const c_char,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle that no other call
    // uses at the same time, kept alive for this call, as `builder_mut` requires, and `value` null
    // or a valid NUL-terminated string, as `c_str_to_option` requires.
    unsafe { builder_mut(self_) }.value = Some(unsafe { c_str_to_option(value) });
}

/// `set_source(source)`: Java's `source`.
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the same
/// time, and `source` a source singleton.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntryOptionsBuilder_set_source(
    self_: *mut kafka_admin_ConfigEntryOptionsBuilder_t,
    source: *const kafka_admin_ConfigEntry_ConfigSource_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle that no other call
    // uses at the same time, kept alive for this call, as `builder_mut` requires, and `source` a
    // source singleton, which only this module returns, as `config_source_value_of` requires.
    unsafe { builder_mut(self_) }.source = Some(unsafe { config_source_value_of(source) });
}

/// `set_is_sensitive(is_sensitive)`: Java's `isSensitive` (0 or 1).
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the same
/// time.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntryOptionsBuilder_set_is_sensitive(
    self_: *mut kafka_admin_ConfigEntryOptionsBuilder_t,
    is_sensitive: i8,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle that no other call
    // uses at the same time, kept alive for this call, as `builder_mut` requires.
    unsafe { builder_mut(self_) }.is_sensitive = Some(is_sensitive != 0);
}

/// `set_is_read_only(is_read_only)`: Java's `isReadOnly` (0 or 1).
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the same
/// time.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntryOptionsBuilder_set_is_read_only(
    self_: *mut kafka_admin_ConfigEntryOptionsBuilder_t,
    is_read_only: i8,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle that no other call
    // uses at the same time, kept alive for this call, as `builder_mut` requires.
    unsafe { builder_mut(self_) }.is_read_only = Some(is_read_only != 0);
}

/// `set_synonyms(synonyms)`: Java's `synonyms`, a borrowed list of
/// `const kafka_admin_ConfigEntry_ConfigSynonym_t *` whose elements are
/// copied; `NULL` reads as an empty list.
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the same
/// time, and `synonyms` null or a valid list whose elements are valid
/// config-synonym handles, all kept alive and not mutated for this call.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntryOptionsBuilder_set_synonyms(
    self_: *mut kafka_admin_ConfigEntryOptionsBuilder_t,
    synonyms: *const kafka_List_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle that no other call
    // uses at the same time, kept alive for this call, as `builder_mut` requires, and `synonyms`
    // null or a valid list whose elements are valid config-synonym handles, all kept alive and not
    // mutated for this call, as `list_config_synonyms` requires; the list is read before the
    // exclusive borrow is taken.
    unsafe { builder_mut(self_) }.synonyms = Some(unsafe { list_config_synonyms(synonyms) });
}

/// `set_config_type(config_type)`: Java's `type`, `NULL` for Java's `null`
/// (the type a `createTopics` entry carries).
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the same
/// time, and `config_type` null or a type singleton.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntryOptionsBuilder_set_config_type(
    self_: *mut kafka_admin_ConfigEntryOptionsBuilder_t,
    config_type: *const kafka_admin_ConfigEntry_ConfigType_t,
) {
    let config_type = if config_type.is_null() {
        None
    } else {
        // SAFETY: `config_type` is non-null (checked above) and, per this function's `# Safety`, a
        // type singleton, which only this module returns, as `config_type_value_of` requires.
        Some(unsafe { config_type_value_of(config_type) })
    };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle that no other call
    // uses at the same time, kept alive for this call, as `builder_mut` requires.
    unsafe { builder_mut(self_) }.config_type = Some(config_type);
}

/// `set_documentation(documentation)`: Java's `documentation`, `NULL` for
/// none; copied.
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the same
/// time, and `documentation` null or a valid NUL-terminated string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntryOptionsBuilder_set_documentation(
    self_: *mut kafka_admin_ConfigEntryOptionsBuilder_t,
    documentation: *const c_char,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle that no other call
    // uses at the same time, kept alive for this call, as `builder_mut` requires, and
    // `documentation` null or a valid NUL-terminated string, as `c_str_to_option` requires.
    unsafe { builder_mut(self_) }.documentation = Some(unsafe { c_str_to_option(documentation) });
}

/// `build()`: returns the owned options (freed with
/// [`kafka_admin_ConfigEntryOptions_destroy`]), or stores the owned
/// `IllegalArgumentException` translation in `*out_err` and returns `NULL`,
/// naming the first mandatory parameter (`name`, `value`) that was not set. The
/// builder stays usable.
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the same
/// time, and `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntryOptionsBuilder_build(
    self_: *mut kafka_admin_ConfigEntryOptionsBuilder_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_admin_ConfigEntryOptions_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle that no other call
    // uses at the same time, kept alive for this call, as `builder_mut` requires; the borrow ends
    // with this statement.
    let result = unsafe { builder_mut(self_) }.builder().build();
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid slot, as `deliver_value` requires.
    unsafe {
        deliver_value(result, out_err, |options| {
            Box::into_raw(Box::new(options)) as *mut kafka_admin_ConfigEntryOptions_t
        })
    }
}

/// Frees an owned builder handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned builder handle not yet destroyed, that no
/// other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntryOptionsBuilder_destroy(
    self_: *mut kafka_admin_ConfigEntryOptionsBuilder_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // builder handle not yet destroyed, that no other call uses at the same time and that is
        // not used afterwards; every handle is the `Box::into_raw` of a
        // `Box<ConfigEntryOptionsBuilderInner>` (`kafka_admin_ConfigEntryOptionsBuilder_new`), so
        // `from_raw` reclaims it exactly once.
        drop(unsafe { Box::from_raw(self_ as *mut ConfigEntryOptionsBuilderInner) });
    }
}

// ---------------------------------------------------------------------------
// ConfigEntry
// ---------------------------------------------------------------------------

/// Opaque handle to a [`ConfigEntry`].
#[repr(C)]
pub struct kafka_admin_ConfigEntry_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_ConfigEntry_t`] points at: the entry plus the
/// NUL-terminated copies its string getters borrow out.
pub(crate) struct ConfigEntryInner {
    entry: ConfigEntry,
    name_c: CString,
    value_c: Option<CString>,
    documentation_c: Option<CString>,
}

impl ConfigEntryInner {
    pub(crate) fn new(entry: ConfigEntry) -> Self {
        let name_c = owned_c_string(entry.name());
        let value_c = entry.value().map(owned_c_string);
        let documentation_c = entry.documentation().map(owned_c_string);
        Self { entry, name_c, value_c, documentation_c }
    }

    /// A borrowed handle on this entry, valid as long as `self` stays where
    /// it is: an `AlterConfigOp` or `Config` handle keeps its entries in
    /// place and hands this out.
    pub(crate) fn as_ptr(&self) -> *const kafka_admin_ConfigEntry_t {
        self as *const ConfigEntryInner as *const kafka_admin_ConfigEntry_t
    }
}

/// What an entry handle points at.
///
/// # Safety
///
/// `entry` must be a valid config-entry handle, owned or borrowed, kept
/// alive for `'a`.
unsafe fn entry_inner_ref<'a>(entry: *const kafka_admin_ConfigEntry_t) -> &'a ConfigEntryInner {
    // SAFETY: Per this function's `# Safety`, `entry` is a valid config-entry handle kept alive for
    // `'a`; every `kafka_admin_ConfigEntry_t` points at a live `ConfigEntryInner`: an owned handle
    // is the `Box::into_raw` of a `Box<ConfigEntryInner>` (`box_config_entry`), and a borrowed one
    // is `ConfigEntryInner::as_ptr` of an entry an `AlterConfigOp` or `Config` handle keeps in
    // place until it is destroyed. No entry point mutates a `ConfigEntryInner`, so the shared
    // reference is sound.
    unsafe { &*(entry as *const ConfigEntryInner) }
}

/// The entry behind a handle.
///
/// # Safety
///
/// `entry` must be a valid config-entry handle, owned or borrowed, kept
/// alive for `'a`.
pub(crate) unsafe fn config_entry_ref<'a>(entry: *const kafka_admin_ConfigEntry_t) -> &'a ConfigEntry {
    // SAFETY: Per this function's `# Safety`, `entry` is a valid config-entry handle kept alive for
    // `'a`, as `entry_inner_ref` requires.
    &unsafe { entry_inner_ref(entry) }.entry
}

/// Hands `entry` to C as an owned handle, freed with
/// [`kafka_admin_ConfigEntry_destroy`].
pub(crate) fn box_config_entry(entry: ConfigEntry) -> *mut kafka_admin_ConfigEntry_t {
    Box::into_raw(Box::new(ConfigEntryInner::new(entry))) as *mut kafka_admin_ConfigEntry_t
}

/// Frees a `kafka_admin_ConfigEntry_t *` element of an owned container.
///
/// # Safety
///
/// `element` must be an owned config-entry handle not yet destroyed, that no
/// other call uses at the same time; it is not used afterwards.
pub(crate) unsafe fn destroy_config_entry_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned config-entry handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards, as
    // `kafka_admin_ConfigEntry_destroy` requires.
    unsafe { kafka_admin_ConfigEntry_destroy(element as *mut kafka_admin_ConfigEntry_t) };
}

/// `new ConfigEntry(String name, String value)`: `value` may be `NULL`
/// (Java's null); both are copied. Owned, freed with
/// [`kafka_admin_ConfigEntry_destroy`].
///
/// # Safety
///
/// `name` must be a valid NUL-terminated string and `value` null or one.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_new(
    name: *const c_char,
    value: *const c_char,
) -> *mut kafka_admin_ConfigEntry_t {
    // SAFETY: Per this function's `# Safety`, `name` is a valid NUL-terminated string, as
    // `c_str_to_string` requires, and `value` null or a valid NUL-terminated string, as
    // `c_str_to_option` requires.
    box_config_entry(ConfigEntry::new(unsafe { c_str_to_string(name) }, unsafe {
        c_str_to_option(value)
    }))
}

/// `new ConfigEntry(String name, String value, ConfigSource source, boolean
/// isSensitive, boolean isReadOnly, List<ConfigSynonym> synonyms, ConfigType
/// type, String documentation)`, Java's widest constructor, taking its
/// parameters as the options built by
/// [`kafka_admin_ConfigEntryOptionsBuilder_build`]; the options are copied,
/// the caller keeps its handle. Owned, freed with
/// [`kafka_admin_ConfigEntry_destroy`].
///
/// # Safety
///
/// `options` must be a valid config-entry-options handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_with_options(
    options: *const kafka_admin_ConfigEntryOptions_t,
) -> *mut kafka_admin_ConfigEntry_t {
    // SAFETY: Per this function's `# Safety`, `options` is a valid config-entry-options handle,
    // kept alive for this call, as `config_entry_options_ref` requires; the options are cloned at
    // once.
    box_config_entry(ConfigEntry::with_options(unsafe { config_entry_options_ref(options) }.clone()))
}

/// `name()`: a borrowed string valid as long as the entry, never passed to
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid config-entry handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_name(self_: *const kafka_admin_ConfigEntry_t) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid config-entry handle, kept alive
    // for this call, as `entry_inner_ref` requires; the returned pointer borrows `name_c`, which
    // lives as long as the handle.
    unsafe { entry_inner_ref(self_) }.name_c.as_ptr()
}

/// `value()`: a borrowed string valid as long as the entry, never passed to
/// `kafka_string_destroy`; `NULL` when the config is unset or sensitive.
///
/// # Safety
///
/// `self_` must be a valid config-entry handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_value(self_: *const kafka_admin_ConfigEntry_t) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid config-entry handle, kept alive
    // for this call, as `entry_inner_ref` requires; the returned pointer borrows `value_c`, which
    // lives as long as the handle.
    unsafe { entry_inner_ref(self_) }
        .value_c
        .as_ref()
        .map_or(std::ptr::null(), |v| v.as_ptr())
}

/// `source()`: a borrowed source singleton.
///
/// # Safety
///
/// `self_` must be a valid config-entry handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_source(
    self_: *const kafka_admin_ConfigEntry_t,
) -> *const kafka_admin_ConfigEntry_ConfigSource_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid config-entry handle, kept alive
    // for this call, as `config_entry_ref` requires.
    config_source_singleton(unsafe { config_entry_ref(self_) }.source())
}

/// `isDefault()`: whether the source is `DEFAULT_CONFIG`.
///
/// # Safety
///
/// `self_` must be a valid config-entry handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_is_default(self_: *const kafka_admin_ConfigEntry_t) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid config-entry handle, kept alive
    // for this call, as `config_entry_ref` requires.
    i8::from(unsafe { config_entry_ref(self_) }.is_default())
}

/// `isSensitive()`.
///
/// # Safety
///
/// `self_` must be a valid config-entry handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_is_sensitive(self_: *const kafka_admin_ConfigEntry_t) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid config-entry handle, kept alive
    // for this call, as `config_entry_ref` requires.
    i8::from(unsafe { config_entry_ref(self_) }.is_sensitive())
}

/// `isReadOnly()`.
///
/// # Safety
///
/// `self_` must be a valid config-entry handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_is_read_only(self_: *const kafka_admin_ConfigEntry_t) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid config-entry handle, kept alive
    // for this call, as `config_entry_ref` requires.
    i8::from(unsafe { config_entry_ref(self_) }.is_read_only())
}

/// `synonyms()`: an owned list of owned
/// `kafka_admin_ConfigEntry_ConfigSynonym_t *` copies, in order of
/// precedence, freed together with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid config-entry handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_synonyms(
    self_: *const kafka_admin_ConfigEntry_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid config-entry handle, kept alive
    // for this call, as `config_entry_ref` requires.
    let elements = unsafe { config_entry_ref(self_) }
        .synonyms()
        .iter()
        .map(|synonym| box_config_synonym(synonym.clone()) as *mut c_void)
        .collect();
    box_list(elements, Some(destroy_config_synonym_element))
}

/// `type()`: a borrowed type singleton, or `NULL` for Java's `null` type,
/// which the entries of a `createTopics` result carry
/// (`kafka_admin_CreateTopicsResult_config`).
///
/// # Safety
///
/// `self_` must be a valid config-entry handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_type(
    self_: *const kafka_admin_ConfigEntry_t,
) -> *const kafka_admin_ConfigEntry_ConfigType_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid config-entry handle, kept alive
    // for this call, as `config_entry_ref` requires.
    unsafe { config_entry_ref(self_) }
        .r#type()
        .map_or(std::ptr::null(), config_type_singleton)
}

/// `documentation()`: a borrowed string valid as long as the entry, never
/// passed to `kafka_string_destroy`; `NULL` when there is none.
///
/// # Safety
///
/// `self_` must be a valid config-entry handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_documentation(
    self_: *const kafka_admin_ConfigEntry_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid config-entry handle, kept alive
    // for this call, as `entry_inner_ref` requires; the returned pointer borrows `documentation_c`,
    // which lives as long as the handle.
    unsafe { entry_inner_ref(self_) }
        .documentation_c
        .as_ref()
        .map_or(std::ptr::null(), |d| d.as_ptr())
}

/// `toString()`, redacting a sensitive value, as an owned string freed with
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid config-entry handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_to_string(self_: *const kafka_admin_ConfigEntry_t) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid config-entry handle, kept alive
    // for this call, as `config_entry_ref` requires.
    into_c_string(&unsafe { config_entry_ref(self_) }.to_string())
}

/// Frees an owned entry handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned entry handle not yet destroyed, that no
/// other call uses at the same time; it is not used afterwards. A borrowed
/// entry handle (from `kafka_admin_Config_get` or
/// `kafka_admin_AlterConfigOp_config_entry`) is never passed here.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ConfigEntry_destroy(self_: *mut kafka_admin_ConfigEntry_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // entry handle not yet destroyed, that no other call uses at the same time and that is not
        // used afterwards, and not a borrowed view; every owned handle is the `Box::into_raw` of a
        // `Box<ConfigEntryInner>` (`box_config_entry`), so `from_raw` reclaims it exactly once.
        drop(unsafe { Box::from_raw(self_ as *mut ConfigEntryInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::ffi::common::{error_ref, kafka_common_Error_destroy};
    use crate::ffi::util::{kafka_List_destroy, kafka_List_get, kafka_List_new, kafka_List_size, kafka_string_destroy};

    #[test]
    fn source_and_type_singletons_match_their_enumerators() {
        // SAFETY: Test code: each `handle` is the singleton `config_source_singleton` or
        // `config_type_singleton` returned, a pointer into the static `SOURCES` or `TYPES`, so
        // it is a singleton of this module for the value, `__enum` and `_to_string` functions
        // read here; each string `_to_string` returns is owned and freed once, after it is read.
        unsafe {
            for (index, &source) in SOURCES.iter().enumerate() {
                let handle = config_source_singleton(source);
                assert_eq!(config_source_value_of(handle), source);
                assert_eq!(kafka_admin_ConfigEntry_ConfigSource__enum(handle) as usize, index);
                let name = kafka_admin_ConfigEntry_ConfigSource_to_string(handle);
                assert_eq!(CStr::from_ptr(name).to_str().unwrap(), source.to_string());
                kafka_string_destroy(name);
            }
            for (index, &config_type) in TYPES.iter().enumerate() {
                let handle = config_type_singleton(config_type);
                assert_eq!(config_type_value_of(handle), config_type);
                assert_eq!(kafka_admin_ConfigEntry_ConfigType__enum(handle) as usize, index);
                let name = kafka_admin_ConfigEntry_ConfigType_to_string(handle);
                assert_eq!(CStr::from_ptr(name).to_str().unwrap(), config_type.to_string());
                kafka_string_destroy(name);
            }
        }
        assert_eq!(
            kafka_admin_ConfigEntry_ConfigSource_default_config(),
            config_source_singleton(ConfigSource::DefaultConfig)
        );
        assert_eq!(
            kafka_admin_ConfigEntry_ConfigType_password(),
            config_type_singleton(ConfigType::Password)
        );
        assert_eq!(ConfigSource::DefaultConfig.to_string(), "DEFAULT_CONFIG");
        assert_eq!(ConfigType::Password.to_string(), "PASSWORD");
    }

    #[test]
    fn narrow_constructor_and_getters() {
        // SAFETY: Test code: `entry` is the owned handle `kafka_admin_ConfigEntry_new` returned,
        // used only on this thread and destroyed once, last; the name it lends is read while
        // `entry` is alive; `synonyms` is the owned list `kafka_admin_ConfigEntry_synonyms`
        // returned, destroyed once; destroying null is a no-op.
        unsafe {
            let entry = kafka_admin_ConfigEntry_new(c"retention.ms".as_ptr(), ptr::null());
            assert_eq!(*config_entry_ref(entry), ConfigEntry::new("retention.ms".to_string(), None));
            assert_eq!(
                CStr::from_ptr(kafka_admin_ConfigEntry_name(entry)).to_str().unwrap(),
                "retention.ms"
            );
            assert!(kafka_admin_ConfigEntry_value(entry).is_null());
            assert!(kafka_admin_ConfigEntry_documentation(entry).is_null());
            assert_eq!(
                kafka_admin_ConfigEntry_source(entry),
                kafka_admin_ConfigEntry_ConfigSource_unknown()
            );
            assert_eq!(
                kafka_admin_ConfigEntry_type(entry),
                kafka_admin_ConfigEntry_ConfigType_unknown()
            );
            assert_eq!(kafka_admin_ConfigEntry_is_default(entry), 0);
            assert_eq!(kafka_admin_ConfigEntry_is_sensitive(entry), 0);
            assert_eq!(kafka_admin_ConfigEntry_is_read_only(entry), 0);
            let synonyms = kafka_admin_ConfigEntry_synonyms(entry);
            assert_eq!(kafka_List_size(synonyms), 0);
            kafka_List_destroy(synonyms);
            kafka_admin_ConfigEntry_destroy(entry);
            kafka_admin_ConfigEntry_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn builder_validates_then_builds_the_wide_constructor() {
        // SAFETY: Test code: `builder` is the owned handle
        // `kafka_admin_ConfigEntryOptionsBuilder_new` returned, used only on this thread and
        // destroyed once, after its last build; `error` and `err` are valid slots pointing at
        // null; `error` then holds the owned error the first build stored, read while alive and
        // destroyed once, and `err` stays null; `options` and `again` are the owned options the
        // later builds returned, each destroyed once after its last read. `synonyms`
        // owns nothing and holds the owned `synonym_handle`; both stay alive until
        // `kafka_admin_ConfigEntryOptionsBuilder_set_synonyms` copied them, then each is destroyed
        // once. `entry` is the owned handle `kafka_admin_ConfigEntry_with_options` returned,
        // destroyed once, last, and the strings it lends are read while it is alive; the second
        // `synonyms` is the owned list `kafka_admin_ConfigEntry_synonyms` returned, whose element
        // `first` and the strings it lends are read while the list is alive, then the list is
        // destroyed once; each `s` is an owned string, read while alive and freed once with
        // `kafka_string_destroy`; destroying null is a no-op.
        unsafe {
            let builder = kafka_admin_ConfigEntryOptionsBuilder_new();
            let mut error = ptr::null_mut();
            let options = kafka_admin_ConfigEntryOptionsBuilder_build(builder, &mut error);
            assert!(!error.is_null());
            assert!(options.is_null());
            assert_eq!(
                error_ref(error).error.message(),
                "ConfigEntryOptionsBuilder::build: mandatory parameter `name` was not set"
            );
            kafka_common_Error_destroy(error);

            let synonym = ConfigSynonym::new("log.retention.ms".to_string(), None, ConfigSource::StaticBrokerConfig);
            let synonym_handle = box_config_synonym(synonym.clone());
            let synonyms = kafka_List_new();
            crate::ffi::util::kafka_List_add(synonyms, synonym_handle as *mut c_void);

            kafka_admin_ConfigEntryOptionsBuilder_set_name(builder, c"retention.ms".as_ptr());
            kafka_admin_ConfigEntryOptionsBuilder_set_value(builder, c"secret".as_ptr());
            kafka_admin_ConfigEntryOptionsBuilder_set_source(
                builder,
                kafka_admin_ConfigEntry_ConfigSource_default_config(),
            );
            kafka_admin_ConfigEntryOptionsBuilder_set_is_sensitive(builder, 1);
            kafka_admin_ConfigEntryOptionsBuilder_set_is_read_only(builder, 1);
            kafka_admin_ConfigEntryOptionsBuilder_set_synonyms(builder, synonyms);
            kafka_admin_ConfigEntryOptionsBuilder_set_config_type(builder, kafka_admin_ConfigEntry_ConfigType_long());
            kafka_admin_ConfigEntryOptionsBuilder_set_documentation(builder, c"docs".as_ptr());
            kafka_List_destroy(synonyms);
            kafka_admin_ConfigEntry_ConfigSynonym_destroy(synonym_handle);

            let mut err = ptr::null_mut();
            let options = kafka_admin_ConfigEntryOptionsBuilder_build(builder, &mut err);
            assert!(err.is_null());
            let expected = ConfigEntryOptionsBuilder::new()
                .set_name("retention.ms".to_string())
                .set_value(Some("secret".to_string()))
                .set_source(ConfigSource::DefaultConfig)
                .set_is_sensitive(true)
                .set_is_read_only(true)
                .set_synonyms(vec![synonym.clone()])
                .set_config_type(Some(ConfigType::Long))
                .set_documentation(Some("docs".to_string()))
                .build()
                .unwrap();
            assert_eq!(*config_entry_options_ref(options), expected);
            // The builder is reusable after `build`.
            let again = kafka_admin_ConfigEntryOptionsBuilder_build(builder, &mut err);
            assert!(err.is_null());
            assert_eq!(*config_entry_options_ref(again), expected);
            kafka_admin_ConfigEntryOptions_destroy(again);
            kafka_admin_ConfigEntryOptionsBuilder_destroy(builder);

            let entry = kafka_admin_ConfigEntry_with_options(options);
            kafka_admin_ConfigEntryOptions_destroy(options);
            let rust_entry = ConfigEntry::with_options(expected);
            assert_eq!(*config_entry_ref(entry), rust_entry);
            assert_eq!(CStr::from_ptr(kafka_admin_ConfigEntry_value(entry)).to_str().unwrap(), "secret");
            assert_eq!(
                CStr::from_ptr(kafka_admin_ConfigEntry_documentation(entry)).to_str().unwrap(),
                "docs"
            );
            assert_eq!(kafka_admin_ConfigEntry_is_default(entry), 1);
            assert_eq!(kafka_admin_ConfigEntry_is_sensitive(entry), 1);
            assert_eq!(kafka_admin_ConfigEntry_is_read_only(entry), 1);
            assert_eq!(kafka_admin_ConfigEntry_type(entry), kafka_admin_ConfigEntry_ConfigType_long());
            let s = kafka_admin_ConfigEntry_to_string(entry);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), rust_entry.to_string());
            kafka_string_destroy(s);

            let synonyms = kafka_admin_ConfigEntry_synonyms(entry);
            assert_eq!(kafka_List_size(synonyms), 1);
            let first = kafka_List_get(synonyms, 0) as *const kafka_admin_ConfigEntry_ConfigSynonym_t;
            assert_eq!(*config_synonym_ref(first), synonym);
            assert_eq!(
                CStr::from_ptr(kafka_admin_ConfigEntry_ConfigSynonym_name(first))
                    .to_str()
                    .unwrap(),
                "log.retention.ms"
            );
            assert!(kafka_admin_ConfigEntry_ConfigSynonym_value(first).is_null());
            assert_eq!(
                kafka_admin_ConfigEntry_ConfigSynonym_source(first),
                kafka_admin_ConfigEntry_ConfigSource_static_broker_config()
            );
            let s = kafka_admin_ConfigEntry_ConfigSynonym_to_string(first);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), synonym.to_string());
            kafka_string_destroy(s);
            kafka_List_destroy(synonyms);
            kafka_admin_ConfigEntry_destroy(entry);
            kafka_admin_ConfigEntry_ConfigSynonym_destroy(ptr::null_mut());
            kafka_admin_ConfigEntryOptions_destroy(ptr::null_mut());
            kafka_admin_ConfigEntryOptionsBuilder_destroy(ptr::null_mut());
        }
    }

    /// New test, no Java original: Java's `type()` is `null` for the entries
    /// of a `createTopics` result (`KafkaAdminClient.java:1992-2002`). A NULL
    /// type set through the builder reaches the entry as `None`, after an
    /// earlier non-null one, and the getter hands it back as NULL.
    #[test]
    fn config_entry_type_is_null_for_an_entry_without_type() {
        // SAFETY: Test code: `builder` is the owned handle
        // `kafka_admin_ConfigEntryOptionsBuilder_new` returned, used only on this thread and
        // destroyed once, after its build; `err` is a valid slot pointing at null and stays null;
        // `options` is the owned options the build returned, destroyed once after
        // `kafka_admin_ConfigEntry_with_options` copied it; `entry` is the owned handle that call
        // returned, destroyed once, last; `s` is an owned string, read while alive and freed once.
        unsafe {
            let builder = kafka_admin_ConfigEntryOptionsBuilder_new();
            kafka_admin_ConfigEntryOptionsBuilder_set_name(builder, c"compression.type".as_ptr());
            kafka_admin_ConfigEntryOptionsBuilder_set_value(builder, c"producer".as_ptr());
            kafka_admin_ConfigEntryOptionsBuilder_set_config_type(builder, kafka_admin_ConfigEntry_ConfigType_long());
            kafka_admin_ConfigEntryOptionsBuilder_set_config_type(builder, ptr::null());
            let mut err = ptr::null_mut();
            let options = kafka_admin_ConfigEntryOptionsBuilder_build(builder, &mut err);
            assert!(err.is_null());
            kafka_admin_ConfigEntryOptionsBuilder_destroy(builder);
            assert_eq!(config_entry_options_ref(options).config_type, None);
            let entry = kafka_admin_ConfigEntry_with_options(options);
            kafka_admin_ConfigEntryOptions_destroy(options);
            assert_eq!(config_entry_ref(entry).r#type(), None);
            assert!(kafka_admin_ConfigEntry_type(entry).is_null());
            let s = kafka_admin_ConfigEntry_to_string(entry);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                "ConfigEntry(name=compression.type, value=producer, source=UNKNOWN, isSensitive=false, \
                 isReadOnly=false, synonyms=[], type=null, documentation=null)"
            );
            kafka_string_destroy(s);
            kafka_admin_ConfigEntry_destroy(entry);
        }
    }
}
