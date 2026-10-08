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

//! `kafka_admin_Config_t`: `org.apache.kafka.clients.admin.Config`
//! (CLAUDE.md §4). `get` returns a borrowed entry handle valid as long as
//! the config; `entries` returns owned copies.

use std::collections::BTreeMap;
use std::ffi::{c_char, c_void};

use crate::admin::Config;
use crate::ffi::admin::config_entry::{
    ConfigEntryInner, box_config_entry, config_entry_ref, destroy_config_entry_element, kafka_admin_ConfigEntry_t,
};
use crate::ffi::util::{box_list, c_str_to_string, into_c_string, kafka_List_t, list_elements};

/// Opaque handle to a [`Config`].
#[repr(C)]
pub struct kafka_admin_Config_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_Config_t`] points at: the config plus one handle per
/// entry, keyed by name, which `get` borrows out.
pub(crate) struct ConfigInner {
    config: Config,
    entries: BTreeMap<String, ConfigEntryInner>,
}

impl ConfigInner {
    pub(crate) fn new(config: Config) -> Self {
        let entries = config
            .entries()
            .map(|entry| (entry.name().to_string(), ConfigEntryInner::new(entry.clone())))
            .collect();
        Self { config, entries }
    }
}

unsafe fn inner_ref<'a>(config: *const kafka_admin_Config_t) -> &'a ConfigInner {
    unsafe { &*(config as *const ConfigInner) }
}

/// The config behind a handle.
///
/// # Safety
///
/// `config` must be a valid config handle.
pub(crate) unsafe fn config_ref<'a>(config: *const kafka_admin_Config_t) -> &'a Config {
    &unsafe { inner_ref(config) }.config
}

/// Hands `config` to C as an owned handle, freed with
/// [`kafka_admin_Config_destroy`].
pub(crate) fn box_config(config: Config) -> *mut kafka_admin_Config_t {
    Box::into_raw(Box::new(ConfigInner::new(config))) as *mut kafka_admin_Config_t
}

/// Frees a `kafka_admin_Config_t *` element of an owned container.
///
/// # Safety
///
/// `element` must be an owned config handle not yet destroyed.
pub(crate) unsafe fn destroy_config_element(element: *mut c_void) {
    unsafe { kafka_admin_Config_destroy(element as *mut kafka_admin_Config_t) };
}

/// `new Config(Collection<ConfigEntry> entries)`: a borrowed list of
/// `const kafka_admin_ConfigEntry_t *` whose elements are copied (`NULL`
/// reads as empty); a later entry with the same name replaces an earlier
/// one, as in Java. Owned, freed with [`kafka_admin_Config_destroy`].
///
/// # Safety
///
/// `entries` must be null or a valid list of config-entry handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Config_new(entries: *const kafka_List_t) -> *mut kafka_admin_Config_t {
    let entries = unsafe { list_elements(entries) }
        .iter()
        .map(|&element| unsafe { config_entry_ref(element as *const kafka_admin_ConfigEntry_t) }.clone());
    box_config(Config::new(entries))
}

/// `entries()`: an owned list of owned `kafka_admin_ConfigEntry_t *` copies,
/// sorted by name (Java's collection order is unspecified), freed together
/// with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid config handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Config_entries(self_: *const kafka_admin_Config_t) -> *mut kafka_List_t {
    let mut entries: Vec<_> = unsafe { config_ref(self_) }.entries().collect();
    entries.sort_unstable_by(|a, b| a.name().cmp(b.name()));
    let elements = entries
        .into_iter()
        .map(|entry| box_config_entry(entry.clone()) as *mut c_void)
        .collect();
    box_list(elements, Some(destroy_config_entry_element))
}

/// `get(String name)`: a borrowed handle valid as long as the config, never
/// passed to `kafka_admin_ConfigEntry_destroy`; `NULL` when there is no
/// entry with that name (Java returns null).
///
/// # Safety
///
/// `self_` must be a valid config handle and `name` a valid NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Config_get(
    self_: *const kafka_admin_Config_t,
    name: *const c_char,
) -> *const kafka_admin_ConfigEntry_t {
    let name = unsafe { c_str_to_string(name) };
    unsafe { inner_ref(self_) }
        .entries
        .get(&name)
        .map_or(std::ptr::null(), ConfigEntryInner::as_ptr)
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid config handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Config_to_string(self_: *const kafka_admin_Config_t) -> *mut c_char {
    into_c_string(&unsafe { config_ref(self_) }.to_string())
}

/// Frees an owned config handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned config handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_Config_destroy(self_: *mut kafka_admin_Config_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ConfigInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::admin::ConfigEntry;
    use crate::ffi::admin::config_entry::{kafka_admin_ConfigEntry_destroy, kafka_admin_ConfigEntry_name};
    use crate::ffi::util::{
        kafka_List_add, kafka_List_destroy, kafka_List_get, kafka_List_new, kafka_List_size, kafka_string_destroy,
    };

    #[test]
    fn config_copies_entries_sorts_them_and_borrows_them_out() {
        let b = ConfigEntry::new("b".to_string(), Some("2".to_string()));
        let a = ConfigEntry::new("a".to_string(), None);
        unsafe {
            let list = kafka_List_new();
            let b_handle = box_config_entry(b.clone());
            let a_handle = box_config_entry(a.clone());
            kafka_List_add(list, b_handle as *mut c_void);
            kafka_List_add(list, a_handle as *mut c_void);
            let config = kafka_admin_Config_new(list);
            kafka_admin_ConfigEntry_destroy(b_handle);
            kafka_admin_ConfigEntry_destroy(a_handle);
            kafka_List_destroy(list);

            let expected = Config::new([b.clone(), a.clone()]);
            assert_eq!(*config_ref(config), expected);
            assert_eq!(*config_entry_ref(kafka_admin_Config_get(config, c"b".as_ptr())), b);
            assert!(kafka_admin_Config_get(config, c"missing".as_ptr()).is_null());

            let entries = kafka_admin_Config_entries(config);
            assert_eq!(kafka_List_size(entries), 2);
            let first = kafka_List_get(entries, 0) as *const kafka_admin_ConfigEntry_t;
            assert_eq!(CStr::from_ptr(kafka_admin_ConfigEntry_name(first)).to_str().unwrap(), "a");
            assert_eq!(
                *config_entry_ref(kafka_List_get(entries, 1) as *const kafka_admin_ConfigEntry_t),
                b
            );
            kafka_List_destroy(entries);

            // `Config.toString()` renders a `HashMap` in Java too, so the entry
            // order is unspecified: check the pieces, not the whole string.
            let s = kafka_admin_Config_to_string(config);
            let rendered = CStr::from_ptr(s).to_str().unwrap();
            assert!(
                rendered.starts_with("Config(entries=[") && rendered.ends_with("])"),
                "{rendered}"
            );
            assert!(
                rendered.contains(&a.to_string()) && rendered.contains(&b.to_string()),
                "{rendered}"
            );
            kafka_string_destroy(s);
            kafka_admin_Config_destroy(config);
            kafka_admin_Config_destroy(ptr::null_mut());
        }
    }
}
