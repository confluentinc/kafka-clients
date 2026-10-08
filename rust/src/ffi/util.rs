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

//! The package-less C helpers standing for `java.util` types (CLAUDE.md §4,
//! "Generic types"): [`kafka_List_t`], [`kafka_Map_t`], [`kafka_Bytes_t`] and
//! [`kafka_string_destroy`].
//!
//! They carry no package segment because they translate no Kafka class: a
//! Java `List<T>` / `Map<K, V>` / `byte[]` / `String` crossing the boundary
//! becomes one of these, with the element type documented by the function
//! that takes or returns the container (`T` is a `void *`, CLAUDE.md §4).
//!
//! # Ownership
//!
//! A container built by C (`kafka_List_new`, `kafka_Map_new`) holds
//! **borrowed** elements: a function taking it copies what it needs during the
//! call and the caller frees the elements and the container afterwards. A
//! container returned by Rust **owns** its elements and frees them in
//! `_destroy`, so the caller destroys only the container. The two cases share
//! one type; the difference is whether the Rust side recorded a per-element
//! destructor when it built the container.
//!
//! An owned `String` crossing to C is a `char *` freed with
//! [`kafka_string_destroy`]; a borrowed `&str` is a `const char *` valid as
//! long as its owner.

use std::collections::HashSet;
use std::ffi::{CStr, CString, c_char, c_void};
use std::ptr;

// ---------------------------------------------------------------------------
// Strings
// ---------------------------------------------------------------------------

/// Frees a `char *` an FFI function returned as an **owned** string.
///
/// Every owned string return in this API is documented as such and freed
/// here, never with `free()`: it was allocated by Rust. Passing null is a
/// no-op, as with `free(NULL)`.
///
/// # Safety
///
/// `s` must be null or an owned string returned by this API and not yet freed.
#[unsafe(no_mangle)]
// an owned-string destructor standing for `java.lang.String` (CLAUDE.md §4 rule 6)
#[doc(alias = "rust-only")]
pub unsafe extern "C" fn kafka_string_destroy(s: *mut c_char) {
    if !s.is_null() {
        drop(unsafe { CString::from_raw(s) });
    }
}

/// Hands `s` to C as an owned string, freed with [`kafka_string_destroy`].
///
/// An interior NUL — impossible for the identifiers and messages this crate
/// produces — truncates the string at the NUL rather than failing.
pub(crate) fn into_c_string(s: &str) -> *mut c_char {
    owned_c_string(s).into_raw()
}

/// A [`CString`] for `s`, truncated at an interior NUL rather than failing.
pub(crate) fn owned_c_string(s: &str) -> CString {
    match CString::new(s) {
        Ok(c) => c,
        Err(e) => {
            let nul = e.nul_position();
            let mut bytes = e.into_vec();
            bytes.truncate(nul);
            // The bytes before the first NUL contain no NUL.
            CString::new(bytes).unwrap_or_default()
        },
    }
}

/// Reads a borrowed C string into a `String`; null reads as empty.
///
/// # Safety
///
/// `s` must be null or a valid NUL-terminated string.
pub(crate) unsafe fn c_str_to_string(s: *const c_char) -> String {
    if s.is_null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(s) }.to_string_lossy().into_owned()
    }
}

/// Reads a borrowed C string into an `Option<String>`; null reads as `None`.
///
/// # Safety
///
/// `s` must be null or a valid NUL-terminated string.
pub(crate) unsafe fn c_str_to_option(s: *const c_char) -> Option<String> {
    if s.is_null() {
        None
    } else {
        Some(unsafe { c_str_to_string(s) })
    }
}

// ---------------------------------------------------------------------------
// Byte buffers
// ---------------------------------------------------------------------------

/// A borrowed byte buffer: how a Java `byte[]` crosses the boundary
/// (CLAUDE.md §4, "Primitive types").
///
/// `data == NULL` stands for a Java `null` array; an empty array has a
/// non-null `data` and `len == 0`. The buffer is never owned by this struct:
/// whoever produced it keeps it alive for as long as the function taking or
/// returning it documents. This is the only place `uint8_t` appears in the
/// C API — every other integer is a signed fixed-width type.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
// a borrowed `byte[]` view standing for the JDK array type (CLAUDE.md §4 rule 6)
#[doc(alias = "rust-only")]
pub struct kafka_Bytes_t {
    /// The first byte, or null for a Java `null` array.
    pub data: *const u8,
    /// The number of bytes behind `data`.
    pub len: i32,
}

// wired by the NULL-serde record key/value path (Phase 2, producer)
#[cfg_attr(not(test), expect(dead_code))]
impl kafka_Bytes_t {
    /// The Java `null` array.
    pub(crate) const NULL: Self = Self { data: ptr::null(), len: 0 };

    /// A view over `bytes`, valid as long as `bytes` is.
    pub(crate) fn from_slice(bytes: &[u8]) -> Self {
        Self { data: bytes.as_ptr(), len: i32::try_from(bytes.len()).unwrap_or(i32::MAX) }
    }

    /// A view over `bytes`, or [`Self::NULL`] for `None`.
    pub(crate) fn from_option(bytes: Option<&[u8]>) -> Self {
        bytes.map_or(Self::NULL, Self::from_slice)
    }

    /// The bytes behind the view, or `None` for a Java `null` array.
    ///
    /// # Safety
    ///
    /// `data` must be null or point to `len` readable bytes that outlive `'a`.
    pub(crate) unsafe fn as_slice<'a>(self) -> Option<&'a [u8]> {
        if self.data.is_null() {
            None
        } else {
            Some(unsafe { std::slice::from_raw_parts(self.data, usize::try_from(self.len).unwrap_or(0)) })
        }
    }
}

// ---------------------------------------------------------------------------
// Lists
// ---------------------------------------------------------------------------

/// Frees one element of an owned container.
pub(crate) type ElementDestroy = unsafe fn(*mut c_void);

/// Compares two keys of a map for `kafka_Map_get`.
pub(crate) type KeyEq = unsafe fn(*mut c_void, *mut c_void) -> bool;

/// What a [`kafka_List_t`] handle points at.
pub(crate) struct ListInner {
    elements: Vec<*mut c_void>,
    /// Set when Rust built the list and owns the elements.
    destroy: Option<ElementDestroy>,
}

impl ListInner {
    /// The elements, in insertion order.
    pub(crate) fn elements(&self) -> &[*mut c_void] {
        &self.elements
    }
}

impl Drop for ListInner {
    fn drop(&mut self) {
        if let Some(destroy) = self.destroy {
            for element in self.elements.drain(..) {
                if !element.is_null() {
                    unsafe { destroy(element) };
                }
            }
        }
    }
}

/// A `java.util.List` of `void *` elements (CLAUDE.md §4, "Generic types").
///
/// The function taking or returning a list documents the element type and
/// whether the list owns its elements (see the module docs).
#[repr(C)]
// a `void *` list standing for `java.util.List` (CLAUDE.md §4 rule 6)
#[doc(alias = "rust-only")]
pub struct kafka_List_t {
    _private: [u8; 0],
}

/// Hands `elements` to C as a list; `destroy`, when set, frees each element
/// in `kafka_List_destroy`.
pub(crate) fn box_list(elements: Vec<*mut c_void>, destroy: Option<ElementDestroy>) -> *mut kafka_List_t {
    Box::into_raw(Box::new(ListInner { elements, destroy })) as *mut kafka_List_t
}

/// Hands `strings` to C as an owned list of `char *`.
pub(crate) fn box_string_list<I, S>(strings: I) -> *mut kafka_List_t
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let elements = strings.into_iter().map(|s| into_c_string(s.as_ref()) as *mut c_void).collect();
    box_list(elements, Some(destroy_string_element))
}

/// Frees a `char *` element of an owned string list or map.
pub(crate) unsafe fn destroy_string_element(element: *mut c_void) {
    unsafe { kafka_string_destroy(element as *mut c_char) };
}

/// Frees an element boxed with `Box::into_raw(Box::new(value))`.
pub(crate) unsafe fn destroy_boxed<T>(element: *mut c_void) {
    drop(unsafe { Box::from_raw(element as *mut T) });
}

/// The list behind a handle.
///
/// # Safety
///
/// `list` must be a valid handle from [`box_list`] / `kafka_List_new`.
pub(crate) unsafe fn list_ref<'a>(list: *const kafka_List_t) -> &'a ListInner {
    unsafe { &*(list as *const ListInner) }
}

/// The elements of a list handle; null reads as empty.
///
/// # Safety
///
/// `list` must be null or a valid list handle.
pub(crate) unsafe fn list_elements<'a>(list: *const kafka_List_t) -> &'a [*mut c_void] {
    if list.is_null() {
        &[]
    } else {
        unsafe { list_ref(list) }.elements()
    }
}

/// Reads a list of `const char *` into `String`s; null reads as empty.
///
/// # Safety
///
/// `list` must be null or a valid list whose elements are NUL-terminated
/// strings.
pub(crate) unsafe fn list_strings(list: *const kafka_List_t) -> Vec<String> {
    unsafe { list_elements(list) }
        .iter()
        .map(|&element| unsafe { c_str_to_string(element as *const c_char) })
        .collect()
}

/// Reads a list of `const char *` into a set, the shape of a Java
/// `Set<String>` parameter; null reads as empty.
///
/// # Safety
///
/// `list` must be null or a valid list whose elements are NUL-terminated
/// strings.
pub(crate) unsafe fn list_string_set(list: *const kafka_List_t) -> HashSet<String> {
    unsafe { list_strings(list) }.into_iter().collect()
}

/// Hands a Java `Set<String>` to C as an owned, sorted list of `char *`, so
/// the order a C caller sees is deterministic.
pub(crate) fn sorted_string_list<I, S>(strings: I) -> *mut kafka_List_t
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut strings: Vec<S> = strings.into_iter().collect();
    strings.sort_unstable_by(|a, b| a.as_ref().cmp(b.as_ref()));
    box_string_list(strings)
}

/// Creates an empty list whose elements the caller owns.
///
/// The caller frees the elements it added and then the list, with
/// [`kafka_List_destroy`]; functions taking the list copy what they need
/// during the call.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_List_new() -> *mut kafka_List_t {
    box_list(Vec::new(), None)
}

/// Appends `element` to the list.
///
/// # Safety
///
/// `self_` must be a valid list handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_List_add(self_: *mut kafka_List_t, element: *mut c_void) {
    unsafe { &mut *(self_ as *mut ListInner) }.elements.push(element);
}

/// The number of elements.
///
/// # Safety
///
/// `self_` must be a valid list handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_List_size(self_: *const kafka_List_t) -> i32 {
    i32::try_from(unsafe { list_ref(self_) }.elements.len()).unwrap_or(i32::MAX)
}

/// The element at `index`, or null when `index` is out of range.
///
/// The element stays owned by whoever owns it (see the module docs).
///
/// # Safety
///
/// `self_` must be a valid list handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_List_get(self_: *const kafka_List_t, index: i32) -> *mut c_void {
    let inner = unsafe { list_ref(self_) };
    usize::try_from(index)
        .ok()
        .and_then(|i| inner.elements.get(i))
        .copied()
        .unwrap_or(ptr::null_mut())
}

/// Frees the list, and its elements when the list owns them.
///
/// # Safety
///
/// `self_` must be null or a valid list handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_List_destroy(self_: *mut kafka_List_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ListInner) });
    }
}

// ---------------------------------------------------------------------------
// Maps
// ---------------------------------------------------------------------------

/// What a [`kafka_Map_t`] handle points at.
pub(crate) struct MapInner {
    entries: Vec<(*mut c_void, *mut c_void)>,
    /// Set when Rust built the map and owns the keys.
    key_destroy: Option<ElementDestroy>,
    /// Set when Rust built the map and owns the values.
    value_destroy: Option<ElementDestroy>,
    /// How `kafka_Map_get` compares keys; pointer identity when unset.
    key_eq: Option<KeyEq>,
}

impl MapInner {
    /// The entries, in insertion order.
    pub(crate) fn entries(&self) -> &[(*mut c_void, *mut c_void)] {
        &self.entries
    }

    fn position(&self, key: *mut c_void) -> Option<usize> {
        self.entries.iter().position(|&(k, _)| match self.key_eq {
            Some(eq) => unsafe { eq(k, key) },
            None => k == key,
        })
    }
}

impl Drop for MapInner {
    fn drop(&mut self) {
        for (key, value) in self.entries.drain(..) {
            if let Some(destroy) = self.key_destroy
                && !key.is_null()
            {
                unsafe { destroy(key) };
            }
            if let Some(destroy) = self.value_destroy
                && !value.is_null()
            {
                unsafe { destroy(value) };
            }
        }
    }
}

/// A `java.util.Map` of `void *` keys and values (CLAUDE.md §4, "Generic
/// types").
///
/// Entries keep insertion order and are reachable by index
/// ([`kafka_Map_key`] / [`kafka_Map_value`]) or by key ([`kafka_Map_get`]).
/// The function taking or returning a map documents the key and value types
/// and whether the map owns them (see the module docs).
#[repr(C)]
// a `void *` map standing for `java.util.Map` (CLAUDE.md §4 rule 6)
#[doc(alias = "rust-only")]
pub struct kafka_Map_t {
    _private: [u8; 0],
}

/// Hands `entries` to C as a map; the destructors, when set, free the keys
/// and values in `kafka_Map_destroy`, and `key_eq` drives `kafka_Map_get`.
pub(crate) fn box_map(
    entries: Vec<(*mut c_void, *mut c_void)>,
    key_destroy: Option<ElementDestroy>,
    value_destroy: Option<ElementDestroy>,
    key_eq: Option<KeyEq>,
) -> *mut kafka_Map_t {
    Box::into_raw(Box::new(MapInner { entries, key_destroy, value_destroy, key_eq })) as *mut kafka_Map_t
}

/// Hands string-keyed `entries` to C as an owned map: the keys become owned
/// `char *`, compared by content in `kafka_Map_get`, and `value_destroy`
/// frees each value.
// wired by the string-keyed result maps of the admin RPCs (Phase 4)
#[cfg_attr(not(test), expect(dead_code))]
pub(crate) fn box_string_keyed_map<I, S>(entries: I, value_destroy: Option<ElementDestroy>) -> *mut kafka_Map_t
where
    I: IntoIterator<Item = (S, *mut c_void)>,
    S: AsRef<str>,
{
    let entries = entries
        .into_iter()
        .map(|(k, v)| (into_c_string(k.as_ref()) as *mut c_void, v))
        .collect();
    box_map(entries, Some(destroy_string_element), value_destroy, Some(string_key_eq))
}

/// Compares two `const char *` keys by content.
pub(crate) unsafe fn string_key_eq(a: *mut c_void, b: *mut c_void) -> bool {
    if a.is_null() || b.is_null() {
        return a == b;
    }
    unsafe { CStr::from_ptr(a as *const c_char) == CStr::from_ptr(b as *const c_char) }
}

/// The map behind a handle.
///
/// # Safety
///
/// `map` must be a valid handle from [`box_map`] / `kafka_Map_new`.
pub(crate) unsafe fn map_ref<'a>(map: *const kafka_Map_t) -> &'a MapInner {
    unsafe { &*(map as *const MapInner) }
}

/// The entries of a map handle; null reads as empty.
///
/// # Safety
///
/// `map` must be null or a valid map handle.
pub(crate) unsafe fn map_entries<'a>(map: *const kafka_Map_t) -> &'a [(*mut c_void, *mut c_void)] {
    if map.is_null() {
        &[]
    } else {
        unsafe { map_ref(map) }.entries()
    }
}

/// Creates an empty map whose keys and values the caller owns.
///
/// Keys are compared by pointer identity in [`kafka_Map_put`] and
/// [`kafka_Map_get`]; a function taking the map reads the keys by the type it
/// documents (two distinct `char *` with the same text are the same key to
/// it). The caller frees what it put in and then the map, with
/// [`kafka_Map_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_Map_new() -> *mut kafka_Map_t {
    box_map(Vec::new(), None, None, None)
}

/// Associates `value` with `key`, replacing the value of an existing key as
/// `java.util.Map.put` does.
///
/// # Safety
///
/// `self_` must be a valid map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_Map_put(self_: *mut kafka_Map_t, key: *mut c_void, value: *mut c_void) {
    let inner = unsafe { &mut *(self_ as *mut MapInner) };
    match inner.position(key) {
        Some(i) => {
            let old = std::mem::replace(&mut inner.entries[i].1, value);
            if let Some(destroy) = inner.value_destroy
                && !old.is_null()
                && old != value
            {
                unsafe { destroy(old) };
            }
        },
        None => inner.entries.push((key, value)),
    }
}

/// The number of entries.
///
/// # Safety
///
/// `self_` must be a valid map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_Map_size(self_: *const kafka_Map_t) -> i32 {
    i32::try_from(unsafe { map_ref(self_) }.entries.len()).unwrap_or(i32::MAX)
}

/// The key of the entry at `index`, or null when `index` is out of range.
///
/// # Safety
///
/// `self_` must be a valid map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_Map_key(self_: *const kafka_Map_t, index: i32) -> *mut c_void {
    let inner = unsafe { map_ref(self_) };
    usize::try_from(index)
        .ok()
        .and_then(|i| inner.entries.get(i))
        .map_or(ptr::null_mut(), |e| e.0)
}

/// The value of the entry at `index`, or null when `index` is out of range.
///
/// # Safety
///
/// `self_` must be a valid map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_Map_value(self_: *const kafka_Map_t, index: i32) -> *mut c_void {
    let inner = unsafe { map_ref(self_) };
    usize::try_from(index)
        .ok()
        .and_then(|i| inner.entries.get(i))
        .map_or(ptr::null_mut(), |e| e.1)
}

/// The value associated with `key`, or null when there is none.
///
/// A map built by Rust compares keys by the documented key type (string keys
/// by content); one built by C compares by pointer identity.
///
/// # Safety
///
/// `self_` must be a valid map handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_Map_get(self_: *const kafka_Map_t, key: *mut c_void) -> *mut c_void {
    let inner = unsafe { map_ref(self_) };
    inner.position(key).map_or(ptr::null_mut(), |i| inner.entries[i].1)
}

/// Frees the map, and its keys and values when the map owns them.
///
/// # Safety
///
/// `self_` must be null or a valid map handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_Map_destroy(self_: *mut kafka_Map_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut MapInner) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(s: &str) -> CString {
        CString::new(s).unwrap()
    }

    #[test]
    fn string_destroy_accepts_null_and_owned_strings() {
        unsafe { kafka_string_destroy(ptr::null_mut()) };
        let owned = into_c_string("hello");
        assert_eq!(unsafe { CStr::from_ptr(owned) }.to_str().unwrap(), "hello");
        unsafe { kafka_string_destroy(owned) };
    }

    #[test]
    fn owned_c_string_truncates_at_an_interior_nul() {
        assert_eq!(owned_c_string("ab\0cd").as_bytes(), b"ab");
        assert_eq!(owned_c_string("").as_bytes(), b"");
    }

    #[test]
    fn c_str_helpers_read_null_as_empty_or_none() {
        assert_eq!(unsafe { c_str_to_string(ptr::null()) }, "");
        assert_eq!(unsafe { c_str_to_option(ptr::null()) }, None);
        let s = c("topic");
        assert_eq!(unsafe { c_str_to_string(s.as_ptr()) }, "topic");
        assert_eq!(unsafe { c_str_to_option(s.as_ptr()) }, Some("topic".to_string()));
    }

    #[test]
    fn bytes_view_distinguishes_null_from_empty() {
        let null = kafka_Bytes_t::from_option(None);
        assert!(null.data.is_null());
        assert_eq!(null.len, 0);
        assert_eq!(unsafe { null.as_slice() }, None);

        let empty: &[u8] = &[];
        let view = kafka_Bytes_t::from_slice(empty);
        assert!(!view.data.is_null());
        assert_eq!(view.len, 0);
        assert_eq!(unsafe { view.as_slice() }, Some(empty));

        let bytes = b"abc";
        let view = kafka_Bytes_t::from_option(Some(bytes));
        assert_eq!(view.len, 3);
        assert_eq!(unsafe { view.as_slice() }, Some(&bytes[..]));
    }

    #[test]
    fn c_built_list_borrows_its_elements() {
        let a = c("a");
        let b = c("b");
        let list = kafka_List_new();
        assert_eq!(unsafe { kafka_List_size(list) }, 0);
        unsafe {
            kafka_List_add(list, a.as_ptr() as *mut c_void);
            kafka_List_add(list, b.as_ptr() as *mut c_void);
        }
        assert_eq!(unsafe { kafka_List_size(list) }, 2);
        assert_eq!(unsafe { kafka_List_get(list, 1) }, b.as_ptr() as *mut c_void);
        assert!(unsafe { kafka_List_get(list, 2) }.is_null());
        assert!(unsafe { kafka_List_get(list, -1) }.is_null());
        assert_eq!(unsafe { list_strings(list) }, vec!["a".to_string(), "b".to_string()]);
        // Destroying the list leaves `a` and `b` untouched: they are still
        // valid CStrings dropped at the end of the test.
        unsafe { kafka_List_destroy(list) };
        assert_eq!(a.to_str().unwrap(), "a");
    }

    #[test]
    fn rust_built_string_list_owns_its_elements() {
        let list = box_string_list(["x", "y", "z"]);
        assert_eq!(unsafe { kafka_List_size(list) }, 3);
        let second = unsafe { kafka_List_get(list, 1) } as *const c_char;
        assert_eq!(unsafe { CStr::from_ptr(second) }.to_str().unwrap(), "y");
        // Frees the three strings along with the list (checked by Miri /
        // sanitizers rather than by an assertion: there is nothing to observe).
        unsafe { kafka_List_destroy(list) };
        unsafe { kafka_List_destroy(ptr::null_mut()) };
        assert!(unsafe { list_strings(ptr::null()) }.is_empty());
        assert!(unsafe { list_elements(ptr::null()) }.is_empty());
    }

    #[test]
    fn boxed_elements_are_freed_with_their_type() {
        static DROPS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        struct Counted;
        impl Drop for Counted {
            fn drop(&mut self) {
                DROPS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let elements = (0..3).map(|_| Box::into_raw(Box::new(Counted)) as *mut c_void).collect();
        let list = box_list(elements, Some(destroy_boxed::<Counted>));
        unsafe { kafka_List_destroy(list) };
        assert_eq!(DROPS.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[test]
    fn c_built_map_compares_keys_by_identity_and_replaces_on_put() {
        let k1 = c("k");
        let k2 = c("k");
        let v1 = c("v1");
        let v2 = c("v2");
        let map = kafka_Map_new();
        unsafe {
            kafka_Map_put(map, k1.as_ptr() as *mut c_void, v1.as_ptr() as *mut c_void);
            kafka_Map_put(map, k2.as_ptr() as *mut c_void, v2.as_ptr() as *mut c_void);
        }
        // Two distinct pointers with the same text are two keys to a C-built map.
        assert_eq!(unsafe { kafka_Map_size(map) }, 2);
        unsafe { kafka_Map_put(map, k1.as_ptr() as *mut c_void, v2.as_ptr() as *mut c_void) };
        assert_eq!(unsafe { kafka_Map_size(map) }, 2);
        assert_eq!(
            unsafe { kafka_Map_get(map, k1.as_ptr() as *mut c_void) },
            v2.as_ptr() as *mut c_void
        );
        assert_eq!(unsafe { kafka_Map_key(map, 1) }, k2.as_ptr() as *mut c_void);
        assert_eq!(unsafe { kafka_Map_value(map, 0) }, v2.as_ptr() as *mut c_void);
        assert!(unsafe { kafka_Map_key(map, 2) }.is_null());
        assert!(unsafe { kafka_Map_value(map, -1) }.is_null());
        assert!(unsafe { kafka_Map_get(map, v1.as_ptr() as *mut c_void) }.is_null());
        assert_eq!(unsafe { map_entries(map) }.len(), 2);
        unsafe { kafka_Map_destroy(map) };
        unsafe { kafka_Map_destroy(ptr::null_mut()) };
        assert!(unsafe { map_entries(ptr::null()) }.is_empty());
    }

    #[test]
    fn rust_built_string_keyed_map_compares_keys_by_content() {
        let values: Vec<*mut c_void> = (1..=2).map(|i| Box::into_raw(Box::new(i as i64)) as *mut c_void).collect();
        let map = box_string_keyed_map([("alpha", values[0]), ("beta", values[1])], Some(destroy_boxed::<i64>));
        assert_eq!(unsafe { kafka_Map_size(map) }, 2);
        let probe = c("beta");
        let found = unsafe { kafka_Map_get(map, probe.as_ptr() as *mut c_void) } as *const i64;
        assert_eq!(unsafe { *found }, 2);
        let key = unsafe { kafka_Map_key(map, 0) } as *const c_char;
        assert_eq!(unsafe { CStr::from_ptr(key) }.to_str().unwrap(), "alpha");
        let missing = c("gamma");
        assert!(unsafe { kafka_Map_get(map, missing.as_ptr() as *mut c_void) }.is_null());
        unsafe { kafka_Map_destroy(map) };
    }

    #[test]
    fn put_on_an_owning_map_frees_the_replaced_value() {
        static DROPS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        // Not zero-sized: every `Box<ZST>` shares one dangling address, which
        // `put` would (rightly) read as "the same value put again".
        struct Counted(#[expect(dead_code)] i32);
        impl Drop for Counted {
            fn drop(&mut self) {
                DROPS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let first = Box::into_raw(Box::new(Counted(1))) as *mut c_void;
        let map = box_string_keyed_map([("k", first)], Some(destroy_boxed::<Counted>));
        let key = c("k");
        let second = Box::into_raw(Box::new(Counted(2))) as *mut c_void;
        unsafe { kafka_Map_put(map, key.as_ptr() as *mut c_void, second) };
        assert_eq!(
            DROPS.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the replaced value is freed"
        );
        assert_eq!(unsafe { kafka_Map_size(map) }, 1);
        unsafe { kafka_Map_destroy(map) };
        assert_eq!(
            DROPS.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the remaining value is freed"
        );
    }
}
