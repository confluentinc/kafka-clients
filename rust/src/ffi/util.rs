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

use crate::ffi::ffi_guard;

// ---------------------------------------------------------------------------
// Strings
// ---------------------------------------------------------------------------

/// Frees a `char *` an FFI function returned as an **owned** string.
///
/// Every owned string return in this API is documented as such and freed
/// here, never with `free()`: it was allocated by Rust. Passing null is a
/// no-op, as with `free(NULL)`.
///
/// # Parameters
///
/// - `s` - The owned string to free, or `NULL`.
///
/// # Safety
///
/// `s` must be null or an owned string returned by this API and not yet freed.
#[ffi_guard]
#[unsafe(no_mangle)]
// an owned-string destructor standing for `java.lang.String` (CLAUDE.md §4 rule 6)
#[doc(alias = "rust-only")]
pub unsafe extern "C" fn kafka_string_destroy(s: *mut c_char) {
    if !s.is_null() {
        // SAFETY: `s` is non-null (checked above) and, per this function's `# Safety`, an
        // owned string this API returned and not yet freed. Every such string comes from
        // `CString::into_raw` in `into_c_string`, so `from_raw` reclaims that allocation,
        // once.
        drop(unsafe { CString::from_raw(s) });
    }
}

/// Hands `s` to C as an owned string, freed with `kafka_string_destroy`.
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
        // SAFETY: `s` is non-null (checked above) and, per this function's `# Safety`, a
        // valid NUL-terminated string. The borrow is copied into an owned `String` before
        // return, so it only has to last for this call.
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
        // SAFETY: per this function's `# Safety`, `s` is a valid NUL-terminated string,
        // which is what `c_str_to_string` requires.
        Some(unsafe { c_str_to_string(s) })
    }
}

// ---------------------------------------------------------------------------
// Byte buffers
// ---------------------------------------------------------------------------

/// A borrowed byte buffer: how a Java `byte[]` crosses the boundary.
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
    /// `data` must be null or point to `len` readable bytes that outlive `'a`
    /// and are not written during `'a`.
    pub(crate) unsafe fn as_slice<'a>(self) -> Option<&'a [u8]> {
        if self.data.is_null() {
            None
        } else {
            // SAFETY: `data` is non-null (checked above) and, per this function's
            // `# Safety`, points to `len` readable bytes that live, unwritten, for `'a`.
            // A `u8` has no alignment to break, and a negative `len` reads as 0 bytes.
            Some(unsafe { std::slice::from_raw_parts(self.data, usize::try_from(self.len).unwrap_or(0)) })
        }
    }
}

/// An owned byte array behind a `kafka_Bytes_t *`: what a built-in
/// deserializer hands out as its `void *` (CLAUDE.md §4, "Generic types"),
/// freed with `kafka_Bytes_destroy`. The view is the first field, so the
/// pointer to the box is the pointer to the view; `bytes` keeps the buffer
/// the view points into alive.
#[repr(C)]
struct OwnedBytes {
    view: kafka_Bytes_t,
    bytes: bytes::Bytes,
}

/// Hands `bytes` to C as an owned array, freed with `kafka_Bytes_destroy`.
pub(crate) fn box_bytes(bytes: bytes::Bytes) -> *mut kafka_Bytes_t {
    // `Bytes` is a pointer into a shared buffer: moving it into the box
    // leaves the bytes where they are, so the view taken first stays valid.
    let view = kafka_Bytes_t::from_slice(&bytes);
    Box::into_raw(Box::new(OwnedBytes { view, bytes })) as *mut kafka_Bytes_t
}

/// Frees an owned byte array built by Rust — the `void *` a built-in
/// deserializer returns. Null is a no-op; a `kafka_Bytes_t` borrowed from a
/// record or a header is never passed here.
///
/// # Parameters
///
/// - `bytes` - The owned byte array to free, or `NULL`.
///
/// # Safety
///
/// `bytes` must be null or an owned array not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
// an owned-array destructor standing for `byte[]` (CLAUDE.md §4 rule 6)
#[doc(alias = "rust-only")]
pub unsafe extern "C" fn kafka_Bytes_destroy(bytes: *mut kafka_Bytes_t) {
    if !bytes.is_null() {
        // SAFETY: `bytes` is non-null (checked above) and, per this function's `# Safety`,
        // an owned array not yet destroyed. Those come only from `box_bytes`, whose
        // `Box::into_raw` of an `OwnedBytes` gave this pointer (the view is its first
        // `repr(C)` field), so `from_raw` reclaims that allocation, once.
        drop(unsafe { Box::from_raw(bytes as *mut OwnedBytes) });
    }
}

// ---------------------------------------------------------------------------
// Generic arguments
// ---------------------------------------------------------------------------

/// The value of a Java generic argument (`K`, `V`, `T`) as it crosses the
/// boundary: the `void *` CLAUDE.md §4 ("Generic types") puts in its place.
///
/// Rust never reads what it points at — only the serializer or deserializer
/// the C caller paired with it does — so it carries no lifetime, and it
/// crosses tasks like every other C registration: the thread-safety of the
/// pointee is the C caller's responsibility. A null pointer is Java's `null`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct GenericValue(*mut c_void);

// SAFETY: see the type's documentation — the pointee is the C caller's.
unsafe impl Send for GenericValue {}
// SAFETY: as for `Send`: a shared `&GenericValue` only copies the pointer value
// out, and Rust never dereferences it, so sharing it adds no access beyond what
// the C caller's contract on the pointee already allows.
unsafe impl Sync for GenericValue {}

impl GenericValue {
    /// The value behind `ptr`; null is Java's `null`.
    pub(crate) fn new(ptr: *mut c_void) -> Self {
        Self(ptr)
    }

    /// The `void *`.
    pub(crate) fn as_ptr(self) -> *mut c_void {
        self.0
    }

    /// Whether this is Java's `null`.
    pub(crate) fn is_null(self) -> bool {
        self.0.is_null()
    }
}

// ---------------------------------------------------------------------------
// Lists
// ---------------------------------------------------------------------------

/// Frees one element of an owned container.
pub(crate) type ElementDestroy = unsafe fn(*mut c_void);

/// Compares two keys of a map for `kafka_Map_get`.
pub(crate) type KeyEq = unsafe fn(*mut c_void, *mut c_void) -> bool;

/// What a `kafka_List_t` handle points at.
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
                    // SAFETY: `destroy` is set only by `box_list`'s callers, each of which
                    // pairs it with the elements it frees (`destroy_string_element` for
                    // `into_c_string` strings, `destroy_boxed::<T>` for `Box<T>`, ...), and
                    // `kafka_List_add`'s `# Safety` requires an element added to such a
                    // list to be one `destroy` frees. The element is non-null (checked
                    // above) and `drain` hands each one out once, so it is freed once.
                    unsafe { destroy(element) };
                }
            }
        }
    }
}

/// A `java.util.List` of `void *` elements.
///
/// The function taking or returning a list documents the element type. A list
/// built by C (`kafka_List_new`) holds borrowed elements: a function taking it
/// copies what it needs during the call, and the caller frees the elements and
/// the list afterwards. A list returned by Rust owns its elements and frees
/// them in `kafka_List_destroy`, so the caller destroys only the list.
#[repr(C)]
// a `void *` list standing for `java.util.List` (CLAUDE.md §4 rule 6)
#[doc(alias = "rust-only")]
pub struct kafka_List_t {
    _private: [u8; 0],
}

/// Hands `elements` to C as a list; `destroy`, when set, frees each element
/// in `kafka_List_destroy`.
///
/// When `destroy` is set, every non-null element must be one it frees: the
/// list owns it from here on.
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
///
/// # Safety
///
/// `element` must be an owned string from [`into_c_string`], not yet freed.
pub(crate) unsafe fn destroy_string_element(element: *mut c_void) {
    // SAFETY: per this function's `# Safety`, `element` is an owned string from
    // `into_c_string`, not yet freed, which is what `kafka_string_destroy` requires.
    unsafe { kafka_string_destroy(element as *mut c_char) };
}

/// Frees an element boxed with `Box::into_raw(Box::new(value))`.
///
/// # Safety
///
/// `element` must come from `Box::into_raw` of a `Box<T>` and not be freed yet.
pub(crate) unsafe fn destroy_boxed<T>(element: *mut c_void) {
    // SAFETY: per this function's `# Safety`, `element` came from `Box::into_raw` of a
    // `Box<T>` and is not freed yet, so `from_raw` reclaims that allocation, once.
    drop(unsafe { Box::from_raw(element as *mut T) });
}

/// The list behind a handle.
///
/// # Safety
///
/// `list` must be a valid handle from [`box_list`] / `kafka_List_new`, alive
/// and not mutated for `'a`.
pub(crate) unsafe fn list_ref<'a>(list: *const kafka_List_t) -> &'a ListInner {
    // SAFETY: per this function's `# Safety`, `list` came from `box_list` (which
    // `kafka_List_new` uses too), whose `Box::into_raw` of a `ListInner` is the
    // allocation behind every list handle, and it stays alive and unmutated for `'a`.
    unsafe { &*(list as *const ListInner) }
}

/// The elements of a list handle; null reads as empty.
///
/// # Safety
///
/// `list` must be null or a valid list handle, alive and not mutated for
/// `'a`.
pub(crate) unsafe fn list_elements<'a>(list: *const kafka_List_t) -> &'a [*mut c_void] {
    if list.is_null() {
        &[]
    } else {
        // SAFETY: `list` is non-null (checked above) and, per this function's
        // `# Safety`, a valid list handle alive and unmutated for `'a`, as `list_ref`
        // requires.
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
    // SAFETY: per this function's `# Safety`, `list` is null or a valid list, kept alive
    // by the caller for this call, which is as long as the borrow of its elements lasts.
    unsafe { list_elements(list) }
        .iter()
        // SAFETY: per this function's `# Safety`, every element is a NUL-terminated
        // string, which `c_str_to_string` copies into an owned `String`.
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
    // SAFETY: per this function's `# Safety`, `list` is null or a valid list of
    // NUL-terminated strings, which is what `list_strings` requires.
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

/// Returns a new empty list.
///
/// # Safety
///
/// The list and the elements the caller adds to it are owned by the caller, which frees the
/// elements it added and then the list, with `kafka_List_destroy`; functions taking the list copy
/// what they need during the call.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_List_new() -> *mut kafka_List_t {
    box_list(Vec::new(), None)
}

/// Appends `element` to the list.
///
/// # Parameters
///
/// - `element` - The element to append.
///
/// # Safety
///
/// `self_` must be a valid list handle that no other call uses at the same
/// time. Whether the list takes `element` depends on how it was built, which
/// the function that returned it documents:
///
/// - A list from `kafka_List_new`, or a list Rust returned as a view (its
///   elements borrowed, or static singletons), only borrows `element`: the
///   caller keeps owning it and frees it after the list.
/// - A list Rust returned that owns its elements takes ownership of
///   `element`, which must then be null or of the type that list documents.
///
/// A list Rust returned is meant to be read; add to one only when its
/// documentation says so.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_List_add(self_: *mut kafka_List_t, element: *mut c_void) {
    // SAFETY: per this function's `# Safety`, `self_` is a valid list handle, so it points
    // at the `ListInner` `box_list` allocated, and no other call uses it meanwhile, so the
    // `&mut`, which lives only for this call, is unique.
    unsafe { &mut *(self_ as *mut ListInner) }.elements.push(element);
}

/// Returns the number of elements.
///
/// # Safety
///
/// `self_` must be a valid list handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_List_size(self_: *const kafka_List_t) -> i32 {
    // SAFETY: per this function's `# Safety`, `self_` is a valid list handle. The borrow
    // lasts for this call only, and the one mutator, `kafka_List_add`, requires that no
    // other call uses the list meanwhile, so nothing writes to it during the borrow.
    i32::try_from(unsafe { list_ref(self_) }.elements.len()).unwrap_or(i32::MAX)
}

/// Returns the element at `index`, or `NULL` when `index` is out of range.
///
/// # Parameters
///
/// - `index` - The zero-based index.
///
/// # Safety
///
/// `self_` must be a valid list handle.
///
/// The element stays owned by whoever owns it: the list, when Rust returned
/// it, else the C caller that added it.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_List_get(self_: *const kafka_List_t, index: i32) -> *mut c_void {
    // SAFETY: per this function's `# Safety`, `self_` is a valid list handle. The borrow
    // lasts for this call only, and the one mutator, `kafka_List_add`, requires that no
    // other call uses the list meanwhile, so nothing writes to it during the borrow.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_List_destroy(self_: *mut kafka_List_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`,
        // a list handle not yet destroyed, that is the `Box::into_raw` of `box_list`, so
        // `from_raw` reclaims that allocation, once.
        drop(unsafe { Box::from_raw(self_ as *mut ListInner) });
    }
}

// ---------------------------------------------------------------------------
// Maps
// ---------------------------------------------------------------------------

/// What a `kafka_Map_t` handle points at.
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

    /// The index of the entry whose key equals `key`, by `key_eq` or, when it
    /// is unset, by pointer identity. A null key, stored or looked up, equals
    /// only a null key: `key_eq` never sees one, as most of them read both
    /// keys.
    ///
    /// # Safety
    ///
    /// When `key_eq` is set, `key` must be null or a key of the type it
    /// compares (the key type the function that built the map documents):
    /// `key_eq` reads it.
    unsafe fn position(&self, key: *mut c_void) -> Option<usize> {
        self.entries.iter().position(|&(k, _)| match self.key_eq {
            Some(_) if k.is_null() || key.is_null() => k == key,
            // SAFETY: neither key is null (the arm above took that case). Every stored
            // non-null `k` is of the type `key_eq` compares: `box_map`'s callers set
            // `key_eq` together with such keys, and `kafka_Map_put` stores a key only
            // under its `# Safety`, which requires null or one. `key` is one per this
            // function's `# Safety`.
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
                // SAFETY: `key_destroy` is set only by `box_map`'s callers, each of which
                // pairs it with the keys it frees, and `kafka_Map_put`'s `# Safety`
                // requires a key put into such a map to be of the type the map documents,
                // so one it frees. The key is non-null (checked above) and `drain` hands
                // each entry out once, so it is freed once.
                unsafe { destroy(key) };
            }
            if let Some(destroy) = self.value_destroy
                && !value.is_null()
            {
                // SAFETY: `value_destroy` is set only by `box_map`'s callers, each of
                // which pairs it with the values it frees, and `kafka_Map_put`'s
                // `# Safety` requires a value put into such a map to be one it frees. The
                // value is non-null (checked above) and `drain` hands each entry out once,
                // so it is freed once.
                unsafe { destroy(value) };
            }
        }
    }
}

/// A `java.util.Map` of `void *` keys and values.
///
/// Entries keep insertion order and are reachable by index
/// (`kafka_Map_key` / `kafka_Map_value`) or by key (`kafka_Map_get`).
/// The function taking or returning a map documents the key and value types.
/// A map built by C (`kafka_Map_new`) holds borrowed keys and values: a
/// function taking it copies what it needs during the call, and the caller
/// frees them and the map afterwards. A map returned by Rust owns its keys and
/// values and frees them in `kafka_Map_destroy`, so the caller destroys only
/// the map.
#[repr(C)]
// a `void *` map standing for `java.util.Map` (CLAUDE.md §4 rule 6)
#[doc(alias = "rust-only")]
pub struct kafka_Map_t {
    _private: [u8; 0],
}

/// Hands `entries` to C as a map; the destructors, when set, free the keys
/// and values in `kafka_Map_destroy`, and `key_eq` drives `kafka_Map_get`.
///
/// When a destructor is set, every non-null key (or value) must be one it
/// frees, and when `key_eq` is set, every key one it compares: the map owns
/// them from here on.
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

/// Hands a Java `Map<String, String>` to C as an owned map of `char *` to
/// `char *`, in iteration order, `kafka_Map_get` comparing keys by content.
pub(crate) fn box_string_map<I, K, V>(entries: I) -> *mut kafka_Map_t
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    box_string_keyed_map(
        entries.into_iter().map(|(k, v)| (k, into_c_string(v.as_ref()) as *mut c_void)),
        Some(destroy_string_element),
    )
}

/// Reads a map of `const char *` keys and values into owned pairs, in entry
/// order; null reads as empty.
///
/// # Safety
///
/// `map` must be null or a valid map whose keys and values are
/// NUL-terminated strings.
pub(crate) unsafe fn map_strings(map: *const kafka_Map_t) -> Vec<(String, String)> {
    // SAFETY: per this function's `# Safety`, `map` is null or a valid map, kept alive by
    // the caller for this call, which is as long as the borrow of its entries lasts.
    unsafe { map_entries(map) }
        .iter()
        .map(|&(k, v)| {
            // SAFETY: per this function's `# Safety`, every key and value is a
            // NUL-terminated string, which `c_str_to_string` copies into a `String`.
            (unsafe { c_str_to_string(k as *const c_char) }, unsafe {
                c_str_to_string(v as *const c_char)
            })
        })
        .collect()
}

/// Compares two `const char *` keys by content.
///
/// # Safety
///
/// `a` and `b` must each be null or a valid NUL-terminated string.
pub(crate) unsafe fn string_key_eq(a: *mut c_void, b: *mut c_void) -> bool {
    if a.is_null() || b.is_null() {
        return a == b;
    }
    // SAFETY: `a` and `b` are non-null (checked above) and, per this function's
    // `# Safety`, valid NUL-terminated strings; both borrows end with the comparison.
    unsafe { CStr::from_ptr(a as *const c_char) == CStr::from_ptr(b as *const c_char) }
}

/// The map behind a handle.
///
/// # Safety
///
/// `map` must be a valid handle from [`box_map`] / `kafka_Map_new`, alive and
/// not mutated for `'a`.
pub(crate) unsafe fn map_ref<'a>(map: *const kafka_Map_t) -> &'a MapInner {
    // SAFETY: per this function's `# Safety`, `map` came from `box_map` (which
    // `kafka_Map_new` uses too), whose `Box::into_raw` of a `MapInner` is the allocation
    // behind every map handle, and it stays alive and unmutated for `'a`.
    unsafe { &*(map as *const MapInner) }
}

/// The entries of a map handle; null reads as empty.
///
/// # Safety
///
/// `map` must be null or a valid map handle, alive and not mutated for `'a`.
pub(crate) unsafe fn map_entries<'a>(map: *const kafka_Map_t) -> &'a [(*mut c_void, *mut c_void)] {
    if map.is_null() {
        &[]
    } else {
        // SAFETY: `map` is non-null (checked above) and, per this function's `# Safety`, a
        // valid map handle alive and unmutated for `'a`, as `map_ref` requires.
        unsafe { map_ref(map) }.entries()
    }
}

/// Returns a new empty map.
///
/// Keys are compared by pointer identity in `kafka_Map_put` and
/// `kafka_Map_get`; a function taking the map reads the keys by the type it
/// documents (two distinct `char *` with the same text are the same key to
/// it).
///
/// # Safety
///
/// The map and the keys and values the caller puts in it are owned by the caller, which frees what
/// it put in and then the map, with `kafka_Map_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_Map_new() -> *mut kafka_Map_t {
    box_map(Vec::new(), None, None, None)
}

/// Associates `value` with `key`, replacing the value of an existing key as
/// `java.util.Map.put` does.
///
/// # Parameters
///
/// - `key` - The key.
/// - `value` - The value to associate with `key`.
///
/// # Safety
///
/// `self_` must be a valid map handle that no other call uses at the same
/// time, and `key` null or a key of the type the map documents when Rust
/// built the map: its non-null keys are compared by reading them. Whether the map takes `key`
/// and `value` depends on how it was built, which the function that returned
/// it documents:
///
/// - A map from `kafka_Map_new` only borrows `key` and `value`: the caller
///   keeps owning them and frees them after the map.
/// - A map Rust returned takes ownership of `value` when it owns its values,
///   and of `key` when it owns its keys and no equal key is present; what it
///   takes must be null or of the type that map documents. What it does not
///   own stays the caller's, as for `kafka_Map_new`.
///
/// A map Rust returned is meant to be read; put into one only when its
/// documentation says so.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_Map_put(self_: *mut kafka_Map_t, key: *mut c_void, value: *mut c_void) {
    // SAFETY: per this function's `# Safety`, `self_` is a valid map handle, so it points
    // at the `MapInner` `box_map` allocated, and no other call uses it meanwhile, so the
    // `&mut`, which lives only for this call, is unique.
    let inner = unsafe { &mut *(self_ as *mut MapInner) };
    // SAFETY: per this function's `# Safety`, `key` is null or of the type the map's
    // `key_eq` compares, when it has one.
    match unsafe { inner.position(key) } {
        Some(i) => {
            let old = std::mem::replace(&mut inner.entries[i].1, value);
            if let Some(destroy) = inner.value_destroy
                && !old.is_null()
                && old != value
            {
                // SAFETY: `old` was stored in this owning map, so it is one
                // `value_destroy` frees (set by `box_map`'s caller with its values, or put
                // here under this function's `# Safety`). It is non-null (checked above),
                // no longer in the map after the `replace`, and not the value just put.
                unsafe { destroy(old) };
            }
        },
        None => inner.entries.push((key, value)),
    }
}

/// Returns the number of entries.
///
/// # Safety
///
/// `self_` must be a valid map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_Map_size(self_: *const kafka_Map_t) -> i32 {
    // SAFETY: per this function's `# Safety`, `self_` is a valid map handle. The borrow
    // lasts for this call only, and the one mutator, `kafka_Map_put`, requires that no
    // other call uses the map meanwhile, so nothing writes to it during the borrow.
    i32::try_from(unsafe { map_ref(self_) }.entries.len()).unwrap_or(i32::MAX)
}

/// Returns the key of the entry at `index`, or `NULL` when `index` is out of range.
///
/// # Parameters
///
/// - `index` - The zero-based index.
///
/// # Safety
///
/// `self_` must be a valid map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_Map_key(self_: *const kafka_Map_t, index: i32) -> *mut c_void {
    // SAFETY: per this function's `# Safety`, `self_` is a valid map handle. The borrow
    // lasts for this call only, and the one mutator, `kafka_Map_put`, requires that no
    // other call uses the map meanwhile, so nothing writes to it during the borrow.
    let inner = unsafe { map_ref(self_) };
    usize::try_from(index)
        .ok()
        .and_then(|i| inner.entries.get(i))
        .map_or(ptr::null_mut(), |e| e.0)
}

/// Returns the value of the entry at `index`, or `NULL` when `index` is out of range.
///
/// # Parameters
///
/// - `index` - The zero-based index.
///
/// # Safety
///
/// `self_` must be a valid map handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_Map_value(self_: *const kafka_Map_t, index: i32) -> *mut c_void {
    // SAFETY: per this function's `# Safety`, `self_` is a valid map handle. The borrow
    // lasts for this call only, and the one mutator, `kafka_Map_put`, requires that no
    // other call uses the map meanwhile, so nothing writes to it during the borrow.
    let inner = unsafe { map_ref(self_) };
    usize::try_from(index)
        .ok()
        .and_then(|i| inner.entries.get(i))
        .map_or(ptr::null_mut(), |e| e.1)
}

/// Returns the value associated with `key`, or `NULL` when there is none.
///
/// A map built by Rust compares keys by the documented key type (string keys
/// by content); one built by C compares by pointer identity.
///
/// # Parameters
///
/// - `key` - The key to look up.
///
/// # Safety
///
/// `self_` must be a valid map handle, and `key` null or a key of the type
/// the map documents when Rust built the map: its non-null keys are compared
/// by reading them.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_Map_get(self_: *const kafka_Map_t, key: *mut c_void) -> *mut c_void {
    // SAFETY: per this function's `# Safety`, `self_` is a valid map handle. The borrow
    // lasts for this call only, and the one mutator, `kafka_Map_put`, requires that no
    // other call uses the map meanwhile, so nothing writes to it during the borrow.
    let inner = unsafe { map_ref(self_) };
    // SAFETY: per this function's `# Safety`, `key` is null or of the type the map's
    // `key_eq` compares, when it has one.
    unsafe { inner.position(key) }.map_or(ptr::null_mut(), |i| inner.entries[i].1)
}

/// Frees the map, and its keys and values when the map owns them.
///
/// # Safety
///
/// `self_` must be null or a valid map handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_Map_destroy(self_: *mut kafka_Map_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`,
        // a map handle not yet destroyed, that is the `Box::into_raw` of `box_map`, so
        // `from_raw` reclaims that allocation, once.
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
        // SAFETY: Test code: null is accepted by `kafka_string_destroy`; `owned` is the fresh
        // `into_c_string` string, read through `CStr::from_ptr` while alive and freed once.
        unsafe {
            kafka_string_destroy(ptr::null_mut());
            let owned = into_c_string("hello");
            assert_eq!(CStr::from_ptr(owned).to_str().unwrap(), "hello");
            kafka_string_destroy(owned);
        }
    }

    #[test]
    fn bytes_destroy_accepts_null_and_owned_arrays() {
        // SAFETY: Test code: null is accepted by `kafka_Bytes_destroy`; `owned` and `empty`
        // are fresh `box_bytes` arrays, read while alive (the view points into the boxed
        // `Bytes`) and each destroyed once.
        unsafe {
            kafka_Bytes_destroy(ptr::null_mut());
            let owned = box_bytes(bytes::Bytes::from_static(b"abc"));
            assert_eq!((*owned).as_slice(), Some(&b"abc"[..]));
            kafka_Bytes_destroy(owned);
            // An empty array is not Java's null.
            let empty = box_bytes(bytes::Bytes::new());
            assert!(!(*empty).data.is_null());
            assert_eq!((*empty).len, 0);
            kafka_Bytes_destroy(empty);
        }
    }

    #[test]
    fn generic_value_null_is_java_null() {
        assert!(GenericValue::new(ptr::null_mut()).is_null());
        let mut x = 1_i32;
        let value = GenericValue::new(&raw mut x as *mut c_void);
        assert!(!value.is_null());
        assert_eq!(value.as_ptr(), &raw mut x as *mut c_void);
    }

    #[test]
    fn owned_c_string_truncates_at_an_interior_nul() {
        assert_eq!(owned_c_string("ab\0cd").as_bytes(), b"ab");
        assert_eq!(owned_c_string("").as_bytes(), b"");
    }

    #[test]
    fn c_str_helpers_read_null_as_empty_or_none() {
        let s = c("topic");
        // SAFETY: Test code: each call passes null or `s`, a live NUL-terminated `CString`.
        unsafe {
            assert_eq!(c_str_to_string(ptr::null()), "");
            assert_eq!(c_str_to_option(ptr::null()), None);
            assert_eq!(c_str_to_string(s.as_ptr()), "topic");
            assert_eq!(c_str_to_option(s.as_ptr()), Some("topic".to_string()));
        }
    }

    #[test]
    fn bytes_view_distinguishes_null_from_empty() {
        let null = kafka_Bytes_t::from_option(None);
        assert!(null.data.is_null());
        assert_eq!(null.len, 0);
        // SAFETY: Test code: a null view is always accepted.
        assert_eq!(unsafe { null.as_slice() }, None);

        let empty: &[u8] = &[];
        let view = kafka_Bytes_t::from_slice(empty);
        assert!(!view.data.is_null());
        assert_eq!(view.len, 0);
        // SAFETY: Test code: `view` points into `empty`, a `'static` slice.
        assert_eq!(unsafe { view.as_slice() }, Some(empty));

        let bytes = b"abc";
        let view = kafka_Bytes_t::from_option(Some(bytes));
        assert_eq!(view.len, 3);
        // SAFETY: Test code: `view` points into `bytes`, a `'static` array of `len` bytes.
        assert_eq!(unsafe { view.as_slice() }, Some(&bytes[..]));
    }

    #[test]
    fn c_built_list_borrows_its_elements() {
        let a = c("a");
        let b = c("b");
        let list = kafka_List_new();
        // SAFETY: Test code: `list` is the fresh `kafka_List_new` handle, used by this thread
        // only and destroyed once at the end; its elements are `a` and `b`, live CStrings that
        // outlive it.
        unsafe {
            assert_eq!(kafka_List_size(list), 0);
            kafka_List_add(list, a.as_ptr() as *mut c_void);
            kafka_List_add(list, b.as_ptr() as *mut c_void);
            assert_eq!(kafka_List_size(list), 2);
            assert_eq!(kafka_List_get(list, 1), b.as_ptr() as *mut c_void);
            assert!(kafka_List_get(list, 2).is_null());
            assert!(kafka_List_get(list, -1).is_null());
            assert_eq!(list_strings(list), vec!["a".to_string(), "b".to_string()]);
            // Destroying the list leaves `a` and `b` untouched: they are still
            // valid CStrings dropped at the end of the test.
            kafka_List_destroy(list);
        }
        assert_eq!(a.to_str().unwrap(), "a");
    }

    #[test]
    fn rust_built_string_list_owns_its_elements() {
        let list = box_string_list(["x", "y", "z"]);
        // SAFETY: Test code: `list` is the fresh `box_string_list` handle, destroyed once at
        // the end; `second` is one of its owned strings, read before that destroy. Null is
        // accepted by `kafka_List_destroy`, `list_strings` and `list_elements`.
        unsafe {
            assert_eq!(kafka_List_size(list), 3);
            let second = kafka_List_get(list, 1) as *const c_char;
            assert_eq!(CStr::from_ptr(second).to_str().unwrap(), "y");
            // Frees the three strings along with the list (checked by Miri /
            // sanitizers rather than by an assertion: there is nothing to observe).
            kafka_List_destroy(list);
            kafka_List_destroy(ptr::null_mut());
            assert!(list_strings(ptr::null()).is_empty());
            assert!(list_elements(ptr::null()).is_empty());
        }
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
        // SAFETY: Test code: `list` is the fresh `box_list` handle, destroyed once; its
        // elements are `Box<Counted>`s, which `destroy_boxed::<Counted>` frees.
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
        // SAFETY: Test code: `map` is the fresh `kafka_Map_new` handle, used by this thread
        // only and destroyed once at the end; it compares keys by identity, so any key
        // pointer is accepted, and its entries are live CStrings that outlive it. Null is
        // accepted by `kafka_Map_destroy` and `map_entries`.
        unsafe {
            kafka_Map_put(map, k1.as_ptr() as *mut c_void, v1.as_ptr() as *mut c_void);
            kafka_Map_put(map, k2.as_ptr() as *mut c_void, v2.as_ptr() as *mut c_void);
            // Two distinct pointers with the same text are two keys to a C-built map.
            assert_eq!(kafka_Map_size(map), 2);
            kafka_Map_put(map, k1.as_ptr() as *mut c_void, v2.as_ptr() as *mut c_void);
            assert_eq!(kafka_Map_size(map), 2);
            assert_eq!(kafka_Map_get(map, k1.as_ptr() as *mut c_void), v2.as_ptr() as *mut c_void);
            assert_eq!(kafka_Map_key(map, 1), k2.as_ptr() as *mut c_void);
            assert_eq!(kafka_Map_value(map, 0), v2.as_ptr() as *mut c_void);
            assert!(kafka_Map_key(map, 2).is_null());
            assert!(kafka_Map_value(map, -1).is_null());
            assert!(kafka_Map_get(map, v1.as_ptr() as *mut c_void).is_null());
            assert_eq!(map_entries(map).len(), 2);
            kafka_Map_destroy(map);
            kafka_Map_destroy(ptr::null_mut());
            assert!(map_entries(ptr::null()).is_empty());
        }
    }

    #[test]
    fn rust_built_string_keyed_map_compares_keys_by_content() {
        let values: Vec<*mut c_void> = (1..=2).map(|i| Box::into_raw(Box::new(i as i64)) as *mut c_void).collect();
        let map = box_string_keyed_map([("alpha", values[0]), ("beta", values[1])], Some(destroy_boxed::<i64>));
        let probe = c("beta");
        let missing = c("gamma");
        // SAFETY: Test code: `map` is the fresh `box_string_keyed_map` handle, destroyed once
        // at the end; every probe key is a live NUL-terminated CString, the key type it
        // compares. `found` and `key` point at its own entries, read before that destroy.
        unsafe {
            assert_eq!(kafka_Map_size(map), 2);
            let found = kafka_Map_get(map, probe.as_ptr() as *mut c_void) as *const i64;
            assert_eq!(*found, 2);
            let key = kafka_Map_key(map, 0) as *const c_char;
            assert_eq!(CStr::from_ptr(key).to_str().unwrap(), "alpha");
            assert!(kafka_Map_get(map, missing.as_ptr() as *mut c_void).is_null());
            kafka_Map_destroy(map);
        }
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
        // SAFETY: Test code: `map` is the fresh `box_string_keyed_map` handle, used by this
        // thread only; `key` is a live NUL-terminated CString equal to a stored key, so the
        // map does not keep it, and `second` is a `Box<Counted>`, the value type its
        // `destroy_boxed::<Counted>` frees, which the map now owns.
        unsafe { kafka_Map_put(map, key.as_ptr() as *mut c_void, second) };
        assert_eq!(
            DROPS.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the replaced value is freed"
        );
        // SAFETY: Test code: `map` is still the live handle above, destroyed here, once.
        unsafe {
            assert_eq!(kafka_Map_size(map), 1);
            kafka_Map_destroy(map);
        }
        assert_eq!(
            DROPS.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "the remaining value is freed"
        );
    }
}
