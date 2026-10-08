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

//! `kafka_common_TopicCollection_t`: `org.apache.kafka.common.TopicCollection`
//! (CLAUDE.md §4).
//!
//! Java's abstract class has two subclasses built by the static factories
//! `ofTopicIds` / `ofTopicNames`; the Rust enum's two data-carrying variants
//! follow the enum rule for variants with data (§4, "Enums"): each is built
//! by its factory and returned owned, and the per-variant constructors
//! (`_topic_ids`, `_topic_names`) build the same values under the variant
//! names, beside `kafka_common_TopicCollection_e` and `__enum` for a
//! `switch`.

#![expect(non_camel_case_types)]

use crate::common::TopicCollection;
use crate::ffi::common::uuid::list_uuids;
use crate::ffi::util::{kafka_List_t, list_strings};

/// Opaque handle to a [`TopicCollection`].
#[repr(C)]
pub struct kafka_common_TopicCollection_t {
    _private: [u8; 0],
}

/// The variants of [`TopicCollection`], for a `switch` over
/// [`kafka_common_TopicCollection__enum`].
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_TopicCollection_e {
    /// `TopicCollection::TopicIds`: Java's `TopicIdCollection`.
    topic_ids,
    /// `TopicCollection::TopicNames`: Java's `TopicNameCollection`.
    topic_names,
}

/// Which variant `self_` holds.
///
/// # Safety
///
/// `self_` must be a valid topic-collection handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicCollection__enum(
    self_: *const kafka_common_TopicCollection_t,
) -> kafka_common_TopicCollection_e {
    match unsafe { topic_collection_ref(self_) } {
        TopicCollection::TopicIds(_) => kafka_common_TopicCollection_e::topic_ids,
        TopicCollection::TopicNames(_) => kafka_common_TopicCollection_e::topic_names,
    }
}

/// `TopicCollection::TopicIds(value)`: the variant constructor, equal to
/// [`kafka_common_TopicCollection_of_topic_ids`]. `value` holds
/// `const kafka_common_Uuid_t *` elements, copied. Owned, freed with
/// [`kafka_common_TopicCollection_destroy`].
///
/// # Safety
///
/// `value` must be null or a valid list of uuid handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicCollection_topic_ids(
    value: *const kafka_List_t,
) -> *mut kafka_common_TopicCollection_t {
    boxed(TopicCollection::TopicIds(unsafe { list_uuids(value) }))
}

/// `TopicCollection::TopicNames(value)`: the variant constructor, equal to
/// [`kafka_common_TopicCollection_of_topic_names`]. `value` holds
/// `const char *` elements, copied.
///
/// # Safety
///
/// `value` must be null or a valid list of NUL-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicCollection_topic_names(
    value: *const kafka_List_t,
) -> *mut kafka_common_TopicCollection_t {
    boxed(TopicCollection::TopicNames(unsafe { list_strings(value) }))
}

/// The collection behind a handle.
///
/// # Safety
///
/// `topics` must be a valid topic-collection handle.
pub(crate) unsafe fn topic_collection_ref<'a>(topics: *const kafka_common_TopicCollection_t) -> &'a TopicCollection {
    unsafe { &*(topics as *const TopicCollection) }
}

fn boxed(topics: TopicCollection) -> *mut kafka_common_TopicCollection_t {
    Box::into_raw(Box::new(topics)) as *mut kafka_common_TopicCollection_t
}

/// `TopicCollection.ofTopicIds(Collection<Uuid>)`: `topics` holds
/// `const kafka_common_Uuid_t *` elements, copied. Owned, freed with
/// [`kafka_common_TopicCollection_destroy`].
///
/// # Safety
///
/// `topics` must be null or a valid list of uuid handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicCollection_of_topic_ids(
    topics: *const kafka_List_t,
) -> *mut kafka_common_TopicCollection_t {
    boxed(TopicCollection::of_topic_ids(unsafe { list_uuids(topics) }))
}

/// `TopicCollection.ofTopicNames(Collection<String>)`: `topics` holds
/// `const char *` elements, copied.
///
/// # Safety
///
/// `topics` must be null or a valid list of NUL-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicCollection_of_topic_names(
    topics: *const kafka_List_t,
) -> *mut kafka_common_TopicCollection_t {
    boxed(TopicCollection::of_topic_names(unsafe { list_strings(topics) }))
}

/// Frees an owned topic-collection handle. Null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicCollection_destroy(self_: *mut kafka_common_TopicCollection_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut TopicCollection) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CString, c_void};
    use std::ptr;

    use super::*;
    use crate::common::Uuid;
    use crate::ffi::common::uuid::uuid_list;
    use crate::ffi::util::{kafka_List_add, kafka_List_destroy, kafka_List_new};

    #[test]
    fn factories_copy_their_lists() {
        let name = CString::new("orders").unwrap();
        unsafe {
            let names = kafka_List_new();
            kafka_List_add(names, name.as_ptr() as *mut c_void);
            let by_name = kafka_common_TopicCollection_of_topic_names(names);
            kafka_List_destroy(names);
            assert_eq!(
                *topic_collection_ref(by_name),
                TopicCollection::of_topic_names(vec!["orders".to_string()])
            );
            kafka_common_TopicCollection_destroy(by_name);

            let ids = uuid_list([Uuid::new(1, 2)]);
            let by_id = kafka_common_TopicCollection_of_topic_ids(ids);
            kafka_List_destroy(ids);
            assert_eq!(
                *topic_collection_ref(by_id),
                TopicCollection::of_topic_ids(vec![Uuid::new(1, 2)])
            );
            kafka_common_TopicCollection_destroy(by_id);
            kafka_common_TopicCollection_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn variant_constructors_build_the_same_values_and_enum_tells_them_apart() {
        let name = CString::new("orders").unwrap();
        unsafe {
            let names = kafka_List_new();
            kafka_List_add(names, name.as_ptr() as *mut c_void);
            let by_name = kafka_common_TopicCollection_topic_names(names);
            kafka_List_destroy(names);
            assert_eq!(
                kafka_common_TopicCollection__enum(by_name),
                kafka_common_TopicCollection_e::topic_names
            );
            assert_eq!(
                *topic_collection_ref(by_name),
                TopicCollection::of_topic_names(vec!["orders".to_string()])
            );
            kafka_common_TopicCollection_destroy(by_name);

            let ids = uuid_list([Uuid::new(1, 2)]);
            let by_id = kafka_common_TopicCollection_topic_ids(ids);
            kafka_List_destroy(ids);
            assert_eq!(
                kafka_common_TopicCollection__enum(by_id),
                kafka_common_TopicCollection_e::topic_ids
            );
            assert_eq!(
                *topic_collection_ref(by_id),
                TopicCollection::of_topic_ids(vec![Uuid::new(1, 2)])
            );
            kafka_common_TopicCollection_destroy(by_id);
        }
    }
}
