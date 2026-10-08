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

//! `kafka_common_Node_t`: `org.apache.kafka.common.Node` (CLAUDE.md §4).
//!
//! The handle owns a [`Node`] plus the NUL-terminated copies of its `host`,
//! `idString()` and `rack` that the borrowed string getters hand out, built
//! once at construction. Every holder of nodes in the FFI (`PartitionInfo`,
//! `Cluster`, the admin descriptions) stores [`NodeInner`]s so the pointers
//! it hands out stay valid for its own lifetime.

use std::ffi::{CString, c_char, c_void};
use std::ptr;
use std::sync::LazyLock;

use crate::common::Node;
use crate::ffi::util::{
    box_list, c_str_to_option, c_str_to_string, destroy_boxed, into_c_string, kafka_List_t, list_elements,
    owned_c_string,
};

/// Opaque handle to a [`Node`] (broker).
#[repr(C)]
pub struct kafka_common_Node_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_Node_t`] points at: the value plus the
/// NUL-terminated strings its getters borrow out.
pub(crate) struct NodeInner {
    node: Node,
    host_c: CString,
    id_string_c: CString,
    rack_c: Option<CString>,
}

impl NodeInner {
    pub(crate) fn new(node: Node) -> Self {
        let host_c = owned_c_string(node.host());
        let id_string_c = owned_c_string(node.id_string());
        let rack_c = node.rack().map(owned_c_string);
        Self { node, host_c, id_string_c, rack_c }
    }

    pub(crate) fn node(&self) -> &Node {
        &self.node
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_Node_t {
        self as *const Self as *const kafka_common_Node_t
    }
}

/// `Node.noNode()`: the one instance standing for "no node", never freed.
static NO_NODE: LazyLock<NodeInner> = LazyLock::new(|| NodeInner::new(Node::no_node().clone()));

/// Hands `node` to C as an owned handle, freed with
/// [`kafka_common_Node_destroy`].
pub(crate) fn box_node(node: Node) -> *mut kafka_common_Node_t {
    Box::into_raw(Box::new(NodeInner::new(node))) as *mut kafka_common_Node_t
}

/// The node behind a handle.
///
/// # Safety
///
/// `node` must be a valid node handle.
pub(crate) unsafe fn node_ref<'a>(node: *const kafka_common_Node_t) -> &'a Node {
    unsafe { &*(node as *const NodeInner) }.node()
}

/// A copy of the node behind a nullable handle: null is Java's `null`.
///
/// # Safety
///
/// `node` must be null or a valid node handle.
pub(crate) unsafe fn optional_node(node: *const kafka_common_Node_t) -> Option<Node> {
    if node.is_null() {
        None
    } else {
        Some(unsafe { node_ref(node) }.clone())
    }
}

/// A borrowed handle on an optional cached node, null for `None`.
pub(crate) fn optional_node_ptr(node: Option<&NodeInner>) -> *const kafka_common_Node_t {
    node.map_or(ptr::null(), NodeInner::as_ptr)
}

/// Hands copies of `nodes` to C as an owned list of `kafka_common_Node_t *`.
pub(crate) fn node_list(nodes: &[Node]) -> *mut kafka_List_t {
    let elements = nodes.iter().map(|node| box_node(node.clone()) as *mut c_void).collect();
    box_list(elements, Some(destroy_boxed::<NodeInner>))
}

/// Reads a list of `const kafka_common_Node_t *` into owned copies; null
/// reads as empty.
///
/// # Safety
///
/// `list` must be null or a valid list whose elements are node handles.
pub(crate) unsafe fn list_nodes(list: *const kafka_List_t) -> Vec<Node> {
    unsafe { list_elements(list) }
        .iter()
        .map(|&element| unsafe { node_ref(element as *const kafka_common_Node_t) }.clone())
        .collect()
}

/// `new Node(int id, String host, int port)`, as an owned handle freed with
/// [`kafka_common_Node_destroy`].
///
/// # Safety
///
/// `host` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_new(id: i32, host: *const c_char, port: i32) -> *mut kafka_common_Node_t {
    box_node(Node::new(id, unsafe { c_str_to_string(host) }, port))
}

/// `new Node(int id, String host, int port, String rack)`; a null `rack` is
/// Java's `null`.
///
/// # Safety
///
/// `host` must be a valid NUL-terminated string and `rack` null or one.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_with_rack(
    id: i32,
    host: *const c_char,
    port: i32,
    rack: *const c_char,
) -> *mut kafka_common_Node_t {
    box_node(Node::with_rack(id, unsafe { c_str_to_string(host) }, port, unsafe {
        c_str_to_option(rack)
    }))
}

/// `new Node(int id, String host, int port, String rack, boolean isFenced)`.
///
/// # Safety
///
/// `host` must be a valid NUL-terminated string and `rack` null or one.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_with_rack_is_fenced(
    id: i32,
    host: *const c_char,
    port: i32,
    rack: *const c_char,
    is_fenced: i8,
) -> *mut kafka_common_Node_t {
    box_node(Node::with_rack_is_fenced(
        id,
        unsafe { c_str_to_string(host) },
        port,
        unsafe { c_str_to_option(rack) },
        is_fenced != 0,
    ))
}

/// `Node.noNode()`: a borrowed handle on the shared "no node" instance,
/// never destroyed.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_Node_no_node() -> *const kafka_common_Node_t {
    NO_NODE.as_ptr()
}

/// `isEmpty()`: whether this is `noNode()` (id `-1`, no host or port).
///
/// # Safety
///
/// `self_` must be a valid node handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_is_empty(self_: *const kafka_common_Node_t) -> i8 {
    i8::from(unsafe { node_ref(self_) }.is_empty())
}

/// `id()`.
///
/// # Safety
///
/// `self_` must be a valid node handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_id(self_: *const kafka_common_Node_t) -> i32 {
    unsafe { node_ref(self_) }.id()
}

/// `idString()`: the id as text, borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid node handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_id_string(self_: *const kafka_common_Node_t) -> *const c_char {
    unsafe { &*(self_ as *const NodeInner) }.id_string_c.as_ptr()
}

/// `host()`: the host name, borrowed from the handle and valid until it is
/// destroyed.
///
/// # Safety
///
/// `self_` must be a valid node handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_host(self_: *const kafka_common_Node_t) -> *const c_char {
    unsafe { &*(self_ as *const NodeInner) }.host_c.as_ptr()
}

/// `port()`.
///
/// # Safety
///
/// `self_` must be a valid node handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_port(self_: *const kafka_common_Node_t) -> i32 {
    unsafe { node_ref(self_) }.port()
}

/// `hasRack()`.
///
/// # Safety
///
/// `self_` must be a valid node handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_has_rack(self_: *const kafka_common_Node_t) -> i8 {
    i8::from(unsafe { node_ref(self_) }.has_rack())
}

/// `rack()`: the rack, borrowed from the handle, or null when the node has
/// none (Java's `null`).
///
/// # Safety
///
/// `self_` must be a valid node handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_rack(self_: *const kafka_common_Node_t) -> *const c_char {
    unsafe { &*(self_ as *const NodeInner) }
        .rack_c
        .as_ref()
        .map_or(ptr::null(), |rack| rack.as_ptr())
}

/// `isFenced()`.
///
/// # Safety
///
/// `self_` must be a valid node handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_is_fenced(self_: *const kafka_common_Node_t) -> i8 {
    i8::from(unsafe { node_ref(self_) }.is_fenced())
}

/// `toString()`: `host:port (id: idString rack: rack)`, as an owned string
/// freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid node handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_to_string(self_: *const kafka_common_Node_t) -> *mut c_char {
    into_c_string(&unsafe { node_ref(self_) }.to_string())
}

/// Frees an owned node handle. Null is a no-op; the `noNode()` singleton and
/// the nodes borrowed from another handle are never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned node handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Node_destroy(self_: *mut kafka_common_Node_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut NodeInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::ffi::util::{kafka_List_destroy, kafka_List_size, kafka_string_destroy};

    #[test]
    fn getters_borrow_cached_strings_and_rack_is_nullable() {
        let host = CString::new("broker-1").unwrap();
        let rack = CString::new("rack-a").unwrap();
        unsafe {
            let plain = kafka_common_Node_new(1, host.as_ptr(), 9092);
            assert_eq!(kafka_common_Node_id(plain), 1);
            assert_eq!(CStr::from_ptr(kafka_common_Node_id_string(plain)).to_str().unwrap(), "1");
            assert_eq!(CStr::from_ptr(kafka_common_Node_host(plain)).to_str().unwrap(), "broker-1");
            assert_eq!(kafka_common_Node_port(plain), 9092);
            assert_eq!(kafka_common_Node_has_rack(plain), 0);
            assert!(kafka_common_Node_rack(plain).is_null());
            assert_eq!(kafka_common_Node_is_fenced(plain), 0);
            assert_eq!(kafka_common_Node_is_empty(plain), 0);
            kafka_common_Node_destroy(plain);

            let racked = kafka_common_Node_with_rack_is_fenced(2, host.as_ptr(), 9093, rack.as_ptr(), 1);
            assert_eq!(kafka_common_Node_has_rack(racked), 1);
            assert_eq!(CStr::from_ptr(kafka_common_Node_rack(racked)).to_str().unwrap(), "rack-a");
            assert_eq!(kafka_common_Node_is_fenced(racked), 1);
            let s = kafka_common_Node_to_string(racked);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                Node::with_rack_is_fenced(2, "broker-1".to_string(), 9093, Some("rack-a".to_string()), true)
                    .to_string()
            );
            kafka_string_destroy(s);
            kafka_common_Node_destroy(racked);
            kafka_common_Node_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn no_node_is_a_shared_empty_singleton() {
        let a = kafka_common_Node_no_node();
        let b = kafka_common_Node_no_node();
        assert_eq!(a, b);
        unsafe {
            assert_eq!(kafka_common_Node_is_empty(a), 1);
            assert_eq!(kafka_common_Node_id(a), -1);
            assert!(optional_node(ptr::null()).is_none());
            assert_eq!(optional_node(a).as_ref(), Some(Node::no_node()));
        }
    }

    #[test]
    fn node_list_round_trips_owned_copies() {
        let nodes = [
            Node::new(1, "h1".to_string(), 1),
            Node::with_rack(2, "h2".to_string(), 2, Some("r".into())),
        ];
        let list = node_list(&nodes);
        unsafe {
            assert_eq!(kafka_List_size(list), 2);
            assert_eq!(list_nodes(list), nodes.to_vec());
            assert!(list_nodes(ptr::null()).is_empty());
            kafka_List_destroy(list);
        }
    }
}
