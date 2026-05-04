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

//! Translation of `org.apache.kafka.common.network.NetworkSend`.

use std::io;
use std::sync::Arc;

use super::{Send, TransferableChannel};

/// A [`Send`] tagged with a destination broker id. Mirrors the Java
/// `NetworkSend` wrapper.
///
/// In Java the destination is a `String`; on the producer hot path we
/// keep an `Arc<str>` so per-message clones are O(1) and do not allocate
/// (CLAUDE.md rule 11).
pub struct NetworkSend {
    destination_id: Arc<str>,
    send: Box<dyn Send + std::marker::Send>,
}

impl NetworkSend {
    /// Construct a new `NetworkSend`. Mirrors
    /// `new NetworkSend(String destinationId, Send send)`.
    pub fn new(destination_id: Arc<str>, send: Box<dyn Send + std::marker::Send>) -> Self {
        NetworkSend { destination_id, send }
    }

    /// The destination broker id. Mirrors `NetworkSend.destinationId()`.
    pub fn destination_id(&self) -> &str {
        &self.destination_id
    }

    /// The destination broker id as a clone of the underlying `Arc<str>`.
    /// Cheap (atomic refcount bump, no allocation).
    pub fn destination_id_arc(&self) -> Arc<str> {
        Arc::clone(&self.destination_id)
    }

    /// Borrow the wrapped `Send`. Mirrors `NetworkSend.send()`.
    pub fn send(&self) -> &dyn Send {
        self.send.as_ref()
    }
}

impl Send for NetworkSend {
    fn completed(&self) -> bool {
        self.send.completed()
    }

    fn write_to(&mut self, channel: &mut dyn TransferableChannel) -> io::Result<u64> {
        self.send.write_to(channel)
    }

    fn size(&self) -> u64 {
        self.send.size()
    }
}

impl std::fmt::Debug for NetworkSend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkSend")
            .field("destination_id", &&*self.destination_id)
            .field("size", &self.send.size())
            .field("completed", &self.send.completed())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::io::IoSlice;

    use bytes::Bytes;

    use super::*;
    use crate::common::network::ByteBufferSend;

    struct MockChannel {
        sink: Vec<u8>,
    }

    impl TransferableChannel for MockChannel {
        fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
            let mut total = 0;
            for s in bufs {
                self.sink.extend_from_slice(s);
                total += s.len();
            }
            Ok(total)
        }

        fn has_pending_writes(&self) -> bool {
            false
        }
    }

    #[test]
    fn delegates_to_inner_send() {
        let inner = ByteBufferSend::size_prefixed(Bytes::from_static(b"hi"));
        let dest: Arc<str> = Arc::from("broker-1");
        let mut send = NetworkSend::new(Arc::clone(&dest), Box::new(inner));
        assert_eq!(send.destination_id(), "broker-1");
        assert_eq!(send.size(), 4 + 2);
        assert!(!send.completed());

        let mut chan = MockChannel { sink: Vec::new() };
        send.write_to(&mut chan).expect("write");
        assert!(send.completed());
        assert_eq!(chan.sink, vec![0, 0, 0, 2, b'h', b'i']);
    }
}
