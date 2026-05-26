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
use std::sync::atomic::{AtomicBool, Ordering};

use super::{Send, TransferableChannel};

/// A cheap, cloneable handle to the completion state of a [`NetworkSend`].
///
/// Mirrors the JVM reference-sharing of `Send` between Java's
/// `KafkaChannel.send` field and the `InFlightRequest.send` field:
/// `NetworkClient.doSend` (NetworkClient.java:608) constructs the `Send`
/// once, then passes the same JVM reference to both the
/// `InFlightRequest` (NetworkClient.java:614) and the selector via
/// `new NetworkSend(destination, send)` (NetworkClient.java:617). Both
/// call sites observe the same `send.completed()` value.
///
/// Rust ownership prevents naive sharing of `Box<dyn Send>` between the
/// two consumers, so we share only what `InFlightRequests.canSendMore`
/// (InFlightRequests.java:99) actually reads: the completion bit. The
/// `NetworkSend` owns the truth; this handle is a read-only observer
/// that the `InFlightRequest` keeps so `can_send_more` can refuse a new
/// request while the prior `NetworkSend` is mid-write.
///
/// Reads are `Ordering::Acquire` and the matching store inside
/// [`NetworkSend::write_to`] is `Ordering::Release` so a `true` here
/// implies all writes that happened before the inner `Send` reported
/// completed are visible to the reader.
#[derive(Clone, Debug)]
pub struct SendCompletion {
    completed: Arc<AtomicBool>,
}

impl SendCompletion {
    /// Returns `true` once the associated `NetworkSend` has finished
    /// writing all of its bytes. Mirrors Java's
    /// `Send.completed()` as observed through `InFlightRequest.send`.
    pub fn completed(&self) -> bool {
        self.completed.load(Ordering::Acquire)
    }
}

/// A [`Send`] tagged with a destination broker id. Mirrors the Java
/// `NetworkSend` wrapper.
///
/// In Java the destination is a `String`; on the producer hot path we
/// keep an `Arc<str>` so per-message clones are O(1) and do not allocate
/// (CLAUDE.md rule 11).
///
/// `NetworkSend` also publishes a cheap, observable completion bit via
/// [`Self::completion_handle`] so `InFlightRequest` can mirror Java's
/// `peekFirst().send.completed()` check in
/// `InFlightRequests.canSendMore` (InFlightRequests.java:99) without
/// physically sharing the underlying `Box<dyn Send>` (which would
/// require either `Arc<Mutex<...>>` on every concrete `Send` impl or
/// pervasive interior mutability — both heavier than the one
/// `AtomicBool` per send required to mirror the actual contract).
pub struct NetworkSend {
    destination_id: Arc<str>,
    send: Box<dyn Send + std::marker::Send>,
    completed: Arc<AtomicBool>,
}

impl NetworkSend {
    /// Construct a new `NetworkSend`. Mirrors
    /// `new NetworkSend(String destinationId, Send send)`.
    pub fn new(destination_id: Arc<str>, send: Box<dyn Send + std::marker::Send>) -> Self {
        // Seed the flag from the inner so a freshly-constructed
        // already-completed send (e.g. zero-byte) is observable as
        // completed from the handle without requiring an intervening
        // `write_to`. Java exposes `send.completed()` directly so the
        // seed value matches.
        let completed = Arc::new(AtomicBool::new(send.completed()));
        NetworkSend { destination_id, send, completed }
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

    /// Return a cheap, cloneable handle to this send's completion bit.
    ///
    /// Used by `NetworkClient::do_send` to populate
    /// `InFlightRequest.send` so `InFlightRequests::can_send_more` can
    /// mirror Java's `peekFirst().send.completed()` check
    /// (InFlightRequests.java:99) — see [`SendCompletion`] for the
    /// rationale.
    pub fn completion_handle(&self) -> SendCompletion {
        SendCompletion { completed: Arc::clone(&self.completed) }
    }
}

impl Send for NetworkSend {
    fn completed(&self) -> bool {
        self.send.completed()
    }

    fn write_to(&mut self, channel: &mut dyn TransferableChannel) -> io::Result<u64> {
        let written = self.send.write_to(channel)?;
        // Publish the completion bit so any `SendCompletion` handle
        // observers (e.g. the `InFlightRequest` paired with this send)
        // see the same value as Java's `Send.completed()` would. Use
        // `Release` so all writes that produced the completion are
        // visible to a corresponding `Acquire` read on the handle.
        if self.send.completed() {
            self.completed.store(true, Ordering::Release);
        }
        Ok(written)
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

    /// The completion handle obtained before any `write_to` must
    /// transition from `false` to `true` after the underlying send
    /// finishes — this is what `InFlightRequests.can_send_more` reads
    /// to mirror Java's `peekFirst().send.completed()` check.
    #[test]
    fn completion_handle_observes_inner_completion() {
        let inner = ByteBufferSend::size_prefixed(Bytes::from_static(b"hi"));
        let dest: Arc<str> = Arc::from("broker-1");
        let mut send = NetworkSend::new(Arc::clone(&dest), Box::new(inner));

        let handle = send.completion_handle();
        assert!(!handle.completed(), "handle reads false before any write");

        let mut chan = MockChannel { sink: Vec::new() };
        send.write_to(&mut chan).expect("write");

        assert!(handle.completed(), "handle reads true after write completes the send");
    }

    /// Multiple `completion_handle()` calls must each observe the
    /// completion via the same underlying flag — `Arc` clones are
    /// shallow and cheap.
    #[test]
    fn multiple_completion_handles_share_state() {
        let inner = ByteBufferSend::size_prefixed(Bytes::from_static(b"hi"));
        let dest: Arc<str> = Arc::from("broker-1");
        let mut send = NetworkSend::new(Arc::clone(&dest), Box::new(inner));

        let h1 = send.completion_handle();
        let h2 = send.completion_handle();
        let h3 = h1.clone();

        let mut chan = MockChannel { sink: Vec::new() };
        send.write_to(&mut chan).expect("write");

        assert!(h1.completed());
        assert!(h2.completed());
        assert!(h3.completed());
    }
}
