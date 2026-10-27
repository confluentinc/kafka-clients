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

//! Java `org.apache.kafka.common.utils.Utils`.

/// Translates the Java static-utility class
/// `org.apache.kafka.common.utils.Utils`,
/// which has no instance state, so it becomes a unit struct hosting its
/// statics as associated items.
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.common.utils.Utils")]
pub struct Utils;

impl Utils {
    /// Feeds `state` one value that does not depend on the iteration order of
    /// `items`: the wrapping sum of each item's own hash, as Java's
    /// `AbstractSet.hashCode()` sums its elements' hashes and
    /// `AbstractMap.hashCode()` its entries'. A map passes its `(key, value)`
    /// pairs.
    ///
    /// Rust-only (DoD #7): `HashMap`, `HashSet` and `IndexSet` do not implement
    /// `Hash`, because two equal ones may iterate in different orders, so a
    /// public type holding one and translating a Java `hashCode()` hashes it
    /// through this. Each item is hashed with a fresh `DefaultHasher`, whose
    /// keys are fixed, so equal collections give equal sums.
    pub(crate) fn hash_unordered<T: std::hash::Hash>(
        items: impl IntoIterator<Item = T>,
        state: &mut impl std::hash::Hasher,
    ) {
        use std::hash::Hasher as _;
        let mut sum: u64 = 0;
        for item in items {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            item.hash(&mut hasher);
            sum = sum.wrapping_add(hasher.finish());
        }
        state.write_u64(sum);
    }

    /// Converts a signed i32 to a non-negative value by clearing the sign bit.
    ///
    /// Translated from `org.apache.kafka.common.utils.Utils.toPositive`.
    #[inline]
    #[doc(alias = "org.apache.kafka.common.utils.Utils#toPositive")]
    pub fn to_positive(number: i32) -> i32 {
        number & 0x7fff_ffff
    }

    /// Whether `string` is `None`, empty, or made only of whitespace.
    ///
    /// Translated from `org.apache.kafka.common.utils.Utils.isBlank`
    /// (`str == null || str.trim().isEmpty()`). Java's `String.trim` strips every
    /// leading and trailing character up to U+0020, control characters included,
    /// so that is the set trimmed here rather than Rust's Unicode whitespace.
    #[doc(alias = "org.apache.kafka.common.utils.Utils#isBlank")]
    pub fn is_blank(string: Option<&str>) -> bool {
        string.is_none_or(|s| s.trim_matches(|c: char| c <= ' ').is_empty())
    }

    /// Packs a set of bit indices (`0..=31`) into a 32-bit field, setting bit `b`
    /// for each byte `b` in the set.
    ///
    /// Translated from `org.apache.kafka.common.utils.Utils.to32BitField`.
    ///
    /// # Panics
    ///
    /// Panics if any bit index is out of the `0..=31` range, matching Java's
    /// `IllegalArgumentException` (a programming error, per CLAUDE.md §12.1).
    pub fn to_32_bit_field(bytes: &std::collections::HashSet<i8>) -> i32 {
        let mut value: i32 = 0;
        for &b in bytes {
            assert!(b <= 31, "out of range: i>31, i = {b}");
            assert!(b >= 0, "out of range: i<0, i = {b}");
            value |= 1 << b;
        }
        value
    }

    /// Unpacks a 32-bit field into the set of bit indices that are set.
    ///
    /// Translated from `org.apache.kafka.common.utils.Utils.from32BitField`.
    pub fn from_32_bit_field(int_value: i32) -> std::collections::HashSet<i8> {
        let mut result = std::collections::HashSet::new();
        let mut itr = int_value as u32;
        let mut count: i8 = 0;
        while itr != 0 {
            if (itr & 1) != 0 {
                result.insert(count);
            }
            count += 1;
            itr >>= 1;
        }
        result
    }

    /// Generates a 32-bit murmur2 hash from a byte slice.
    ///
    /// This is a Kafka-specific implementation that must produce identical output
    /// to the Java `org.apache.kafka.common.utils.Utils.murmur2` function, because
    /// the hash is used for deterministic key-based partitioning.
    ///
    /// Translated from `org.apache.kafka.common.utils.Utils.murmur2`.
    #[doc(alias = "org.apache.kafka.common.utils.Utils#murmur2")]
    pub fn murmur2(data: &[u8]) -> i32 {
        let length = data.len() as i32;
        let seed: i32 = -0x68b84d74_i32; // 0x9747b28c as signed i32
        let m: i32 = 0x5bd1_e995;
        let r: i32 = 24;

        // Initialize the hash to a random value
        let mut h: i32 = seed ^ length;
        let length4 = (length >> 2) as usize;

        for i in 0..length4 {
            let i4 = i * 4;
            let k_bytes = [data[i4], data[i4 + 1], data[i4 + 2], data[i4 + 3]];
            let mut k = i32::from_le_bytes(k_bytes);
            k = k.wrapping_mul(m);
            k ^= (k as u32 >> r) as i32;
            k = k.wrapping_mul(m);
            h = h.wrapping_mul(m);
            h ^= k;
        }

        // Handle the last few bytes of the input array
        let index = length4 * 4;
        let remaining = data.len() - index;
        if remaining >= 3 {
            h ^= (data[index + 2] as i32 & 0xff) << 16;
        }
        if remaining >= 2 {
            h ^= (data[index + 1] as i32 & 0xff) << 8;
        }
        if remaining >= 1 {
            h ^= data[index] as i32 & 0xff;
            h = h.wrapping_mul(m);
        }

        h ^= (h as u32 >> 13) as i32;
        h = h.wrapping_mul(m);
        h ^= (h as u32 >> 15) as i32;

        h
    }

    /// Runs an async `close` from synchronous code, for the clients' failed
    /// constructors: Java's constructor `catch` closes what it already built
    /// with `Utils#closeQuietly(AutoCloseable,String,AtomicReference)`, but a Rust constructor is
    /// synchronous while a serde's `close` is async. `close` logs its own
    /// failures, as `closeQuietly` does.
    ///
    /// Rust-only (DoD #7). The close is polled once on the calling task, which
    /// completes it for any closeable that does not wait. One that does wait
    /// (an asynchronous C serde whose `close` reports later, maybe from the
    /// very thread that called the constructor) cannot be waited for here, so
    /// it is finished on a task of its own, on a thread with its own
    /// runtime: not on the ambient runtime, which may be the failed client's
    /// own and dropped with it, cancelling the close before the value serde's
    /// `close` was even called. A close that never completes keeps that thread
    /// parked.
    pub(crate) fn close_quietly_from_sync(
        close: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>,
        name: &str,
    ) {
        use futures_util::FutureExt;

        let mut close = close;
        if close.as_mut().now_or_never().is_some() {
            return;
        }
        let closed = name.to_string();
        let finish = move || match tokio::runtime::Builder::new_current_thread().enable_all().build() {
            Ok(runtime) => runtime.block_on(close),
            Err(error) => log::error!("Failed to close {closed}: no runtime can finish the pending close: {error}"),
        };
        if let Err(error) = std::thread::Builder::new().name(format!("{name} close")).spawn(finish) {
            log::error!("Failed to close {name}: the close is pending and no task can finish it: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from `UtilsTest.testIsBlank`, plus the characters Java's `trim`
    /// strips that Rust's Unicode-whitespace `trim` treats differently.
    #[test]
    #[doc(alias = "org.apache.kafka.common.utils.UtilsTest#testIsBlank")]
    fn test_is_blank() {
        assert!(Utils::is_blank(None));
        assert!(Utils::is_blank(Some("")));
        assert!(Utils::is_blank(Some(" ")));
        assert!(!Utils::is_blank(Some("bob")));
        assert!(!Utils::is_blank(Some(" bob ")));

        // Java's `trim` strips every char <= U+0020 (here NUL and US) ...
        assert!(Utils::is_blank(Some("\u{0}\t\n\u{1f}")));
        // ... and not U+00A0 / U+3000, which Rust's `trim` would strip.
        assert!(!Utils::is_blank(Some("\u{a0}")));
        assert!(!Utils::is_blank(Some("\u{3000}")));
    }

    /// A close that is done at its first poll runs on the calling thread.
    #[test]
    fn close_quietly_from_sync_completes_a_ready_close_inline() {
        let closed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = std::sync::Arc::clone(&closed);
        Utils::close_quietly_from_sync(
            Box::pin(async move { flag.store(true, std::sync::atomic::Ordering::SeqCst) }),
            "test",
        );
        assert!(closed.load(std::sync::atomic::Ordering::SeqCst));
    }

    /// A close still pending at its first poll is finished on a task of its
    /// own after the call returned, whether or not a runtime is ambient, and
    /// whether or not the ambient one outlives the call.
    #[test]
    fn close_quietly_from_sync_finishes_a_pending_close_on_its_own_task() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        {
            let _enter = runtime.enter();
            let done_tx = done_tx.clone();
            Utils::close_quietly_from_sync(
                Box::pin(async move {
                    release_rx.await.unwrap();
                    // A timer works: the task has a runtime of its own.
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    done_tx.send("with a runtime").unwrap();
                }),
                "test",
            );
        }
        // The ambient runtime goes away, as a failed FFI client's does.
        drop(runtime);
        assert!(done_rx.try_recv().is_err(), "the close is still pending");
        release_tx.send(()).unwrap();
        assert_eq!(Ok("with a runtime"), done_rx.recv_timeout(std::time::Duration::from_secs(10)));

        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        Utils::close_quietly_from_sync(
            Box::pin(async move {
                release_rx.await.unwrap();
                done_tx.send("without a runtime").unwrap();
            }),
            "test",
        );
        release_tx.send(()).unwrap();
        assert_eq!(
            Ok("without a runtime"),
            done_rx.recv_timeout(std::time::Duration::from_secs(10))
        );
    }

    #[test]
    fn test_to_positive() {
        assert_eq!(0, Utils::to_positive(0));
        assert_eq!(1, Utils::to_positive(1));
        assert_eq!(0x7fff_ffff, Utils::to_positive(-1));
        assert_eq!(0, Utils::to_positive(i32::MIN));
        assert_eq!(i32::MAX, Utils::to_positive(i32::MAX));
    }

    /// Known murmur2 vectors, translated verbatim from the Java client's
    /// `UtilsTest.testMurmur2`
    /// (`kafka/clients/src/test/java/org/apache/kafka/common/utils/UtilsTest.java`).
    ///
    /// These pin the EXACT 32-bit output for known inputs, not merely that the
    /// hash equals itself. murmur2 is a wire-visible partitioning contract: a
    /// consistently wrong implementation round-trips against itself yet routes
    /// keyed records to different partitions than the Java client, so
    /// `Murmur2RandomPartitioner`'s promise of exact Java parity would silently
    /// break. The inputs are 2, 6, 24, 26, 48, and 3 bytes long (tail remainders
    /// 2, 2, 0, 2, 0, 3), so the trailing-byte handling is exercised for
    /// remainders 0, 2, and 3.
    #[test]
    #[doc(alias = "org.apache.kafka.common.utils.UtilsTest#testMurmur2")]
    fn test_murmur2() {
        // `(input, expected)` exactly as Java's `cases` map.
        let cases: &[(&[u8], i32)] = &[
            (b"21", -973932308),
            (b"foobar", -790332482),
            (b"a-little-bit-long-string", -985981536),
            (b"a-little-bit-longer-string", -1486304829),
            (b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58897971),
            // Java's `new byte[]{'a', 'b', 'c'}`; `b"abc"` is the identical `[97, 98, 99]`.
            (b"abc", 479470107),
        ];
        for (input, expected) in cases {
            assert_eq!(*expected, Utils::murmur2(input), "murmur2 mismatch for {input:?}");
        }
    }

    // NOTE: Java's `UtilsTest.testMurmur2Checksum` (a 100-trial x length-0..=1000
    // fuzz checksum asserting `0xc3b8cf7c99fcL`) is intentionally NOT translated
    // here. It is faithful only if the random byte stream is byte-for-byte
    // identical to Java's, which requires porting `java.util.SplittableRandom`
    // (seed 0). That is an OpenJDK translation gated on CLAUDE.md §1.3
    // ("ask before doing it" + keep the GPL+Classpath-Exception licence) and is
    // outside this change's approved scope. The known-vector `test_murmur2`
    // above plus `test_murmur2_consistency` cover the implementation; the
    // checksum test can be added when `SplittableRandom` is ported.

    #[test]
    fn test_murmur2_consistency() {
        // Ensure the hash is deterministic
        for i in 0..100 {
            let data = format!("test-key-{}", i);
            let h1 = Utils::murmur2(data.as_bytes());
            let h2 = Utils::murmur2(data.as_bytes());
            assert_eq!(h1, h2, "murmur2 not deterministic for {}", data);
        }
    }

    #[test]
    fn test_from_32_bit_field() {
        use std::collections::HashSet;
        assert_eq!(Utils::from_32_bit_field(0), HashSet::new());
        assert_eq!(Utils::from_32_bit_field(1), HashSet::from([0]));
        // bits 0, 3, 8 set -> 1 + 8 + 256 = 265
        assert_eq!(Utils::from_32_bit_field(265), HashSet::from([0, 3, 8]));
        // High bit (31) set: -2147483648 as i32.
        assert_eq!(Utils::from_32_bit_field(i32::MIN), HashSet::from([31]));
    }

    #[test]
    fn test_to_32_bit_field() {
        use std::collections::HashSet;
        assert_eq!(Utils::to_32_bit_field(&HashSet::new()), 0);
        assert_eq!(Utils::to_32_bit_field(&HashSet::from([0])), 1);
        assert_eq!(Utils::to_32_bit_field(&HashSet::from([0, 3, 8])), 265);
        assert_eq!(Utils::to_32_bit_field(&HashSet::from([31])), i32::MIN);
    }

    #[test]
    fn test_32_bit_field_round_trips() {
        for value in [0, 1, 42, 265, i32::MAX, -1] {
            let bits = Utils::from_32_bit_field(value);
            assert_eq!(Utils::to_32_bit_field(&bits), value, "round trip failed for {value}");
        }
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn test_to_32_bit_field_rejects_out_of_range() {
        use std::collections::HashSet;
        let _ = Utils::to_32_bit_field(&HashSet::from([32]));
    }
}
