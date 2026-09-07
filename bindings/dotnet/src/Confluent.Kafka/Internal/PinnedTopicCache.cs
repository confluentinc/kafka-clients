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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace Confluent.Kafka.Internal;

/// <summary>
/// One permanently-pinned, NUL-terminated UTF-8 buffer <b>per distinct topic name</b>, cached for
/// the producer's lifetime — the third buffer a deferred send has to keep alive (M11/P3.1 §4.1).
/// </summary>
/// <remarks>
/// <para>
/// <b>Why interning rather than a per-record pin.</b> The ABI's <c>ProducerRecord_t.topic</c> is a
/// <c>const char *</c> the core reads (and copies, via <c>to_string_lossy().into_owned()</c>)
/// <em>inside</em> <c>send_batch</c>. While the send was inline that was a call-scoped
/// <c>Utf8Marshal.Pin</c> and there was nothing to think about; once the send is deferred, a
/// call-scoped topic pointer is a use-after-free. The Python anchor solves it with an owned
/// <c>topic_owned</c> malloc <b>per record object</b>
/// (<c>bindings/python/_confluentkafka.c</c>'s <c>ProducerRecordObject</c>); .NET cannot copy the
/// topic per record without adding exactly the per-record allocation CLAUDE.md §12 / ffi §A4 exist
/// to prevent. Interning gives <b>O(distinct topics) permanent pins instead of O(records) transient
/// ones</b>, which also drops the per-record pin count from 3 to ≤ 2.
/// </para>
/// <para>
/// <b>Bounded, and it never evicts</b> (decision D5). The cache holds at most
/// <see cref="MaxInternedTopics"/> entries so an application sending to unbounded distinct topics
/// cannot grow it without limit; beyond the cap <see cref="Rent"/> falls back to a
/// <b>per-record</b> pinned topic buffer that the caller releases (3 pins for those records instead
/// of 2), rather than evicting. Eviction is not an option, not a simplification that was skipped:
/// freeing a pinned buffer that an in-flight accumulator node still points at would be a
/// use-after-free. So the cache is <b>insert-only for the producer's lifetime</b>. The fallback
/// degrades to <em>Python's</em> shape (a per-record topic allocation) rather than failing, which is
/// what makes the cap a safety valve rather than a behavior switch.
/// </para>
/// <para>
/// <b>When the interned pins are freed — and why it is not an explicit teardown call yet.</b> The
/// pointers this hands out are raw and un-ref-counted, so an eager free is safe only at a point
/// where no send can still be holding one. Once the accumulator's teardown handshake lands
/// (M11/P3.1 §3.8 / slice S5 — close the accumulator to new appends, then drain it to empty before
/// anything else is released) the end of teardown is such a point, and the free becomes explicit
/// there. Until then the only such point is <b>unreachability</b>: the cache is reachable from the
/// <see cref="NativeProducer"/>, which is reachable from any thread that could still be inside a
/// send, so the finalizer cannot run while a topic pointer is in use. That is why this type has a
/// finalizer and no <c>Dispose</c> — an eager free wired today would be a use-after-free against a
/// concurrent inline send, and a <c>Dispose</c> nothing calls would be worse than either.
/// </para>
/// </remarks>
internal sealed class PinnedTopicCache
{
    /// <summary>
    /// The interning cap (decision D5). Beyond this many <b>distinct</b> topic names the cache stops
    /// growing and <see cref="Rent"/> returns a caller-released per-record pin instead.
    /// </summary>
    internal const int MaxInternedTopics = 1024;

    // Read on every send: a lock-free, allocation-free TryGetValue is the fast path. Writes happen
    // once per distinct topic and take _internGate.
    private readonly ConcurrentDictionary<string, IntPtr> _interned =
        new ConcurrentDictionary<string, IntPtr>();

    // Guards _pins (and the interned-count increment). Not taken on the read fast path, and not
    // taken at all once the cap is reached.
    private readonly object _internGate = new object();

    // Every interned pin, so the finalizer can release them as a whole. Guarded by _internGate.
    private readonly List<GCHandle> _pins = new List<GCHandle>();

    // The interned entry count, read WITHOUT the lock so a capped cache never contends.
    private int _internedCount;

    /// <summary>Releases every interned pin once the cache becomes unreachable (see the remarks).</summary>
    ~PinnedTopicCache()
    {
        // No lock: finalization means nothing else can reach this instance. GCHandle.Free during
        // finalization is safe — the pinned byte[] is still alive precisely because the handle
        // roots it.
        foreach (GCHandle pin in _pins)
        {
            if (pin.IsAllocated)
            {
                pin.Free();
            }
        }
    }

    /// <summary>The number of topics currently interned (test/diagnostic; never decreases).</summary>
    internal int InternedCount => Volatile.Read(ref _internedCount);

    /// <summary>
    /// Returns a pinned, NUL-terminated UTF-8 pointer for <paramref name="topic"/>, valid until the
    /// returned <see cref="TopicPin"/> is released (fallback) or for the producer's lifetime
    /// (interned). The caller <b>must</b> call <see cref="TopicPin.Release"/> exactly once on every
    /// path — it is a no-op for an interned hit and frees the per-record pin beyond the cap.
    /// </summary>
    /// <param name="topic">The topic name (non-null; validated by the public record ctor).</param>
    internal TopicPin Rent(string topic)
    {
        if (_interned.TryGetValue(topic, out IntPtr cached))
        {
            return new TopicPin(cached, default);
        }

        if (Volatile.Read(ref _internedCount) >= MaxInternedTopics)
        {
            // Capped: a per-record pin, released by the caller. Checked WITHOUT the lock so the
            // pathological unbounded-topics workload does not also serialize on _internGate.
            GCHandle fallback = PinUtf8(topic);
            return new TopicPin(fallback.AddrOfPinnedObject(), fallback);
        }

        lock (_internGate)
        {
            // Re-check: another thread may have interned this topic, or filled the last slot,
            // between the lock-free miss above and here.
            if (_interned.TryGetValue(topic, out cached))
            {
                return new TopicPin(cached, default);
            }

            GCHandle pin = PinUtf8(topic);
            if (_internedCount >= MaxInternedTopics)
            {
                return new TopicPin(pin.AddrOfPinnedObject(), pin);
            }

            _pins.Add(pin);
            IntPtr pointer = pin.AddrOfPinnedObject();
            _interned[topic] = pointer;

            // Published AFTER the dictionary entry, so a concurrent reader that sees the count is
            // guaranteed to find the entry it implies.
            Volatile.Write(ref _internedCount, _internedCount + 1);
            return new TopicPin(pointer, default);
        }
    }

    /// <summary>
    /// Encodes <paramref name="topic"/> as NUL-terminated UTF-8 into a fresh array and pins it. The
    /// trailing zero is what the ABI's <c>const char *</c> parameter requires (ffi §A3); the array
    /// is one byte longer than the encoding and is zero-initialized, so no explicit terminator write
    /// is needed.
    /// </summary>
    private static GCHandle PinUtf8(string topic)
    {
        byte[] encoded = Encoding.UTF8.GetBytes(topic);
        byte[] buffer = new byte[encoded.Length + 1];
        Array.Copy(encoded, buffer, encoded.Length);
        return GCHandle.Alloc(buffer, GCHandleType.Pinned);
    }

    /// <summary>
    /// A rented topic pointer plus, beyond the cap, the per-record pin the caller owns.
    /// <see cref="Release"/> is a no-op for the interned (common) case, so the caller can call it
    /// unconditionally in a <c>finally</c> without branching.
    /// </summary>
    internal readonly struct TopicPin
    {
        private readonly GCHandle _fallback;

        internal TopicPin(IntPtr pointer, GCHandle fallback)
        {
            Pointer = pointer;
            _fallback = fallback;
        }

        /// <summary>The pinned NUL-terminated UTF-8 topic buffer's address.</summary>
        internal IntPtr Pointer { get; }

        /// <summary>
        /// Frees the per-record fallback pin, if this rent produced one. Call exactly once, in a
        /// <c>finally</c>, after the native send has returned (ffi §A4 — the borrow ends there).
        /// </summary>
        internal void Release()
        {
            if (_fallback.IsAllocated)
            {
                _fallback.Free();
            }
        }
    }
}
