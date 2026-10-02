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
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M11/P3.1 slice S2 — the deferred-pin machinery: the interned topic cache (§4.1), the
/// process-wide static empty sentinel (§4.2), and the exactly-once unpin contract (§4.4). These
/// are the memory-safety core of the phase, so they are asserted directly rather than inferred from
/// a send succeeding: a wrong pin lifetime "works" almost always and corrupts rarely.
/// </summary>
public sealed class ProducerSendPinLifetimeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "pin-lifetime-topic";

    // ---------------------------------------------------------------- topic interning (§4.1) ----

    [Fact]
    public void TopicCache_SameTopic_ReturnsTheSameInternedPointer()
    {
        PinnedTopicCache cache = new PinnedTopicCache();

        PinnedTopicCache.TopicPin first = cache.Rent(Topic);
        PinnedTopicCache.TopicPin second = cache.Rent(Topic);
        try
        {
            // Interned, not re-pinned: the whole point of the cache (O(distinct topics) permanent
            // pins instead of O(records) transient ones).
            Assert.Equal(first.Pointer, second.Pointer);
            Assert.Equal(1, cache.InternedCount);
            Assert.Equal(Topic, Utf8Marshal.PtrToString(first.Pointer));
        }
        finally
        {
            second.Release();
            first.Release();
        }
    }

    [Fact]
    public void TopicCache_DistinctTopics_GetDistinctPointersAndGrowTheCacheOncePerTopic()
    {
        PinnedTopicCache cache = new PinnedTopicCache();

        PinnedTopicCache.TopicPin a = cache.Rent("topic-a");
        PinnedTopicCache.TopicPin b = cache.Rent("topic-b");
        PinnedTopicCache.TopicPin aAgain = cache.Rent("topic-a");
        try
        {
            Assert.NotEqual(a.Pointer, b.Pointer);
            Assert.Equal(a.Pointer, aAgain.Pointer);

            // Two DISTINCT topics over three rents — the count tracks topics, not sends.
            Assert.Equal(2, cache.InternedCount);
            Assert.Equal("topic-a", Utf8Marshal.PtrToString(a.Pointer));
            Assert.Equal("topic-b", Utf8Marshal.PtrToString(b.Pointer));
        }
        finally
        {
            aAgain.Release();
            b.Release();
            a.Release();
        }
    }

    [Fact]
    public void TopicCache_NonAsciiTopic_RoundTripsThroughTheInternedBuffer()
    {
        // Guards the hand-rolled UTF-8 + the NUL terminator (ffi §A3): an interned buffer that
        // dropped the terminator would over-read on the NUL scan, and an ANSI encoding would
        // corrupt silently on a non-ASCII name.
        const string unicodeTopic = "тема-Ünïcödé-主题-🎯";
        PinnedTopicCache cache = new PinnedTopicCache();

        PinnedTopicCache.TopicPin pin = cache.Rent(unicodeTopic);
        try
        {
            Assert.Equal(unicodeTopic, Utf8Marshal.PtrToString(pin.Pointer));
        }
        finally
        {
            pin.Release();
        }
    }

    [Fact]
    public void TopicCache_InternedPointer_SurvivesAggressiveGc()
    {
        // The interned buffer is a MANAGED byte[] held only by a pinned GCHandle. If the pin were
        // ever dropped (or the handle not pinned), a GC could move or collect it and the pointer
        // would read garbage — the exact corruption the cache exists to prevent.
        PinnedTopicCache cache = new PinnedTopicCache();
        PinnedTopicCache.TopicPin pin = cache.Rent(Topic);
        try
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            Assert.Equal(Topic, Utf8Marshal.PtrToString(pin.Pointer));
        }
        finally
        {
            pin.Release();
        }
    }

    [Fact]
    public void TopicCache_BeyondTheCap_FallsBackToAPerRecordPinAndStopsGrowing()
    {
        // Decision D5: the cache is bounded and NEVER evicts (evicting a pinned buffer an in-flight
        // record still points at would be a use-after-free), so beyond the cap it degrades to
        // Python's shape — a per-record topic pin the caller releases.
        PinnedTopicCache cache = new PinnedTopicCache();

        List<PinnedTopicCache.TopicPin> interned = new List<PinnedTopicCache.TopicPin>();
        try
        {
            for (int i = 0; i < PinnedTopicCache.MaxInternedTopics; i++)
            {
                interned.Add(cache.Rent($"capped-topic-{i}"));
            }

            Assert.Equal(PinnedTopicCache.MaxInternedTopics, cache.InternedCount);

            // A NEW topic beyond the cap: two rents must give two DIFFERENT pointers, because each
            // is its own per-record pin rather than a cached one. That differential is what
            // distinguishes "fell back" from "silently interned anyway".
            PinnedTopicCache.TopicPin overflowFirst = cache.Rent("overflow-topic");
            PinnedTopicCache.TopicPin overflowSecond = cache.Rent("overflow-topic");
            try
            {
                Assert.NotEqual(overflowFirst.Pointer, overflowSecond.Pointer);
                Assert.Equal("overflow-topic", Utf8Marshal.PtrToString(overflowFirst.Pointer));
                Assert.Equal("overflow-topic", Utf8Marshal.PtrToString(overflowSecond.Pointer));

                // The cache did not grow past the cap.
                Assert.Equal(PinnedTopicCache.MaxInternedTopics, cache.InternedCount);
            }
            finally
            {
                overflowSecond.Release();
                overflowFirst.Release();
            }

            // An ALREADY-interned topic still hits the cache after the cap is reached — the cap
            // bounds insertion, not lookup.
            PinnedTopicCache.TopicPin cachedHit = cache.Rent("capped-topic-0");
            try
            {
                Assert.Equal(interned[0].Pointer, cachedHit.Pointer);
            }
            finally
            {
                cachedHit.Release();
            }
        }
        finally
        {
            foreach (PinnedTopicCache.TopicPin pin in interned)
            {
                pin.Release();
            }
        }
    }

    // ------------------------------------------- the static empty sentinel + Fill (§4.2/§4.4) ----

    [Fact]
    public void Fill_AbsentKeyAndValue_UseTheMinusOneSentinel()
    {
        SerializedProducerRecord record = new SerializedProducerRecord(Topic, null, null, null, null);
        ProducerRecordNative native = default;

        ProducerSendBatchMarshal.Fill(ref native, record, new IntPtr(0x1234), default, default);

        Assert.Equal(new IntPtr(0x1234), native.Topic);
        Assert.Equal(-1, native.Partition);
        Assert.Equal(-1L, native.Timestamp);
        Assert.Equal(IntPtr.Zero, native.Key);
        Assert.Equal(-1, native.KeyLength);
        Assert.Equal(IntPtr.Zero, native.Value);
        Assert.Equal(-1, native.ValueLength);
    }

    [Fact]
    public void Fill_EmptyKeyAndValue_UseAStableProcessWideSentinel()
    {
        // ⚠ THE §4.2 REGRESSION GUARD. The sync path's empty sentinel is a STACK byte, which is
        // dead by the time a deferred batch reads it — "works" almost always, corrupts rarely. The
        // deferred sentinel must therefore be a single process-wide pinned buffer, so:
        //   * it is the SAME address for the key and the value of one record, and
        //   * it is the SAME address across separate Fill calls (a stack sentinel would differ, or
        //     coincide only by frame reuse).
        SerializedProducerRecord record = new SerializedProducerRecord(
            Topic, 7, 1234L, ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty);

        ProducerRecordNative first = default;
        ProducerRecordNative second = default;

        ProducerSendBatchMarshal.Fill(ref first, record, new IntPtr(0x1), default, default);
        FillFromADeeperFrame(ref second, record);

        Assert.NotEqual(IntPtr.Zero, first.Key);
        Assert.Equal(0, first.KeyLength);
        Assert.Equal(0, first.ValueLength);

        // One shared sentinel, not one per field.
        Assert.Equal(first.Key, first.Value);

        // Stable across calls AND across stack depth — the property a stack address cannot have.
        Assert.Equal(first.Key, second.Key);

        // The explicit partition / timestamp survive rather than being replaced by the -1 sentinel.
        Assert.Equal(7, first.Partition);
        Assert.Equal(1234L, first.Timestamp);
    }

    [Fact]
    public void Fill_PresentKeyAndValue_UseThePinnedPointersAndSurviveGc()
    {
        byte[] key = new byte[] { 1, 2, 3, 4 };
        byte[] value = new byte[] { 9, 8, 7 };
        SerializedProducerRecord record = new SerializedProducerRecord(Topic, null, null, key, value);

        MemoryHandle keyPin = ProducerSendBatchMarshal.PinIfNeeded(record.Key);
        MemoryHandle valuePin = ProducerSendBatchMarshal.PinIfNeeded(record.Value);
        try
        {
            ProducerRecordNative native = default;
            ProducerSendBatchMarshal.Fill(ref native, record, new IntPtr(0x1), keyPin, valuePin);

            Assert.Equal(key.Length, native.KeyLength);
            Assert.Equal(value.Length, native.ValueLength);
            Assert.NotEqual(IntPtr.Zero, native.Key);
            Assert.NotEqual(IntPtr.Zero, native.Value);

            // Forcing a GC while the pins are held must not move the buffers — read the bytes back
            // through the raw pointers the ABI would read.
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            for (int i = 0; i < key.Length; i++)
            {
                Assert.Equal(key[i], Marshal.ReadByte(native.Key, i));
            }

            for (int i = 0; i < value.Length; i++)
            {
                Assert.Equal(value[i], Marshal.ReadByte(native.Value, i));
            }
        }
        finally
        {
            valuePin.Dispose();
            keyPin.Dispose();
        }
    }

    // ------------------------------------------------------- the exactly-once unpin (§4.4) ------

    [Fact]
    public async Task Send_ManyRecords_LeavesNoBufferPinned()
    {
        // The unpin-balance test, made observable: a MemoryHandle pin is a GCHandle(Pinned), which
        // is a STRONG ROOT. So a leaked pin keeps its byte[] alive forever — and that is detectable
        // without any pin-counting API by handing each send a buffer nobody else references and
        // asserting the buffer becomes collectable once the send has completed.
        WeakReference[] valueRefs;

        using (AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray))
        {
            valueRefs = await SendThrowawayBuffersAsync(producer, count: 64);
        }

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        int alive = 0;
        foreach (WeakReference reference in valueRefs)
        {
            if (reference.IsAlive)
            {
                alive++;
            }
        }

        Assert.Equal(0, alive);
    }

    [Fact]
    public void Fill_PinnedBuffer_IsCollectableOnceThePinIsDisposed()
    {
        // The control for the test above, at the primitive level: the very same buffer is
        // un-collectable while its pin lives and collectable after Dispose. Without this, a
        // "0 alive" result could not be attributed to the unpin at all.
        WeakReference reference = PinAndForget(out MemoryHandle pin);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        Assert.True(reference.IsAlive, "a live MemoryHandle pin must keep its buffer alive");

        pin.Dispose();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        Assert.False(reference.IsAlive, "disposing the pin must let the buffer be collected");
    }

    // A separate frame so the empty sentinel is compared across DIFFERENT stack depths — the
    // property that fails for a stack-allocated sentinel but holds for a static one.
    private static void FillFromADeeperFrame(ref ProducerRecordNative native, in SerializedProducerRecord record) =>
        ProducerSendBatchMarshal.Fill(ref native, record, new IntPtr(0x2), default, default);

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static WeakReference PinAndForget(out MemoryHandle pin)
    {
        byte[] buffer = new byte[4096];
        pin = ProducerSendBatchMarshal.PinIfNeeded(buffer);
        return new WeakReference(buffer);
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static async Task<WeakReference[]> SendThrowawayBuffersAsync(
        AsyncMockProducer<byte[], byte[]> producer,
        int count)
    {
        WeakReference[] references = new WeakReference[count];
        Task<RecordMetadata>[] sends = new Task<RecordMetadata>[count];

        for (int i = 0; i < count; i++)
        {
            // 4 KiB so a surviving pin is unmistakable, and referenced ONLY by the record and the
            // weak reference — the record itself is dropped as soon as Send returns.
            byte[] value = new byte[4096];
            references[i] = new WeakReference(value);
            sends[i] = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, value, partition: 0));
        }

        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);
        Array.Clear(sends, 0, sends.Length);
        return references;
    }
}
