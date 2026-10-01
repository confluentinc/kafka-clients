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
using System.Collections.Generic;
using System.Runtime.InteropServices;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// M11/P3.1 slice S1 — the batch send ABI wiring, driven directly against a broker-free
/// <c>MockProducer</c>. These tests prove the three things S1 exists to de-risk before the pins are
/// deferred (S2) or a real accumulator appears (S3): the blittable
/// <see cref="ProducerRecordNative"/> mirror struct's <b>layout</b>, the ffi §A4
/// <b>absent / empty / present</b> sentinels as the core actually enforces them, and the ABI's
/// <b>per-record</b> result semantics (one <c>(future, error)</c> pair per index, plus the accepted
/// count) that the compaction step in S3 is built on.
/// </summary>
/// <remarks>
/// They go through <see cref="ProducerSendBatchMarshal.SendBatch"/> — the production entry point the
/// batch thread will use — rather than the raw <c>[DllImport]</c>, so the <c>fixed</c> pinning and
/// the chunk-offset arithmetic are covered too (DoD §12: exercise the same primitive production
/// does). Every non-null handle the ABI hands back is freed here, so a leak in the test itself
/// cannot mask one in the code.
/// </remarks>
public sealed class ProducerSendBatchTests
{
    private const string Topic = "send-batch-topic";

    // src/common/protocol/errors.rs: InvalidRequest = 42 — the code send_batch_inner reports for a
    // null topic and for a null key/value pointer with a non-negative length.
    private const int InvalidRequestCode = 42;

    [Fact]
    public void ProducerRecordNative_Layout_MirrorsTheAbiStruct()
    {
        // Field ORDER is what a blittable mirror can get wrong silently (the runtime would happily
        // marshal a reordered struct), so assert every offset, not just the size.
        Assert.Equal(0, (int)Marshal.OffsetOf<ProducerRecordNative>(nameof(ProducerRecordNative.Topic)));

        int partition = (int)Marshal.OffsetOf<ProducerRecordNative>(nameof(ProducerRecordNative.Partition));
        int timestamp = (int)Marshal.OffsetOf<ProducerRecordNative>(nameof(ProducerRecordNative.Timestamp));
        int key = (int)Marshal.OffsetOf<ProducerRecordNative>(nameof(ProducerRecordNative.Key));
        int keyLength = (int)Marshal.OffsetOf<ProducerRecordNative>(nameof(ProducerRecordNative.KeyLength));
        int value = (int)Marshal.OffsetOf<ProducerRecordNative>(nameof(ProducerRecordNative.Value));
        int valueLength = (int)Marshal.OffsetOf<ProducerRecordNative>(nameof(ProducerRecordNative.ValueLength));

        // Strictly increasing, each field clearing the previous one — catches a reorder on any
        // pointer width.
        Assert.Equal(IntPtr.Size, partition);
        Assert.True(timestamp >= partition + sizeof(int));
        Assert.Equal(timestamp + sizeof(long), key);
        Assert.Equal(key + IntPtr.Size, keyLength);
        Assert.True(value >= keyLength + sizeof(int));
        Assert.Equal(value + IntPtr.Size, valueLength);

        if (IntPtr.Size == 8)
        {
            // The exact 64-bit layout Rust's #[repr(C)] produces for
            // kafka_producer_ProducerRecord_t (src/ffi/producer.rs): 8/4+pad/8/8/4+pad/8/4+pad.
            Assert.Equal(8, partition);
            Assert.Equal(16, timestamp);
            Assert.Equal(24, key);
            Assert.Equal(32, keyLength);
            Assert.Equal(40, value);
            Assert.Equal(48, valueLength);
            Assert.Equal(56, Marshal.SizeOf<ProducerRecordNative>());
        }
    }

    [Fact]
    public void SendBatch_AllRecordsValid_AcceptsEveryIndex()
    {
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        using PinnedBuffers pins = new PinnedBuffers();

        ProducerRecordNative[] records = new ProducerRecordNative[3];
        for (int i = 0; i < records.Length; i++)
        {
            records[i] = pins.Record(Topic, key: new byte[] { (byte)i }, value: new byte[] { 0xEE });
        }

        IntPtr[] futures = new IntPtr[records.Length];
        IntPtr[] errors = new IntPtr[records.Length];

        int accepted = ProducerSendBatchMarshal.SendBatch(
            producer.Handle, records, offset: 0, count: records.Length, futures, errors);

        try
        {
            Assert.Equal(3, accepted);
            for (int i = 0; i < records.Length; i++)
            {
                Assert.NotEqual(IntPtr.Zero, futures[i]);
                Assert.Equal(IntPtr.Zero, errors[i]);
            }
        }
        finally
        {
            FreeResults(futures, errors);
        }
    }

    [Fact]
    public void SendBatch_NullTopicRecord_FailsOnlyThatIndex()
    {
        // The per-record error semantics S3's compaction depends on: one bad record must not take
        // its neighbours down, and its slot must carry the error rather than a future.
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        using PinnedBuffers pins = new PinnedBuffers();

        ProducerRecordNative[] records = new ProducerRecordNative[3];
        records[0] = pins.Record(Topic, key: null, value: new byte[] { 1 });
        records[1] = pins.Record(Topic, key: null, value: new byte[] { 2 });
        records[1].Topic = IntPtr.Zero;   // the ABI's null-topic guard -> InvalidRequest
        records[2] = pins.Record(Topic, key: null, value: new byte[] { 3 });

        IntPtr[] futures = new IntPtr[records.Length];
        IntPtr[] errors = new IntPtr[records.Length];

        int accepted = ProducerSendBatchMarshal.SendBatch(
            producer.Handle, records, offset: 0, count: records.Length, futures, errors);

        try
        {
            Assert.Equal(2, accepted);

            Assert.NotEqual(IntPtr.Zero, futures[0]);
            Assert.Equal(IntPtr.Zero, errors[0]);

            Assert.Equal(IntPtr.Zero, futures[1]);
            Assert.NotEqual(IntPtr.Zero, errors[1]);

            Assert.NotEqual(IntPtr.Zero, futures[2]);
            Assert.Equal(IntPtr.Zero, errors[2]);

            // Read + free the error exactly once, and assert its CONTENT (DoD §3), not just that
            // one arrived.
            KafkaException failure = KafkaException.FromHandle(errors[1])!;
            errors[1] = IntPtr.Zero;
            Assert.Equal(InvalidRequestCode, failure.Code);
            Assert.Contains("malformed", failure.Message, StringComparison.Ordinal);
        }
        finally
        {
            FreeResults(futures, errors);
        }
    }

    [Fact]
    public void SendBatch_EmptyKeyAndValue_NeedTheNonNullSentinel()
    {
        // This is the ffi §A4 sentinel rule, asserted against the core rather than assumed: a
        // zero-length key/value must pass a NON-NULL pointer, because the core rejects
        // (null, len >= 0). Index 0 does it wrong (what a bare `fixed` over an empty span would
        // produce) and index 1 does it right.
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        using PinnedBuffers pins = new PinnedBuffers();

        ProducerRecordNative[] records = new ProducerRecordNative[2];

        records[0] = pins.Record(Topic, key: null, value: null);
        records[0].Key = IntPtr.Zero;
        records[0].KeyLength = 0;

        records[1] = pins.Record(Topic, key: Array.Empty<byte>(), value: Array.Empty<byte>());

        IntPtr[] futures = new IntPtr[records.Length];
        IntPtr[] errors = new IntPtr[records.Length];

        int accepted = ProducerSendBatchMarshal.SendBatch(
            producer.Handle, records, offset: 0, count: records.Length, futures, errors);

        try
        {
            Assert.Equal(1, accepted);

            Assert.Equal(IntPtr.Zero, futures[0]);
            Assert.NotEqual(IntPtr.Zero, errors[0]);
            KafkaException failure = KafkaException.FromHandle(errors[0])!;
            errors[0] = IntPtr.Zero;
            Assert.Equal(InvalidRequestCode, failure.Code);

            // The sentinel form (non-null pointer + length 0) is accepted — which is why
            // ProducerSendBatchMarshal never passes the `fixed` null for an empty buffer.
            Assert.NotEqual(IntPtr.Zero, futures[1]);
            Assert.Equal(IntPtr.Zero, errors[1]);
            Assert.Equal(0, records[1].KeyLength);
            Assert.NotEqual(IntPtr.Zero, records[1].Key);
        }
        finally
        {
            FreeResults(futures, errors);
        }
    }

    [Fact]
    public void SendBatch_AbsentKeyAndValue_UseTheMinusOneSentinel()
    {
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        using PinnedBuffers pins = new PinnedBuffers();

        ProducerRecordNative[] records = new ProducerRecordNative[1];
        records[0] = pins.Record(Topic, key: null, value: null);

        Assert.Equal(IntPtr.Zero, records[0].Key);
        Assert.Equal(-1, records[0].KeyLength);
        Assert.Equal(IntPtr.Zero, records[0].Value);
        Assert.Equal(-1, records[0].ValueLength);

        IntPtr[] futures = new IntPtr[1];
        IntPtr[] errors = new IntPtr[1];

        int accepted = ProducerSendBatchMarshal.SendBatch(
            producer.Handle, records, offset: 0, count: 1, futures, errors);

        try
        {
            Assert.Equal(1, accepted);
            Assert.NotEqual(IntPtr.Zero, futures[0]);
            Assert.Equal(IntPtr.Zero, errors[0]);
        }
        finally
        {
            FreeResults(futures, errors);
        }
    }

    [Fact]
    public void SendBatch_WithOffset_TouchesOnlyTheRequestedChunk()
    {
        // The chunk-offset arithmetic (PLAN §3.4): a chunk starting mid-array must read the records
        // from `offset` AND write the results at the same indices, leaving the earlier slots alone.
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        using PinnedBuffers pins = new PinnedBuffers();

        ProducerRecordNative[] records = new ProducerRecordNative[3];
        for (int i = 0; i < records.Length; i++)
        {
            records[i] = pins.Record(Topic, key: null, value: new byte[] { (byte)i });
        }

        IntPtr[] futures = new IntPtr[records.Length];
        IntPtr[] errors = new IntPtr[records.Length];

        int accepted = ProducerSendBatchMarshal.SendBatch(
            producer.Handle, records, offset: 1, count: 2, futures, errors);

        try
        {
            Assert.Equal(2, accepted);

            // Index 0 was outside the chunk: untouched, so nothing to free there.
            Assert.Equal(IntPtr.Zero, futures[0]);
            Assert.Equal(IntPtr.Zero, errors[0]);

            Assert.NotEqual(IntPtr.Zero, futures[1]);
            Assert.NotEqual(IntPtr.Zero, futures[2]);
            Assert.Equal(IntPtr.Zero, errors[1]);
            Assert.Equal(IntPtr.Zero, errors[2]);
        }
        finally
        {
            FreeResults(futures, errors);
        }
    }

    [Fact]
    public void SendBatch_ZeroCount_IsANoOpAndDoesNotCallNative()
    {
        // `fixed` over a zero-length array yields NULL, and the ABI asserts `records != null` — so
        // the empty chunk must short-circuit rather than reach native (an assert across FFI is UB).
        using NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);

        ProducerRecordNative[] records = Array.Empty<ProducerRecordNative>();
        IntPtr[] futures = Array.Empty<IntPtr>();
        IntPtr[] errors = Array.Empty<IntPtr>();

        int accepted = ProducerSendBatchMarshal.SendBatch(
            producer.Handle, records, offset: 0, count: 0, futures, errors);

        Assert.Equal(0, accepted);
    }

    /// <summary>
    /// Frees every non-null handle the ABI wrote: the futures via the singular
    /// <c>FutureRecordMetadata_destroy</c>, any remaining errors via
    /// <c>KafkaError_destroy</c> (both null-safe).
    /// </summary>
    private static void FreeResults(IntPtr[] futures, IntPtr[] errors)
    {
        for (int i = 0; i < futures.Length; i++)
        {
            NativeMethods.FutureRecordMetadataDestroy(futures[i]);
            NativeMethods.ErrorDestroy(errors[i]);
        }
    }

    /// <summary>
    /// Holds the pins the record array's raw pointers point at for the whole test — the test-side
    /// stand-in for the pin bookkeeping slice S2 adds to the accumulator. Every pin is released in
    /// <see cref="Dispose"/>, so a test that leaks one fails the balance assertion below rather than
    /// leaving a permanent pin behind.
    /// </summary>
    private sealed class PinnedBuffers : IDisposable
    {
        private readonly List<GCHandle> _handles = new List<GCHandle>();
        private readonly List<Utf8Marshal.PinnedUtf8String> _topics = new List<Utf8Marshal.PinnedUtf8String>();

        internal ProducerRecordNative Record(string topic, byte[]? key, byte[]? value)
        {
            Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic);
            _topics.Add(topicPin);

            ProducerRecordNative record = default;
            record.Topic = topicPin.Pointer;
            record.Partition = -1;
            record.Timestamp = -1;
            (record.Key, record.KeyLength) = PinBytes(key);
            (record.Value, record.ValueLength) = PinBytes(value);
            return record;
        }

        public void Dispose()
        {
            foreach (GCHandle handle in _handles)
            {
                handle.Free();
            }

            _handles.Clear();

            foreach (Utf8Marshal.PinnedUtf8String topic in _topics)
            {
                topic.Dispose();
            }

            _topics.Clear();
        }

        private (IntPtr Pointer, int Length) PinBytes(byte[]? bytes)
        {
            if (bytes is null)
            {
                // Absent: the ABI's (null, -1) sentinel.
                return (IntPtr.Zero, -1);
            }

            // GCHandle.AddrOfPinnedObject is non-null even for a zero-length array on current
            // runtimes, but that is undocumented (ffi §A4), so pin a 1-byte scratch buffer and
            // report length 0 — the same "non-null pointer, length 0" shape production uses.
            byte[] buffer = bytes.Length == 0 ? new byte[1] : bytes;
            GCHandle handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
            _handles.Add(handle);
            return (handle.AddrOfPinnedObject(), bytes.Length);
        }
    }
}
