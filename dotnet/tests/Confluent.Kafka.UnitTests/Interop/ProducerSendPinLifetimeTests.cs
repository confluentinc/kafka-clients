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
using System.Diagnostics;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The send's pins: the process-wide empty sentinel, a slice's own address passed through to the native record, the
/// async send's one pin and one unpin per buffer by the first stage, and no pin left by the sync send, asserted
/// directly, since a wrong pin lifetime works almost always and corrupts rarely.
/// </summary>
public sealed class ProducerSendPinLifetimeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "pin-lifetime-topic";

    [Fact]
    public void Pin_AbsentKeyAndValue_PassNoBytes()
    {
        SendPins pins = default;
        pins.Pin(null, null, out IntPtr keyBytes, out IntPtr valueBytes);
        try
        {
            Assert.Equal(IntPtr.Zero, keyBytes);
            Assert.Equal(IntPtr.Zero, valueBytes);
        }
        finally
        {
            pins.Release();
        }
    }

    [Fact]
    public void Pin_EmptyKeyAndValue_UseAStableProcessWideSentinel()
    {
        // The core reads an async send's bytes until the hand-over, after Send's frame is gone, so an
        // empty key or value points at one process-wide byte: the same address for the key and the
        // value, and across calls at different stack depths, which a stack sentinel cannot give.
        SendPins first = default;
        SendPins second = default;
        first.Pin(ReadOnlyMemory<byte>.Empty, ReadOnlyMemory<byte>.Empty, out IntPtr firstKey, out IntPtr firstValue);
        try
        {
            IntPtr secondKey = PinAnEmptyKeyFromADeeperFrame(ref second);
            KafkaBytes key = Marshal.PtrToStructure<KafkaBytes>(firstKey);
            KafkaBytes value = Marshal.PtrToStructure<KafkaBytes>(firstValue);

            Assert.NotEqual(IntPtr.Zero, key.Data);
            Assert.Equal(0, key.Length);
            Assert.Equal(0, value.Length);
            Assert.Equal(key.Data, value.Data);
            Assert.Equal(key.Data, Marshal.PtrToStructure<KafkaBytes>(secondKey).Data);
        }
        finally
        {
            second.Release();
            first.Release();
        }
    }

    [Fact]
    public void Pin_PresentKeyAndValue_PointAtThePinnedBuffersAndSurviveGc()
    {
        byte[] key = new byte[] { 1, 2, 3, 4 };
        byte[] value = new byte[] { 9, 8, 7 };
        SendPins pins = default;
        pins.Pin(new ReadOnlyMemory<byte>(key), new ReadOnlyMemory<byte>(value), out IntPtr keyBytes, out IntPtr valueBytes);
        try
        {
            Collect();

            AssertPointsAt(key, Marshal.PtrToStructure<KafkaBytes>(keyBytes));
            AssertPointsAt(value, Marshal.PtrToStructure<KafkaBytes>(valueBytes));
        }
        finally
        {
            pins.Release();
        }
    }

    [Fact]
    public async Task Send_ManyRecords_LeavesNoBufferPinned()
    {
        // A MemoryHandle pin is a strong root, so a leaked pin keeps its buffer alive: each send gets a buffer nothing
        // else references, which must become collectable once the send has completed.
        WeakReference[] valueRefs;

        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());
        using (TestTimeout.Disposal(producer))
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

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Pin_NativeAndArraySlices_PassTheirOffsetPointersToTheNativeRecord(bool nativeKey)
    {
        using NativeMemoryManager native = new NativeMemoryManager(16);
        byte[] array = new byte[16];
        GCHandle arrayHandle = GCHandle.Alloc(array, GCHandleType.Pinned);
        try
        {
            ReadOnlyMemory<byte> nativeSlice = native.Memory.Slice(3, 8);
            ReadOnlyMemory<byte> arraySlice = new ReadOnlyMemory<byte>(array, 4, 7);
            IntPtr nativeData = native.Pointer + 3;
            IntPtr arrayData = arrayHandle.AddrOfPinnedObject() + 4;
            ReadOnlyMemory<byte> key = nativeKey ? nativeSlice : arraySlice;
            ReadOnlyMemory<byte> value = nativeKey ? arraySlice : nativeSlice;

            SendPins pins = default;
            pins.Pin(key, value, out IntPtr keyBytes, out IntPtr valueBytes);
            try
            {
                AssertBytesAt(nativeKey ? nativeData : arrayData, key.Length, keyBytes);
                AssertBytesAt(nativeKey ? arrayData : nativeData, value.Length, valueBytes);

                SerializedProducerRecord record = new SerializedProducerRecord(Topic, 0, null, key, value);
                IntPtr nativeRecord = ProducerRecordBuildTests.Build(in record, keyBytes, valueBytes);
                try
                {
                    IntPtr recordKey = ProducerRecordBuildTests.Read(nativeRecord, ProducerRecordBuildTests.RecordNative.Key);
                    IntPtr recordValue = ProducerRecordBuildTests.Read(nativeRecord, ProducerRecordBuildTests.RecordNative.Value);

                    Assert.Equal(keyBytes, recordKey);
                    Assert.Equal(valueBytes, recordValue);
                    AssertBytesAt(nativeKey ? nativeData : arrayData, key.Length, recordKey);
                    AssertBytesAt(nativeKey ? arrayData : nativeData, value.Length, recordValue);
                }
                finally
                {
                    ProducerRecordBuildTests.Destroy(nativeRecord);
                }
            }
            finally
            {
                pins.Release();
            }
        }
        finally
        {
            arrayHandle.Free();
        }
    }

    [Fact]
    public async Task AsyncSend_PinsKeyAndValueOnce_AndUnpinsThemWhenTheFirstStageCompletes()
    {
        using NativeMemoryManager key = new NativeMemoryManager(8);
        using NativeMemoryManager value = new NativeMemoryManager(32);
        AsyncMockProducer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> producer =
            new AsyncMockProducer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(
                MemorySerializer.Instance, MemorySerializer.Instance);
        using (TestTimeout.Disposal(producer))
        {
            AsyncKafkaFuture<RecordMetadata> future = await TestTimeout.Run(
                () => producer.Send(MemoryRecord(Topic, key, value)).AsTask(), s_deadline);

            Assert.Equal(1, key.PinCount);
            Assert.Equal(1, key.UnpinCount);
            Assert.Equal(1, value.PinCount);
            Assert.Equal(1, value.UnpinCount);

            await TestTimeout.Run(() => future.Get(), s_deadline);
            Assert.Equal(1, key.PinCount);
            Assert.Equal(1, value.PinCount);
        }
    }

    [Fact]
    public void AsyncSend_RejectedTopic_IsRefusedBeforeAnythingIsPinned()
    {
        using NativeMemoryManager key = new NativeMemoryManager(8);
        using NativeMemoryManager value = new NativeMemoryManager(32);
        AsyncMockProducer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> producer =
            new AsyncMockProducer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(
                MemorySerializer.Instance, MemorySerializer.Instance);
        using (TestTimeout.Disposal(producer))
        {
            ArgumentException rejected = Assert.Throws<ArgumentException>(
                () => { _ = producer.Send(MemoryRecord("bad\0topic", key, value)); });

            Assert.StartsWith(AdminStrings.InvalidTopicMessage, rejected.Message);
            Assert.Equal(0, key.PinCount);
            Assert.Equal(0, key.UnpinCount);
            Assert.Equal(0, value.PinCount);
            Assert.Equal(0, value.UnpinCount);
            Assert.Equal(0, producer.HistoryCount());
        }
    }

    [Fact]
    public void AsyncSend_FailingAfterThePin_UnpinsEachBufferOnce()
    {
        // The abandoned send must release each pin exactly once.
        using NativeMemoryManager key = new NativeMemoryManager(8);
        using NativeMemoryManager value = new NativeMemoryManager(32);

        IntPtr outErr = SendOnAClosedProducer(key, value);

        Assert.Equal(IntPtr.Zero, outErr);
        Assert.Equal(1, key.PinCount);
        Assert.Equal(1, key.UnpinCount);
        Assert.Equal(1, value.PinCount);
        Assert.Equal(1, value.UnpinCount);
    }

    [Fact]
    public void SyncSend_LeavesNoPinOutstandingOnceItReturns()
    {
        using NativeMemoryManager key = new NativeMemoryManager(8);
        using NativeMemoryManager value = new NativeMemoryManager(32);
        MockProducer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> producer =
            new MockProducer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(
                MemorySerializer.Instance, MemorySerializer.Instance);
        using (TestTimeout.Disposal(producer))
        {
            KafkaFuture<RecordMetadata> future = default;
            TestTimeout.Run(() => future = producer.Send(MemoryRecord(Topic, key, value)), s_deadline);

            Assert.Equal(key.PinCount, key.UnpinCount);
            Assert.Equal(value.PinCount, value.UnpinCount);

            TestTimeout.Run(() => _ = future.Get(), s_deadline);
            Assert.Equal(1, producer.HistoryCount());
        }
    }

    [Fact]
    public void Pin_Buffer_IsCollectableOnceThePinsAreReleased()
    {
        // The control for the test above: the same pin keeps its buffer alive until it is released,
        // so a "0 alive" result can be attributed to the release.
        SendPins pins = default;
        WeakReference reference = PinAndForget(ref pins);

        Collect();
        Assert.True(reference.IsAlive, "a live pin must keep its buffer alive");

        pins.Release();

        Collect();
        Assert.False(reference.IsAlive, "releasing the pins must let the buffer be collected");
    }

    private static void Collect()
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
    }

    [Fact]
    public void Release_AnUnpinThatThrows_StillUnpinsTheOtherBuffer_OnceAndTracesTheFailure()
    {
        string marker = UnpinMarker();
        using NativeMemoryManager key = new NativeMemoryManager(8, marker);
        using NativeMemoryManager value = new NativeMemoryManager(32);
        using UnpinFailureListener listener = new UnpinFailureListener(marker);
        SendPins pins = default;
        pins.Pin((ReadOnlyMemory<byte>)key.Memory, (ReadOnlyMemory<byte>)value.Memory, out _, out _);

        pins.Release();
        pins.Release();

        Assert.Equal(1, key.UnpinCount);
        Assert.Equal(1, value.UnpinCount);
        string traced = Assert.Single(listener.Messages);
        Assert.StartsWith(
            "Exception while unpinning a key or value buffer: System.InvalidOperationException: " + marker, traced);
    }

    [Fact]
    public async Task AsyncSend_AnUnpinThatThrowsAtAnInlineHandOver_CompletesTheFirstStage()
    {
        string keyMarker = UnpinMarker();
        string valueMarker = UnpinMarker();
        using NativeMemoryManager key = new NativeMemoryManager(8, keyMarker);
        using NativeMemoryManager value = new NativeMemoryManager(32, valueMarker);
        using UnpinFailureListener keyListener = new UnpinFailureListener(keyMarker);
        using UnpinFailureListener valueListener = new UnpinFailureListener(valueMarker);
        AsyncMockProducer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> producer =
            new AsyncMockProducer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(
                MemorySerializer.Instance, MemorySerializer.Instance);
        using (TestTimeout.Disposal(producer))
        {
            AsyncKafkaFuture<RecordMetadata> future = await TestTimeout.Run(
                () => producer.Send(MemoryRecord(Topic, key, value)).AsTask(), s_deadline);
            await TestTimeout.Run(() => future.Get(), s_deadline);

            Assert.Equal(1, producer.HistoryCount());
            Assert.Equal(1, key.UnpinCount);
            Assert.Equal(1, value.UnpinCount);
            Assert.Single(keyListener.Messages);
            Assert.Single(valueListener.Messages);
        }
    }

    [Fact]
    public async Task AsyncSend_AnUnpinThatThrowsAtADeferredHandOver_CompletesTheFirstStage()
    {
        string marker = UnpinMarker();
        string topic = "pin-lifetime-unreachable-" + Guid.NewGuid().ToString("N");
        using NativeMemoryManager key = new NativeMemoryManager(8, marker);
        using NativeMemoryManager value = new NativeMemoryManager(32);
        using UnpinFailureListener listener = new UnpinFailureListener(marker);
        AsyncKafkaProducer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> producer =
            new AsyncKafkaProducer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(
                new Dictionary<string, object?>
                {
                    ["bootstrap.servers"] = "localhost:1",
                    ["max.block.ms"] = 1000,
                },
                MemorySerializer.Instance,
                MemorySerializer.Instance);
        try
        {
            ValueTask<AsyncKafkaFuture<RecordMetadata>> stage = producer.Send(MemoryRecord(topic, key, value));
            Assert.False(stage.IsCompleted, "the hand-over ran inside the call");

            AsyncKafkaFuture<RecordMetadata> future = await TestTimeout.Run(() => stage.AsTask(), s_deadline);
            KafkaException failure = await FutureAssert.ThrowsAnyAsync<KafkaException>(
                () => TestTimeout.Run(() => future.Get(), s_deadline));
            Assert.Equal(7, failure.Code);

            Assert.Equal(1, key.UnpinCount);
            Assert.Equal(1, value.UnpinCount);
            Assert.Single(listener.Messages);
        }
        finally
        {
            await TestTimeout.Run(() => producer.DisposeAsync().AsTask(), s_deadline);
        }
    }

    [Fact]
    public void AsyncSend_FailingAfterThePin_WithAnUnpinThatThrows_ThrowsTheSendsOwnError()
    {
        string marker = UnpinMarker();
        using NativeMemoryManager key = new NativeMemoryManager(8, marker);
        using NativeMemoryManager value = new NativeMemoryManager(32);
        using UnpinFailureListener listener = new UnpinFailureListener(marker);

        IntPtr outErr = SendOnAClosedProducer(key, value);

        Assert.Equal(IntPtr.Zero, outErr);
        Assert.Equal(1, key.UnpinCount);
        Assert.Equal(1, value.UnpinCount);
        Assert.Single(listener.Messages);
    }

    // The record is valid, so both buffers are pinned before the send reaches the P/Invoke, whose marshaller refuses
    // the closed producer handle; the send must throw that error, not an unpin failure.
    private static IntPtr SendOnAClosedProducer(NativeMemoryManager key, NativeMemoryManager value)
    {
        NativeProducer producer = NativeProducer.CreateMock(autoComplete: true);
        SafeProducerHandle handle = producer.Handle;
        DeliveryReporter reporter = (DeliveryReporter)typeof(NativeProducer)
            .GetField("_reporter", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(producer)!;
        TestTimeout.Dispose(producer);
        Assert.True(handle.IsClosed);

        AsyncDeliveryState state = new AsyncDeliveryState(reporter, null, null, Topic, 0, CancellationToken.None);
        SerializedProducerRecord record = new SerializedProducerRecord(
            Topic, 0, null, (ReadOnlyMemory<byte>)key.Memory, (ReadOnlyMemory<byte>)value.Memory);
        IntPtr outErr = IntPtr.Zero;
        Assert.Throws<ObjectDisposedException>(() => state.Send(handle, in record, ref outErr));
        return outErr;
    }

    private static string UnpinMarker() => "unpin failed " + Guid.NewGuid().ToString("N");

    private static ProducerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> MemoryRecord(
        string topic, NativeMemoryManager key, NativeMemoryManager value) =>
        new ProducerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(
            topic, (ReadOnlyMemory<byte>)value.Memory, (ReadOnlyMemory<byte>)key.Memory, partition: 0);

    private static void AssertBytesAt(IntPtr data, int length, IntPtr bytes)
    {
        KafkaBytes read = Marshal.PtrToStructure<KafkaBytes>(bytes);
        Assert.Equal(data, read.Data);
        Assert.Equal(length, read.Length);
    }

    private static void AssertPointsAt(byte[] expected, KafkaBytes bytes)
    {
        Assert.Equal(expected.Length, bytes.Length);
        for (int i = 0; i < expected.Length; i++)
        {
            Assert.Equal(expected[i], Marshal.ReadByte(bytes.Data, i));
        }
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static IntPtr PinAnEmptyKeyFromADeeperFrame(ref SendPins pins)
    {
        pins.Pin(ReadOnlyMemory<byte>.Empty, null, out IntPtr keyBytes, out _);
        return keyBytes;
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.NoInlining)]
    private static WeakReference PinAndForget(ref SendPins pins)
    {
        byte[] buffer = new byte[4096];
        pins.Pin(null, new ReadOnlyMemory<byte>(buffer), out _, out _);
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
            sends[i] = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, value, partition: 0)).Delivery();
        }

        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);
        Array.Clear(sends, 0, sends.Length);
        return references;
    }

    // Records the traced lines that name its marker, so tests running in parallel do not see each other's warnings.
    private sealed class UnpinFailureListener : TraceListener
    {
        private readonly string _marker;
        private readonly ConcurrentQueue<string> _messages = new ConcurrentQueue<string>();

        internal UnpinFailureListener(string marker)
        {
            _marker = marker;
            Trace.Listeners.Add(this);
        }

        internal IReadOnlyCollection<string> Messages => _messages.ToArray();

        public override void Write(string? message)
        {
        }

        public override void WriteLine(string? message)
        {
            if (message is not null && message.Contains(_marker))
            {
                _messages.Enqueue(message);
            }
        }

        protected override void Dispose(bool disposing)
        {
            Trace.Listeners.Remove(this);
            base.Dispose(disposing);
        }
    }
}
