// Copyright 2026 Confluent Inc.
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
using System.Runtime.InteropServices;

using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// <see cref="BytesMemoryManager"/> over a <see cref="SafeConsumerRecordsHandle"/> that owns a native batch: a pin
/// defers the batch's release, a released batch refuses spans and pins, and a revoked memory refuses new ones while
/// its earlier pin stays valid.
/// </summary>
public sealed class BytesMemoryManagerTests : IDisposable
{
    private static readonly byte[] s_data = { 1, 2, 3, 4, 5 };

    private readonly IntPtr _block = Marshal.AllocHGlobal(s_data.Length);

    public BytesMemoryManagerTests()
    {
        Marshal.Copy(s_data, 0, _block, s_data.Length);
    }

    public void Dispose() => Marshal.FreeHGlobal(_block);

    [Fact]
    public void AMemory_ReadsTheNativeBytesWithoutCopyingThem()
    {
        using SafeConsumerRecordsHandle batch = NewBatch();

        ReadOnlyMemory<byte> memory = batch.HandOut(_block, s_data.Length);

        Assert.True(batch.HandedOut);
        Assert.True(memory.Span.SequenceEqual(s_data));
        Assert.False(MemoryMarshal.TryGetArray(memory, out _));
        Marshal.WriteByte(_block, 9);
        Assert.Equal(9, memory.Span[0]);
    }

    [Fact]
    public unsafe void APin_PointsAtTheNativeBytes_FromItsElementIndex()
    {
        using SafeConsumerRecordsHandle batch = NewBatch();
        ReadOnlyMemory<byte> memory = batch.HandOut(_block, s_data.Length);

        using (MemoryHandle pin = memory.Pin())
        {
            Assert.Equal(_block, (IntPtr)pin.Pointer);
        }

        using (MemoryHandle pin = memory.Slice(2).Pin())
        {
            Assert.Equal(_block + 2, (IntPtr)pin.Pointer);
        }
    }

    [Fact]
    public unsafe void APin_DefersTheBatchsRelease_UntilItIsUnpinned()
    {
        SafeConsumerRecordsHandle batch = NewBatch();
        int released = 0;
        batch.Retain(() => released++);
        ReadOnlyMemory<byte> memory = batch.HandOut(_block, s_data.Length);
        MemoryHandle pin = memory.Pin();

        batch.Dispose();

        Assert.Equal(0, released);
        Assert.False(batch.IsClosed);
        Assert.Equal(3, ((byte*)pin.Pointer)[2]);
        Assert.True(memory.Span.SequenceEqual(s_data));
        MemoryHandle second = memory.Pin();

        pin.Dispose();

        Assert.Equal(0, released);
        Assert.Equal(4, ((byte*)second.Pointer)[3]);

        second.Dispose();

        Assert.Equal(1, released);
        Assert.True(batch.IsClosed);
        ObjectDisposedException closed = Assert.Throws<ObjectDisposedException>(() => memory.Pin());
        Assert.Equal(BytesMemoryManager.ReleasedMessage, closed.Message);
    }

    [Fact]
    public void AReleasedBatch_RefusesASpanAndAPin()
    {
        SafeConsumerRecordsHandle batch = NewBatch();
        ReadOnlyMemory<byte> memory = batch.HandOut(_block, s_data.Length);

        batch.Dispose();

        Assert.True(batch.IsClosed);
        ObjectDisposedException span = Assert.Throws<ObjectDisposedException>(() => memory.Span.Length);
        Assert.Equal(BytesMemoryManager.ReleasedMessage, span.Message);
        ObjectDisposedException pin = Assert.Throws<ObjectDisposedException>(() => memory.Pin());
        Assert.Equal(BytesMemoryManager.ReleasedMessage, pin.Message);
        Assert.Equal("The records these bytes belong to were released.", BytesMemoryManager.ReleasedMessage);
    }

    [Fact]
    public unsafe void ARevokedMemory_RefusesANewSpanAndPin_AndKeepsItsEarlierPinValid()
    {
        SafeConsumerRecordsHandle batch = NewBatch();
        int released = 0;
        batch.Retain(() => released++);
        BytesMemoryManager manager = new BytesMemoryManager(batch, _block, s_data.Length);
        ReadOnlyMemory<byte> memory = manager.ReadOnlyMemory;
        MemoryHandle earlier = memory.Pin();

        manager.Revoke();

        ObjectDisposedException span = Assert.Throws<ObjectDisposedException>(() => memory.Span.Length);
        Assert.Equal(BytesMemoryManager.RevokedMessage, span.Message);
        ObjectDisposedException pin = Assert.Throws<ObjectDisposedException>(() => memory.Pin());
        Assert.Equal(BytesMemoryManager.RevokedMessage, pin.Message);
        Assert.Equal(
            "The bytes of a record's key or value were valid only until the deserializer's call completed.",
            BytesMemoryManager.RevokedMessage);

        batch.Dispose();
        Assert.Equal(0, released);
        Assert.False(batch.IsClosed);
        Assert.Equal(5, ((byte*)earlier.Pointer)[4]);

        earlier.Dispose();
        Assert.Equal(1, released);
        Assert.True(batch.IsClosed);
    }

    [Fact]
    public void AnEmptyKeyOrValue_IsTheEmptyMemory_WithNoManager()
    {
        using SafeConsumerRecordsHandle batch = NewBatch();

        ReadOnlyMemory<byte> memory = batch.HandOut(_block, 0);

        Assert.True(memory.IsEmpty);
        Assert.False(batch.HandedOut);
        Assert.False(MemoryMarshal.TryGetMemoryManager<byte, BytesMemoryManager>(memory, out _));
    }

    [Fact]
    public void APin_RefusesAnElementIndexPastTheEnd()
    {
        using SafeConsumerRecordsHandle batch = NewBatch();
        BytesMemoryManager manager = new BytesMemoryManager(batch, _block, s_data.Length);

        ArgumentOutOfRangeException thrown = Assert.Throws<ArgumentOutOfRangeException>(
            () => manager.Pin(s_data.Length + 1));

        Assert.Equal("elementIndex", thrown.ParamName);
        manager.Pin(s_data.Length).Dispose();
    }

    [Fact]
    public void ARetainedBatch_ReportsItsBytesAsMemoryPressure_UntilTheRelease_WhichRunsItsCallback()
    {
        SafeConsumerRecordsHandle batch = NewBatch();
        batch.AddRecord(100);
        batch.AddRecord(23);
        int released = 0;

        Assert.Equal(0, batch.Pressure);

        batch.Retain(() => released++);

        Assert.Equal(123 + (2 * SafeConsumerRecordsHandle.RecordOverhead), batch.Pressure);
        Assert.Equal(0, released);

        batch.Dispose();

        Assert.Equal(0, batch.Pressure);
        Assert.Equal(1, released);
    }

    [Fact]
    public void ARetainedBatchsMemoryPressure_StaysWhileAPinDefersTheRelease()
    {
        SafeConsumerRecordsHandle batch = NewBatch();
        batch.AddRecord(s_data.Length);
        ReadOnlyMemory<byte> memory = batch.HandOut(_block, s_data.Length);
        int released = 0;
        batch.Retain(() => released++);
        MemoryHandle pin = memory.Pin();

        batch.Dispose();

        Assert.Equal(s_data.Length + SafeConsumerRecordsHandle.RecordOverhead, batch.Pressure);
        Assert.Equal(0, released);

        pin.Dispose();

        Assert.Equal(0, batch.Pressure);
        Assert.Equal(1, released);
    }

    [Fact]
    public void ADisposedBatch_ThatWasNotRetained_ReportsNoMemoryPressure()
    {
        SafeConsumerRecordsHandle batch = NewBatch();
        batch.AddRecord(123);

        batch.Dispose();

        Assert.Equal(0, batch.Pressure);
    }

    [Fact]
    public void ARetainedBatchWithNoBytes_ReportsNoMemoryPressure()
    {
        using SafeConsumerRecordsHandle batch = NewBatch();

        batch.Retain(null);

        Assert.Equal(0, batch.Pressure);
    }

    // An empty native batch: the handle under test needs one to destroy, and the memories read `_block`.
    private static SafeConsumerRecordsHandle NewBatch()
    {
        IntPtr records = Call(NativeMethods.MapNew);
        IntPtr nextOffsets = Call(NativeMethods.MapNew);
        try
        {
            return new SafeConsumerRecordsHandle(Call((ref IntPtr outErr) =>
                Native.ConsumerRecordsWithNextOffsets(records, nextOffsets, ref outErr)));
        }
        finally
        {
            Free(nextOffsets);
            Free(records);
        }
    }

    private static IntPtr Call(NativeCall call)
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr result = call(ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
        Assert.NotEqual(IntPtr.Zero, result);
        return result;
    }

    private static void Free(IntPtr map)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.MapDestroy(map, ref outErr);
        Assert.Equal(IntPtr.Zero, outErr);
    }

    private delegate IntPtr NativeCall(ref IntPtr outErr);

    private static class Native
    {
        [DllImport(
            "confluent_kafka",
            EntryPoint = "kafka_consumer_ConsumerRecords_with_next_offsets_err",
            CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ConsumerRecordsWithNextOffsets(
            IntPtr records, IntPtr nextOffsets, ref IntPtr outErr);
    }
}
