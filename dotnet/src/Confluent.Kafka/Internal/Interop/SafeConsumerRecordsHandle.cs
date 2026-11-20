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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The owned <c>kafka_consumer_ConsumerRecords_t</c> of a polled batch whose keys and values are handed out as memory
/// through <see cref="BytesMemoryManager"/>. Each such memory references the handle and each pin adds a reference to
/// it, so the batch is destroyed only once no memory over it is reachable and no pin is open. It holds no reference to
/// the consumer, whose disposal the batch outlives.
/// </summary>
internal sealed class SafeConsumerRecordsHandle : SafeHandleZeroIsInvalid
{
    private long _bytes;
    private long _pressure;
    private bool _handedOut;
    private bool _lent;
    private Action? _released;

    internal SafeConsumerRecordsHandle(IntPtr records)
    {
        SetHandle(records);
    }

    // Whether a deserializer was given a memory over the batch, which it may have kept.
    internal bool HandedOut => _handedOut;

    // Whether a deserializer was lent a memory over the batch, a pin of which may outlive its call.
    internal bool Lent => _lent;

    internal long Pressure => _pressure;

    internal void AddBytes(long length) => _bytes += length;

    // An empty key or value has no bytes to keep valid, so it references no batch.
    internal ReadOnlyMemory<byte> HandOut(IntPtr data, int length)
    {
        if (length <= 0)
        {
            return ReadOnlyMemory<byte>.Empty;
        }

        _handedOut = true;
        return new BytesMemoryManager(this, data, length).ReadOnlyMemory;
    }

    // The manager of a memory valid only for one deserializer call, which the caller revokes when the call completes,
    // or null for an empty key or value. Such a memory cannot be kept, so it does not count as handed out.
    internal BytesMemoryManager? Lend(IntPtr data, int length)
    {
        if (length <= 0)
        {
            return null;
        }

        _lent = true;
        return new BytesMemoryManager(this, data, length);
    }

    // Disposes the batch, whose release an open pin defers, and runs `released` at the release.
    internal void Dispose(Action released)
    {
        _released = released;
        Dispose();
    }

    // Leaves the batch to the garbage collector, which cannot see the native bytes it keeps: they are reported as
    // memory pressure until the release, which then runs `released`.
    internal void Retain(Action? released)
    {
        _released = released;
        long pressure = IntPtr.Size == 4 ? Math.Min(_bytes, int.MaxValue) : _bytes;
        if (pressure > 0)
        {
            GC.AddMemoryPressure(pressure);
            _pressure = pressure;
        }

        GC.KeepAlive(this);
    }

    protected override bool ReleaseHandle()
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.ConsumerRecordsDestroy(handle, ref outErr);
        if (_pressure > 0)
        {
            GC.RemoveMemoryPressure(_pressure);
            _pressure = 0;
        }

        _released?.Invoke();
        if (outErr == IntPtr.Zero)
        {
            return true;
        }

        NativeMethods.ErrorDestroy(outErr);
        return false;
    }
}
