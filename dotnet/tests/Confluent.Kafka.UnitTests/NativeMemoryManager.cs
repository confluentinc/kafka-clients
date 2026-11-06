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
using System.Threading;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A <see cref="MemoryManager{T}"/> over native memory from <see cref="Marshal.AllocHGlobal(int)"/>, filled with
/// each byte's index; it counts its <see cref="Pin"/> and <see cref="Unpin"/> calls and frees the memory when
/// disposed.
/// </summary>
internal sealed unsafe class NativeMemoryManager : MemoryManager<byte>
{
    private readonly int _length;
    private IntPtr _pointer;
    private int _pins;
    private int _unpins;

    internal NativeMemoryManager(int length)
    {
        _length = length;
        _pointer = Marshal.AllocHGlobal(Math.Max(length, 1));
        byte* bytes = (byte*)_pointer;
        for (int i = 0; i < length; i++)
        {
            bytes[i] = unchecked((byte)i);
        }
    }

    internal IntPtr Pointer => _pointer;

    internal int PinCount => Volatile.Read(ref _pins);

    internal int UnpinCount => Volatile.Read(ref _unpins);

    public override Span<byte> GetSpan() => new Span<byte>((void*)_pointer, _length);

    public override MemoryHandle Pin(int elementIndex = 0)
    {
        if ((uint)elementIndex > (uint)_length)
        {
            throw new ArgumentOutOfRangeException(nameof(elementIndex));
        }

        Interlocked.Increment(ref _pins);
        return new MemoryHandle((byte*)_pointer + elementIndex, default, this);
    }

    public override void Unpin() => Interlocked.Increment(ref _unpins);

    protected override void Dispose(bool disposing)
    {
        IntPtr pointer = Interlocked.Exchange(ref _pointer, IntPtr.Zero);
        if (pointer != IntPtr.Zero)
        {
            Marshal.FreeHGlobal(pointer);
        }
    }
}
