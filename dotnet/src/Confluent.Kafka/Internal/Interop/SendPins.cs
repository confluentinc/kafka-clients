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
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The key and value of an async send, pinned, with their <c>kafka_Bytes_t</c> in native memory, from the call until
/// the hand-over; an empty one points at one process-wide byte that never moves.
/// </summary>
internal unsafe struct SendPins
{
    private static readonly IntPtr s_empty = Marshal.AllocHGlobal(1);

    private MemoryHandle _key;
    private MemoryHandle _value;
    private IntPtr _bytes;

    internal void Pin(ReadOnlyMemory<byte>? key, ReadOnlyMemory<byte>? value, out IntPtr keyBytes, out IntPtr valueBytes)
    {
        keyBytes = IntPtr.Zero;
        valueBytes = IntPtr.Zero;
        if (key is null && value is null)
        {
            return;
        }

        _bytes = Marshal.AllocHGlobal(2 * sizeof(KafkaBytes));
        KafkaBytes* bytes = (KafkaBytes*)_bytes;
        if (key is ReadOnlyMemory<byte> pinnedKey)
        {
            bytes[0] = PinOne(pinnedKey, ref _key);
            keyBytes = (IntPtr)bytes;
        }

        if (value is ReadOnlyMemory<byte> pinnedValue)
        {
            bytes[1] = PinOne(pinnedValue, ref _value);
            valueBytes = (IntPtr)(bytes + 1);
        }
    }

    internal void Release()
    {
        _key.Dispose();
        _value.Dispose();
        if (_bytes != IntPtr.Zero)
        {
            Marshal.FreeHGlobal(_bytes);
            _bytes = IntPtr.Zero;
        }
    }

    private static KafkaBytes PinOne(ReadOnlyMemory<byte> memory, ref MemoryHandle handle)
    {
        if (memory.IsEmpty)
        {
            return new KafkaBytes(s_empty, 0);
        }

        handle = memory.Pin();
        return new KafkaBytes((IntPtr)handle.Pointer, memory.Length);
    }
}
