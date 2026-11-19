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
using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The key and value of an async send, pinned, with their <c>kafka_Bytes_t</c> in native memory, from the call until
/// the hand-over; an empty one points at one process-wide byte that never moves.
/// </summary>
internal unsafe struct SendPins
{
    private const string UnpinFailedMessage = "Exception while unpinning a key or value buffer: {0}";

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

    // Never throws: a user's MemoryManager<byte>.Unpin that throws is traced, and the other pin and the native block
    // are still released, so the send's outcome is the producer's.
    internal void Release()
    {
        Unpin(ref _key);
        Unpin(ref _value);
        if (_bytes != IntPtr.Zero)
        {
            Marshal.FreeHGlobal(_bytes);
            _bytes = IntPtr.Zero;
        }
    }

    private static void Unpin(ref MemoryHandle handle)
    {
        try
        {
            handle.Dispose();
        }
        catch (Exception exception)
        {
            TraceUnpinFailure(exception);
        }
        finally
        {
            handle = default;
        }
    }

    private static void TraceUnpinFailure(Exception exception)
    {
        try
        {
            Trace.TraceWarning(string.Format(CultureInfo.InvariantCulture, UnpinFailedMessage, ExceptionText(exception)));
        }
        catch (Exception)
        {
            // A throwing TraceListener must not escape the hand-over.
        }
    }

    // A ToString that throws falls back to the type's name and message, so the failure is still traced.
    private static string ExceptionText(Exception exception)
    {
        try
        {
            return exception.ToString();
        }
        catch (Exception)
        {
            string type = exception.GetType().FullName ?? exception.GetType().Name;
            try
            {
                return type + ": " + exception.Message;
            }
            catch (Exception)
            {
                return type;
            }
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
