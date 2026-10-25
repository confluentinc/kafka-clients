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
using System.Runtime.InteropServices;
using System.Text;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Hand-rolled UTF-8 marshalling across the C ABI, as the netstandard2.0 floor requires: pinned NUL-terminated input
/// buffers, and copies of NUL-terminated or length-delimited output strings.
/// </summary>
internal static class Utf8Marshal
{
    internal static PinnedUtf8String Pin(string value) => new PinnedUtf8String(value);

    internal static unsafe string? PtrToString(IntPtr ptr)
    {
        if (ptr == IntPtr.Zero)
        {
            return null;
        }

        byte* bytes = (byte*)ptr;
        int length = 0;
        while (bytes[length] != 0)
        {
            length++;
        }

        return Encoding.UTF8.GetString(bytes, length);
    }

    internal static unsafe string? PtrToString(IntPtr ptr, int length)
    {
        if (ptr == IntPtr.Zero || length < 0)
        {
            return null;
        }

        if (length == 0)
        {
            return string.Empty;
        }

        return Encoding.UTF8.GetString((byte*)ptr, length);
    }

    internal static unsafe string GetString(ReadOnlySpan<byte> data)
    {
        if (data.IsEmpty)
        {
            return string.Empty;
        }

#if NETSTANDARD2_0
        fixed (byte* ptr = data)
        {
            return Encoding.UTF8.GetString(ptr, data.Length);
        }
#else
        return Encoding.UTF8.GetString(data);
#endif
    }

    /// <summary>A NUL-terminated UTF-8 copy of a string, pinned until it is disposed.</summary>
    /// <remarks>A class, not a struct, so that no copy of it can free the pin a second time.</remarks>
    internal sealed class PinnedUtf8String : IDisposable
    {
        private GCHandle _handle;

        internal PinnedUtf8String(string value)
        {
            byte[] encoded = Encoding.UTF8.GetBytes(value);

            byte[] buffer = new byte[encoded.Length + 1];
            Array.Copy(encoded, buffer, encoded.Length);

            _handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
        }

        internal IntPtr Pointer => _handle.AddrOfPinnedObject();

        public void Dispose()
        {
            if (_handle.IsAllocated)
            {
                _handle.Free();
            }
        }
    }
}
