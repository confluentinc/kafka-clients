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
/// Hand-rolled UTF-8 marshalling across the C ABI. The netstandard2.0 floor lacks
/// <c>LPUTF8Str</c> / <c>Marshal.PtrToStringUTF8</c> (ffi-marshalling.md §0.1), and
/// UTF-8 is the contract both ways, so strings are marshalled by hand (§A3/§B3):
/// input via <see cref="Pin(string)"/>, output via
/// <see cref="PtrToString(IntPtr)"/>.
/// </summary>
internal static class Utf8Marshal
{
    /// <summary>
    /// Encodes <paramref name="value"/> as a NUL-terminated UTF-8 buffer and pins
    /// it for the duration of the returned <see cref="PinnedUtf8String"/>. The pin
    /// is <b>call-scoped</b>: the caller wraps it in a <c>using</c> so it is freed
    /// promptly and never held across a <see cref="System.Threading.Tasks.Task"/>
    /// (ffi §A4 — a Task-scoped pin fragments the GC heap).
    /// </summary>
    /// <remarks>
    /// <see cref="GCHandle"/> + <see cref="GCHandle.AddrOfPinnedObject"/> is safe
    /// managed API — no <c>unsafe</c> needed here.
    /// </remarks>
    internal static PinnedUtf8String Pin(string value) => new PinnedUtf8String(value);

    /// <summary>
    /// Copies a NUL-terminated, callee-owned <c>const char*</c> into a managed
    /// <see cref="string"/> (ffi §A3/§B3). Returns <see langword="null"/> for
    /// <see cref="IntPtr.Zero"/>. The pointer is borrowed — the copy happens now,
    /// before the owning handle is freed; the raw pointer is never stored.
    /// </summary>
    /// <remarks>
    /// This is the NUL-terminated form only. The length-delimited
    /// (<c>out_len</c>) receive-path form — which borrows into the fetch batch and
    /// must use the length rather than a NUL-scan (ffi §B3) — is deferred to a
    /// later phase.
    /// </remarks>
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

    /// <summary>
    /// A call-scoped pin over a NUL-terminated UTF-8 buffer. A reference type (not
    /// a <c>readonly struct</c>) on purpose: <see cref="Dispose"/> then mutates the
    /// real <see cref="GCHandle"/> field instead of a compiler defensive copy, so
    /// the unpin is genuinely idempotent and there is no value-copy double-free
    /// hazard. The caller wraps it in a <c>using</c> — the pin is call-scoped and
    /// never held across a <see cref="System.Threading.Tasks.Task"/> (ffi §A4). The
    /// buffer's address is exposed as <see cref="Pointer"/> for passing to a
    /// <c>const char*</c> ABI parameter.
    /// </summary>
    internal sealed class PinnedUtf8String : IDisposable
    {
        private GCHandle _handle;

        internal PinnedUtf8String(string value)
        {
            byte[] encoded = Encoding.UTF8.GetBytes(value);

            // +1 for the trailing NUL the ABI's `const char*` parameters expect.
            byte[] buffer = new byte[encoded.Length + 1];
            Array.Copy(encoded, buffer, encoded.Length);

            _handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
        }

        /// <summary>
        /// The pinned buffer's address, valid while this pin is undisposed. Pass to
        /// a <c>const char*</c> ABI parameter.
        /// </summary>
        internal IntPtr Pointer => _handle.AddrOfPinnedObject();

        /// <summary>Unpins the buffer. Idempotent for a single owner.</summary>
        public void Dispose()
        {
            if (_handle.IsAllocated)
            {
                _handle.Free();
            }
        }
    }
}
