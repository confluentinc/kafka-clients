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

using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Managed unit tests for the <b>length-delimited</b>
/// <see cref="Utf8Marshal.PtrToString(IntPtr, int)"/> — the receive-path form (ffi §B3)
/// that the poll copy-out uses for <c>ConsumerRecord_topic</c> and each
/// <c>_header_key</c> slice (both non-NUL-terminated, borrowing into the batch). These
/// exercise the codec in isolation over a pinned buffer, with no native call. The most
/// important property is the §B3 anti-pattern guard: the marshaller must use the length
/// and <b>never</b> scan for a NUL — proven here by placing non-NUL bytes right after
/// the slice end and asserting they are not read.
/// </summary>
public sealed class Utf8MarshalLengthDelimitedTests
{
    [Fact]
    public void PtrToString_NonAsciiSlice_RoundTripsViaLength()
    {
        // A multi-byte char (🎉, 4 bytes) at the very end of the slice — the boundary
        // case that a byte-at-a-time scan would corrupt.
        const string original = "topic-grüße-Ω-🎉";
        byte[] encoded = Encoding.UTF8.GetBytes(original);

        GCHandle pin = GCHandle.Alloc(encoded, GCHandleType.Pinned);
        try
        {
            string? result = Utf8Marshal.PtrToString(pin.AddrOfPinnedObject(), encoded.Length);
            Assert.Equal(original, result);
        }
        finally
        {
            pin.Free();
        }
    }

    [Fact]
    public void PtrToString_UsesLength_NeverNulScans()
    {
        // The §B3 guard: build a buffer with a valid slice followed IMMEDIATELY by more
        // non-NUL bytes (no terminator between them). A NUL-scan would over-read past
        // the slice into the trailing bytes; the length-delimited read must stop at
        // exactly `sliceLength` and return only the slice.
        byte[] slice = Encoding.UTF8.GetBytes("field-A");
        byte[] trailing = Encoding.UTF8.GetBytes("GARBAGE-past-the-end");
        byte[] buffer = new byte[slice.Length + trailing.Length];
        Array.Copy(slice, 0, buffer, 0, slice.Length);
        Array.Copy(trailing, 0, buffer, slice.Length, trailing.Length);

        GCHandle pin = GCHandle.Alloc(buffer, GCHandleType.Pinned);
        try
        {
            string? result = Utf8Marshal.PtrToString(pin.AddrOfPinnedObject(), slice.Length);
            Assert.Equal("field-A", result);
        }
        finally
        {
            pin.Free();
        }
    }

    [Fact]
    public void PtrToString_ZeroLengthNonNull_ReturnsEmpty()
    {
        // A non-null pointer with length 0 is a genuine empty string (a valid, non-NUL
        // slice), distinct from the null-pointer (absent) case.
        byte[] buffer = new byte[] { 0x41 }; // 'A', but length 0 must not read it.
        GCHandle pin = GCHandle.Alloc(buffer, GCHandleType.Pinned);
        try
        {
            string? result = Utf8Marshal.PtrToString(pin.AddrOfPinnedObject(), 0);
            Assert.Equal(string.Empty, result);
        }
        finally
        {
            pin.Free();
        }
    }

    [Fact]
    public void PtrToString_NullPointer_ReturnsNull()
    {
        Assert.Null(Utf8Marshal.PtrToString(IntPtr.Zero, 0));
        Assert.Null(Utf8Marshal.PtrToString(IntPtr.Zero, 5));
    }

    [Fact]
    public void PtrToString_NegativeLength_ReturnsNull()
    {
        // Defensive: a negative length is treated as absent (the receive-path accessors
        // never return a negative length for a non-null pointer, but the guard holds).
        byte[] buffer = new byte[] { 0x41 };
        GCHandle pin = GCHandle.Alloc(buffer, GCHandleType.Pinned);
        try
        {
            Assert.Null(Utf8Marshal.PtrToString(pin.AddrOfPinnedObject(), -1));
        }
        finally
        {
            pin.Free();
        }
    }
}
