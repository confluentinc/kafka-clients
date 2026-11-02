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
/// Tests the length-delimited <see cref="Utf8Marshal.PtrToString(IntPtr, int)"/> over a pinned buffer: it reads
/// exactly the given length and never scans for a NUL.
/// </summary>
public sealed class Utf8MarshalLengthDelimitedTests
{
    [Fact]
    public void PtrToString_NonAsciiSlice_RoundTripsViaLength()
    {
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
        byte[] buffer = new byte[] { 0x41 };
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
