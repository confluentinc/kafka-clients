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

using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Proves the native library loads through default <c>[DllImport]</c> probing and that a UTF-8 string round-trips
/// into and out of it over <c>Cdecl</c>, using a common error factory.
/// </summary>
public sealed class NativeLoadProbeTests
{
    [Fact]
    public void ErrorFactory_NewReadDestroy_LoadsNativeAndRoundTrips() => AssertRoundTrips("native load probe");

    [Fact]
    public void ErrorFactory_NonAsciiMessage_RoundTripsThroughNative() => AssertRoundTrips("café-brøker-🎉");

    private static void AssertRoundTrips(string message)
    {
        IntPtr error;
        using (Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(message))
        {
            IntPtr outErr = IntPtr.Zero;
            error = NativeMethods.ErrorLocalIllegalArgument(pinned.Pointer, ref outErr);
            Assert.Equal(IntPtr.Zero, outErr);
        }

        Assert.NotEqual(IntPtr.Zero, error);
        try
        {
            Assert.Equal(message, Utf8Marshal.PtrToString(NativeMethods.Message(error)));
        }
        finally
        {
            NativeMethods.ErrorDestroy(error);
        }
    }
}
