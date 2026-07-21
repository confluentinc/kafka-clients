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
using Confluent.Kafka.ShareConsumer.Internal.Interop;
using Xunit;

namespace Confluent.Kafka.ShareConsumer.UnitTests;

/// <summary>
/// Native-load probe for the M1/P1 interop foundation. Proves the cdylib loads via
/// default <c>[DllImport]</c> probing (the MSBuild native-copy target placed it in
/// the test output), the first ABI round-trip works over <c>Cdecl</c> and the
/// ffi §0.1 type map, and <see cref="Utf8Marshal"/> marshals UTF-8 both ways (§A3). The
/// probe drives internal interop only — no public API exists yet — through the
/// existing <c>InternalsVisibleTo</c> grant. The test project stays unsafe-free
/// (PLAN D2): <see cref="Utf8Marshal.PtrToString"/> is <c>unsafe</c> internally but is a
/// plain managed call here.
/// </summary>
public sealed class NativeLoadProbeTests
{
    /// <summary>
    /// Smoke: <c>ConsumerProperties_new</c> → <c>_put</c> (key/value pinned via
    /// <see cref="Utf8Marshal.Pin"/>) → <c>_destroy</c>. Proves the native loads, the
    /// first <c>[DllImport]</c> resolves its <c>EntryPoint</c>, and the type map +
    /// <see cref="Utf8Marshal.Pin"/> round-trip a <c>const char*</c> into native.
    /// </summary>
    [Fact]
    public void ConsumerProperties_NewPutDestroy_LoadsNativeAndRoundTrips()
    {
        IntPtr props = Native.ConsumerPropertiesNew();
        try
        {
            Assert.NotEqual(IntPtr.Zero, props);

            using Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin("bootstrap.servers");
            using Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin("localhost:9092");
            Native.ConsumerPropertiesPut(props, key.Pointer, value.Pointer);
        }
        finally
        {
            Native.ConsumerPropertiesDestroy(props);
        }
    }

    /// <summary>
    /// Non-ASCII variant of the smoke flow. Guards UTF-8 marshalling INTO native
    /// (catches an <c>LPStr</c>/ANSI mistake that would corrupt multi-byte input);
    /// <c>_put</c> returns <c>void</c>, so the assertion is "no crash / corruption".
    /// </summary>
    [Fact]
    public void ConsumerProperties_NonAsciiConfig_MarshalsWithoutCorruption()
    {
        IntPtr props = Native.ConsumerPropertiesNew();
        try
        {
            Assert.NotEqual(IntPtr.Zero, props);

            using Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin("clï.ïd");
            using Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin("café-brøker-🎉");
            Native.ConsumerPropertiesPut(props, key.Pointer, value.Pointer);
        }
        finally
        {
            Native.ConsumerPropertiesDestroy(props);
        }
    }

    /// <summary>
    /// <see cref="Utf8Marshal"/> managed round-trip: <see cref="Utf8Marshal.Pin"/> a non-ASCII
    /// string, then <see cref="Utf8Marshal.PtrToString"/> it back and assert equality (a
    /// 4-byte char sits at the buffer boundary, right before the NUL). Also asserts
    /// <see cref="Utf8Marshal.PtrToString"/> maps <see cref="IntPtr.Zero"/> to
    /// <see langword="null"/> (ffi §A3 obligation).
    /// </summary>
    [Fact]
    public void Utf8_PinThenPtrToString_RoundTripsAndHandlesNull()
    {
        const string original = "café-brøker-🎉";

        using (Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(original))
        {
            Assert.Equal(original, Utf8Marshal.PtrToString(pinned.Pointer));
        }

        Assert.Null(Utf8Marshal.PtrToString(IntPtr.Zero));
    }
}
