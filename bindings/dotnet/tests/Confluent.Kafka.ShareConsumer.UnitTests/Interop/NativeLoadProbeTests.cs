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

namespace Confluent.Kafka.ShareConsumer.UnitTests.Interop;

/// <summary>
/// Native-load probe for the M1/P1 interop foundation. Proves the cdylib loads via
/// default <c>[DllImport]</c> probing (the MSBuild native-copy target placed it in
/// the test output), and the first ABI round-trip works over <c>Cdecl</c> and the
/// ffi §0.1 type map — driving <see cref="NativeMethods"/> directly through the existing
/// <c>InternalsVisibleTo</c> grant (no public API exists yet). Every test here
/// invokes a native <c>[DllImport]</c>; the managed-only <see cref="Utf8Marshal"/>
/// codec coverage lives in <c>Utf8MarshalTests</c>.
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
        IntPtr props = NativeMethods.ConsumerPropertiesNew();
        try
        {
            Assert.NotEqual(IntPtr.Zero, props);

            using Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin("bootstrap.servers");
            using Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin("localhost:9092");
            NativeMethods.ConsumerPropertiesPut(props, key.Pointer, value.Pointer);
        }
        finally
        {
            NativeMethods.ConsumerPropertiesDestroy(props);
        }
    }

    /// <summary>
    /// Non-ASCII variant of the smoke flow. Exercises the multi-byte UTF-8 encode +
    /// pin path across a real native call and asserts it does not crash / AV. It does
    /// NOT verify the stored value: <c>_put</c> returns <c>void</c> and the ABI
    /// exposes no <c>ConsumerProperties</c> getter, so a true corruption round-trip is
    /// deferred to a phase with a config readback path (e.g. reading a config value
    /// back through <c>ConsumerGroupMetadata_group_id</c>). The managed UTF-8 codec
    /// correctness itself is covered in <c>Utf8MarshalTests</c>.
    /// </summary>
    [Fact]
    public void ConsumerProperties_NonAsciiConfig_MarshalsWithoutCrashing()
    {
        IntPtr props = NativeMethods.ConsumerPropertiesNew();
        try
        {
            Assert.NotEqual(IntPtr.Zero, props);

            using Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin("clï.ïd");
            using Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin("café-brøker-🎉");
            NativeMethods.ConsumerPropertiesPut(props, key.Pointer, value.Pointer);
        }
        finally
        {
            NativeMethods.ConsumerPropertiesDestroy(props);
        }
    }
}
