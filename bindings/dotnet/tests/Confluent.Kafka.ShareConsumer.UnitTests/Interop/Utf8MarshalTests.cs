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
/// Managed unit tests for <see cref="Utf8Marshal"/> — the hand-rolled UTF-8
/// marshalling helpers (ffi §A3/§B3). These exercise the encode/decode codec in
/// isolation with no native call; the cross-ABI round-trip lives in
/// <c>NativeLoadProbeTests</c>.
/// </summary>
public sealed class Utf8MarshalTests
{
    /// <summary>
    /// <see cref="Utf8Marshal.Pin"/> a non-ASCII string, then
    /// <see cref="Utf8Marshal.PtrToString"/> it back and assert equality (a 4-byte
    /// char sits at the buffer boundary, right before the NUL). Also asserts
    /// <see cref="Utf8Marshal.PtrToString"/> maps <see cref="IntPtr.Zero"/> to
    /// <see langword="null"/> (ffi §A3 obligation).
    /// </summary>
    [Fact]
    public void PinThenPtrToString_RoundTripsAndHandlesNull()
    {
        const string original = "café-brøker-🎉";

        using (Utf8Marshal.PinnedUtf8String pinned = Utf8Marshal.Pin(original))
        {
            Assert.Equal(original, Utf8Marshal.PtrToString(pinned.Pointer));
        }

        Assert.Null(Utf8Marshal.PtrToString(IntPtr.Zero));
    }
}
