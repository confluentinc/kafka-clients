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
/// Tests the <see cref="Utf8Marshal"/> UTF-8 encode and NUL-terminated decode in isolation, with no native call.
/// </summary>
public sealed class Utf8MarshalTests
{
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
