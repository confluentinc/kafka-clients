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
using System.Collections.Generic;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Round-trips a non-ASCII <c>group.id</c> config value through the consumer's group metadata and the NUL-terminated
/// <see cref="Utf8Marshal.PtrToString(IntPtr)"/>.
/// </summary>
public sealed class Utf8RoundTripTests
{
    [Fact]
    public void ConfiguredGroupId_NonAscii_RoundTripsThroughGroupMetadata()
    {
        const string groupId = "café-Ω-日本語-😀";

        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:9092",
            ["group.protocol"] = "consumer",
            ["group.id"] = groupId,
        };

        using NativeConsumer consumer = NativeConsumer.Create(config);

        IntPtr meta = NativeMethods.ConsumerGroupMetadata(consumer.Handle);
        Assert.NotEqual(IntPtr.Zero, meta);
        try
        {
            string? readBack = Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupMetadataGroupId(meta));
            Assert.Equal(groupId, readBack);
        }
        finally
        {
            NativeMethods.ConsumerGroupMetadataDestroy(meta);
        }
    }
}
