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
/// D5 — round-trips a non-ASCII <c>group.id</c> config value out through
/// <c>Consumer_group_metadata</c> → <c>ConsumerGroupMetadata_group_id</c> →
/// <see cref="Utf8Marshal.PtrToString(IntPtr)"/> (the NUL-terminated output form,
/// ffi §B3). This proves a configured UTF-8 value survives the in → out boundary
/// intact and exercises an owned Category-3 handle (get → read → destroy, ffi §B2).
///
/// The configured <c>group.id</c> surfaces broker-free / pre-join because the
/// core's <c>group_metadata()</c> falls back to a stub built from the configured
/// group id when no membership metadata exists yet (verified in
/// <c>async_kafka_consumer.rs::group_metadata_after_creation_with_group_id</c>).
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

        IntPtr meta = NativeMethods.ConsumerGroupMetadata(consumer.Handle.DangerousGetHandle());
        Assert.NotEqual(IntPtr.Zero, meta);
        try
        {
            string? readBack = Utf8Marshal.PtrToString(NativeMethods.ConsumerGroupMetadataGroupId(meta));
            Assert.Equal(groupId, readBack);
        }
        finally
        {
            // Owned Category-3 handle — free it exactly once after reading (ffi §B2).
            NativeMethods.ConsumerGroupMetadataDestroy(meta);
        }
    }
}
