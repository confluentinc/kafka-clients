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

using Confluent.Kafka.ShareConsumer.Internal;

using Xunit;

namespace Confluent.Kafka.ShareConsumer.UnitTests;

/// <summary>
/// The public <see cref="KafkaException"/> error model: operational errors from
/// the core map to a flat <see cref="KafkaException"/> (code + retriable + fatal +
/// message) via <c>KafkaException.FromHandle</c>, freeing the error handle exactly
/// once (ffi §A5/§B5). The broker-free operational error source is
/// <c>KafkaConsumer_new</c> with <c>group.protocol=classic</c> (PLAN D1).
/// </summary>
public sealed class KafkaExceptionTests
{
    // The Kafka protocol code for UNSUPPORTED_VERSION (org.apache.kafka Errors),
    // which the core's `unsupported_version` error carries.
    private const int UnsupportedVersionCode = 35;

    private static Dictionary<string, string> BaseConfig() => new()
    {
        ["bootstrap.servers"] = "localhost:9092",
    };

    [Fact]
    public void ClassicGroupProtocol_ThrowsFlatKafkaException_UnsupportedVersion()
    {
        Dictionary<string, string> config = BaseConfig();
        config["group.protocol"] = "classic";

        KafkaException ex = Assert.Throws<KafkaException>(() => NativeConsumer.Create(config));

        Assert.Equal(UnsupportedVersionCode, ex.Code);

        // The I1-guard FALSE case: unsupported_version is neither retriable nor
        // fatal. A missing [MarshalAs(I1)] would read a 4-byte BOOL and could flip
        // these — asserting both false pins the bool marshalling.
        Assert.False(ex.IsRetriable);
        Assert.False(ex.IsFatal);

        Assert.Contains("Classic group protocol", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DefaultGroupProtocol_IsClassic_ThrowsUnsupportedVersion()
    {
        // No group.protocol set → the core defaults to "classic" (PLAN D1).
        KafkaException ex = Assert.Throws<KafkaException>(() => NativeConsumer.Create(BaseConfig()));

        Assert.Equal(UnsupportedVersionCode, ex.Code);
        Assert.Contains("Classic group protocol", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void NonAsciiInvalidConfigValue_ErrorMessageEchoesValue()
    {
        // An invalid group.protocol value fails config validation, and the message
        // echoes the value — round-tripping non-ASCII through the error message
        // (the NUL-terminated output form, ffi §A3/§B3).
        Dictionary<string, string> config = BaseConfig();
        config["group.protocol"] = "café";

        KafkaException ex = Assert.Throws<KafkaException>(() => NativeConsumer.Create(config));

        Assert.Contains("café", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void FromHandle_NullHandle_ReturnsNull()
    {
        // A null (IntPtr.Zero) error handle means success — no throw, null result.
        Assert.Null(KafkaException.FromHandle(IntPtr.Zero));
    }

    [Fact]
    public void ErrorPath_RepeatedManyTimes_NoDoubleFreeCorruption()
    {
        // FromHandle frees the error handle in a finally, exactly once. A double
        // free would corrupt the native allocator and crash under repetition, so
        // driving the error path many times is the practical single-free guard.
        Dictionary<string, string> config = BaseConfig();
        config["group.protocol"] = "classic";

        for (int i = 0; i < 200; i++)
        {
            Assert.Throws<KafkaException>(() => NativeConsumer.Create(config));
        }
    }
}
