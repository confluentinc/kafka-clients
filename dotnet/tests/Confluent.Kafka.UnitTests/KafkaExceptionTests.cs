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

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Tests the flat <see cref="KafkaException"/> error model: a core error becomes a <see cref="KafkaException"/> with its
/// code, retriable flag and message, and its handle is freed exactly once.
/// </summary>
public sealed class KafkaExceptionTests
{
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

        Assert.False(ex.IsRetriable);

        Assert.Contains("Classic group protocol", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DefaultGroupProtocol_IsClassic_ThrowsUnsupportedVersion()
    {
        KafkaException ex = Assert.Throws<KafkaException>(() => NativeConsumer.Create(BaseConfig()));

        Assert.Equal(UnsupportedVersionCode, ex.Code);
        Assert.Contains("Classic group protocol", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void NonAsciiInvalidConfigValue_ErrorMessageEchoesValue()
    {
        Dictionary<string, string> config = BaseConfig();
        config["group.protocol"] = "café";

        KafkaException ex = Assert.Throws<KafkaException>(() => NativeConsumer.Create(config));

        Assert.Contains("café", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void FromHandle_NullHandle_ReturnsNull()
    {
        Assert.Null(KafkaException.FromHandle(IntPtr.Zero));
    }

    [Fact]
    public void ErrorPath_RepeatedManyTimes_NoDoubleFreeCorruption()
    {
        for (int i = 0; i < 200; i++)
        {
            IntPtr outErr = IntPtr.Zero;
            IntPtr error = NativeMethods.ErrorKafka(ref outErr);
            Assert.Equal(IntPtr.Zero, outErr);
            Assert.IsType<KafkaException>(KafkaException.FromFfiError(error, owned: true));
        }
    }
}
