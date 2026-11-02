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

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Tests the config marshalling into <c>kafka_Map_t</c> and <c>ProducerConfig_new</c>: a valid config builds a
/// producer without a broker, a config the core rejects throws its <see cref="KafkaException"/>, and a bad argument
/// throws a .NET exception before any native call.
/// </summary>
public sealed class ProducerConfigMarshalTests
{
    private const int ConfigErrorCode = -10;

    private static readonly TimeSpan s_disposeDeadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void Create_ValidConfig_Succeeds()
    {
        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:9092",
            ["client.id"] = "producer-foundation-test",
        };

        NativeProducer producer = NativeProducer.Create(config);

        Assert.False(producer.Handle.IsInvalid);
        TestTimeout.Run(producer.Dispose, s_disposeDeadline);
    }

    [Fact]
    public void Create_NonAsciiConfigValue_Succeeds()
    {
        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:9092",
            ["client.id"] = "グループ-クライアント",
        };

        NativeProducer producer = NativeProducer.Create(config);

        Assert.False(producer.Handle.IsInvalid);
        TestTimeout.Run(producer.Dispose, s_disposeDeadline);
    }

    [Fact]
    public void Create_MissingBootstrapServers_ThrowsKafkaException()
    {
        KafkaException failure = Assert.Throws<KafkaException>(
            () => NativeProducer.Create(new Dictionary<string, string>()));

        Assert.Equal(
            "Missing required configuration \"bootstrap.servers\" which has no default value.",
            failure.Message);
        Assert.Equal(ConfigErrorCode, failure.Code);
        Assert.Null(failure.InnerException);
    }

    [Fact]
    public void Create_UnparseableConfigValue_ThrowsKafkaException()
    {
        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:9092",
            ["batch.size"] = "not-a-number",
        };

        KafkaException failure = Assert.Throws<KafkaException>(() => NativeProducer.Create(config));

        Assert.Contains("Invalid value not-a-number for configuration batch.size", failure.Message, StringComparison.Ordinal);
        Assert.Equal(ConfigErrorCode, failure.Code);
        Assert.False(failure.IsRetriable);
    }

    [Fact]
    public void Create_UnparseableConfigValue_Repeatedly_ThrowsEachTime()
    {
        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:9092",
            ["batch.size"] = "not-a-number",
        };

        for (int i = 0; i < 50; i++)
        {
            KafkaException failure = Assert.Throws<KafkaException>(() => NativeProducer.Create(config));
            Assert.Contains("Invalid value not-a-number for configuration batch.size", failure.Message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Create_NullConfig_ThrowsArgumentNullException()
    {
        ArgumentNullException failure = Assert.Throws<ArgumentNullException>(() => NativeProducer.Create(null!));

        Assert.Equal("config", failure.ParamName);
    }

    [Fact]
    public void Create_ConfigWithNullValue_ThrowsArgumentException()
    {
        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = null!,
        };

        ArgumentException failure = Assert.Throws<ArgumentException>(() => NativeProducer.Create(config));

        Assert.Equal("config", failure.ParamName);
        Assert.StartsWith(
            "Configuration value for key 'bootstrap.servers' must not be null.",
            failure.Message,
            StringComparison.Ordinal);
    }

    [Fact]
    public void Map_NewPutDestroy_InIsolation_NoError()
    {
        IntPtr outErr = IntPtr.Zero;
        IntPtr map = NativeMethods.MapNew(ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
        Assert.NotEqual(IntPtr.Zero, map);

        using (Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin("client.id"))
        using (Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin("グループ-クライアント"))
        {
            NativeMethods.MapPut(map, key.Pointer, value.Pointer, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
        }

        NativeMethods.MapDestroy(map, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }
}
