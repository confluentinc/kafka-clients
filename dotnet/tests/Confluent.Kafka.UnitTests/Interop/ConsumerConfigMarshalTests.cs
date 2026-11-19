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

using Confluent.Kafka.Common.Config;
using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The consumer config map: a valid config builds a consumer; a config the core rejects throws its
/// <see cref="ConfigException"/> (code -10); a <c>null</c> value its key's validator refuses throws
/// <see cref="ConfigException"/> before any native call, and one with no validator leaves its key unset; a <c>null</c>
/// map throws <see cref="ArgumentNullException"/>.
/// </summary>
public sealed class ConsumerConfigMarshalTests
{
    // A regression bound: these consumers never subscribe, so their close returns in milliseconds.
    private static readonly TimeSpan s_disposeDeadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void Create_ValidConsumerProtocolConfig_Succeeds()
    {
        var config = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:9092",
            ["group.protocol"] = "consumer",
            ["group.id"] = "test-group",
        };

        NativeConsumer consumer = NativeConsumer.Create(config);

        Assert.False(consumer.Handle.IsInvalid);

        TestTimeout.Run(consumer.Dispose, s_disposeDeadline);
    }

    [Fact]
    public void Create_NullConfig_ThrowsArgumentNullException()
    {
        ArgumentNullException failure = Assert.Throws<ArgumentNullException>(() => NativeConsumer.Create(null!));

        Assert.Equal("configs", failure.ParamName);
    }

    [Fact]
    public void Create_NullValueItsValidatorRefuses_ThrowsConfigException()
    {
        var config = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:9092",
            ["group.protocol"] = null,
        };

        ConfigException failure = Assert.Throws<ConfigException>(() => NativeConsumer.Create(config));

        Assert.Equal(
            "Invalid value null for configuration group.protocol: "
            + "String must be one of (case insensitive): CONSUMER, CLASSIC",
            failure.Message);
        Assert.Equal(-10, failure.Code);
    }

    [Fact]
    public void Create_NullValueWithNoValidator_LeavesTheKeyUnset()
    {
        var config = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:9092",
            ["group.protocol"] = "consumer",
            ["sasl.login.connect.timeout.ms"] = null,
        };

        NativeConsumer consumer = NativeConsumer.Create(config);

        Assert.False(consumer.Handle.IsInvalid);

        TestTimeout.Run(consumer.Dispose, s_disposeDeadline);

        config["sasl.login.connect.timeout.ms"] = "";
        ConfigException failure = Assert.Throws<ConfigException>(() => NativeConsumer.Create(config));
        Assert.Equal(
            "Invalid value  for configuration sasl.login.connect.timeout.ms: Not a number of type INT",
            failure.Message);
        Assert.Equal(-10, failure.Code);
    }

    [Fact]
    public void Handle_AfterDispose_ThrowsIllegalState()
    {
        var config = new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:9092",
            ["group.protocol"] = "consumer",
        };

        NativeConsumer consumer = NativeConsumer.Create(config);
        TestTimeout.Run(consumer.Dispose, s_disposeDeadline);

        Assert.Throws<IllegalStateException>(() => consumer.Handle);
    }
}
