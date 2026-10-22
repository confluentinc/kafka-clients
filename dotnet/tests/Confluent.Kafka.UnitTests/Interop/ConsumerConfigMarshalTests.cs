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

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// The consumer config map: a valid config builds a consumer, and a bad argument is rejected with a .NET exception
/// before any native call.
/// </summary>
public sealed class ConsumerConfigMarshalTests
{
    // A regression bound: these consumers never subscribe, so their close returns in milliseconds.
    private static readonly TimeSpan s_disposeDeadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void Create_ValidConsumerProtocolConfig_Succeeds()
    {
        var config = new Dictionary<string, string>
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
        Assert.Throws<ArgumentNullException>(() => NativeConsumer.Create(null!));
    }

    [Fact]
    public void Create_ConfigWithNullValue_ThrowsArgumentException()
    {
        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:9092",
            ["group.protocol"] = null!,
        };

        Assert.Throws<ArgumentException>(() => NativeConsumer.Create(config));
    }

    [Fact]
    public void Handle_AfterDispose_ThrowsObjectDisposedException()
    {
        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:9092",
            ["group.protocol"] = "consumer",
        };

        NativeConsumer consumer = NativeConsumer.Create(config);
        TestTimeout.Run(consumer.Dispose, s_disposeDeadline);

        Assert.Throws<ObjectDisposedException>(() => consumer.Handle);
    }
}
