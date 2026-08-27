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
/// Config-map → <c>ConsumerProperties</c> marshalling and the precondition surface
/// (ffi §A5/§B5): a valid consumer config constructs successfully, while bad
/// arguments are rejected with standard .NET exceptions <b>before</b> any native
/// call — never <see cref="KafkaException"/> (the ABI does not validate
/// preconditions and would panic).
/// </summary>
public sealed class ConsumerConfigMarshalTests
{
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
        // Precondition validated before any native call (no panic across FFI).
        Assert.Throws<ArgumentNullException>(() => NativeConsumer.Create(null!));
    }

    [Fact]
    public void Create_ConfigWithNullValue_ThrowsArgumentException()
    {
        // Bad-shape config: a null value is a programmer error, rejected before any
        // pin/marshal/P-Invoke (ArgumentException, not KafkaException).
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
