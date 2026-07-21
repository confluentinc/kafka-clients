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

namespace Confluent.Kafka.ShareConsumer.UnitTests.Interop;

/// <summary>
/// Category-1 owned-handle lifecycle over <c>SafeConsumerHandle</c> (ffi §B2):
/// create → assert valid → graceful <c>Dispose</c> (close_with_timeout → destroy)
/// → no crash. Double-dispose is safe; use-after-dispose throws
/// <see cref="ObjectDisposedException"/>. A broker-less close that blocks fails
/// fast via <see cref="TestTimeout"/> rather than hanging the run.
/// </summary>
public sealed class SafeConsumerHandleTests
{
    // A blocked broker-less close must surface as a test failure quickly, not hang.
    private static readonly TimeSpan s_disposeDeadline = TimeSpan.FromSeconds(30);

    private static Dictionary<string, string> ValidConsumerConfig() => new()
    {
        ["bootstrap.servers"] = "localhost:9092",
        ["group.protocol"] = "consumer",
    };

    [Fact]
    public void MockConsumer_CreateThenDispose_HandleValidThenReleased()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        Assert.False(consumer.Handle.IsInvalid);

        TestTimeout.Run(consumer.Dispose, s_disposeDeadline);
    }

    [Fact]
    public void KafkaConsumer_CreateThenDispose_HandleValidThenReleased()
    {
        // A real KIP-848 consumer is constructed broker-free; the graceful close
        // (close_with_timeout → destroy) must return without a broker present.
        NativeConsumer consumer = NativeConsumer.Create(ValidConsumerConfig());

        Assert.False(consumer.Handle.IsInvalid);

        TestTimeout.Run(consumer.Dispose, s_disposeDeadline);
    }

    [Fact]
    public void Dispose_CalledTwice_IsSafe()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();

        TestTimeout.Run(consumer.Dispose, s_disposeDeadline);

        // The second dispose is a no-op (guarded by the disposed flag) — no double
        // close, no double destroy, no throw.
        TestTimeout.Run(consumer.Dispose, s_disposeDeadline);
    }

    [Fact]
    public void Handle_AccessedAfterDispose_ThrowsObjectDisposedException()
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        TestTimeout.Run(consumer.Dispose, s_disposeDeadline);

        Assert.Throws<ObjectDisposedException>(() => consumer.Handle);
    }

    [Fact]
    public void CreateAndDisposeMany_MockConsumers_NoLeakOrCrash()
    {
        // Baseline churn: repeated create/dispose must not crash or leak handles.
        for (int i = 0; i < 100; i++)
        {
            NativeConsumer consumer = NativeConsumer.CreateMock();
            Assert.False(consumer.Handle.IsInvalid);
            consumer.Dispose();
        }
    }
}
