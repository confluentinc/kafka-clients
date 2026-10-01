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
/// Category-1 owned-handle lifecycle over <c>SafeConsumerHandle</c> (ffi §B2):
/// create → assert valid → graceful <c>Dispose</c> (close_with_timeout → destroy)
/// → no crash. Double-dispose is safe; use-after-dispose throws
/// <see cref="ObjectDisposedException"/>. A broker-less close that blocks fails
/// fast via <see cref="TestTimeout"/> rather than hanging the run.
///
/// Also pins the M2/P2 SafeHandle-return failure path: a null native return from
/// <c>KafkaConsumer_new</c> yields an <b>IsInvalid</b> <see cref="SafeConsumerHandle"/>
/// whose <c>Dispose</c> skips <c>ReleaseHandle</c> (no spurious <c>Consumer_destroy</c>),
/// while the accompanying <c>out_error</c> still round-trips through
/// <c>KafkaException.FromHandle</c>.
/// </summary>
public sealed class SafeConsumerHandleTests
{
    // A blocked broker-less close must surface as a test failure quickly, not hang.
    private static readonly TimeSpan s_disposeDeadline = TimeSpan.FromSeconds(30);

    // The Kafka protocol code for UNSUPPORTED_VERSION (org.apache.kafka Errors),
    // which the core's classic-group-protocol rejection carries (PLAN D1).
    private const int UnsupportedVersionCode = 35;

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

    [Fact]
    public void KafkaConsumerNew_ClassicProtocol_ReturnsInvalidHandleAndError_DisposeSkipsRelease()
    {
        // M2/P2 KEY REGRESSION. The classic group protocol is unsupported (PLAN D1),
        // so kafka_consumer_KafkaConsumer_new returns a null native pointer. The
        // SafeHandle-return marshaller then hands back an IsInvalid SafeConsumerHandle
        // AND writes a non-null out_error — the fallible contract is preserved.
        //
        // Disposing an IsInvalid SafeHandle must SKIP ReleaseHandle (so no spurious
        // Consumer_destroy is called on a null pointer), and the out_error must still
        // round-trip through the flat KafkaException. Driven in a loop so a double
        // free / leak / crash on this path would corrupt the allocator and fail.
        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:9092",
            ["group.protocol"] = "classic",
        };

        for (int i = 0; i < 50; i++)
        {
            SafeConsumerHandle handle;
            IntPtr outError;

            SafeConsumerPropertiesHandle props = SafeConsumerPropertiesHandle.Create();
            try
            {
                foreach (KeyValuePair<string, string> entry in config)
                {
                    using Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(entry.Key);
                    using Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin(entry.Value);
                    NativeMethods.ConsumerPropertiesPut(props.DangerousGetHandle(), key.Pointer, value.Pointer);
                }

                // Return type is SafeConsumerHandle: the marshaller created-and-set it
                // atomically, so a null native return arrives as an IsInvalid handle.
                handle = NativeMethods.KafkaConsumerNew(props, out outError);
            }
            finally
            {
                props.Dispose();
            }

            Assert.True(handle.IsInvalid);
            Assert.NotEqual(IntPtr.Zero, outError);

            // Disposing an IsInvalid handle is a no-op for ReleaseHandle → no
            // Consumer_destroy on a null pointer. No crash, no double-free.
            handle.Dispose();

            // The fallible contract's error still round-trips (classic-protocol
            // message / retriable flag), freed exactly once by FromHandle.
            KafkaException? failure = KafkaException.FromHandle(outError);
            Assert.NotNull(failure);
            Assert.Equal(UnsupportedVersionCode, failure!.Code);
            Assert.False(failure.IsRetriable);
            Assert.Contains("Classic group protocol", failure.Message, StringComparison.Ordinal);
        }
    }
}
