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
using System.Threading.Tasks;

using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests.Interop;

/// <summary>
/// Category-1 owned-handle lifecycle over <c>SafeProducerHandle</c> (ffi §A2):
/// create → assert valid → <c>Dispose</c> (the pinned M11/P1 <c>Producer_destroy</c>-only
/// teardown) → no crash. Double-dispose is safe; use-after-dispose throws
/// <see cref="ObjectDisposedException"/>. A <c>Producer_destroy</c> that blocks
/// (it joins the background Sender, ffi §A2) fails fast via <see cref="TestTimeout"/>
/// rather than hanging the run.
///
/// Also pins the M2/P2 SafeHandle-return failure path: a null native return from
/// <c>KafkaProducer_new</c> (here forced by an unparseable config value) yields an
/// <b>IsInvalid</b> <see cref="SafeProducerHandle"/> whose <c>Dispose</c> skips
/// <c>ReleaseHandle</c> (no spurious <c>Producer_destroy</c>), while the accompanying
/// <c>out_error</c> still round-trips through <c>KafkaException.FromHandle</c>.
/// </summary>
public sealed class SafeProducerHandleTests
{
    // A blocked broker-less destroy must surface as a test failure quickly, not hang.
    private static readonly TimeSpan s_disposeDeadline = TimeSpan.FromSeconds(30);

    private static Dictionary<string, string> ValidProducerConfig() => new()
    {
        ["bootstrap.servers"] = "localhost:9092",
    };

    [Fact]
    public void MockProducer_CreateThenDispose_HandleValidThenReleased()
    {
        NativeProducer producer = NativeProducer.CreateMock();

        Assert.False(producer.Handle.IsInvalid);

        TestTimeout.Run(producer.Dispose, s_disposeDeadline);
    }

    [Fact]
    public void KafkaProducer_CreateThenDispose_HandleValidThenReleased()
    {
        // A real producer is constructed broker-free (Java-faithful: the ctor sets up
        // the accumulator / Sender but does not connect). The teardown routes straight
        // through Producer_destroy (which blocks joining the Sender) and must return
        // without a broker present.
        NativeProducer producer = NativeProducer.Create(ValidProducerConfig());

        Assert.False(producer.Handle.IsInvalid);

        TestTimeout.Run(producer.Dispose, s_disposeDeadline);
    }

    [Fact]
    public void Dispose_CalledTwice_IsSafe()
    {
        NativeProducer producer = NativeProducer.CreateMock();

        TestTimeout.Run(producer.Dispose, s_disposeDeadline);

        // The second dispose is a no-op (guarded by the atomic disposed latch) — no
        // double destroy, no throw.
        TestTimeout.Run(producer.Dispose, s_disposeDeadline);
    }

    [Fact]
    public async Task DisposeAsync_ThenDispose_IsSafe()
    {
        // DisposeAsync delegates to Dispose in M11/P1 (no async teardown work yet), so
        // the two teardown paths share the one-shot latch: DisposeAsync tears down, and
        // a following Dispose no-ops.
        NativeProducer producer = NativeProducer.CreateMock();

        await producer.DisposeAsync();
        TestTimeout.Run(producer.Dispose, s_disposeDeadline);

        Assert.Throws<ObjectDisposedException>(() => producer.Handle);
    }

    [Fact]
    public void Handle_AccessedAfterDispose_ThrowsObjectDisposedException()
    {
        NativeProducer producer = NativeProducer.CreateMock();
        TestTimeout.Run(producer.Dispose, s_disposeDeadline);

        Assert.Throws<ObjectDisposedException>(() => producer.Handle);
    }

    [Fact]
    public void CreateAndDisposeMany_MockProducers_NoLeakOrCrash()
    {
        // Baseline churn: repeated create/dispose must not crash or leak handles.
        for (int i = 0; i < 100; i++)
        {
            NativeProducer producer = NativeProducer.CreateMock();
            Assert.False(producer.Handle.IsInvalid);
            producer.Dispose();
        }
    }

    [Fact]
    public void KafkaProducerNew_InvalidConfigValue_ReturnsInvalidHandleAndError_DisposeSkipsRelease()
    {
        // M2/P2 KEY REGRESSION (producer analog). An unparseable config value
        // (batch.size = "not-a-number") makes ProducerConfig::from_properties fail, so
        // kafka_producer_KafkaProducer_new returns a null native pointer. The
        // SafeHandle-return marshaller then hands back an IsInvalid SafeProducerHandle
        // AND writes a non-null out_error — the fallible contract is preserved.
        //
        // Disposing an IsInvalid SafeHandle must SKIP ReleaseHandle (so no spurious
        // Producer_destroy on a null pointer), and the out_error must still round-trip
        // through the flat KafkaException. Driven in a loop so a double free / leak /
        // crash on this path would corrupt the allocator and fail. Cheap: the failure
        // occurs during config parse, before any tokio runtime / Sender is created.
        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:9092",
            ["batch.size"] = "not-a-number",
        };

        for (int i = 0; i < 50; i++)
        {
            SafeProducerHandle handle;
            IntPtr outError;

            SafeProducerPropertiesHandle props = SafeProducerPropertiesHandle.Create();
            try
            {
                foreach (KeyValuePair<string, string> entry in config)
                {
                    using Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin(entry.Key);
                    using Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin(entry.Value);
                    NativeMethods.ProducerPropertiesPut(props.DangerousGetHandle(), key.Pointer, value.Pointer);
                }

                // Return type is SafeProducerHandle: the marshaller created-and-set it
                // atomically, so a null native return arrives as an IsInvalid handle.
                handle = NativeMethods.KafkaProducerNew(props, out outError);
            }
            finally
            {
                props.Dispose();
            }

            Assert.True(handle.IsInvalid);
            Assert.NotEqual(IntPtr.Zero, outError);

            // Disposing an IsInvalid handle is a no-op for ReleaseHandle → no
            // Producer_destroy on a null pointer. No crash, no double-free.
            handle.Dispose();

            // The fallible contract's error still round-trips (the parse message /
            // flags), freed exactly once by FromHandle. IllegalArgument is not
            // retriable. The exact message is the behavioral contract (DoD §3); the
            // format mirrors Java's `ConfigException(String name, Object value)`:
            // "Invalid value {value} for configuration {name}". This failure occurs
            // while parsing properties, before `KafkaProducer::from_config`'s
            // relabeling try/catch, so it surfaces unwrapped (unlike the
            // "Failed to construct kafka producer" wrapper on later construction
            // failures — see ProducerConfigMarshalTests).
            KafkaException? failure = KafkaException.FromHandle(outError);
            Assert.NotNull(failure);
            Assert.Contains("Invalid value not-a-number for configuration batch.size", failure!.Message, StringComparison.Ordinal);
            Assert.False(failure.IsRetriable);
        }
    }
}
