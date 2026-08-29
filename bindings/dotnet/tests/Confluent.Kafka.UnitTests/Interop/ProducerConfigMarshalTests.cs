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
/// Config-map → <c>ProducerProperties</c> marshalling and the precondition surface
/// (ffi §A5): a valid producer config constructs successfully broker-free, an
/// unparseable value surfaces the core's failure as a flat <see cref="KafkaException"/>,
/// and bad arguments are rejected with standard .NET exceptions <b>before</b> any
/// native call — never <see cref="KafkaException"/> (the ABI does not validate
/// preconditions and would panic).
/// </summary>
public sealed class ProducerConfigMarshalTests
{
    private static readonly TimeSpan s_disposeDeadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void Create_ValidConfig_Succeeds()
    {
        // The full config path exercised broker-free: dict → ProducerProperties_new →
        // per-entry _put → KafkaProducer_new. The ctor does not connect (Java-faithful),
        // so a bootstrap.servers-only config constructs a valid handle without a broker.
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
    public void Create_MissingBootstrapServers_ThrowsKafkaException()
    {
        // An empty config reaches KafkaProducer_new (proving the new→(no put)→new
        // marshalling path) but the core requires a resolvable bootstrap.servers, so it
        // surfaces the operational failure as a flat KafkaException — a second
        // deterministic broker-free config failure alongside the unparseable-value one.
        // The message content is the behavioral contract (DoD §3).
        KafkaException failure = Assert.Throws<KafkaException>(
            () => NativeProducer.Create(new Dictionary<string, string>()));
        Assert.Contains("bootstrap.servers", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Create_UnparseableConfigValue_ThrowsKafkaException()
    {
        // A deterministic broker-free config failure: batch.size must parse as an int.
        // The core rejects it during construction → a flat KafkaException (operational),
        // NOT a precondition .NET exception. The message content is the behavioral
        // contract (DoD §3); IllegalArgument is neither retriable nor fatal.
        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:9092",
            ["batch.size"] = "not-a-number",
        };

        KafkaException failure = Assert.Throws<KafkaException>(() => NativeProducer.Create(config));
        Assert.Contains("Invalid value for 'batch.size'", failure.Message, StringComparison.Ordinal);
        Assert.False(failure.IsRetriable);
        Assert.False(failure.IsFatal);
    }

    [Fact]
    public void Create_NullConfig_ThrowsArgumentNullException()
    {
        // Precondition validated before any native call (no panic across FFI).
        Assert.Throws<ArgumentNullException>(() => NativeProducer.Create(null!));
    }

    [Fact]
    public void Create_ConfigWithNullValue_ThrowsArgumentException()
    {
        // Bad-shape config: a null value is a programmer error, rejected before any
        // pin/marshal/P-Invoke (ArgumentException, not KafkaException).
        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = null!,
        };

        Assert.Throws<ArgumentException>(() => NativeProducer.Create(config));
    }

    [Fact]
    public void Handle_AfterDispose_ThrowsObjectDisposedException()
    {
        var config = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "localhost:9092",
        };

        NativeProducer producer = NativeProducer.Create(config);
        TestTimeout.Run(producer.Dispose, s_disposeDeadline);

        Assert.Throws<ObjectDisposedException>(() => producer.Handle);
    }

    [Fact]
    public void ProducerProperties_NewPutDestroy_InIsolation_NoCrash()
    {
        // The raw props marshalling path exercised on its own (the PLAN §3 fallback,
        // kept as a belt-and-suspenders alongside the full Create path above): the
        // SafeHandle-return new, a UTF-8 hand-marshalled _put (incl. a non-ASCII value),
        // then _destroy via the SafeHandle's ReleaseHandle. No producer constructed.
        SafeProducerPropertiesHandle props = SafeProducerPropertiesHandle.Create();
        Assert.False(props.IsInvalid);

        using (Utf8Marshal.PinnedUtf8String key = Utf8Marshal.Pin("client.id"))
        using (Utf8Marshal.PinnedUtf8String value = Utf8Marshal.Pin("グループ-クライアント"))
        {
            NativeMethods.ProducerPropertiesPut(props.DangerousGetHandle(), key.Pointer, value.Pointer);
        }

        // ReleaseHandle → ProducerProperties_destroy, exactly once.
        props.Dispose();
    }
}
