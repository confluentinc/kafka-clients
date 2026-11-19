// Copyright 2026 Confluent Inc.
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

using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The eight clients that take a serializer or deserializer refuse, when they are built, one with a public method they
/// would never call: a headers form without the interface that makes them call it, or, on the four that dispose their
/// serdes, a <c>Close()</c> without <see cref="IDisposable"/>. Nothing is disposed on that refusal. A serde that
/// implements the interface, or <see cref="IDisposable"/>, is taken.
/// </summary>
public sealed class SerdeGuardTests
{
    private const string Prefix = "Confluent.Kafka.UnitTests.SerdeGuardTests+";

    private const string StraySerializerMessage = Prefix + "StraySerializer declares Serialize(string, Headers, string) "
        + "but does not implement ISerializerWithHeaders<string>, so the producer would never call it.";

    private const string StrayDeserializerMessage = Prefix + "StrayDeserializer declares Deserialize(string, Headers, "
        + "ReadOnlySpan<byte>) but does not implement IDeserializerWithHeaders<string>, so the consumer would never call it.";

    private const string ClosingSerializerMessage = Prefix + "ClosingSerializer declares Close() but does not implement "
        + "IDisposable, so the producer would never call it.";

    private const string ClosingDeserializerMessage = Prefix + "ClosingDeserializer declares Close() but does not "
        + "implement IDisposable, so the consumer would never call it.";

    private static readonly string[] s_producers = { "KafkaProducer", "AsyncKafkaProducer", "MockProducer", "AsyncMockProducer" };

    private static readonly string[] s_consumers = { "KafkaConsumer", "AsyncKafkaConsumer", "MockConsumer", "AsyncMockConsumer" };

    public static TheoryData<string, bool> Producers => Cases(s_producers);

    public static TheoryData<string, bool> Consumers => Cases(s_consumers);

    public static TheoryData<string, bool> RealProducers => Cases(new[] { "KafkaProducer", "AsyncKafkaProducer" });

    public static TheoryData<string, bool> RealConsumers => Cases(new[] { "KafkaConsumer", "AsyncKafkaConsumer" });

    public static TheoryData<string> MockProducers => new TheoryData<string> { "MockProducer", "AsyncMockProducer" };

    public static TheoryData<string> MockConsumers => new TheoryData<string> { "MockConsumer", "AsyncMockConsumer" };

    [Theory]
    [MemberData(nameof(Producers))]
    public void AProducer_RefusesASerializerWithAHeadersFormItWouldNeverCall_DisposingNothing(string producer, bool asKey)
    {
        DisposableSerializer other = new DisposableSerializer();

        IllegalArgumentException e = Assert.Throws<IllegalArgumentException>(
            () => Producer(producer, asKey ? new StraySerializer() : other, asKey ? other : new StraySerializer()));

        Assert.Equal(StraySerializerMessage, e.Message);
        Assert.Equal(0, other.Disposals);
    }

    [Theory]
    [MemberData(nameof(Consumers))]
    public void AConsumer_RefusesADeserializerWithAHeadersFormItWouldNeverCall_DisposingNothing(string consumer, bool asKey)
    {
        DisposableDeserializer other = new DisposableDeserializer();

        IllegalArgumentException e = Assert.Throws<IllegalArgumentException>(
            () => Consumer(consumer, asKey ? new StrayDeserializer() : other, asKey ? other : new StrayDeserializer()));

        Assert.Equal(StrayDeserializerMessage, e.Message);
        Assert.Equal(0, other.Disposals);
    }

    [Theory]
    [MemberData(nameof(RealProducers))]
    public void ARealProducer_RefusesASerializerWithACloseItWouldNeverCall_DisposingNothing(string producer, bool asKey)
    {
        DisposableSerializer other = new DisposableSerializer();

        IllegalArgumentException e = Assert.Throws<IllegalArgumentException>(
            () => Producer(producer, asKey ? new ClosingSerializer() : other, asKey ? other : new ClosingSerializer()));

        Assert.Equal(ClosingSerializerMessage, e.Message);
        Assert.Equal(0, other.Disposals);
    }

    [Theory]
    [MemberData(nameof(RealConsumers))]
    public void ARealConsumer_RefusesADeserializerWithACloseItWouldNeverCall_DisposingNothing(string consumer, bool asKey)
    {
        DisposableDeserializer other = new DisposableDeserializer();

        IllegalArgumentException e = Assert.Throws<IllegalArgumentException>(
            () => Consumer(consumer, asKey ? new ClosingDeserializer() : other, asKey ? other : new ClosingDeserializer()));

        Assert.Equal(ClosingDeserializerMessage, e.Message);
        Assert.Equal(0, other.Disposals);
    }

    [Theory]
    [MemberData(nameof(MockProducers))]
    public async Task AMockProducer_TakesASerializerWithAClose_SinceItDisposesNothing(string producer) =>
        await DisposeBounded(Producer(producer, new ClosingSerializer(), new ClosingSerializer()));

    [Theory]
    [MemberData(nameof(MockConsumers))]
    public async Task AMockConsumer_TakesADeserializerWithAClose_SinceItDisposesNothing(string consumer) =>
        await DisposeBounded(Consumer(consumer, new ClosingDeserializer(), new ClosingDeserializer()));

    [Theory]
    [MemberData(nameof(Producers))]
    public async Task AProducer_TakesASerializerWithTheHeadersInterface_AndOneWithACloseThatIsDisposable(string producer, bool asKey)
    {
        DisposableSerializer disposable = new DisposableSerializer();

        await DisposeBounded(Producer(
            producer, asKey ? new HeadersSerializer() : disposable, asKey ? disposable : new HeadersSerializer()));

        Assert.Equal(producer.IndexOf("Mock", StringComparison.Ordinal) >= 0 ? 0 : 1, disposable.Disposals);
    }

    [Theory]
    [MemberData(nameof(Consumers))]
    public async Task AConsumer_TakesADeserializerWithTheHeadersInterface_AndOneWithACloseThatIsDisposable(string consumer, bool asKey)
    {
        DisposableDeserializer disposable = new DisposableDeserializer();

        await DisposeBounded(Consumer(
            consumer, asKey ? new HeadersDeserializer() : disposable, asKey ? disposable : new HeadersDeserializer()));

        Assert.Equal(consumer.IndexOf("Mock", StringComparison.Ordinal) >= 0 ? 0 : 1, disposable.Disposals);
    }

    [Fact]
    public void TheMessage_WritesANullableValueType_AsCSharpDoes()
    {
        IllegalArgumentException e = Assert.Throws<IllegalArgumentException>(
            () => new MockProducer<int?, string>(new StrayIntSerializer(), new HeadersSerializer()));

        Assert.Equal(
            Prefix + "StrayIntSerializer declares Serialize(string, Headers, int?) but does not implement "
                + "ISerializerWithHeaders<int?>, so the producer would never call it.",
            e.Message);
    }

    [Fact]
    public void AHeadersMethodOfAnotherType_IsNotTheHeadersForm()
    {
        MockProducer<string, string> producer = new MockProducer<string, string>(new OtherTypeSerializer(), new HeadersSerializer());
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        Assert.False(producer.Closed());
    }

    private static TheoryData<string, bool> Cases(IEnumerable<string> clients)
    {
        TheoryData<string, bool> data = new TheoryData<string, bool>();
        foreach (string client in clients)
        {
            data.Add(client, true);
            data.Add(client, false);
        }

        return data;
    }

    private static object Producer(string producer, ISerializer<string> key, ISerializer<string> value) => producer switch
    {
        "KafkaProducer" => new KafkaProducer<string, string>(ProducerConfigs(), key, value),
        "AsyncKafkaProducer" => new AsyncKafkaProducer<string, string>(ProducerConfigs(), key, value),
        "MockProducer" => new MockProducer<string, string>(key, value),
        _ => new AsyncMockProducer<string, string>(key, value),
    };

    private static object Consumer(string consumer, IDeserializer<string> key, IDeserializer<string> value) => consumer switch
    {
        "KafkaConsumer" => new KafkaConsumer<string, string>(ConsumerConfigs(), key, value),
        "AsyncKafkaConsumer" => new AsyncKafkaConsumer<string, string>(ConsumerConfigs(), key, value),
        "MockConsumer" => new MockConsumer<string, string>(key, value),
        _ => new AsyncMockConsumer<string, string>(key, value),
    };

    private static Task DisposeBounded(object client) =>
        client is IAsyncDisposable async ? TestTimeout.DisposeAsync(async) : Task.Run(() => TestTimeout.Dispose((IDisposable)client));

    private static Dictionary<string, object?> ProducerConfigs() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["max.block.ms"] = "5",
        };

    private static Dictionary<string, object?> ConsumerConfigs() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["group.protocol"] = "consumer",
            ["group.id"] = "serde-guard-group",
            ["enable.auto.commit"] = "false",
        };

    private static ReadOnlyMemory<byte>? Bytes(string? data) =>
        data is null ? (ReadOnlyMemory<byte>?)null : System.Text.Encoding.UTF8.GetBytes(data);

    /// <summary>A serializer with a public headers form but not the interface that makes a producer call it.</summary>
    private sealed class StraySerializer : ISerializer<string>
    {
        public ReadOnlyMemory<byte>? Serialize(string topic, string? data) => Bytes(data);

        public ReadOnlyMemory<byte>? Serialize(string topic, Headers headers, string? data) => Bytes(data);
    }

    /// <summary>A serializer of a nullable value type with a public headers form but not the interface.</summary>
    private sealed class StrayIntSerializer : ISerializer<int?>
    {
        public ReadOnlyMemory<byte>? Serialize(string topic, int? data) => null;

        public ReadOnlyMemory<byte>? Serialize(string topic, Headers headers, int? data) => null;
    }

    /// <summary>A string serializer whose headers-like method takes another type of data, so it is no headers form.</summary>
    private sealed class OtherTypeSerializer : ISerializer<string>
    {
        public ReadOnlyMemory<byte>? Serialize(string topic, string? data) => Bytes(data);

        public ReadOnlyMemory<byte>? Serialize(string topic, Headers headers, object? data) => null;
    }

    /// <summary>A serializer with Java's public close but not IDisposable.</summary>
    private sealed class ClosingSerializer : ISerializer<string>
    {
        public ReadOnlyMemory<byte>? Serialize(string topic, string? data) => Bytes(data);

        public void Close()
        {
        }
    }

    /// <summary>A serializer that implements the headers interface.</summary>
    private sealed class HeadersSerializer : ISerializerWithHeaders<string>
    {
        public ReadOnlyMemory<byte>? Serialize(string topic, string? data) => Bytes(data);

        public ReadOnlyMemory<byte>? Serialize(string topic, Headers headers, string? data) => Bytes(data);
    }

    /// <summary>A serializer with a public close that is also IDisposable, counting its disposals.</summary>
    private sealed class DisposableSerializer : ISerializer<string>, IDisposable
    {
        internal int Disposals { get; private set; }

        public ReadOnlyMemory<byte>? Serialize(string topic, string? data) => Bytes(data);

        public void Close()
        {
        }

        public void Dispose() => Disposals++;
    }

    /// <summary>A deserializer with a public headers form but not the interface that makes a consumer call it.</summary>
    private sealed class StrayDeserializer : IDeserializer<string>
    {
        public string? Deserialize(string topic, ReadOnlySpan<byte> data) => null;

        public string? Deserialize(string topic, Headers headers, ReadOnlySpan<byte> data) => null;
    }

    /// <summary>A deserializer with Java's public close but not IDisposable.</summary>
    private sealed class ClosingDeserializer : IDeserializer<string>
    {
        public string? Deserialize(string topic, ReadOnlySpan<byte> data) => null;

        public void Close()
        {
        }
    }

    /// <summary>A deserializer that implements the headers interface.</summary>
    private sealed class HeadersDeserializer : IDeserializerWithHeaders<string>
    {
        public string? Deserialize(string topic, ReadOnlySpan<byte> data) => null;

        public string? Deserialize(string topic, Headers headers, ReadOnlySpan<byte> data) => null;
    }

    /// <summary>A deserializer with a public close that is also IDisposable, counting its disposals.</summary>
    private sealed class DisposableDeserializer : IDeserializer<string>, IDisposable
    {
        internal int Disposals { get; private set; }

        public string? Deserialize(string topic, ReadOnlySpan<byte> data) => null;

        public void Close()
        {
        }

        public void Dispose() => Disposals++;
    }
}
