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
using System.Text;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Unit tests for the M11/P5 typed generic producer — the send-side serialize skin above the
/// bytes-based native producer, exercised through the public
/// <see cref="MockProducer{TKey, TValue}"/> / <see cref="AsyncMockProducer{TKey, TValue}"/> surface.
/// Covers the typed round-trip, the Java-faithful <b>invoke-on-null</b> three-state (PLAN §3.4 —
/// the send-side inverse of the consumer's not-invoked short-circuit), the value-type tombstone
/// guidance, and the mandatory synchronous <see cref="SerializationException"/> wrap (PLAN §3.5).
/// </summary>
/// <remarks>
/// The mock exposes <b>no history-record value accessor</b> (only <see cref="HistoryCount"/> — the
/// ABI gives a count), so the serialized bytes are not observable broker-free. These tests therefore
/// assert the observable facts: the send resolves, <c>HistoryCount</c> reflects it, the serializer
/// was invoked the expected number of times (including on a null datum), and a serializer throw
/// surfaces synchronously as a <see cref="SerializationException"/> with nothing reaching the native
/// mock (<c>HistoryCount() == 0</c>).
/// </remarks>
public sealed class PublicProducerTypedSendTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "typed-send-topic";

    // ---- Typed round-trip (String key / Int64 value) — sync + async ----

    [Fact]
    public void Sync_TypedRoundTrip_StringLong_Succeeds()
    {
        using MockProducer<string, long> producer =
            new MockProducer<string, long>(Serdes.String, Serdes.Int64);

        RecordMetadata metadata = null!;
        TestTimeout.Run(
            () => metadata = producer.Send(new ProducerRecord<string, long>(Topic, 42L, "k", partition: 3)),
            s_deadline);

        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(3, metadata.Partition);
        Assert.Equal(0L, metadata.Offset);
        Assert.Equal(1, producer.HistoryCount());
    }

    [Fact]
    public async Task Async_TypedRoundTrip_StringLong_Succeeds()
    {
        await using AsyncMockProducer<string, long> producer =
            new AsyncMockProducer<string, long>(Serdes.String, Serdes.Int64);

        RecordMetadata metadata = null!;
        await TestTimeout.Run(
            async () => metadata = await producer.Send(new ProducerRecord<string, long>(Topic, 42L, "k", partition: 3)),
            s_deadline);

        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(3, metadata.Partition);
        Assert.Equal(0L, metadata.Offset);
        Assert.Equal(1, producer.HistoryCount());
    }

    [Fact]
    public void Sync_TypedRoundTrip_ByteArray_Succeeds()
    {
        // <byte[],byte[]> + Serdes.ByteArray is the former bytes surface (generic-only, M11/P5).
        using MockProducer<byte[], byte[]> producer =
            new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        RecordMetadata metadata = null!;
        TestTimeout.Run(
            () => metadata = producer.Send(new ProducerRecord<byte[], byte[]>(
                Topic, Encoding.UTF8.GetBytes("v"), Encoding.UTF8.GetBytes("k"), partition: 1)),
            s_deadline);

        Assert.Equal(1, metadata.Partition);
        Assert.Equal(1, producer.HistoryCount());
    }

    // ---- Invoke-on-null (PLAN §3.4): the serializer IS invoked, even on a null key/value ----

    [Fact]
    public void Sync_NullKeyAndValue_InvokesSerializer_ThenSends()
    {
        RecordingSerializer<string> keySer = new RecordingSerializer<string>(Serdes.String);
        RecordingSerializer<string> valueSer = new RecordingSerializer<string>(Serdes.String);
        using MockProducer<string, string> producer =
            new MockProducer<string, string>(keySer, valueSer);

        // A null key and null value: the serializer is invoked on BOTH (Java-faithful — the send-side
        // inverse of the consumer's not-invoked short-circuit), returns null → the absent sentinel.
        RecordMetadata metadata = null!;
        TestTimeout.Run(
            () => metadata = producer.Send(new ProducerRecord<string, string>(Topic, value: null, key: null)),
            s_deadline);

        Assert.Equal(1, keySer.InvocationCount);
        Assert.True(keySer.LastDatumWasNull);
        Assert.Equal(1, valueSer.InvocationCount);
        Assert.True(valueSer.LastDatumWasNull);
        Assert.Equal(1, producer.HistoryCount());
    }

    [Fact]
    public async Task Async_NullValue_InvokesSerializer_ThenSends()
    {
        RecordingSerializer<string> valueSer = new RecordingSerializer<string>(Serdes.String);
        await using AsyncMockProducer<string, string> producer =
            new AsyncMockProducer<string, string>(Serdes.String, valueSer);

        await TestTimeout.Run(
            () => producer.Send(new ProducerRecord<string, string>(Topic, value: null, key: "k")),
            s_deadline);

        Assert.Equal(1, valueSer.InvocationCount);
        Assert.True(valueSer.LastDatumWasNull);
        Assert.Equal(1, producer.HistoryCount());
    }

    [Fact]
    public void Sync_EmptyValue_InvokesSerializer_ThenSends()
    {
        RecordingSerializer<string> valueSer = new RecordingSerializer<string>(Serdes.String);
        using MockProducer<string, string> producer =
            new MockProducer<string, string>(Serdes.String, valueSer);

        // An empty string serializes to a non-null empty byte[] → present-but-empty (distinct from
        // the null → absent case above).
        TestTimeout.Run(
            () => producer.Send(new ProducerRecord<string, string>(Topic, value: string.Empty, key: "k")),
            s_deadline);

        Assert.Equal(1, valueSer.InvocationCount);
        Assert.False(valueSer.LastDatumWasNull);
        Assert.Equal(1, producer.HistoryCount());
    }

    // ---- Value-type tombstone (PLAN §3.4): <string, long?> distinguishes null from a present 0L ----

    [Fact]
    public void Sync_ValueTypeTombstone_NullDistinctFromDefault()
    {
        NullableInt64Serializer valueSer = new NullableInt64Serializer();
        using MockProducer<string, long?> producer =
            new MockProducer<string, long?>(Serdes.String, valueSer);

        // A null long? is a tombstone (serializer invoked → null bytes → absent); a present 0L is a
        // value (serializer invoked → 8 bytes → present). Both invoke the serializer (invoke-on-null).
        TestTimeout.Run(
            () => producer.Send(new ProducerRecord<string, long?>(Topic, value: null, key: "k")),
            s_deadline);
        TestTimeout.Run(
            () => producer.Send(new ProducerRecord<string, long?>(Topic, value: 0L, key: "k")),
            s_deadline);

        Assert.Equal(2, valueSer.InvocationCount);
        Assert.Equal(2, producer.HistoryCount());
    }

    // ---- Serialize throws → synchronous SerializationException; nothing reaches the native mock ----

    [Fact]
    public void Sync_ValueSerializerThrows_ThrowsSerializationExceptionSynchronously()
    {
        ThrowingSerializer<string> valueSer = new ThrowingSerializer<string>();
        using MockProducer<string, string> producer =
            new MockProducer<string, string>(Serdes.String, valueSer);

        SerializationException ex = Assert.Throws<SerializationException>(
            () => producer.Send(new ProducerRecord<string, string>(Topic, value: "v", key: "k")));

        // Wrap carries the topic context + "value", and the original throw as the inner exception.
        Assert.Contains(Topic, ex.Message, StringComparison.Ordinal);
        Assert.Contains("value", ex.Message, StringComparison.Ordinal);
        Assert.IsType<ThrowingSerializer<string>.BoomException>(ex.InnerException);

        // Nothing was enqueued / no native send occurred — the throw is pre-native (PLAN §3.5).
        Assert.Equal(0, producer.HistoryCount());
    }

    [Fact]
    public void Sync_KeySerializerThrows_MessageMentionsKey()
    {
        ThrowingSerializer<string> keySer = new ThrowingSerializer<string>();
        using MockProducer<string, string> producer =
            new MockProducer<string, string>(keySer, Serdes.String);

        SerializationException ex = Assert.Throws<SerializationException>(
            () => producer.Send(new ProducerRecord<string, string>(Topic, value: "v", key: "k")));

        Assert.Contains("key", ex.Message, StringComparison.Ordinal);
        Assert.IsType<ThrowingSerializer<string>.BoomException>(ex.InnerException);
        Assert.Equal(0, producer.HistoryCount());
    }

    [Fact]
    public void Async_ValueSerializerThrows_ThrowsSynchronouslyBeforeReturningTask()
    {
        ThrowingSerializer<string> valueSer = new ThrowingSerializer<string>();
        using AsyncMockProducer<string, string> producer =
            new AsyncMockProducer<string, string>(Serdes.String, valueSer);

        // The async Send serializes inline before enqueuing to the pump, so the wrap surfaces
        // SYNCHRONOUSLY (Assert.Throws, not ThrowsAsync) — before the Task is returned. The statement
        // lambda binds to Action (not the obsolete Func<Task>) and proves nothing was awaited.
        SerializationException ex = Assert.Throws<SerializationException>(() =>
        {
            _ = producer.Send(new ProducerRecord<string, string>(Topic, value: "v", key: "k"));
        });

        Assert.Contains(Topic, ex.Message, StringComparison.Ordinal);
        Assert.IsType<ThrowingSerializer<string>.BoomException>(ex.InnerException);

        // Nothing enqueued to the pump / no native send occurred.
        Assert.Equal(0, producer.HistoryCount());
    }

    // ---- Built-in serde null-input dependency (PLAN §3.4) + Serdes.ByteArray zero-copy identity ----

    [Fact]
    public void BuiltInSerdes_NullInput_MapToTombstone()
    {
        // The tombstone path relies on the reference-type built-in serdes returning null on null
        // input (Java parity), and Serdes.Null always returning null. The value-type serdes
        // (Int32/Int64/Double/Guid) cannot receive a null input (value types), so "null on null" is
        // N/A for them — they always return present bytes.
        Assert.Null(Serdes.String.Serialize(Topic, null!));
        Assert.Null(Serdes.ByteArray.Serialize(Topic, null!));
        Assert.Null(Serdes.Null.Serialize(Topic, null));
        Assert.Null(Serdes.Null.Serialize(Topic, new object()));
        Assert.NotNull(Serdes.Int64.Serialize(Topic, 0L));
    }

    [Fact]
    public void ByteArraySerde_IsZeroCopyIdentity()
    {
        // Serdes.ByteArray returns the user's array with NO extra copy (identity), so the
        // <byte[],byte[]> send path adds no value-sized allocation over the raw bytes (PLAN §3.6).
        byte[] value = Encoding.UTF8.GetBytes("value");
        Assert.Same(value, Serdes.ByteArray.Serialize(Topic, value));
    }
}
