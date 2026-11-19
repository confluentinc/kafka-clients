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
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal.Interop;

using Xunit;

using Origin = Confluent.Kafka.Common.Errors.RecordDeserializationException.DeserializationExceptionOrigin;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The sync <c>Poll</c>'s memory form on <see cref="MockConsumer{TKey, TValue}"/>: a deserializer that implements
/// <see cref="IDeserializerWithReadOnlyMemory{T}"/> is given the batch's own bytes, which stay valid while the memory
/// is reachable, past the next poll and the consumer's disposal.
/// </summary>
public sealed class ZeroCopyDeserializationTests
{
    private const string Topic = "zero-copy-topic";

    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    [Fact]
    public void AMemoryValue_StaysReadable_AfterPollReturns_AfterTheNextPoll_AndAfterTheConsumerIsDisposed()
    {
        MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer = Ready();
        ReadOnlyMemory<byte> key;
        ReadOnlyMemory<byte> value;
        using (TestTimeout.Disposal(consumer))
        {
            consumer.AddRecord(Topic, 0, 0, Bytes("key-0"), Bytes("value-0"));
            ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> record =
                Assert.Single(consumer.Poll(s_pollTimeout));
            key = record.Key()!.Value;
            value = record.Value()!.Value;
            AssertBytes("key-0", key);
            AssertBytes("value-0", value);

            consumer.AddRecord(Topic, 0, 1, Bytes("key-1"), Bytes("value-1"));
            ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> next =
                Assert.Single(consumer.Poll(s_pollTimeout));
            AssertBytes("value-1", next.Value()!.Value);
            AssertBytes("key-0", key);
            AssertBytes("value-0", value);

            Assert.Empty(consumer.Poll(s_pollTimeout));
            AssertBytes("value-0", value);
        }

        Collect();
        AssertBytes("key-0", key);
        AssertBytes("value-0", value);
    }

    [Fact]
    public void AMemoryValue_StaysReadable_HeldWithoutItsRecord()
    {
        MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer = Ready();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        ReadOnlyMemory<byte> value = PollOneValue(consumer, 0, "held-alone");
        Collect();
        ReadOnlyMemory<byte> next = PollOneValue(consumer, 1, "the-next-one");
        Collect();

        AssertBytes("held-alone", value);
        AssertBytes("the-next-one", next);
        using MemoryHandle pin = value.Slice(5).Pin();
        Collect();
        AssertBytes("alone", value.Slice(5));
    }

    [Fact]
    public void TheValuesOfOnePoll_StayReadable_HeldAsAListWithoutTheirRecords()
    {
        string[] expected = { "first-of-three", "second-of-three", "third-of-three" };
        MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer = Ready();
        List<ReadOnlyMemory<byte>> values;
        using (TestTimeout.Disposal(consumer))
        {
            values = PollValues(consumer, expected);
            Collect();
            AssertBytes("after-them", PollOneValue(consumer, expected.Length, "after-them"));
        }

        Collect();

        Assert.Equal(expected.Length, values.Count);
        for (int i = 0; i < expected.Length; i++)
        {
            AssertBytes(expected[i], values[i]);
            Assert.True(MemoryMarshal.TryGetMemoryManager(values[i], out BytesMemoryManager? manager));
            Assert.NotNull(manager);
        }
    }

    [Fact]
    public void ABatchNoMemoryReaches_IsCollected_WhileTheConsumerLives()
    {
        MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer = Ready();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);

        (WeakReference manager, WeakReference batch) = PollOneManager(consumer);
        Collect();

        Assert.False(manager.IsAlive);
        Assert.False(batch.IsAlive);
        consumer.AddRecord(Topic, 0, 1, null, Bytes("after"));
        AssertBytes("after", Assert.Single(consumer.Poll(s_pollTimeout)).Value()!.Value);
    }

    [Fact]
    public void ARetainedBatch_ReportsTheBytesOfItsKeysAndValues_AsMemoryPressure()
    {
        MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer = Ready();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, 0, 0, Bytes("key-0"), Bytes("value-0"));
        consumer.AddRecord(Topic, 0, 1, null, Bytes("second"));
        consumer.AddRecord(Topic, 0, 2, Bytes("k2"), Array.Empty<byte>());

        List<ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>> records =
            consumer.Poll(s_pollTimeout).ToList();

        Assert.Equal(3, records.Count);
        Assert.True(MemoryMarshal.TryGetMemoryManager(records[0].Value()!.Value, out BytesMemoryManager? first));
        Assert.True(MemoryMarshal.TryGetMemoryManager(records[1].Value()!.Value, out BytesMemoryManager? second));
        Assert.True(MemoryMarshal.TryGetMemoryManager(records[2].Key()!.Value, out BytesMemoryManager? third));
        Assert.Same(first!.Batch, second!.Batch);
        Assert.Same(first.Batch, third!.Batch);
        Assert.Equal(
            (3 * SafeConsumerRecordsHandle.RecordOverhead) + "key-0".Length + "value-0".Length + "second".Length
                + "k2".Length,
            first.Batch.Pressure);
    }

    [Fact]
    public void AMemoryKeyOrValue_IsTheBatchsOwnBytes_OneManagerEach_AndNoneForAnEmptyOrAbsentOne()
    {
        MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer = Ready();
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.AddRecord(Topic, 0, 0, Bytes("k"), Bytes("v"));
        consumer.AddRecord(Topic, 0, 1, Array.Empty<byte>(), Array.Empty<byte>());
        consumer.AddRecord(Topic, 0, 2, null, null);

        ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>[] records =
            consumer.Poll(s_pollTimeout).OrderBy(r => r.Offset()).ToArray();

        Assert.Equal(3, records.Length);
        ReadOnlyMemory<byte> key = records[0].Key()!.Value;
        ReadOnlyMemory<byte> value = records[0].Value()!.Value;
        Assert.False(MemoryMarshal.TryGetArray(key, out _));
        Assert.False(MemoryMarshal.TryGetArray(value, out _));
        Assert.True(MemoryMarshal.TryGetMemoryManager(key, out BytesMemoryManager? keyManager));
        Assert.True(MemoryMarshal.TryGetMemoryManager(value, out BytesMemoryManager? valueManager));
        Assert.NotSame(keyManager, valueManager);
        AssertBytes("k", key);
        AssertBytes("v", value);

        ReadOnlyMemory<byte> emptyKey = records[1].Key()!.Value;
        ReadOnlyMemory<byte> emptyValue = records[1].Value()!.Value;
        Assert.True(emptyKey.IsEmpty);
        Assert.True(emptyValue.IsEmpty);
        Assert.False(MemoryMarshal.TryGetMemoryManager<byte, BytesMemoryManager>(emptyKey, out _));
        Assert.False(MemoryMarshal.TryGetMemoryManager<byte, BytesMemoryManager>(emptyValue, out _));

        Assert.Null(records[2].Key());
        Assert.Null(records[2].Value());
    }

    [Fact]
    public void ADeserializerWithEveryForm_IsCalledThroughItsMemoryForm()
    {
        FormRecorder keys = new FormRecorder();
        FormRecorder values = new FormRecorder();
        MockConsumer<string?, string?> consumer = new MockConsumer<string?, string?>(keys, values);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_tp0 });
        consumer.Seek(s_tp0, 0);
        consumer.AddRecord(Topic, 0, 0, Bytes("k"), Bytes("v"));

        ConsumerRecord<string?, string?> record = Assert.Single(consumer.Poll(s_pollTimeout));

        Assert.Equal("k", record.Key());
        Assert.Equal("v", record.Value());
        Assert.Equal(new[] { "memory" }, keys.Forms);
        Assert.Equal(new[] { "memory" }, values.Forms);
        Assert.Same(record.Headers(), keys.Headers);
        Assert.Same(record.Headers(), values.Headers);
    }

    [Fact]
    public void ADeserializerWithTheHeadersForm_IsCalledThroughIt_AndAPlainOneThroughItsSpanForm()
    {
        HeadersFormRecorder keys = new HeadersFormRecorder();
        SpanFormRecorder values = new SpanFormRecorder();
        MockConsumer<string?, string?> consumer = new MockConsumer<string?, string?>(keys, values);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_tp0 });
        consumer.Seek(s_tp0, 0);
        consumer.AddRecord(Topic, 0, 0, Bytes("k"), Bytes("v"));

        ConsumerRecord<string?, string?> record = Assert.Single(consumer.Poll(s_pollTimeout));

        Assert.Equal("k", record.Key());
        Assert.Equal("v", record.Value());
        Assert.Equal(new[] { "headers" }, keys.Forms);
        Assert.Equal(new[] { "span" }, values.Forms);
    }

    [Fact]
    public void AMemoryValueThatFailsMidBatch_IsPolledAgain_AndTheRecordsBeforeItStayReadable()
    {
        FailingMemoryDeserializer values = new FailingMemoryDeserializer(failOn: "x");
        MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer =
            new MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(
                Serdes.ReadOnlyMemory().Deserializer(), values);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { s_tp0 });
        consumer.Seek(s_tp0, 0);
        consumer.AddRecord(Topic, 0, 0, Bytes("k0"), Bytes("before"));
        consumer.AddRecord(Topic, 0, 1, Bytes("k1"), Bytes("x"));

        ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> first =
            Assert.Single(consumer.Poll(s_pollTimeout));
        ReadOnlyMemory<byte> before = first.Value()!.Value;
        Assert.Equal(0L, first.Offset());
        AssertBytes("before", before);
        Assert.Equal(1L, consumer.Position(s_tp0));

        consumer.AddRecord(Topic, 0, 1, Bytes("k1"), Bytes("x"));
        RecordDeserializationException e =
            Assert.Throws<RecordDeserializationException>(() => consumer.Poll(s_pollTimeout));

        Assert.Equal(Origin.VALUE, e.Origin());
        Assert.Equal(s_tp0, e.TopicPartition());
        Assert.Equal(1L, e.Offset());
        Assert.Equal(Bytes("k1"), e.KeyBuffer());
        Assert.Equal(Bytes("x"), e.ValueBuffer());
        Assert.Equal(
            "Error deserializing VALUE for partition zero-copy-topic-0 at offset 1. If needed, please seek past the "
                + "record to continue consumption.",
            e.Message);
        Assert.Same(values.Thrown.Last(), e.InnerException);
        Assert.Equal(1L, consumer.Position(s_tp0));
        Collect();
        AssertBytes("before", before);
        AssertBytes("k0", first.Key()!.Value);

        consumer.Seek(s_tp0, e.Offset() + 1);
        consumer.AddRecord(Topic, 0, 2, null, Bytes("after"));
        ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> next =
            Assert.Single(consumer.Poll(s_pollTimeout));
        Assert.Equal(2L, next.Offset());
        AssertBytes("after", next.Value()!.Value);
        AssertBytes("before", before);
    }

    private static MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> Ready()
    {
        MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer =
            new MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(
                Serdes.ReadOnlyMemory().Deserializer(), Serdes.ReadOnlyMemory().Deserializer());
        consumer.Assign(new[] { s_tp0 });
        consumer.Seek(s_tp0, 0);
        return consumer;
    }

    // Not inlined, so the record and its batch are unreachable once it returns and only the memory keeps the bytes.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static ReadOnlyMemory<byte> PollOneValue(
        MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer, long offset, string value)
    {
        consumer.AddRecord(Topic, 0, offset, Bytes("key"), Bytes(value));
        return Assert.Single(consumer.Poll(s_pollTimeout)).Value()!.Value;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static List<ReadOnlyMemory<byte>> PollValues(
        MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer, string[] values)
    {
        for (int i = 0; i < values.Length; i++)
        {
            consumer.AddRecord(Topic, 0, i, Bytes("key"), Bytes(values[i]));
        }

        ConsumerRecords<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> records = consumer.Poll(s_pollTimeout);
        return records.Select(r => r.Value()!.Value).ToList();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (WeakReference Manager, WeakReference Batch) PollOneManager(
        MockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer)
    {
        ReadOnlyMemory<byte> value = PollOneValue(consumer, 0, "dropped");
        Assert.True(MemoryMarshal.TryGetMemoryManager(value, out BytesMemoryManager? manager));

        // The batch's reference tracks resurrection, so it dies only after the finalizer that releases the batch.
        return (new WeakReference(manager), new WeakReference(manager!.Batch, trackResurrection: true));
    }

    private static void Collect()
    {
        for (int i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
    }

    private static void AssertBytes(string expected, ReadOnlyMemory<byte> actual) =>
        Assert.True(
            actual.Span.SequenceEqual(Bytes(expected)),
            "Expected the bytes of '" + expected + "', got '" + Encoding.UTF8.GetString(actual.ToArray()) + "'.");

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    /// <summary>A UTF-8 deserializer with the three forms that records which one the consumer called.</summary>
    private sealed class FormRecorder : IDeserializerWithHeaders<string?>, IDeserializerWithReadOnlyMemory<string?>
    {
        internal List<string> Forms { get; } = new List<string>();

        internal Headers? Headers { get; private set; }

        public string? Deserialize(string topic, ReadOnlySpan<byte> data)
        {
            Forms.Add("span");
            return Encoding.UTF8.GetString(data.ToArray());
        }

        public string? Deserialize(string topic, Headers headers, ReadOnlySpan<byte> data)
        {
            Forms.Add("headers");
            return Encoding.UTF8.GetString(data.ToArray());
        }

        public string? Deserialize(string topic, Headers headers, ReadOnlyMemory<byte> data)
        {
            Forms.Add("memory");
            Headers = headers;
            return Encoding.UTF8.GetString(data.ToArray());
        }
    }

    /// <summary>A UTF-8 deserializer with the span and headers forms that records which one the consumer called.</summary>
    private sealed class HeadersFormRecorder : IDeserializerWithHeaders<string?>
    {
        internal List<string> Forms { get; } = new List<string>();

        public string? Deserialize(string topic, ReadOnlySpan<byte> data)
        {
            Forms.Add("span");
            return Encoding.UTF8.GetString(data.ToArray());
        }

        public string? Deserialize(string topic, Headers headers, ReadOnlySpan<byte> data)
        {
            Forms.Add("headers");
            return Encoding.UTF8.GetString(data.ToArray());
        }
    }

    /// <summary>A UTF-8 deserializer with the span form only that records each call.</summary>
    private sealed class SpanFormRecorder : IDeserializer<string?>
    {
        internal List<string> Forms { get; } = new List<string>();

        public string? Deserialize(string topic, ReadOnlySpan<byte> data)
        {
            Forms.Add("span");
            return Encoding.UTF8.GetString(data.ToArray());
        }
    }

    /// <summary>A memory-form deserializer that keeps the bytes as given and throws for the one value it is told to.</summary>
    private sealed class FailingMemoryDeserializer : IDeserializerWithReadOnlyMemory<ReadOnlyMemory<byte>?>
    {
        private readonly string _failOn;

        internal FailingMemoryDeserializer(string failOn)
        {
            _failOn = failOn;
        }

        internal List<Exception> Thrown { get; } = new List<Exception>();

        public ReadOnlyMemory<byte>? Deserialize(string topic, ReadOnlySpan<byte> data) =>
            throw new InvalidOperationException("The consumer calls the memory form.");

        public ReadOnlyMemory<byte>? Deserialize(string topic, Headers headers, ReadOnlyMemory<byte> data)
        {
            if (Encoding.UTF8.GetString(data.ToArray()) == _failOn)
            {
                FormatException thrown = new FormatException("bad " + _failOn);
                Thrown.Add(thrown);
                throw thrown;
            }

            return data;
        }
    }
}
