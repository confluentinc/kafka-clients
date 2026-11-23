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
using System.Threading;
using System.Threading.Tasks;

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
/// The async <c>Poll</c>'s deserialization on <see cref="AsyncMockConsumer{TKey, TValue}"/>: a deserializer that is
/// not a built-in is awaited one record at a time, over a memory revoked when its call completes unless it implements
/// <see cref="IAsyncDeserializerWithRetention{T}"/>, and a failure or a cancellation mid-batch seeks back.
/// </summary>
public sealed class AsyncConsumerDeserializationTests
{
    private const string Topic = "async-deser-topic";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ACallScopedMemory_IsRevokedOnceTheCallCompletes_AndAPinTakenDuringItStaysValid(bool awaits)
    {
        KeepingDeserializer values = new KeepingDeserializer(awaits);
        AsyncMockConsumer<string, string> consumer =
            new AsyncMockConsumer<string, string>(new AsyncStringDeserializer(), values);
        ReadOnlyMemory<byte> kept;
        await using (TestTimeout.AsyncDisposal(consumer))
        {
            await Ready(consumer);
            consumer.AddRecord(Topic, 0, 0, Bytes("key"), Bytes("kept"));

            ConsumerRecord<string, string> record = Assert.Single(await Poll(consumer));

            Assert.Equal("key", record.Key());
            Assert.Equal("kept", record.Value());
            kept = Assert.Single(values.Kept);
            Assert.Equal(4, kept.Length);
            Assert.True(IsNative(kept));
            AssertRevoked(kept);
            Assert.Equal("kept", Read(values.Pins[0], 4));

            consumer.AddRecord(Topic, 0, 1, null, Bytes("next"));
            Assert.Equal("next", Assert.Single(await Poll(consumer)).Value());
            Collect();
            Assert.Equal("kept", Read(values.Pins[0], 4));
        }

        Collect();
        AssertRevoked(kept);
        Assert.Equal(2, values.Pins.Count);
        foreach (MemoryHandle pin in values.Pins)
        {
            Assert.Equal("kept", Read(pin, 4));
            pin.Dispose();
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ARetainedMemory_StaysReadable_AfterPollReturns_AfterTheNextPoll_AndAfterTheConsumerIsDisposed(
        bool awaits)
    {
        AsyncMockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer =
            new AsyncMockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(
                new RetainingDeserializer(awaits), new RetainingDeserializer(awaits));
        ReadOnlyMemory<byte> key;
        ReadOnlyMemory<byte> value;
        await using (TestTimeout.AsyncDisposal(consumer))
        {
            await Ready(consumer);
            consumer.AddRecord(Topic, 0, 0, Bytes("key-0"), Bytes("value-0"));
            ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> record = Assert.Single(await Poll(consumer));
            key = record.Key()!.Value;
            value = record.Value()!.Value;
            AssertBytes("key-0", key);
            AssertBytes("value-0", value);
            Assert.False(MemoryMarshal.TryGetArray(key, out _));
            Assert.False(MemoryMarshal.TryGetArray(value, out _));
            Assert.True(MemoryMarshal.TryGetMemoryManager(key, out BytesMemoryManager? keyManager));
            Assert.True(MemoryMarshal.TryGetMemoryManager(value, out BytesMemoryManager? valueManager));
            Assert.NotSame(keyManager, valueManager);

            consumer.AddRecord(Topic, 0, 1, Bytes("key-1"), Bytes("value-1"));
            ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> next = Assert.Single(await Poll(consumer));
            AssertBytes("value-1", next.Value()!.Value);
            Collect();
            AssertBytes("key-0", key);
            AssertBytes("value-0", value);
        }

        Collect();
        AssertBytes("key-0", key);
        AssertBytes("value-0", value);
        using MemoryHandle pin = value.Slice(6).Pin();
        Assert.Equal("0", Read(pin, 1));
    }

    [Fact]
    public async Task ARetainedMemory_OfAnEmptyOrAbsentKeyOrValue_HasNoManager()
    {
        RetainingDeserializer keys = new RetainingDeserializer(awaits: false);
        RetainingDeserializer values = new RetainingDeserializer(awaits: false);
        AsyncMockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer =
            new AsyncMockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(keys, values);
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await Ready(consumer);
        consumer.AddRecord(Topic, 0, 0, Array.Empty<byte>(), Array.Empty<byte>());
        consumer.AddRecord(Topic, 0, 1, null, null);

        ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>[] records =
            (await Poll(consumer)).OrderBy(r => r.Offset()).ToArray();

        Assert.Equal(2, records.Length);
        ReadOnlyMemory<byte> emptyKey = records[0].Key()!.Value;
        ReadOnlyMemory<byte> emptyValue = records[0].Value()!.Value;
        Assert.True(emptyKey.IsEmpty);
        Assert.True(emptyValue.IsEmpty);
        Assert.False(MemoryMarshal.TryGetMemoryManager<byte, BytesMemoryManager>(emptyKey, out _));
        Assert.False(MemoryMarshal.TryGetMemoryManager<byte, BytesMemoryManager>(emptyValue, out _));
        Assert.Null(records[1].Key());
        Assert.Null(records[1].Value());
        Assert.Equal(1, keys.Calls);
        Assert.Equal(1, values.Calls);
    }

    [Fact]
    public async Task AnAwaitingDeserializer_YieldsTheRecordsInOrder_TheKeyBeforeTheValue()
    {
        List<string> calls = new List<string>();
        AsyncMockConsumer<string, string> consumer = new AsyncMockConsumer<string, string>(
            new OrderedDeserializer("key", calls), new OrderedDeserializer("value", calls));
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await Ready(consumer);
        for (int i = 0; i < 3; i++)
        {
            consumer.AddRecord(Topic, 0, i, Bytes("k" + i), Bytes("v" + i));
        }

        ConsumerRecord<string, string>[] records = (await Poll(consumer)).ToArray();

        Assert.Equal(new long[] { 0, 1, 2 }, records.Select(r => r.Offset()));
        Assert.Equal(new[] { "k0", "k1", "k2" }, records.Select(r => r.Key()));
        Assert.Equal(new[] { "v0", "v1", "v2" }, records.Select(r => r.Value()));
        Assert.Equal(
            new[]
            {
                "key k0 called", "key k0 completed", "value v0 called", "value v0 completed",
                "key k1 called", "key k1 completed", "value v1 called", "value v1 completed",
                "key k2 called", "key k2 completed", "value v2 called", "value v2 completed",
            },
            calls);
    }

    [Fact]
    public async Task ADeserializerCompletedLater_HoldsThePollUntilThen_AndTheNextRecordIsNotStartedBefore()
    {
        GatedDeserializer values = new GatedDeserializer(gateOn: "v0");
        AsyncMockConsumer<string, string> consumer =
            new AsyncMockConsumer<string, string>(new AsyncStringDeserializer(), values);
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await Ready(consumer);
        consumer.AddRecord(Topic, 0, 0, Bytes("k0"), Bytes("v0"));
        consumer.AddRecord(Topic, 0, 1, Bytes("k1"), Bytes("v1"));

        Task<ConsumerRecords<string, string>> poll = consumer.Poll(s_pollTimeout);
        await TestTimeout.Run(() => values.Entered.Task, s_deadline);

        Assert.False(poll.IsCompleted);
        Assert.Equal(new[] { "v0" }, values.Seen);

        values.Gate.SetResult(true);
        ConsumerRecord<string, string>[] records = (await TestTimeout.Run(() => poll, s_deadline)).ToArray();

        Assert.Equal(new[] { "v0", "v1" }, records.Select(r => r.Value()));
        Assert.Equal(new[] { "k0", "k1" }, records.Select(r => r.Key()));
        Assert.Equal(new[] { "v0", "v1" }, values.Seen);
    }

    [Theory]
    [InlineData(Failure.Thrown)]
    [InlineData(Failure.Faulted)]
    [InlineData(Failure.ThrownAfterAnAwait)]
    public async Task AValueThatFailsMidBatch_IsReturnedAfterTheRecordsBeforeIt_ThenThrown_ThenPolledAgain(
        Failure failure)
    {
        FailingDeserializer values = new FailingDeserializer(failure, failOn: "x");
        AsyncMockConsumer<string, string> consumer =
            new AsyncMockConsumer<string, string>(new AsyncStringDeserializer(), values);
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await Ready(consumer);
        consumer.AddRecord(Topic, 0, 0, Bytes("k0"), Bytes("before"));
        consumer.AddRecord(Topic, 0, 1, Bytes("k1"), Bytes("x"));
        consumer.AddRecord(Topic, 0, 2, Bytes("k2"), Bytes("after"));

        ConsumerRecord<string, string> first = Assert.Single(await Poll(consumer));

        Assert.Equal(0L, first.Offset());
        Assert.Equal("before", first.Value());
        Assert.Equal(new[] { "before", "x" }, values.Seen);
        Assert.Equal(1L, await Position(consumer));

        consumer.AddRecord(Topic, 0, 1, Bytes("k1"), Bytes("x"));
        RecordDeserializationException e =
            await Assert.ThrowsAsync<RecordDeserializationException>(() => Poll(consumer));

        Assert.Equal(Origin.VALUE, e.Origin());
        Assert.Equal(s_tp0, e.TopicPartition());
        Assert.Equal(1L, e.Offset());
        Assert.Equal(Bytes("k1"), e.KeyBuffer());
        Assert.Equal(Bytes("x"), e.ValueBuffer());
        Assert.Equal(
            "Error deserializing VALUE for partition async-deser-topic-0 at offset 1. If needed, please seek past the "
                + "record to continue consumption.",
            e.Message);
        Assert.Same(values.Thrown.Last(), e.InnerException);
        Assert.Equal("bad x", e.InnerException!.Message);
        Assert.Equal(1L, await Position(consumer));

        values.Fails = false;
        consumer.AddRecord(Topic, 0, 1, Bytes("k1"), Bytes("x"));
        ConsumerRecord<string, string> again = Assert.Single(await Poll(consumer));

        Assert.Equal(1L, again.Offset());
        Assert.Equal("x", again.Value());
        Assert.Equal(2L, await Position(consumer));
    }

    [Theory]
    [InlineData(Failure.Thrown)]
    [InlineData(Failure.Faulted)]
    [InlineData(Failure.ThrownAfterAnAwait)]
    public async Task AKeyThatFails_ThrowsForTheKey_WithBothBuffers_AndLeavesTheValueUnread(Failure failure)
    {
        FailingDeserializer keys = new FailingDeserializer(failure, failOn: "x");
        FailingDeserializer values = new FailingDeserializer(failure, failOn: "never");
        AsyncMockConsumer<string, string> consumer = new AsyncMockConsumer<string, string>(keys, values);
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await Ready(consumer);
        consumer.AddRecord(Topic, 0, 0, Bytes("x"), Bytes("unread"));

        RecordDeserializationException e =
            await Assert.ThrowsAsync<RecordDeserializationException>(() => Poll(consumer));

        Assert.Equal(Origin.KEY, e.Origin());
        Assert.Equal(0L, e.Offset());
        Assert.Equal(Bytes("x"), e.KeyBuffer());
        Assert.Equal(Bytes("unread"), e.ValueBuffer());
        Assert.Equal(
            "Error deserializing KEY for partition async-deser-topic-0 at offset 0. If needed, please seek past the "
                + "record to continue consumption.",
            e.Message);
        Assert.Same(Assert.Single(keys.Thrown), e.InnerException);
        Assert.Empty(values.Seen);
        Assert.Equal(0L, await Position(consumer));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ACancellationMidBatch_EndsThePollCancelled_SeeksBackToTheFirstRecord_AndTheNextPollReturnsIt(
        bool observed)
    {
        using CancellationTokenSource cancellation = new CancellationTokenSource();
        CancelledDeserializer values = new CancelledDeserializer(cancellation, cancelOn: "v1", observed);
        AsyncMockConsumer<string, string> consumer =
            new AsyncMockConsumer<string, string>(new AsyncStringDeserializer(), values);
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await Ready(consumer);
        for (int i = 0; i < 3; i++)
        {
            consumer.AddRecord(Topic, 0, i, Bytes("k" + i), Bytes("v" + i));
        }

        OperationCanceledException e = await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => TestTimeout.Run(() => consumer.Poll(s_pollTimeout, cancellation.Token), s_deadline));

        Assert.Equal(cancellation.Token, e.CancellationToken);
        Assert.Equal(new[] { "v0", "v1" }, values.Seen);
        Assert.Equal(new[] { cancellation.Token, cancellation.Token }, values.Tokens);
        Assert.Equal(0L, await Position(consumer));

        for (int i = 0; i < 3; i++)
        {
            consumer.AddRecord(Topic, 0, i, Bytes("k" + i), Bytes("v" + i));
        }

        values.Cancels = false;
        ConsumerRecord<string, string>[] records = (await Poll(consumer)).ToArray();

        Assert.Equal(new long[] { 0, 1, 2 }, records.Select(r => r.Offset()));
        Assert.Equal(new[] { "v0", "v1", "v2" }, records.Select(r => r.Value()));
        Assert.Equal(3L, await Position(consumer));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ADeserializerThatDisposesTheConsumerMidBatch_ReturnsTheRecords_AndTheConsumerIsClosed(
        bool closes)
    {
        ConsumerDisposingDeserializer values = new ConsumerDisposingDeserializer(closes);
        AsyncMockConsumer<string, string> consumer =
            new AsyncMockConsumer<string, string>(new AsyncStringDeserializer(), values);
        values.Target = consumer;
        try
        {
            await Ready(consumer);
            consumer.AddRecord(Topic, 0, 0, Bytes("k0"), Bytes("a"));
            consumer.AddRecord(Topic, 0, 1, Bytes("k1"), Bytes("b"));
        }
        catch
        {
            await TestTimeout.DisposeAsync(consumer);
            throw;
        }

        ConsumerRecord<string, string>[] records = (await Poll(consumer)).ToArray();

        Assert.Equal(new[] { "a", "b" }, records.Select(r => r.Value()));
        Assert.Equal(new[] { "k0", "k1" }, records.Select(r => r.Key()));
        Assert.Equal(2, values.Calls);
        Assert.Throws<IllegalStateException>(() => consumer.Assignment());
    }

    [Fact]
    public async Task ABuiltInKey_WithEachKindOfValue_AndTheOtherWayRound()
    {
        await PollsBothSides(Span(), Span());
        await PollsBothSides(Span(), Memory());
        await PollsBothSides(Span(), Retaining());
        await PollsBothSides(Span(), CallScoped());
        await PollsBothSides(Memory(), Span());
        await PollsBothSides(Retaining(), Span());
        await PollsBothSides(CallScoped(), Span());
    }

    [Fact]
    public async Task AMemoryKey_WithEachKindOfValue_AndTheOtherWayRound()
    {
        await PollsBothSides(Memory(), Memory());
        await PollsBothSides(Memory(), Retaining());
        await PollsBothSides(Memory(), CallScoped());
        await PollsBothSides(Retaining(), Memory());
        await PollsBothSides(CallScoped(), Memory());
    }

    [Fact]
    public async Task ARetainingKey_WithACallScopedValue_TheOtherWayRound_AndEachWithItself()
    {
        await PollsBothSides(Retaining(), CallScoped());
        await PollsBothSides(CallScoped(), Retaining());
        await PollsBothSides(Retaining(), Retaining());
        await PollsBothSides(CallScoped(), CallScoped());
    }

    // Polls a record with a key and a value, one with both empty and one with neither, then another batch, and reads
    // the first batch's keys and values again, also once the consumer is disposed: a memory a side may keep is still
    // the record's bytes, and one it may not keep was revoked.
    private static async Task PollsBothSides<TKey, TValue>(Side<TKey> keys, Side<TValue> values)
    {
        string sides = keys.Name + " key, " + values.Name + " value";
        AsyncMockConsumer<TKey, TValue> consumer =
            new AsyncMockConsumer<TKey, TValue>(keys.Deserializer, values.Deserializer);
        ReadOnlyMemory<byte> key;
        ReadOnlyMemory<byte> value;
        await using (TestTimeout.AsyncDisposal(consumer))
        {
            await Ready(consumer);
            consumer.AddRecord(Topic, 0, 0, Bytes("the-key"), Bytes("the-value"));
            consumer.AddRecord(Topic, 0, 1, Array.Empty<byte>(), Array.Empty<byte>());
            consumer.AddRecord(Topic, 0, 2, null, null);

            ConsumerRecord<TKey, TValue>[] records = (await Poll(consumer)).ToArray();

            Assert.Equal(new long[] { 0, 1, 2 }, records.Select(r => r.Offset()));
            key = keys.Bytes(records[0].Key())!.Value;
            value = values.Bytes(records[0].Value())!.Value;
            AssertBytes("the-key", key);
            AssertBytes("the-value", value);
            Assert.True(keys.Bytes(records[1].Key())!.Value.IsEmpty, sides);
            Assert.True(values.Bytes(records[1].Value())!.Value.IsEmpty, sides);
            Assert.Null(records[2].Key());
            Assert.Null(records[2].Value());
            Assert.True(keys.KeepsTheBytes == IsNative(key), sides);
            Assert.True(values.KeepsTheBytes == IsNative(value), sides);

            consumer.AddRecord(Topic, 0, 3, Bytes("next-key"), Bytes("next-value"));
            ConsumerRecord<TKey, TValue> next = Assert.Single(await Poll(consumer));
            AssertBytes("next-key", keys.Bytes(next.Key())!.Value);
            AssertBytes("next-value", values.Bytes(next.Value())!.Value);
            Collect();
            AssertBytes("the-key", key);
            AssertBytes("the-value", value);
        }

        Collect();
        AssertBytes("the-key", key);
        AssertBytes("the-value", value);
        keys.AssertTheMemoriesItWasLent(sides);
        values.AssertTheMemoriesItWasLent(sides);
    }

    private static Side<byte[]> Span() =>
        new Side<byte[]>(
            "span",
            new AsyncByteArrayDeserializer(),
            bytes => bytes is null ? null : new ReadOnlyMemory<byte>(bytes),
            keepsTheBytes: false,
            null);

    private static Side<ReadOnlyMemory<byte>?> Memory() =>
        new Side<ReadOnlyMemory<byte>?>(
            "memory", new AsyncReadOnlyMemoryDeserializer(), bytes => bytes, keepsTheBytes: true, null);

    private static Side<ReadOnlyMemory<byte>?> Retaining() =>
        new Side<ReadOnlyMemory<byte>?>(
            "retaining", new RetainingDeserializer(awaits: true), bytes => bytes, keepsTheBytes: true, null);

    private static Side<ReadOnlyMemory<byte>?> CallScoped()
    {
        CopyingDeserializer copying = new CopyingDeserializer();
        return new Side<ReadOnlyMemory<byte>?>("call-scoped", copying, bytes => bytes, keepsTheBytes: false, copying);
    }

    private static void AssertRevoked(ReadOnlyMemory<byte> lent)
    {
        ObjectDisposedException read = Assert.Throws<ObjectDisposedException>(() => lent.ToArray());
        Assert.Equal(BytesMemoryManager.RevokedMessage, read.Message);
        ObjectDisposedException pinned = Assert.Throws<ObjectDisposedException>(() => lent.Pin());
        Assert.Equal(BytesMemoryManager.RevokedMessage, pinned.Message);
    }

    private static bool IsNative(ReadOnlyMemory<byte> memory) =>
        MemoryMarshal.TryGetMemoryManager<byte, BytesMemoryManager>(memory, out _);

    private static async Task Ready<TKey, TValue>(AsyncMockConsumer<TKey, TValue> consumer)
    {
        await TestTimeout.Run(() => consumer.Assign(new[] { s_tp0 }), s_deadline);
        await TestTimeout.Run(() => consumer.Seek(s_tp0, 0), s_deadline);
    }

    private static Task<ConsumerRecords<TKey, TValue>> Poll<TKey, TValue>(AsyncMockConsumer<TKey, TValue> consumer) =>
        TestTimeout.Run(() => consumer.Poll(s_pollTimeout), s_deadline);

    private static Task<long> Position<TKey, TValue>(AsyncMockConsumer<TKey, TValue> consumer) =>
        TestTimeout.Run(() => consumer.Position(s_tp0), s_deadline);

    private static void Collect()
    {
        for (int i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
    }

    private static unsafe string Read(MemoryHandle pin, int length) =>
        Encoding.UTF8.GetString(new ReadOnlySpan<byte>(pin.Pointer, length).ToArray());

    private static void AssertBytes(string expected, ReadOnlyMemory<byte> actual) =>
        Assert.True(
            actual.Span.SequenceEqual(Bytes(expected)),
            "Expected the bytes of '" + expected + "', got '" + Encoding.UTF8.GetString(actual.ToArray()) + "'.");

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    private static string Text(ReadOnlyMemory<byte> data) => Encoding.UTF8.GetString(data.ToArray());

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ARetainedBatch_ReportsTheBytesOfItsKeysAndValues_AsMemoryPressure(bool awaits)
    {
        AsyncMockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer =
            new AsyncMockConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(
                new RetainingDeserializer(awaits), new RetainingDeserializer(awaits));
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await Ready(consumer);
        consumer.AddRecord(Topic, 0, 0, Bytes("key-0"), Bytes("value-0"));
        consumer.AddRecord(Topic, 0, 1, null, Bytes("second"));
        consumer.AddRecord(Topic, 0, 2, Bytes("k2"), Array.Empty<byte>());

        List<ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>> records = (await Poll(consumer)).ToList();

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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ACallScopedBatch_ReportsItsRecordsAsMemoryPressure_AndOutlivesItsPins(bool awaits)
    {
        KeepingDeserializer values = new KeepingDeserializer(awaits);
        AsyncMockConsumer<string, string> consumer =
            new AsyncMockConsumer<string, string>(new AsyncStringDeserializer(), values);
        await using IAsyncDisposable consumerDisposal = TestTimeout.AsyncDisposal(consumer);
        await Ready(consumer);
        consumer.AddRecord(Topic, 0, 0, Bytes("key"), Bytes("kept"));

        Assert.Equal("kept", Assert.Single(await Poll(consumer)).Value());

        Assert.True(MemoryMarshal.TryGetMemoryManager(Assert.Single(values.Kept), out BytesMemoryManager? manager));
        SafeConsumerRecordsHandle batch = manager!.Batch;
        Assert.Equal(SafeConsumerRecordsHandle.RecordOverhead + "key".Length + "kept".Length, batch.Pressure);
        Assert.Equal(2, values.Pins.Count);
        values.Pins[0].Dispose();
        Assert.False(batch.IsClosed);
        Assert.Equal("kept", Read(values.Pins[1], 4));

        values.Pins[1].Dispose();

        // Only the garbage collector releases a batch, so it outlives the pins.
        Assert.False(batch.IsClosed);
    }

    public enum Failure
    {
        Thrown,
        Faulted,
        ThrownAfterAnAwait,
    }

    /// <summary>A UTF-8 deserializer that keeps each memory it is given, and two pins of it.</summary>
    private sealed class KeepingDeserializer : IAsyncDeserializer<string>
    {
        private readonly bool _awaits;

        internal KeepingDeserializer(bool awaits)
        {
            _awaits = awaits;
        }

        internal List<ReadOnlyMemory<byte>> Kept { get; } = new List<ReadOnlyMemory<byte>>();

        internal List<MemoryHandle> Pins { get; } = new List<MemoryHandle>();

        public async ValueTask<string?> Deserialize(
            string topic, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            if (Kept.Count == 0)
            {
                Pins.Add(data.Pin());
            }

            if (_awaits)
            {
                await Task.Yield();
            }

            if (Kept.Count == 0)
            {
                Pins.Add(data.Pin());
            }

            Kept.Add(data);
            return Text(data);
        }
    }

    /// <summary>A deserializer that keeps the bytes as given, which its interface allows.</summary>
    private sealed class RetainingDeserializer : IAsyncDeserializerWithRetention<ReadOnlyMemory<byte>?>
    {
        private readonly bool _awaits;

        internal RetainingDeserializer(bool awaits)
        {
            _awaits = awaits;
        }

        internal int Calls { get; private set; }

        public ValueTask<ReadOnlyMemory<byte>?> Deserialize(
            string topic, ReadOnlyMemory<byte> data, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("the consumer called the form without headers");

        public async ValueTask<ReadOnlyMemory<byte>?> Deserialize(
            string topic, IHeaders headers, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            Calls++;
            if (_awaits)
            {
                await Task.Yield();
            }

            return data;
        }
    }

    /// <summary>A UTF-8 deserializer that awaits, recording when it was called and when it completed.</summary>
    private sealed class OrderedDeserializer : IAsyncDeserializer<string>
    {
        private readonly string _side;
        private readonly List<string> _calls;

        internal OrderedDeserializer(string side, List<string> calls)
        {
            _side = side;
            _calls = calls;
        }

        public async ValueTask<string?> Deserialize(
            string topic, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            string text = Text(data);
            lock (_calls)
            {
                _calls.Add(_side + " " + text + " called");
            }

            await Task.Yield();
            lock (_calls)
            {
                _calls.Add(_side + " " + Text(data) + " completed");
            }

            return text;
        }
    }

    /// <summary>A UTF-8 deserializer that completes one value only once the test lets it.</summary>
    private sealed class GatedDeserializer : IAsyncDeserializer<string>
    {
        private readonly string _gateOn;

        internal GatedDeserializer(string gateOn)
        {
            _gateOn = gateOn;
        }

        internal TaskCompletionSource<bool> Entered { get; } =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource<bool> Gate { get; } =
            new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        internal List<string> Seen { get; } = new List<string>();

        public async ValueTask<string?> Deserialize(
            string topic, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            string text = Text(data);
            lock (Seen)
            {
                Seen.Add(text);
            }

            if (text == _gateOn)
            {
                Entered.SetResult(true);
                await Gate.Task;
            }

            return Text(data);
        }
    }

    /// <summary>A UTF-8 deserializer that fails for one text, in the way it is told to.</summary>
    private sealed class FailingDeserializer : IAsyncDeserializer<string>
    {
        private readonly Failure _failure;
        private readonly string _failOn;

        internal FailingDeserializer(Failure failure, string failOn)
        {
            _failure = failure;
            _failOn = failOn;
        }

        internal bool Fails { get; set; } = true;

        internal List<string> Seen { get; } = new List<string>();

        internal List<Exception> Thrown { get; } = new List<Exception>();

        public ValueTask<string?> Deserialize(
            string topic, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            string text = Text(data);
            Seen.Add(text);
            if (!Fails || text != _failOn)
            {
                return new ValueTask<string?>(text);
            }

            FormatException thrown = new FormatException("bad " + text);
            Thrown.Add(thrown);
            return _failure switch
            {
                Failure.Thrown => throw thrown,
                Failure.Faulted => new ValueTask<string?>(Task.FromException<string?>(thrown)),
                _ => ThrowAfterAnAwait(thrown),
            };
        }

        private static async ValueTask<string?> ThrowAfterAnAwait(Exception thrown)
        {
            await Task.Yield();
            throw thrown;
        }
    }

    /// <summary>
    /// A UTF-8 deserializer that cancels the poll's token on one value, then either observes it or completes.
    /// </summary>
    private sealed class CancelledDeserializer : IAsyncDeserializer<string>
    {
        private readonly CancellationTokenSource _cancellation;
        private readonly string _cancelOn;
        private readonly bool _observed;

        internal CancelledDeserializer(CancellationTokenSource cancellation, string cancelOn, bool observed)
        {
            _cancellation = cancellation;
            _cancelOn = cancelOn;
            _observed = observed;
        }

        internal bool Cancels { get; set; } = true;

        internal List<string> Seen { get; } = new List<string>();

        internal List<CancellationToken> Tokens { get; } = new List<CancellationToken>();

        public async ValueTask<string?> Deserialize(
            string topic, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            string text = Text(data);
            if (Cancels)
            {
                Seen.Add(text);
                Tokens.Add(cancellationToken);
            }

            if (Cancels && text == _cancelOn)
            {
                Task observing = Task.Delay(Timeout.Infinite, cancellationToken);
                _cancellation.Cancel();
                if (_observed)
                {
                    await observing;
                }
            }

            return text;
        }
    }

    /// <summary>A UTF-8 deserializer that counts its calls and closes or disposes its target on the first one.</summary>
    private sealed class ConsumerDisposingDeserializer : IAsyncDeserializer<string>
    {
        private readonly bool _closes;

        internal ConsumerDisposingDeserializer(bool closes)
        {
            _closes = closes;
        }

        internal AsyncMockConsumer<string, string>? Target { get; set; }

        internal int Calls { get; private set; }

        public async ValueTask<string?> Deserialize(
            string topic, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            Calls++;
            if (Calls == 1 && _closes)
            {
                await Target!.Close(CancellationToken.None);
            }
            else if (Calls == 1)
            {
                await Target!.DisposeAsync();
            }

            return Text(data);
        }
    }

    /// <summary>One side of a consumer: a deserializer of one kind, and how to read what it returns.</summary>
    private sealed class Side<T>
    {
        private readonly CopyingDeserializer? _callScoped;

        internal Side(
            string name,
            IAsyncDeserializer<T> deserializer,
            Func<T?, ReadOnlyMemory<byte>?> bytes,
            bool keepsTheBytes,
            CopyingDeserializer? callScoped)
        {
            Name = name;
            Deserializer = deserializer;
            Bytes = bytes;
            KeepsTheBytes = keepsTheBytes;
            _callScoped = callScoped;
        }

        internal string Name { get; }

        internal IAsyncDeserializer<T> Deserializer { get; }

        internal Func<T?, ReadOnlyMemory<byte>?> Bytes { get; }

        internal bool KeepsTheBytes { get; }

        // A call-scoped side was lent one memory per key or value with bytes, revoked since, and an empty one for an
        // empty key or value.
        internal void AssertTheMemoriesItWasLent(string sides)
        {
            if (_callScoped is null)
            {
                return;
            }

            Assert.Equal(3, _callScoped.Lent.Count);
            Assert.True(_callScoped.Lent[1].IsEmpty, sides);
            Assert.False(IsNative(_callScoped.Lent[1]), sides);
            AssertRevoked(_callScoped.Lent[0]);
            AssertRevoked(_callScoped.Lent[2]);
        }
    }

    /// <summary>A deserializer that awaits, then gives a copy of the bytes, keeping the memory it was lent.</summary>
    private sealed class CopyingDeserializer : IAsyncDeserializer<ReadOnlyMemory<byte>?>
    {
        internal List<ReadOnlyMemory<byte>> Lent { get; } = new List<ReadOnlyMemory<byte>>();

        public async ValueTask<ReadOnlyMemory<byte>?> Deserialize(
            string topic, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            Lent.Add(data);
            await Task.Yield();
            return data.ToArray();
        }
    }
}
