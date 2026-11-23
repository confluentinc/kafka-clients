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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Header.Internals;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;
using Confluent.Kafka.Producer;
using Confluent.Kafka.UnitTests.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The headers of a polled record over its native batch. Held alone, an <see cref="IHeader"/>, unread
/// <see cref="IHeaders"/> or a <see cref="HeaderExtensions.ValueMemory(IHeader)"/> result keeps the batch across the
/// consumer's disposal and forced collections, reads the fetched data, and lets the batch go once dropped. Every
/// member of <see cref="IHeaders"/>, as the first access of headers not read yet, behaves as on headers built from
/// the same data, as Java's <c>RecordHeaders</c> does.
/// </summary>
public sealed class PolledHeadersTests
{
    private const string Topic = "polled-headers-topic";

    private const string Rendering =
        "RecordHeaders(headers = [RecordHeader(key = a, value = [1, 2, 3]), RecordHeader(key = b, value = null), "
        + "RecordHeader(key = a, value = []), RecordHeader(key = c, value = [9])], isReadOnly = false)";

    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private static readonly TopicPartition s_tp0 = new TopicPartition(Topic, 0);

    private static readonly byte[] s_a = { 1, 2, 3 };

    private static readonly byte[] s_c = { 9 };

    [Theory]
    [InlineData(0, "a")]
    [InlineData(1, "b")]
    public void AHeaderHeldAlone_KeepsItsBatch_AfterTheConsumerIsDisposed_UntilItIsUnreachable(int index, string key)
    {
        StrongBox<IHeader?> held = new StrongBox<IHeader?>();
        WeakReference batch;
        MockConsumer<byte[], byte[]> consumer = Ready(out IDisposable blocks);
        using (blocks)
        {
            using (TestTimeout.Disposal(consumer))
            {
                batch = PollHeader(consumer, index, held);
            }

            Collect();

            Assert.True(batch.IsAlive);
            AssertHeader(held, key, index == 0 ? s_a : null);

            held.Value = null;
            Collect();

            Assert.False(batch.IsAlive);
        }
    }

    [Fact]
    public void UnreadHeadersHeldAlone_KeepTheirBatch_AfterTheConsumerIsDisposed_UntilTheyAreUnreachable()
    {
        StrongBox<IHeaders?> held = new StrongBox<IHeaders?>();
        WeakReference batch;
        MockConsumer<byte[], byte[]> consumer = Ready(out IDisposable blocks);
        using (blocks)
        {
            using (TestTimeout.Disposal(consumer))
            {
                batch = PollHeaders(consumer, held);
            }

            Collect();

            Assert.True(batch.IsAlive);
            AssertHeaders(held);

            held.Value = null;
            Collect();

            Assert.False(batch.IsAlive);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public unsafe void AValueMemoryHeldAlone_KeepsItsBatch_AfterTheConsumerIsDisposed_UntilItIsUnreachable(bool pinned)
    {
        StrongBox<ReadOnlyMemory<byte>?> held = new StrongBox<ReadOnlyMemory<byte>?>();
        StrongBox<MemoryHandle> pin = new StrongBox<MemoryHandle>();
        WeakReference batch;
        MockConsumer<byte[], byte[]> consumer = Ready(out IDisposable blocks);
        using (blocks)
        {
            using (TestTimeout.Disposal(consumer))
            {
                batch = PollValueMemory(consumer, held, pinned ? pin : null);
            }

            Collect();

            Assert.True(batch.IsAlive);
            if (pinned)
            {
                // Only the pin is held: it keeps the memory's manager, and through it the batch.
                Assert.Null(held.Value);
                Assert.Equal(s_a, new ReadOnlySpan<byte>(pin.Value.Pointer, s_a.Length).ToArray());
                pin.Value.Dispose();
                pin.Value = default;
            }
            else
            {
                AssertMemory(held);
                held.Value = null;
            }

            Collect();

            Assert.False(batch.IsAlive);
        }
    }

    [Theory]
    [InlineData("Add(Header)")]
    [InlineData("Add(string, byte[])")]
    [InlineData("Remove")]
    [InlineData("Headers(string)")]
    [InlineData("LastHeader")]
    [InlineData("Equals")]
    [InlineData("EqualsAsTheArgument")]
    [InlineData("EqualsEmpty")]
    [InlineData("GetHashCode")]
    [InlineData("ToString")]
    [InlineData("ToArray")]
    [InlineData("CopyConstructor")]
    [InlineData("ProducerRecord")]
    [InlineData("SetReadOnly")]
    public void EachMember_AsTheFirstAccessOfPolledHeaders_BehavesAsOnBuiltHeaders(string member)
    {
        MockConsumer<byte[], byte[]> consumer = Ready(out IDisposable blocks);
        using (blocks)
        using (TestTimeout.Disposal(consumer))
        {
            RecordHeaders polled = (RecordHeaders)Assert.Single(consumer.Poll(s_pollTimeout)).Headers();
            Assert.IsType<SafeConsumerRecordsHandle>(Field(polled, "_state"));
            RecordHeaders built = Built();

            switch (member)
            {
                case "Add(Header)":
                    IHeader added = new RecordHeader("d", new byte[] { 4 });
                    Assert.Same(polled, polled.Add(added));
                    built.Add(added);
                    Assert.Equal(built.ToArray(), polled.ToArray());
                    Assert.Equal(5, polled.ToArray().Length);
                    break;
                case "Add(string, byte[])":
                    Assert.Same(polled, polled.Add("d", new byte[] { 4 }));
                    built.Add("d", new byte[] { 4 });
                    Assert.Equal(built.ToArray(), polled.ToArray());
                    Assert.Equal(5, polled.ToArray().Length);
                    break;
                case "Remove":
                    Assert.Same(polled, polled.Remove("a"));
                    built.Remove("a");
                    Assert.Equal(built.ToArray(), polled.ToArray());
                    Assert.Equal(new[] { "b", "c" }, polled.Select(header => header.Key()));
                    break;
                case "Headers(string)":
                    Assert.Equal(built.Headers("a"), polled.Headers("a"));
                    Assert.Equal(
                        new[] { s_a, Array.Empty<byte>() },
                        Built().Headers("a").Select(header => header.Value()));
                    Assert.Equal(new[] { s_a, Array.Empty<byte>() }, polled.Headers("a").Select(h => h.Value()));
                    break;
                case "LastHeader":
                    Assert.Equal(built.LastHeader("a"), polled.LastHeader("a"));
                    Assert.Empty(polled.LastHeader("a")!.Value()!);
                    break;
                case "Equals":
                    Assert.True(polled.Equals(built));
                    break;
                case "EqualsAsTheArgument":
                    Assert.True(built.Equals(polled));
                    break;
                case "EqualsEmpty":
                    Assert.False(polled.Equals(new RecordHeaders()));
                    Assert.False(new RecordHeaders().Equals(polled));
                    break;
                case "GetHashCode":
                    Assert.Equal(built.GetHashCode(), polled.GetHashCode());
                    Assert.NotEqual(new RecordHeaders().GetHashCode(), polled.GetHashCode());
                    break;
                case "ToString":
                    Assert.Equal(Rendering, polled.ToString());
                    Assert.Equal(built.ToString(), polled.ToString());
                    break;
                case "ToArray":
                    Assert.Equal(built.ToArray(), polled.ToArray());
                    break;
                case "CopyConstructor":
                    RecordHeaders copy = new RecordHeaders(polled);
                    Assert.Equal(built, copy);
                    // Java's RecordHeaders(Iterable) copies the list, not its headers.
                    Assert.Equal(polled.ToArray().Length, copy.ToArray().Length);
                    for (int i = 0; i < copy.ToArray().Length; i++)
                    {
                        Assert.Same(polled.ToArray()[i], copy.ToArray()[i]);
                    }

                    copy.Add("d", null);
                    Assert.Equal(built, polled);
                    break;
                case "ProducerRecord":
                    ProducerRecord<byte[], byte[]> record =
                        new ProducerRecord<byte[], byte[]>("t", (int?)null, (byte[]?)null, (byte[]?)null, polled);
                    Assert.Equal(built, record.Headers());
                    Assert.False(polled.IsReadOnly());
                    Assert.Equal(built, polled);
                    break;
                case "SetReadOnly":
                    polled.SetReadOnly();
                    Assert.True(polled.IsReadOnly());
                    Assert.Equal(built.ToArray(), polled.ToArray());
                    IllegalStateException e = Assert.Throws<IllegalStateException>(() => polled.Add("d", null));
                    Assert.Equal("RecordHeaders has been closed.", e.Message);
                    Assert.Equal(Rendering.Replace("isReadOnly = false", "isReadOnly = true"), polled.ToString());
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(member), member, null);
            }
        }
    }

    // The reads after the collection are not inlined either, so no temporary of theirs (a header array, a memory)
    // stays reported live in the test's frame and roots the batch after the held object is dropped.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void AssertHeader(StrongBox<IHeader?> held, string key, byte[]? value)
    {
        IHeader header = held.Value!;
        Assert.Equal(key, header.Key());
        Assert.Equal(value, header.ValueMemory()?.ToArray());
        Assert.Equal(value, header.Value());
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void AssertHeaders(StrongBox<IHeaders?> held)
    {
        IHeaders headers = held.Value!;
        Assert.IsType<SafeConsumerRecordsHandle>(Field(headers, "_state"));
        Assert.Equal(Built().ToArray(), headers.ToArray());
        Assert.Equal(Rendering, headers.ToString());
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void AssertMemory(StrongBox<ReadOnlyMemory<byte>?> held) =>
        Assert.Equal(s_a, held.Value!.Value.ToArray());

    // Not inlined, so the records, the record and its headers are unreachable once it returns: only the held header
    // roots the batch.
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference PollHeader(MockConsumer<byte[], byte[]> consumer, int index, StrongBox<IHeader?> held)
    {
        held.Value = Assert.Single(consumer.Poll(s_pollTimeout)).Headers().ToArray()[index];
        Assert.Null(Field(held.Value, "_key"));
        return Weak(Field(held.Value, "_batch"));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference PollHeaders(MockConsumer<byte[], byte[]> consumer, StrongBox<IHeaders?> held)
    {
        held.Value = Assert.Single(consumer.Poll(s_pollTimeout)).Headers();
        return Weak(Field(held.Value, "_state"));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference PollValueMemory(
        MockConsumer<byte[], byte[]> consumer, StrongBox<ReadOnlyMemory<byte>?> held, StrongBox<MemoryHandle>? pin)
    {
        ReadOnlyMemory<byte> memory =
            Assert.Single(consumer.Poll(s_pollTimeout)).Headers().ToArray()[0].ValueMemory()!.Value;
        Assert.True(MemoryMarshal.TryGetMemoryManager(memory, out BytesMemoryManager? manager));
        if (pin is null)
        {
            held.Value = memory;
        }
        else
        {
            pin.Value = memory.Pin();
        }

        return Weak(manager!.Batch);
    }

    // The reference tracks resurrection, so it dies only after the finalizer that releases the batch.
    private static WeakReference Weak(object? batch) =>
        new WeakReference(Assert.IsType<SafeConsumerRecordsHandle>(batch), trackResurrection: true);

    // A consumer with one record at offset 0 whose native headers are a = 01 02 03, b = null, a = empty and c = 09;
    // `blocks` is disposed after the consumer.
    private static MockConsumer<byte[], byte[]> Ready(out IDisposable blocks)
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(
            Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        consumer.Assign(new[] { s_tp0 });
        consumer.Seek(s_tp0, 0);
        NativeConsumer native = Assert.IsType<NativeConsumer>(Field(consumer, "_native"));
        blocks = ConsumerPollHeadersTests.AddRecordWithHeaders(
            native, Topic, 0, offset: 0, key: null, value: null,
            ("a", s_a), ("b", null), ("a", Array.Empty<byte>()), ("c", s_c));
        return consumer;
    }

    private static RecordHeaders Built() =>
        (RecordHeaders)new RecordHeaders().Add("a", new byte[] { 1, 2, 3 }).Add("b", null).Add("a", Array.Empty<byte>()).Add("c", new byte[] { 9 });

    private static object? Field(object target, string name) =>
        target.GetType().GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(target);

    private static void Collect()
    {
        for (int i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }
    }
}
