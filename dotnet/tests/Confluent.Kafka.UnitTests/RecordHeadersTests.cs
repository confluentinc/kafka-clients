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
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Header.Internals;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="RecordHeader"/> and <see cref="RecordHeaders"/>: every test of Java's <c>RecordHeadersTest</c> in Java
/// order, under Java's names, then the null key, equality, hash and string form of both classes, the add forms, the
/// live view of <see cref="RecordHeaders.Headers(string)"/>, and the headers of a record to send and of a fetched
/// record, as Python's <c>test_headers.py</c> tests them.
/// </summary>
/// <remarks>
/// A .NET enumerator has no <c>remove()</c>, so Java's <c>testHeadersIteratorRemove</c> and the two
/// <c>iterator.remove()</c> checks of <c>testReadOnly</c> do not apply. The two thread-safety tests build their header
/// over buffers, as Java's do. Kafka 4.4.0 has no <c>RecordHeaderTest</c>, so <see cref="RecordHeader"/>'s checks follow
/// the Java source.
/// </remarks>
public sealed class RecordHeadersTests
{
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan s_joinBound = TimeSpan.FromSeconds(30);

    private const string Topic = "headers-topic";
    private const int Partition = 0;

    [Fact]
    public void TestAdd()
    {
        IHeaders headers = new RecordHeaders();
        headers.Add(new RecordHeader("key", Bytes("value")));

        using (IEnumerator<IHeader> iterator = headers.GetEnumerator())
        {
            Assert.True(iterator.MoveNext());
            IHeader header = iterator.Current;
            AssertHeader("key", "value", header);
        }

        headers.Add(new RecordHeader("key2", Bytes("value2")));

        AssertHeader("key2", "value2", headers.LastHeader("key2"));
        Assert.Equal(2, GetCount(headers));
    }

    [Fact]
    public void TestAddHeadersPreserveOrder()
    {
        IHeaders headers = new RecordHeaders();
        headers.Add(new RecordHeader("key", Bytes("value")));
        headers.Add(new RecordHeader("key2", Bytes("value2")));
        headers.Add(new RecordHeader("key3", Bytes("value3")));

        IHeader[] headersArr = headers.ToArray();
        AssertHeader("key", "value", headersArr[0]);
        AssertHeader("key2", "value2", headersArr[1]);
        AssertHeader("key3", "value3", headersArr[2]);

        Assert.Equal(3, GetCount(headers));
    }

    [Fact]
    public void TestRemove()
    {
        IHeaders headers = new RecordHeaders();
        headers.Add(new RecordHeader("key", Bytes("value")));

        Assert.True(HasNext(headers));

        headers.Remove("key");

        Assert.False(HasNext(headers));
    }

    [Fact]
    public void TestPreserveOrderAfterRemove()
    {
        IHeaders headers = new RecordHeaders();
        headers.Add(new RecordHeader("key", Bytes("value")));
        headers.Add(new RecordHeader("key2", Bytes("value2")));
        headers.Add(new RecordHeader("key3", Bytes("value3")));

        headers.Remove("key");
        IHeader[] headersArr = headers.ToArray();
        AssertHeader("key2", "value2", headersArr[0]);
        AssertHeader("key3", "value3", headersArr[1]);
        Assert.Equal(2, GetCount(headers));

        headers.Add(new RecordHeader("key4", Bytes("value4")));
        headers.Remove("key3");
        headersArr = headers.ToArray();
        AssertHeader("key2", "value2", headersArr[0]);
        AssertHeader("key4", "value4", headersArr[1]);
        Assert.Equal(2, GetCount(headers));
    }

    [Fact]
    public void TestAddRemoveInterleaved()
    {
        IHeaders headers = new RecordHeaders();
        headers.Add(new RecordHeader("key", Bytes("value")));
        headers.Add(new RecordHeader("key2", Bytes("value2")));

        Assert.True(HasNext(headers));

        headers.Remove("key");

        Assert.Equal(1, GetCount(headers));

        headers.Add(new RecordHeader("key3", Bytes("value3")));

        Assert.Null(headers.LastHeader("key"));

        AssertHeader("key2", "value2", headers.LastHeader("key2"));

        AssertHeader("key3", "value3", headers.LastHeader("key3"));

        Assert.Equal(2, GetCount(headers));

        headers.Remove("key2");

        Assert.Null(headers.LastHeader("key"));

        Assert.Null(headers.LastHeader("key2"));

        AssertHeader("key3", "value3", headers.LastHeader("key3"));

        Assert.Equal(1, GetCount(headers));

        headers.Add(new RecordHeader("key3", Bytes("value4")));

        AssertHeader("key3", "value4", headers.LastHeader("key3"));

        Assert.Equal(2, GetCount(headers));

        headers.Add(new RecordHeader("key", Bytes("valueNew")));

        Assert.Equal(3, GetCount(headers));

        AssertHeader("key", "valueNew", headers.LastHeader("key"));

        headers.Remove("key3");

        Assert.Equal(1, GetCount(headers));

        Assert.Null(headers.LastHeader("key2"));

        headers.Remove("key");

        Assert.False(HasNext(headers));
    }

    [Fact]
    public void TestLastHeader()
    {
        IHeaders headers = new RecordHeaders();
        headers.Add(new RecordHeader("key", Bytes("value")));
        headers.Add(new RecordHeader("key", Bytes("value2")));
        headers.Add(new RecordHeader("key", Bytes("value3")));

        AssertHeader("key", "value3", headers.LastHeader("key"));
        Assert.Equal(3, GetCount(headers));
    }

    [Fact]
    public void TestReadOnly()
    {
        RecordHeaders headers = new RecordHeaders();
        headers.Add(new RecordHeader("key", Bytes("value")));
        using IEnumerator<IHeader> headerIteratorBeforeClose = headers.GetEnumerator();
        headers.SetReadOnly();

        IllegalStateException add = Assert.Throws<IllegalStateException>(
            () => headers.Add(new RecordHeader("key", Bytes("value"))));
        Assert.Equal("RecordHeaders has been closed.", add.Message);

        IllegalStateException remove = Assert.Throws<IllegalStateException>(() => headers.Remove("key"));
        Assert.Equal("RecordHeaders has been closed.", remove.Message);

        using (IEnumerator<IHeader> headerIterator = headers.GetEnumerator())
        {
            Assert.True(headerIterator.MoveNext());
            AssertHeader("key", "value", headerIterator.Current);
        }

        Assert.True(headerIteratorBeforeClose.MoveNext());
        AssertHeader("key", "value", headerIteratorBeforeClose.Current);
        Assert.Equal(1, GetCount(headers));
    }

    [Fact]
    public void TestHeaders()
    {
        RecordHeaders headers = new RecordHeaders();
        headers.Add(new RecordHeader("key", Bytes("value")));
        headers.Add(new RecordHeader("key1", Bytes("key1value")));
        headers.Add(new RecordHeader("key", Bytes("value2")));
        headers.Add(new RecordHeader("key2", Bytes("key2value")));

        using (IEnumerator<IHeader> keyHeaders = headers.Headers("key").GetEnumerator())
        {
            Assert.True(keyHeaders.MoveNext());
            AssertHeader("key", "value", keyHeaders.Current);
            Assert.True(keyHeaders.MoveNext());
            AssertHeader("key", "value2", keyHeaders.Current);
            Assert.False(keyHeaders.MoveNext());
        }

        using (IEnumerator<IHeader> keyHeaders = headers.Headers("key1").GetEnumerator())
        {
            Assert.True(keyHeaders.MoveNext());
            AssertHeader("key1", "key1value", keyHeaders.Current);
            Assert.False(keyHeaders.MoveNext());
        }

        using (IEnumerator<IHeader> keyHeaders = headers.Headers("key2").GetEnumerator())
        {
            Assert.True(keyHeaders.MoveNext());
            AssertHeader("key2", "key2value", keyHeaders.Current);
            Assert.False(keyHeaders.MoveNext());
        }
    }

    [Fact]
    public void TestNew()
    {
        RecordHeaders headers = new RecordHeaders();
        headers.Add(new RecordHeader("key", Bytes("value")));
        headers.SetReadOnly();

        RecordHeaders newHeaders = new RecordHeaders(headers);
        newHeaders.Add(new RecordHeader("key", Bytes("value2")));

        AssertHeader("key", "value", headers.LastHeader("key"));
        Assert.Equal(1, GetCount(headers));

        AssertHeader("key", "value2", newHeaders.LastHeader("key"));
        Assert.Equal(2, GetCount(newHeaders));
    }

    [Fact]
    public void ShouldThrowNpeWhenAddingNullHeader()
    {
        RecordHeaders recordHeaders = new RecordHeaders();
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => recordHeaders.Add(null!));
        AssertNullArgument("header", "Header cannot be null.", exception);
    }

    [Fact]
    public void ShouldThrowNpeWhenAddingCollectionWithNullHeader()
    {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new RecordHeaders(new IHeader[1]));
        AssertNullArgument("headers", "Header cannot be null.", exception);
    }

    [Fact]
    public async Task TestRecordHeaderIsReadThreadSafe()
    {
        for (int i = 0; i < 100; i++)
        {
            RecordHeader header = new RecordHeader(
                new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes("key")),
                new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes("value")));
            await AssertRecordHeaderReadThreadSafe(header);
        }
    }

    [Fact]
    public async Task TestRecordHeaderWithNullValueIsReadThreadSafe()
    {
        for (int i = 0; i < 100; i++)
        {
            RecordHeader header = new RecordHeader(new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes("key")), null);
            await AssertRecordHeaderReadThreadSafe(header);
        }
    }

    [Fact]
    public void RecordHeaderNullKey_IsJavasRequireNonNull()
    {
        // Python's other two checks (a positional call, a str value) are type errors the C# compiler rejects.
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new RecordHeader(null!, new byte[] { 1 }));
        AssertNullArgument("key", "Null header keys are not permitted", exception);
    }

    [Fact]
    public void RecordHeaderEqualityHashAndString_AreJavas()
    {
        RecordHeader a = new RecordHeader("k", new byte[] { 0x01, 0xff });
        RecordHeader sameBytes = new RecordHeader("k", new byte[] { 0x01, 0xff });
        Assert.True(a.Equals(sameBytes));
        Assert.Equal(a.GetHashCode(), sameBytes.GetHashCode());
        Assert.False(a.Equals(new RecordHeader("k", new byte[] { 0x01 })));
        Assert.False(a.Equals(new RecordHeader("j", new byte[] { 0x01, 0xff })));
        Assert.True(new RecordHeader("n", null).Equals(new RecordHeader("n", null)));
        Assert.False(new RecordHeader("n", null).Equals(new RecordHeader("n", Array.Empty<byte>())));
        Assert.False(new RecordHeader("n", Array.Empty<byte>()).Equals(new RecordHeader("n", null)));
        Assert.Equal(new RecordHeader("n", null).GetHashCode(), new RecordHeader("n", null).GetHashCode());

        // RecordHeader.hashCode: 31 * the key's hash + Arrays.hashCode(value), which is 31 * (31 + 1) - 1 for
        // the signed bytes [1, -1], and 0 for a null value.
        int keyHash = StringComparer.Ordinal.GetHashCode("k");
        Assert.Equal(unchecked((31 * keyHash) + 991), a.GetHashCode());
        Assert.Equal(unchecked(31 * StringComparer.Ordinal.GetHashCode("n")), new RecordHeader("n", null).GetHashCode());

        Assert.False(a.Equals(null));
        Assert.False(a.Equals("k"));

        Assert.Equal("RecordHeader(key = k, value = [1, -1])", a.ToString());
        Assert.Equal("RecordHeader(key = n, value = null)", new RecordHeader("n", null).ToString());
        Assert.Equal("RecordHeader(key = e, value = [])", new RecordHeader("e", Array.Empty<byte>()).ToString());
    }

    [Fact]
    public void RecordHeadersNullKeyChecks_AndOrder()
    {
        // Java's RecordHeaders.checkKey throws IllegalArgumentException, not a NullPointerException.
        RecordHeaders headers = new RecordHeaders();
        Assert.Equal(
            "key cannot be null.", Assert.Throws<IllegalArgumentException>(() => headers.Remove(null!)).Message);
        Assert.Equal(
            "key cannot be null.", Assert.Throws<IllegalArgumentException>(() => headers.LastHeader(null!)).Message);
        Assert.Equal(
            "key cannot be null.",
            Assert.Throws<IllegalArgumentException>(() => { _ = headers.Headers(null!); }).Message);

        headers.SetReadOnly();
        Assert.Throws<IllegalStateException>(() => headers.Remove(null!));

        AssertNullArgument(
            "key",
            "Null header keys are not permitted",
            Assert.Throws<ArgumentNullException>(() => headers.Add(null!, new byte[] { (byte)'v' })));

        AssertNullArgument(
            "header", "Header cannot be null.", Assert.Throws<ArgumentNullException>(() => headers.Add(null!)));
    }

    [Fact]
    public void RecordHeadersAddForms_AndChaining()
    {
        // Python's keyword-form errors are overload resolution in C#.
        RecordHeaders headers = new RecordHeaders();
        Assert.Same(headers, headers.Add("a", new byte[] { (byte)'1' }).Add(new RecordHeader("b", null)));
        Assert.Collection(
            headers,
            header =>
            {
                Assert.Equal("a", header.Key());
                Assert.Equal(new byte[] { (byte)'1' }, header.Value());
            },
            header =>
            {
                Assert.Equal("b", header.Key());
                Assert.Null(header.Value());
            });
        Assert.Same(headers, headers.Remove("a"));
        Assert.Same(headers, headers.Remove("absent"));
        AssertHeader("b", null, Assert.Single(headers));
    }

    [Fact]
    public void Headers_IsALiveView()
    {
        RecordHeaders headers = new RecordHeaders();
        IEnumerable<IHeader> byKey = headers.Headers("k");
        Assert.Empty(byKey);

        headers.Add("k", new byte[] { 1 }).Add("other", null).Add("k", new byte[] { 2 });
        Assert.Collection(
            byKey,
            header => Assert.Equal(new byte[] { 1 }, header.Value()),
            header => Assert.Equal(new byte[] { 2 }, header.Value()));

        headers.Remove("k");
        Assert.Empty(byKey);
        Assert.Empty(new RecordHeaders().ToArray());
        Assert.Same(Array.Empty<IHeader>(), new RecordHeaders().ToArray());
    }

    [Fact]
    public void RecordHeadersEqualityHashAndString_AreJavas()
    {
        RecordHeaders a = new RecordHeaders(new[] { new RecordHeader("k", new byte[] { 0x01, 0xff }), new RecordHeader("n", null) });
        RecordHeaders b = new RecordHeaders(new[] { new RecordHeader("k", new byte[] { 0x01, 0xff }), new RecordHeader("n", null) });

        b.SetReadOnly();
        Assert.True(a.Equals(b));
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.False(a.Equals(new RecordHeaders(new[] { new RecordHeader("n", null), new RecordHeader("k", new byte[] { 0x01, 0xff }) })));
        Assert.False(a.Equals(new RecordHeaders(new[] { new RecordHeader("k", new byte[] { 0x01, 0xff }) })));
        Assert.False(a.Equals(null));
        Assert.True(new RecordHeaders().Equals(new RecordHeaders(Array.Empty<RecordHeader>())));
        Assert.True(new RecordHeaders().Equals(new RecordHeaders().Add("k", null).Remove("k")));
        Assert.Equal(new RecordHeaders().GetHashCode(), new RecordHeaders().Add("k", null).Remove("k").GetHashCode());

        Assert.Equal(
            "RecordHeaders(headers = [RecordHeader(key = k, value = [1, -1]), RecordHeader(key = n, value = null)], "
                + "isReadOnly = false)",
            a.ToString());
        Assert.EndsWith("isReadOnly = true)", b.ToString(), StringComparison.Ordinal);
        Assert.Equal("RecordHeaders(headers = [], isReadOnly = false)", new RecordHeaders().ToString());
    }

    [Fact]
    public void RecordHeaders_NeverNull()
    {
        ProducerRecord<string, string> plain = new ProducerRecord<string, string>(Topic, value: "v");
        Assert.True(new RecordHeaders().Equals(plain.Headers()));
        Assert.True(
            new RecordHeaders().Equals(new ProducerRecord<string, string>(Topic, value: "v", key: null, partition: 0, headers: null).Headers()));
        Assert.NotSame(plain.Headers(), new ProducerRecord<string, string>(Topic, value: "v").Headers());

        IReadOnlyList<ConsumerRecord<byte[], byte[]>> records = PollMockRecords(2);
        Assert.NotNull(records[0].Headers());
        Assert.Empty(records[0].Headers().ToArray());
        Assert.NotSame(records[0].Headers(), records[1].Headers());
    }

    [Fact]
    public void Poll_BuildsAWritableHeadersPerRecord()
    {
        IReadOnlyList<ConsumerRecord<byte[], byte[]>> records = PollMockRecords(2);
        IHeaders first = records[0].Headers();
        IHeaders second = records[1].Headers();
        Assert.Empty(first.ToArray());
        Assert.False(((RecordHeaders)first).IsReadOnly());
        Assert.NotSame(first, second);

        first.Add("k", new byte[] { (byte)'v' });
        Assert.Empty(second.ToArray());
    }

    [Fact]
    public void Poll_HandsOutWritableHeadersAfterTheDeserializers()
    {
        // Java's CompletedFetch leaves a fetched record's headers writable: only KafkaProducer.doSend seals them.
        // The deserializer half does not apply: IDeserializer has no headers form.
        ConsumerRecord<byte[], byte[]> record = Assert.Single(PollMockRecords(1));
        Assert.False(((RecordHeaders)record.Headers()).IsReadOnly());
        record.Headers().Add("added", Bytes("after-poll")).Add("k", null);
        AssertHeader("added", "after-poll", record.Headers().ToArray()[0]);
        Assert.NotNull(record.Headers().LastHeader("k"));
    }

    private static IReadOnlyList<ConsumerRecord<byte[], byte[]>> PollMockRecords(int count)
    {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<byte[], byte[]>(Serdes.ByteArray().Deserializer(), Serdes.ByteArray().Deserializer());
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        consumer.Assign(new[] { new TopicPartition(Topic, Partition) });
        consumer.Seek(new TopicPartition(Topic, Partition), offset: 0);
        for (int i = 0; i < count; i++)
        {
            consumer.AddRecord(Topic, Partition, offset: i, Bytes("k"), Bytes("v"));
        }

        List<ConsumerRecord<byte[], byte[]>> records = new List<ConsumerRecord<byte[], byte[]>>(consumer.Poll(s_pollTimeout));
        Assert.Equal(count, records.Count);
        return records;
    }

    private static async Task AssertRecordHeaderReadThreadSafe(RecordHeader header)
    {
        const int threadCount = 16;
        using CountdownEvent startLatch = new CountdownEvent(1);

        List<Task> futures = Enumerable.Range(0, threadCount)
            .Select(i => Task.Run(() =>
            {
                startLatch.Wait();
                _ = header.Key();
                _ = header.Value();
            }))
            .ToList();

        startLatch.Signal();
        await TestTimeout.Run(() => Task.WhenAll(futures), s_joinBound);
    }

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    private static int GetCount(IHeaders headers) => headers.ToArray().Length;

    private static bool HasNext(IHeaders headers)
    {
        using IEnumerator<IHeader> iterator = headers.GetEnumerator();
        return iterator.MoveNext();
    }

    private static void AssertHeader(string key, string? value, IHeader? actual)
    {
        Assert.NotNull(actual);
        Assert.Equal(key, actual.Key());
        Assert.Equal(value is null ? null : Bytes(value), actual.Value());
    }

    private static void AssertNullArgument(string paramName, string message, ArgumentNullException exception)
    {
        Assert.Equal(paramName, exception.ParamName);
        Assert.Equal(new ArgumentNullException(paramName, message).Message, exception.Message);
    }
}
