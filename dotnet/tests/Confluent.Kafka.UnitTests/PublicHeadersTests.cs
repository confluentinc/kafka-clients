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
using System.Text;
using System.Threading;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="Header"/> and <see cref="Headers"/>: every test of Java's <c>RecordHeadersTest</c> in Java order, then
/// the null key, equality, hash and string form of Java's <c>RecordHeader</c> and <c>RecordHeaders</c>, and the
/// headers of a record to send and of a fetched record, as Python's <c>test_headers.py</c> tests them.
/// </summary>
/// <remarks>
/// A .NET enumerator has no <c>remove()</c>, so Java's <c>testHeadersIteratorRemove</c> and the two
/// <c>iterator.remove()</c> checks of <c>testReadOnly</c> do not apply. Kafka 4.4.0 has no <c>RecordHeaderTest</c>,
/// so <see cref="Header"/>'s checks follow the Java source.
/// </remarks>
public sealed class PublicHeadersTests
{
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan s_joinBound = TimeSpan.FromSeconds(30);

    private const string Topic = "headers-topic";
    private const int Partition = 0;

    [Fact]
    public void Add()
    {
        Headers headers = new Headers();
        headers.Add(new Header("key", Bytes("value")));

        using (IEnumerator<Header> iterator = headers.GetEnumerator())
        {
            Assert.True(iterator.MoveNext());
            AssertHeader("key", "value", iterator.Current);
        }

        headers.Add(new Header("key2", Bytes("value2")));

        AssertHeader("key2", "value2", headers.LastHeader("key2"));
        Assert.Equal(2, GetCount(headers));
    }

    [Fact]
    public void AddHeadersPreserveOrder()
    {
        Headers headers = new Headers();
        headers.Add(new Header("key", Bytes("value")));
        headers.Add(new Header("key2", Bytes("value2")));
        headers.Add(new Header("key3", Bytes("value3")));

        Header[] headersArr = headers.ToArray();
        AssertHeader("key", "value", headersArr[0]);
        AssertHeader("key2", "value2", headersArr[1]);
        AssertHeader("key3", "value3", headersArr[2]);

        Assert.Equal(3, GetCount(headers));
    }

    [Fact]
    public void Remove()
    {
        Headers headers = new Headers();
        headers.Add(new Header("key", Bytes("value")));

        Assert.True(HasNext(headers));

        headers.Remove("key");

        Assert.False(HasNext(headers));
    }

    [Fact]
    public void PreserveOrderAfterRemove()
    {
        Headers headers = new Headers();
        headers.Add(new Header("key", Bytes("value")));
        headers.Add(new Header("key2", Bytes("value2")));
        headers.Add(new Header("key3", Bytes("value3")));

        headers.Remove("key");
        Header[] headersArr = headers.ToArray();
        AssertHeader("key2", "value2", headersArr[0]);
        AssertHeader("key3", "value3", headersArr[1]);
        Assert.Equal(2, GetCount(headers));

        headers.Add(new Header("key4", Bytes("value4")));
        headers.Remove("key3");
        headersArr = headers.ToArray();
        AssertHeader("key2", "value2", headersArr[0]);
        AssertHeader("key4", "value4", headersArr[1]);
        Assert.Equal(2, GetCount(headers));
    }

    [Fact]
    public void AddRemoveInterleaved()
    {
        Headers headers = new Headers();
        headers.Add(new Header("key", Bytes("value")));
        headers.Add(new Header("key2", Bytes("value2")));

        Assert.True(HasNext(headers));

        headers.Remove("key");

        Assert.Equal(1, GetCount(headers));

        headers.Add(new Header("key3", Bytes("value3")));

        Assert.Null(headers.LastHeader("key"));

        AssertHeader("key2", "value2", headers.LastHeader("key2"));

        AssertHeader("key3", "value3", headers.LastHeader("key3"));

        Assert.Equal(2, GetCount(headers));

        headers.Remove("key2");

        Assert.Null(headers.LastHeader("key"));

        Assert.Null(headers.LastHeader("key2"));

        AssertHeader("key3", "value3", headers.LastHeader("key3"));

        Assert.Equal(1, GetCount(headers));

        headers.Add(new Header("key3", Bytes("value4")));

        AssertHeader("key3", "value4", headers.LastHeader("key3"));

        Assert.Equal(2, GetCount(headers));

        headers.Add(new Header("key", Bytes("valueNew")));

        Assert.Equal(3, GetCount(headers));

        AssertHeader("key", "valueNew", headers.LastHeader("key"));

        headers.Remove("key3");

        Assert.Equal(1, GetCount(headers));

        Assert.Null(headers.LastHeader("key2"));

        headers.Remove("key");

        Assert.False(HasNext(headers));
    }

    [Fact]
    public void LastHeader()
    {
        Headers headers = new Headers();
        headers.Add(new Header("key", Bytes("value")));
        headers.Add(new Header("key", Bytes("value2")));
        headers.Add(new Header("key", Bytes("value3")));

        AssertHeader("key", "value3", headers.LastHeader("key"));
        Assert.Equal(3, GetCount(headers));
    }

    [Fact]
    public void ReadOnly()
    {
        Headers headers = new Headers();
        headers.Add(new Header("key", Bytes("value")));
        using IEnumerator<Header> headerIteratorBeforeClose = headers.GetEnumerator();
        headers.SetReadOnly();

        IllegalStateException add = Assert.Throws<IllegalStateException>(
            () => headers.Add(new Header("key", Bytes("value"))));
        Assert.Equal("RecordHeaders has been closed.", add.Message);

        IllegalStateException remove = Assert.Throws<IllegalStateException>(() => headers.Remove("key"));
        Assert.Equal("RecordHeaders has been closed.", remove.Message);

        using (IEnumerator<Header> headerIterator = headers.GetEnumerator())
        {
            Assert.True(headerIterator.MoveNext());
            AssertHeader("key", "value", headerIterator.Current);
        }

        Assert.True(headerIteratorBeforeClose.MoveNext());
        AssertHeader("key", "value", headerIteratorBeforeClose.Current);
        Assert.Equal(1, GetCount(headers));
    }

    [Fact]
    public void HeadersByKey()
    {
        Headers headers = new Headers();
        headers.Add(new Header("key", Bytes("value")));
        headers.Add(new Header("key1", Bytes("key1value")));
        headers.Add(new Header("key", Bytes("value2")));
        headers.Add(new Header("key2", Bytes("key2value")));

        using (IEnumerator<Header> keyHeaders = headers.GetHeaders("key").GetEnumerator())
        {
            Assert.True(keyHeaders.MoveNext());
            AssertHeader("key", "value", keyHeaders.Current);
            Assert.True(keyHeaders.MoveNext());
            AssertHeader("key", "value2", keyHeaders.Current);
            Assert.False(keyHeaders.MoveNext());
        }

        using (IEnumerator<Header> keyHeaders = headers.GetHeaders("key1").GetEnumerator())
        {
            Assert.True(keyHeaders.MoveNext());
            AssertHeader("key1", "key1value", keyHeaders.Current);
            Assert.False(keyHeaders.MoveNext());
        }

        using (IEnumerator<Header> keyHeaders = headers.GetHeaders("key2").GetEnumerator())
        {
            Assert.True(keyHeaders.MoveNext());
            AssertHeader("key2", "key2value", keyHeaders.Current);
            Assert.False(keyHeaders.MoveNext());
        }
    }

    [Fact]
    public void New()
    {
        Headers headers = new Headers();
        headers.Add(new Header("key", Bytes("value")));
        headers.SetReadOnly();

        Headers newHeaders = new Headers(headers);
        newHeaders.Add(new Header("key", Bytes("value2")));

        AssertHeader("key", "value", headers.LastHeader("key"));
        Assert.Equal(1, GetCount(headers));

        AssertHeader("key", "value2", newHeaders.LastHeader("key"));
        Assert.Equal(2, GetCount(newHeaders));
    }

    [Fact]
    public void ShouldThrowNpeWhenAddingNullHeader()
    {
        Headers recordHeaders = new Headers();
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => recordHeaders.Add(null!));
        AssertNullArgument("header", "Header cannot be null.", exception);
    }

    [Fact]
    public void ShouldThrowNpeWhenAddingCollectionWithNullHeader()
    {
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new Headers(new Header[1]));
        AssertNullArgument("headers", "Header cannot be null.", exception);
    }

    [Fact]
    public void RecordHeaderIsReadThreadSafe()
    {
        for (int i = 0; i < 100; i++)
        {
            AssertRecordHeaderReadThreadSafe(new Header("key", Bytes("value")));
        }
    }

    [Fact]
    public void RecordHeaderWithNullValueIsReadThreadSafe()
    {
        for (int i = 0; i < 100; i++)
        {
            AssertRecordHeaderReadThreadSafe(new Header("key", null));
        }
    }

    [Fact]
    public void HeaderNullKey_IsJavasRequireNonNull()
    {
        // Python's other two checks (a positional call, a str value) are type errors the C# compiler rejects.
        ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new Header(null!, new byte[] { 1 }));
        AssertNullArgument("key", "Null header keys are not permitted", exception);
    }

    [Fact]
    public void HeaderEqualityHashAndString_AreRecordHeaders()
    {
        Header a = new Header("k", new byte[] { 0x01, 0xff });
        Header sameBytes = new Header("k", new byte[] { 0x01, 0xff });
        Assert.True(a.Equals(sameBytes));
        Assert.Equal(a.GetHashCode(), sameBytes.GetHashCode());
        Assert.False(a.Equals(new Header("k", new byte[] { 0x01 })));
        Assert.False(a.Equals(new Header("j", new byte[] { 0x01, 0xff })));
        Assert.True(new Header("n", null).Equals(new Header("n", null)));
        Assert.False(new Header("n", null).Equals(new Header("n", Array.Empty<byte>())));
        Assert.False(new Header("n", Array.Empty<byte>()).Equals(new Header("n", null)));
        Assert.Equal(new Header("n", null).GetHashCode(), new Header("n", null).GetHashCode());

        // RecordHeader.hashCode: 31 * the key's hash + Arrays.hashCode(value), which is 31 * (31 + 1) - 1 for
        // the signed bytes [1, -1], and 0 for a null value.
        int keyHash = StringComparer.Ordinal.GetHashCode("k");
        Assert.Equal(unchecked((31 * keyHash) + 991), a.GetHashCode());
        Assert.Equal(unchecked(31 * StringComparer.Ordinal.GetHashCode("n")), new Header("n", null).GetHashCode());

        Assert.False(a.Equals(null));
        Assert.False(a.Equals("k"));

        Assert.Equal("RecordHeader(key = k, value = [1, -1])", a.ToString());
        Assert.Equal("RecordHeader(key = n, value = null)", new Header("n", null).ToString());
        Assert.Equal("RecordHeader(key = e, value = [])", new Header("e", Array.Empty<byte>()).ToString());
    }

    [Fact]
    public void HeadersNullKeyChecks_AndOrder()
    {
        // Java's RecordHeaders.checkKey throws IllegalArgumentException, not a NullPointerException.
        Headers headers = new Headers();
        Assert.Equal(
            "key cannot be null.", Assert.Throws<IllegalArgumentException>(() => headers.Remove(null!)).Message);
        Assert.Equal(
            "key cannot be null.", Assert.Throws<IllegalArgumentException>(() => headers.LastHeader(null!)).Message);
        Assert.Equal(
            "key cannot be null.",
            Assert.Throws<IllegalArgumentException>(() => { _ = headers.GetHeaders(null!); }).Message);

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
    public void HeadersAddForms_AndChaining()
    {
        // Python's keyword-form errors are overload resolution in C#.
        Headers headers = new Headers();
        Assert.Same(headers, headers.Add("a", new byte[] { (byte)'1' }).Add(new Header("b", null)));
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
    public void GetHeaders_IsALiveView()
    {
        Headers headers = new Headers();
        IEnumerable<Header> byKey = headers.GetHeaders("k");
        Assert.Empty(byKey);

        headers.Add("k", new byte[] { 1 }).Add("other", null).Add("k", new byte[] { 2 });
        Assert.Collection(
            byKey,
            header => Assert.Equal(new byte[] { 1 }, header.Value()),
            header => Assert.Equal(new byte[] { 2 }, header.Value()));

        headers.Remove("k");
        Assert.Empty(byKey);
        Assert.Empty(new Headers().ToArray());
        Assert.Same(Array.Empty<Header>(), new Headers().ToArray());
    }

    [Fact]
    public void HeadersEqualityHashAndString_AreRecordHeaders()
    {
        Headers a = new Headers(new[] { new Header("k", new byte[] { 0x01, 0xff }), new Header("n", null) });
        Headers b = new Headers(new[] { new Header("k", new byte[] { 0x01, 0xff }), new Header("n", null) });

        b.SetReadOnly();
        Assert.True(a.Equals(b));
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.False(a.Equals(new Headers(new[] { new Header("n", null), new Header("k", new byte[] { 0x01, 0xff }) })));
        Assert.False(a.Equals(new Headers(new[] { new Header("k", new byte[] { 0x01, 0xff }) })));
        Assert.False(a.Equals(null));
        Assert.True(new Headers().Equals(new Headers(Array.Empty<Header>())));
        Assert.True(new Headers().Equals(new Headers().Add("k", null).Remove("k")));
        Assert.Equal(new Headers().GetHashCode(), new Headers().Add("k", null).Remove("k").GetHashCode());

        Assert.Equal(
            "RecordHeaders(headers = [RecordHeader(key = k, value = [1, -1]), RecordHeader(key = n, value = null)], "
                + "isReadOnly = false)",
            a.ToString());
        Assert.EndsWith("isReadOnly = true)", b.ToString(), StringComparison.Ordinal);
        Assert.Equal("RecordHeaders(headers = [], isReadOnly = false)", new Headers().ToString());
    }

    [Fact]
    public void RecordHeaders_NeverNull()
    {
        ProducerRecord<string, string> plain = new ProducerRecord<string, string>(Topic, value: "v");
        Assert.True(new Headers().Equals(plain.Headers()));
        Assert.True(
            new Headers().Equals(new ProducerRecord<string, string>(Topic, value: "v", key: null, partition: 0, headers: null).Headers()));
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
        Headers first = records[0].Headers();
        Headers second = records[1].Headers();
        Assert.Empty(first.ToArray());
        Assert.False(first.IsReadOnly);
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
        Assert.False(record.Headers().IsReadOnly);
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

    private static void AssertRecordHeaderReadThreadSafe(Header header)
    {
        const int threadCount = 16;
        using ManualResetEventSlim startLatch = new ManualResetEventSlim(false);
        int failures = 0;
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++)
        {
            threads[i] = new Thread(() =>
            {
                try
                {
                    startLatch.Wait();
                    _ = header.Key();
                    _ = header.Value();
                }
                catch (Exception)
                {
                    Interlocked.Increment(ref failures);
                }
            });
            threads[i].Start();
        }

        startLatch.Set();
        foreach (Thread thread in threads)
        {
            Assert.True(thread.Join(s_joinBound));
        }

        Assert.Equal(0, Volatile.Read(ref failures));
    }

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    private static int GetCount(Headers headers) => headers.ToArray().Length;

    private static bool HasNext(Headers headers)
    {
        using IEnumerator<Header> iterator = headers.GetEnumerator();
        return iterator.MoveNext();
    }

    private static void AssertHeader(string key, string? value, Header? actual)
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
