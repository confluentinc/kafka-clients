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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Header.Internals;
using Confluent.Kafka.Common.Record;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;
using Confluent.Kafka.Producer;
using Confluent.Kafka.UnitTests.Interop;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Header classes of the user's own, an <see cref="IHeader"/> and an <see cref="IHeaders"/>, as Java lets a user
/// implement <c>Header</c> and <c>Headers</c>: a user header given to a record, added to its headers or added by a
/// serializer is sent, its value read once per send; one whose key is <see langword="null"/>, at its first read or at a
/// later one, makes the send throw Java's <see cref="IllegalArgumentException"/> on all four producers, from
/// <c>Send</c> itself or, on an asynchronous producer that awaits a serializer, from its first stage, releasing the
/// async send's pins; the record keeps its own copy of the headers it is given; the consumer's record and
/// <see cref="RecordDeserializationException"/> keep a user's headers as given; a <see cref="RecordHeader"/> equals no user
/// header; and the headers print <c>null</c> for a user header whose string form is <see langword="null"/>.
/// </summary>
public sealed class PublicUserHeaderTests
{
    private const string Topic = "user-headers";

    private const string NullKeyMessage = "Invalid null header key found in headers";

    private const string Kafka = nameof(KafkaProducer<string, string>);

    private const string AsyncKafka = nameof(AsyncKafkaProducer<string, string>);

    private const string Mock = nameof(MockProducer<string, string>);

    private const string AsyncMock = nameof(AsyncMockProducer<string, string>);

    private const int TimeoutCode = 7;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    public static IEnumerable<object[]> Producers() =>
        new[] { Kafka, AsyncKafka, Mock, AsyncMock }.Select(producer => new object[] { producer });

    [Theory]
    [InlineData(Mock)]
    [InlineData(AsyncMock)]
    public async Task AMockProducer_SendsTheUserHeadersGivenToTheRecordOrAdded_ReadingEachValueOnce(string producer)
    {
        UserHeader given = new UserHeader("given", Bytes("1"));
        UserHeader added = new UserHeader("added", null);
        ProducerRecord<string, string> record = Record(given);
        Assert.Same(record.Headers(), record.Headers().Add(added));
        await using Subject subject = await Create(producer, Serdes.String().Serializer(), Serdes.String().Serializer());

        RecordMetadata? metadata = await subject.Deliver(record);

        Assert.Equal(0L, metadata!.Offset());
        Assert.Collection(
            Assert.Single(HistoryHeaders(subject.Producer)),
            header => AssertHeader("given", Bytes("1"), header),
            header => AssertHeader("added", null, header));
        Assert.Equal(1, given.ValueReads);
        Assert.Equal(1, added.ValueReads);
    }

    [Theory]
    [InlineData(Kafka)]
    [InlineData(AsyncKafka)]
    public async Task AKafkaProducer_SendsTheUserHeadersOfTheRecordAndOfItsSerializer_ReadingEachValueOnce(string producer)
    {
        UserHeader given = new UserHeader("given", Bytes("1"));
        UserHeader schema = new UserHeader("schema-id", Bytes("7"));
        await using Subject subject =
            await Create(producer, Serdes.String().Serializer(), new UserHeaderAddingSerializer(schema));
        ProducerRecord<string, string> record = Record(given);

        Assert.Null(await subject.Deliver(record));

        Assert.True(((RecordHeaders)record.Headers()).IsReadOnly());
        Assert.Equal(new IHeader[] { given, schema }, record.Headers().ToArray());
        Assert.Equal(1, given.ValueReads);
        Assert.Equal(1, schema.ValueReads);
    }

    [Fact]
    public void TheKafkaProducersRecord_CarriesTheUserHeadersOfTheRecordAndOfItsSerializer()
    {
        UserHeader given = new UserHeader("given", Bytes("1"));
        UserHeader added = new UserHeader("added", Array.Empty<byte>());
        UserHeader schema = new UserHeader("schema-id", Bytes("7"));
        ProducerRecord<string, string> record = Record(given);
        record.Headers().Add(added);
        UserHeaderAddingSerializer serializer = new UserHeaderAddingSerializer(schema);

        SerializedProducerRecord serialized = SerializedProducerRecord.Serialize(
            record, Serdes.String().Serializer(), null, serializer, serializer, mock: false);
        IntPtr native = ProducerRecordBuildTests.Build(in serialized, IntPtr.Zero, IntPtr.Zero);
        try
        {
            Assert.Collection(
                ProducerRecordBuildTests.HeadersOf(native),
                header => AssertHeader("given", Bytes("1"), header),
                header => AssertHeader("added", Array.Empty<byte>(), header),
                header => AssertHeader("schema-id", Bytes("7"), header));
        }
        finally
        {
            ProducerRecordBuildTests.Destroy(native);
        }
    }

    [Theory]
    [MemberData(nameof(Producers))]
    public async Task Send_AUserHeaderWithANullKey_ThrowsJavasIllegalArgumentException(string producer)
    {
        UserHeader first = new UserHeader("first", Bytes("1"));
        UserHeader nullKey = new UserHeader((string?)null, Bytes("2"));
        await AssertRejectedThenSends(producer, Record(first, nullKey), first);
    }

    [Theory]
    [MemberData(nameof(Producers))]
    public async Task Send_AUserHeaderWhoseKeyIsNullOnlyAtALaterRead_ThrowsJavasIllegalArgumentException(string producer)
    {
        UserHeader laterNull = new UserHeader(new[] { "k", null }, Bytes("1"));
        await AssertRejectedThenSends(producer, Record(laterNull), header: null);
        Assert.True(laterNull.KeyReads >= 2);
    }

    [Theory]
    [InlineData(AsyncKafka)]
    [InlineData(AsyncMock)]
    public async Task AsyncSend_WithTwoBuiltInSerializers_ThrowsTheNullKeyRejectionItself(string producer)
    {
        IAsyncProducer<string, string> subject = producer == AsyncKafka
            ? await AsyncKafkaProducer<string, string>.Create(
                Unreachable(), new AsyncStringSerializer(), new AsyncStringSerializer())
            : new AsyncMockProducer<string, string>(new AsyncStringSerializer(), new AsyncStringSerializer());
        CountingCallback callback = new CountingCallback();
        UserHeader nullKey = new UserHeader((string?)null, Bytes("1"));
        ProducerRecord<string, string> record = Record(nullKey);
        try
        {
            IllegalArgumentException rejected =
                Assert.Throws<IllegalArgumentException>(() => { _ = subject.Send(record, callback); });

            Assert.Equal(NullKeyMessage, rejected.Message);
            Assert.Null(rejected.ParamName);
            Assert.Equal(producer == AsyncKafka, ((RecordHeaders)record.Headers()).IsReadOnly());
            Assert.Equal(0, nullKey.ValueReads);
        }
        finally
        {
            await TestTimeout.AsyncDisposal(subject).DisposeAsync();
        }

        Assert.Equal(0, callback.Count);
    }

    [Fact]
    public void AsyncMockSend_AUserHeaderWhoseKeyIsNullOnlyAtALaterRead_UnpinsTheKeyAndValueOnce()
    {
        // The async send pins the key and value after the first key read and before the record is built, so the
        // rejection at the later read must release each pin it took, once.
        using NativeMemoryManager key = new NativeMemoryManager(8);
        using NativeMemoryManager value = new NativeMemoryManager(32);
        UserHeader laterNull = new UserHeader(new[] { "k", null }, Bytes("1"));
        ProducerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> record =
            new ProducerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(
                Topic,
                value: (ReadOnlyMemory<byte>)value.Memory,
                headers: new IHeader[] { laterNull },
                key: (ReadOnlyMemory<byte>)key.Memory,
                partition: null);
        AsyncMockProducer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> producer =
            new AsyncMockProducer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(
                new AsyncReadOnlyMemorySerializer(), new AsyncReadOnlyMemorySerializer());
        using (TestTimeout.Disposal(producer))
        {
            IllegalArgumentException rejected =
                Assert.Throws<IllegalArgumentException>(() => { _ = producer.Send(record); });

            Assert.Equal(NullKeyMessage, rejected.Message);
            Assert.Equal(1, key.PinCount);
            Assert.Equal(1, key.UnpinCount);
            Assert.Equal(1, value.PinCount);
            Assert.Equal(1, value.UnpinCount);
            Assert.Equal(0, laterNull.ValueReads);
            Assert.Equal(0, producer.HistoryCount());
        }
    }

    [Fact]
    public void ProducerRecord_CopiesTheUserHeadersItIsGiven()
    {
        UserHeader given = new UserHeader("given", null);
        UserHeaders input = new UserHeaders();
        input.Add(given);

        ProducerRecord<string, string> record =
            new ProducerRecord<string, string>(Topic, value: "v", headers: input, key: "k", partition: null);
        input.Add(new UserHeader("later", null));

        Assert.NotSame(input, record.Headers());
        Assert.IsType<RecordHeaders>(record.Headers());
        Assert.Same(given, Assert.Single(record.Headers()));
        record.Headers().Add("own", null);
        Assert.Equal(2, input.ToArray().Length);
    }

    [Fact]
    public void Headers_CopiesAUserHeaders()
    {
        UserHeader first = new UserHeader("a", Bytes("1"));
        UserHeader second = new UserHeader("b", null);
        UserHeaders input = new UserHeaders();
        input.Add(first).Add(second);

        RecordHeaders copy = new RecordHeaders(input);
        input.Remove("a");

        Assert.Equal(new IHeader[] { first, second }, copy.ToArray());
        Assert.False(copy.IsReadOnly());
        Assert.Same(copy, copy.Add("c", null));
        Assert.Single(input.ToArray());
    }

    [Fact]
    public void ConsumerRecord_KeepsTheUserHeadersItIsGiven()
    {
        UserHeaders headers = new UserHeaders();

        ConsumerRecord<string, string> eleven = new ConsumerRecord<string, string>(
            Topic, 0, 1L, 2L, TimestampType.CREATE_TIME, 1, 1, "k", "v", headers, null);
        ConsumerRecord<string, string> twelve = new ConsumerRecord<string, string>(
            Topic, 0, 1L, 2L, TimestampType.CREATE_TIME, 1, 1, "k", "v", headers, null, null);

        Assert.Same(headers, eleven.Headers());
        Assert.Same(headers, twelve.Headers());
    }

    [Fact]
    public void RecordDeserializationException_KeepsTheUserHeadersItIsGiven()
    {
        UserHeaders headers = new UserHeaders();

        RecordDeserializationException e = new RecordDeserializationException(
            RecordDeserializationException.DeserializationExceptionOrigin.VALUE,
            new TopicPartition(Topic, 0),
            1L,
            2L,
            TimestampType.CREATE_TIME,
            null,
            null,
            headers,
            "m",
            null);

        Assert.Same(headers, e.Headers());
    }

    [Fact]
    public void AHeader_NeverEqualsAUserHeader_AndHeadersCompareTheirElementsByTheirOwnEquality()
    {
        byte[] value = Bytes("v");
        RecordHeader header = new RecordHeader("k", value);
        EqualityHeader user = new EqualityHeader("k", value);

        Assert.False(header.Equals(user));
        Assert.False(new RecordHeaders(new IHeader[] { header }).Equals(new RecordHeaders(new IHeader[] { user })));

        RecordHeaders ofUser = new RecordHeaders(new IHeader[] { user });
        RecordHeaders ofEqualUser = new RecordHeaders(new IHeader[] { new EqualityHeader("k", Bytes("v")) });
        Assert.True(ofUser.Equals(ofEqualUser));
        Assert.Equal(ofUser.GetHashCode(), ofEqualUser.GetHashCode());
        Assert.Equal(unchecked(31 + user.GetHashCode()), ofUser.GetHashCode());
        Assert.Equal("RecordHeaders(headers = [EqualityHeader(k)], isReadOnly = false)", ofUser.ToString());
    }

    [Fact]
    public void HeadersToString_PrintsNullForAUserHeaderWhoseToStringIsNull()
    {
        RecordHeaders headers = new RecordHeaders(new IHeader[] { new NullTextHeader(), new EqualityHeader("k", Bytes("v")) });

        Assert.Equal("RecordHeaders(headers = [null, EqualityHeader(k)], isReadOnly = false)", headers.ToString());
    }

    private static async Task AssertRejectedThenSends(
        string producer, ProducerRecord<string, string> record, UserHeader? header)
    {
        CountingSerializer serializer = new CountingSerializer();
        CountingCallback callback = new CountingCallback();
        Subject subject = await Create(producer, serializer, serializer);
        try
        {
            IllegalArgumentException rejected = await subject.Reject(record, callback);

            Assert.Equal(NullKeyMessage, rejected.Message);
            Assert.Null(rejected.ParamName);
            Assert.Equal(2, serializer.Calls);
            Assert.Equal(!subject.IsMock, ((RecordHeaders)record.Headers()).IsReadOnly());
            if (header is not null)
            {
                Assert.Equal(0, header.ValueReads);
            }

            if (subject.IsMock)
            {
                Assert.Equal(0, subject.HistoryCount());
                Assert.Equal(0L, (await subject.Deliver(Record(new UserHeader("ok", null))))!.Offset());
                Assert.Equal(1, subject.HistoryCount());
            }
            else
            {
                Assert.Null(await subject.Deliver(Record(new UserHeader("ok", null))));
            }
        }
        finally
        {
            await subject.DisposeAsync();
        }

        // The producer's destroy runs every pending callback, so a record that had reached the core would have fired
        // its callback by now.
        Assert.Equal(0, callback.Count);
    }

    private static async Task<Subject> Create(
        string producer, ISerializer<string> keySerializer, ISerializer<string> valueSerializer) =>
        producer switch
        {
            Kafka => new SyncSubject(
                KafkaProducer<string, string>.Create(Unreachable(), keySerializer, valueSerializer), historyCount: null),
            AsyncKafka => new AsyncSubject(
                await AsyncKafkaProducer<string, string>.Create(Unreachable(), Async(keySerializer), Async(valueSerializer)),
                historyCount: null),
            Mock => MockSubject(new MockProducer<string, string>(keySerializer, valueSerializer)),
            AsyncMock => AsyncMockSubject(
                new AsyncMockProducer<string, string>(Async(keySerializer), Async(valueSerializer))),
            _ => throw new ArgumentOutOfRangeException(nameof(producer), producer, null),
        };

    private static IAsyncSerializer<string> Async(ISerializer<string> serializer) => serializer switch
    {
        StringSerializer => new AsyncStringSerializer(),
        IAsyncSerializer<string> async => async,
        _ => serializer.AsAsync(),
    };

    private static Subject MockSubject(MockProducer<string, string> mock) => new SyncSubject(mock, mock.HistoryCount);

    private static Subject AsyncMockSubject(AsyncMockProducer<string, string> mock) => new AsyncSubject(mock, mock.HistoryCount);

    private static ProducerRecord<string, string> Record(params IHeader[] headers) =>
        new ProducerRecord<string, string>(Topic, value: "v", headers: headers, key: "k", partition: null);

    // The headers of each record of the core mock's history, which the record holds as its own copies.
    private static List<List<(string? Key, byte[]? Value)>> HistoryHeaders(object producer)
    {
        NativeProducer native = (NativeProducer)producer.GetType()
            .GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(producer)!;
        SafeProducerHandle handle = native.Handle;
        bool added = false;
        try
        {
            IntPtr mock = handle.DangerousAddRefClass(ref added);
            IntPtr outErr = IntPtr.Zero;
            IntPtr history = NativeMethods.MockProducerHistory(mock, ref outErr);
            KafkaException.ThrowIfFfiFailed(ref outErr);
            try
            {
                int count = NativeMethods.ListSize(history, ref outErr);
                KafkaException.ThrowIfFfiFailed(ref outErr);
                List<List<(string? Key, byte[]? Value)>> headers = new List<List<(string? Key, byte[]? Value)>>(count);
                for (int i = 0; i < count; i++)
                {
                    IntPtr record = NativeMethods.ListGet(history, i, ref outErr);
                    KafkaException.ThrowIfFfiFailed(ref outErr);
                    headers.Add(ProducerRecordBuildTests.HeadersOf(record));
                }

                return headers;
            }
            finally
            {
                IntPtr destroyErr = IntPtr.Zero;
                NativeMethods.ListDestroy(history, ref destroyErr);
                KafkaException.ThrowIfFfiFailed(ref destroyErr);
            }
        }
        finally
        {
            if (added)
            {
                handle.DangerousRelease();
            }
        }
    }

    private static void AssertHeader(string key, byte[]? value, (string? Key, byte[]? Value) header)
    {
        Assert.Equal(key, header.Key);
        if (value is null)
        {
            Assert.Null(header.Value);
        }
        else
        {
            Assert.NotNull(header.Value);
            Assert.Equal(value, header.Value);
        }
    }

    private static Dictionary<string, object?> Unreachable() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["max.block.ms"] = "5",
        };

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    private static ReadOnlyMemory<byte>? Encode(string? data) =>
        data is null ? (ReadOnlyMemory<byte>?)null : Encoding.UTF8.GetBytes(data);

    /// <summary>One of the four producers, behind the calls these tests make.</summary>
    private abstract class Subject : IAsyncDisposable
    {
        private readonly Func<int>? _historyCount;

        protected Subject(object producer, Func<int>? historyCount)
        {
            Producer = producer;
            _historyCount = historyCount;
        }

        internal object Producer { get; }

        internal bool IsMock => _historyCount is not null;

        internal int HistoryCount() => _historyCount!();

        internal abstract Task<IllegalArgumentException> Reject(ProducerRecord<string, string> record, ICallback callback);

        internal abstract Task<RecordMetadata?> Deliver(ProducerRecord<string, string> record);

        public abstract ValueTask DisposeAsync();
    }

    /// <summary>A <see cref="IProducer{TKey, TValue}"/> subject.</summary>
    private sealed class SyncSubject : Subject
    {
        private readonly IProducer<string, string> _producer;

        internal SyncSubject(IProducer<string, string> producer, Func<int>? historyCount)
            : base(producer, historyCount) => _producer = producer;

        internal override Task<IllegalArgumentException> Reject(ProducerRecord<string, string> record, ICallback callback) =>
            Task.FromResult(Assert.Throws<IllegalArgumentException>(() => _producer.Send(record, callback)));

        internal override Task<RecordMetadata?> Deliver(ProducerRecord<string, string> record)
        {
            KafkaFuture<RecordMetadata> future = null!;
            TestTimeout.Run(() => future = _producer.Send(record), s_deadline);
            if (IsMock)
            {
                return Task.FromResult<RecordMetadata?>(future.Get(s_deadline));
            }

            Common.Errors.TimeoutException failure =
                FutureAssert.Throws<Common.Errors.TimeoutException>(() => future.Get(s_deadline));
            Assert.Equal(TimeoutCode, failure.Code);
            return Task.FromResult<RecordMetadata?>(null);
        }

        public override ValueTask DisposeAsync()
        {
            TestTimeout.Dispose(_producer);
            return default;
        }
    }

    /// <summary>A <see cref="IAsyncProducer{TKey, TValue}"/> subject.</summary>
    private sealed class AsyncSubject : Subject
    {
        private readonly IAsyncProducer<string, string> _producer;

        internal AsyncSubject(IAsyncProducer<string, string> producer, Func<int>? historyCount)
            : base(producer, historyCount) => _producer = producer;

        // The subject's serializers are awaited, so the rejection ends Send's first stage instead of being thrown.
        internal override async Task<IllegalArgumentException> Reject(
            ProducerRecord<string, string> record, ICallback callback)
        {
            ValueTask<AsyncKafkaFuture<RecordMetadata>> firstStage = _producer.Send(record, callback);
            return await Assert.ThrowsAsync<IllegalArgumentException>(
                () => TestTimeout.Run(() => firstStage.AsTask(), s_deadline));
        }

        internal override async Task<RecordMetadata?> Deliver(ProducerRecord<string, string> record)
        {
            if (IsMock)
            {
                return await TestTimeout.Run(() => _producer.Send(record).Delivery(), s_deadline);
            }

            Common.Errors.TimeoutException failure = await FutureAssert.ThrowsAsync<Common.Errors.TimeoutException>(
                () => TestTimeout.Run(() => _producer.Send(record).Delivery(), s_deadline));
            Assert.Equal(TimeoutCode, failure.Code);
            return null;
        }

        public override ValueTask DisposeAsync() => new ValueTask(TestTimeout.DisposeAsync(_producer));
    }

    /// <summary>
    /// A header of the user's own, with reference equality, that counts its reads and answers its key reads in turn
    /// from a list, the last answer repeated.
    /// </summary>
    private sealed class UserHeader : IHeader
    {
        private readonly string?[] _keys;
        private readonly byte[]? _value;
        private int _keyReads;
        private int _valueReads;

        internal UserHeader(string? key, byte[]? value)
            : this(new[] { key }, value)
        {
        }

        internal UserHeader(string?[] keys, byte[]? value)
        {
            _keys = keys;
            _value = value;
        }

        internal int KeyReads => Volatile.Read(ref _keyReads);

        internal int ValueReads => Volatile.Read(ref _valueReads);

        public string Key()
        {
            int read = Interlocked.Increment(ref _keyReads) - 1;
            return _keys[Math.Min(read, _keys.Length - 1)]!;
        }

        public byte[]? Value()
        {
            Interlocked.Increment(ref _valueReads);
            return _value;
        }
    }

    /// <summary>A header of the user's own that equals another of its class with the same key and value.</summary>
    private sealed class EqualityHeader : IHeader
    {
        private readonly string _key;
        private readonly byte[] _value;

        internal EqualityHeader(string key, byte[] value)
        {
            _key = key;
            _value = value;
        }

        public string Key() => _key;

        public byte[]? Value() => _value;

        public override bool Equals(object? obj) =>
            obj is EqualityHeader other
            && string.Equals(_key, other._key, StringComparison.Ordinal)
            && _value.AsSpan().SequenceEqual(other._value);

        public override int GetHashCode() => StringComparer.Ordinal.GetHashCode(_key);

        public override string ToString() => "EqualityHeader(" + _key + ")";
    }

    /// <summary>A header of the user's own whose string form is <see langword="null"/>.</summary>
    private sealed class NullTextHeader : IHeader
    {
        public string Key() => "k";

        public byte[]? Value() => null;

        public override string? ToString() => null;
    }

    /// <summary>Headers of the user's own, a list in the order they were added.</summary>
    private sealed class UserHeaders : IHeaders
    {
        private readonly List<IHeader> _headers = new List<IHeader>();

        public IHeaders Add(IHeader header)
        {
            _headers.Add(header);
            return this;
        }

        public IHeaders Add(string key, byte[]? value) => Add(new UserHeader(key, value));

        public IHeaders Remove(string key)
        {
            _headers.RemoveAll(header => string.Equals(header.Key(), key, StringComparison.Ordinal));
            return this;
        }

        public IHeader? LastHeader(string key) =>
            _headers.LastOrDefault(header => string.Equals(header.Key(), key, StringComparison.Ordinal));

        public IEnumerable<IHeader> Headers(string key) =>
            _headers.Where(header => string.Equals(header.Key(), key, StringComparison.Ordinal)).ToList();

        public IHeader[] ToArray() => _headers.ToArray();

        public IEnumerator<IHeader> GetEnumerator() => _headers.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>A string serializer whose headers form adds a header of the user's own to the record's headers.</summary>
    private sealed class UserHeaderAddingSerializer : ISerializerWithHeaders<string>
    {
        private readonly IHeader _header;

        internal UserHeaderAddingSerializer(IHeader header) => _header = header;

        public ReadOnlyMemory<byte>? Serialize(string topic, string? data) => Encode(data);

        public ReadOnlyMemory<byte>? Serialize(string topic, IHeaders headers, string? data)
        {
            headers.Add(_header);
            return Encode(data);
        }
    }

    /// <summary>A string serializer that counts its calls.</summary>
    private sealed class CountingSerializer : ISerializer<string>
    {
        private int _calls;

        internal int Calls => Volatile.Read(ref _calls);

        public ReadOnlyMemory<byte>? Serialize(string topic, string? data)
        {
            Interlocked.Increment(ref _calls);
            return Encode(data);
        }
    }

    /// <summary>A delivery callback that counts its calls.</summary>
    private sealed class CountingCallback : ICallback
    {
        private int _count;

        internal int Count => Volatile.Read(ref _count);

        public void OnCompletion(RecordMetadata metadata, Exception? exception) => Interlocked.Increment(ref _count);
    }
}
