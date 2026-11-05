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
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A topic or a header key the C ABI would change, one holding a NUL character or an unpaired UTF-16 surrogate, makes
/// the send throw <see cref="ArgumentException"/> on all four producers, before the record reaches the core: the Kafka
/// producers have sealed the headers by then, the mocks leave them writable, no callback fires for the record, and the
/// producer still sends. A non-ASCII topic and header key, a surrogate pair included, are sent.
/// </summary>
public sealed class PublicProducerSendStringTests
{
    private const string TopicMessage =
        "A topic must not contain a NUL character or an unpaired UTF-16 " +
        "surrogate: such a string cannot be passed to the native client unchanged.";

    private const string HeaderKeyMessage =
        "A header key must not contain a NUL character or an unpaired UTF-16 " +
        "surrogate: such a string cannot be passed to the native client unchanged.";

    private const string Topic = "topic";

    private const string TopicField = "topic";

    private const string HeaderKeyField = "header-key";

    private const string Kafka = nameof(KafkaProducer<string, string>);

    private const string AsyncKafka = nameof(AsyncKafkaProducer<string, string>);

    private const string Mock = nameof(MockProducer<string, string>);

    private const string AsyncMock = nameof(AsyncMockProducer<string, string>);

    private const string NonAsciiTopic = "tópico-ключ-🔑";

    private const string NonAsciiKey = "clé-ключ-🔑";

    private const int TimeoutCode = 7;

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private static readonly string[] s_producers = { Kafka, AsyncKafka, Mock, AsyncMock };

    // Looked up by label, never passed as theory data: the InlineData round trip replaces a lone surrogate with
    // U+FFFD, so the row would test a valid string.
    private static readonly IReadOnlyDictionary<string, string> s_altered =
        new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["nul"] = "t\0u",
            ["lone-high"] = "t\uD800u",
            ["lone-low"] = "t\uDC00u",
        };

    public static IEnumerable<object[]> RejectedCases() =>
        from producer in s_producers
        from field in new[] { TopicField, HeaderKeyField }
        from label in s_altered.Keys
        select new object[] { producer, field, label };

    public static IEnumerable<object[]> Producers() => s_producers.Select(producer => new object[] { producer });

    [Fact]
    public void LabelTable_HoldsTheStringsItsNamesSay()
    {
        Assert.Equal('\0', s_altered["nul"][1]);
        Assert.True(char.IsHighSurrogate(s_altered["lone-high"][1]));
        Assert.True(char.IsLowSurrogate(s_altered["lone-low"][1]));
        Assert.True(char.IsSurrogatePair(NonAsciiTopic[NonAsciiTopic.Length - 2], NonAsciiTopic[NonAsciiTopic.Length - 1]));
        Assert.True(char.IsSurrogatePair(NonAsciiKey[NonAsciiKey.Length - 2], NonAsciiKey[NonAsciiKey.Length - 1]));
    }

    [Theory]
    [MemberData(nameof(RejectedCases))]
    public async Task Send_ATopicOrAHeaderKeyTheAbiWouldChange_IsRejected(string producer, string field, string label)
    {
        await using Subject subject = Create(producer);
        string altered = s_altered[label];
        ProducerRecord<string, string> record = field == TopicField ? Record(altered, "h") : Record(Topic, altered);

        ArgumentException rejected = subject.Reject(record, callback: null);

        Assert.Equal("record", rejected.ParamName);
        string message = field == TopicField ? TopicMessage : HeaderKeyMessage;
        Assert.Equal(new ArgumentException(message, "record").Message, rejected.Message);

        Assert.Equal(!subject.IsMock, record.Headers.IsReadOnly);

        if (subject.IsMock)
        {
            Assert.Equal(0, subject.HistoryCount());
            RecordMetadata? metadata = await subject.Deliver(Record(Topic, "h"));
            Assert.Equal(0L, metadata!.Offset);
            Assert.Equal(1, subject.HistoryCount());
        }
        else
        {
            Assert.Null(await subject.Deliver(Record(Topic, "h")));
        }
    }

    [Theory]
    [MemberData(nameof(Producers))]
    public async Task Send_WithACallback_ARejectedRecordFiresNoCallback(string producer)
    {
        CountingCallback callback = new CountingCallback();
        Subject subject = Create(producer);
        try
        {
            subject.Reject(Record(s_altered["nul"], "h"), callback);
            subject.Reject(Record(Topic, s_altered["lone-low"]), callback);
            await subject.Deliver(Record(Topic, "h"));
        }
        finally
        {
            await subject.DisposeAsync();
        }

        // The producer's destroy runs every pending callback, so a record that had reached the core would have fired
        // its callback by now.
        Assert.Equal(0, callback.Count);
    }

    [Theory]
    [MemberData(nameof(Producers))]
    public async Task Send_ANonAsciiTopicAndHeaderKey_IsSent(string producer)
    {
        await using Subject subject = Create(producer);
        ProducerRecord<string, string> record = Record(NonAsciiTopic, NonAsciiKey);

        RecordMetadata? metadata = await subject.Deliver(record);

        Assert.Equal(!subject.IsMock, record.Headers.IsReadOnly);
        if (subject.IsMock)
        {
            Assert.Equal(NonAsciiTopic, metadata!.Topic);
            Assert.Equal(1, subject.HistoryCount());
        }
    }

    private static Subject Create(string producer)
    {
        switch (producer)
        {
            case Kafka:
                return new SyncSubject(
                    new KafkaProducer<string, string>(Unreachable(), Serdes.String, Serdes.String), historyCount: null);
            case AsyncKafka:
                return new AsyncSubject(
                    new AsyncKafkaProducer<string, string>(Unreachable(), Serdes.String, Serdes.String),
                    historyCount: null);
            case Mock:
                MockProducer<string, string> mock = new MockProducer<string, string>(Serdes.String, Serdes.String);
                return new SyncSubject(mock, mock.HistoryCount);
            case AsyncMock:
                AsyncMockProducer<string, string> asyncMock =
                    new AsyncMockProducer<string, string>(Serdes.String, Serdes.String);
                return new AsyncSubject(asyncMock, asyncMock.HistoryCount);
            default:
                throw new ArgumentOutOfRangeException(nameof(producer), producer, null);
        }
    }

    private static ProducerRecord<string, string> Record(string topic, string headerKey) =>
        new ProducerRecord<string, string>(
            topic,
            "value",
            "key",
            headers: new[] { new Header("first", Bytes("1")), new Header(headerKey, Bytes("2")) });

    private static Dictionary<string, object?> Unreachable() =>
        new Dictionary<string, object?>
        {
            ["bootstrap.servers"] = "localhost:1",
            ["max.block.ms"] = "5",
        };

    private static byte[] Bytes(string value) => Encoding.UTF8.GetBytes(value);

    /// <summary>One of the four producers, behind the calls these tests make.</summary>
    private abstract class Subject : IAsyncDisposable
    {
        private readonly Func<int>? _historyCount;

        protected Subject(Func<int>? historyCount) => _historyCount = historyCount;

        internal bool IsMock => _historyCount is not null;

        internal int HistoryCount() => _historyCount!();

        internal abstract ArgumentException Reject(ProducerRecord<string, string> record, IDeliveryCallback? callback);

        internal abstract Task<RecordMetadata?> Deliver(ProducerRecord<string, string> record);

        public abstract ValueTask DisposeAsync();
    }

    /// <summary>A <see cref="IProducer{TKey, TValue}"/> subject.</summary>
    private sealed class SyncSubject : Subject
    {
        private readonly IProducer<string, string> _producer;

        internal SyncSubject(IProducer<string, string> producer, Func<int>? historyCount)
            : base(historyCount) => _producer = producer;

        internal override ArgumentException Reject(ProducerRecord<string, string> record, IDeliveryCallback? callback) =>
            Assert.Throws<ArgumentException>(
                () => callback is null ? _producer.Send(record) : _producer.Send(record, callback));

        internal override Task<RecordMetadata?> Deliver(ProducerRecord<string, string> record)
        {
            KafkaFuture<RecordMetadata> future = default;
            TestTimeout.Run(() => future = _producer.Send(record), s_deadline);
            if (IsMock)
            {
                return Task.FromResult<RecordMetadata?>(future.Get(s_deadline));
            }

            Common.Errors.TimeoutException failure = Assert.Throws<Common.Errors.TimeoutException>(() => future.Get(s_deadline));
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
            : base(historyCount) => _producer = producer;

        internal override ArgumentException Reject(ProducerRecord<string, string> record, IDeliveryCallback? callback) =>
            Assert.Throws<ArgumentException>(
                () => { _ = callback is null ? _producer.Send(record) : _producer.Send(record, callback); });

        internal override async Task<RecordMetadata?> Deliver(ProducerRecord<string, string> record)
        {
            if (IsMock)
            {
                return await TestTimeout.Run(() => _producer.Send(record).Delivery(), s_deadline);
            }

            Common.Errors.TimeoutException failure = await Assert.ThrowsAsync<Common.Errors.TimeoutException>(
                () => TestTimeout.Run(() => _producer.Send(record).Delivery(), s_deadline));
            Assert.Equal(TimeoutCode, failure.Code);
            return null;
        }

        public override ValueTask DisposeAsync() => new ValueTask(TestTimeout.DisposeAsync(_producer));
    }

    /// <summary>A delivery callback that counts its calls.</summary>
    private sealed class CountingCallback : IDeliveryCallback
    {
        private int _count;

        internal int Count => Volatile.Read(ref _count);

        public void OnCompletion(RecordMetadata metadata, Exception? exception) => Interlocked.Increment(ref _count);
    }
}
