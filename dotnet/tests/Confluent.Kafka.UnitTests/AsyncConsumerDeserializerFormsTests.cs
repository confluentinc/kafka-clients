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
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Consumer;
using Confluent.Kafka.Internal;
using Confluent.Kafka.UnitTests.Interop;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Which form of an asynchronous deserializer a consumer calls: a plain <see cref="IAsyncDeserializer{T}"/> is called
/// without headers, an <see cref="IAsyncDeserializerWithHeaders{T}"/> with the record's own headers, and an
/// <see cref="IAsyncDeserializerWithRetention{T}"/> through its headers form, over a memory it may keep. The
/// <see cref="AsyncKafkaConsumer{TKey, TValue}"/> is checked through its public surface, and the poll both consumers
/// run (<c>NativeConsumer.PollWithCallback</c> over the deserializer calls they build) over a record with headers.
/// </summary>
public sealed class AsyncConsumerDeserializerFormsTests
{
    private const string Topic = "deserializer-forms-topic";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    [Fact]
    public async Task OnTheMock_APlainDeserializer_IsCalledWithoutHeaders_ForTheKeyThenTheValue()
    {
        Plain deserializer = new Plain();
        AsyncKafkaConsumer<string, string> consumer = DecodingConsumers.CreateAsync<string, string>(deserializer, deserializer);
        await using (TestTimeout.AsyncDisposal(consumer))
        {
            ConsumerRecord<string, string> record = await PollOne(consumer);

            Assert.Equal("key", record.Key());
            Assert.Equal("value", record.Value());
            Assert.Equal(new[] { "key", "value" }, deserializer.Calls);
        }
    }

    [Fact]
    public async Task OnTheMock_AHeadersDeserializer_IsCalledWithTheRecordsOwnHeaders_ForTheKeyThenTheValue()
    {
        WithHeaders deserializer = new WithHeaders();
        AsyncKafkaConsumer<string, string> consumer = DecodingConsumers.CreateAsync<string, string>(deserializer, deserializer);
        await using (TestTimeout.AsyncDisposal(consumer))
        {
            ConsumerRecord<string, string> record = await PollOne(consumer);

            Assert.Equal("key|0", record.Key());
            Assert.Equal("value|0", record.Value());
            Assert.Equal(2, deserializer.Seen.Count);
            Assert.Same(record.Headers(), deserializer.Seen[0]);
            Assert.Same(record.Headers(), deserializer.Seen[1]);
        }
    }

    [Fact]
    public async Task OnTheMock_ARetainingDeserializer_IsCalledWithHeaders_AndKeepsItsMemory()
    {
        Retaining deserializer = new Retaining();
        AsyncKafkaConsumer<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> consumer =
            DecodingConsumers.CreateAsync<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?>(deserializer, deserializer);
        ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> record;
        await using (TestTimeout.AsyncDisposal(consumer))
        {
            record = await PollOne(consumer);

            Assert.Same(record.Headers(), deserializer.Seen[0]);
            Assert.Same(record.Headers(), deserializer.Seen[1]);
        }

        Assert.Equal("key", Text(record.Key()!.Value));
        Assert.Equal("value", Text(record.Value()!.Value));
    }

    [Fact]
    public async Task OnTheConsumersPoll_APlainDeserializer_IsCalledWithoutHeaders_ARecordWithHeadersIncluded()
    {
        Plain deserializer = new Plain();

        ConsumerRecord<string, string> record = await PollOneWithHeaders(deserializer);

        Assert.Equal("value", record.Value());
        Assert.Equal(new[] { "key", "value" }, deserializer.Calls);
        Assert.Equal(Bytes("one"), record.Headers().LastHeader("h")?.Value());
    }

    [Fact]
    public async Task OnTheConsumersPoll_AHeadersDeserializer_IsCalledWithTheRecordsOwnHeaders()
    {
        WithHeaders deserializer = new WithHeaders();

        ConsumerRecord<string, string> record = await PollOneWithHeaders(deserializer);

        Assert.Equal("key|1", record.Key());
        Assert.Equal("value|1", record.Value());
        Assert.Same(record.Headers(), deserializer.Seen[0]);
        Assert.Same(record.Headers(), deserializer.Seen[1]);
    }

    [Fact]
    public async Task OnTheConsumersPoll_ARetainingDeserializer_IsCalledWithTheRecordsOwnHeaders_AndKeepsItsMemory()
    {
        Retaining deserializer = new Retaining();
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using (IDisposable blocks = Seed(consumer))
        using (TestTimeout.Disposal(consumer))
        {
            ConsumerRecords<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> records = await TestTimeout.Run(
                () => consumer.PollWithCallback(
                    s_pollTimeout,
                    new AsyncDeserializerCall<ReadOnlyMemory<byte>?>(deserializer),
                    new AsyncDeserializerCall<ReadOnlyMemory<byte>?>(deserializer)),
                s_deadline);
            ConsumerRecord<ReadOnlyMemory<byte>?, ReadOnlyMemory<byte>?> record = Assert.Single(records);

            Assert.Same(record.Headers(), deserializer.Seen[0]);
            Assert.Same(record.Headers(), deserializer.Seen[1]);
            Assert.Equal("key", Text(record.Key()!.Value));
            Assert.Equal("value", Text(record.Value()!.Value));
        }
    }

    private static async Task<ConsumerRecord<TKey, TValue>> PollOne<TKey, TValue>(
        AsyncKafkaConsumer<TKey, TValue> consumer)
    {
        TopicPartition partition = new TopicPartition(Topic, 0);
        await TestTimeout.Run(() => consumer.Assign(new[] { partition }), s_deadline);
        await TestTimeout.Run(() => consumer.Seek(partition, 0), s_deadline);
        consumer.AddRecord(Topic, 0, 0, Bytes("key"), Bytes("value"));
        return Assert.Single(await TestTimeout.Run(() => consumer.Poll(s_pollTimeout), s_deadline));
    }

    private static async Task<ConsumerRecord<string, string>> PollOneWithHeaders(IAsyncDeserializer<string> deserializer)
    {
        NativeConsumer consumer = NativeConsumer.CreateMock();
        using IDisposable blocks = Seed(consumer);
        using IDisposable consumerDisposal = TestTimeout.Disposal(consumer);
        ConsumerRecords<string, string> records = await TestTimeout.Run(
            () => consumer.PollWithCallback(
                s_pollTimeout,
                new AsyncDeserializerCall<string>(deserializer),
                new AsyncDeserializerCall<string>(deserializer)),
            s_deadline);
        return Assert.Single(records);
    }

    // Declared before the consumer's disposal, so the blocks the record points into outlive the consumer.
    private static IDisposable Seed(NativeConsumer consumer)
    {
        consumer.Assign(new[] { (Topic, 0) });
        consumer.Seek(new TopicPartition(Topic, 0), offset: 0);
        return ConsumerPollHeadersTests.AddRecordWithHeaders(
            consumer, Topic, 0, offset: 0, key: Bytes("key"), value: Bytes("value"), ("h", Bytes("one")));
    }

    private static byte[] Bytes(string text) => Encoding.UTF8.GetBytes(text);

    private static string Text(ReadOnlyMemory<byte> data) => Encoding.UTF8.GetString(data.ToArray());

    /// <summary>A UTF-8 deserializer with the form without headers only, recording the texts it is given.</summary>
    private sealed class Plain : IAsyncDeserializer<string>
    {
        internal List<string> Calls { get; } = new List<string>();

        public ValueTask<string?> Deserialize(
            string topic, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            Calls.Add(Text(data));
            return new ValueTask<string?>(Text(data));
        }
    }

    /// <summary>
    /// A UTF-8 deserializer with both forms, whose headers form appends the number of headers and records the
    /// headers it is given; its form without headers throws, so a consumer calling it fails the test.
    /// </summary>
    private sealed class WithHeaders : IAsyncDeserializerWithHeaders<string>
    {
        internal List<IHeaders> Seen { get; } = new List<IHeaders>();

        public ValueTask<string?> Deserialize(
            string topic, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("the consumer called the form without headers");

        public ValueTask<string?> Deserialize(
            string topic, IHeaders headers, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            Seen.Add(headers);
            return new ValueTask<string?>(Text(data) + "|" + headers.ToArray().Length);
        }
    }

    /// <summary>
    /// A deserializer that keeps the memory it is given, which its interface allows, recording the headers it is
    /// given; its form without headers throws, so a consumer calling it fails the test.
    /// </summary>
    private sealed class Retaining : IAsyncDeserializerWithRetention<ReadOnlyMemory<byte>?>
    {
        internal List<IHeaders> Seen { get; } = new List<IHeaders>();

        public ValueTask<ReadOnlyMemory<byte>?> Deserialize(
            string topic, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("the consumer called the form without headers");

        public async ValueTask<ReadOnlyMemory<byte>?> Deserialize(
            string topic, IHeaders headers, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
        {
            Seen.Add(headers);
            await Task.Yield();
            return data;
        }
    }
}
