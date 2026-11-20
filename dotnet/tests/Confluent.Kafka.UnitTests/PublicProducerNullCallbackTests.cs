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

using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;
using Xunit;

using static Confluent.Kafka.UnitTests.UnreachableProducers;

using KafkaTimeoutException = Confluent.Kafka.Common.Errors.TimeoutException;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A null delivery callback means no callback, as Java's <c>send(record, null)</c>: on all four producers, for the
/// <see cref="ICallback"/> and the <see cref="IAsyncCallback"/> overloads alike, the record is sent as
/// <c>Send(record)</c> sends it. The real producers face an unreachable broker, so the record's future fails with the
/// metadata timeout, as it does without a callback.
/// </summary>
public sealed class PublicProducerNullCallbackTests
{
    private const int MaxBlockMs = 500;

    [Fact]
    public void KafkaProducer_SendWithANullCallback_SendsTheRecord()
    {
        string topic = Topic("null-callback");
        using KafkaProducer<byte[], byte[]> producer = Sync(MaxBlockMs);

        KafkaFuture<RecordMetadata> future = producer.Send(Record(topic), null);

        KafkaTimeoutException failure = FutureAssert.Throws<KafkaTimeoutException>(() => future.Get(Deadline));
        Assert.Equal(MetadataTimeout(topic, MaxBlockMs), failure.Message);
    }

    [Fact]
    public async Task AsyncKafkaProducer_SendWithANullCallback_SendsTheRecord_ForBothCallbackForms()
    {
        string topic = Topic("async-null-callback");
        await using AsyncKafkaProducer<byte[], byte[]> producer = Async(MaxBlockMs);

        AsyncKafkaFuture<RecordMetadata> withCallback = await producer.Send(Record(topic), (ICallback?)null);
        AsyncKafkaFuture<RecordMetadata> withAsyncCallback = await producer.Send(Record(topic), (IAsyncCallback?)null);

        foreach (AsyncKafkaFuture<RecordMetadata> future in new[] { withCallback, withAsyncCallback })
        {
            KafkaTimeoutException failure =
                await FutureAssert.ThrowsAsync<KafkaTimeoutException>(() => future.Get(Deadline));
            Assert.Equal(MetadataTimeout(topic, MaxBlockMs), failure.Message);
        }
    }

    [Fact]
    public void MockProducer_SendWithANullCallback_SendsTheRecord()
    {
        using MockProducer<byte[], byte[]> producer =
            new MockProducer<byte[], byte[]>(Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());

        RecordMetadata metadata = producer.Send(Record("t"), null).Get(Deadline);

        Assert.Equal("t", metadata.Topic());
        Assert.Equal(1, producer.HistoryCount());
    }

    [Fact]
    public async Task AsyncMockProducer_SendWithANullCallback_SendsTheRecord_ForBothCallbackForms()
    {
        await using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray().Serializer(), Serdes.ByteArray().Serializer());

        AsyncKafkaFuture<RecordMetadata> withCallback = await producer.Send(Record("t"), (ICallback?)null);
        AsyncKafkaFuture<RecordMetadata> withAsyncCallback = await producer.Send(Record("t"), (IAsyncCallback?)null);

        Assert.Equal("t", (await withCallback.Get(Deadline)).Topic());
        Assert.Equal("t", (await withAsyncCallback.Get(Deadline)).Topic());
        Assert.Equal(2, producer.HistoryCount());
    }
}
