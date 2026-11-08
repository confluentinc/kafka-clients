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

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// <see cref="RecordMetadata"/>: Java's constructor and its batch-index rule, a null topic partition, the <c>Has*</c>
/// and serialized-size properties, <see cref="RecordMetadata.UNKNOWN_PARTITION"/>, the metadata the core reports, and
/// what the two producer mocks deliver on success and on failure.
/// </summary>
public sealed class PublicRecordMetadataTests
{
    private const string Sync = "sync";
    private const string Async = "async";
    private const string Topic = "metadata-topic";

    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    [Fact]
    public void ConstructionWithMissingBatchIndex()
    {
        TopicPartition tp = new TopicPartition("foo", 0);
        long timestamp = 2340234L;
        int keySize = 3;
        int valueSize = 5;

        RecordMetadata metadata = new RecordMetadata(tp, -1L, -1, timestamp, keySize, valueSize);

        Assert.Equal(tp.Topic, metadata.Topic);
        Assert.Equal(tp.Partition, metadata.Partition);
        Assert.Equal(timestamp, metadata.Timestamp);
        Assert.False(metadata.HasOffset);
        Assert.Equal(-1L, metadata.Offset);
        Assert.Equal(keySize, metadata.SerializedKeySize);
        Assert.Equal(valueSize, metadata.SerializedValueSize);
    }

    [Fact]
    public void ConstructionWithBatchIndexOffset()
    {
        TopicPartition tp = new TopicPartition("foo", 0);
        long timestamp = 2340234L;
        int keySize = 3;
        int valueSize = 5;
        long baseOffset = 15L;
        int batchIndex = 3;

        RecordMetadata metadata = new RecordMetadata(tp, baseOffset, batchIndex, timestamp, keySize, valueSize);

        Assert.Equal(tp.Topic, metadata.Topic);
        Assert.Equal(tp.Partition, metadata.Partition);
        Assert.Equal(timestamp, metadata.Timestamp);
        Assert.Equal(baseOffset + batchIndex, metadata.Offset);
        Assert.Equal(keySize, metadata.SerializedKeySize);
        Assert.Equal(valueSize, metadata.SerializedValueSize);
    }

    [Fact]
    public void Surface_UnknownPartition_HasOffset_HasTimestamp_AndToString()
    {
        RecordMetadata metadata = new RecordMetadata(new TopicPartition("foo", 2), 7L, 0, -1L, -1, -1);

        Assert.Equal(-1, RecordMetadata.UNKNOWN_PARTITION);
        Assert.True(metadata.HasOffset);
        Assert.False(metadata.HasTimestamp);
        Assert.Equal("foo-2@7", metadata.ToString());
    }

    [Fact]
    public void NullTopicPartition_Constructs_AndTheOtherMembersAnswer()
    {
        RecordMetadata metadata = new RecordMetadata(null, 15L, 3, 2340234L, 3, 5);

        Assert.True(metadata.HasOffset);
        Assert.Equal(18L, metadata.Offset);
        Assert.True(metadata.HasTimestamp);
        Assert.Equal(2340234L, metadata.Timestamp);
        Assert.Equal(3, metadata.SerializedKeySize);
        Assert.Equal(5, metadata.SerializedValueSize);
    }

    [Theory]
    [InlineData(nameof(RecordMetadata.Topic))]
    [InlineData(nameof(RecordMetadata.Partition))]
    [InlineData(nameof(RecordMetadata.ToString))]
    public void NullTopicPartition_TopicPartitionAndToStringThrow(string member)
    {
        RecordMetadata metadata = new RecordMetadata(null, 15L, 3, 2340234L, 3, 5);

        InvalidOperationException e = Assert.Throws<InvalidOperationException>(() => Read(metadata, member));
        Assert.Equal("topicPartition must not be null", e.Message);
    }

    [Theory]
    [InlineData("t", 0, 5L, 123L, -1, 100)]
    [InlineData("orders", 7, -1L, -1L, 3, 0)]
    [InlineData("a", int.MaxValue, 0L, 0L, 0, 0)]
    public void FromTheCore_IsWhatTheConstructorBuilds(
        string topic, int partition, long offset, long timestamp, int keySize, int valueSize)
    {
        TopicPartition tp = new TopicPartition(topic, partition);

        RecordMetadata made = new RecordMetadata(tp, offset, timestamp, keySize, valueSize);
        RecordMetadata built = new RecordMetadata(tp, offset, 0, timestamp, keySize, valueSize);

        Assert.Equal(built.Topic, made.Topic);
        Assert.Equal(built.Partition, made.Partition);
        Assert.Equal(built.Offset, made.Offset);
        Assert.Equal(built.HasOffset, made.HasOffset);
        Assert.Equal(built.Timestamp, made.Timestamp);
        Assert.Equal(built.HasTimestamp, made.HasTimestamp);
        Assert.Equal(built.SerializedKeySize, made.SerializedKeySize);
        Assert.Equal(built.SerializedValueSize, made.SerializedValueSize);
        Assert.Equal(built.ToString(), made.ToString());
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task MockDelivery_ReportsSizesZero_AndNoTimestamp(string flavor)
    {
        // Java's MockProducer reports serialized sizes 0 and no timestamp, whatever the record carries.
        RecordingCallback callback = new RecordingCallback();
        RecordMetadata metadata = flavor == Sync
            ? SendOnMockProducer(callback, fail: false)
            : await SendOnAsyncMockProducer(callback, fail: false);

        Assert.Same(metadata, callback.Metadata);
        Assert.Null(callback.Exception);
        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(2, metadata.Partition);
        Assert.True(metadata.HasOffset);
        Assert.Equal(0L, metadata.Offset);
        Assert.False(metadata.HasTimestamp);
        Assert.Equal(-1L, metadata.Timestamp);
        Assert.Equal(0, metadata.SerializedKeySize);
        Assert.Equal(0, metadata.SerializedValueSize);
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task MockFailedDelivery_ThePlaceholderHasSizesMinusOne(string flavor)
    {
        RecordingCallback callback = new RecordingCallback();
        if (flavor == Sync)
        {
            SendOnMockProducer(callback, fail: true);
        }
        else
        {
            await SendOnAsyncMockProducer(callback, fail: true);
        }

        RecordMetadata placeholder = Assert.IsType<RecordMetadata>(callback.Metadata);
        Assert.Equal(2, Assert.IsType<CorruptRecordException>(callback.Exception).Code);
        Assert.Equal(Topic, placeholder.Topic);
        Assert.Equal(2, placeholder.Partition);
        Assert.False(placeholder.HasOffset);
        Assert.Equal(-1L, placeholder.Offset);
        Assert.False(placeholder.HasTimestamp);
        Assert.Equal(-1L, placeholder.Timestamp);
        Assert.Equal(-1, placeholder.SerializedKeySize);
        Assert.Equal(-1, placeholder.SerializedValueSize);
    }

    private static object Read(RecordMetadata metadata, string member) => member switch
    {
        nameof(RecordMetadata.Topic) => metadata.Topic,
        nameof(RecordMetadata.Partition) => metadata.Partition,
        nameof(RecordMetadata.ToString) => metadata.ToString(),
        _ => throw new ArgumentOutOfRangeException(nameof(member), member, null),
    };

    private static ProducerRecord<byte[], byte[]> Record() =>
        new ProducerRecord<byte[], byte[]>(
            Topic, Encoding.UTF8.GetBytes("value"), Encoding.UTF8.GetBytes("key"), partition: 2, timestamp: 42L);

    private static RecordMetadata SendOnMockProducer(RecordingCallback callback, bool fail)
    {
        MockProducer<byte[], byte[]> producer =
            new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: !fail);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        RecordMetadata metadata = null!;
        TestTimeout.Run(
            () =>
            {
                KafkaFuture<RecordMetadata> future = producer.Send(Record(), callback);
                if (fail)
                {
                    Assert.True(producer.ErrorNext(new CorruptRecordException("metadata-failure")));
                    FutureAssert.Throws<CorruptRecordException>(() => future.Get());
                }
                else
                {
                    metadata = future.Get();
                }
            },
            s_deadline);
        return metadata;
    }

    private static async Task<RecordMetadata> SendOnAsyncMockProducer(RecordingCallback callback, bool fail)
    {
        AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: !fail);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);
        RecordMetadata metadata = null!;
        await TestTimeout.Run(
            async () =>
            {
                AsyncKafkaFuture<RecordMetadata> future = await producer.Send(Record(), callback);
                if (fail)
                {
                    Assert.True(producer.ErrorNext(new CorruptRecordException("metadata-failure")));
                    await FutureAssert.ThrowsAsync<CorruptRecordException>(() => future.Get());
                }
                else
                {
                    metadata = await future.Get();
                }
            },
            s_deadline);
        return metadata;
    }

    private sealed class RecordingCallback : ICallback
    {
        internal RecordMetadata? Metadata { get; private set; }

        internal Exception? Exception { get; private set; }

        public void OnCompletion(RecordMetadata metadata, Exception? exception)
        {
            Metadata = metadata;
            Exception = exception;
        }
    }
}
