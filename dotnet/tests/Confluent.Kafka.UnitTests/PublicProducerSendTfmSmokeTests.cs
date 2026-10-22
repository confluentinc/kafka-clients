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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The TFM-matrix smoke test for the async send path: the mock's auto-complete and manual send round-trips and a
/// real <see cref="AsyncKafkaProducer{TKey, TValue}"/> created and disposed, with netstandard2.0-safe APIs only
/// so it compiles on net462.
/// </summary>
public sealed class PublicProducerSendTfmSmokeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "tfm-send-topic";

    [Fact]
    public async Task MockProducer_SendAutoComplete_RoundTrips()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        RecordMetadata metadata = default!;
        await TestTimeout.Run(
            async () => metadata = await producer.Send(
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("value"), Encoding.UTF8.GetBytes("key"), partition: 0)).Delivery(),
            s_deadline);

        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(0, metadata.Partition);
        Assert.Equal(0L, metadata.Offset);
    }

    [Fact]
    public async Task MockProducer_SendManualComplete_RoundTrips()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("value"), partition: 0)).Delivery();
        Assert.True(producer.CompleteNext());

        RecordMetadata metadata = default!;
        await TestTimeout.Run(async () => metadata = await sendTask, s_deadline);
        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(1, producer.HistoryCount());
    }

    [Fact]
    public async Task MockProducer_SendManualError_RoundTrips()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("value"), partition: 0)).Delivery();
        Assert.True(producer.ErrorNext(new KafkaException(2, "tfm-error", isRetriable: true)));

        await Assert.ThrowsAsync<KafkaException>(async () => await sendTask);
    }

    [Fact]
    public async Task RealProducer_CreateDisposeAsync_RoundTrips()
    {
        // A real producer is created and disposed but sends nothing: without a broker a send would not deliver.
        AsyncKafkaProducer<byte[], byte[]> producer = new AsyncKafkaProducer<byte[], byte[]>(
            new System.Collections.Generic.Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" },
            Serdes.ByteArray,
            Serdes.ByteArray);

        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
    }
}
