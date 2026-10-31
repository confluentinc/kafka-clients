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
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The TFM-matrix smoke test for the sync producer: a mock send round-trip, the manual mock path, and a real
/// <see cref="KafkaProducer{TKey, TValue}"/> created and disposed, with netstandard2.0-safe APIs only so it
/// compiles on net462.
/// </summary>
public sealed class PublicSyncProducerTfmSmokeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "sync-tfm-topic";

    [Fact]
    public void MockProducer_SendCloseRoundTrip_AutoComplete()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        RecordMetadata metadata = null!;
        TestTimeout.Run(
            () => metadata = producer.Send(
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("value"), Encoding.UTF8.GetBytes("key"), partition: 0)).Get(),
            s_deadline);

        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(0, metadata.Partition);
        Assert.Equal(0L, metadata.Offset);

        TestTimeout.Run(producer.Close, s_deadline);
    }

    [Fact]
    public async Task MockProducer_SendManualComplete_RoundTrips()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        Task<RecordMetadata> sendTask = Task.Run(
            () => producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("value"), partition: 0)).Get());

        DriveUntilResolved(producer.CompleteNext);

        RecordMetadata metadata = null!;
        await TestTimeout.Run(async () => metadata = await sendTask, s_deadline);
        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(1, producer.HistoryCount());
    }

    [Fact]
    public async Task MockProducer_SendManualError_RoundTrips()
    {
        MockProducer<byte[], byte[]> producer = new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        using IDisposable producerDisposal = TestTimeout.Disposal(producer);

        Task<RecordMetadata> sendTask = Task.Run(
            () => producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("value"), partition: 0)).Get());

        DriveUntilResolved(() => producer.ErrorNext(new KafkaException(2, "tfm-error", isRetriable: true)));

        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => sendTask, s_deadline));
    }

    [Fact]
    public void RealProducer_CreateDispose_RoundTrips()
    {
        // A real producer is created and disposed but sends nothing: without a broker a send would not deliver.
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(
            new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" },
            Serdes.ByteArray,
            Serdes.ByteArray);

        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    // Retries drive until it resolves the worker's pending send or the deadline passes.
    private static void DriveUntilResolved(Func<bool> drive)
    {
        Stopwatch sw = Stopwatch.StartNew();
        while (!drive())
        {
            if (sw.Elapsed > s_deadline)
            {
                throw new TimeoutException(
                    "No pending send registered to drive within the deadline — treated as a hang.");
            }

            Thread.Sleep(2);
        }
    }
}
