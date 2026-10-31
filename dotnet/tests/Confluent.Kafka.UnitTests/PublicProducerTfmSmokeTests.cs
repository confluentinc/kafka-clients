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
using System.Threading.Tasks;

using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Serialization;
using Confluent.Kafka.Producer;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The TFM-matrix smoke test for the async producer: a mock create, flush, partitions-for and close round-trip,
/// Dispose, and a real <see cref="AsyncKafkaProducer{TKey, TValue}"/> created and disposed, with
/// netstandard2.0-safe APIs only so it compiles on net462.
/// </summary>
public sealed class PublicProducerTfmSmokeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "tfm-producer-topic";

    [Fact]
    public async Task MockProducer_FlushPartitionsForClose_RoundTrips()
    {
        IAsyncProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        try
        {
            await TestTimeout.Run(() => producer.Flush(), s_deadline);

            IReadOnlyList<PartitionInfo> partitions = default!;
            await TestTimeout.Run(async () => partitions = await producer.PartitionsFor(Topic), s_deadline);

            // Empty: the mock's cluster is empty.
            Assert.NotNull(partitions);
            Assert.Empty(partitions);
        }
        finally
        {
            await TestTimeout.Run(() => producer.Close(), s_deadline);
        }
    }

    [Fact]
    public void MockProducer_FlushClose_ViaSyncDispose_RoundTrips()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    [Fact]
    public async Task RealProducer_CreateDisposeAsync_RoundTrips()
    {
        AsyncKafkaProducer<byte[], byte[]> producer = new AsyncKafkaProducer<byte[], byte[]>(
            new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" },
            Serdes.ByteArray,
            Serdes.ByteArray);

        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
    }
}
