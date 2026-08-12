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
/// The TFM-matrix smoke test (PLAN §5.7): the public <see cref="AsyncMockConsumer"/> create →
/// subscribe → assign → add → poll → close round-trip. It uses only APIs available on the
/// netstandard2.0 floor so it <b>compiles on net462</b> (via ns2.0) as well as net8.0 /
/// net10.0 — the net462 leg's <em>build</em> must pass locally even though its <em>run</em>
/// is Windows/CI-only; net10.0 runs locally. Nothing here uses <c>GC.GetTotalAllocatedBytes</c>
/// / <c>Task.IsCompletedSuccessfully</c> or other modern-only APIs.
/// </summary>
public sealed class PublicConsumerTfmSmokeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan s_pollTimeout = TimeSpan.FromMilliseconds(100);

    private const string Topic = "tfm-smoke-topic";
    private const int Partition = 0;

    [Fact]
    public async Task MockConsumer_AssignAddPollClose_RoundTrips()
    {
        // The record-carrying round-trip uses Assign (AddRecord requires an assigned
        // partition; subscribe + assign are mutually exclusive in Kafka). Covers
        // create → assign → seek → add → poll → close on the TFM matrix.
        AsyncMockConsumer consumer = new AsyncMockConsumer();
        try
        {
            consumer.Assign(new[] { new TopicPartition(Topic, Partition) });
            await TestTimeout.Run(() => consumer.Seek(new TopicPartition(Topic, Partition), 0L), s_deadline);
            consumer.AddRecord(Topic, Partition, offset: 5, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("v"));

            ConsumerRecords records = await Poll(consumer);

            ConsumerRecord record = Assert.Single(records);
            Assert.Equal(Topic, record.Topic);
            Assert.Equal(5, record.Offset);
            Assert.Equal(Encoding.UTF8.GetBytes("k"), record.Key);
            Assert.Equal(Encoding.UTF8.GetBytes("v"), record.Value);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.Close(), s_deadline);
        }
    }

    [Fact]
    public async Task MockConsumer_SubscribeUnsubscribeClose_RoundTrips()
    {
        // Covers the subscribe / unsubscribe wire on the TFM matrix (no records —
        // subscribe and assign are mutually exclusive, so records ride the assign leg).
        AsyncMockConsumer consumer = new AsyncMockConsumer();
        try
        {
            await TestTimeout.Run(() => consumer.Subscribe(new[] { Topic }), s_deadline);
            await TestTimeout.Run(() => consumer.Unsubscribe(), s_deadline);
        }
        finally
        {
            await TestTimeout.Run(() => consumer.Close(), s_deadline);
        }
    }

    private static async Task<ConsumerRecords> Poll(AsyncMockConsumer consumer)
    {
        ConsumerRecords result = default!;
        await TestTimeout.Run(async () => result = await consumer.Poll(s_pollTimeout), s_deadline);
        return result;
    }
}
