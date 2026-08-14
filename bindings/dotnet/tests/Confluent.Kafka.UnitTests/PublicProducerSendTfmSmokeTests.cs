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
/// The TFM-matrix smoke test for the M11/P3 producer send path (PLAN §7): the public
/// <see cref="AsyncMockProducer"/> / <see cref="IAsyncProducer"/> create → send → resolve
/// round-trip, plus the auto-complete and manual (<c>completeNext</c> / <c>errorNext</c>) mock
/// paths. Uses only APIs available on the netstandard2.0 floor so it <b>compiles on net462</b>
/// (via ns2.0) as well as net8.0 / net10.0 — the net462 leg's <em>build</em> must pass locally
/// even though its <em>run</em> is Windows/CI-only; net10.0 runs locally.
/// </summary>
public sealed class PublicProducerSendTfmSmokeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "tfm-send-topic";

    [Fact]
    public async Task MockProducer_SendAutoComplete_RoundTrips()
    {
        using AsyncMockProducer producer = new AsyncMockProducer();

        RecordMetadata metadata = default!;
        await TestTimeout.Run(
            async () => metadata = await producer.Send(
                new ProducerRecord(Topic, Encoding.UTF8.GetBytes("value"), Encoding.UTF8.GetBytes("key"), partition: 0)),
            s_deadline);

        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(0, metadata.Partition);
        Assert.Equal(0L, metadata.Offset);
    }

    [Fact]
    public async Task MockProducer_SendManualComplete_RoundTrips()
    {
        using AsyncMockProducer producer = new AsyncMockProducer(autoComplete: false);

        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord(Topic, Encoding.UTF8.GetBytes("value"), partition: 0));
        Assert.True(producer.CompleteNext());

        RecordMetadata metadata = default!;
        await TestTimeout.Run(async () => metadata = await sendTask, s_deadline);
        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(1, producer.HistoryCount);
    }

    [Fact]
    public async Task MockProducer_SendManualError_RoundTrips()
    {
        using AsyncMockProducer producer = new AsyncMockProducer(autoComplete: false);

        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord(Topic, Encoding.UTF8.GetBytes("value"), partition: 0));
        Assert.True(producer.ErrorNext(2, "tfm-error"));

        await Assert.ThrowsAsync<KafkaException>(async () => await sendTask);
    }

    [Fact]
    public async Task RealProducer_CreateDisposeAsync_RoundTrips()
    {
        // The real AsyncKafkaProducer (bootstrap.servers only, no broker): create → graceful async
        // close → destroy on the TFM matrix, with the send path present. Proves the send-path types
        // + the pump-integrated teardown compile-and-run on every target with ns2.0-safe APIs. (A
        // real send is not fired here — it would not deliver without a broker; the mock tests cover
        // the send round-trip and the pump-join teardown deterministically.)
        AsyncKafkaProducer producer = new AsyncKafkaProducer(
            new System.Collections.Generic.Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" });

        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
    }
}
