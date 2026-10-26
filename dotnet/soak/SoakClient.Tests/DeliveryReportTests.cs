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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Confluent.Kafka.Soak.Tests;

/// <summary>
/// The delivery report of a send through <see cref="AsyncMockProducer{TKey, TValue}"/>: a completed
/// send future counts as delivered, a failed one as a delivery failure tagged with its error code.
/// </summary>
[Collection(EnvironmentCollection.Name)]
public sealed class DeliveryReportTests
{
    private const string Prefix = "p.";

    private static SoakClient NewClient(AsyncMockProducer<byte[], SoakRecord> producer, CapturingSink sink) =>
        new SoakClient(
            SoakTestSupport.Options(("SOAK_TESTID", "soak1"), ("SOAK_TOPIC", "t")),
            new SoakLogger(SoakLogLevel.Fatal),
            new SoakMetrics(new StringWriter(), new Dictionary<string, string>(StringComparer.Ordinal), Prefix, sink),
            new ProcessResourceSampler(),
            new CancellationTokenSource(),
            producer,
            new AsyncMockConsumer<byte[], SoakRecord>(Serdes.ByteArray, new SoakRecordDeserializer()),
            "h",
            0,
            0);

    private static async Task AwaitTheDeliveryReport(SoakClient client) =>
        Assert.Equal(0L, await SoakClient.DrainCounterAsync(() => client.ProducerCounts.PendingDeliveries, TimeSpan.FromSeconds(10)));

    [Fact]
    public async Task DeliveryReportOfACompletedSendFuture()
    {
        var sink = new CapturingSink();
        using SoakClient client = NewClient(
            new AsyncMockProducer<byte[], SoakRecord>(Serdes.ByteArray, new SoakRecordSerializer(50)),
            sink);

        await client.ProduceRecordAsync();
        await AwaitTheDeliveryReport(client);

        Assert.Equal((1L, 0L, 0L, 0L), client.ProducerCounts);
        Assert.Contains((Prefix + "producer.drok", 1L, string.Empty), sink.Counters);
        Assert.DoesNotContain(sink.Counters, counter => counter.Name == Prefix + "producer.drerr");
    }

    [Fact]
    public async Task DeliveryReportOfAFailedSendFutureCountsTheErrorCode()
    {
        var sink = new CapturingSink();
        var producer = new AsyncMockProducer<byte[], SoakRecord>(
            Serdes.ByteArray, new SoakRecordSerializer(50), autoComplete: false);
        using SoakClient client = NewClient(producer, sink);

        await client.ProduceRecordAsync();
        Assert.Equal((0L, 0L, 1L, 1L), client.ProducerCounts);

        Assert.True(producer.ErrorNext(SoakTestSupport.KafkaError(16, "x")));
        await AwaitTheDeliveryReport(client);

        Assert.Equal((0L, 1L, 0L, 0L), client.ProducerCounts);
        Assert.Contains((Prefix + "producer.drerr", 1L, string.Empty), sink.Counters);
        Assert.Contains((Prefix + "producer.delivery.failure", 1L, "err=16"), sink.Counters);
        Assert.DoesNotContain(sink.Counters, counter => counter.Name == Prefix + "producer.drok");
    }
}
