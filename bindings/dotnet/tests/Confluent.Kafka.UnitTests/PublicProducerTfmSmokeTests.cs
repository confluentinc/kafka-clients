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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The TFM-matrix smoke test for the M11/P2 producer peripherals (PLAN §3): the public
/// <see cref="AsyncMockProducer"/> / <see cref="IAsyncProducer"/> create → flush → partitions-for
/// → close round-trip. It uses only APIs available on the netstandard2.0 floor so it
/// <b>compiles on net462</b> (via ns2.0) as well as net8.0 / net10.0 — the net462 leg's
/// <em>build</em> must pass locally even though its <em>run</em> is Windows/CI-only; net10.0 runs
/// locally. Nothing here uses modern-only APIs.
/// </summary>
public sealed class PublicProducerTfmSmokeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "tfm-producer-topic";

    [Fact]
    public async Task MockProducer_FlushPartitionsForClose_RoundTrips()
    {
        // Drive the two completion-bridge shapes (void flush; owned-handle partitions_for) plus
        // the graceful async Close upgrade through the public interface on the TFM matrix.
        IAsyncProducer producer = new AsyncMockProducer();
        try
        {
            await TestTimeout.Run(() => producer.Flush(), s_deadline);

            IReadOnlyList<PartitionInfo> partitions = default!;
            await TestTimeout.Run(async () => partitions = await producer.PartitionsFor(Topic), s_deadline);

            // Empty on the mock (empty cluster) — a success with an empty result (PLAN §2).
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
        // The blocking Dispose upgrade (sync Producer_close → destroy) on the TFM matrix, driven
        // through IDisposable. Bounded by TestTimeout (the blocking close/destroy).
        AsyncMockProducer producer = new AsyncMockProducer();

        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    [Fact]
    public async Task RealProducer_CreateDisposeAsync_RoundTrips()
    {
        // The real AsyncKafkaProducer (bootstrap.servers only, no broker) create → graceful async
        // close → destroy on the TFM matrix. Proves the config-path construct + the Dispose
        // upgrade compile-and-run on every target with ns2.0-safe APIs.
        AsyncKafkaProducer producer = new AsyncKafkaProducer(
            new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" });

        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
    }
}
