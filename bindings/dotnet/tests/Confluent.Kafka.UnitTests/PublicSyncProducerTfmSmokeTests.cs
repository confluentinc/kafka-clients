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

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The TFM-matrix smoke test for the M11/P4 <b>sync</b> producer (PLAN §7): the public
/// <see cref="MockProducer"/> / <see cref="IProducer"/> create → <see cref="IProducer.Send"/> →
/// <see cref="IProducer.Close"/> round-trip, plus the manual (<c>completeNext</c> / <c>errorNext</c>)
/// mock path and a real <see cref="KafkaProducer"/> create → dispose. Uses only APIs available on the
/// netstandard2.0 floor so it <b>compiles on net462</b> (via ns2.0) as well as net8.0 / net10.0 — the
/// net462 leg's <em>build</em> must pass locally even though its <em>run</em> is Windows/CI-only;
/// net10.0 runs locally.
/// </summary>
public sealed class PublicSyncProducerTfmSmokeTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "sync-tfm-topic";

    [Fact]
    public void MockProducer_SendCloseRoundTrip_AutoComplete()
    {
        using MockProducer producer = new MockProducer();

        RecordMetadata metadata = null!;
        TestTimeout.Run(
            () => metadata = producer.Send(
                new ProducerRecord(Topic, Encoding.UTF8.GetBytes("value"), Encoding.UTF8.GetBytes("key"), partition: 0)),
            s_deadline);

        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(0, metadata.Partition);
        Assert.Equal(0L, metadata.Offset);

        TestTimeout.Run(producer.Close, s_deadline);
    }

    [Fact]
    public async Task MockProducer_SendManualComplete_RoundTrips()
    {
        using MockProducer producer = new MockProducer(autoComplete: false);

        Task<RecordMetadata> sendTask = Task.Run(
            () => producer.Send(new ProducerRecord(Topic, Encoding.UTF8.GetBytes("value"), partition: 0)));

        DriveUntilResolved(producer.CompleteNext);

        RecordMetadata metadata = null!;
        await TestTimeout.Run(async () => metadata = await sendTask, s_deadline);
        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(1, producer.HistoryCount());
    }

    [Fact]
    public async Task MockProducer_SendManualError_RoundTrips()
    {
        using MockProducer producer = new MockProducer(autoComplete: false);

        Task<RecordMetadata> sendTask = Task.Run(
            () => producer.Send(new ProducerRecord(Topic, Encoding.UTF8.GetBytes("value"), partition: 0)));

        DriveUntilResolved(() => producer.ErrorNext(2, "tfm-error"));

        await Assert.ThrowsAsync<KafkaException>(() => TestTimeout.Run(() => sendTask, s_deadline));
    }

    [Fact]
    public void RealProducer_CreateDispose_RoundTrips()
    {
        // The real KafkaProducer (bootstrap.servers only, no broker): create → graceful sync close →
        // destroy on the TFM matrix, with the send path present. Proves the sync producer types load
        // and tear down on every target with ns2.0-safe APIs. (A real send is not fired here — it
        // would not deliver without a broker; the mock tests cover the send round-trip.)
        KafkaProducer producer = new KafkaProducer(
            new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" });

        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    /// <summary>
    /// Retries <paramref name="drive"/> from the test thread until it resolves the worker thread's
    /// pending, blocking <see cref="IProducer.Send"/> (returns <see langword="true"/>), or the
    /// deadline elapses (a fail-fast hang guard).
    /// </summary>
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
