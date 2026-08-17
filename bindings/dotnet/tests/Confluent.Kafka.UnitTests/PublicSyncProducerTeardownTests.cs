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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// The M11/P4 <b>sync</b> producer teardown (decision #6, ffi §A2): <see cref="IProducer.Close"/>
/// (graceful sync <c>Producer_close</c>, <b>surfaces</b> a close error) and
/// <see cref="IDisposable.Dispose"/> (<b>swallows</b> it), both idempotent under a single one-shot
/// latch. Because a producer used only through the sync surface never starts the send pump (only the
/// async <see cref="AsyncKafkaProducer.Send"/> does), teardown degenerates to the <b>pump-less</b>
/// path — <c>StopPump</c> finds no pump and returns immediately (no flush-before-join, no thread to
/// join). The regression these tests guard is that teardown <b>returns without hanging</b> and stays
/// idempotent / crash-free.
/// </summary>
/// <remarks>
/// <b>Close-error-still-destroys is a documented reachability limit (matches the async teardown
/// test).</b> The "a close error still proceeds to destroy" path (the <c>finally</c> in
/// <c>NativeProducer.CloseSync</c>) cannot be triggered broker-free: the mock's <c>Producer_close</c>
/// always succeeds, and a real producer against no broker does not fault a close quickly. The
/// mechanism (destroy in a <c>finally</c>, independent of the close outcome, mirroring
/// <c>NativeConsumer.CloseSync</c> and the async producer teardown) is verified by inspection — a
/// documented limit, not a silent gap.
/// </remarks>
public sealed class PublicSyncProducerTeardownTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "sync-teardown-topic";

    // ---- Close / Dispose round-trip (pump-less, returns without hanging) ----

    [Fact]
    public void Close_OnMock_Succeeds()
    {
        MockProducer producer = new MockProducer();

        // Graceful sync close → destroy, returns without hanging (pump-less teardown).
        TestTimeout.Run(producer.Close, s_deadline);
    }

    [Fact]
    public void Dispose_WhenIdle_Returns()
    {
        MockProducer producer = new MockProducer();

        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    [Fact]
    public void Close_AfterAutoCompleteSend_Returns()
    {
        MockProducer producer = new MockProducer();

        // A sync Send does NOT start the pump (it blocks on its own get), so close is still the
        // pump-less path even after a send.
        RecordMetadata metadata = null!;
        TestTimeout.Run(
            () => metadata = producer.Send(new ProducerRecord(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)),
            s_deadline);
        Assert.Equal(0L, metadata.Offset);

        TestTimeout.Run(producer.Close, s_deadline);
    }

    // ---- Idempotence / mixed teardown (one-shot latch) ----

    [Fact]
    public void Dispose_CalledTwice_IsSafe()
    {
        MockProducer producer = new MockProducer();

        TestTimeout.Run(producer.Dispose, s_deadline);
        // The second call is a no-op (one-shot latch) — no double close / double destroy / throw.
        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    [Fact]
    public void Close_ThenDispose_IsSafe()
    {
        MockProducer producer = new MockProducer();

        // Close wins the latch (close+destroy); the following Dispose loses it and no-ops — no
        // double destroy.
        TestTimeout.Run(producer.Close, s_deadline);
        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    [Fact]
    public async Task ConcurrentDispose_IsSafe()
    {
        MockProducer producer = new MockProducer();

        // Many threads race Dispose: exactly one wins the latch; the rest no-op. No double destroy,
        // no crash, all return.
        Task[] disposers = Enumerable
            .Range(0, 8)
            .Select(_ => Task.Run(() => producer.Dispose()))
            .ToArray();

        await TestTimeout.Run(() => Task.WhenAll(disposers), s_deadline);
    }

    // ---- Use-after-teardown guard ----

    [Fact]
    public void Ops_AfterClose_ThrowObjectDisposed()
    {
        MockProducer producer = new MockProducer();
        producer.Close();

        // Every op throws ObjectDisposedException post-teardown (type-only, the consumer norm).
        Assert.Throws<ObjectDisposedException>(
            () => producer.Send(new ProducerRecord(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)));
        Assert.Throws<ObjectDisposedException>(() => producer.Flush());
        Assert.Throws<ObjectDisposedException>(() => producer.PartitionsFor(Topic));

        producer.Dispose();
    }

    // ---- Real producer (config path) teardown round-trips broker-free ----

    [Fact]
    public void RealProducer_Dispose_Returns()
    {
        // The real KafkaProducer (bootstrap.servers only, no broker) tears down without a broker:
        // graceful sync close → destroy, returns without hanging. Proves the sync teardown on the
        // real client, not just the mock.
        KafkaProducer producer = new KafkaProducer(
            new Dictionary<string, string> { ["bootstrap.servers"] = "localhost:9092" });

        TestTimeout.Run(producer.Dispose, s_deadline);
    }
}
