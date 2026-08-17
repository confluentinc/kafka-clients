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
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Public-surface tests for the M11/P4 <b>sync</b> producer SEND path —
/// <see cref="IProducer.Send(ProducerRecord)"/>, which <b>blocks</b> and returns the
/// <see cref="RecordMetadata"/> directly (= Java <c>send(record).get()</c>, decision #1) — exercised
/// through the public <see cref="MockProducer"/> / <see cref="IProducer"/> surface (PLAN §7). Covers:
/// the auto-complete send returning the correct metadata; the manual (<c>autoComplete: false</c>)
/// send driven to success / failure <b>from another thread</b> (the single-owner "another thread
/// completes" pattern — sync <see cref="Send"/> blocks, so a helper thread resolves it); a non-ASCII
/// topic round-trip (UTF-8 guard, ffi §A3); and preconditions (before any native call).
/// </summary>
/// <remarks>
/// <b>Manual-mock completion is cross-thread by necessity (PLAN §7 note).</b> Because sync
/// <see cref="IProducer.Send"/> blocks until the send resolves, the manual-mock tests fire
/// <see cref="IProducer.Send"/> on a worker thread (via <see cref="Task.Run(Action)"/>) and drive
/// completion (<see cref="MockProducer.CompleteNext"/> / <see cref="MockProducer.ErrorNext"/>) from
/// the test thread, retrying until the pending send registers (the send's
/// <c>Producer_send</c> enqueues the pending completion just before it blocks on the get, so an
/// early <c>CompleteNext</c> returns <see langword="false"/> until it lands). Every blocking wait
/// runs under a <see cref="TestTimeout"/> hang guard so a lost wakeup / deadlock fails the run fast.
/// The mock's timestamp is not populated from the record (integration-only), so these assert
/// topic / partition / offset only.
/// </remarks>
public sealed class PublicSyncProducerSendTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "sync-send-topic";

    // ---- Auto-complete: Send blocks briefly and returns the metadata directly ----

    [Fact]
    public void Send_OnAutoCompleteMock_ReturnsMetadata()
    {
        using MockProducer producer = new MockProducer();

        RecordMetadata metadata = null!;
        TestTimeout.Run(
            () => metadata = producer.Send(
                new ProducerRecord(Topic, Encoding.UTF8.GetBytes("value"), Encoding.UTF8.GetBytes("key"), partition: 2)),
            s_deadline);

        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(2, metadata.Partition);
        Assert.Equal(0L, metadata.Offset);
        // RecordMetadata.ToString mirrors Java's "topic-partition@offset".
        Assert.Equal("sync-send-topic-2@0", metadata.ToString());
    }

    [Fact]
    public void Send_SequentialToSamePartition_OffsetsIncrement()
    {
        using MockProducer producer = new MockProducer();

        for (int i = 0; i < 5; i++)
        {
            int captured = i;
            RecordMetadata metadata = null!;
            TestTimeout.Run(
                () => metadata = producer.Send(
                    new ProducerRecord(Topic, Encoding.UTF8.GetBytes($"value-{captured}"), partition: 0)),
                s_deadline);

            Assert.Equal(0, metadata.Partition);
            Assert.Equal((long)captured, metadata.Offset);
        }
    }

    // ---- Manual mock: Send blocks until another thread resolves it ----

    [Fact]
    public async Task Send_ManualComplete_UnblocksAndReturnsMetadata()
    {
        using MockProducer producer = new MockProducer(autoComplete: false);

        // Send BLOCKS on a worker thread until CompleteNext resolves it from THIS thread.
        Task<RecordMetadata> sendTask = Task.Run(
            () => producer.Send(new ProducerRecord(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)));

        DriveUntilResolved(producer.CompleteNext);

        RecordMetadata metadata = null!;
        await TestTimeout.Run(async () => metadata = await sendTask, s_deadline);

        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(0, metadata.Partition);
        Assert.Equal(0L, metadata.Offset);
    }

    [Fact]
    public async Task Send_ManualError_FaultsWithKafkaException()
    {
        using MockProducer producer = new MockProducer(autoComplete: false);

        Task<RecordMetadata> sendTask = Task.Run(
            () => producer.Send(new ProducerRecord(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)));

        // Drive the pending send to a failure from this thread (code + custom message).
        DriveUntilResolved(() => producer.ErrorNext(2, "sync-mock-error"));

        // The blocking Send faults with a flat KafkaException carrying the code AND message (DoD §3,
        // ffi §A5 — assert content, not just the type).
        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => TestTimeout.Run(() => sendTask, s_deadline));
        Assert.Equal(2, failure.Code);
        Assert.Contains("sync-mock-error", failure.Message, StringComparison.Ordinal);
    }

    // ---- UTF-8 round-trip (topic in → out) ----

    [Fact]
    public void Send_NonAsciiTopic_RoundTrips()
    {
        // A non-ASCII topic guards the manual UTF-8 marshalling both ways (pinned in via
        // Utf8Marshal.Pin, copied out of RecordMetadata via Utf8Marshal.PtrToString) — an LPStr
        // mistake would corrupt this silently (ffi §A3).
        const string topic = "sync-主题-ünîcödé";
        using MockProducer producer = new MockProducer();

        RecordMetadata metadata = null!;
        TestTimeout.Run(
            () => metadata = producer.Send(
                new ProducerRecord(topic, Encoding.UTF8.GetBytes("v"), partition: 0)),
            s_deadline);

        Assert.Equal(topic, metadata.Topic);
    }

    // ---- Preconditions (before any native call, ffi §A5) ----

    [Fact]
    public void Send_NullRecord_ThrowsArgumentNull()
    {
        using MockProducer producer = new MockProducer();

        // Default message (ArgumentNullException(nameof(record))) → assert ParamName only.
        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => producer.Send(null!));
        Assert.Equal("record", ex.ParamName);
    }

    [Fact]
    public void Send_NullRecord_ThrownBeforeDisposedCheck_EvenWhenClosed()
    {
        // The null-record guard in SendSync precedes ThrowIfClosed, so a disposed producer + null
        // record surfaces ArgumentNullException, NOT ObjectDisposedException (verified in the source).
        MockProducer producer = new MockProducer();
        producer.Dispose();

        ArgumentNullException ex = Assert.Throws<ArgumentNullException>(() => producer.Send(null!));
        Assert.Equal("record", ex.ParamName);
    }

    [Fact]
    public void Send_AfterDispose_ThrowsObjectDisposed()
    {
        MockProducer producer = new MockProducer();
        producer.Dispose();

        // Post-dispose stays type-only (the consumer norm asserts no ObjectName).
        Assert.Throws<ObjectDisposedException>(
            () => producer.Send(new ProducerRecord(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)));
    }

    [Fact]
    public void Send_AfterClose_ThrowsObjectDisposed()
    {
        MockProducer producer = new MockProducer();
        producer.Close();

        Assert.Throws<ObjectDisposedException>(
            () => producer.Send(new ProducerRecord(Topic, Encoding.UTF8.GetBytes("v"), partition: 0)));

        producer.Dispose();
    }

    /// <summary>
    /// Retries <paramref name="drive"/> (a mock <c>CompleteNext</c> / <c>ErrorNext</c>) from the test
    /// thread until it resolves a pending send (returns <see langword="true"/>), or the deadline
    /// elapses. The worker thread's blocking <see cref="IProducer.Send"/> registers the pending
    /// completion just before it parks on the get, so an early call returns <see langword="false"/>
    /// until the send lands — hence the spin (a fail-fast hang guard, not a busy-wait forever).
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
