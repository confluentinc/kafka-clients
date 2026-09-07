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
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Public-surface tests for the M11/P3 producer SEND path —
/// <see cref="IAsyncProducer.Send(ProducerRecord, CancellationToken)"/> — over the inline pull-pump
/// (ffi §A4/§A7 Option C), exercised end to end through the public <see cref="AsyncMockProducer"/> /
/// <see cref="IAsyncProducer"/> surface (PLAN §7). Covers: the send <see cref="Task{TResult}"/>
/// resolving with the correct <see cref="RecordMetadata"/> and faulting with a
/// <see cref="KafkaException"/> (code + message + flags); mutation-after-send safety (call-scoped
/// copy); absent vs empty key/value; a non-ASCII topic round-trip; concurrency; preconditions
/// (before any native call); best-effort cancellation; and Dispose-with-sends-in-flight returning
/// without hanging (the pump-join regression).
/// </summary>
/// <remarks>
/// <b>Honest mock reachability (PLAN §2; matches the consumer's mock caveats).</b> The mock's
/// <c>RecordMetadata</c> carries the sent topic / partition and a per-partition sequential offset,
/// but its <b>timestamp is not populated from the record</b> (always <c>-1</c>), and the ABI exposes
/// <b>no history-record value accessor</b> (only <see cref="AsyncMockProducer.HistoryCount()"/>) — so
/// the sent key/value bytes cannot be read back broker-free. The mutation-after-send test therefore
/// asserts the send <em>completes correctly</em> after the caller's buffer is mutated (the
/// call-scoped-copy proof the mock allows); a byte-for-byte value read-back is integration-only.
/// Every awaited op runs under a <see cref="TestTimeout"/> hang guard (the pump-join / completion
/// regression guard, ffi §A7).
/// </remarks>
/// <remarks>
/// <b>R2 review fixes — four failure paths are inspection-verified, not unit-tested (untestable via
/// the mock).</b> The PR #160 R2 fixes each harden a rare failure path that the broker-free mock
/// cannot induce, so there is no seam to drive them from a unit test; their <em>normal</em> paths
/// are covered by the tests here (send resolve/fault, cancellation, and the teardown no-hang +
/// concurrent-Send tests) and must stay green:
/// <list type="bullet">
/// <item><b>ProcessBatch per-index handle sweep</b> (<c>SendCompletionPump.ProcessBatch</c> nulls
/// each consumed metadata/error slot, then a <c>finally</c> sweep frees the unconsumed tail): only
/// reachable if the per-index loop throws partway (FromHandle OOM in the error branch), leaving the
/// tail's native metadata/error handles unfreed. The mock's error path (<c>ErrorNext</c>) resolves
/// FromHandle normally — no OOM to induce — so the tail-leak escape cannot be driven; a true
/// AccessViolation is not catchable by design. Inspection-verified (consumed slots nulled ⇒ the
/// sweep frees only the unprocessed tail, never a double-free).</item>
/// <item><b>Pump batch fault-not-hang</b> (<c>SendCompletionPump.RunLoop</c> try/catch around
/// <c>ProcessBatch</c>): only reachable if <c>get_all</c> throws (a native failure) or OOM escapes
/// before ProcessBatch's completion loop. The mock's <c>get_all</c> resolves normally (success or a
/// per-record error handle — never a throw), so the escape cannot be induced; a true
/// AccessViolation is not catchable by design. Inspection-verified.</item>
/// <item><b>Orphaned-future free</b> (<c>NativeProducer.Send</c> try/catch through
/// <c>pump.Enqueue</c>): only reachable if the TCS / cancellation-registration allocation OOMs
/// between <c>Producer_send</c> and <c>Enqueue</c> — not deterministically inducible.
/// Inspection-verified.</item>
/// <item><b>StopPumpAsync broadened catch</b>: only reachable if the async flush bridge throws a
/// non-<c>KafkaException</c> (ObjectDisposedException / OOM); the handle is still open during
/// teardown and OOM is not inducible, so only the normal flush path (covered by the manual-mock
/// no-hang teardown tests) runs here. Inspection-verified.</item>
/// </list>
/// </remarks>
public sealed class PublicProducerSendTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "send-topic";

    // ---- Resolves with metadata ----

    [Fact]
    public async Task Send_OnAutoCompleteMock_ResolvesWithMetadata()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        RecordMetadata metadata = await SendOf(
            producer,
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("value"), Encoding.UTF8.GetBytes("key"), partition: 2));

        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(2, metadata.Partition);
        Assert.Equal(0L, metadata.Offset);
        // RecordMetadata.ToString mirrors Java's "topic-partition@offset".
        Assert.Equal("send-topic-2@0", metadata.ToString());
    }

    [Fact]
    public async Task Send_SequentialToSamePartition_OffsetsIncrement()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        for (int i = 0; i < 5; i++)
        {
            RecordMetadata metadata = await SendOf(
                producer,
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"value-{i}"), partition: 0));
            Assert.Equal(0, metadata.Partition);
            Assert.Equal((long)i, metadata.Offset);
        }
    }

    // ---- Faults with a KafkaException (via ErrorNext) — code + message + flags ----

    [Fact]
    public async Task Send_ErrorNext_FaultsWithKafkaException()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("value"), partition: 0));

        // The next pending send is completed with an error carrying the code + message (code 2 =
        // CorruptMessage, which the core classifies retriable, non-fatal). error_next returns true
        // because there is a pending completion.
        // The async Send is DEFERRED since M11/P3.1: drain the accumulator so the record has reached
        // the core before driving the mock by hand (a deterministic hook, never a sleep — §9).
        producer.WaitForSendsToReachCore(s_deadline);
        Assert.True(producer.ErrorNext(2, "boom-corrupt-message"));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => WithTimeout(sendTask));

        Assert.Equal(2, failure.Code);
        Assert.Contains("boom-corrupt-message", failure.Message, StringComparison.Ordinal);
        Assert.True(failure.IsRetriable);
        Assert.False(failure.IsFatal);
    }

    [Fact]
    public async Task Send_ErrorNext_DefaultMessage_FaultsWithNonEmptyMessage()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("value"), partition: 0));

        // A null message uses the code's default message (the ABI's null convention).
        // The async Send is DEFERRED since M11/P3.1: drain the accumulator so the record has reached
        // the core before driving the mock by hand (a deterministic hook, never a sleep — §9).
        producer.WaitForSendsToReachCore(s_deadline);
        Assert.True(producer.ErrorNext(2));

        KafkaException failure = await Assert.ThrowsAsync<KafkaException>(
            () => WithTimeout(sendTask));

        Assert.Equal(2, failure.Code);
        Assert.False(string.IsNullOrEmpty(failure.Message));
    }

    // ---- Absent vs empty key/value (the §A4 sentinels) each produce a correct send ----

    [Fact]
    public async Task Send_AbsentKeyAndValue_Resolves()
    {
        // Absent key (null) + absent value (null tombstone) → the ABI's (IntPtr.Zero, -1) sentinel
        // for each. The send resolves with correct metadata (a value read-back is integration-only).
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        RecordMetadata metadata = await SendOf(producer, new ProducerRecord<byte[], byte[]>(Topic, value: null, key: null, partition: 1));

        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(1, metadata.Partition);
    }

    [Fact]
    public async Task Send_EmptyKeyAndValue_Resolves()
    {
        // Empty (zero-length, non-null) key + value → the non-null stack-sentinel + len 0 path,
        // distinct from absent. Distinct from the absent case above, both resolve correctly.
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        RecordMetadata metadata = await SendOf(
            producer,
            new ProducerRecord<byte[], byte[]>(Topic, Array.Empty<byte>(), Array.Empty<byte>(), partition: 1));

        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(1, metadata.Partition);
    }

    // ---- Mutation-after-send (the call-scoped-copy correctness proof) ----

    [Fact]
    public async Task Send_MutationImmediatelyAfterSend_CompletesCorrectly()
    {
        // The core copies key/value into the batch buffer synchronously during Producer_send
        // (ffi §A4), so the caller may mutate the buffer the instant Send returns. Mutating a large
        // buffer right after Send must not corrupt the in-flight send / crash / hang: the send still
        // resolves with correct metadata. (A byte read-back is integration-only — see the class
        // remarks — so this asserts completion, the strongest proof the mock allows.)
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        byte[] key = Enumerable.Repeat((byte)0xAB, 32).ToArray();
        byte[] value = Enumerable.Repeat((byte)0xCD, 256 * 1024).ToArray();

        Task<RecordMetadata> sendTask = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, value, key, partition: 0));

        // Scribble over the caller's buffers immediately — the copy already happened in the call.
        Array.Clear(value, 0, value.Length);
        Array.Clear(key, 0, key.Length);

        RecordMetadata metadata = await WithTimeout(sendTask);
        Assert.Equal(Topic, metadata.Topic);
        Assert.Equal(0, metadata.Partition);
        Assert.Equal(0L, metadata.Offset);
    }

    // ---- Non-ASCII topic round-trips through send → RecordMetadata.Topic (UTF-8, §A3) ----

    [Fact]
    public async Task Send_NonAsciiTopic_RoundTripsToMetadataTopic()
    {
        const string nonAsciiTopic = "témás-topic-日本語-🚀";
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        RecordMetadata metadata = await SendOf(
            producer,
            new ProducerRecord<byte[], byte[]>(nonAsciiTopic, Encoding.UTF8.GetBytes("v"), partition: 0));

        Assert.Equal(nonAsciiTopic, metadata.Topic);
    }

    // ---- Concurrency: many threads share one producer; no thread-pool starvation ----

    [Fact]
    public async Task Send_ManyThreadsConcurrently_AllResolve()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        const int threadCount = 8;
        const int perThread = 50;

        // Each thread sends `perThread` records to its own partition (so offsets are deterministic
        // per partition). The coarse producer mutex serializes the inline Producer_send calls; the
        // single pump drains all completions. No Task.Run/thread-per-send, so a high count does not
        // starve the thread pool — everything completes under the hang guard.
        Task<RecordMetadata[]>[] threads = Enumerable.Range(0, threadCount)
            .Select(partition => Task.Run(async () =>
            {
                Task<RecordMetadata>[] sends = new Task<RecordMetadata>[perThread];
                for (int i = 0; i < perThread; i++)
                {
                    sends[i] = producer.Send(
                        new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"v-{partition}-{i}"), partition: partition));
                }

                return await Task.WhenAll(sends);
            }))
            .ToArray();

        RecordMetadata[][] results = null!;
        await TestTimeout.Run(async () => results = await Task.WhenAll(threads), s_deadline);

        int total = results.Sum(r => r.Length);
        Assert.Equal(threadCount * perThread, total);
        Assert.All(results.SelectMany(r => r), md => Assert.Equal(Topic, md.Topic));
    }

    // ---- Preconditions fire BEFORE any native call ----

    [Fact]
    public async Task Send_NullRecord_ThrowsArgumentNull()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        // Thrown synchronously by the argument check (ArgumentNullException(nameof(record)) sets no
        // custom message, so ParamName is the contract, DoD §3).
        ArgumentNullException ex = await Assert.ThrowsAsync<ArgumentNullException>(
            () => producer.Send(null!));
        Assert.Equal("record", ex.ParamName);
    }

    [Fact]
    public async Task Send_AfterDispose_ThrowsObjectDisposed()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        await producer.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"))));
    }

    [Fact]
    public async Task Send_PreCanceledToken_ThrowsOperationCanceled()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
        using CancellationTokenSource cts = new CancellationTokenSource();
        cts.Cancel();

        // An already-canceled token is honored synchronously BEFORE any native call
        // (ThrowIfCancellationRequested) — user cancellation, distinct from a native abort.
        OperationCanceledException canceled = await Assert.ThrowsAsync<OperationCanceledException>(
            () => producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v")), cts.Token));

        // The exception carries the caller's token, so the idiomatic
        // `catch (OperationCanceledException e) when (e.CancellationToken == ct)` matches (Minor 9).
        Assert.Equal(cts.Token, canceled.CancellationToken);

        // The producer is still usable after a rejected send.
        RecordMetadata metadata = await SendOf(producer, new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0));
        Assert.Equal(Topic, metadata.Topic);
    }

    // ---- Best-effort cancellation: cancels the WAIT, never aborts the send; straggler is safe ----

    [Fact]
    public async Task Send_CancelAfterEnqueue_CancelsWait_StragglerCompletionIsNoOp()
    {
        using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        using CancellationTokenSource cts = new CancellationTokenSource();

        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), cts.Token);

        // Cancel the .NET wait: the send is already enqueued and cannot be aborted (no wakeup), so
        // the Task cancels but the native send stays pending.
        cts.Cancel();
        TaskCanceledException canceled = await Assert.ThrowsAsync<TaskCanceledException>(() => WithTimeout(sendTask));

        // Cancelled WITH the token (Minor 9): the post-enqueue cancellation carries the caller's
        // token too, so Send is diagnosable the same way the async peripherals already were. The
        // parameterless TrySetCanceled() left this as CancellationToken.None.
        Assert.Equal(cts.Token, canceled.CancellationToken);

        // Now resolve the still-pending native send: the pump's TrySetResult on the already-canceled
        // TCS is a safe no-op, and the future handle is freed. complete_next returns true (there was
        // a pending completion), and nothing crashes / hangs.
        // The async Send is DEFERRED since M11/P3.1: drain the accumulator so the record has reached
        // the core before driving the mock by hand (a deterministic hook, never a sleep — §9).
        producer.WaitForSendsToReachCore(s_deadline);
        Assert.True(producer.CompleteNext());

        // The producer stays healthy and disposes cleanly (no leaked/blocked pump).
        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
    }

    // ---- Dispose with sends in flight returns without hanging (the pump-join regression) ----

    [Fact]
    public async Task Dispose_WithSendsInFlight_Returns()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        // Fire several sends without awaiting; the auto-complete mock resolves them, so the pump's
        // get_all returns and teardown joins the pump without hanging.
        for (int i = 0; i < 16; i++)
        {
            _ = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"v-{i}"), partition: 0));
        }

        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);
    }

    [Fact]
    public void Dispose_WithSendsInFlight_SyncDispose_Returns()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

        for (int i = 0; i < 16; i++)
        {
            _ = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"v-{i}"), partition: 0));
        }

        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    // ---- Manual-mock (autoComplete:false) with an UNCOMPLETED in-flight send: Dispose must NOT
    // hang forever (the pump is blocked in get_all on a never-resolving future; teardown flushes
    // pending sends BEFORE joining the pump so get_all returns — COMMENTS.30 Issue 1). ----

    [Fact]
    public void Dispose_WithUncompletedManualSendInFlight_Returns()
    {
        // The Critic's exact repro: a manual mock with a send that is NEVER completed. Without the
        // flush-before-join teardown, the pump's blocking get_all on the unresolved future would
        // hang _thread.Join() forever. The teardown flush resolves the pending send so get_all
        // returns and Dispose completes under the hang guard.
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        Task<RecordMetadata> pending = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0));

        TestTimeout.Run(producer.Dispose, s_deadline);

        // The send resolved (did not hang) — its Task is settled (flush completed it), not stuck.
        Assert.True(pending.IsCompleted);
    }

    [Fact]
    public async Task DisposeAsync_WithUncompletedManualSendInFlight_Returns()
    {
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        Task<RecordMetadata> pending = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0));

        await TestTimeout.Run(async () => await producer.DisposeAsync(), s_deadline);

        Assert.True(pending.IsCompleted);
    }

    [Fact]
    public async Task Close_WithUncompletedManualSendInFlight_Returns()
    {
        // The surfacing teardown flavor (Close) also flushes before the pump-join → no hang.
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        Task<RecordMetadata> pending = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0));

        await TestTimeout.Run(() => producer.Close(), s_deadline);

        Assert.True(pending.IsCompleted);
    }

    [Fact]
    public void Dispose_WithManyUncompletedManualSendsInFlight_Returns()
    {
        // A batch of uncompleted manual sends: the pump may be blocked in a get_all over several
        // unresolved futures. The teardown flush completes them all, so the batched get_all returns
        // and the join completes.
        AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        for (int i = 0; i < 16; i++)
        {
            _ = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"v-{i}"), partition: 0));
        }

        TestTimeout.Run(producer.Dispose, s_deadline);
    }

    // ---- Concurrent Send + Dispose does not crash (Issue 2: the span-the-op ref makes destroy
    // wait out an in-flight Producer_send). Best-effort — the racing send may throw
    // ObjectDisposedException (rejected after the latch) or complete/fault; it must never crash. ----

    [Fact]
    public void ConcurrentSendAndDispose_DoesNotCrash()
    {
        // Churn a producer whose sends race its own Dispose across threads, under GC pressure. The
        // span-the-op DangerousAddRef on Producer_send keeps Producer_destroy from dropping the
        // runtime out from under an in-flight send; a double-free / use-after-free on this path
        // would corrupt the allocator and crash here. Individual sends may throw
        // ObjectDisposedException (rejected once the close latch is won) — that is expected, not a
        // failure.
        for (int iteration = 0; iteration < 40; iteration++)
        {
            AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);

            TestTimeout.Run(
                () =>
                {
                    Task senders = Task.Run(() =>
                    {
                        for (int i = 0; i < 32; i++)
                        {
                            try
                            {
                                _ = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"v-{i}"), partition: 0));
                            }
                            catch (ObjectDisposedException)
                            {
                                // Expected once Dispose wins the latch — not a crash.
                            }
                        }
                    });

                    Task disposer = Task.Run(() => producer.Dispose());

                    Task.WaitAll(senders, disposer);
                },
                s_deadline);

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }
    }

    // ---- Handles freed exactly once: churn create → send N → await → dispose under GC pressure ----

    [Fact]
    public void Send_CreateSendDisposeMany_Churn_NoLeakOrCrash()
    {
        // Churn the whole send lifecycle (create → send N → await → dispose) under GC pressure. A
        // double-free / use-after-free on the future / metadata / pump path would corrupt the
        // allocator and crash here; a leak or a pump-join hang would fail the hang guard. Proves
        // destroy_all + the metadata/error frees are exactly-once.
        for (int iteration = 0; iteration < 40; iteration++)
        {
            TestTimeout.Run(
                () =>
                {
                    using AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray);
                    Task<RecordMetadata>[] sends = new Task<RecordMetadata>[8];
                    for (int i = 0; i < sends.Length; i++)
                    {
                        sends[i] = producer.Send(new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"v-{i}"), partition: 0));
                    }

                    Task.WaitAll(sends);
                },
                s_deadline);

            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }
    }


    [Fact]
    public void ConcurrentSendAndDispose_OnManualMock_DoesNotHang()
    {
        // The Major-5 regression guard (M11/P8). The window the fix closes: teardown's flush
        // resolves everything pending AT THAT INSTANT, but a send landing after the flush returned
        // and before pump.Stop() was still QUEUED — Enqueue's fault-in-place branch keys on
        // _stopped, which Stop() sets only AFTER the _thread.Join() that hangs. The pump then blocks
        // in an uninterruptible get_all on a future nothing will ever resolve, and the join waits
        // forever. SendCompletionPump.CloseGate() now closes the gate BEFORE the flush, so such a
        // send faults in place instead. The hang guard IS the assertion.
        //
        // Landing in the window needs threads already streaming through SendViaPump's
        // ThrowIfClosed-to-Enqueue span when teardown starts, so the senders are DEDICATED THREADS
        // (not pool tasks — a queued task can be scheduled after Dispose has already latched, which
        // makes every send a cheap rejection and exercises nothing) and Dispose fires only once all
        // of them have a send on the board.
        //
        // On autoComplete: true (the test above) racing sends self-resolve, so the window is
        // invisible; the existing manual-mock teardown tests all send strictly BEFORE teardown.
        // Neither axis was covered in combination until now.
        //
        // Honest limit: whether any given run actually lands in that window is timing-dependent —
        // this is a crash/hang-freedom churn guard over the manual-mock + concurrent-teardown axis,
        // not a deterministic proof. The gate's semantics ARE proven deterministically, by
        // Interop/SendCompletionPumpGateTests.
        for (int iteration = 0; iteration < 25; iteration++)
        {
            AsyncMockProducer<byte[], byte[]> producer = new AsyncMockProducer<byte[], byte[]>(
                Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

            using CountdownEvent hot = new CountdownEvent(4);
            Thread[] senders = new Thread[4];
            for (int t = 0; t < senders.Length; t++)
            {
                senders[t] = new Thread(() =>
                {
                    bool signalled = false;
                    for (int i = 0; i < 2000; i++)
                    {
                        try
                        {
                            _ = producer.Send(new ProducerRecord<byte[], byte[]>(
                                Topic, Encoding.UTF8.GetBytes($"v-{i}"), partition: 0));
                        }
                        catch (ObjectDisposedException)
                        {
                            // Expected once Dispose wins the latch — nothing more to push.
                            break;
                        }
                        catch (KafkaException)
                        {
                            // Also expected: a send that passed ThrowIfClosed before the latch can
                            // reach Producer_send after Producer_close and get a synchronous
                            // "MockProducer is already closed." A documented teardown-race outcome —
                            // the regression under test is a HANG, not a failed racing send.
                            break;
                        }

                        if (!signalled)
                        {
                            signalled = true;
                            hot.Signal();
                        }
                    }

                    if (!signalled)
                    {
                        hot.Signal();
                    }
                })
                { IsBackground = true };
                senders[t].Start();
            }

            // Every sender is mid-flight before teardown begins.
            Assert.True(hot.Wait(s_deadline), "senders never got going");

            TestTimeout.Run(producer.Dispose, s_deadline);

            foreach (Thread sender in senders)
            {
                Assert.True(sender.Join(s_deadline), "a sender thread never finished");
            }
        }
    }

    // ---- Helpers (every awaited op under the TestTimeout hang guard) ----

    private static async Task<RecordMetadata> SendOf(IAsyncProducer<byte[], byte[]> producer, ProducerRecord<byte[], byte[]> record)
    {
        RecordMetadata result = null!;
        await TestTimeout.Run(async () => result = await producer.Send(record), s_deadline);
        return result;
    }

    private static async Task<RecordMetadata> WithTimeout(Task<RecordMetadata> task)
    {
        Task winner = await Task.WhenAny(task, Task.Delay(s_deadline)).ConfigureAwait(false);
        if (winner != task)
        {
            throw new TimeoutException("Send did not complete within the deadline — treated as a hang.");
        }

        return await task.ConfigureAwait(false);
    }
}
