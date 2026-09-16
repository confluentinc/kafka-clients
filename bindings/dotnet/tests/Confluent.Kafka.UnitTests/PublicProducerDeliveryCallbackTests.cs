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
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Public-surface tests for the M14/P1 producer <b>delivery callback</b> —
/// <see cref="IDeliveryCallback"/> plus the <c>Send(record, callback)</c> overload on
/// <b>both</b> producer interfaces, restoring Java's second <c>send</c> signature
/// (<c>Producer.java:86</c>). Broker-free throughout, driven through the public
/// <see cref="MockProducer{TKey, TValue}"/> / <see cref="AsyncMockProducer{TKey, TValue}"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Every behavioural test runs against BOTH flavors.</b> The sync and async send paths are
/// separate code — the sync send blocks on the record's own future, the async one hands it to the
/// completion pump — so "fixed in one flavor only" is the natural bug here (PLAN §10 risk 6). The
/// <c>Flavor</c> harness at the bottom normalizes the two into one shape (the sync flavor's
/// blocking <c>Send</c> runs on a worker thread and its result is surfaced as a
/// <see cref="Task{TResult}"/>), so each test body is written once and driven twice.
/// </para>
/// <para>
/// <b>Two tests are deliberately NOT shared</b>, and both say why at the site: the ordering test
/// (its "is the result observable yet?" probe is necessarily different per flavor) and the
/// cancellation test (the sync surface takes no <see cref="CancellationToken"/> at all — decision
/// #4 of M11/P4).
/// </para>
/// <para>
/// <b>Honest mock reachability</b> (unchanged from the M11/P3 send tests): the mock's
/// <c>RecordMetadata</c> carries the sent topic / partition and a per-partition sequential offset,
/// but its <b>timestamp is never populated from the record</b> (always <c>-1</c>), so no test here
/// asserts a delivered success timestamp equals the sent one. One failure path is
/// inspection-verified rather than tested: a <see cref="KafkaException"/> raised by the
/// <em>synchronous</em> <c>Producer_send</c> out-param (which per decision D5 must fire <b>no</b>
/// callback) has no broker-free trigger — the managed closed-latch intercepts the only mock route
/// to it (a send after close) and surfaces <see cref="ObjectDisposedException"/> instead, which
/// case 1 of <c>NoCallback_OnSynchronousThrow</c> does cover.
/// </para>
/// <para>
/// <b>One firing site is NOT covered here, and cannot be</b> (M11/P3.1 §6.1): the accumulator's
/// per-record immediate-error compaction on the batch thread. Reaching it needs the core to reject
/// an individual record, and the public surface cannot produce that — the mock accepts every record
/// it is given, and the one broker-free way to make it reject (closing it) also latches the managed
/// closed flag, so the next <c>Send</c> throws <see cref="ObjectDisposedException"/> before
/// anything is appended. It is covered instead at the accumulator level, where the core can be
/// closed underneath a live accumulator, by
/// <c>Interop.SendAccumulatorTests.ImmediateError_FiresTheDeliveryCallbackExactlyOnce_AndFaultsThatSend</c>
/// and its compaction twin.
/// </para>
/// </remarks>
public sealed class PublicProducerDeliveryCallbackTests
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private const string Topic = "delivery-topic";

    private const string Sync = "sync";

    private const string Async = "async";

    // ---- 1. Success delivers the REAL metadata ----

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task Success_DeliversTheRealMetadata(string flavor)
    {
        using Flavor producer = Flavor.Create(flavor);
        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();

        RecordMetadata returned = await WithTimeout(producer.Send(
            new ProducerRecord<byte[], byte[]>(
                Topic, Encoding.UTF8.GetBytes("value"), Encoding.UTF8.GetBytes("key"), partition: 2),
            callback));

        (RecordMetadata Metadata, KafkaException? Exception) completion =
            Assert.Single(await SettledCompletions(callback, expected: 1));

        Assert.Null(completion.Exception);
        Assert.Equal(Topic, completion.Metadata.Topic);
        Assert.Equal(2, completion.Metadata.Partition);
        Assert.Equal(0L, completion.Metadata.Offset);

        // The callback is handed the SAME instance the caller gets back — Java's ordering means one
        // completion drives both, and it also proves the success path allocates no second
        // RecordMetadata (DoD §10).
        Assert.Same(returned, completion.Metadata);
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task Success_NonAsciiTopic_RoundTripsThroughTheDeliveredMetadata(string flavor)
    {
        // Guards a MarshalAs(LPStr) mistake, which corrupts non-ASCII silently and hides in
        // ASCII-only tests (ffi §A3). The delivered topic is the one copied out of the native
        // RecordMetadata handle before it was destroyed.
        const string NonAscii = "livraison-主题-ünîcödé-🚚";
        using Flavor producer = Flavor.Create(flavor);
        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();

        await WithTimeout(producer.Send(
            new ProducerRecord<byte[], byte[]>(NonAscii, Encoding.UTF8.GetBytes("v"), partition: 0),
            callback));

        (RecordMetadata Metadata, KafkaException? Exception) completion =
            Assert.Single(await SettledCompletions(callback, expected: 1));
        Assert.Equal(NonAscii, completion.Metadata.Topic);
    }

    // ---- 2. Exactly-once per record (DoD §3, CLAUDE.md §9.5) ----

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task ExactlyOnce_OneRecord_InvokesTheCallbackExactlyOnce(string flavor)
    {
        using Flavor producer = Flavor.Create(flavor);
        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();

        await WithTimeout(producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0),
            callback));

        // Asserted AFTER a settle window, not on the first observation: a double invocation would
        // otherwise pass because the first count of 1 is reached either way.
        Assert.Single(await SettledCompletions(callback, expected: 1));
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task ExactlyOnce_ManyRecords_InvokesTheCallbackOncePerRecord(string flavor)
    {
        // The interesting shape for the async flavor: N records fired without awaiting land in ONE
        // pump batch, so the per-index firing inside ProcessBatch's loop is what is under test — a
        // guard placed outside that loop, or a fire-once-per-batch bug, shows up here.
        const int RecordCount = 16;
        using Flavor producer = Flavor.Create(flavor);
        RecordingDeliveryCallback shared = new RecordingDeliveryCallback();
        RecordingDeliveryCallback[] perRecord = Enumerable.Range(0, RecordCount)
            .Select(_ => new RecordingDeliveryCallback())
            .ToArray();

        List<Task<RecordMetadata>> sends = new List<Task<RecordMetadata>>(RecordCount * 2);
        for (int i = 0; i < RecordCount; i++)
        {
            sends.Add(producer.Send(
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"shared-{i}"), partition: 0),
                shared));
            sends.Add(producer.Send(
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"each-{i}"), partition: 1),
                perRecord[i]));
        }

        await TestTimeout.Run(() => Task.WhenAll(sends), s_deadline);

        // One shared callback: N records → N invocations (never N+1, never 1).
        Assert.Equal(RecordCount, (await SettledCompletions(shared, expected: RecordCount)).Count);

        // One callback per record: each fires exactly once.
        foreach (RecordingDeliveryCallback callback in perRecord)
        {
            Assert.Single(await SettledCompletions(callback, expected: 1));
        }
    }

    // ---- 3. Ordering (D3): the callback runs BEFORE the result is observable ----

    [Fact]
    public async Task Ordering_Async_CallbackRunsBeforeTheTaskIsCompleted()
    {
        // Java sets the future's value, fires the callbacks, and only then releases the waiters
        // (ProducerBatch.java:303-323 — produceFuture.done() is last). The deterministic probe is
        // the send Task's own state as seen FROM INSIDE the callback: if the implementation fired
        // after TrySetResult, IsCompleted would be true there. Asserting "callback ticket <
        // continuation ticket" instead would be racy, because RunContinuationsAsynchronously only
        // SCHEDULES the continuation.
        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        TaskStateProbeCallback probe = new TaskStateProbeCallback();
        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), probe);
        probe.Task = sendTask;

        DriveUntilResolved(producer.CompleteNext);
        await WithTimeout(sendTask);

        Assert.True(probe.WasInvoked);
        // Closes the vacuous path: if the field had still been null the probe would have recorded
        // nothing and the assertion below would pass for the wrong reason.
        Assert.False(probe.TaskWasNull);
        Assert.False(probe.TaskWasCompleted);
    }

    [Fact]
    public async Task Ordering_Sync_CallbackRunsBeforeSendReturns()
    {
        // The sync flavor's equivalent probe: the callback must already have run at the instant the
        // blocking Send returns. A deferred / queued invocation would make this false or racy.
        using MockProducer<byte[], byte[]> producer =
            new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();
        bool invokedAtReturn = false;
        Task<RecordMetadata> sendTask = Task.Run(() =>
        {
            RecordMetadata metadata = producer.Send(
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), callback);
            invokedAtReturn = callback.Completions.Count == 1;
            return metadata;
        });

        DriveUntilResolved(producer.CompleteNext);
        await WithTimeout(sendTask);

        Assert.True(invokedAtReturn, "the delivery callback had not run when the blocking Send returned");
    }

    [Fact]
    public async Task Ordering_Sync_CallbackRunsBeforeSendThrows()
    {
        // The failure half of the same rule: on a delivery failure the callback fires and THEN Send
        // throws (Java has already fired on the I/O thread before future.get() unblocks).
        using MockProducer<byte[], byte[]> producer =
            new MockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);

        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();
        bool invokedAtThrow = false;
        Task sendTask = Task.Run(() =>
        {
            try
            {
                producer.Send(
                    new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), callback);
            }
            catch (KafkaException)
            {
                invokedAtThrow = callback.Completions.Count == 1;
            }
        });

        DriveUntilResolved(() => producer.ErrorNext(2, "ordering-throw"));
        await TestTimeout.Run(() => sendTask, s_deadline);

        Assert.True(invokedAtThrow, "the delivery callback had not run when the blocking Send threw");
    }

    // ---- 4. Error path (D5/D6): non-null placeholder metadata + the exception ----

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task Failure_DeliversPlaceholderMetadataAndTheException(string flavor)
    {
        using Flavor producer = Flavor.Create(flavor, autoComplete: false);
        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();

        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 3), callback);

        // Code 2 = CorruptMessage, which the core classifies retriable / non-fatal.
        DriveUntilResolved(() => producer.ErrorNext(2, "boom-delivery"));

        KafkaException thrown = await Assert.ThrowsAsync<KafkaException>(() => WithTimeout(sendTask));

        (RecordMetadata Metadata, KafkaException? Exception) completion =
            Assert.Single(await SettledCompletions(callback, expected: 1));

        // D2/D6: Java's user callback NEVER sees a null metadata — it gets the -1 placeholder
        // (Callback.java:28-33). The topic and the record's explicit partition survive; offset and
        // timestamp are -1.
        Assert.NotNull(completion.Metadata);
        Assert.Equal(Topic, completion.Metadata.Topic);
        Assert.Equal(3, completion.Metadata.Partition);
        Assert.Equal(-1L, completion.Metadata.Offset);
        Assert.Equal(-1L, completion.Metadata.Timestamp);

        // The delivered exception is the send's own outcome — same code, message and flags the
        // caller sees (DoD §3: assert the content, not just the type).
        Assert.NotNull(completion.Exception);
        Assert.Equal(2, completion.Exception!.Code);
        Assert.Contains("boom-delivery", completion.Exception.Message, StringComparison.Ordinal);
        Assert.True(completion.Exception.IsRetriable);
        Assert.Equal(thrown.Code, completion.Exception.Code);
        Assert.Equal(thrown.Message, completion.Exception.Message);
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task Failure_WithoutAnExplicitPartition_DeliversMinusOnePartition(string flavor)
    {
        // The recorded D6 deviation: Java's placeholder uses topicPartition(), which prefers the
        // RESOLVED partition; the core's error carries none, so a record that let the producer
        // choose gets -1 here.
        using Flavor producer = Flavor.Create(flavor, autoComplete: false);
        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();

        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v")), callback);

        DriveUntilResolved(() => producer.ErrorNext(2, "no-partition"));
        await Assert.ThrowsAsync<KafkaException>(() => WithTimeout(sendTask));

        (RecordMetadata Metadata, KafkaException? Exception) completion =
            Assert.Single(await SettledCompletions(callback, expected: 1));
        Assert.Equal(Topic, completion.Metadata.Topic);
        Assert.Equal(-1, completion.Metadata.Partition);
    }

    // ---- 5. NO callback on a synchronous throw (D5) — the discriminating test ----

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public void NoCallback_OnSynchronousThrow_AfterDispose(string flavor)
    {
        // Java's throwIfProducerClosed → IllegalStateException takes doSend's re-throwing branch
        // (KafkaProducer.java:1069-1081), which does NOT invoke the callback.
        Flavor producer = Flavor.Create(flavor);
        producer.Dispose();
        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();

        // Post-dispose stays type-only (the consumer norm asserts no ObjectName).
        Assert.Throws<ObjectDisposedException>(() => producer.SendInline(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), callback));

        Assert.Empty(callback.Completions);
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public void NoCallback_OnSynchronousThrow_SerializerThrows(string flavor)
    {
        // Java's SerializationException extends KafkaException (not ApiException), so it too hits
        // doSend's re-throwing catch — no callback. Here the serialize runs in the binding, above
        // the native send, so nothing was ever handed to the core.
        ThrowingSerializer<byte[]> throwing = new ThrowingSerializer<byte[]>();
        using Flavor producer = Flavor.Create(flavor, valueSerializer: throwing);
        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();

        SerializationException error = Assert.Throws<SerializationException>(() => producer.SendInline(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), callback));

        Assert.Contains("value", error.Message, StringComparison.Ordinal);
        Assert.Contains(Topic, error.Message, StringComparison.Ordinal);
        Assert.IsType<ThrowingSerializer<byte[]>.BoomException>(error.InnerException);
        Assert.Equal(1, throwing.InvocationCount);
        Assert.Empty(callback.Completions);
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public void NoCallback_OnSynchronousThrow_NullRecord(string flavor)
    {
        using Flavor producer = Flavor.Create(flavor);
        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();

        // ArgumentNullException(nameof(record)) sets no custom message → ParamName is the contract.
        ArgumentNullException error =
            Assert.Throws<ArgumentNullException>(() => producer.SendInline(null!, callback));

        Assert.Equal("record", error.ParamName);
        Assert.Empty(callback.Completions);
    }

    // ---- 6. A throwing callback is swallowed, traced, and does not disturb the send (D4) ----

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task ThrowingCallback_IsSwallowed_AndTheSendStillSucceeds(string flavor)
    {
        // Java logs and swallows (ProducerBatch.java:318-320); Python logs and swallows. The send
        // must be unaffected, and — the regression a catch placed OUTSIDE the pump's per-index loop
        // would fail — a SECOND send must still complete normally.
        using Flavor producer = Flavor.Create(flavor);
        ThrowingDeliveryCallback throwing = new ThrowingDeliveryCallback("boom-callback");

        RecordMetadata first = await WithTimeout(producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("first"), partition: 0), throwing));

        Assert.True(throwing.WasInvoked);
        Assert.Equal(Topic, first.Topic);
        Assert.Equal(0L, first.Offset);

        // The pump kept draining / the caller thread is healthy: a well-behaved callback on a later
        // send still fires, and that send still resolves.
        RecordingDeliveryCallback recording = new RecordingDeliveryCallback();
        RecordMetadata second = await WithTimeout(producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("second"), partition: 0), recording));

        Assert.Equal(1L, second.Offset);
        Assert.Single(await SettledCompletions(recording, expected: 1));
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task ThrowingCallback_DoesNotStrandTheOtherRecordsInTheBatch(string flavor)
    {
        // The specific failure a batch-scoped catch would produce: one user bug turning into N
        // failed sends. Every record in the batch carries a throwing callback, and every send must
        // still resolve.
        const int RecordCount = 8;
        using Flavor producer = Flavor.Create(flavor);
        ThrowingDeliveryCallback throwing = new ThrowingDeliveryCallback("boom-batch");

        List<Task<RecordMetadata>> sends = new List<Task<RecordMetadata>>(RecordCount);
        for (int i = 0; i < RecordCount; i++)
        {
            sends.Add(producer.Send(
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes($"v-{i}"), partition: 0), throwing));
        }

        RecordMetadata[] results = null!;
        await TestTimeout.Run(async () => results = await Task.WhenAll(sends), s_deadline);

        Assert.Equal(RecordCount, results.Length);
        Assert.All(results, metadata => Assert.Equal(Topic, metadata.Topic));
        Assert.Equal(RecordCount, throwing.InvocationCount);
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task ThrowingCallback_IsTraced_NotSilentlyDiscarded(string flavor)
    {
        // Swallow AND write to System.Diagnostics.Trace — swallowing silently would leave no trace
        // at all of a user callback that failed. Trace is the whole of the binding's diagnostics.
        // A per-test unique marker keeps this safe under the assembly's parallel execution.
        string marker = "delivery-trace-" + Guid.NewGuid().ToString("N");
        CapturingTraceListener listener = new CapturingTraceListener();
        Trace.Listeners.Add(listener);
        try
        {
            using Flavor producer = Flavor.Create(flavor);
            await WithTimeout(producer.Send(
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0),
                new ThrowingDeliveryCallback(marker)));
        }
        finally
        {
            Trace.Listeners.Remove(listener);
        }

        string line = await SettledTraceLine(listener, marker);

        // Attributed to the user's callback — the diagnostic must not blame the trampoline, and must
        // not name the offset-commit callback whose swallow-site shape this reuses.
        Assert.Contains("IDeliveryCallback", line, StringComparison.Ordinal);
        Assert.DoesNotContain("IOffsetCommitCallback", line, StringComparison.Ordinal);
    }

    // ---- 6b. Reentrancy from inside the callback (the canonical retry-on-failure use) ----

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public async Task Callback_MaySendAgainFromInsideItself(string flavor)
    {
        // The canonical thing users do in a delivery callback is retry the record, so a reentrant
        // Send must not deadlock. It does not, and for a different reason per flavor: on the async
        // surface the pump thread is not holding any managed lock when it fires the callback (the
        // enqueue gate's lock is not held across ProcessBatch), so the reentrant send is simply
        // queued and drained on the pump's next iteration; on the sync surface the callback runs
        // after the blocking get has already returned, so the core's producer mutex is free.
        // Asserted here rather than claimed in prose.
        using Flavor producer = Flavor.Create(flavor);
        ReentrantDeliveryCallback reentrant = new ReentrantDeliveryCallback(
            () => producer.Send(
                new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("retry"), partition: 1),
                new RecordingDeliveryCallback()));

        RecordMetadata first = await WithTimeout(producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("first"), partition: 0),
            reentrant));

        Assert.Equal(0, first.Partition);

        Task<RecordMetadata>? retry = null;
        Stopwatch elapsed = Stopwatch.StartNew();
        while (retry is null && elapsed.Elapsed < s_deadline)
        {
            retry = reentrant.Reentrant;
            await Task.Delay(2);
        }

        Assert.NotNull(retry);
        RecordMetadata second = await WithTimeout(retry!);
        Assert.Equal(1, second.Partition);
    }

    // ---- 7. Cancellation (D7): async only — the sync surface takes no CancellationToken ----

    [Fact]
    public async Task Cancellation_Async_CallbackStillFiresForACanceledTask()
    {
        // CLAUDE.md §9.5 makes exactly-once invocation an obligation per RECORD, not per awaiter,
        // and Python states it explicitly ("even if the returned Future was cancelled or already
        // resolved", producer.py:301-303). So the invocation must NOT be gated on TrySetResult's
        // bool return.
        using AsyncMockProducer<byte[], byte[]> producer =
            new AsyncMockProducer<byte[], byte[]>(Serdes.ByteArray, Serdes.ByteArray, autoComplete: false);
        using CancellationTokenSource cts = new CancellationTokenSource();
        RecordingDeliveryCallback callback = new RecordingDeliveryCallback();

        Task<RecordMetadata> sendTask = producer.Send(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0),
            callback,
            cts.Token);

        // Cancel the .NET wait first: the record is already with the core and cannot be aborted.
        cts.Cancel();
        TaskCanceledException canceled =
            await Assert.ThrowsAsync<TaskCanceledException>(() => WithTimeout(sendTask));
        Assert.Equal(cts.Token, canceled.CancellationToken);
        Assert.Empty(callback.Completions);

        // Now let the core complete the record. The awaiter is long gone, but the delivery
        // notification is still owed.
        DriveUntilResolved(producer.CompleteNext);

        (RecordMetadata Metadata, KafkaException? Exception) completion =
            Assert.Single(await SettledCompletions(callback, expected: 1));
        Assert.Null(completion.Exception);
        Assert.Equal(Topic, completion.Metadata.Topic);
    }

    // ---- 8. Null callback (D8) — deliberately stricter than Java ----

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public void NullCallback_ThrowsArgumentNullException(string flavor)
    {
        using Flavor producer = Flavor.Create(flavor);

        // ArgumentNullException(nameof(callback)) sets no custom message → ParamName is the whole
        // contract to pin (the M11/P2 precedent: type-only assertions were filed as a defect).
        ArgumentNullException error = Assert.Throws<ArgumentNullException>(() => producer.SendInline(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), callback: null));

        Assert.Equal("callback", error.ParamName);
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public void NullRecord_IsReportedBeforeANullCallback(string flavor)
    {
        // The documented precondition order — record, then callback, then the closed check — mirrors
        // Java, where doSend is reached through interceptors.onSend(record).
        using Flavor producer = Flavor.Create(flavor);

        ArgumentNullException error =
            Assert.Throws<ArgumentNullException>(() => producer.SendInline(null!, callback: null));
        Assert.Equal("record", error.ParamName);
    }

    [Theory]
    [InlineData(Sync)]
    [InlineData(Async)]
    public void NullCallback_IsReportedBeforeTheDisposedCheck(string flavor)
    {
        // Both argument guards precede the state guard, so a disposed producer plus a null callback
        // surfaces ArgumentNullException, not ObjectDisposedException.
        Flavor producer = Flavor.Create(flavor);
        producer.Dispose();

        ArgumentNullException error = Assert.Throws<ArgumentNullException>(() => producer.SendInline(
            new ProducerRecord<byte[], byte[]>(Topic, Encoding.UTF8.GetBytes("v"), partition: 0), callback: null));
        Assert.Equal("callback", error.ParamName);
    }

    // ---- 9. Shape: the overload is on every producer surface, and the callback is sync `void` ----

    [Theory]
    [InlineData(typeof(IProducer<byte[], byte[]>))]
    [InlineData(typeof(KafkaProducer<byte[], byte[]>))]
    [InlineData(typeof(MockProducer<byte[], byte[]>))]
    public void SyncSurfaces_DeclareTheCallbackOverload(Type surface)
    {
        // The real clients cannot be driven broker-free, so this pins the SHAPE across both
        // interfaces and all four implementations; behaviour is covered against the mocks above.
        MethodInfo? method = FindSend(
            surface, typeof(ProducerRecord<byte[], byte[]>), typeof(IDeliveryCallback));

        Assert.NotNull(method);
        Assert.Equal(typeof(RecordMetadata), method!.ReturnType);
    }

    [Theory]
    [InlineData(typeof(IAsyncProducer<byte[], byte[]>))]
    [InlineData(typeof(AsyncKafkaProducer<byte[], byte[]>))]
    [InlineData(typeof(AsyncMockProducer<byte[], byte[]>))]
    public void AsyncSurfaces_DeclareTheCallbackOverload(Type surface)
    {
        MethodInfo? method = FindSend(
            surface,
            typeof(ProducerRecord<byte[], byte[]>),
            typeof(IDeliveryCallback),
            typeof(CancellationToken));

        Assert.NotNull(method);
        Assert.Equal(typeof(Task<RecordMetadata>), method!.ReturnType);

        // The CancellationToken keeps its default, so `Send(record, callback)` compiles on the
        // async surface too.
        Assert.True(method.GetParameters()[2].HasDefaultValue);
    }

    [Fact]
    public void OnCompletion_ReturnsVoid_NotTask()
    {
        // The §4 delivery-callback divergence: the callback stays sync `void` like Java's, and the
        // callback-taking overload is kept IN ADDITION to the Task rather than replaced by it —
        // which is what §4's "takes a completion callback" row says for every other such callback.
        Assert.Equal(
            typeof(void),
            typeof(IDeliveryCallback).GetMethod(nameof(IDeliveryCallback.OnCompletion))!.ReturnType);

        ParameterInfo[] parameters =
            typeof(IDeliveryCallback).GetMethod(nameof(IDeliveryCallback.OnCompletion))!.GetParameters();
        Assert.Equal(typeof(RecordMetadata), parameters[0].ParameterType);
        Assert.Equal(typeof(KafkaException), parameters[1].ParameterType);
    }

    // ---- Helpers ----

    /// <summary>
    /// Finds a <c>Send</c> overload on <paramref name="surface"/>, searching inherited interfaces
    /// too (<see cref="Type.GetMethod(string, Type[])"/> on an interface does not walk its bases).
    /// </summary>
    private static MethodInfo? FindSend(Type surface, params Type[] parameters) =>
        surface.GetMethod("Send", parameters)
        ?? surface.GetInterfaces()
            .Select(i => i.GetMethod("Send", parameters))
            .FirstOrDefault(m => m is not null);

    /// <summary>
    /// Waits for <paramref name="callback"/> to reach <paramref name="expected"/> completions, then
    /// waits out a settle window and asserts the count has <b>not</b> grown — so a double
    /// invocation fails instead of passing on the first observation. The async flavor fires on the
    /// pump thread, so the count can lag the awaited send task by a moment.
    /// </summary>
    private static async Task<List<(RecordMetadata Metadata, KafkaException? Exception)>> SettledCompletions(
        RecordingDeliveryCallback callback, int expected)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (callback.Completions.Count < expected && elapsed.Elapsed < s_deadline)
        {
            await Task.Delay(2).ConfigureAwait(false);
        }

        Assert.Equal(expected, callback.Completions.Count);

        await Task.Delay(100).ConfigureAwait(false);
        Assert.Equal(expected, callback.Completions.Count);

        return callback.Completions;
    }

    /// <summary>
    /// Waits for the single trace line carrying <paramref name="marker"/> (the swallow is written
    /// from the pump thread on the async flavor, so it can lag the awaited send).
    /// </summary>
    private static async Task<string> SettledTraceLine(CapturingTraceListener listener, string marker)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (elapsed.Elapsed < s_deadline)
        {
            string? found = listener.Snapshot()
                .FirstOrDefault(line => line.Contains(marker, StringComparison.Ordinal));
            if (found is not null)
            {
                return found;
            }

            await Task.Delay(2).ConfigureAwait(false);
        }

        throw new TimeoutException("the swallowed delivery-callback failure was never traced");
    }

    /// <summary>
    /// Retries a mock <c>CompleteNext</c> / <c>ErrorNext</c> until it resolves a pending send. The
    /// sync flavor's blocking <c>Send</c> runs on a worker and registers its pending completion just
    /// before it parks, so an early call returns <see langword="false"/> until the send lands.
    /// </summary>
    private static void DriveUntilResolved(Func<bool> drive)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (!drive())
        {
            if (elapsed.Elapsed > s_deadline)
            {
                throw new TimeoutException(
                    "No pending send registered to drive within the deadline — treated as a hang.");
            }

            Thread.Sleep(2);
        }
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

    // ---- Fixtures ----

    private sealed class RecordingDeliveryCallback : IDeliveryCallback
    {
        private readonly List<(RecordMetadata Metadata, KafkaException? Exception)> _completions =
            new List<(RecordMetadata, KafkaException?)>();

        /// <summary>
        /// A stable snapshot of what the callback has been handed. The async flavor invokes it on
        /// the pump thread, so both the list and the reads are locked.
        /// </summary>
        internal List<(RecordMetadata Metadata, KafkaException? Exception)> Completions
        {
            get
            {
                lock (_completions)
                {
                    return new List<(RecordMetadata, KafkaException?)>(_completions);
                }
            }
        }

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            // Assert nothing here — a throw would be swallowed by design, so a failed assertion
            // inside the callback would vanish. Record, and let the test assert.
            lock (_completions)
            {
                _completions.Add((metadata, exception));
            }
        }
    }

    private sealed class ThrowingDeliveryCallback : IDeliveryCallback
    {
        private readonly string _message;

        private int _invocationCount;

        internal ThrowingDeliveryCallback(string message)
        {
            _message = message;
        }

        internal bool WasInvoked => Volatile.Read(ref _invocationCount) > 0;

        internal int InvocationCount => Volatile.Read(ref _invocationCount);

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            Interlocked.Increment(ref _invocationCount);
            throw new InvalidOperationException(_message);
        }
    }

    /// <summary>
    /// Sends again from inside the callback — the canonical retry-on-failure shape. Any throw is
    /// captured rather than allowed to escape (an escaping throw would be swallowed by design, so
    /// the test would see nothing).
    /// </summary>
    private sealed class ReentrantDeliveryCallback : IDeliveryCallback
    {
        private readonly Func<Task<RecordMetadata>> _send;

        internal ReentrantDeliveryCallback(Func<Task<RecordMetadata>> send)
        {
            _send = send;
        }

        internal Task<RecordMetadata>? Reentrant { get; private set; }

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception) =>
            Reentrant = _send();
    }

    /// <summary>
    /// Records the send <see cref="Task{TResult}"/>'s completion state as seen from inside the
    /// callback — the deterministic probe for the async ordering contract (D3).
    /// </summary>
    private sealed class TaskStateProbeCallback : IDeliveryCallback
    {
        internal Task<RecordMetadata>? Task { get; set; }

        internal bool WasInvoked { get; private set; }

        internal bool TaskWasNull { get; private set; }

        internal bool TaskWasCompleted { get; private set; }

        public void OnCompletion(RecordMetadata metadata, KafkaException? exception)
        {
            Task<RecordMetadata>? sendTask = Task;
            TaskWasNull = sendTask is null;
            TaskWasCompleted = sendTask is not null && sendTask.IsCompleted;
            WasInvoked = true;
        }
    }

    /// <summary>
    /// Captures whatever the binding writes to <see cref="Trace"/>. Registered only for the
    /// duration of one test and matched on a per-test unique marker, so assembly-wide parallel
    /// execution cannot make it observe another test's output.
    /// </summary>
    private sealed class CapturingTraceListener : TraceListener
    {
        private readonly StringBuilder _pending = new StringBuilder();

        private readonly List<string> _lines = new List<string>();

        internal List<string> Snapshot()
        {
            lock (_lines)
            {
                return new List<string>(_lines);
            }
        }

        public override void Write(string? message)
        {
            lock (_lines)
            {
                _pending.Append(message);
            }
        }

        public override void WriteLine(string? message)
        {
            lock (_lines)
            {
                _pending.Append(message);
                _lines.Add(_pending.ToString());
                _pending.Clear();
            }
        }
    }

    /// <summary>
    /// The two-flavor harness: one shape over the <b>sync</b> <see cref="IProducer{TKey, TValue}"/>
    /// and the <b>async</b> <see cref="IAsyncProducer{TKey, TValue}"/> mocks, so every behavioural
    /// test body is written once and run against both (PLAN §10 risk 6).
    /// </summary>
    /// <remarks>
    /// The sync flavor's <c>Send</c> <b>blocks</b>, so it is fired on a worker thread and its result
    /// surfaced as a <see cref="Task{TResult}"/>; awaiting that task therefore means "the blocking
    /// <c>Send</c> has returned", which is the sync analogue of "the awaiter observed the result".
    /// <see cref="SendInline"/> is the opposite: it calls <c>Send</c> on the <em>calling</em> thread
    /// so a synchronous precondition throw is directly observable by
    /// <see cref="Assert.Throws{T}(Action)"/>.
    /// </remarks>
    private abstract class Flavor : IDisposable
    {
        internal static Flavor Create(
            string flavor,
            bool autoComplete = true,
            ISerializer<byte[]>? valueSerializer = null)
        {
            ISerializer<byte[]> value = valueSerializer ?? Serdes.ByteArray;
            return flavor == Sync
                ? new SyncFlavor(autoComplete, value)
                : new AsyncFlavor(autoComplete, value);
        }

        internal abstract Task<RecordMetadata> Send(
            ProducerRecord<byte[], byte[]> record, IDeliveryCallback callback);

        internal abstract void SendInline(
            ProducerRecord<byte[], byte[]> record, IDeliveryCallback? callback);

        internal abstract bool CompleteNext();

        internal abstract bool ErrorNext(int code, string? message);

        public abstract void Dispose();

        private sealed class SyncFlavor : Flavor
        {
            private readonly MockProducer<byte[], byte[]> _producer;

            internal SyncFlavor(bool autoComplete, ISerializer<byte[]> valueSerializer)
            {
                _producer = new MockProducer<byte[], byte[]>(
                    Serdes.ByteArray, valueSerializer, autoComplete);
            }

            internal override Task<RecordMetadata> Send(
                ProducerRecord<byte[], byte[]> record, IDeliveryCallback callback) =>
                Task.Run(() => _producer.Send(record, callback));

            internal override void SendInline(
                ProducerRecord<byte[], byte[]> record, IDeliveryCallback? callback) =>
                _producer.Send(record, callback!);

            internal override bool CompleteNext() => _producer.CompleteNext();

            internal override bool ErrorNext(int code, string? message) =>
                _producer.ErrorNext(code, message);

            public override void Dispose() => _producer.Dispose();
        }

        private sealed class AsyncFlavor : Flavor
        {
            private readonly AsyncMockProducer<byte[], byte[]> _producer;

            internal AsyncFlavor(bool autoComplete, ISerializer<byte[]> valueSerializer)
            {
                _producer = new AsyncMockProducer<byte[], byte[]>(
                    Serdes.ByteArray, valueSerializer, autoComplete);
            }

            internal override Task<RecordMetadata> Send(
                ProducerRecord<byte[], byte[]> record, IDeliveryCallback callback) =>
                _producer.Send(record, callback);

            internal override void SendInline(
                ProducerRecord<byte[], byte[]> record, IDeliveryCallback? callback) =>
                _ = _producer.Send(record, callback!);

            internal override bool CompleteNext() => _producer.CompleteNext();

            internal override bool ErrorNext(int code, string? message) =>
                _producer.ErrorNext(code, message);

            public override void Dispose() => _producer.Dispose();
        }
    }
}
