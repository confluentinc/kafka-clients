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
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The async producer's binding-side send accumulator and its batch thread — the .NET realization of
/// the Python binding's <c>Producer_send_thread</c> (M11/P3.1, anchor
/// <c>bindings/python/_confluentkafka.c:523-655</c>). <c>Send</c> pins the record's buffers, appends
/// them to a node chain and returns immediately; this thread waits for a threshold or a free-running
/// window, takes the whole chain, and drives one <c>kafka_producer_Producer_send_batch</c> per node,
/// then unpins and hands the resulting futures to the existing <see cref="SendCompletionPump"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Two background threads, not three</b> (§1.2): this batch thread and the completion pump. The
/// caller's thread is not counted. The completion side is unchanged — this type ends at
/// <see cref="SendCompletionPump.Enqueue"/>.
/// </para>
/// <para>
/// <b>The node mirrors the anchor's parallel arrays</b> (<c>BatchNode</c>,
/// <c>_confluentkafka.c:360-369</c>), each sized <see cref="SendAccumulatorSettings.SlotCapacity"/>,
/// with a new node allocated only when the current one fills — exactly the anchor's rule
/// (<c>:806</c>). Two arrays have no Python counterpart and both are forced by .NET: the
/// <c>MemoryHandle</c> / topic pins the deferred send has to keep alive (Python keeps a refcount on
/// the record object and an owned topic copy instead), and the blittable
/// <see cref="ProducerRecordNative"/> array the P/Invoke reads (Python builds the equivalent flat
/// array on its stack, <c>:585</c>).
/// </para>
/// <para>
/// <b>Pin ownership transfers on a successful <see cref="Append"/>.</b> Before it, the caller owns
/// the pins and releases them if the append throws; after it, this type owns them and releases every
/// one exactly once — in a <c>finally</c>, after <c>send_batch</c> returns and before the futures
/// reach the pump (§4.4). Never in the pump, never across the returned <see cref="Task"/>.
/// </para>
/// <para>
/// <b>Submission order is call order</b> (M11/P3.2 §F1; M11/P3.4 now gets it the anchor's own way).
/// The anchor appends <em>unconditionally</em> under its mutex and computes "full" only afterwards
/// (<c>_confluentkafka.c:819-830</c>), so its append order is its call order by construction.
/// <see cref="SubmitAdmitted"/> now does exactly that: it appends under <c>_gate</c> <b>first</b>
/// and only then waits on the admission bound, so a record's position in the chain is fixed by the
/// call that placed it and no send can overtake one that is still waiting. The FIFO submission
/// queue and single-submitter loop that M11/P3.2 needed to reproduce the property without
/// append-first are gone with it.
/// </para>
/// <para>
/// <b>Admission is BOUNDED, and exceeding it blocks the calling thread</b> (M11/P3.3,
/// <see cref="SendAccumulatorSettings.MaxAdmittedRecords"/> / <see cref="SubmitAdmitted"/>). Before
/// the bound the client accepted an unbounded number of sends (measured: 2.05 M records in flight,
/// p50 3,524 ms, RSS 2.04 GiB, against 41 ms / 239 MB immediately before). The bound is
/// <b>synchronous</b>, because the caller never awaits admission — it is handed the record's
/// delivery <see cref="Task"/> and moves on (deviation DV-1), so an asynchronous admission wait
/// bounds a container and throttles nobody.
/// </para>
/// <para>
/// <b>Teardown is minimal-but-correct in this slice.</b> <see cref="Stop"/> closes the accumulator
/// to new appends, wakes the thread, and joins it — the thread performs one final drain before
/// exiting, so no accepted record is abandoned. It is called <b>before</b>
/// <see cref="SendCompletionPump.CloseGate"/> so the final drain's futures arrive at an <em>open</em>
/// gate: reversing those two would route normal teardown through the pump's fault-in-place branch,
/// which is recorded residual 1 and fires no delivery callback (§3.8). The full handshake — a
/// cancellable backpressure gate and <b>bounded</b> waits with a defined outcome on expiry — is
/// slice S5; the join here is unbounded.
/// </para>
/// </remarks>
internal sealed class SendAccumulator
{
    private readonly SafeProducerHandle _handle;
    private readonly PinnedTopicCache _topics;
    private readonly SendCompletionPump _pump;
    private readonly SendAccumulatorSettings _settings;

    // Guards the node chain, the accumulated counter, the closed/force/draining flags, and doubles
    // as the batch thread's condition variable (Monitor.Wait/PulseAll) — the direct analogue of the
    // anchor's record_batches_mutex + record_batches_new_record_cnd pair.
    private readonly object _gate = new object();

    private static readonly Task s_alreadyDrained = Task.FromResult(true);

    private readonly Thread _thread;
    private readonly long _windowTicks;

    // THE ADMISSION BOUND (M11/P3.3, append-first since M11/P3.4): one permit per record that has
    // reached a node and has not yet been taken by the batch thread. Under append-first the take is
    // the ONLY release site on the normal path (TakeChainLocked -> ReleaseAdmission), and
    // SubmitAdmitted is the only take site, so the accounting is single-sited in both directions.
    //
    // Saturating it makes the CALLING THREAD block. That is the whole point and it is Java's shape
    // (KafkaProducer.send() blocks once buffer.memory is full); an asynchronous admission wait does
    // not throttle anyone, because the caller does not await admission — it is handed the record's
    // delivery Task and moves on (deviation DV-1). Measured twice: M11/P6's
    // `await _inflight.WaitAsync` gave 63.5k msg/s / 3.0 GB / p50 10,001 ms, and M11/P3.2's
    // unbounded queue gave 583k / 2.04 GiB / p50 3,524 ms, against 537k / 239 MB / 41 ms
    // immediately before it. The synchronous wait is what throttles (M11/P7: 591.6k / 127 MiB /
    // p50 7 ms).
    //
    // ⚠ THE CEILING IS int.MaxValue, NOT MaxAdmittedRecords, AND THAT IS FORCED BY APPEND-FIRST.
    // The permit for a record is released when the batch thread TAKES it, which under append-first
    // can happen BEFORE the caller that appended it has taken its own permit — so the count can
    // legitimately sit above the starting value and a MaxAdmittedRecords ceiling throws
    // SemaphoreFullException on the first drain (observed: it kills the batch thread, and
    // AbandonOnThreadFailure then closes the producer). The STARTING count is still
    // MaxAdmittedRecords and is what makes the gate block.
    //
    // ⚠ THE BOUND THE CEILING NO LONGER POLICES, STATED SO IT CAN BE TESTED INSTEAD. Let P be the
    // records appended-but-not-taken (_chainRecords) and W the callers currently between their
    // append and the return of their Wait. Every append increments _chainRecords exactly once and
    // is released exactly once by the take that zeroes it, and every SubmitAdmitted that appended
    // attempts exactly one Wait, so
    //
    //     available = MaxAdmittedRecords + (records taken) - (callers past their Wait)
    //               = MaxAdmittedRecords - P + W  >= 0   =>   P <= MaxAdmittedRecords + W
    //
    // i.e. the accepted-but-unforwarded population is bounded by the cap plus the number of
    // concurrently parked caller threads. That is the anchor's own shape (py_Producer_send appends
    // unconditionally, so C concurrent senders reach bound + C - 1) and is asserted by
    // Admission_IsBounded_WhenTheFloodOutrunsTheDrain. The ONLY drift is at teardown: a cancelled
    // gate makes Wait throw without taking (SemaphoreSlim checks the token before the permit), so
    // each caller released that way leaves one permit behind — harmless, because the accumulator is
    // closed and nothing can be admitted afterwards.
    //
    // ⚠ STARVATION — recorded, accepted, NOT claimed as parity. Java's BufferPool is fair by
    // contract: "It is fair. That is all memory is given to the longest waiting thread until it has
    // sufficient memory" (BufferPool.java:40), a Deque<Condition> with addLast / peekFirst().signal().
    // SemaphoreSlim documents no waiter ordering and can barge, so under sustained saturation a
    // caller can wait longer than one that arrived after it. Since M11/P3.4 the wait has no timeout,
    // so barging can no longer turn into a spurious refusal — the starved caller is delayed, never
    // failed. ORDERING does not depend on any of this: a record's position is fixed by its append,
    // which happens before the wait.
    private readonly SemaphoreSlim _admission;

    // Cancelled by Stop() so a Send parked on the admission bound is released instead of pinning
    // teardown behind it (§3.8 step 2, which must precede closing the accumulator), and —
    // unconditionally — by AbandonOnThreadFailure, so the batch thread dying releases those waiters
    // too rather than leaving them to permit arithmetic the failure may already have corrupted
    // (M11/P3.2 slice S5). Never disposed: a CancellationTokenSource with no timer holds no
    // unmanaged resource, and disposing one while a linked registration is being torn down is its
    // own hazard — and a disposed CTS would make `.Token` throw ObjectDisposedException at the wait
    // site instead of the OperationCanceledException that site is written to swallow.
    //
    // ⚠ IT IS THE ONLY THING THAT CAN END THE ADMISSION WAIT EARLY (M11/P3.4). The wait takes no
    // timeout and NOT the caller's token, because by then the record is already appended and may
    // already have been sent: nothing may make SubmitAdmitted throw and strand an awaiter the
    // caller has not been handed yet. Both Cancel() sites therefore double as the liveness
    // guarantee for a parked caller, and the record it appended is settled by the chain machinery
    // either way (the final drain, or SettleAbandonedChain).
    private readonly CancellationTokenSource _spaceGate = new CancellationTokenSource();

    // The chain the batch thread TOOK from the accumulator and has not finished sending yet.
    // Written and read ONLY by the batch thread (RunLoopCore publishes it, SendChain advances it,
    // AbandonOnThreadFailure settles whatever is left), so it needs no lock.
    //
    // It exists because the local variable holding the taken chain used to be the ONLY reference to
    // it: a throw between the take and the end of the send — ReleaseAdmission over-releasing, or
    // ReleasePins / FaultNode failing inside SendNode — reached RunLoop's catch, which settles the
    // ACCUMULATOR's chain (_head) and could not see the in-flight one. Every record in it was then
    // stranded forever: its awaiter never completed, its pins never released, its future handles
    // never destroyed — and Stop still reported the thread as exited, so the interned topic buffers
    // those un-released pins point at were freed.
    private Node? _inFlight;

    private Node? _head;      // oldest node, the one the wait loop measures (anchor: next_batches_to_send)
    private Node? _tail;      // the node being filled (anchor: last_accumulating_batch)
    private Node? _spare;     // one fully-settled node kept for reuse (see RecycleNode)

    // Records appended but not yet taken by the batch thread — the quantity _admission bounds, and
    // the count TakeChainLocked hands to ReleaseAdmission. Incremented UNCONDITIONALLY by Append
    // (one per record that reaches a node) and zeroed only by TakeChainLocked, both under _gate;
    // read lock-free (Volatile) by AdmittedRecordCount.
    private int _chainRecords;
    private bool _closed;     // no further appends; the thread drains once more and exits
    private bool _forceDrain; // a DrainPending caller is waiting; skip the rest of the window
    private bool _draining;   // a taken chain is being sent right now (outside the lock)
    // Awaiters of DrainPendingAsync, released when the accumulator next reaches empty-and-idle.
    // Guarded by _gate.
    private readonly List<TaskCompletionSource<bool>> _idleWaiters = new List<TaskCompletionSource<bool>>();

    private long _sendBatchCalls;
    private long _sendBatchRecords;
    private int _largestSendBatch;

    internal SendAccumulator(
        SafeProducerHandle handle,
        PinnedTopicCache topics,
        SendCompletionPump pump,
        SendAccumulatorSettings settings)
    {
        _handle = handle;
        _topics = topics;
        _pump = pump;
        _settings = settings;
        _windowTicks = (long)(Stopwatch.Frequency * (settings.BatchWindowMs / 1000.0));

        // Starting count = the bound; ceiling = int.MaxValue, which append-first forces — see the
        // field's own comment for the arithmetic and for the bound that replaces the ceiling.
        _admission = new SemaphoreSlim(settings.MaxAdmittedRecords, int.MaxValue);

        _thread = new Thread(RunLoop)
        {
            IsBackground = true,
            Name = "confluent-kafka-producer-send-batch",
        };
        _thread.Start();
    }

    /// <summary>The settings this accumulator was constructed with (read once, §3.2).</summary>
    internal SendAccumulatorSettings Settings => _settings;

    /// <summary>
    /// The number of <c>send_batch</c> calls issued so far. Exposed so the chunking rule (§3.4) can
    /// be asserted by <b>call count</b> rather than by "the records arrived", which any batching
    /// would satisfy.
    /// </summary>
    internal long SendBatchCallCount => Interlocked.Read(ref _sendBatchCalls);

    /// <summary>
    /// The record count of the largest <c>send_batch</c> call issued so far. This is the direct
    /// witness for "a chunk never spans two nodes": it can never exceed
    /// <see cref="SendAccumulatorSettings.BatchChunk"/>, however deep the drained chain was.
    /// </summary>
    internal int LargestSendBatchCount => Volatile.Read(ref _largestSendBatch);

    /// <summary>The total records passed to <c>send_batch</c> so far (the calls' sizes summed).</summary>
    internal long SendBatchRecordCount => Interlocked.Read(ref _sendBatchRecords);

    /// <summary>
    /// <b>The admitted population</b> (M11/P3.3): records that have reached a node and which the
    /// batch thread has not taken yet. The quantity
    /// <see cref="SendAccumulatorSettings.MaxAdmittedRecords"/> bounds.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Derived from production's own bookkeeping, not from the gate.</b> <c>_chainRecords</c> is
    /// maintained by <see cref="Append"/> and does not read <see cref="_admission"/>, so this is a
    /// genuine witness rather than a restatement of the semaphore: remove the admission wait and it
    /// keeps growing.
    /// </para>
    /// <para>
    /// ⚠ <b>Under append-first it counts a record from the moment it is appended</b>, which is
    /// <em>before</em> its caller has taken an admission permit — so a sampled peak includes the
    /// callers currently parked, and the quantity it is bounded by is
    /// <c>MaxAdmittedRecords + (parked callers)</c> rather than <c>MaxAdmittedRecords</c> (the
    /// arithmetic is at <see cref="_admission"/>). At rest — every caller returned, nothing drained
    /// — it equals the number of accepted-but-unforwarded records exactly.
    /// <see cref="AvailableAdmissions"/> is the gate's own count and moves the other way; the two
    /// witnesses are complementary rather than interchangeable.
    /// </para>
    /// </remarks>
    internal int AdmittedRecordCount => Volatile.Read(ref _chainRecords);

    /// <summary>
    /// How many admission permits are still available — the <b>leak witness</b> for
    /// <see cref="ReleaseAdmission"/>, and the second half of the bound assertion.
    /// </summary>
    /// <remarks>
    /// <b>Test observation point, not production surface</b> (the <c>SendBatchCallCount</c>
    /// precedent). It is <em>not</em> a restatement of <see cref="AdmittedRecordCount"/>: that one
    /// counts records the accumulator is holding, this one counts permits, and net drift in the
    /// release accounting moves only the second — silently, since the
    /// <see cref="int.MaxValue"/> ceiling no longer throws on an over-release.
    /// </remarks>
    internal int AvailableAdmissions => _admission.CurrentCount;

    /// <summary>
    /// <b>The production send path's single entry point:</b> <b>appends the record first</b> — under
    /// <see cref="_gate"/>, so its position in the chain is fixed by this call — and only then takes
    /// an admission permit, <b>blocking the calling thread</b> while the bound is saturated. On a
    /// normal return the record is in the chain; on a throw nothing has been appended, pinned, or
    /// charged.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Append-first is the anchor's mechanism, and it is what makes submission order call order</b>
    /// (M11/P3.4). <c>py_Producer_send</c> writes the slot unconditionally under its mutex
    /// (<c>_confluentkafka.c:819-823</c>) and computes <c>full</c> only afterwards (<c>:830</c>), so
    /// no send can overtake one that is waiting — the waiting one has already taken its place. That
    /// replaces M11/P3.2's routing count + FIFO submission queue + single submitter, which existed
    /// only to reproduce the property while the permit was taken <em>before</em> the append. Per-caller
    /// ordering is all Java promises (<c>ProducerConfig.java:274</c>) and it now holds by construction:
    /// a caller's next <c>Send</c> cannot start until this one has returned, and this one appended
    /// before it parked.
    /// </para>
    /// <para>
    /// <b>The wait has NO TIMEOUT and does NOT honour <paramref name="cancellationToken"/>, deliberately.</b>
    /// By the time it runs the record is already appended and may already have been sent, so nothing
    /// may make this method throw and strand a <paramref name="completion"/> the caller has not been
    /// handed yet. <paramref name="cancellationToken"/> is unchanged in every other respect — it still
    /// marks the returned <see cref="Task"/> cancelled through <c>SendViaPump</c>'s own registration —
    /// it simply has no say over this wait. The only thing that can end it early is
    /// <see cref="_spaceGate"/> (teardown, or the batch thread dying), and even that does not throw:
    /// the record is in the chain and is settled by the chain machinery either way.
    /// </para>
    /// <para>
    /// <b>It stays synchronous, and that is the throttle.</b> An <c>async</c> admission wait cannot
    /// throttle: the caller is handed the record's delivery <see cref="Task"/> and does not await
    /// admission (deviation DV-1), so a parked continuation is just another unbounded container —
    /// measured as such, twice (see <see cref="_admission"/>). A blocking
    /// <see cref="SemaphoreSlim.Wait(CancellationToken)"/> is a genuine synchronous primitive, not an
    /// asynchronous operation being blocked on, so it is <em>not</em> the sync-over-async footgun
    /// ffi §B7 / CLAUDE.md §4 forbid — the same distinction CLAUDE.md §4 draws for the consumer's sync
    /// <c>Seek</c>. It is also what lets <c>SendViaPump</c> stay non-<c>async</c>, which its own
    /// comment requires.
    /// </para>
    /// <para>
    /// <b>Pins are taken with the append, and are owned by the accumulator before the wait begins.</b>
    /// M11/P3.1 §4.4's invariant — <em>a blocked sender must not hold pins</em> — is satisfied a
    /// different way than it was under permit-first: the parked caller holds none, because
    /// <see cref="Submit"/> transferred every one of them to the node it appended to, and that node's
    /// pins are released by the batch thread after its <c>send_batch</c> exactly as for any other
    /// record. A parked caller adds no pin window at all.
    /// </para>
    /// <para>
    /// <b>The steady-state path allocates nothing here</b> (DoD §10). Both statements are
    /// allocation-free: <see cref="Submit"/>'s append reuses the node chain, and
    /// <see cref="SemaphoreSlim.Wait(CancellationToken)"/> with a free permit takes no lock object and
    /// builds no registration (the linked-token source the pre-M11/P3.4 slow path allocated is gone
    /// with the timeout).
    /// </para>
    /// </remarks>
    /// <param name="record">The already-serialized record to accept.</param>
    /// <param name="completion">The record's awaiter.</param>
    /// <param name="delivery">The record's delivery-callback carrier, or <see langword="null"/>.</param>
    /// <param name="cancellationToken">
    /// The caller's token. It does <b>not</b> interrupt the admission wait (see the remarks); it is
    /// accepted so the entry point's shape matches <c>SendViaPump</c>'s.
    /// </param>
    /// <exception cref="ObjectDisposedException">
    /// The producer is closing — nothing was appended.
    /// </exception>
    internal void SubmitAdmitted(
        in SerializedProducerRecord record,
        TaskCompletionSource<RecordMetadata> completion,
        DeliveryRegistration? delivery,
        CancellationToken cancellationToken)
    {
        _ = cancellationToken;

        // APPEND FIRST. This throws ObjectDisposedException if teardown already closed the
        // accumulator, and that throw must propagate: nothing was appended, the caller has not been
        // handed `completion`, and SendViaPump turns it into the synchronous disposed-producer
        // outcome its preconditions already document.
        Submit(record, completion, delivery);

        // BLOCK AFTER. The record is committed, so this wait is purely a throttle on the CALLER.
        try
        {
            _admission.Wait(_spaceGate.Token);
        }
        catch (OperationCanceledException)
        {
            // Teardown (Stop, or AbandonOnThreadFailure) cancelled the gate while parked here.
            // Swallow — this must not throw: the record is already appended and is settled by the
            // chain machinery, so all that is left to do is release the caller's thread.
            //
            // Note the permit was NOT taken (SemaphoreSlim checks the token before the permit), so
            // the take/release accounting ends one release ahead for this record. Harmless: the
            // accumulator is closed by the same events that cancel the gate, so no later send can
            // consume the surplus — see _admission.
        }
    }

    /// <summary>
    /// Returns <paramref name="count"/> admission permits — the counterpart of
    /// <see cref="SubmitAdmitted"/>'s wait, called by the batch thread's take. A no-op for zero,
    /// because <see cref="SemaphoreSlim.Release(int)"/> rejects a zero count.
    /// </summary>
    /// <remarks>
    /// Never called with <see cref="_gate"/> held: a release can make a blocked caller runnable, and
    /// that caller appends (so it would take <see cref="_gate"/>).
    /// </remarks>
    private void ReleaseAdmission(int count)
    {
        if (count > 0)
        {
            _admission.Release(count);
        }
    }

    /// <summary>
    /// Pins the record's buffers and appends it. The one primitive both the production send path
    /// (<see cref="SubmitAdmitted"/>) and the tests use, so a fixture cannot diverge from what
    /// production does (DoD §12).
    /// </summary>
    /// <remarks>
    /// On any failure this releases every pin it took, so a refused submit leaves no trace. It takes
    /// and charges no admission permit — under append-first the permit is taken by
    /// <see cref="SubmitAdmitted"/> <em>after</em> this returns, and is given back by the batch
    /// thread's take (<see cref="TakeChainLocked"/>) for every record that reached a node.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The producer is closing — nothing was appended.</exception>
    internal void Submit(
        in SerializedProducerRecord record,
        TaskCompletionSource<RecordMetadata> completion,
        DeliveryRegistration? delivery)
    {
        MemoryHandle keyPin = default;
        MemoryHandle valuePin = default;
        PinnedTopicCache.TopicPin topicPin = default;
        bool appended = false;
        try
        {
            // key / value -> ReadOnlyMemory<byte>.Pin() (NOT GCHandle.Alloc, which cannot pin a
            // ReadOnlyMemory); PinIfNeeded also decides that an ABSENT or EMPTY buffer needs no pin,
            // the empty case using the marshaller's process-wide static sentinel (§4.2). topic -> one
            // permanently-pinned buffer per DISTINCT topic from the interning cache (§4.1).
            keyPin = ProducerSendBatchMarshal.PinIfNeeded(record.Key);
            valuePin = ProducerSendBatchMarshal.PinIfNeeded(record.Value);
            topicPin = _topics.Rent(record.Topic);

            Append(record, topicPin, keyPin, valuePin, completion, delivery);
            appended = true;
        }
        finally
        {
            if (!appended)
            {
                // Nothing was stored, so this frame is still the sole owner of all three pins.
                topicPin.Release();
                valuePin.Dispose();
                keyPin.Dispose();
            }
        }
    }

    /// <summary>
    /// Appends one already-pinned record. On return the accumulator owns
    /// <paramref name="topicPin"/> / <paramref name="keyPin"/> / <paramref name="valuePin"/> and
    /// will release each exactly once after the record's <c>send_batch</c>; if this throws, it owns
    /// none of them and the caller must release them.
    /// </summary>
    /// <param name="record">The record whose blittable slot is filled here.</param>
    /// <param name="topicPin">The interned topic pin ownership transfers with.</param>
    /// <param name="keyPin">The key buffer's pin ownership transfers with.</param>
    /// <param name="valuePin">The value buffer's pin ownership transfers with.</param>
    /// <param name="completion">The record's awaiter.</param>
    /// <param name="delivery">The record's delivery-callback carrier, or <see langword="null"/>.</param>
    /// <exception cref="ObjectDisposedException">The producer is closing — nothing was appended.</exception>
    private void Append(
        in SerializedProducerRecord record,
        PinnedTopicCache.TopicPin topicPin,
        MemoryHandle keyPin,
        MemoryHandle valuePin,
        TaskCompletionSource<RecordMetadata> completion,
        DeliveryRegistration? delivery)
    {
        lock (_gate)
        {
            if (_closed)
            {
                // Teardown already closed the accumulator. Nothing is stored, so the caller still
                // owns the pins — and the record never reached the core, so throwing here is the
                // same "nothing was sent" outcome the disposed guard above this layer produces.
                throw new ObjectDisposedException(nameof(NativeProducer));
            }

            Node? tail = _tail;
            if (tail is null || tail.Count == _settings.SlotCapacity)
            {
                // A new node only when the current one is full — the anchor's rule (:806). Allocate
                // BEFORE mutating anything, so an allocation failure leaves the chain untouched and
                // the caller's pins un-transferred. A recycled node costs nothing at all, which is
                // what keeps the steady-state send path free of node allocations (see _spare).
                Node fresh = TakeSpareLocked() ?? new Node(_settings.SlotCapacity);
                if (tail is null)
                {
                    _head = fresh;
                }
                else
                {
                    tail.Next = fresh;
                }

                _tail = fresh;
                tail = fresh;
            }

            // Grow the node's parallel arrays if this record needs a new slot. Also allocation
            // BEFORE any mutation: a failure here leaves Count untouched, so the caller still owns
            // its pins and the chain is unchanged.
            tail.EnsureSlot();

            int slot = tail.Count;
            // Marshal the record into its blittable slot HERE, on the caller's thread, rather than
            // keeping the SerializedProducerRecord until the drain. Two reasons, and neither changes
            // behavior: the node then needs no record array at all (its widest slot, ~80 B of the
            // ~232 B a stored record cost), and the batch thread's critical section shrinks to
            // "call send_batch" — a per-record marshal on the drain would be work the caller could
            // have done in parallel. Fill only records POINTERS and LENGTHS; the core still reads the
            // bytes at drain time, so the §4.7 mutation window is exactly as documented.
            ProducerSendBatchMarshal.Fill(
                ref tail.Natives[slot], record, topicPin.Pointer, keyPin, valuePin);

            tail.TopicPins[slot] = topicPin;
            tail.KeyPins[slot] = keyPin;
            tail.ValuePins[slot] = valuePin;
            tail.Completions[slot] = completion;
            tail.Deliveries[slot] = delivery;
            tail.Count = slot + 1;

            // UNCONDITIONAL: every record that reaches a node is one the admission bound is
            // counting, and the batch thread's take is what gives its permit back. This increment
            // is the ONLY writer that grows the count, and TakeChainLocked is the only one that
            // clears it — which is what makes the release count structurally equal to the append
            // count (see _admission).
            _chainRecords++;

            // Early wake at the threshold (anchor :825-827). Below it the batch thread is purely
            // timer-driven, which is what makes the sub-threshold delay 0..window uniform.
            if (tail.Count >= _settings.SlotThreshold)
            {
                Monitor.PulseAll(_gate);
            }
        }
    }

    /// <summary>
    /// Blocks until every send that had <b>returned</b> before this call has been handed to
    /// <c>send_batch</c> and its future enqueued to the completion pump — i.e. the accumulator is
    /// empty and idle in <see cref="IsEmptyAndIdleLocked"/>'s two-stage sense. Wakes the batch
    /// thread immediately rather than waiting out its window.
    /// </summary>
    /// <remarks>
    /// This is the primitive behind two things: the test-only "drain now and wait" hook that
    /// replaces a <c>Thread.Sleep</c> in the mock manual-completion tests (§9), and — from slice S6 —
    /// <c>Flush</c>'s accumulator drain, which is the deliberate divergence <em>toward</em> Java that
    /// the anchor lacks (§3.5: Python's <c>flush()</c> never signals its send thread, so it can
    /// return with records still buffered in the binding).
    /// </remarks>
    /// <param name="timeout">The maximum time to wait.</param>
    /// <returns><see langword="true"/> if the accumulator reached empty-and-idle in time.</returns>
    internal bool DrainPending(TimeSpan timeout)
    {
        long deadline = Stopwatch.GetTimestamp() + (long)(Stopwatch.Frequency * timeout.TotalSeconds);

        lock (_gate)
        {
            while (!IsEmptyAndIdleLocked())
            {
                // Re-arm the force flag on EVERY iteration, not once before the loop. The batch
                // thread consumes it when it takes a chain, and a record can land after that — a
                // caller released by the very drain this forced is the ordinary case — so a single
                // arming would leave the newcomer waiting out the full window while this caller
                // waits out its whole timeout. "Drain until empty AND idle" is the contract; one
                // drain is not it.
                _forceDrain = true;
                Monitor.PulseAll(_gate);

                int remainingMs = RemainingMilliseconds(deadline);
                if (remainingMs <= 0)
                {
                    return false;
                }

                Monitor.Wait(_gate, remainingMs);
            }

            return true;
        }
    }

    /// <summary>
    /// The awaitable form of <see cref="DrainPending"/>: completes once every record appended before
    /// the call has been handed to <c>send_batch</c> and its future enqueued to the completion pump.
    /// Used by <c>Flush</c>, which must not block its caller's thread.
    /// </summary>
    /// <remarks>
    /// <b>This is the deliberate divergence TOWARD Java that the anchor lacks (§3.5).</b> Python's
    /// <c>flush()</c> never signals its send thread — <c>record_batches_new_record_cnd</c> has three
    /// signal sites and none is a flush entry point — so it can return with records still buffered
    /// inside the binding, although from the caller's view those records <em>were</em> sent, because
    /// <c>send()</c> returned. Java's contract is that <c>flush()</c> blocks until every previously
    /// sent record completes, so the .NET flush drains first.
    /// </remarks>
    /// <param name="cancellationToken">Cancels this caller's wait only; the drain itself continues.</param>
    internal Task DrainPendingAsync(CancellationToken cancellationToken)
    {
        TaskCompletionSource<bool> waiter;
        lock (_gate)
        {
            if (IsEmptyAndIdleLocked())
            {
                return s_alreadyDrained;
            }

            // RunContinuationsAsynchronously so completing this under the accumulator's lock cannot
            // run a continuation inline on the batch thread.
            waiter = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _idleWaiters.Add(waiter);
            _forceDrain = true;
            Monitor.PulseAll(_gate);
        }

        if (!cancellationToken.CanBeCanceled)
        {
            return waiter.Task;
        }

        // Per-waiter cancellation: a cancelled waiter simply stops observing, and the drain it asked
        // for carries on for everyone else. Its TCS stays in the list and the idle signal's
        // TrySetResult on it is a harmless no-op.
        CancellationTokenRegistration registration = cancellationToken.Register(
            static state =>
            {
                (TaskCompletionSource<bool> tcs, CancellationToken token) =
                    ((TaskCompletionSource<bool>, CancellationToken))state!;
                tcs.TrySetCanceled(token);
            },
            (waiter, cancellationToken));

        waiter.Task.ContinueWith(
            static (_, state) => ((CancellationTokenRegistration)state!).Dispose(),
            registration,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        return waiter.Task;
    }

    /// <summary>
    /// "Empty and idle": the node chain is empty and no drain is in flight. Caller holds
    /// <see cref="_gate"/>.
    /// </summary>
    /// <remarks>
    /// <b>Under append-first this single stage covers every send whose <c>Send</c> has returned</b>
    /// (M11/P3.4). M11/P3.2 needed a second term because a record whose <c>Send</c> had already
    /// returned could still be sitting in the submission queue with the chain empty; now the append
    /// happens <em>before</em> the caller can return at all, so "the chain is empty" already implies
    /// "nothing accepted is unforwarded". It is in fact stronger: a record whose caller is still
    /// parked on the admission bound is in the chain too, so <c>Flush</c> includes it.
    /// </remarks>
    private bool IsEmptyAndIdleLocked() => _head is null && !_draining;

    /// <summary>
    /// Releases every <see cref="DrainPendingAsync"/> waiter if the accumulator is now empty and
    /// idle, and re-arms the force flag if it is not. Caller holds <see cref="_gate"/>.
    /// </summary>
    private void SignalIdleLocked()
    {
        if (_idleWaiters.Count == 0)
        {
            return;
        }

        if (!IsEmptyAndIdleLocked())
        {
            // Not idle yet — a record landed after the drain that was forced for these waiters took
            // its chain. Re-arm rather than let them wait out the window (the same reason
            // DrainPending re-arms on every iteration).
            _forceDrain = true;
            Monitor.PulseAll(_gate);
            return;
        }

        foreach (TaskCompletionSource<bool> waiter in _idleWaiters)
        {
            waiter.TrySetResult(true);
        }

        _idleWaiters.Clear();
    }

    /// <summary>
    /// Teardown steps 2–3 of the §3.8 handshake: cancel the admission gate so a parked caller is
    /// released, close the accumulator to new appends, wake the batch thread, and wait —
    /// <b>bounded</b> — for it to finish its final drain and exit. Idempotent.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>The two steps, and why they are in this order.</b> This sits inside §3.8's handshake,
    /// between its step 2 and step 3, so the accumulator drain still completes <em>before</em>
    /// <see cref="SendCompletionPump.CloseGate"/> — see <c>NativeProducer.StopAccumulator</c>'s
    /// remarks for why that ordering is load-bearing.
    /// <list type="number">
    /// <item><b>Cancel the gate</b> (§3.8 step 2, which must precede closing the accumulator). A
    /// caller parked in <see cref="SubmitAdmitted"/>'s admission wait has <em>already appended</em>
    /// its record, so this does not decide that record's fate — it only releases the caller's
    /// thread, which would otherwise wait for a permit that the batch thread may never hand back
    /// (it may be parked, or already gone). The record itself is taken by the final drain below and
    /// is <b>sent</b>, which is the anchor's outcome for a record that was already accumulated when
    /// its close arrived.</item>
    /// <item><b><c>_closed = true</c> + pulse.</b> Strictly after the cancel, so the caller it wakes
    /// is released rather than left holding teardown; and it is what tells the batch thread the
    /// chain it takes next is its <em>final</em> one. <see cref="Append"/> reads <c>_closed</c> under
    /// the same lock, and the batch thread reads it in the same acquisition as its take, so every
    /// record appended before this line is in the final chain and every append after it is refused
    /// with <see cref="ObjectDisposedException"/> — no record can fall between the two.</item>
    /// </list>
    /// </para>
    /// <para>
    /// <b>Why the join is bounded, and what expiry means.</b> The batch thread can be stuck for a
    /// long time inside <c>send_batch</c>: that call takes the core's coarse producer mutex and can
    /// block on a full <c>buffer.memory</c> for up to <c>max.block.ms</c> (default 60 s), for a whole
    /// chunk of records rather than one (§3.6). An unbounded join would make <c>Dispose</c> inherit
    /// that, and the §6.3 re-enumeration asks for a defined outcome instead of a hang.
    /// </para>
    /// <para>
    /// <b>On expiry this ABANDONS the batch thread rather than tearing its state down, deliberately.</b>
    /// The thread owns the node chain it took, and everything in that chain is either pinned (its
    /// buffers) or in flight (its futures). Faulting those records from here would mean releasing
    /// pins the core may still be reading and writing slots the thread is reading — a use-after-free
    /// and a data race, traded for a marginally earlier <see cref="Task"/> completion. So the defined
    /// outcome is: leave it running, and let the properties that already hold carry it. It is a
    /// background thread; <c>_closed</c> is set, so it drains once more and exits on its own; every
    /// record it holds still settles, only later. It stays memory-safe throughout, because
    /// <c>send_batch</c> takes the <see cref="SafeProducerHandle"/> as a P/Invoke parameter, so the
    /// marshaller's ref keeps <c>Producer_destroy</c> from running underneath an in-flight call and
    /// a handle already closed surfaces as <see cref="ObjectDisposedException"/>, which
    /// <see cref="SendNode"/> turns into a faulted send. A future that reaches the pump after its
    /// gate closed is faulted and freed in place — recorded residual 1, which this is the path that
    /// makes reachable.
    /// </para>
    /// <para>
    /// The caller uses the return value for exactly one decision: whether it is safe to free the
    /// interned topic buffers eagerly (they are pointed at by whatever the abandoned thread still
    /// holds), which is why <see cref="PinnedTopicCache"/> keeps its finalizer as the fallback.
    /// </para>
    /// </remarks>
    /// <param name="timeout">How long to wait for the batch thread's final drain.</param>
    /// <returns>
    /// <see langword="true"/> if the batch thread finished and exited; <see langword="false"/> if it
    /// was abandoned still running.
    /// </returns>
    internal bool Stop(TimeSpan timeout)
    {
        long deadline = Stopwatch.GetTimestamp() + (long)(Stopwatch.Frequency * timeout.TotalSeconds);

        // Step 1. Release anyone parked in SubmitAdmitted's admission wait, BEFORE _closed, so they
        // are not left waiting on a permit whose release depends on a batch thread that teardown is
        // about to join (or that may already have died). Their records are already in the chain and
        // the final take below sends them.
        //
        // ⚠ There are TWO _spaceGate.Cancel() call sites: this one and the unconditional one in
        // AbandonOnThreadFailure (M11/P3.2 slice S5 / §F5 / decision D5). Both are safe to race each
        // other: Cancel is idempotent, the wait site swallows the cancellation rather than acting on
        // it, and neither site decides a record's fate — the chain machinery does.
        _spaceGate.Cancel();

        // Step 2. Only now: the chain the batch thread takes next is its final one.
        lock (_gate)
        {
            _closed = true;
            Monitor.PulseAll(_gate);
        }

        // Step 3. The bounded join, on whatever is left of the deadline.
        return _thread.Join(RemainingMilliseconds(deadline));
    }

    private void RunLoop()
    {
        try
        {
            RunLoopCore();
        }
        catch (Exception exception)
        {
            // The batch thread must not die silently: everything it holds would then hang forever,
            // and appends would keep accumulating into a thread that will never drain them. Close
            // the accumulator so further appends are refused, and settle what is in hand — we are on
            // the batch thread, so this is the sole owner and touching the chain is safe here in a
            // way it is not from Stop's expiry path.
            //
            // SendNode already catches per-node failures, so reaching here means something outside
            // that — which is exactly why it is worth handling rather than assuming unreachable.
            AbandonOnThreadFailure(exception);
        }
    }

    /// <summary>
    /// Settles everything the batch thread owns after an unexpected failure of the thread itself,
    /// then lets it exit. Runs ON the batch thread, so it is the sole owner of both chains.
    /// </summary>
    /// <remarks>
    /// <b>Both chains, not just the accumulator's.</b> The failure can land while the thread holds a
    /// chain it has already taken — <see cref="ReleasePins"/> / <see cref="FaultNode"/> throwing
    /// inside <see cref="SendNode"/>, or <see cref="ReleaseAdmission"/> failing between the take and
    /// the send — so <see cref="_inFlight"/> is settled first, then whatever is still in the
    /// accumulator. Settling only the latter left every in-flight record stranded forever (awaiter
    /// never completed, pins never released, futures never destroyed) while <see cref="Stop"/> still
    /// reported the thread as exited.
    /// </remarks>
    private void AbandonOnThreadFailure(Exception cause)
    {
        Node? inFlight = _inFlight;
        _inFlight = null;

        Node? chain;
        int admitted;
        lock (_gate)
        {
            // Refuse further appends: an accumulator whose thread is gone can only strand them.
            _closed = true;
            chain = TakeChainLocked(out admitted);
            _draining = false;
            Monitor.PulseAll(_gate);
            SignalIdleLocked();
        }

        SettleAbandonedChain(inFlight, cause);
        SettleAbandonedChain(chain, cause);

        // M11/P3.2 slice S5 (§F5 / decision D5). UNCONDITIONAL — and that is the point: a caller
        // parked in SubmitAdmitted's admission wait is released by an EXPLICIT event, never by a
        // coincidence of permit accounting.
        //
        // ⚠ WHY NOT THE PERMIT ARITHMETIC. Leaving it to the ReleaseAdmission below rests on an
        // UNSTATED invariant — that a parked caller implies permits are exhausted, so a non-zero
        // release must wake it. The failure this handler exists for can BE an over-release
        // (SemaphoreFullException), i.e. precisely a case where that accounting is already known
        // broken, and SemaphoreSlim.Release validates the whole count BEFORE releasing anything, so
        // the throwing call releases NOTHING and the caller is never woken. Resting a liveness
        // property on arithmetic the handler's own trigger has already corrupted is the hang this
        // line removes.
        //
        // ⚠ IT DOES NOT DECIDE ANY RECORD'S FATE, which is what makes it safe to fire here. The
        // released caller's record was appended before it parked, so it is in one of the two chains
        // settled just above — _closed was set and the chain taken in the ONE _gate acquisition
        // above, so an append can only be refused (ObjectDisposedException, nothing stored) or land
        // in a node that same acquisition took. The caller itself just returns.
        //
        // Ordered AFTER the settles, so a pathological throw out of Cancel cannot strand the chains
        // this handler exists to settle.
        _spaceGate.Cancel();

        try
        {
            ReleaseAdmission(admitted);
        }
        catch (Exception)
        {
            // Last-resort handler: an over-release is exactly one of the failures that gets us here,
            // and the permit accounting is already unrecoverable at this point. Settling the records
            // is what matters, and it has already happened above; liveness for a parked caller is
            // the unconditional Cancel() above, not this call. So swallow rather than let this
            // escape and kill the thread with an unhandled exception.
        }
    }

    /// <summary>
    /// Releases the pins and faults the awaiters of every node in <paramref name="chain"/>, node by
    /// node, on the batch thread's failure path.
    /// </summary>
    /// <remarks>
    /// Both halves are idempotent per index — <see cref="ReleasePins"/> resets each slot to its
    /// <c>default</c> and <see cref="FaultNode"/> skips an index whose completion was already
    /// nulled — so a node that <see cref="SendNode"/> had partly settled is finished here rather
    /// than settled twice. The per-node <c>catch</c> is what keeps one failing node from stranding
    /// its successors: this is the handler of last resort, so it must not itself be the thing that
    /// abandons records.
    /// </remarks>
    private static void SettleAbandonedChain(Node? chain, Exception cause)
    {
        while (chain is not null)
        {
            Node node = chain;
            chain = node.Next;
            node.Next = null;

            try
            {
                int count = node.Count;
                ReleasePins(node, count);

                // Where nothing reached the core (both result slots zero) FaultNode fires the
                // delivery callback — §6.2's "correct and complete": a failure notification for an
                // un-accepted record invents nothing and can never duplicate. Where the core DID
                // accept a record, FaultNode destroys its future unread and fires nothing.
                FaultNode(node, settled: 0, count: count, cause: cause);
            }
            catch (Exception)
            {
                // See the remarks: keep going, so one node cannot strand the rest of the chain.
            }
        }
    }

    private void RunLoopCore()
    {
        while (true)
        {
            Node? chain;
            bool stopping;
            int admitted;

            lock (_gate)
            {
                // FREE-RUNNING window (anchor :535): the deadline comes from THIS loop's own clock
                // at the top of the iteration, unrelated to when any record arrived. That is what
                // makes a sub-threshold batch wait 0..window UNIFORMLY rather than a fixed window —
                // an accepted cost of Python parity (§3.3), not a bug to "fix" by restarting the
                // timer on the first append.
                long deadline = Stopwatch.GetTimestamp() + _windowTicks;

                while (!_closed
                    && !_forceDrain
                    && (_head is null || _head.Count < _settings.SlotThreshold))
                {
                    int remainingMs = RemainingMilliseconds(deadline);
                    if (remainingMs <= 0)
                    {
                        break;
                    }

                    Monitor.Wait(_gate, remainingMs);
                }

                _forceDrain = false;
                stopping = _closed;
                chain = TakeChainLocked(out admitted);
                if (chain is not null)
                {
                    _draining = true;

                    // Publish the taken chain BEFORE leaving the lock, and so before anything that
                    // can throw between the take and the send (ReleaseAdmission below is the only
                    // such statement left). Until this assignment the local `chain` is the only
                    // reference to it, and a throw there would strand every record it holds — see
                    // _inFlight.
                    _inFlight = chain;
                }
                else
                {
                    // Idle: release any drain waiter (and, when closing, do it before exiting).
                    Monitor.PulseAll(_gate);
                    SignalIdleLocked();
                }
            }

            // THE ADMISSION BOUND'S ONLY RELEASE SITE ON THE NORMAL PATH — the one that unblocks
            // callers parked in SubmitAdmitted. Released OUTSIDE the lock, as the anchor fires its
            // space callbacks after unlocking (:581): releasing under the lock could make a blocked
            // caller runnable and that caller appends, so it would take _gate.
            //
            // ⚠ THE OVERSHOOT IS THE IN-FLIGHT CHAIN (Critic 72 finding 72.5). Permits are released
            // for the WHOLE taken chain, before send_batch has run for any of it, so the
            // accepted-but-unsent POPULATION exceeds the count _chainRecords reports by |chain| —
            // every record taken but not yet passed to a send_batch call. A chain holds at most the
            // appends possible between two takes, which the bound itself limits, so
            //
            //     peak accepted-but-unsent <= 2 * (MaxAdmittedRecords + concurrently parked callers)
            //
            // — one term for the chain being filled, one for the chain in flight. That is the same
            // shape as before append-first (there it was MaxAdmitted + min(MaxAdmitted,
            // MaxAccumulated)); what changed is that the second term is now the same bound rather
            // than a second one, and that it carries the parked-caller slack the anchor also has.
            ReleaseAdmission(admitted);

            if (chain is not null)
            {
                try
                {
                    SendChain();
                }
                finally
                {
                    lock (_gate)
                    {
                        _draining = false;
                        Monitor.PulseAll(_gate);
                        SignalIdleLocked();
                    }
                }
            }

            if (stopping)
            {
                // The chain taken above was final: _closed was set under the same lock that
                // Append checks, so nothing can have been appended after it.
                break;
            }
        }
    }

    /// <summary>
    /// Takes the whole chain and resets the accumulation (anchor :562-579), reporting how many
    /// admission permits the caller must release once it has left the lock.
    /// </summary>
    /// <param name="admitted">
    /// Admission permits owed back — <em>every</em> record in the taken chain, which is exactly what
    /// <c>_chainRecords</c> counts. This is the only site that clears it, so the total released over
    /// a lifetime equals the total appended.
    /// </param>
    private Node? TakeChainLocked(out int admitted)
    {
        Node? chain = _head;
        _head = null;
        _tail = null;

        admitted = _chainRecords;
        _chainRecords = 0;
        return chain;
    }

    /// <summary>
    /// Sends every node of the chain <see cref="_inFlight"/> names, advancing it as each node is
    /// fully settled so a throw leaves exactly the unfinished remainder published.
    /// </summary>
    private void SendChain()
    {
        // One send_batch per chunk, walking the chain node by node (anchor :593 issues one call per
        // node). Never one call for the whole chain: chunking is ALSO what bounds how long the
        // core's coarse producer mutex is held in a single stretch (§3.4), so collapsing this loop
        // into a single flattened call would silently remove that cap.
        while (_inFlight is not null)
        {
            Node node = _inFlight;

            // SendNode settles (or faults) every slot of this node, and its own catch swallows
            // everything that happens inside it into FaultNode — so the ONLY throw that escapes is
            // FaultNode itself failing. When it does, this node is left partly unsettled and
            // _inFlight must still name IT, which is what lets AbandonOnThreadFailure finish it
            // instead of stranding every record it holds.
            SendNode(node);

            // The advance is pinned between TWO neighbours and BOTH directions are load-bearing:
            //
            //   * AFTER SendNode — hoisting it above the call drops the failing node out of
            //     _inFlight the instant SendNode throws, so the handler of last resort cannot see
            //     it and its remaining records are stranded forever (awaiters never completed,
            //     futures never destroyed). That is the second of 65.3's two triggers, and
            //     BatchThreadFailure_INSIDESendNode_StillSettlesTheRestOfThatNode guards it.
            //   * BEFORE RecycleNode — deferring it past the recycle lets the failure handler
            //     re-enter a node that is already back in the spare slot and re-fillable by Append.
            _inFlight = node.Next;
            node.Next = null;
            RecycleNode(node);
        }
    }

    /// <summary>
    /// Offers a fully-settled node back to <see cref="Append"/> for reuse, keeping <b>one</b> spare.
    /// </summary>
    /// <remarks>
    /// <b>A second deliberate deviation from the anchor, and the one that makes the steady-state
    /// send path allocation-free.</b> The anchor <c>PyMem_RawFree</c>s each node once its completions
    /// are read, and mallocs a fresh one per drain; a node there is five pointer arrays. Here a node
    /// is eight arrays of by-value slots, so allocating one per drain would charge the <em>caller</em>
    /// thread a fresh growth sequence every window — a per-record cost on the hot path that the DoD
    /// §10 audit is precisely about. Reuse is safe because by the time <see cref="SendNode"/> has
    /// returned, every slot of the node is settled: the pins were released, each future was either
    /// destroyed or handed to the pump, and the completion / delivery references were nulled. The
    /// recycle happens on the batch thread under the same lock <see cref="Append"/> takes, so the
    /// node can never be filled while it is still being sent.
    /// <para>
    /// One spare, not a pool: it removes the steady-state allocation without turning node lifetime
    /// into a free-list to reason about. A burst deep enough to need several nodes at once still
    /// allocates the extra ones and lets the GC take them.
    /// </para>
    /// </remarks>
    private void RecycleNode(Node node)
    {
        lock (_gate)
        {
            _spare ??= node;
        }
    }

    /// <summary>Takes the spare node, if any, reset for refilling. Caller holds <see cref="_gate"/>.</summary>
    private Node? TakeSpareLocked()
    {
        Node? spare = _spare;
        if (spare is null)
        {
            return null;
        }

        _spare = null;

        // Only Count and Next need resetting: every other slot is written before it is read —
        // Natives by Fill, the pins / completion / delivery by Append, and Futures / Errors by the
        // Array.Clear in SendNode. ReleasePins already reset the pin slots to `default`, so no stale
        // GCHandle survives into the reused node.
        spare.Count = 0;
        spare.Next = null;
        return spare;
    }

    private void SendNode(Node node)
    {
        int count = node.Count;
        if (count == 0)
        {
            return;
        }

        // How many indices CompleteNode has fully handled. Read by FaultNode so a partial throw
        // faults only the tail — an index already handed to the pump must not be touched again.
        int settled = 0;

        // Read ONCE and shared by the send loop and the completion loop below, so the two provably
        // walk the same chunk boundaries: a completion group IS one send_batch call's records
        // (M11/P3.2 §3B.1), and recomputing the bound twice from the settings would let a future
        // edit silently move one of them.
        int chunk = _settings.BatchChunk;
        try
        {
            try
            {
                // The natives were filled at Append time, on each caller's own thread.
                //
                // The callee writes both slots for every index it processes, but a chunk that never
                // runs (an earlier chunk threw) would leave stale values behind, so start from zero
                // and let "both slots zero" mean "the core never saw this record" (anchor :599-600
                // memsets the same two arrays).
                Array.Clear(node.Futures, 0, count);
                Array.Clear(node.Errors, 0, count);

                for (int offset = 0; offset < count; offset += chunk)
                {
                    int length = Math.Min(chunk, count - offset);
                    _ = ProducerSendBatchMarshal.SendBatch(
                        _handle, node.Natives, offset, length, node.Futures, node.Errors);

                    // Instrumentation only — written by this thread alone (§3.4 is asserted by CALL
                    // COUNT and by the largest call, not by "the records arrived").
                    Interlocked.Increment(ref _sendBatchCalls);
                    Interlocked.Add(ref _sendBatchRecords, length);
                    if (length > _largestSendBatch)
                    {
                        Volatile.Write(ref _largestSendBatch, length);
                    }
                }
            }
            finally
            {
                // UNPIN every buffer in this node, exactly once, on every path (§4.4) — after
                // send_batch returned (the core copied everything synchronously inside it) and
                // before any future reaches the pump. Never in the pump; never across the Task.
                ReleasePins(node, count);
            }

            // ONE COMPLETION GROUP PER send_batch CALL (M11/P3.2 §3B.1) — the same boundaries the
            // send loop above used, walked in the same order. `settled` is a whole-node cursor, so
            // each call resumes where the previous one stopped and FaultNode's "the tail from
            // `settled`" contract is unchanged.
            //
            // Why the unit is the CALL and not the node, even though they coincide at the defaults
            // (chunk == SlotCapacity == node capacity, so this loop runs once and is identical to
            // the anchor's one get_all per BatchNode): a LOWERED CONFLUENT_KAFKA_PRODUCER_BATCH_CHUNK
            // splits a node into ceil(count / chunk) calls, and grouping per node would then re-mix
            // several send_batch calls into one get_all — reintroducing under an override exactly the
            // shape grouping exists to remove. Do not "simplify" this back to one call per node.
            for (int offset = 0; offset < count; offset += chunk)
            {
                CompleteNode(node, offset + Math.Min(chunk, count - offset), ref settled);
            }
        }
        catch (Exception exception)
        {
            FaultNode(node, settled, count, exception);
        }
    }

    /// <summary>
    /// Walks the per-record results of <b>one <c>send_batch</c> call</b> — the node's indices from
    /// <paramref name="settled"/> up to <paramref name="end"/>: faults the immediate-error indices
    /// here and hands the rest to the completion pump as a <b>single group</b> — the anchor's
    /// compaction (<c>:600-621</c>), expressed as "settle in place, collect the survivors" rather
    /// than "shift the survivors down", because .NET has no reason to compact arrays it is about to
    /// drop.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>This is the phase's new delivery-callback firing site (§6.1).</b> An immediate error is a
    /// core completion — the core reported that it would not take this record — so the callback is
    /// owed exactly once here, and that index deliberately does not also reach the pump.
    /// </para>
    /// <para>
    /// <b>The group is built as the walk goes, so compaction costs nothing extra</b> (M11/P3.2
    /// §3B.2): only the accepted indices are written into it, and its <c>count</c> is what the pump
    /// bounds every loop by. The three arrays are sized for the whole call because the accepted
    /// count is not known until the walk ends; they are allocated lazily, so a call the core
    /// rejected outright allocates nothing.
    /// </para>
    /// <para>
    /// ⚠ <b>THE HAND-OVER ORDER IS THE MOST DELICATE THING HERE.</b> See the note at the
    /// <c>Enqueue</c> call below — it is the one that must not be reordered.
    /// </para>
    /// </remarks>
    /// <param name="node">The node being sent.</param>
    /// <param name="end">
    /// One past the last index of this <c>send_batch</c> call — the walk runs from
    /// <paramref name="settled"/> to here.
    /// </param>
    /// <param name="settled">
    /// The whole-node cursor: how many indices have been fully handled. Advanced to
    /// <paramref name="end"/> only once this call's group has been handed over, so
    /// <see cref="FaultNode"/> can still finish everything this call did not.
    /// </param>
    private void CompleteNode(Node node, int end, ref int settled)
    {
        int start = settled;

        // This call's completion group, allocated on the first accepted index. Sized for the whole
        // call; `accepted` is the count that travels with it.
        IntPtr[]? futures = null;
        TaskCompletionSource<RecordMetadata>[]? completions = null;
        DeliveryRegistration?[]? deliveries = null;
        int accepted = 0;

        for (int i = start; i < end; i++)
        {
            IntPtr error = node.Errors[i];
            IntPtr future = node.Futures[i];
            TaskCompletionSource<RecordMetadata> completion = node.Completions[i]!;
            DeliveryRegistration? delivery = node.Deliveries[i];

            if (error != IntPtr.Zero)
            {
                // Per-record rejection. Null both slots FIRST: this frame now owns both handles and
                // frees both below, so a throw from here lands in FaultNode with nothing left to
                // double-free. The ABI writes a null future alongside an error, but free
                // defensively rather than assume — a leaked future is invisible to every managed
                // assertion (ffi §A2).
                node.Errors[i] = IntPtr.Zero;
                node.Futures[i] = IntPtr.Zero;
                if (future != IntPtr.Zero)
                {
                    NativeMethods.FutureRecordMetadataDestroy(future);
                }

                KafkaException failure = KafkaException.FromHandle(error)
                    ?? new KafkaException("The send failed without an error handle.");

                // D3 ordering: the callback runs BEFORE the awaiter is released, and unconditionally
                // (never gated on TrySet*'s bool) — a canceled awaiter still owes its notification.
                delivery?.Fire(null, failure);
                completion.TrySetException(failure);
            }
            else if (future == IntPtr.Zero)
            {
                // Defensive: the ABI writes exactly one of the pair per index, so neither being set
                // is a core contract violation. The record never reached the core, so the callback
                // IS owed here (§6.2 — faulting an un-accepted record is correct and complete).
                KafkaException failure = new KafkaException(
                    "kafka_producer_Producer_send_batch returned a null future without an error.");
                delivery?.Fire(null, failure);
                completion.TrySetException(failure);
            }
            else
            {
                // Accepted: this index joins the group whose ownership transfers to the pump below.
                // Its Futures / Completions / Deliveries slots are deliberately LEFT AS THEY ARE —
                // see the hand-over note at the Enqueue call.
                if (futures is null)
                {
                    int capacity = end - start;
                    futures = new IntPtr[capacity];
                    completions = new TaskCompletionSource<RecordMetadata>[capacity];
                    deliveries = new DeliveryRegistration?[capacity];
                }

                futures[accepted] = future;
                completions![accepted] = completion;
                deliveries![accepted] = delivery;
                accepted++;
                continue;
            }

            // Settled in place: drop the references so a node held anywhere does not retain user
            // objects. A null Completions slot is also how FaultNode recognizes an index it must
            // leave alone, which is what makes a throw later in this walk safe to recover from.
            node.Completions[i] = null;
            node.Deliveries[i] = null;
        }

        if (accepted > 0)
        {
            // ⚠ OWNERSHIP TRANSFERS HERE, AND THE NULLING BELOW MUST STAY BELOW IT.
            //
            // If Enqueue throws (its queue growing under out-of-memory — its own _stopped branch
            // frees the futures and returns normally), ownership never transferred, so FaultNode
            // must still see a live future at each of these indices and free it. Nulling first would
            // make those indices look like "the core never saw this record", and FaultNode would
            // then fire delivery callbacks the pump is also about to fire — DUPLICATES, which the
            // exactly-once obligation makes strictly worse than the drop (root CLAUDE.md §9.5).
            //
            // Unchanged in substance from the pre-grouping form, which nulled one slot after one
            // per-record Enqueue: same invariant, one enqueue per send_batch call instead of one per
            // record.
            _pump.Enqueue(futures!, completions!, deliveries!, accepted);

            // Transferred: release this node's claim on exactly the handed-over indices. A non-zero
            // future in [start, end) identifies them precisely — the settle-in-place branches above
            // zeroed theirs, and SendNode cleared the whole range before send_batch wrote it.
            for (int i = start; i < end; i++)
            {
                if (node.Futures[i] != IntPtr.Zero)
                {
                    node.Futures[i] = IntPtr.Zero;
                    node.Completions[i] = null;
                    node.Deliveries[i] = null;
                }
            }
        }

        // Only now: everything in [start, end) is either settled in place or owned by the pump, so
        // FaultNode has nothing left to do for this call's range.
        settled = end;
    }

    /// <summary>
    /// The no-hang guard for a node whose processing threw: frees whatever native handle each
    /// unsettled index still holds and faults its awaiter, so no <see cref="Task"/> is left pending.
    /// Indices <c>[0, settled)</c> are untouched — they already completed, or the pump owns them.
    /// </summary>
    /// <remarks>
    /// <b>Delivery callbacks fire only where the core never accepted the record</b> — both result
    /// slots zero, i.e. the throw preceded this record's <c>send_batch</c>. That case is §6.2's
    /// "correct and complete": nothing was sent, so a failure notification invents nothing and can
    /// never duplicate. Where the core <em>did</em> accept the record (a live future this method
    /// destroys unread), no callback is fired, for the reason recorded residuals 1–3 give: there is
    /// no core completion in hand and the record may still be delivered, so a fabricated failure
    /// would be an invented one. Reachable only under an unexpected managed or native failure on the
    /// batch thread (out of memory, or a P/Invoke against a torn-down producer).
    /// </remarks>
    private static void FaultNode(Node node, int settled, int count, Exception cause)
    {
        KafkaException failure = cause as KafkaException
            ?? new KafkaException("The producer send-batch thread failed to process a batch.", cause);

        for (int i = settled; i < count; i++)
        {
            IntPtr future = node.Futures[i];
            IntPtr error = node.Errors[i];
            node.Futures[i] = IntPtr.Zero;
            node.Errors[i] = IntPtr.Zero;

            bool acceptedByCore = future != IntPtr.Zero;
            if (acceptedByCore)
            {
                NativeMethods.FutureRecordMetadataDestroy(future);
            }

            if (error != IntPtr.Zero)
            {
                NativeMethods.ErrorDestroy(error);
            }

            TaskCompletionSource<RecordMetadata>? completion = node.Completions[i];
            if (completion is null)
            {
                continue;
            }

            if (!acceptedByCore && error == IntPtr.Zero)
            {
                node.Deliveries[i]?.Fire(null, failure);
            }

            completion.TrySetException(failure);
            node.Completions[i] = null!;
            node.Deliveries[i] = null;
        }
    }

    /// <summary>
    /// Releases every pin the node owns, exactly once. Each slot is reset to its <c>default</c> as
    /// it goes, so a second call (or a later sweep) cannot double-free — <c>GCHandle.Free</c> throws
    /// on a freed handle, which on the batch thread would be an unhandled exception.
    /// </summary>
    private static void ReleasePins(Node node, int count)
    {
        for (int i = 0; i < count; i++)
        {
            node.TopicPins[i].Release();
            node.TopicPins[i] = default;

            node.ValuePins[i].Dispose();
            node.ValuePins[i] = default;

            node.KeyPins[i].Dispose();
            node.KeyPins[i] = default;
        }
    }

    private static int RemainingMilliseconds(long deadlineTimestamp)
    {
        long remaining = deadlineTimestamp - Stopwatch.GetTimestamp();
        if (remaining <= 0)
        {
            return 0;
        }

        double ms = remaining * 1000.0 / Stopwatch.Frequency;
        return ms >= 1.0 ? (int)Math.Min(ms, int.MaxValue) : 1;
    }

    /// <summary>
    /// One accumulator node — the .NET twin of the anchor's <c>BatchNode</c>
    /// (<c>_confluentkafka.c:360-369</c>), holding at most
    /// <see cref="SendAccumulatorSettings.SlotCapacity"/> records, after which
    /// <see cref="SendAccumulator.Append"/> starts a new one (the anchor's rule, <c>:806</c>).
    /// </summary>
    /// <remarks>
    /// <b>Deliberate deviation from the anchor: the arrays GROW to that cap instead of being
    /// allocated at it</b> — every constant is unchanged (a node still holds at most
    /// <c>SLOT_CAPACITY</c> records, and the chunk still equals it), only the allocation strategy
    /// differs. The anchor's arrays are fixed because a C struct has no other option, and its five
    /// slots are <em>pointers</em> — 44 KB per node (5 × 1100 × 8). .NET's eight slots are mostly
    /// by-value: a <see cref="PinnedTopicCache.TopicPin"/> (16 B), two
    /// <see cref="MemoryHandle"/>s (24 B each), a <see cref="ProducerRecordNative"/> (56 B), two
    /// references and two <see cref="IntPtr"/> result slots (8 B each) — <b>~152 B per slot, so
    /// ~167 KB per node, roughly 3.8× the anchor's</b>. (Not ~232 B: that figure counts a
    /// <see cref="SerializedProducerRecord"/> slot this node does <em>not</em> have — §4.3's type
    /// sketch had one and <see cref="Append"/> marshals in its place, which is where its ~80 B
    /// went.) A node is allocated
    /// per drain once the previous chain is taken, so at a low send rate (a handful of records per
    /// window) a fixed node would turn ~100 drains/second into ~17 MB/second of garbage for a
    /// handful of records — a .NET-only cost with no counterpart in the design being mirrored.
    /// Growing from <see cref="InitialCapacity"/> makes the node cost track what the drain actually
    /// used, and a full node still ends up at exactly the anchor's capacity.
    /// </remarks>
    private sealed class Node
    {
        // Small enough that a low-rate drain allocates almost nothing, large enough that the
        // doubling to SLOT_CAPACITY costs a handful of copies (7 growths, ~2x total slot copies).
        private const int InitialCapacity = 16;

        private readonly int _maxCapacity;

        internal Node(int maxCapacity)
        {
            _maxCapacity = maxCapacity;
            int initial = Math.Min(InitialCapacity, maxCapacity);

            TopicPins = new PinnedTopicCache.TopicPin[initial];
            KeyPins = new MemoryHandle[initial];
            ValuePins = new MemoryHandle[initial];
            Completions = new TaskCompletionSource<RecordMetadata>?[initial];
            Deliveries = new DeliveryRegistration?[initial];
            Natives = new ProducerRecordNative[initial];
            Futures = new IntPtr[initial];
            Errors = new IntPtr[initial];
        }

        /// <summary>No Python counterpart: Python owns a <c>topic_owned</c> copy per record.</summary>
        internal PinnedTopicCache.TopicPin[] TopicPins;

        /// <summary>No Python counterpart: Python holds a refcount on the record's <c>bytes</c>.</summary>
        internal MemoryHandle[] KeyPins;

        internal MemoryHandle[] ValuePins;

        /// <summary>Nulled per index as it settles, so a node retains no user objects it is done with.</summary>
        internal TaskCompletionSource<RecordMetadata>?[] Completions;

        internal DeliveryRegistration?[] Deliveries;

        /// <summary>The blittable array the P/Invoke reads (anchor: a stack array at <c>:585</c>).</summary>
        internal ProducerRecordNative[] Natives;

        internal IntPtr[] Futures;

        internal IntPtr[] Errors;

        internal int Count { get; set; }

        internal Node? Next { get; set; }

        /// <summary>
        /// Grows every parallel array so index <see cref="Count"/> is writable. Called under the
        /// accumulator's lock, and only from <see cref="SendAccumulator.Append"/> — never while the
        /// batch thread is sending this node, which happens only after the node has been taken out of
        /// the chain. Grows all eight together or not at all: a partial growth would leave the
        /// parallel arrays at different lengths, and the index that walks them assumes they match.
        /// </summary>
        internal void EnsureSlot()
        {
            int required = Count + 1;

            // Gate on the LAST array resized below, not the first: an out-of-memory part way through
            // the sequence leaves the earlier arrays longer than the later ones, and gating on
            // `TopicPins` (the first) would then wave through a write that indexes past `Errors`.
            // Errors reaching `required` means all eight did.
            if (required <= Errors.Length)
            {
                return;
            }

            int grown = Math.Min(Math.Max(Errors.Length * 2, required), _maxCapacity);

            Array.Resize(ref TopicPins, grown);
            Array.Resize(ref KeyPins, grown);
            Array.Resize(ref ValuePins, grown);
            Array.Resize(ref Completions, grown);
            Array.Resize(ref Deliveries, grown);
            Array.Resize(ref Natives, grown);
            Array.Resize(ref Futures, grown);
            Array.Resize(ref Errors, grown);
        }
    }
}
