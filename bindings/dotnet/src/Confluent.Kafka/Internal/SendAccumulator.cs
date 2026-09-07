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

    private readonly Thread _thread;
    private readonly long _windowTicks;

    // The stage-1 backpressure bound (§4.6): one permit per record appended-but-not-yet-taken. The
    // anchor's own comment is the Java-faithfulness argument for having it at all — "one complete
    // batch beyond the one being filled — mirrors Java's send() blocking once buffer.memory is
    // full, applied here at batch granularity in front of the Rust accumulator"
    // (_confluentkafka.c:21-26). Unlike Option C's native block inside the coarse FFI mutex, this is
    // a MANAGED, cancellable wait, which is what lets teardown wake a blocked sender (§2.2).
    private readonly SemaphoreSlim _space;

    // Cancelled by Stop() so a Send parked on _space is released instead of pinning teardown behind
    // it (§3.8 step 2, which must precede closing the accumulator). Never disposed: a
    // CancellationTokenSource with no timer holds no unmanaged resource, and disposing one while a
    // linked registration is being torn down is its own hazard.
    private readonly CancellationTokenSource _spaceGate = new CancellationTokenSource();

    private Node? _head;      // oldest node, the one the wait loop measures (anchor: next_batches_to_send)
    private Node? _tail;      // the node being filled (anchor: last_accumulating_batch)
    private Node? _spare;     // one fully-settled node kept for reuse (see RecycleNode)
    private int _accumulated; // records appended but not yet taken by the batch thread
    private bool _closed;     // no further appends; the thread drains once more and exits
    private bool _forceDrain; // a DrainPending caller is waiting; skip the rest of the window
    private bool _draining;   // a taken chain is being sent right now (outside the lock)
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
        _space = new SemaphoreSlim(settings.MaxAccumulatedRecords, settings.MaxAccumulatedRecords);

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
    /// Takes a backpressure permit without waiting — the <b>fast path</b>, which must stay
    /// allocation-free so the common send does not regress the DoD §10 budget. Returns
    /// <see langword="false"/> when the accumulator already holds
    /// <see cref="SendAccumulatorSettings.MaxAccumulatedRecords"/> records that the batch thread has
    /// not taken yet, in which case the caller must go through
    /// <see cref="WaitForSpaceAsync"/> (and, unlike this method, yield).
    /// </summary>
    internal bool TryAcquireSpace() => _space.Wait(0);

    /// <summary>
    /// Waits for a backpressure permit — the <b>slow path</b>. Completes when the batch thread's
    /// next take frees capacity, faults with <see cref="ObjectDisposedException"/> if teardown
    /// cancels the gate first, or cancels if <paramref name="cancellationToken"/> fires.
    /// </summary>
    /// <remarks>
    /// <b>No lost wakeup, by a different mechanism than the anchor's.</b> Python does the
    /// check-and-register under the <em>same</em> mutex its drain holds
    /// (<c>py_Producer_on_space_available</c>), because its "space available" signal is a callback
    /// list that a drain running in between would simply miss. A <see cref="SemaphoreSlim"/> counts
    /// <em>permits</em> instead, so a drain that completes between this caller's failed
    /// <see cref="TryAcquireSpace"/> and its arrival here has already left a permit behind and this
    /// returns immediately. The property is the anchor's; the mechanism is the one .NET already
    /// provides. That is also why the permits can be released <b>outside</b> the accumulator's lock
    /// (as the anchor fires its space callbacks after unlocking, <c>:581</c>), which matters:
    /// releasing under the lock could run a waiter's continuation inline on the batch thread, and
    /// that continuation appends.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">Teardown cancelled the gate; nothing was appended.</exception>
    internal async Task WaitForSpaceAsync(CancellationToken cancellationToken)
    {
        CancellationToken gate = _spaceGate.Token;
        if (!cancellationToken.CanBeCanceled)
        {
            try
            {
                await _space.WaitAsync(gate).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (gate.IsCancellationRequested)
            {
                throw ClosedDuringBackpressure();
            }

            return;
        }

        using CancellationTokenSource linked =
            CancellationTokenSource.CreateLinkedTokenSource(gate, cancellationToken);
        try
        {
            await _space.WaitAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (gate.IsCancellationRequested)
        {
            // Teardown wins over the caller's token when both fired: the producer is going away, so
            // "closed" is the more accurate answer than "you cancelled".
            throw ClosedDuringBackpressure();
        }
    }

    /// <summary>
    /// Pins the record's buffers and appends it, taking ownership of the permit the caller acquired.
    /// The one primitive both the production send path and the tests use, so a fixture cannot
    /// diverge from what production does (DoD §12).
    /// </summary>
    /// <remarks>
    /// <b>The permit is acquired BEFORE the pins, never after</b> (§4.4): a sender blocked on
    /// backpressure while holding pins would turn a bound on <em>records</em> into an unbounded pin
    /// window. On any failure this releases both the pins and the permit, so a rejected submit
    /// leaves no trace.
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
                // Nothing was stored, so this frame is still the sole owner of all three pins — and
                // of the permit, which must go back or the bound leaks one slot per rejected submit.
                topicPin.Release();
                valuePin.Dispose();
                keyPin.Dispose();
                ReleaseSpace(1);
            }
        }
    }

    /// <summary>
    /// Appends one already-pinned record. On return the accumulator owns
    /// <paramref name="topicPin"/> / <paramref name="keyPin"/> / <paramref name="valuePin"/> and
    /// will release each exactly once after the record's <c>send_batch</c>; if this throws, it owns
    /// none of them and the caller must release them.
    /// </summary>
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
            _accumulated++;

            // Early wake at the threshold (anchor :825-827). Below it the batch thread is purely
            // timer-driven, which is what makes the sub-threshold delay 0..window uniform.
            if (tail.Count >= _settings.SlotThreshold)
            {
                Monitor.PulseAll(_gate);
            }
        }
    }

    /// <summary>
    /// Blocks until every record appended before this call has been handed to <c>send_batch</c> and
    /// its future enqueued to the completion pump — i.e. the accumulator is empty and no drain is in
    /// flight. Wakes the batch thread immediately rather than waiting out its window.
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
            _forceDrain = true;
            Monitor.PulseAll(_gate);

            while (_head is not null || _draining)
            {
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
    /// Closes the accumulator to new appends, wakes the batch thread and joins it. The thread
    /// performs one final drain first, so every record accepted before this call reaches the core
    /// (and its future the completion pump) rather than being abandoned. Idempotent.
    /// </summary>
    internal void Stop()
    {
        // §3.8 step 2 BEFORE step 3: cancel the backpressure gate first, so a Send parked on a
        // permit is released rather than holding teardown behind it. Doing it after closing the
        // accumulator would still work here, but the ordering is the contract — and it is the one
        // place this design is strictly better than the inline send it replaces, where a caller
        // blocked in the core could not be woken by a concurrent close at all (§2.2 / §4.6).
        _spaceGate.Cancel();

        lock (_gate)
        {
            _closed = true;
            Monitor.PulseAll(_gate);
        }

        // Unbounded in this slice (see the type remarks): slice S5 replaces it with a bounded wait
        // and a defined outcome on expiry, together with the rest of the §3.8 handshake.
        _thread.Join();
    }

    private void RunLoop()
    {
        while (true)
        {
            Node? chain;
            bool stopping;
            int freed;

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
                chain = TakeChainLocked(out freed);
                if (chain is not null)
                {
                    _draining = true;
                }
                else if (stopping)
                {
                    // Nothing left and closing: wake any DrainPending waiter before exiting.
                    Monitor.PulseAll(_gate);
                }
            }

            // Capacity is free again — release OUTSIDE the lock, as the anchor fires its space
            // callbacks after unlocking (:581). Releasing under the lock could run a waiter's
            // continuation inline on this thread, and that continuation appends.
            ReleaseSpace(freed);

            if (chain is not null)
            {
                try
                {
                    SendChain(chain);
                }
                finally
                {
                    lock (_gate)
                    {
                        _draining = false;
                        Monitor.PulseAll(_gate);
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
    /// backpressure permits the caller must release once it has left the lock.
    /// </summary>
    private Node? TakeChainLocked(out int freed)
    {
        Node? chain = _head;
        _head = null;
        _tail = null;

        freed = _accumulated;
        _accumulated = 0;
        return chain;
    }

    /// <summary>
    /// Returns <paramref name="count"/> backpressure permits. Never called with the accumulator's
    /// lock held (see <see cref="RunLoop"/>), and a no-op for zero because
    /// <see cref="SemaphoreSlim.Release(int)"/> rejects a zero count.
    /// </summary>
    private void ReleaseSpace(int count)
    {
        if (count > 0)
        {
            _space.Release(count);
        }
    }

    private static ObjectDisposedException ClosedDuringBackpressure() =>
        new ObjectDisposedException(
            nameof(NativeProducer),
            "The producer was closed while this send was waiting for accumulator space.");

    private void SendChain(Node? chain)
    {
        // One send_batch per chunk, walking the chain node by node (anchor :593 issues one call per
        // node). Never one call for the whole chain: chunking is ALSO what bounds how long the
        // core's coarse producer mutex is held in a single stretch (§3.4), so collapsing this loop
        // into a single flattened call would silently remove that cap.
        while (chain is not null)
        {
            Node node = chain;
            chain = node.Next;
            node.Next = null;
            SendNode(node);
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
    /// is nine arrays of by-value slots, so allocating one per drain would charge the <em>caller</em>
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

                int chunk = _settings.BatchChunk;
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

            CompleteNode(node, count, ref settled);
        }
        catch (Exception exception)
        {
            FaultNode(node, settled, count, exception);
        }
    }

    /// <summary>
    /// Walks the per-record results: faults the immediate-error indices here and hands the rest to
    /// the completion pump — the anchor's compaction (<c>:600-621</c>), expressed as "settle in
    /// place" rather than "shift the survivors down", because .NET has no reason to compact arrays
    /// it is about to drop.
    /// </summary>
    /// <remarks>
    /// <b>This is the phase's new delivery-callback firing site (§6.1).</b> An immediate error is a
    /// core completion — the core reported that it would not take this record — so the callback is
    /// owed exactly once here, and that index deliberately does not also reach the pump.
    /// </remarks>
    private void CompleteNode(Node node, int count, ref int settled)
    {
        for (; settled < count; settled++)
        {
            int i = settled;
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
                // Accepted: ownership of the future transfers to the pump, which reads its
                // completion, fires the delivery callback and completes the awaiter.
                //
                // The slot is nulled ONLY AFTER Enqueue returns. If it threw (its queue growing
                // under out-of-memory — its own _stopped branch frees the future and returns
                // normally), ownership never transferred, so FaultNode must still see a live future
                // at this index and free it. Nulling first would make that index look like "the
                // core never saw this record", and FaultNode would then fire a delivery callback the
                // pump is also about to fire — a DUPLICATE, which the exactly-once obligation makes
                // strictly worse than the drop (root CLAUDE.md §9.5).
                _pump.Enqueue(future, completion, delivery);
                node.Futures[i] = IntPtr.Zero;
            }

            // Settled: drop the references so a node held anywhere does not retain user objects.
            node.Completions[i] = null;
            node.Deliveries[i] = null;
        }
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
    /// differs. The anchor's arrays are fixed because a C struct has no other option, and its ten
    /// slots are five <em>pointers</em> — 44 KB per node. .NET's slots are by-value
    /// (<see cref="SerializedProducerRecord"/>, <see cref="ProducerRecordNative"/>, two
    /// <see cref="MemoryHandle"/>s, a <see cref="PinnedTopicCache.TopicPin"/>), so the same shape
    /// costs ~232 B per slot — <b>~254 KB per node, roughly 6× the anchor's</b>. A node is allocated
    /// per drain once the previous chain is taken, so at a low send rate (a handful of records per
    /// window) a fixed node would turn ~100 drains/second into ~25 MB/second of garbage for a
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
        /// the chain. Grows all nine together or not at all: a partial growth would leave the
        /// parallel arrays at different lengths, and the index that walks them assumes they match.
        /// </summary>
        internal void EnsureSlot()
        {
            int required = Count + 1;

            // Gate on the LAST array resized below, not the first: an out-of-memory part way through
            // the sequence leaves the earlier arrays longer than the later ones, and gating on
            // `Records` would then wave through a write that indexes past `Errors`. Errors reaching
            // `required` means all nine did.
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
