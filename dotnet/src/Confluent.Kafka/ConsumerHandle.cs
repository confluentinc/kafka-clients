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
using System.Threading;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka;

/// <summary>
/// A reentrancy handle onto a consumer — the sanctioned way to call back <b>into</b> a
/// consumer from <b>inside</b> one of its own callbacks (an
/// <see cref="IConsumerRebalanceListener"/>, an <see cref="IOffsetCommitCallback"/>).
/// Obtain one from <see cref="IConsumerCommon.Handle"/> and dispose it when done —
/// <b>before</b> the consumer.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why this type exists (DoD #7 — it is not a Java type).</b> Java needs no equivalent:
/// its callbacks run on the polling thread and <c>KafkaConsumer.acquire()</c> is reentrant
/// there, so a listener simply captures the <c>consumer</c> variable and calls
/// <c>consumer.commitSync()</c> from <c>onPartitionsRevoked</c>
/// (<c>consumer-threading.md</c> §31). The C ABI cannot flatten that: its single-owner access
/// guard is held for the whole operation that fired the callback, so the consumer's own API is
/// rejected with <c>ConcurrentModification</c> from inside one. This handle is the binding
/// scaffolding that <b>restores the Java behavior</b> — it deliberately takes no guard
/// (<c>src/ffi/consumer_handle.rs:29-37</c>) — rather than new API surface. Python has the
/// identical type for the identical reason (<c>bindings/python/consumer.py:378-545</c>).
/// </para>
/// <para>
/// <b>Lifetime — this handle keeps its consumer alive.</b> The ABI requires every handle to be
/// destroyed <b>before</b> its consumer, and .NET cannot force user ordering, so the handle
/// holds a reference count on the consumer for its whole life. A live handle therefore
/// <b>defers</b> the consumer's native destroy rather than dangling a pointer into freed
/// memory. The consequence to know about: <b>a handle you never dispose defers the consumer's
/// native teardown indefinitely</b> — always <c>using</c> it, or dispose it explicitly.
/// Disposing the consumer while a handle is outstanding does not hang and does not throw; the
/// native teardown simply completes when the last handle goes.
/// </para>
/// <para>
/// <b>Method surface — deliberately smaller than the consumer's.</b> There is no <c>Poll</c>,
/// <c>Subscribe</c>, <c>Unsubscribe</c> or <c>Close</c>: "Java never invokes those reentrantly
/// from a callback" (<c>src/ffi/consumer_handle.rs:76-80</c>). There is also no
/// callback-taking commit — register an <see cref="IOffsetCommitCallback"/> on the owning
/// consumer instead. Unlike the consumer's own <see cref="IConsumer{TKey, TValue}.Assign"/>, an
/// <b>empty</b> <see cref="Assign"/> here is rejected: on the consumer that leaves the group,
/// which a reentrancy handle does not expose.
/// </para>
/// <para>
/// <b>On a mock-derived handle</b> <see cref="Wakeup"/> and the three getters work (the getters
/// return <b>empty</b>, never null), while every other operation fails with a
/// <see cref="KafkaException"/> carrying the <c>UnsupportedVersion</c> code — the mock has no
/// event pipeline. That is documented <em>core</em> behavior, not a binding limitation.
/// </para>
/// <para>
/// <b>Threading.</b> Every operation is synchronous and runs to completion on the calling
/// thread. It is safe to call from the callback thread and from any ordinary thread; calling
/// one from inside the core's own async runtime fails fast with a
/// <see cref="KafkaException"/> rather than deadlocking or crashing the process. That
/// failure takes the ordinary operational-error route, like every other error this type
/// reports — it is <b>not</b> an <see cref="InvalidOperationException"/>, and it carries the
/// same flat shape as the consumer's own errors for the same conditions.
/// </para>
/// </remarks>
public sealed class ConsumerHandle : IDisposable
{
    // IDisposable ONLY — deliberately NOT IAsyncDisposable, which every other disposable in
    // this binding (KafkaConsumer / AsyncKafkaConsumer / MockConsumer / AsyncMockConsumer)
    // implements (P8-D4). The asymmetry is honest, not an oversight: every one of this type's
    // operations is a SYNCHRONOUS C call, so a DisposeAsync would wrap a sync destroy in a
    // completed ValueTask and imply an async surface that does not exist. Python's handle is a
    // plain context manager for the same reason. The consumers implement both because they own
    // a genuinely async close (Consumer_close_async); this owns only ConsumerHandle_destroy.
    private readonly NativeConsumerHandle _native;

    // 0 = live, 1 = disposed. Interlocked so Dispose is idempotent and safe under a concurrent
    // double-dispose — the ListenerRegistration.Release pattern.
    private int _disposed;

    private ConsumerHandle(NativeConsumerHandle native)
    {
        _native = native;
    }

    /// <summary>
    /// Creates a handle over <paramref name="consumer"/>, taking the consumer ref-count that
    /// keeps it alive for this handle's lifetime.
    /// </summary>
    internal static ConsumerHandle Create(SafeConsumerHandle consumer) =>
        new ConsumerHandle(NativeConsumerHandle.Create(consumer));

    /// <summary>
    /// The inner reentrancy <c>SafeHandle</c> — test-only, for grading the P8-D1 release-site
    /// ordering (see <see cref="NativeConsumerHandle.NativeHandle"/>).
    /// </summary>
    internal SafeConsumerReentrancyHandle NativeHandle => _native.NativeHandle;

    /// <summary>
    /// Wakes up the owning consumer, exactly like <see cref="IConsumerCommon.Wakeup"/>. Neither
    /// blocks nor takes the access guard, and works on a mock-derived handle.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    public void Wakeup()
    {
        ThrowIfDisposed();
        _native.Wakeup();
    }

    /// <summary>
    /// Returns the owning consumer's current assignment (Java <c>assignment()</c>).
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="IConsumerCommon.Assignment"/> this <b>never</b> throws
    /// <see cref="InvalidOperationException"/> for concurrent access — the ABI documents the
    /// result as non-null precisely because the handle takes no guard. That asymmetry is the
    /// whole point of the type. Always empty on a mock-derived handle.
    /// </remarks>
    /// <returns>An owned, immutable snapshot.</returns>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    public IReadOnlyCollection<TopicPartition> Assignment()
    {
        ThrowIfDisposed();
        return _native.Assignment();
    }

    /// <summary>
    /// Returns the owning consumer's current topic subscription (Java <c>subscription()</c>).
    /// Non-null; always empty on a mock-derived handle.
    /// </summary>
    /// <returns>An owned, immutable snapshot.</returns>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    public IReadOnlyCollection<string> Subscription()
    {
        ThrowIfDisposed();
        return _native.Subscription();
    }

    /// <summary>
    /// Returns the owning consumer's paused partitions (Java <c>paused()</c>). Non-null;
    /// always empty on a mock-derived handle.
    /// </summary>
    /// <returns>An owned, immutable snapshot.</returns>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    public IReadOnlyCollection<TopicPartition> Paused()
    {
        ThrowIfDisposed();
        return _native.Paused();
    }

    /// <summary>
    /// Assigns the owning consumer to <paramref name="partitions"/> (Java
    /// <c>assign(Collection)</c>).
    /// </summary>
    /// <remarks>
    /// An <b>empty</b> collection is <b>rejected</b> with a <see cref="KafkaException"/>,
    /// where <see cref="IConsumer{TKey, TValue}.Assign"/> accepts it: on the owning consumer
    /// <c>assign([])</c> leaves the group, which a reentrancy handle deliberately does not
    /// expose (<c>consumer-threading.md</c> §31).
    /// </remarks>
    /// <param name="partitions">The new full assignment (non-empty).</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    /// <exception cref="KafkaException">The core rejected the assignment (including an empty one).</exception>
    public void Assign(IReadOnlyCollection<TopicPartition> partitions)
    {
        ThrowIfDisposed();
        _native.Assign(partitions);
    }

    /// <summary>
    /// Pauses fetching for <paramref name="partitions"/> (Java <c>pause(Collection)</c>).
    /// </summary>
    /// <param name="partitions">The partitions to pause.</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    /// <exception cref="KafkaException">The core reported a failure (e.g. an unassigned partition).</exception>
    public void Pause(IReadOnlyCollection<TopicPartition> partitions)
    {
        ThrowIfDisposed();
        _native.Pause(partitions);
    }

    /// <summary>
    /// Resumes fetching for <paramref name="partitions"/> (Java <c>resume(Collection)</c>).
    /// </summary>
    /// <param name="partitions">The partitions to resume.</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    /// <exception cref="KafkaException">The core reported a failure (e.g. an unassigned partition).</exception>
    public void Resume(IReadOnlyCollection<TopicPartition> partitions)
    {
        ThrowIfDisposed();
        _native.Resume(partitions);
    }

    /// <summary>
    /// Requests an EARLIEST offset reset for <paramref name="partitions"/> (Java
    /// <c>seekToBeginning(Collection)</c>).
    /// </summary>
    /// <param name="partitions">The partitions to reset.</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    /// <exception cref="KafkaException">The core reported a failure.</exception>
    public void SeekToBeginning(IReadOnlyCollection<TopicPartition> partitions)
    {
        ThrowIfDisposed();
        _native.SeekToBeginning(partitions);
    }

    /// <summary>
    /// Requests a LATEST offset reset for <paramref name="partitions"/> (Java
    /// <c>seekToEnd(Collection)</c>).
    /// </summary>
    /// <param name="partitions">The partitions to reset.</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    /// <exception cref="KafkaException">The core reported a failure.</exception>
    public void SeekToEnd(IReadOnlyCollection<TopicPartition> partitions)
    {
        ThrowIfDisposed();
        _native.SeekToEnd(partitions);
    }

    /// <summary>
    /// Seeks <paramref name="partition"/> to <paramref name="offset"/> (Java
    /// <c>seek(TopicPartition, long)</c>).
    /// </summary>
    /// <param name="partition">The topic-partition to seek.</param>
    /// <param name="offset">The offset to seek to (non-negative).</param>
    /// <exception cref="ArgumentNullException"><paramref name="partition"/>'s topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="partition"/>'s partition is negative, or <paramref name="offset"/> is
    /// negative (Java: <c>"seek offset must not be a negative number"</c>).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    /// <exception cref="KafkaException">The core reported a seek failure.</exception>
    public void Seek(TopicPartition partition, long offset)
    {
        ThrowIfDisposed();
        _native.Seek(partition, offset);
    }

    /// <summary>
    /// Seeks <paramref name="partition"/> to <paramref name="offsetAndMetadata"/>, carrying its
    /// commit metadata and leader epoch (Java <c>seek(TopicPartition, OffsetAndMetadata)</c>).
    /// </summary>
    /// <param name="partition">The topic-partition to seek.</param>
    /// <param name="offsetAndMetadata">The offset (with metadata / leader epoch) to seek to.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="partition"/>'s topic or <paramref name="offsetAndMetadata"/> is null.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/>'s partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    /// <exception cref="KafkaException">The core reported a seek failure.</exception>
    public void Seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata)
    {
        ThrowIfDisposed();
        _native.Seek(partition, offsetAndMetadata);
    }

    /// <summary>
    /// Returns the owning consumer's current position for <paramref name="partition"/>, using
    /// its <c>default.api.timeout.ms</c> (Java <c>position(TopicPartition)</c>).
    /// </summary>
    /// <param name="partition">The topic-partition to query.</param>
    /// <returns>The next offset to be fetched.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partition"/>'s topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/>'s partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    /// <exception cref="KafkaException">The core reported a position failure.</exception>
    public long Position(TopicPartition partition)
    {
        ThrowIfDisposed();
        return _native.Position(partition);
    }

    /// <summary>
    /// The timeout-bounded twin of <see cref="Position(TopicPartition)"/> (Java
    /// <c>position(TopicPartition, Duration)</c>), mirroring the consumer's own overload pair.
    /// </summary>
    /// <param name="partition">The topic-partition to query.</param>
    /// <param name="timeout">The maximum time to wait.</param>
    /// <returns>The next offset to be fetched.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partition"/>'s topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="partition"/>'s partition is negative, or <paramref name="timeout"/> is negative.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    /// <exception cref="KafkaException">The core reported a position failure.</exception>
    public long Position(TopicPartition partition, TimeSpan timeout)
    {
        if (timeout < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(timeout), timeout, "Timeout must not be negative.");
        }

        ThrowIfDisposed();
        return _native.Position(partition, (long)timeout.TotalMilliseconds);
    }

    /// <summary>
    /// Returns the last committed offset for each of <paramref name="partitions"/> (Java
    /// <c>committed(Set&lt;TopicPartition&gt;)</c>). Partitions with no commit are omitted.
    /// </summary>
    /// <param name="partitions">The partitions to query.</param>
    /// <returns>An owned, immutable snapshot.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    /// <exception cref="KafkaException">The core reported a committed-query failure.</exception>
    public IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> Committed(
        IReadOnlyCollection<TopicPartition> partitions)
    {
        ThrowIfDisposed();
        return _native.Committed(partitions);
    }

    /// <summary>
    /// Returns the earliest available offset for each of <paramref name="partitions"/> (Java
    /// <c>beginningOffsets(Collection)</c>).
    /// </summary>
    /// <param name="partitions">The partitions to query.</param>
    /// <returns>An owned, immutable snapshot.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    /// <exception cref="KafkaException">The core reported a beginning-offsets failure.</exception>
    public IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(
        IReadOnlyCollection<TopicPartition> partitions)
    {
        ThrowIfDisposed();
        return _native.BeginningOffsets(partitions);
    }

    /// <summary>
    /// Returns the latest offset for each of <paramref name="partitions"/> (Java
    /// <c>endOffsets(Collection)</c>).
    /// </summary>
    /// <param name="partitions">The partitions to query.</param>
    /// <returns>An owned, immutable snapshot.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    /// <exception cref="KafkaException">The core reported an end-offsets failure.</exception>
    public IReadOnlyDictionary<TopicPartition, long> EndOffsets(
        IReadOnlyCollection<TopicPartition> partitions)
    {
        ThrowIfDisposed();
        return _native.EndOffsets(partitions);
    }

    /// <summary>
    /// Looks up the earliest offset whose timestamp is at or after the given one, per partition
    /// (Java <c>offsetsForTimes(Map)</c>). Unresolved partitions are omitted.
    /// </summary>
    /// <param name="timestampsToSearch">The per-partition timestamps to search for.</param>
    /// <returns>An owned, immutable snapshot.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="timestampsToSearch"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    /// <exception cref="KafkaException">The core reported an offsets-for-times failure.</exception>
    public IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch)
    {
        ThrowIfDisposed();
        return _native.OffsetsForTimes(timestampsToSearch);
    }

    /// <summary>
    /// Commits the owning consumer's current positions and <b>waits</b> for the result (Java
    /// <c>commitSync()</c>) — the operation a rebalance listener calls to flush offsets before
    /// its partitions are taken away. Named to match the consumer's own <c>Commit</c>.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    /// <exception cref="KafkaException">The core reported a commit failure.</exception>
    public void Commit()
    {
        ThrowIfDisposed();
        _native.CommitSync();
    }

    /// <summary>
    /// Commits the specific <paramref name="offsets"/> and <b>waits</b> for the result (Java
    /// <c>commitSync(Map)</c>). An empty map commits nothing.
    /// </summary>
    /// <param name="offsets">The offsets to commit.</param>
    /// <exception cref="ArgumentNullException"><paramref name="offsets"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null, or a value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    /// <exception cref="KafkaException">The core reported a commit failure.</exception>
    public void Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets)
    {
        ThrowIfDisposed();
        _native.CommitSyncOffsets(offsets);
    }

    /// <summary>
    /// Commits the owning consumer's current positions <b>without waiting</b> (Java
    /// <c>commitAsync()</c>); returns once the commit has been initiated.
    /// </summary>
    /// <remarks>
    /// There is deliberately <b>no callback-taking overload</b> here, matching the core and
    /// Python: register an <see cref="IOffsetCommitCallback"/> on the owning consumer via
    /// <see cref="IConsumerCommon.CommitAsync(IOffsetCommitCallback)"/> instead.
    /// </remarks>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    /// <exception cref="KafkaException">The core rejected the commit before initiating it.</exception>
    public void CommitAsync()
    {
        ThrowIfDisposed();
        _native.CommitAsync();
    }

    /// <summary>
    /// Commits the specific <paramref name="offsets"/> <b>without waiting</b> (Java
    /// <c>commitAsync(Map)</c>). No callback-taking overload — see <see cref="CommitAsync()"/>.
    /// </summary>
    /// <param name="offsets">The offsets to commit.</param>
    /// <exception cref="ArgumentNullException"><paramref name="offsets"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null, or a value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The handle is disposed.</exception>
    /// <exception cref="KafkaException">The core rejected the commit before initiating it.</exception>
    public void CommitAsync(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets)
    {
        ThrowIfDisposed();
        _native.CommitAsyncOffsets(offsets);
    }

    /// <summary>
    /// Destroys the handle and releases the consumer reference it holds. <b>Idempotent</b> — a
    /// second call is a no-op — and safe under a concurrent double-dispose. Every subsequent
    /// operation throws <see cref="ObjectDisposedException"/>.
    /// </summary>
    /// <remarks>
    /// If this is the last reference to the owning consumer (the consumer was already
    /// disposed), the consumer's native teardown completes here, on the calling thread.
    /// </remarks>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _native.Dispose();
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0)
        {
            throw new ObjectDisposedException(nameof(ConsumerHandle));
        }
    }
}
