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
using System.Threading.Tasks;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;

namespace Confluent.Kafka.Consumer;

/// <summary>
/// The asynchronous Kafka consumer, Java's <c>org.apache.kafka.clients.consumer.Consumer&lt;K, V&gt;</c>,
/// implemented by <see cref="AsyncKafkaConsumer{TKey, TValue}"/> and the broker-free
/// <see cref="AsyncMockConsumer{TKey, TValue}"/>. <see cref="IConsumer{TKey, TValue}"/> is its synchronous
/// sibling. A method that blocks in Java returns a <see cref="Task"/>; the non-blocking members are on
/// <see cref="IConsumerCommon"/>. Method names mirror Java's, with no <c>Async</c> suffix.
/// </summary>
/// <remarks>
/// <para>
/// The consumer has one owner at a time, as Java's does. A call from another thread while a call is in
/// progress throws <see cref="InvalidOperationException"/> ("KafkaConsumer is not safe for multi-threaded
/// access."); <see cref="AsyncMockConsumer{TKey, TValue}"/> makes it wait instead. Calls awaited one after
/// another may resume on any thread. <see cref="IConsumerCommon.Wakeup"/> and
/// <see cref="IConsumerCommon.Handle"/> may be called from any thread, and a <see cref="IDisposable.Dispose"/>
/// from another thread wakes the call in progress, waits for it, and closes the consumer.
/// </para>
/// <para>
/// Rebalance listeners and commit callbacks run on the consumer's drain thread,
/// <c>confluent-kafka-consumer-pump</c>; listeners run only while <c>Poll</c>, <c>Unsubscribe</c> or
/// <c>Close</c> runs. What a callback may call on its own consumer is in
/// <see cref="IConsumerRebalanceListener"/>. Inside a callback a call routed to the consumer's own handle runs to its
/// end before its <see cref="Task"/> is returned: its cancellation token is checked only before it starts, and no
/// <c>wakeup()</c> is sent.
/// </para>
/// <para>
/// A cancellation token never cancels the operation. On <c>Poll</c>, <c>Commit</c>, <c>Position</c>,
/// <c>Committed</c>, <c>PartitionsFor</c> and <c>ListTopics</c>, the operations Java's <c>wakeup()</c>
/// interrupts, a cancel sends <c>wakeup()</c> so that the operation ends sooner; on the others it sends none.
/// Either way, once the token has fired the <see cref="Task"/> ends with
/// <see cref="OperationCanceledException"/> when the operation has ended. A token already cancelled at the
/// call throws <see cref="OperationCanceledException"/> before anything is sent.
/// </para>
/// <para>
/// <see cref="IAsyncDisposable.DisposeAsync"/> closes and then frees the consumer;
/// <see cref="IDisposable.Dispose"/> is its blocking form. Both swallow a close error and are idempotent.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The deserialized key type.</typeparam>
/// <typeparam name="TValue">The deserialized value type.</typeparam>
public interface IAsyncConsumer<TKey, TValue> : IConsumerCommon, IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Polls for records (Java <c>poll(Duration)</c>). An empty poll returns an empty
    /// <see cref="ConsumerRecords{TKey, TValue}"/>, not <see langword="null"/>. The <see cref="Task"/> faults with
    /// a <see cref="KafkaException"/> on failure, or with a <see cref="RecordDeserializationException"/> as
    /// <see cref="IConsumer{TKey, TValue}.Poll(TimeSpan)"/> throws one.
    /// </summary>
    /// <param name="timeout">The maximum time to wait (must be non-negative).</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: <c>wakeup()</c> is sent, and the <see cref="Task"/> ends with
    /// <see cref="OperationCanceledException"/> once the operation has ended.
    /// </param>
    /// <returns>The records polled, as an owned <see cref="ConsumerRecords{TKey, TValue}"/>.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    Task<ConsumerRecords<TKey, TValue>> Poll(TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>
    /// Subscribes to <paramref name="topics"/> (Java <c>subscribe(Collection)</c>).
    /// </summary>
    /// <param name="topics">The topics to subscribe to.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> is null.</exception>
    /// <exception cref="ArgumentException">A topic name is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    Task Subscribe(IReadOnlyCollection<string> topics, CancellationToken cancellationToken = default);

    /// <summary>
    /// Subscribes to <paramref name="topics"/> with a rebalance listener (Java
    /// <c>subscribe(Collection, ConsumerRebalanceListener)</c>). The listener fires on every later rebalance
    /// until a subscribe that succeeds replaces it.
    /// </summary>
    /// <remarks>
    /// A replacing subscribe releases the previous listener, a listener-less
    /// <see cref="Subscribe(IReadOnlyCollection{string}, CancellationToken)"/> included, as does disposing the
    /// consumer. <see cref="Unsubscribe"/> and <c>Close</c> keep it, as Java's
    /// <c>SubscriptionState.unsubscribe()</c> does. <see cref="IConsumerRebalanceListener"/> describes where the
    /// listener runs, what throwing does, and what it may call on its own consumer.
    /// </remarks>
    /// <param name="topics">The topics to subscribe to.</param>
    /// <param name="listener">The rebalance listener to register.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> or <paramref name="listener"/> is null.</exception>
    /// <exception cref="ArgumentException">A topic name is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    Task Subscribe(
        IReadOnlyCollection<string> topics,
        IConsumerRebalanceListener listener,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Subscribes to <paramref name="topics"/> with an asynchronous rebalance listener (Java
    /// <c>subscribe(Collection, ConsumerRebalanceListener)</c>). The listener fires on every later rebalance
    /// until a subscribe that succeeds replaces it.
    /// </summary>
    /// <param name="topics">The topics to subscribe to.</param>
    /// <param name="listener">The rebalance listener to register.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <returns>A <see cref="Task"/> that completes when the subscription is applied.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> or <paramref name="listener"/> is null.</exception>
    /// <exception cref="ArgumentException">A topic name is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    Task Subscribe(
        IReadOnlyCollection<string> topics,
        IAsyncConsumerRebalanceListener listener,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Unsubscribes from all topics and partitions (Java <c>unsubscribe()</c>).
    /// </summary>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    Task Unsubscribe(CancellationToken cancellationToken = default);

    /// <summary>
    /// Manually assigns <paramref name="partitions"/> to this consumer (Java <c>assign(Collection)</c>).
    /// </summary>
    /// <remarks>
    /// An empty collection clears the assignment, as Java's <c>assign</c> does; a <see langword="null"/> one is
    /// rejected.
    /// </remarks>
    /// <param name="partitions">The topic-partitions to assign (an empty collection clears the assignment).</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    Task Assign(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);

    /// <summary>
    /// Suspends fetching for <paramref name="partitions"/> (Java <c>pause(Collection)</c>); the paused set is
    /// read with <see cref="IConsumerCommon.Paused"/>.
    /// </summary>
    /// <remarks>An empty collection does nothing.</remarks>
    /// <param name="partitions">The topic-partitions to pause.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="NotSupportedException">
    /// The <see cref="Task"/> faults with it when called inside a callback of an
    /// <see cref="AsyncMockConsumer{TKey, TValue}"/>, whose handle does not support it.
    /// </exception>
    Task Pause(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);

    /// <summary>
    /// Resumes fetching for <paramref name="partitions"/> paused by <see cref="Pause"/> (Java
    /// <c>resume(Collection)</c>).
    /// </summary>
    /// <remarks>An empty collection does nothing.</remarks>
    /// <param name="partitions">The topic-partitions to resume.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="NotSupportedException">
    /// The <see cref="Task"/> faults with it when called inside a callback of an
    /// <see cref="AsyncMockConsumer{TKey, TValue}"/>, whose handle does not support it.
    /// </exception>
    Task Resume(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);

    /// <summary>
    /// Seeks <paramref name="partitions"/> to their first available offset (Java
    /// <c>seekToBeginning(Collection)</c>). The offset is resolved on the next <see cref="Poll"/>.
    /// </summary>
    /// <remarks>An empty collection does nothing.</remarks>
    /// <param name="partitions">The topic-partitions to seek to the beginning.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="NotSupportedException">
    /// The <see cref="Task"/> faults with it when called inside a callback of an
    /// <see cref="AsyncMockConsumer{TKey, TValue}"/>, whose handle does not support it.
    /// </exception>
    Task SeekToBeginning(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);

    /// <summary>
    /// Seeks <paramref name="partitions"/> to their last offset (Java <c>seekToEnd(Collection)</c>). The offset
    /// is resolved on the next <see cref="Poll"/>.
    /// </summary>
    /// <remarks>An empty collection does nothing.</remarks>
    /// <param name="partitions">The topic-partitions to seek to the end.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="NotSupportedException">
    /// The <see cref="Task"/> faults with it when called inside a callback of an
    /// <see cref="AsyncMockConsumer{TKey, TValue}"/>, whose handle does not support it.
    /// </exception>
    Task SeekToEnd(IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the offset of the next record that will be fetched from <paramref name="partition"/> (Java
    /// <c>position(TopicPartition)</c>). For a partition not assigned to the consumer the <see cref="Task"/>
    /// faults with <see cref="ArgumentException"/>.
    /// </summary>
    /// <param name="partition">The topic-partition whose position to read.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: <c>wakeup()</c> is sent, and the <see cref="Task"/> ends with
    /// <see cref="OperationCanceledException"/> once the operation has ended.
    /// </param>
    /// <returns>The current offset position of <paramref name="partition"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partition"/>'s topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/>'s partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="NotSupportedException">
    /// The <see cref="Task"/> faults with it when called inside a callback of an
    /// <see cref="AsyncMockConsumer{TKey, TValue}"/>, whose handle does not support it.
    /// </exception>
    Task<long> Position(TopicPartition partition, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the current offset position of <paramref name="partition"/>, waiting at most
    /// <paramref name="timeout"/> (Java <c>position(TopicPartition, Duration)</c>).
    /// </summary>
    /// <param name="partition">The topic-partition whose position to read.</param>
    /// <param name="timeout">The maximum time to wait.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: <c>wakeup()</c> is sent, and the <see cref="Task"/> ends with
    /// <see cref="OperationCanceledException"/> once the operation has ended.
    /// </param>
    /// <returns>The current offset position of <paramref name="partition"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partition"/>'s topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="partition"/>'s partition is negative, or <paramref name="timeout"/> is negative
    /// ("Invalid negative timeout &lt;ms&gt;").
    /// </exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="NotSupportedException">
    /// The <see cref="Task"/> faults with it when called inside a callback of an
    /// <see cref="AsyncMockConsumer{TKey, TValue}"/>, whose handle does not support it.
    /// </exception>
    Task<long> Position(TopicPartition partition, TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits the current fetch positions for all assigned partitions and waits for the commit (Java
    /// <c>commitSync()</c>); the <see cref="Task"/> faults with a <see cref="KafkaException"/> if it fails.
    /// <see cref="CommitAsync(CancellationToken)"/> is Java's <c>commitAsync()</c>, which does not wait.
    /// </summary>
    /// <param name="cancellationToken">
    /// Cancels the wait: <c>wakeup()</c> is sent, and the <see cref="Task"/> ends with
    /// <see cref="OperationCanceledException"/> once the operation has ended.
    /// </param>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    Task Commit(CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits the current fetch positions, waiting at most <paramref name="timeout"/> (Java
    /// <c>commitSync(Duration)</c>).
    /// </summary>
    /// <param name="timeout">The maximum time to wait.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: <c>wakeup()</c> is sent, and the <see cref="Task"/> ends with
    /// <see cref="OperationCanceledException"/> once the operation has ended.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="timeout"/> is negative ("Invalid negative timeout &lt;ms&gt;").
    /// </exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="KafkaException">
    /// An <see cref="AsyncKafkaConsumer{TKey, TValue}"/> without <c>group.id</c> throws code 24
    /// (<c>INVALID_GROUP_ID</c>) at once for a negative <paramref name="timeout"/>, before the timeout check. The
    /// <see cref="Task"/> faults with code 7 when the commit did not complete within <paramref name="timeout"/>.
    /// </exception>
    Task Commit(TimeSpan timeout, CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits <paramref name="offsets"/> and waits for the commit (Java
    /// <c>commitSync(Map&lt;TopicPartition, OffsetAndMetadata&gt;)</c>); the <see cref="Task"/> faults with a
    /// <see cref="KafkaException"/> if it fails. An empty map commits nothing.
    /// </summary>
    /// <param name="offsets">The offsets to commit, keyed by topic-partition.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: <c>wakeup()</c> is sent, and the <see cref="Task"/> ends with
    /// <see cref="OperationCanceledException"/> once the operation has ended.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="offsets"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null, or a value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    Task Commit(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits the specific <paramref name="offsets"/>, waiting at most <paramref name="timeout"/> (Java
    /// <c>commitSync(Map&lt;TopicPartition, OffsetAndMetadata&gt;, Duration)</c>).
    /// </summary>
    /// <param name="offsets">The offsets to commit, keyed by topic-partition.</param>
    /// <param name="timeout">The maximum time to wait.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: <c>wakeup()</c> is sent, and the <see cref="Task"/> ends with
    /// <see cref="OperationCanceledException"/> once the operation has ended.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="offsets"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null, or a value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// A key partition is negative, or <paramref name="timeout"/> is negative ("Invalid negative timeout &lt;ms&gt;").
    /// </exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="KafkaException">
    /// An <see cref="AsyncKafkaConsumer{TKey, TValue}"/> without <c>group.id</c> throws code 24
    /// (<c>INVALID_GROUP_ID</c>) at once for a negative <paramref name="timeout"/>, before the timeout check. The
    /// <see cref="Task"/> faults with code 7 when the commit did not complete within <paramref name="timeout"/>.
    /// </exception>
    Task Commit(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        TimeSpan timeout,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits the current fetch positions without waiting for the commit (Java <c>commitAsync()</c>): the
    /// <see cref="Task"/> completes once the core has accepted the commit.
    /// </summary>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    Task CommitAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits the current fetch positions without waiting for the commit, and runs <paramref name="callback"/>
    /// with its outcome (Java <c>commitAsync(OffsetCommitCallback)</c>): the <see cref="Task"/> completes once the
    /// core has accepted the commit.
    /// </summary>
    /// <remarks>
    /// The callback runs on the consumer's drain thread. A faulted <see cref="Task"/> means the commit never
    /// started; the commit's own outcome reaches <paramref name="callback"/>. A <see langword="null"/> callback,
    /// which Java accepts, is rejected: <see cref="CommitAsync(CancellationToken)"/> commits without one.
    /// </remarks>
    /// <param name="callback">The callback that receives the commit's outcome.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="callback"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    Task CommitAsync(IOffsetCommitCallback callback, CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits the specific <paramref name="offsets"/> without waiting for the commit, and runs
    /// <paramref name="callback"/>, if any, with its outcome (Java
    /// <c>commitAsync(Map&lt;TopicPartition, OffsetAndMetadata&gt;, OffsetCommitCallback)</c>): the
    /// <see cref="Task"/> completes once the core has accepted the commit.
    /// </summary>
    /// <remarks>
    /// The callback runs as for <see cref="CommitAsync(IOffsetCommitCallback, CancellationToken)"/>. An empty
    /// <paramref name="offsets"/> commits nothing; the callback then receives <see langword="null"/> offsets, or the
    /// empty map on <see cref="AsyncMockConsumer{TKey, TValue}"/>, as Java's <c>MockConsumer</c> passes it.
    /// </remarks>
    /// <param name="offsets">The offsets to commit, keyed by topic-partition.</param>
    /// <param name="callback">The callback that receives the commit's outcome, or <see langword="null"/> for none.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="offsets"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null, or a value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    Task CommitAsync(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        IOffsetCommitCallback? callback = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits the current fetch positions without waiting for the commit, and runs the asynchronous
    /// <paramref name="callback"/> with its outcome (Java <c>commitAsync(OffsetCommitCallback)</c>, whose callback
    /// Java has only in its synchronous form): the <see cref="Task"/> completes once the core has accepted the
    /// commit.
    /// </summary>
    /// <remarks>
    /// The callback is called on the consumer's drain thread, which does not wait for its <see cref="Task"/>; the
    /// call that delivers it waits for that <see cref="Task"/> (<see cref="IAsyncOffsetCommitCallback"/>). A faulted
    /// <see cref="Task"/> from this method means the commit never started; the commit's own outcome reaches
    /// <paramref name="callback"/>.
    /// </remarks>
    /// <param name="callback">The callback that receives the commit's outcome.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="callback"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    Task CommitAsync(IAsyncOffsetCommitCallback callback, CancellationToken cancellationToken = default);

    /// <summary>
    /// Commits the specific <paramref name="offsets"/> without waiting for the commit, and runs the asynchronous
    /// <paramref name="callback"/> with its outcome (Java
    /// <c>commitAsync(Map&lt;TopicPartition, OffsetAndMetadata&gt;, OffsetCommitCallback)</c>, whose callback Java
    /// has only in its synchronous form): the <see cref="Task"/> completes once the core has accepted the commit.
    /// </summary>
    /// <remarks>
    /// The callback runs as for <see cref="CommitAsync(IAsyncOffsetCommitCallback, CancellationToken)"/>. An empty
    /// <paramref name="offsets"/> commits nothing; the callback then receives <see langword="null"/> offsets, or the
    /// empty map on <see cref="AsyncMockConsumer{TKey, TValue}"/>, as Java's <c>MockConsumer</c> passes it. With a
    /// <see langword="null"/> literal for the callback the call is ambiguous: write
    /// <c>CommitAsync(offsets)</c> to commit without one.
    /// </remarks>
    /// <param name="offsets">The offsets to commit, keyed by topic-partition.</param>
    /// <param name="callback">The callback that receives the commit's outcome.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="offsets"/> or <paramref name="callback"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null, or a value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    Task CommitAsync(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        IAsyncOffsetCommitCallback callback,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Seeks <paramref name="partition"/> to <paramref name="offset"/> (Java <c>seek(TopicPartition, long)</c>);
    /// the <see cref="Task"/> completes once the seek is applied.
    /// </summary>
    /// <param name="partition">The topic-partition to seek.</param>
    /// <param name="offset">The offset to seek to.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="offset"/> is negative ("seek offset must not be a negative number").
    /// </exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="NotSupportedException">
    /// The <see cref="Task"/> faults with it when called inside a callback of an
    /// <see cref="AsyncMockConsumer{TKey, TValue}"/>, whose handle does not support it.
    /// </exception>
    Task Seek(TopicPartition partition, long offset, CancellationToken cancellationToken = default);

    /// <summary>
    /// Seeks <paramref name="partition"/> to the offset, leader epoch and metadata of
    /// <paramref name="offsetAndMetadata"/> (Java <c>seek(TopicPartition, OffsetAndMetadata)</c>); the
    /// <see cref="Task"/> completes once the seek is applied.
    /// </summary>
    /// <param name="partition">The topic-partition to seek.</param>
    /// <param name="offsetAndMetadata">The offset to seek to.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="offsetAndMetadata"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="NotSupportedException">
    /// The <see cref="Task"/> faults with it when called inside a callback of an
    /// <see cref="AsyncMockConsumer{TKey, TValue}"/>, whose handle does not support it.
    /// </exception>
    Task Seek(
        TopicPartition partition,
        OffsetAndMetadata offsetAndMetadata,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the consumer's lag for <paramref name="partition"/> (Java <c>currentLag(TopicPartition)</c>), or
    /// <see langword="null"/> when it is not known.
    /// </summary>
    /// <param name="partition">The topic-partition whose lag to read.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <returns>The current lag, or <see langword="null"/> when it is not known.</returns>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    Task<long?> CurrentLag(TopicPartition partition, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the last committed offset, with its metadata and leader epoch, for each of
    /// <paramref name="partitions"/> (Java <c>committed(Set&lt;TopicPartition&gt;)</c>). Every requested partition
    /// is in the result, <see langword="null"/> where it has no committed offset;
    /// <see cref="AsyncMockConsumer{TKey, TValue}"/>, like Java's <c>MockConsumer</c>, leaves such a partition out.
    /// </summary>
    /// <param name="partitions">The topic-partitions whose committed offsets to read.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: <c>wakeup()</c> is sent, and the <see cref="Task"/> ends with
    /// <see cref="OperationCanceledException"/> once the operation has ended.
    /// </param>
    /// <returns>The committed offsets, keyed by topic-partition, <see langword="null"/> where a partition has none.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="KafkaException">
    /// An <see cref="AsyncKafkaConsumer{TKey, TValue}"/> without <c>group.id</c> throws code 24
    /// (<c>INVALID_GROUP_ID</c>) at once for a null <paramref name="partitions"/>, before the null check.
    /// </exception>
    /// <exception cref="NotSupportedException">
    /// The <see cref="Task"/> faults with it when called inside a callback of an
    /// <see cref="AsyncMockConsumer{TKey, TValue}"/>, whose handle does not support it.
    /// </exception>
    Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> Committed(
        IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the last committed offset for each of <paramref name="partitions"/>, waiting at most
    /// <paramref name="timeout"/> (Java <c>committed(Set&lt;TopicPartition&gt;, Duration)</c>).
    /// </summary>
    /// <param name="partitions">The topic-partitions whose committed offsets to read.</param>
    /// <param name="timeout">The maximum time to wait.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: <c>wakeup()</c> is sent, and the <see cref="Task"/> ends with
    /// <see cref="OperationCanceledException"/> once the operation has ended.
    /// </param>
    /// <returns>The committed offsets, keyed by topic-partition, <see langword="null"/> where a partition has none.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// An element partition is negative, or <paramref name="timeout"/> is negative.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="KafkaException">
    /// An <see cref="AsyncKafkaConsumer{TKey, TValue}"/> without <c>group.id</c> throws code 24
    /// (<c>INVALID_GROUP_ID</c>) at once for a negative <paramref name="timeout"/> or a null
    /// <paramref name="partitions"/>, before those checks. The <see cref="Task"/> faults with code 7 when the query
    /// did not complete within <paramref name="timeout"/>.
    /// </exception>
    /// <exception cref="NotSupportedException">
    /// The <see cref="Task"/> faults with it when called inside a callback of an
    /// <see cref="AsyncMockConsumer{TKey, TValue}"/>, whose handle does not support it.
    /// </exception>
    Task<IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?>> Committed(
        IReadOnlyCollection<TopicPartition> partitions,
        TimeSpan timeout,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Looks up, for each entry in <paramref name="timestampsToSearch"/>, the offset of the earliest record
    /// whose timestamp is at least the given one (Java <c>offsetsForTimes(Map&lt;TopicPartition, Long&gt;)</c>).
    /// Every requested partition is in the result, <see langword="null"/> where no offset was found.
    /// </summary>
    /// <remarks>
    /// A negative timestamp is passed through, not rejected: Kafka uses negative special timestamps (earliest,
    /// latest).
    /// </remarks>
    /// <param name="timestampsToSearch">The target timestamp (ms since epoch) per topic-partition.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <returns>The resolved offset and timestamp per topic-partition, <see langword="null"/> where there is none.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="timestampsToSearch"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="NotSupportedException">
    /// The <see cref="Task"/> faults with it on an <see cref="AsyncMockConsumer{TKey, TValue}"/>, which does not
    /// implement it.
    /// </exception>
    Task<IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?>> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch, CancellationToken cancellationToken = default);

    /// <summary>
    /// Looks up, for each entry in <paramref name="timestampsToSearch"/>, the offset of the earliest record
    /// whose timestamp is at least the given one, waiting at most <paramref name="timeout"/> (Java
    /// <c>offsetsForTimes(Map&lt;TopicPartition, Long&gt;, Duration)</c>).
    /// </summary>
    /// <param name="timestampsToSearch">The target timestamp (ms since epoch) per topic-partition.</param>
    /// <param name="timeout">The maximum time to wait.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <returns>The resolved offset and timestamp per topic-partition, <see langword="null"/> where there is none.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="timestampsToSearch"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative, or <paramref name="timeout"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="NotSupportedException">
    /// The <see cref="Task"/> faults with it on an <see cref="AsyncMockConsumer{TKey, TValue}"/>, which does not
    /// implement it.
    /// </exception>
    Task<IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?>> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch,
        TimeSpan timeout,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the earliest available offset for each of <paramref name="partitions"/>
    /// (Java <c>beginningOffsets(Collection&lt;TopicPartition&gt;)</c>).
    /// </summary>
    /// <param name="partitions">The topic-partitions whose beginning offsets to read.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <returns>The earliest offset per topic-partition.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="NotSupportedException">
    /// The <see cref="Task"/> faults with it when called inside a callback of an
    /// <see cref="AsyncMockConsumer{TKey, TValue}"/>, whose handle does not support it.
    /// </exception>
    Task<IReadOnlyDictionary<TopicPartition, long>> BeginningOffsets(
        IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the earliest available offset for each of <paramref name="partitions"/>, waiting at most
    /// <paramref name="timeout"/> (Java <c>beginningOffsets(Collection&lt;TopicPartition&gt;, Duration)</c>).
    /// </summary>
    /// <param name="partitions">The topic-partitions to read.</param>
    /// <param name="timeout">The maximum time to wait.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <returns>The earliest offset per topic-partition.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// An element partition is negative, or <paramref name="timeout"/> is negative.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="NotSupportedException">
    /// The <see cref="Task"/> faults with it when called inside a callback of an
    /// <see cref="AsyncMockConsumer{TKey, TValue}"/>, whose handle does not support it.
    /// </exception>
    Task<IReadOnlyDictionary<TopicPartition, long>> BeginningOffsets(
        IReadOnlyCollection<TopicPartition> partitions,
        TimeSpan timeout,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the end offset (the offset of the next record to be written) for each of
    /// <paramref name="partitions"/> (Java <c>endOffsets(Collection&lt;TopicPartition&gt;)</c>).
    /// </summary>
    /// <param name="partitions">The topic-partitions whose end offsets to read.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <returns>The latest offset per topic-partition.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="NotSupportedException">
    /// The <see cref="Task"/> faults with it when called inside a callback of an
    /// <see cref="AsyncMockConsumer{TKey, TValue}"/>, whose handle does not support it.
    /// </exception>
    Task<IReadOnlyDictionary<TopicPartition, long>> EndOffsets(
        IReadOnlyCollection<TopicPartition> partitions, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the end offset for each of <paramref name="partitions"/>, waiting at most
    /// <paramref name="timeout"/> (Java <c>endOffsets(Collection&lt;TopicPartition&gt;, Duration)</c>).
    /// </summary>
    /// <param name="partitions">The topic-partitions to read.</param>
    /// <param name="timeout">The maximum time to wait.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: the <see cref="Task"/> ends with <see cref="OperationCanceledException"/> once the
    /// operation has ended; no <c>wakeup()</c> is sent.
    /// </param>
    /// <returns>The latest offset per topic-partition.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// An element partition is negative, or <paramref name="timeout"/> is negative.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="NotSupportedException">
    /// The <see cref="Task"/> faults with it when called inside a callback of an
    /// <see cref="AsyncMockConsumer{TKey, TValue}"/>, whose handle does not support it.
    /// </exception>
    Task<IReadOnlyDictionary<TopicPartition, long>> EndOffsets(
        IReadOnlyCollection<TopicPartition> partitions,
        TimeSpan timeout,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the partition metadata for <paramref name="topic"/> (Java <c>partitionsFor(String)</c>), an empty
    /// list for a topic with no known partitions.
    /// </summary>
    /// <remarks>
    /// An empty <paramref name="topic"/> is passed to the core, not rejected, as in Java; only a
    /// <see langword="null"/> one throws.
    /// </remarks>
    /// <param name="topic">The topic whose partition metadata to read.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: <c>wakeup()</c> is sent, and the <see cref="Task"/> ends with
    /// <see cref="OperationCanceledException"/> once the operation has ended.
    /// </param>
    /// <returns>The partition metadata for <paramref name="topic"/>, as an owned list.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="NotSupportedException">
    /// The <see cref="Task"/> faults with it when called inside a callback of an
    /// <see cref="AsyncMockConsumer{TKey, TValue}"/>, whose handle does not support it.
    /// </exception>
    Task<IReadOnlyList<PartitionInfo>> PartitionsFor(string topic, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the partition metadata for <paramref name="topic"/>, waiting at most <paramref name="timeout"/>
    /// (Java <c>partitionsFor(String, Duration)</c>).
    /// </summary>
    /// <param name="topic">The topic whose partition metadata to read.</param>
    /// <param name="timeout">The maximum time to wait.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: <c>wakeup()</c> is sent, and the <see cref="Task"/> ends with
    /// <see cref="OperationCanceledException"/> once the operation has ended.
    /// </param>
    /// <returns>The partition metadata for <paramref name="topic"/>, as an owned list.</returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="topic"/> is null, unless the consumer is an <see cref="AsyncKafkaConsumer{TKey, TValue}"/> and
    /// <paramref name="timeout"/> is zero.
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">
    /// On an <see cref="AsyncKafkaConsumer{TKey, TValue}"/> a null <paramref name="topic"/> with a zero
    /// <paramref name="timeout"/> throws code 7 at once, with an empty message. The <see cref="Task"/> faults with
    /// code 7 when the metadata was not available within <paramref name="timeout"/>.
    /// </exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="NotSupportedException">
    /// The <see cref="Task"/> faults with it when called inside a callback of an
    /// <see cref="AsyncMockConsumer{TKey, TValue}"/>, whose handle does not support it.
    /// </exception>
    Task<IReadOnlyList<PartitionInfo>> PartitionsFor(
        string topic,
        TimeSpan timeout,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the partition metadata of every topic the consumer is authorized to see (Java
    /// <c>listTopics()</c>), keyed by topic name; an empty dictionary when no topic is known.
    /// </summary>
    /// <param name="cancellationToken">
    /// Cancels the wait: <c>wakeup()</c> is sent, and the <see cref="Task"/> ends with
    /// <see cref="OperationCanceledException"/> once the operation has ended.
    /// </param>
    /// <returns>The partition metadata per topic, keyed by topic name.</returns>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="NotSupportedException">
    /// The <see cref="Task"/> faults with it when called inside a callback of an
    /// <see cref="AsyncMockConsumer{TKey, TValue}"/>, whose handle does not support it.
    /// </exception>
    Task<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>> ListTopics(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns metadata for all topics the consumer is authorized to view, waiting at most
    /// <paramref name="timeout"/> (Java <c>listTopics(Duration)</c>).
    /// </summary>
    /// <param name="timeout">The maximum time to wait.</param>
    /// <param name="cancellationToken">
    /// Cancels the wait: <c>wakeup()</c> is sent, and the <see cref="Task"/> ends with
    /// <see cref="OperationCanceledException"/> once the operation has ended.
    /// </param>
    /// <returns>The partition metadata per topic, keyed by topic name.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was canceled.</exception>
    /// <exception cref="NotSupportedException">
    /// The <see cref="Task"/> faults with it when called inside a callback of an
    /// <see cref="AsyncMockConsumer{TKey, TValue}"/>, whose handle does not support it.
    /// </exception>
    Task<IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>>> ListTopics(
        TimeSpan timeout,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Closes the consumer (Java <c>close()</c>), then frees it. Unlike
    /// <see cref="IAsyncDisposable.DisposeAsync"/>, it reports a close failure. A later
    /// <see cref="IDisposable.Dispose"/> or <see cref="IAsyncDisposable.DisposeAsync"/> does nothing.
    /// </summary>
    /// <remarks>
    /// The close waits at most 30 seconds, the bound of Java's <c>close()</c>. Inside a callback of this consumer
    /// (a rebalance listener or a commit callback), after an <c>await</c> too, <see cref="IDisposable.Dispose"/>
    /// and <see cref="IAsyncDisposable.DisposeAsync"/> return at once, and the consumer closes once the call that
    /// runs the callback has ended.
    /// </remarks>
    /// <param name="cancellationToken">
    /// A token already cancelled ends the <see cref="Task"/> with <see cref="OperationCanceledException"/> and
    /// nothing closes. Once the close has started it runs to its end, and the <see cref="Task"/> then ends with
    /// <see cref="OperationCanceledException"/> if the token fired.
    /// </param>
    /// <exception cref="InvalidOperationException">
    /// Another thread is inside a call on this consumer, or the call is made inside a callback of this consumer,
    /// unless a close has begun; the consumer stays open.
    /// </exception>
    /// <exception cref="KafkaException">The core reported a close failure.</exception>
    Task Close(CancellationToken cancellationToken = default);

    /// <summary>
    /// Closes the consumer as <paramref name="option"/> says (Java <c>close(CloseOptions)</c>), then releases
    /// native resources.
    /// </summary>
    /// <param name="option">The close timeout and group membership operation.</param>
    /// <param name="cancellationToken">
    /// A token already cancelled ends the <see cref="Task"/> with <see cref="OperationCanceledException"/> and
    /// nothing closes. Once the close has started it runs to its end, and the <see cref="Task"/> then ends with
    /// <see cref="OperationCanceledException"/> if the token fired.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="option"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The timeout of <paramref name="option"/> is negative ("The timeout cannot be negative.").
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// Another thread is inside a call on this consumer, or the call is made inside a callback of this consumer,
    /// unless a close has begun; the consumer stays open.
    /// </exception>
    /// <exception cref="KafkaException">The core reported a close failure.</exception>
    Task Close(CloseOptions option, CancellationToken cancellationToken = default);
}
