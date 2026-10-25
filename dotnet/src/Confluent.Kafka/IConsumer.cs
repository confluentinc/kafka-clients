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

namespace Confluent.Kafka;

/// <summary>
/// The synchronous Kafka consumer, Java's <c>org.apache.kafka.clients.consumer.Consumer&lt;K, V&gt;</c>, implemented
/// by <see cref="KafkaConsumer{TKey, TValue}"/> and the broker-free <see cref="MockConsumer{TKey, TValue}"/>.
/// <see cref="IAsyncConsumer{TKey, TValue}"/> is its asynchronous sibling over the same native consumer, not a
/// wrapper. A method that blocks in Java blocks here; the non-blocking members are on <see cref="IConsumerCommon"/>.
/// </summary>
/// <remarks>
/// <para>
/// There is no cancellation token, as in Java: <see cref="IConsumerCommon.Wakeup"/> from another thread makes a
/// blocked <see cref="Poll"/>, <c>Commit</c>, <c>Position</c>, <c>Committed</c>, <c>PartitionsFor</c> or
/// <c>ListTopics</c> throw a <see cref="KafkaException"/> with the Wakeup code, once.
/// </para>
/// <para>
/// The consumer has one owner at a time, as Java's does. A call from another thread while a call is in progress
/// throws <see cref="InvalidOperationException"/> ("KafkaConsumer is not safe for multi-threaded access.");
/// <see cref="MockConsumer{TKey, TValue}"/> makes it wait instead. <see cref="IConsumerCommon.Wakeup"/> and
/// <see cref="IConsumerCommon.Handle"/> may be called from any thread, and a <see cref="IDisposable.Dispose"/> from
/// another thread wakes the call in progress, waits for it, and closes the consumer.
/// </para>
/// <para>
/// Rebalance listeners run on the calling thread, only inside <c>Poll</c>, <c>Unsubscribe</c> and <c>Close</c>;
/// commit callbacks run on it inside the blocking call that delivers them. What a callback may call on its own
/// consumer is in <see cref="IConsumerRebalanceListener"/>.
/// </para>
/// </remarks>
/// <typeparam name="TKey">The deserialized key type.</typeparam>
/// <typeparam name="TValue">The deserialized value type.</typeparam>
public interface IConsumer<TKey, TValue> : IConsumerCommon, IDisposable
{
    /// <summary>
    /// Polls for records (Java <c>poll(Duration)</c>). An empty poll returns an empty
    /// <see cref="ConsumerRecords{TKey, TValue}"/>, not <see langword="null"/>. The records own copies of their
    /// bytes.
    /// </summary>
    /// <param name="timeout">The maximum time to wait (must be non-negative).</param>
    /// <returns>The records polled, as an owned <see cref="ConsumerRecords{TKey, TValue}"/>.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a poll failure (or a <see cref="IConsumerCommon.Wakeup"/> interrupted it).</exception>
    /// <exception cref="RecordDeserializationException">
    /// The first record of the batch failed to deserialize. A record that fails after others is not thrown: the
    /// records before it are returned. Either way the partitions are sought back to their first record not
    /// returned.
    /// </exception>
    ConsumerRecords<TKey, TValue> Poll(TimeSpan timeout);

    /// <summary>
    /// Subscribes to <paramref name="topics"/> (Java <c>subscribe(Collection)</c>).
    /// </summary>
    /// <param name="topics">The topics to subscribe to.</param>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> is null.</exception>
    /// <exception cref="ArgumentException">A topic name is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    void Subscribe(IReadOnlyCollection<string> topics);

    /// <summary>
    /// Subscribes to <paramref name="topics"/> with a rebalance listener (Java
    /// <c>subscribe(Collection, ConsumerRebalanceListener)</c>). The listener fires on every later rebalance
    /// until a subscribe that succeeds replaces it.
    /// </summary>
    /// <remarks>
    /// A replacing subscribe releases the previous listener, a listener-less
    /// <see cref="Subscribe(IReadOnlyCollection{string})"/> included, as does disposing the consumer.
    /// <see cref="Unsubscribe"/> and <c>Close</c> keep it, as Java's <c>SubscriptionState.unsubscribe()</c> does.
    /// <see cref="IConsumerRebalanceListener"/> describes where the listener runs, what throwing does, and what it
    /// may call on its own consumer.
    /// </remarks>
    /// <param name="topics">The topics to subscribe to.</param>
    /// <param name="listener">The rebalance listener to register.</param>
    /// <exception cref="ArgumentNullException"><paramref name="topics"/> or <paramref name="listener"/> is null.</exception>
    /// <exception cref="ArgumentException">A topic name is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a subscribe failure.</exception>
    void Subscribe(IReadOnlyCollection<string> topics, IConsumerRebalanceListener listener);

    /// <summary>
    /// Unsubscribes from all topics and partitions (Java <c>unsubscribe()</c>).
    /// </summary>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    void Unsubscribe();

    /// <summary>
    /// Manually assigns <paramref name="partitions"/> to this consumer (Java <c>assign(Collection)</c>).
    /// </summary>
    /// <remarks>
    /// An empty collection clears the assignment, as Java's <c>assign</c> does; a <see langword="null"/> one is
    /// rejected.
    /// </remarks>
    /// <param name="partitions">The topic-partitions to assign (an empty collection clears the assignment).</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported an assignment failure.</exception>
    void Assign(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Suspends fetching for <paramref name="partitions"/> (Java <c>pause(Collection)</c>); the paused set is
    /// read with <see cref="IConsumerCommon.Paused"/>.
    /// </summary>
    /// <remarks>An empty collection does nothing.</remarks>
    /// <param name="partitions">The topic-partitions to pause.</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">A partition is not assigned to the consumer.</exception>
    /// <exception cref="KafkaException">The core reported a pause failure.</exception>
    /// <exception cref="NotSupportedException">
    /// Called inside a callback of a <see cref="MockConsumer{TKey, TValue}"/>, whose
    /// <see cref="ConsumerHandle"/> does not support it.
    /// </exception>
    void Pause(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Resumes fetching for <paramref name="partitions"/> paused by <see cref="Pause"/> (Java
    /// <c>resume(Collection)</c>).
    /// </summary>
    /// <remarks>An empty collection does nothing.</remarks>
    /// <param name="partitions">The topic-partitions to resume.</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">A partition is not assigned to the consumer.</exception>
    /// <exception cref="KafkaException">The core reported a resume failure.</exception>
    /// <exception cref="NotSupportedException">
    /// Called inside a callback of a <see cref="MockConsumer{TKey, TValue}"/>, whose
    /// <see cref="ConsumerHandle"/> does not support it.
    /// </exception>
    void Resume(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Seeks <paramref name="partitions"/> to their first available offset (Java
    /// <c>seekToBeginning(Collection)</c>). The offset is resolved on the next <see cref="Poll"/>.
    /// </summary>
    /// <remarks>An empty collection does nothing.</remarks>
    /// <param name="partitions">The topic-partitions to seek to the beginning.</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a seek failure.</exception>
    /// <exception cref="NotSupportedException">
    /// Called inside a callback of a <see cref="MockConsumer{TKey, TValue}"/>, whose
    /// <see cref="ConsumerHandle"/> does not support it.
    /// </exception>
    void SeekToBeginning(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Seeks <paramref name="partitions"/> to their last offset (Java <c>seekToEnd(Collection)</c>). The offset
    /// is resolved on the next <see cref="Poll"/>.
    /// </summary>
    /// <remarks>An empty collection does nothing.</remarks>
    /// <param name="partitions">The topic-partitions to seek to the end.</param>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a seek failure.</exception>
    /// <exception cref="NotSupportedException">
    /// Called inside a callback of a <see cref="MockConsumer{TKey, TValue}"/>, whose
    /// <see cref="ConsumerHandle"/> does not support it.
    /// </exception>
    void SeekToEnd(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Returns the offset of the next record that will be fetched from <paramref name="partition"/> (Java
    /// <c>position(TopicPartition)</c>).
    /// </summary>
    /// <param name="partition">The topic-partition whose position to read.</param>
    /// <returns>The current offset position of <paramref name="partition"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partition"/>'s topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="partition"/>'s partition is negative.</exception>
    /// <exception cref="ArgumentException"><paramref name="partition"/> is not assigned to the consumer.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a position failure.</exception>
    /// <exception cref="NotSupportedException">
    /// Called inside a callback of a <see cref="MockConsumer{TKey, TValue}"/>, whose
    /// <see cref="ConsumerHandle"/> does not support it.
    /// </exception>
    long Position(TopicPartition partition);

    /// <summary>
    /// Returns the current offset position of <paramref name="partition"/>, blocking at most
    /// <paramref name="timeout"/> (Java <c>position(TopicPartition, Duration)</c>).
    /// </summary>
    /// <param name="partition">The topic-partition whose position to read.</param>
    /// <param name="timeout">The maximum time to block.</param>
    /// <returns>The current offset position of <paramref name="partition"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partition"/>'s topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="partition"/>'s partition is negative, or <paramref name="timeout"/> is negative
    /// ("Invalid negative timeout &lt;ms&gt;").
    /// </exception>
    /// <exception cref="ArgumentException"><paramref name="partition"/> is not assigned to the consumer.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">
    /// The position could not be determined within <paramref name="timeout"/> (code 7), or the core reported another
    /// position failure.
    /// </exception>
    /// <exception cref="NotSupportedException">
    /// Called inside a callback of a <see cref="MockConsumer{TKey, TValue}"/>, whose
    /// <see cref="ConsumerHandle"/> does not support it.
    /// </exception>
    long Position(TopicPartition partition, TimeSpan timeout);

    /// <summary>
    /// Commits the current fetch positions for all assigned partitions and waits for the commit (Java
    /// <c>commitSync()</c>). <see cref="CommitAsync()"/> is Java's <c>commitAsync()</c>, which does not wait.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a commit failure.</exception>
    void Commit();

    /// <summary>
    /// Commits the current fetch positions for all assigned partitions, blocking at most
    /// <paramref name="timeout"/> (Java <c>commitSync(Duration)</c>).
    /// </summary>
    /// <param name="timeout">The maximum time to block.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="timeout"/> is negative ("Invalid negative timeout &lt;ms&gt;").
    /// </exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">
    /// The commit did not complete within <paramref name="timeout"/> (code 7), or the core reported another commit
    /// failure.
    /// </exception>
    void Commit(TimeSpan timeout);

    /// <summary>
    /// Commits the specific <paramref name="offsets"/> (Java
    /// <c>commitSync(Map&lt;TopicPartition, OffsetAndMetadata&gt;)</c>) and waits for the commit. An empty map
    /// commits nothing.
    /// </summary>
    /// <param name="offsets">The offsets to commit, keyed by topic-partition.</param>
    /// <exception cref="ArgumentNullException"><paramref name="offsets"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null, or a value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a commit failure.</exception>
    void Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets);

    /// <summary>
    /// Commits the specific <paramref name="offsets"/>, blocking at most <paramref name="timeout"/>
    /// (Java <c>commitSync(Map&lt;TopicPartition, OffsetAndMetadata&gt;, Duration)</c>).
    /// </summary>
    /// <param name="offsets">The offsets to commit, keyed by topic-partition.</param>
    /// <param name="timeout">The maximum time to block.</param>
    /// <exception cref="ArgumentNullException"><paramref name="offsets"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null, or a value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// A key partition is negative, or <paramref name="timeout"/> is negative ("Invalid negative timeout &lt;ms&gt;").
    /// </exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">
    /// The commit did not complete within <paramref name="timeout"/> (code 7), or the core reported another commit
    /// failure.
    /// </exception>
    void Commit(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, TimeSpan timeout);

    /// <summary>
    /// Commits the current fetch positions without waiting for the commit (Java <c>commitAsync()</c>):
    /// returns once the commit is started.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">Another thread is using the consumer.</exception>
    /// <exception cref="KafkaException">The core could not start the commit.</exception>
    void CommitAsync();

    /// <summary>
    /// Commits the current fetch positions without waiting for the commit, and runs <paramref name="callback"/>
    /// with its outcome (Java <c>commitAsync(OffsetCommitCallback)</c>): returns once the commit is started.
    /// </summary>
    /// <remarks>
    /// The callback runs on the calling thread, inside a later call that delivers it (<c>Poll</c>, <c>Commit</c>,
    /// <c>CommitAsync</c>, <c>Close</c>, …). An exception this method throws means the commit never started; the
    /// commit's own outcome reaches <paramref name="callback"/>. A <see langword="null"/> callback, which Java
    /// accepts, is rejected: <see cref="CommitAsync()"/> commits without one.
    /// </remarks>
    /// <param name="callback">The callback that receives the commit's outcome.</param>
    /// <exception cref="ArgumentNullException"><paramref name="callback"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">Another thread is using the consumer.</exception>
    /// <exception cref="KafkaException">The core could not start the commit.</exception>
    void CommitAsync(IOffsetCommitCallback callback);

    /// <summary>
    /// Commits the specific <paramref name="offsets"/> without waiting for the commit, and runs
    /// <paramref name="callback"/>, if any, with its outcome (Java
    /// <c>commitAsync(Map&lt;TopicPartition, OffsetAndMetadata&gt;, OffsetCommitCallback)</c>): returns once the
    /// commit is started.
    /// </summary>
    /// <remarks>
    /// The callback runs as for <see cref="CommitAsync(IOffsetCommitCallback)"/>. An empty
    /// <paramref name="offsets"/> commits nothing; the callback then receives <see langword="null"/> offsets, or the
    /// empty map on <see cref="MockConsumer{TKey, TValue}"/>, as Java's <c>MockConsumer</c> passes it.
    /// </remarks>
    /// <param name="offsets">The offsets to commit, keyed by topic-partition.</param>
    /// <param name="callback">The callback that receives the commit's outcome, or <see langword="null"/> for none.</param>
    /// <exception cref="ArgumentNullException"><paramref name="offsets"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null, or a value is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">Another thread is using the consumer.</exception>
    /// <exception cref="KafkaException">The core could not start the commit.</exception>
    void CommitAsync(
        IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets,
        IOffsetCommitCallback? callback = null);

    /// <summary>
    /// Seeks <paramref name="partition"/> to <paramref name="offset"/> (Java <c>seek(TopicPartition, long)</c>);
    /// blocks until the seek is applied.
    /// </summary>
    /// <param name="partition">The topic-partition to seek.</param>
    /// <param name="offset">The offset to seek to.</param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="offset"/> is negative ("seek offset must not be a negative number").
    /// </exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">The partition is not assigned, or another thread is using the consumer.</exception>
    /// <exception cref="NotSupportedException">
    /// Called inside a callback of a <see cref="MockConsumer{TKey, TValue}"/>, whose
    /// <see cref="ConsumerHandle"/> does not support it.
    /// </exception>
    void Seek(TopicPartition partition, long offset);

    /// <summary>
    /// Seeks <paramref name="partition"/> to the offset, leader epoch and metadata of
    /// <paramref name="offsetAndMetadata"/> (Java <c>seek(TopicPartition, OffsetAndMetadata)</c>); blocks until
    /// the seek is applied.
    /// </summary>
    /// <param name="partition">The topic-partition to seek.</param>
    /// <param name="offsetAndMetadata">The offset to seek to.</param>
    /// <exception cref="ArgumentNullException"><paramref name="offsetAndMetadata"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">The partition is not assigned, or another thread is using the consumer.</exception>
    /// <exception cref="NotSupportedException">
    /// Called inside a callback of a <see cref="MockConsumer{TKey, TValue}"/>, whose
    /// <see cref="ConsumerHandle"/> does not support it.
    /// </exception>
    void Seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata);

    /// <summary>
    /// Returns the consumer's lag for <paramref name="partition"/> (Java <c>currentLag(TopicPartition)</c>), or
    /// <see langword="null"/> when it is not known.
    /// </summary>
    /// <param name="partition">The topic-partition whose lag to read.</param>
    /// <returns>The current lag, or <see langword="null"/> when it is not known.</returns>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="InvalidOperationException">Another thread is using the consumer.</exception>
    long? CurrentLag(TopicPartition partition);

    /// <summary>
    /// Returns the last committed offset for each of <paramref name="partitions"/> (Java
    /// <c>committed(Set&lt;TopicPartition&gt;)</c>). Every requested partition is in the result,
    /// <see langword="null"/> where it has no committed offset; <see cref="MockConsumer{TKey, TValue}"/>,
    /// like Java's <c>MockConsumer</c>, leaves such a partition out.
    /// </summary>
    /// <param name="partitions">The topic-partitions whose committed offsets to read.</param>
    /// <returns>The committed offset (and metadata) of each partition, <see langword="null"/> where it has none.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a committed-query failure.</exception>
    /// <exception cref="NotSupportedException">
    /// Called inside a callback of a <see cref="MockConsumer{TKey, TValue}"/>, whose
    /// <see cref="ConsumerHandle"/> does not support it.
    /// </exception>
    IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> Committed(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Returns the last committed offset for each of <paramref name="partitions"/>, blocking at most <paramref name="timeout"/>
    /// (Java <c>committed(Set&lt;TopicPartition&gt;, Duration)</c>).
    /// </summary>
    /// <param name="partitions">The topic-partitions to read.</param>
    /// <param name="timeout">The maximum time to block.</param>
    /// <returns>The committed offset (and metadata) of each partition, <see langword="null"/> where it has none.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative, or <paramref name="timeout"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">
    /// The query did not complete within <paramref name="timeout"/> (code 7), or the core reported another failure.
    /// </exception>
    IReadOnlyDictionary<TopicPartition, OffsetAndMetadata?> Committed(IReadOnlyCollection<TopicPartition> partitions, TimeSpan timeout);

    /// <summary>
    /// Looks up the offset of the first record at or after each timestamp in
    /// <paramref name="timestampsToSearch"/> (Java
    /// <c>offsetsForTimes(Map&lt;TopicPartition, Long&gt;)</c>). A negative timestamp is passed through, not
    /// rejected: Kafka uses negative special timestamps (earliest, latest). Every requested partition
    /// is in the result, <see langword="null"/> where no offset was found for its timestamp.
    /// </summary>
    /// <param name="timestampsToSearch">The per-partition timestamps to search for.</param>
    /// <returns>The first offset at or after each timestamp, per partition, <see langword="null"/> where there is none.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="timestampsToSearch"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported an offsets-for-times failure.</exception>
    /// <exception cref="NotSupportedException">The consumer is a <see cref="MockConsumer{TKey, TValue}"/>, which does not implement it.</exception>
    IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch);

    /// <summary>
    /// Looks up the offset of the first record at or after each timestamp in
    /// <paramref name="timestampsToSearch"/>, blocking at most <paramref name="timeout"/> (Java
    /// <c>offsetsForTimes(Map&lt;TopicPartition, Long&gt;, Duration)</c>).
    /// </summary>
    /// <param name="timestampsToSearch">The per-partition timestamps to search for.</param>
    /// <param name="timeout">The maximum time to block.</param>
    /// <returns>The first offset at or after each timestamp, per partition, <see langword="null"/> where there is none.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="timestampsToSearch"/> is null.</exception>
    /// <exception cref="ArgumentException">A key topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A key partition is negative, or <paramref name="timeout"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">
    /// The query did not complete within <paramref name="timeout"/> (code 7), or the core reported another failure.
    /// </exception>
    /// <exception cref="NotSupportedException">The consumer is a <see cref="MockConsumer{TKey, TValue}"/>, which does not implement it.</exception>
    IReadOnlyDictionary<TopicPartition, OffsetAndTimestamp?> OffsetsForTimes(
        IReadOnlyDictionary<TopicPartition, long> timestampsToSearch, TimeSpan timeout);

    /// <summary>
    /// Returns the earliest available offset for each of <paramref name="partitions"/> (Java
    /// <c>beginningOffsets(Collection&lt;TopicPartition&gt;)</c>).
    /// </summary>
    /// <param name="partitions">The topic-partitions whose beginning offsets to read.</param>
    /// <returns>The earliest available offset for each partition.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a beginning-offsets failure.</exception>
    /// <exception cref="NotSupportedException">
    /// Called inside a callback of a <see cref="MockConsumer{TKey, TValue}"/>, whose
    /// <see cref="ConsumerHandle"/> does not support it.
    /// </exception>
    IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Returns the earliest available offset for each of <paramref name="partitions"/>, blocking at most <paramref name="timeout"/>
    /// (Java <c>beginningOffsets(Collection&lt;TopicPartition&gt;, Duration)</c>).
    /// </summary>
    /// <param name="partitions">The topic-partitions to read.</param>
    /// <param name="timeout">The maximum time to block.</param>
    /// <returns>The earliest available offset for each partition.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative, or <paramref name="timeout"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">
    /// The query did not complete within <paramref name="timeout"/> (code 7), or the core reported another failure.
    /// </exception>
    IReadOnlyDictionary<TopicPartition, long> BeginningOffsets(IReadOnlyCollection<TopicPartition> partitions, TimeSpan timeout);

    /// <summary>
    /// Returns the latest offset (log-end offset) for each of <paramref name="partitions"/> (Java
    /// <c>endOffsets(Collection&lt;TopicPartition&gt;)</c>).
    /// </summary>
    /// <param name="partitions">The topic-partitions whose end offsets to read.</param>
    /// <returns>The latest offset for each partition.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported an end-offsets failure.</exception>
    /// <exception cref="NotSupportedException">
    /// Called inside a callback of a <see cref="MockConsumer{TKey, TValue}"/>, whose
    /// <see cref="ConsumerHandle"/> does not support it.
    /// </exception>
    IReadOnlyDictionary<TopicPartition, long> EndOffsets(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Returns the latest offset (log-end offset) for each of <paramref name="partitions"/>, blocking at most <paramref name="timeout"/>
    /// (Java <c>endOffsets(Collection&lt;TopicPartition&gt;, Duration)</c>).
    /// </summary>
    /// <param name="partitions">The topic-partitions to read.</param>
    /// <param name="timeout">The maximum time to block.</param>
    /// <returns>The latest offset for each partition.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="partitions"/> is null.</exception>
    /// <exception cref="ArgumentException">An element topic is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">An element partition is negative, or <paramref name="timeout"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">
    /// The query did not complete within <paramref name="timeout"/> (code 7), or the core reported another failure.
    /// </exception>
    IReadOnlyDictionary<TopicPartition, long> EndOffsets(IReadOnlyCollection<TopicPartition> partitions, TimeSpan timeout);

    /// <summary>
    /// Returns the partition metadata for <paramref name="topic"/> (Java
    /// <c>partitionsFor(String)</c>), an empty list for a topic with no known partitions.
    /// </summary>
    /// <remarks>
    /// An empty <paramref name="topic"/> is passed to the core, not rejected, as in Java; only a
    /// <see langword="null"/> one throws.
    /// </remarks>
    /// <param name="topic">The topic whose partition metadata to read.</param>
    /// <returns>The partition metadata for the topic.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a partitions-for failure.</exception>
    IReadOnlyList<PartitionInfo> PartitionsFor(string topic);

    /// <summary>
    /// Returns the partition metadata for <paramref name="topic"/>, blocking at most
    /// <paramref name="timeout"/> (Java <c>partitionsFor(String, Duration)</c>).
    /// </summary>
    /// <param name="topic">The topic whose partition metadata to read.</param>
    /// <param name="timeout">The maximum time to block.</param>
    /// <returns>The partition metadata for the topic.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="topic"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">
    /// The metadata was not available within <paramref name="timeout"/> (code 7), or the core reported another
    /// failure.
    /// </exception>
    IReadOnlyList<PartitionInfo> PartitionsFor(string topic, TimeSpan timeout);

    /// <summary>
    /// Returns metadata for all topics the consumer is authorized to view (Java
    /// <c>listTopics()</c>), keyed by topic name; an empty dictionary when no topic is known.
    /// </summary>
    /// <returns>The partition metadata for every visible topic, keyed by topic.</returns>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">The core reported a list-topics failure.</exception>
    IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> ListTopics();

    /// <summary>
    /// Returns metadata for all topics the consumer is authorized to view, blocking at most
    /// <paramref name="timeout"/> (Java <c>listTopics(Duration)</c>).
    /// </summary>
    /// <param name="timeout">The maximum time to block.</param>
    /// <returns>The partition metadata for every visible topic, keyed by topic.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The consumer is closed.</exception>
    /// <exception cref="KafkaException">
    /// The metadata was not available within <paramref name="timeout"/> (code 7), or the core reported another
    /// failure.
    /// </exception>
    IReadOnlyDictionary<string, IReadOnlyList<PartitionInfo>> ListTopics(TimeSpan timeout);

    /// <summary>
    /// Closes the consumer (Java <c>close()</c>), then frees it. Unlike <see cref="IDisposable.Dispose"/>, it
    /// reports a close failure. A later <see cref="IDisposable.Dispose"/> does nothing.
    /// </summary>
    /// <remarks>
    /// The close waits at most 30 seconds, the bound of Java's <c>close()</c>. Java's deprecated
    /// <c>close(Duration)</c> is not translated; <see cref="Close(CloseOptions)"/> sets the bound. Inside a callback
    /// of this consumer (a rebalance listener or a commit callback), <see cref="IDisposable.Dispose"/> throws
    /// <see cref="InvalidOperationException"/> as this method does, and both return without effect once a close has
    /// begun.
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    /// Another thread is inside a call on this consumer, or the call is made inside a callback of this consumer;
    /// the consumer stays open.
    /// </exception>
    /// <exception cref="KafkaException">The core reported a close failure.</exception>
    void Close();

    /// <summary>
    /// Closes the consumer with the timeout and group membership operation of
    /// <paramref name="option"/> (Java <c>close(CloseOptions)</c>), then releases native resources.
    /// </summary>
    /// <param name="option">The close options.</param>
    /// <exception cref="ArgumentNullException"><paramref name="option"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The timeout of <paramref name="option"/> is negative ("The timeout cannot be negative.").
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// Another thread is inside a call on this consumer, or the call is made inside a callback of this consumer;
    /// the consumer stays open.
    /// </exception>
    /// <exception cref="KafkaException">The core reported a close failure.</exception>
    void Close(CloseOptions option);
}
