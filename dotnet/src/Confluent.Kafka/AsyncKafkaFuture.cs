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
using System.Threading.Tasks;

namespace Confluent.Kafka;

/// <summary>
/// The delivery handle of an accepted async send — the .NET realization of the
/// <c>java.util.concurrent.Future&lt;RecordMetadata&gt;</c> that Java's <c>Producer.send</c> returns
/// (<c>Producer.java:81</c>, <c>:86</c>). The <see cref="ValueTask{TResult}"/> returned by
/// <see cref="IAsyncProducer{TKey, TValue}.Send(ProducerRecord{TKey, TValue}, System.Threading.CancellationToken)"/>
/// completes when the record is <b>accepted</b> and yields this value; awaiting <see cref="Get"/> yields the
/// <see cref="RecordMetadata"/> once the record is <b>delivered</b> — <c>await future.Get()</c> is Java's
/// <c>future.get()</c>.
/// </summary>
/// <typeparam name="T">The delivery result: <see cref="RecordMetadata"/> for a producer send.</typeparam>
/// <remarks>
/// <code>
/// AsyncKafkaFuture&lt;RecordMetadata&gt; future = await producer.Send(record);   // accepted
/// RecordMetadata metadata = await future.Get();                            // delivered
/// </code>
/// <list type="bullet">
/// <item><description>
/// <b>It does not block; await it.</b> A <c>readonly struct</c> over the delivery <see cref="Task{TResult}"/>, so it
/// costs no allocation (the common, unsaturated send allocates nothing for its first stage). <see cref="Get"/>
/// returns the <b>same</b> task on every call and never blocks; that task may be awaited any number of times and
/// stored freely — unlike the <see cref="ValueTask{TResult}"/> stage, which is awaited once.
/// </description></item>
/// <item><description>
/// <b>Not awaitable itself</b> — await <see cref="Get"/>. <c>ConfigureAwait</c> goes on the task:
/// <c>await future.Get().ConfigureAwait(false)</c>.
/// </description></item>
/// <item><description>
/// <b>⚠ Acceptance is not release.</b> Receiving this value (acceptance) does <b>not</b> end the borrow of the
/// record's key / value buffers. Reuse a buffer only after <see cref="Get"/>'s task completes <b>without</b> being
/// canceled, after the record's delivery callback (<see cref="IDeliveryCallback"/>) fires, or after a later
/// <see cref="IAsyncProducer{TKey, TValue}.Flush(System.Threading.CancellationToken)"/> completes successfully
/// (M11/P3.5 91.11; see <c>IAsyncProducer.Send</c>'s remarks).
/// </description></item>
/// <item><description>
/// <b>Cancellation.</b> A token that fires after acceptance cancels <see cref="Get"/>'s task; it never un-sends the
/// record.
/// </description></item>
/// <item><description>
/// <b>Default value.</b> <c>default(AsyncKafkaFuture&lt;T&gt;)</c> carries no send: <see cref="Get"/> throws
/// <see cref="InvalidOperationException"/>, synchronously. Only <c>IAsyncProducer.Send</c> returns a usable value.
/// </description></item>
/// <item><description>
/// <b>Equality.</b> Two values are equal exactly when they wrap the same delivery task (reference identity) — the
/// identity of Java's <c>Future</c> and of <see cref="ValueTask{TResult}"/>'s equality. There is no
/// <c>ToString</c> override (Java's <c>Future</c> has none).
/// </description></item>
/// <item><description>
/// <b>Naming — a deliberate deviation.</b> This type has a Java counterpart,
/// <c>java.util.concurrent.Future&lt;RecordMetadata&gt;</c>, which this binding would otherwise map to a plain
/// <see cref="Task{TResult}"/> (the mapping the admin client uses for Java's <c>KafkaFuture&lt;T&gt;</c>). It is a
/// struct over that task instead, so the two stages of <c>Send</c> are distinguishable (a
/// <c>ValueTask&lt;Task&lt;T&gt;&gt;</c> reads as one awaitable too many); it costs nothing at runtime. Note the
/// name says <i>KafkaFuture</i> although Java's <c>org.apache.kafka.common.KafkaFuture</c> maps to
/// <see cref="Task{TResult}"/> in the admin client — this type is not that one.
/// </description></item>
/// </list>
/// </remarks>
public readonly struct AsyncKafkaFuture<T> : IEquatable<AsyncKafkaFuture<T>>
{
    private readonly Task<T>? _delivery;

    // Never throws: production builds this after the record is appended (the M11/P3.5 post-append
    // no-throw invariant), so it must stay a plain field store.
    internal AsyncKafkaFuture(Task<T> delivery) => _delivery = delivery;

    /// <summary>
    /// Returns the record's delivery task: it resolves with the value, faults with a <see cref="KafkaException"/>
    /// carrying the delivery failure, or is canceled by the send's token. Await it for Java's <c>future.get()</c>.
    /// </summary>
    /// <returns>The delivery <see cref="Task{TResult}"/> — the same instance on every call. This call never blocks.</returns>
    /// <exception cref="InvalidOperationException">
    /// This is a <see langword="default"/> value, which carries no send. Thrown synchronously, by this call.
    /// </exception>
    public Task<T> Get() =>
        _delivery ?? throw new InvalidOperationException(
            "This AsyncKafkaFuture is a default value and carries no send; only IAsyncProducer.Send returns a usable one.");

    /// <summary>
    /// Determines whether two <see cref="AsyncKafkaFuture{T}"/> values are equal — they wrap the same delivery task.
    /// </summary>
    public static bool operator ==(AsyncKafkaFuture<T> left, AsyncKafkaFuture<T> right) => left.Equals(right);

    /// <summary>
    /// Determines whether two <see cref="AsyncKafkaFuture{T}"/> values are not equal — they wrap different delivery
    /// tasks.
    /// </summary>
    public static bool operator !=(AsyncKafkaFuture<T> left, AsyncKafkaFuture<T> right) => !left.Equals(right);

    /// <summary>
    /// Determines whether this value equals <paramref name="other"/> — both wrap the same delivery task (reference
    /// identity, never the task's result). Two <see langword="default"/> values are equal.
    /// </summary>
    public bool Equals(AsyncKafkaFuture<T> other) => ReferenceEquals(_delivery, other._delivery);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is AsyncKafkaFuture<T> other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => _delivery?.GetHashCode() ?? 0;
}
