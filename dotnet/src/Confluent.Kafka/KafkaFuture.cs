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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka;

// M11/P4.2: these docs state the contract the sync Send takes on once IProducer.Send returns this type.
/// <summary>
/// The handle on one sync send's delivery — Java's <c>java.util.concurrent.Future&lt;RecordMetadata&gt;</c>
/// as returned by <c>Producer.send</c>. <see cref="Get()"/> blocks until the record is acknowledged or
/// fails (Java's <c>future.get()</c>).
/// </summary>
/// <typeparam name="T">The delivery result: <see cref="RecordMetadata"/> for a producer send.</typeparam>
/// <remarks>
/// <list type="bullet">
/// <item><b>Send does not wait for delivery.</b> <see cref="IProducer{TKey, TValue}.Send(ProducerRecord{TKey, TValue})"/>
/// returns once the core has accepted the record (it blocks only while <c>buffer.memory</c> is full, up to
/// <c>max.block.ms</c> — Java's <c>send</c> blocking). The key and value buffers are reusable on return.</item>
/// <item><b>Get blocks the calling thread</b> on a managed latch — not a <c>Task</c>, so this is not
/// sync-over-async. Call it any number of times from any number of threads; every call returns the
/// same <typeparamref name="T"/> or rethrows the same exception.</item>
/// <item><b>Failure.</b> <see cref="Get()"/> rethrows the send's exception itself (a <see cref="KafkaException"/>
/// for a delivery failure). Java wraps it in <c>ExecutionException</c>; .NET has no such type.</item>
/// <item><b>Delivery callback.</b> With <c>Send(record, callback)</c> the callback runs on the producer's
/// send-completion thread, before <see cref="Get()"/> returns (Java's ordering).</item>
/// <item><b>Inside a delivery callback</b>, calling <see cref="Get()"/> for a not-yet-completed send of the
/// same producer throws <see cref="InvalidOperationException"/> instead of deadlocking.</item>
/// <item><b>Fire-and-forget is fine.</b> Discarding the value leaks nothing — it holds no native
/// resource — and the callback still fires.</item>
/// <item><b>Order.</b> One producer completes its sends in the order <c>Send</c> returned, so a slow
/// partition delays later completions on the same producer (§D1).</item>
/// <item><b>default.</b> <see cref="Get()"/> on <c>default</c> throws <see cref="InvalidOperationException"/>.</item>
/// <item><b>Equality.</b> Two values are equal exactly when they came from the same <c>Send</c>.</item>
/// <item><b>Naming deviation.</b> Java's <c>org.apache.kafka.common.KafkaFuture</c> is the Admin result type,
/// which this binding maps to <see cref="System.Threading.Tasks.Task{TResult}"/>; this name pairs the sync
/// handle with <see cref="AsyncKafkaFuture{T}"/>.</item>
/// </list>
/// </remarks>
public readonly struct KafkaFuture<T> : IEquatable<KafkaFuture<T>>
{
    private readonly SyncCompletion<T>? _completion;

    // A plain field store: it never throws, and the value holds no native resource (S-1).
    internal KafkaFuture(SyncCompletion<T> completion) => _completion = completion;

    /// <summary>Java <c>Future.get()</c>.</summary>
    /// <returns>
    /// The send's result once it has completed — the same instance on every call. Blocks the calling thread until
    /// then; if the send failed, rethrows its exception (the same instance on every call, not wrapped).
    /// </returns>
    /// <exception cref="InvalidOperationException">A default value, or (D9) a call on the owning
    /// pump thread for a send that has not completed.</exception>
    public T Get() =>
        (_completion ?? throw new InvalidOperationException(
            "This KafkaFuture is a default value and carries no send; only IProducer.Send returns a usable one."))
        .Get();

    // No Get(TimeSpan) this phase (D7 deferred, FU-4: a timed or cancellable Get, with a CancellationToken).

    /// <summary>
    /// Determines whether two <see cref="KafkaFuture{T}"/> values are equal — they came from the same send.
    /// </summary>
    public static bool operator ==(KafkaFuture<T> left, KafkaFuture<T> right) => left.Equals(right);

    /// <summary>
    /// Determines whether two <see cref="KafkaFuture{T}"/> values are not equal — they came from different sends.
    /// </summary>
    public static bool operator !=(KafkaFuture<T> left, KafkaFuture<T> right) => !left.Equals(right);

    /// <summary>
    /// Determines whether this value equals <paramref name="other"/> — both came from the same send (reference
    /// identity of the completion, never the result). Two <see langword="default"/> values are equal.
    /// </summary>
    public bool Equals(KafkaFuture<T> other) => ReferenceEquals(_completion, other._completion);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is KafkaFuture<T> other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => _completion?.GetHashCode() ?? 0;
}
