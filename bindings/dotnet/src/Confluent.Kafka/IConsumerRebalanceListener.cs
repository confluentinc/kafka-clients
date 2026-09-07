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

using System.Collections.Generic;

namespace Confluent.Kafka;

/// <summary>
/// A callback set invoked when the set of partitions assigned to the consumer changes —
/// the C# realization of Java's
/// <c>org.apache.kafka.clients.consumer.ConsumerRebalanceListener</c>. Pass an
/// implementation to <see cref="IConsumer{TKey, TValue}.Subscribe(IReadOnlyCollection{string}, IConsumerRebalanceListener)"/>
/// or <see cref="IAsyncConsumer{TKey, TValue}.Subscribe(IReadOnlyCollection{string}, IConsumerRebalanceListener, System.Threading.CancellationToken)"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>The methods are synchronous, not <see cref="System.Threading.Tasks.Task"/>-returning
/// (declared divergence D1).</b> The ABI callback is a synchronous C function pointer
/// returning <c>kafka_common_KafkaError_t*</c>, and the rebalance blocks until it returns
/// (<c>confluent_kafka.h:209-213</c>). <c>bindings/dotnet/CLAUDE.md</c> §3's "(async)" row
/// describes the <em>Rust core's</em> trait, which the C ABI flattens
/// (<c>bindings/CLAUDE.md</c> §1.2); restoring "blocks until it returns" faithfully in C#
/// means a sync method. Settled as roadmap Q6. An async listener would have to be blocked
/// on from the dispatcher thread, which is the documented deadlock source both Python
/// reference servers avoid by using plain sync methods.
/// </para>
/// <para>
/// <b>Callbacks run on the core's callback-dispatcher thread, not the caller's task
/// (declared divergence D3).</b> <c>consumer-threading.md</c> §31's caller's-task model is
/// the Rust core's contract; the C ABI flattens it. Python documents the identical
/// divergence (<c>consumer.py:977-981</c>). The dispatcher never runs two callbacks of the
/// same consumer concurrently, and the rebalance — and the operation that triggered it —
/// does not proceed until the callback returns.
/// </para>
/// <para>
/// <b>A listener must not call back into its own consumer.</b> The plain consumer API is
/// rejected with a <see cref="KafkaException"/> (ConcurrentModification) while the
/// application thread driving the rebalance holds the core's access guard. The sanctioned
/// reentrancy path is <see cref="ConsumerHandle"/> (Java gets the equivalent for free by
/// running the callback on the polling thread), obtained from
/// <see cref="IConsumerCommon.Handle"/> and <b>shipped since M9/P8</b>: capture one before
/// subscribing and use it from inside the callback, where the consumer's own API is not
/// available.
/// </para>
/// <para>
/// <b>Throwing is meaningful.</b> An exception thrown by a listener method is caught at the
/// interop boundary (it is never allowed to unwind into native code) and converted into the
/// error the Rust core sees, so it propagates out of the operation that triggered the
/// rebalance as a <see cref="KafkaException"/> carrying the exception's message. This
/// mirrors a Java listener throwing.
/// </para>
/// <para>
/// <b>Registration lifetime.</b> A listener is bound to a <em>subscription</em>, not to one
/// operation, and fires many times. The registration is released by a <b>replacing</b>
/// subscribe (including a listener-less one) or by disposing the consumer; it is
/// deliberately <b>not</b> released by <c>Unsubscribe()</c> or <c>Close()</c>, matching
/// Java's <c>SubscriptionState.unsubscribe()</c>.
/// </para>
/// <para>
/// <b>Why three required methods.</b> Java gives <c>onPartitionsLost</c> a default
/// implementation that delegates to <c>onPartitionsRevoked</c>. C# default interface methods
/// require .NET Standard 2.1 / C# 8, above this binding's netstandard2.0 floor, so the
/// default cannot live on the interface. Derive from
/// <see cref="ConsumerRebalanceListenerBase"/> to get Java's default behaviour; implement
/// this interface directly to be explicit about all three.
/// </para>
/// </remarks>
public interface IConsumerRebalanceListener
{
    /// <summary>
    /// Invoked before the consumer gives up ownership of <paramref name="partitions"/>
    /// (Java <c>onPartitionsRevoked(Collection&lt;TopicPartition&gt;)</c>). Called with the
    /// partitions being <b>taken away</b>, and only when something is actually being
    /// removed. The rebalance does not proceed until this returns.
    /// </summary>
    /// <param name="partitions">The partitions being revoked (never null; may be empty).</param>
    void OnPartitionsRevoked(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Invoked after the consumer has been assigned <paramref name="partitions"/> and
    /// before it starts fetching them (Java
    /// <c>onPartitionsAssigned(Collection&lt;TopicPartition&gt;)</c>). Called with the
    /// <b>newly added</b> partitions — <em>not</em> the full assignment, matching Java —
    /// and it fires even when the added set is empty. The rebalance does not proceed until
    /// this returns.
    /// </summary>
    /// <param name="partitions">The newly added partitions (never null; may be empty).</param>
    void OnPartitionsAssigned(IReadOnlyCollection<TopicPartition> partitions);

    /// <summary>
    /// Invoked when the consumer detects that it has lost <paramref name="partitions"/>
    /// without a clean revocation (Java
    /// <c>onPartitionsLost(Collection&lt;TopicPartition&gt;)</c>) — for example after being
    /// fenced by the group coordinator. Java's default delegates to
    /// <see cref="OnPartitionsRevoked"/>; on this binding's netstandard2.0 floor that
    /// default lives on <see cref="ConsumerRebalanceListenerBase"/> instead.
    /// </summary>
    /// <param name="partitions">The lost partitions (never null; may be empty).</param>
    void OnPartitionsLost(IReadOnlyCollection<TopicPartition> partitions);
}
