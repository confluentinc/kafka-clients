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
/// The completion callback of an asynchronous offset commit — the C# realization of Java's
/// <c>org.apache.kafka.clients.consumer.OffsetCommitCallback</c>. Pass an implementation to
/// <see cref="IConsumerCommon.CommitAsync(IOffsetCommitCallback)"/> or
/// <see cref="IConsumerCommon.CommitAsync(IReadOnlyDictionary{TopicPartition, OffsetAndMetadata}, IOffsetCommitCallback)"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>The method is synchronous, not <see cref="System.Threading.Tasks.Task"/>-returning
/// (declared divergence D4 / the §4 commit-callback divergence).</b> The ABI callback typedef
/// returns <c>void</c> (<c>confluent_kafka.h:264</c>), so there is no completion for the core
/// to await; restoring Java's <c>void onComplete(Map, Exception)</c> faithfully in C# means a
/// sync method. This is the same divergence, for the same reason, as
/// <see cref="IConsumerRebalanceListener"/>.
/// </para>
/// <para>
/// <b>It runs on the core's callback-dispatcher thread, not the caller's task.</b>
/// <c>consumer-threading.md</c> §31's caller's-task model is the Rust core's contract; the C
/// ABI flattens it, and Python documents the identical divergence
/// (<c>consumer.py:686-696</c>). The dispatcher never runs two callbacks of the same consumer
/// concurrently. On a <c>MockConsumer</c> the core invokes it <b>inline during the commit
/// call</b> (with <c>exception</c> always <see langword="null"/>), so it has
/// already run by the time <c>CommitAsync</c> returns.
/// </para>
/// <para>
/// <b>Do not block on consumer progress from inside it.</b> A callback that waits for another
/// thread's <c>Poll</c> to return deadlocks the single dispatcher queue that must also deliver
/// every other callback of this consumer (<c>confluent_kafka.h:2478-2481</c>).
/// </para>
/// <para>
/// <b>It must not call back into its own consumer.</b> The plain consumer API is rejected with
/// a <see cref="KafkaException"/> (ConcurrentModification) because the core's access guard is
/// held for the duration of the commit. The sanctioned reentrancy path is the
/// <c>ConsumerHandle</c> escape hatch, which this binding does not yet expose; until then,
/// record what is needed and act on it after the operation returns.
/// </para>
/// <para>
/// <b>Throwing is NOT meaningful (unlike <see cref="IConsumerRebalanceListener"/>).</b> Java's
/// <c>onComplete</c> returns <c>void</c> and has nowhere to report a failure of its own, and
/// the ABI callback likewise has no error return channel. An exception thrown here is caught
/// at the interop boundary (it is never allowed to unwind into native code) and
/// <b>swallowed</b>, after being written to <see cref="System.Diagnostics.Trace"/> so the
/// failure leaves a trace — matching Python, which logs and swallows
/// (<c>consumer.py:363-372</c>). It does not fail the commit and it does not surface anywhere
/// else.
/// </para>
/// <para>
/// <b>Two distinct error surfaces.</b> The <see cref="KafkaException"/> thrown
/// <em>synchronously</em> by <c>CommitAsync</c> is a commit-<b>initiation</b> failure (the
/// commit never started); the <c>exception</c> delivered here is the
/// <b>commit's own</b> outcome.
/// </para>
/// </remarks>
public interface IOffsetCommitCallback
{
    /// <summary>
    /// Invoked when the commit completes (Java
    /// <c>onComplete(Map&lt;TopicPartition, OffsetAndMetadata&gt;, Exception)</c>) — exactly
    /// once per successful <c>CommitAsync</c> call.
    /// </summary>
    /// <param name="offsets">
    /// The offsets the commit applied to (never <see langword="null"/>; may be empty). An owned
    /// snapshot — nothing native-backed escapes the callback.
    /// </param>
    /// <param name="exception">
    /// The commit's outcome: <see langword="null"/> on success, mirroring Java's
    /// "exception == null means success".
    /// </param>
    void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata> offsets, KafkaException? exception);
}
