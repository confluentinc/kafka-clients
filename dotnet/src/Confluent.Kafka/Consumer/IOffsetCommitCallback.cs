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
using Confluent.Kafka.Common;

namespace Confluent.Kafka.Consumer;

/// <summary>
/// The completion callback of an asynchronous offset commit (Java's <c>OffsetCommitCallback</c>), passed to
/// <see cref="IConsumer{TKey, TValue}.CommitAsync(IOffsetCommitCallback)"/>,
/// <see cref="IAsyncConsumer{TKey, TValue}.CommitAsync(IOffsetCommitCallback, System.Threading.CancellationToken)"/>
/// and their offsets-taking overloads.
/// </summary>
/// <remarks>
/// <para>
/// On <see cref="IConsumer{TKey, TValue}"/> it runs on the caller's thread, inside the consumer call that delivers
/// it (<c>Poll</c>, <c>Commit</c>, <c>CommitAsync</c>, <c>Close</c>, …); on <see cref="IAsyncConsumer{TKey, TValue}"/>,
/// on the consumer's <c>confluent-kafka-consumer-pump</c> thread. A call back into the consumer from inside it goes
/// through the consumer's own <see cref="ConsumerHandle"/>, as from a rebalance listener
/// (<see cref="IConsumerRebalanceListener"/>).
/// </para>
/// <para>
/// An exception it throws propagates out of the consumer call that delivered it: <c>Commit</c> and
/// <c>CommitAsync</c> rethrow it as is, <c>Poll</c> wraps it in a <see cref="KafkaException"/>, and <c>Close</c>
/// in a <see cref="KafkaException"/> with the message "Failed to close kafka consumer". The callbacks queued behind
/// it run at the next such call.
/// </para>
/// </remarks>
public interface IOffsetCommitCallback
{
    /// <summary>
    /// Invoked once when the commit completes (Java
    /// <c>onComplete(Map&lt;TopicPartition, OffsetAndMetadata&gt;, Exception)</c>).
    /// </summary>
    /// <param name="offsets">
    /// The offsets the commit applied to, or <see langword="null"/> when the commit failed or, on a
    /// <c>KafkaConsumer</c>, was given an empty offsets map; a mock consumer hands that commit's empty map.
    /// </param>
    /// <param name="exception">
    /// <see langword="null"/> on success; otherwise the commit's failure: a <see cref="KafkaException"/>, or the
    /// .NET exception the error maps to.
    /// </param>
    void OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception);
}
