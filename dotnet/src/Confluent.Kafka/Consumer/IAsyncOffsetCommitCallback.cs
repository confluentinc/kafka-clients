// Copyright 2026 Confluent Inc.
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
using System.Threading.Tasks;
using Confluent.Kafka.Common;

namespace Confluent.Kafka.Consumer;

/// <summary>
/// The asynchronous form of <see cref="IOffsetCommitCallback"/>, accepted by
/// <see cref="IAsyncConsumer{TKey, TValue}.CommitAsync(IAsyncOffsetCommitCallback, System.Threading.CancellationToken)"/>
/// and its offsets-taking overload.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="OnComplete"/> is called on the consumer's <c>confluent-kafka-consumer-pump</c> thread, which does not
/// wait for the returned <see cref="Task"/>. The consumer call that delivers the callback (<c>Poll</c>,
/// <c>Commit</c>, <c>CommitAsync</c>, <c>Close</c>, …) waits until the <see cref="Task"/> ends, and the consumer
/// delivers the next commit callback only after it ended, so the callbacks start one at a time, in commit order.
/// </para>
/// <para>
/// A faulted or cancelled <see cref="Task"/>, or an exception thrown before one is returned, propagates out of the
/// call that delivered the callback, as a synchronous callback's exception does: <c>Commit</c> and
/// <c>CommitAsync</c> rethrow it as is, <c>Poll</c> wraps it in a <see cref="KafkaException"/>, and <c>Close</c> in
/// a <see cref="KafkaException"/> with the message "Failed to close kafka consumer".
/// </para>
/// <para>
/// A callback that a blocking commit runs, when a callback commits again through its consumer or a
/// <see cref="ConsumerHandle"/>, is reported at once instead, so the callbacks after it may start before it ends. The
/// call that delivered the outer callback waits until its <see cref="Task"/> ends, then throws its exception, wrapped
/// as above, instead of its own outcome; a <c>Poll</c> returns its records, and a cancelled call ends cancelled, and
/// the consumer's next asynchronous call throws the exception.
/// </para>
/// </remarks>
public interface IAsyncOffsetCommitCallback
{
    /// <summary>
    /// Invoked once when the commit completes (Java
    /// <c>onComplete(Map&lt;TopicPartition, OffsetAndMetadata&gt;, Exception)</c>).
    /// </summary>
    /// <param name="offsets">
    /// The offsets the commit applied to, or <see langword="null"/> when the commit failed or, on an
    /// <c>AsyncKafkaConsumer</c>, was given an empty offsets map; a mock consumer hands that commit's empty map.
    /// </param>
    /// <param name="exception">
    /// <see langword="null"/> on success; otherwise the commit's failure: a <see cref="KafkaException"/>, or the
    /// .NET exception the error maps to.
    /// </param>
    /// <returns>A <see cref="Task"/> whose end lets the call that delivered the callback go on.</returns>
    Task OnComplete(IReadOnlyDictionary<TopicPartition, OffsetAndMetadata>? offsets, Exception? exception);
}
