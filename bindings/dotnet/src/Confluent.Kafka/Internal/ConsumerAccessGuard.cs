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

using System.Threading;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The managed one-operation-in-flight guard (ffi-marshalling.md §B5). A Kafka
/// consumer is <b>not</b> safe for multi-threaded access — at most one operation
/// may be in progress at a time. This <see cref="Interlocked"/> flag <b>mirrors</b>
/// the Rust core's own access guard (it does not replace it); it exists so a
/// concurrent misuse surfaces as the <em>right .NET exception type</em> before the
/// P/Invoke, rather than as a race into the core.
/// </summary>
/// <remarks>
/// <para>
/// The rejection type splits by the kind of operation, matching the Java client
/// (and the Python sibling) exactly (ffi §B5, CLAUDE.md §3):
/// </para>
/// <list type="bullet">
/// <item>
/// a concurrent <b>async op</b> (<c>SubscribeAsync</c> / <c>SeekAsync</c> / …) →
/// a flat <see cref="KafkaException"/> (Java <c>ConcurrentModificationException</c>
/// semantics);
/// </item>
/// <item>
/// a concurrent <b>sync state read</b> (group metadata / assignment / …) →
/// <see cref="System.InvalidOperationException"/> ("not safe for multi-threaded
/// access").
/// </item>
/// </list>
/// <para>
/// An async op holds the guard from submission until its completion callback fires
/// (the whole submit→fire window); the callback releases it via
/// <see cref="Release"/> just <em>before</em> completing the awaiter's
/// <c>Task</c>, so an <c>await</c>-then-resubmit from the continuation does not hit
/// the one-op rejection. A sync state read acquires and releases the guard within a
/// single synchronous critical section.
/// </para>
/// </remarks>
internal sealed class ConsumerAccessGuard
{
    private const int Free = 0;
    private const int Held = 1;

    private int _state;

    /// <summary>
    /// Acquires the guard for an <b>async operation</b>. Throws
    /// <see cref="KafkaException"/> (concurrent-modification semantics) if another
    /// operation or state read already holds it. On success the caller owns the
    /// guard until it calls <see cref="Release"/> (from the completion callback).
    /// </summary>
    internal void EnterOperation()
    {
        if (Interlocked.CompareExchange(ref _state, Held, Free) != Free)
        {
            throw new KafkaException(
                "KafkaConsumer is not safe for multi-threaded access.");
        }
    }

    /// <summary>
    /// Acquires the guard for a <b>synchronous state read</b>. Throws
    /// <see cref="System.InvalidOperationException"/> if another operation or state
    /// read already holds it. On success the caller owns the guard until it calls
    /// <see cref="Release"/>.
    /// </summary>
    internal void EnterStateRead()
    {
        if (Interlocked.CompareExchange(ref _state, Held, Free) != Free)
        {
            throw new System.InvalidOperationException(
                "KafkaConsumer is not safe for multi-threaded access.");
        }
    }

    /// <summary>
    /// Releases the guard. Idempotent with respect to the flag value (a redundant
    /// release simply leaves it <see cref="Free"/>); the caller is responsible for
    /// releasing exactly once per successful enter.
    /// </summary>
    internal void Release() => Interlocked.Exchange(ref _state, Free);
}
