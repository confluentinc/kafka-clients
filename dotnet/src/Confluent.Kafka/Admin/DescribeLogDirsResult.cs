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
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.DescribeLogDirs"/> — the .NET realization of Java's
/// <c>DescribeLogDirsResult</c>: one future <b>per broker</b>, each yielding that
/// broker's log directories keyed by path (<c>DescribeLogDirsResult.java:41, :50</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The key is a bare <see cref="int"/> broker id</b> — the first scalar key in M15;
/// every earlier one was a string, a base64-parsed <see cref="Uuid"/>, or a composite.
/// </para>
/// <para>
/// ⚠ <b>Two different errors live in this RPC and mean different things.</b> A broker whose
/// query failed fails <em>its own</em> future (the header: "a per-broker failure is not
/// a call failure"). A <em>log directory</em> that is offline does <b>not</b> fail
/// anything — the future succeeds and the offending
/// <see cref="LogDirDescription.Error"/> is non-null. The two are also owned differently.
/// The per-broker error is <b>owned</b>: it arrives with that broker's own callback and is
/// freed once it has been copied into the <see cref="KafkaException"/>. The per-directory
/// error is <b>borrowed</b> from that broker's log-dir map and is never destroyed on its
/// own; it is copied out before the map is.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until its broker (or, for
/// <see cref="AllDescriptions"/>, every broker) has completed, and needs no thread-pool thread: the futures here are
/// composed on the waiting thread, never over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncDescribeLogDirsResult"/>
public sealed class DescribeLogDirsResult
{
    private readonly IReadOnlyDictionary<int, KafkaFuture<IReadOnlyDictionary<string, LogDirDescription>>> _descriptions;

    /// <summary>
    /// Wraps the per-broker futures — Java's package-private
    /// <c>DescribeLogDirsResult(Map&lt;Integer, KafkaFuture&lt;Map&lt;String, LogDirDescription&gt;&gt;&gt;)</c>
    /// (<c>:33</c>).
    /// </summary>
    internal DescribeLogDirsResult(
        IReadOnlyDictionary<int, KafkaFuture<IReadOnlyDictionary<string, LogDirDescription>>> descriptions)
    {
        _descriptions = descriptions;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latches: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static DescribeLogDirsResult From(
        KeyedAdminOperation<int, IReadOnlyDictionary<string, LogDirDescription>> operation) =>
        new DescribeLogDirsResult(SyncAdminFutures.Wrap(operation.SyncSources));

    /// <summary>
    /// One future per requested broker, each yielding that broker's log directories
    /// keyed by path — Java's <c>descriptions()</c> (<c>:41</c>).
    /// </summary>
    public IReadOnlyDictionary<int, KafkaFuture<IReadOnlyDictionary<string, LogDirDescription>>> Descriptions =>
        _descriptions;

    /// <summary>
    /// Every broker's log directories in one map, succeeding only if every broker answered
    /// without error — Java's <c>allDescriptions()</c> (<c>:50</c>).
    /// </summary>
    /// <returns>A future yielding the whole map, or failing with the first failure in key order.</returns>
    /// <remarks>
    /// Every future is waited for first, so a failure fails the aggregate with the first failure in key order exactly
    /// as Java's <c>allOf(...).thenApply(...)</c> chain does, rather than whichever key a reader happens to reach
    /// first. The map is keyed with the default <see cref="int"/> comparer — Java's own <c>Integer</c>
    /// <c>HashMap</c> key semantics. Each call builds a new aggregate.
    /// </remarks>
    public KafkaFuture<IReadOnlyDictionary<int, IReadOnlyDictionary<string, LogDirDescription>>> AllDescriptions() =>
        SyncAdminFutures.GatherAll(_descriptions, EqualityComparer<int>.Default);
}
