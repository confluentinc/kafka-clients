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
using System.Threading.Tasks;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.DescribeLogDirs"/> — the .NET realization of Java's
/// <c>DescribeLogDirsResult</c>: one awaitable <b>per broker</b>, each yielding that
/// broker's log directories keyed by path (<c>DescribeLogDirsResult.java:41, :50</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>The key is a bare <see cref="int"/> broker id</b> — the first scalar key in M15;
/// every earlier one was a string, a base64-parsed <see cref="Uuid"/>, or a composite.
/// </para>
/// <para>
/// ⚠ <b>Two different errors live in this RPC and mean different things.</b> A broker whose
/// query failed faults <em>its own</em> awaitable (the header: "a per-broker failure is not
/// a call failure"). A <em>log directory</em> that is offline does <b>not</b> fault
/// anything — the awaitable succeeds and the offending
/// <see cref="LogDirDescription.Error"/> is non-null. Both underlying errors are
/// <b>borrowed</b> from the result root and neither is ever destroyed.
/// </para>
/// <para>
/// <b>Deviation from Java, recorded.</b> Per-broker <em>granularity</em> is fully
/// preserved, but per-broker <em>timing independence</em> is not: all the
/// <see cref="Task"/>s complete at the same instant, because the C ABI has no future type
/// and resolves every key together before reporting. The same recorded limitation as
/// <see cref="CreateTopicsResult"/>, for the same reason.
/// </para>
/// </remarks>
public sealed class DescribeLogDirsResult
{
    private readonly IReadOnlyDictionary<int, Task<IReadOnlyDictionary<string, LogDirDescription>>> _descriptions;

    /// <summary>
    /// Wraps the per-broker awaitables — Java's package-private
    /// <c>DescribeLogDirsResult(Map&lt;Integer, KafkaFuture&lt;Map&lt;String, LogDirDescription&gt;&gt;&gt;)</c>
    /// (<c>:33</c>).
    /// </summary>
    internal DescribeLogDirsResult(
        IReadOnlyDictionary<int, Task<IReadOnlyDictionary<string, LogDirDescription>>> descriptions)
    {
        _descriptions = descriptions;
    }

    /// <summary>
    /// One awaitable per requested broker, each yielding that broker's log directories
    /// keyed by path — Java's <c>descriptions()</c> (<c>:41</c>).
    /// </summary>
    public IReadOnlyDictionary<int, Task<IReadOnlyDictionary<string, LogDirDescription>>> Descriptions =>
        _descriptions;

    /// <summary>
    /// Every broker's log directories in one map, succeeding only if every broker answered
    /// without error — Java's <c>allDescriptions()</c> (<c>:50</c>).
    /// </summary>
    /// <returns>A task yielding the whole map, or faulting with the first failure.</returns>
    public Task<IReadOnlyDictionary<int, IReadOnlyDictionary<string, LogDirDescription>>> AllDescriptions() =>
        Gather(_descriptions);

    private static async Task<IReadOnlyDictionary<int, IReadOnlyDictionary<string, LogDirDescription>>> Gather(
        IReadOnlyDictionary<int, Task<IReadOnlyDictionary<string, LogDirDescription>>> descriptions)
    {
        // WhenAll first, so a failure faults this task with the first failure exactly as
        // Java's `allOf(...).thenApply(...)` chain does, rather than surfacing whichever
        // key happens to be enumerated first.
        await Task.WhenAll(descriptions.Values).ConfigureAwait(false);

        Dictionary<int, IReadOnlyDictionary<string, LogDirDescription>> all =
            new Dictionary<int, IReadOnlyDictionary<string, LogDirDescription>>(descriptions.Count);
        foreach (KeyValuePair<int, Task<IReadOnlyDictionary<string, LogDirDescription>>> entry in descriptions)
        {
            all.Add(entry.Key, entry.Value.Result);
        }

        return all;
    }
}
