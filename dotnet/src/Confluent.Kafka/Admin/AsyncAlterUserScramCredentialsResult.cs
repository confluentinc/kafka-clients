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
using System.Threading.Tasks;
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.AlterUserScramCredentials"/> — Java's
/// <c>org.apache.kafka.clients.admin.AlterUserScramCredentialsResult</c>: one awaitable
/// <b>per user</b> (<c>:31</c>).
/// </summary>
/// <remarks>
/// ⚠ This type sits on an accessor set byte-identical to <see cref="AsyncUpdateFeaturesResult"/>'s
/// (and to P6's <c>CreateAclsResult</c>), so a cross-wired reader would return a plausible
/// answer; the readers are pinned apart by a wiring guard in the unit tests.
/// </remarks>
/// <seealso cref="AlterUserScramCredentialsResult"/>
public sealed class AsyncAlterUserScramCredentialsResult
{
    private readonly IReadOnlyDictionary<string, AsyncKafkaFuture> _values;

    /// <summary>
    /// Wraps one awaitable per user — Java's <b>public</b>
    /// <c>AlterUserScramCredentialsResult(Map&lt;String, KafkaFuture&lt;Void&gt;&gt;)</c>
    /// (<c>AlterUserScramCredentialsResult.java:38</c>), which a caller can use to fabricate a
    /// result, for a test double say.
    /// </summary>
    /// <remarks>
    /// <b>Internal on purpose</b>, unlike Java's: a recorded Java-parity deviation, follow-up FU-5 in
    /// <c>design/current/STATUS.md</c>. The dictionary is held by reference, as Java holds its map, so
    /// <see cref="Values"/> and <see cref="All"/> read whatever it holds when they are called.
    /// </remarks>
    /// <param name="futures">One awaitable per user, keyed by user name.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="futures"/> is null — where Java stores the null and throws
    /// <c>NullPointerException</c> on first use.
    /// </exception>
    internal AsyncAlterUserScramCredentialsResult(IReadOnlyDictionary<string, AsyncKafkaFuture> futures)
    {
        _values = futures ?? throw new ArgumentNullException(nameof(futures));
    }

    internal AsyncAlterUserScramCredentialsResult(
        IReadOnlyDictionary<string, Task<bool>> values, IEqualityComparer<string> comparer)
    {
        // The bool is the void bridge's internal success token and must not reach the public
        // surface; each entry wraps the same Task instance.
        Dictionary<string, AsyncKafkaFuture> erased = new Dictionary<string, AsyncKafkaFuture>(values.Count, comparer);
        foreach (KeyValuePair<string, Task<bool>> entry in values)
        {
            erased.Add(entry.Key, new AsyncKafkaFuture(entry.Value));
        }

        _values = erased;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Async"/> operation's tasks: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static AsyncAlterUserScramCredentialsResult From(VoidKeyedAdminOperation<string> operation) =>
        new AsyncAlterUserScramCredentialsResult(operation.Tasks, operation.KeyComparer);

    /// <summary>One awaitable per requested user — Java's <c>values()</c> (<c>:46</c>).</summary>
    public IReadOnlyDictionary<string, AsyncKafkaFuture> Values => _values;

    /// <summary>
    /// Completes when every user's alteration has been applied — Java's <c>all()</c>
    /// (<c>:53</c>).
    /// </summary>
    /// <returns>A future representing the whole batch.</returns>
    public AsyncKafkaFuture All() => new AsyncKafkaFuture(AsyncAdminFutures.WhenAll(_values.Values));
}
