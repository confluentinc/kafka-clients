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
using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.AlterUserScramCredentials"/> — Java's
/// <c>org.apache.kafka.clients.admin.AlterUserScramCredentialsResult</c>: one future
/// <b>per user</b> (<c>:31</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ This type sits on an accessor set byte-identical to <see cref="UpdateFeaturesResult"/>'s
/// (and to P6's <c>CreateAclsResult</c>), so a cross-wired reader would return a plausible
/// answer; the readers are pinned apart by a wiring guard in the unit tests.
/// </para>
/// <para>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until its user (or, for
/// <see cref="All"/>, every user) has completed, and needs no thread-pool thread: the futures here are composed
/// on the waiting thread, never over a <c>Task</c>.
/// </para>
/// </remarks>
/// <seealso cref="AsyncAlterUserScramCredentialsResult"/>
public sealed class AlterUserScramCredentialsResult
{
    private readonly IReadOnlyDictionary<string, KafkaFuture> _values;

    /// <summary>
    /// Wraps one future per user — Java's <b>public</b>
    /// <c>AlterUserScramCredentialsResult(Map&lt;String, KafkaFuture&lt;Void&gt;&gt;)</c>
    /// (<c>AlterUserScramCredentialsResult.java:38</c>), which a caller can use to fabricate a
    /// result, for a test double say.
    /// </summary>
    /// <remarks>
    /// <b>Internal on purpose</b>, unlike Java's: a recorded Java-parity deviation, follow-up FU-5 in
    /// <c>design/current/STATUS.md</c>. The dictionary is held by reference, as Java holds an unmodifiable view
    /// over its map (<c>:39</c>), so <see cref="Values"/> and <see cref="All"/> read whatever it holds when they
    /// are called.
    /// </remarks>
    /// <param name="futures">One future per user, keyed by user name.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="futures"/> is null — where Java stores the null and throws
    /// <c>NullPointerException</c> on first use.
    /// </exception>
    internal AlterUserScramCredentialsResult(IReadOnlyDictionary<string, KafkaFuture> futures)
    {
        _values = futures ?? throw new ArgumentNullException(nameof(futures));
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latches: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    /// <remarks>
    /// Presents the bridge's <c>bool</c> latches as valueless <see cref="KafkaFuture"/>s: the <c>bool</c> is the
    /// void bridge's internal success token (result shape 2 — the ABI has no <c>get_value</c> here, so a null
    /// per-key error <em>is</em> the success value) and must not reach the public surface. Each void future is erased
    /// over its key's <b>same</b> latch — no second completion — with the bridge's own comparer, so the view and the
    /// sources cannot disagree about key identity.
    /// </remarks>
    internal static AlterUserScramCredentialsResult From(VoidKeyedAdminOperation<string> operation) =>
        new AlterUserScramCredentialsResult(SyncAdminFutures.Erase(operation.SyncSources, operation.KeyComparer));

    /// <summary>One future per requested user — Java's <c>values()</c> (<c>:46</c>).</summary>
    public IReadOnlyDictionary<string, KafkaFuture> Values => _values;

    /// <summary>
    /// Completes when every user's alteration has been applied, and fails with the first failure in key order if
    /// any user failed — Java's <c>all()</c> (<c>:53</c>).
    /// </summary>
    /// <returns>A future representing the whole batch. Its <c>Get()</c> waits for every user.</returns>
    public KafkaFuture All() => SyncAdminFutures.AllOf(_values.Values);
}
