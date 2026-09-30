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
/// The result of <see cref="IAdmin.AlterUserScramCredentials"/> — Java's
/// <c>org.apache.kafka.clients.admin.AlterUserScramCredentialsResult</c>: one awaitable
/// <b>per user</b> (<c>:31</c>).
/// </summary>
/// <remarks>
/// ⚠ This type sits on an accessor set byte-identical to <see cref="UpdateFeaturesResult"/>'s
/// (and to P6's <c>CreateAclsResult</c>), so a cross-wired reader would return a plausible
/// answer; the readers are pinned apart by a wiring guard in the unit tests.
/// <para>
/// Per-user <em>granularity</em> is preserved; per-user <em>timing independence</em> is not —
/// the recorded limitation every keyed admin result shares.
/// </para>
/// </remarks>
public sealed class AlterUserScramCredentialsResult
{
    private readonly IReadOnlyDictionary<string, Task> _values;

    internal AlterUserScramCredentialsResult(
        IReadOnlyDictionary<string, Task<bool>> values, IEqualityComparer<string> comparer)
    {
        // The bool is the void bridge's internal success token and must not reach the public
        // surface; the erasure is a reference upcast, so each entry is the same Task instance.
        Dictionary<string, Task> erased = new Dictionary<string, Task>(values.Count, comparer);
        foreach (KeyValuePair<string, Task<bool>> entry in values)
        {
            erased.Add(entry.Key, entry.Value);
        }

        _values = erased;
    }

    /// <summary>One awaitable per requested user — Java's <c>values()</c> (<c>:46</c>).</summary>
    public IReadOnlyDictionary<string, Task> Values => _values;

    /// <summary>
    /// Completes when every user's alteration has been applied — Java's <c>all()</c>
    /// (<c>:53</c>).
    /// </summary>
    /// <returns>A task representing the whole batch.</returns>
    public Task All() => Task.WhenAll(_values.Values);
}
