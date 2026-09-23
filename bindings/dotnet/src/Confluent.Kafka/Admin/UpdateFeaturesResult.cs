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
/// The result of <see cref="IAdmin.UpdateFeatures"/> — Java's
/// <c>org.apache.kafka.clients.admin.UpdateFeaturesResult</c>: one awaitable <b>per feature</b>
/// (<c>:29</c>).
/// </summary>
/// <remarks>
/// ⚠ See <see cref="AlterUserScramCredentialsResult"/> on the byte-identical accessor sets and
/// the wiring guard that keeps the two readers apart.
/// </remarks>
public sealed class UpdateFeaturesResult
{
    private readonly IReadOnlyDictionary<string, Task> _values;

    internal UpdateFeaturesResult(
        IReadOnlyDictionary<string, Task<bool>> values, IEqualityComparer<string> comparer)
    {
        Dictionary<string, Task> erased = new Dictionary<string, Task>(values.Count, comparer);
        foreach (KeyValuePair<string, Task<bool>> entry in values)
        {
            erased.Add(entry.Key, entry.Value);
        }

        _values = erased;
    }

    /// <summary>
    /// One awaitable per requested feature — Java's <c>values()</c> (<c>:39</c>).
    /// </summary>
    public IReadOnlyDictionary<string, Task> Values => _values;

    /// <summary>
    /// Completes when every feature's update has been applied — Java's <c>all()</c>
    /// (<c>:46</c>).
    /// </summary>
    /// <returns>A task representing the whole batch.</returns>
    public Task All() => Task.WhenAll(_values.Values);
}
