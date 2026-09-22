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

using System.Threading.Tasks;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.RenewDelegationToken"/> — Java's
/// <c>org.apache.kafka.clients.admin.RenewDelegationTokenResult</c>: one awaitable over the
/// token's new expiry timestamp (<c>:26</c>).
/// </summary>
/// <remarks>
/// ⚠ This type and <see cref="ExpireDelegationTokenResult"/> sit on <b>byte-identical</b> ABI
/// accessor sets, so a cross-wired reader would return a plausible answer for either. Their
/// readers are pinned apart by a wiring guard in the unit tests.
/// </remarks>
public sealed class RenewDelegationTokenResult
{
    private readonly Task<long> _expiryTimestamp;

    internal RenewDelegationTokenResult(Task<long> expiryTimestamp)
    {
        _expiryTimestamp = expiryTimestamp;
    }

    /// <summary>
    /// The new expiry timestamp, in ms since the epoch — Java's <c>expiryTimestamp()</c>
    /// (<c>:35</c>).
    /// </summary>
    /// <returns>An awaitable over the timestamp.</returns>
    public Task<long> ExpiryTimestamp() => _expiryTimestamp;
}
