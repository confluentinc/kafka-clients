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
/// The result of <see cref="IAdmin.ExpireDelegationToken"/> — Java's
/// <c>org.apache.kafka.clients.admin.ExpireDelegationTokenResult</c>: one awaitable over the
/// token's expiry timestamp (<c>:26</c>).
/// </summary>
/// <remarks>
/// ⚠ See <see cref="RenewDelegationTokenResult"/> on the byte-identical accessor sets and the
/// wiring guard that keeps the two readers apart.
/// </remarks>
public sealed class ExpireDelegationTokenResult
{
    private readonly Task<long> _expiryTimestamp;

    internal ExpireDelegationTokenResult(Task<long> expiryTimestamp)
    {
        _expiryTimestamp = expiryTimestamp;
    }

    /// <summary>
    /// The expiry timestamp, in ms since the epoch — Java's <c>expiryTimestamp()</c>
    /// (<c>:35</c>).
    /// </summary>
    /// <returns>An awaitable over the timestamp.</returns>
    public Task<long> ExpiryTimestamp() => _expiryTimestamp;
}
