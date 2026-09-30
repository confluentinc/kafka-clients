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

using Token = Confluent.Kafka.DelegationToken;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.CreateDelegationToken"/> — Java's
/// <c>org.apache.kafka.clients.admin.CreateDelegationTokenResult</c>: one awaitable over the
/// issued token (<c>:27</c>).
/// </summary>
public sealed class CreateDelegationTokenResult
{
    private readonly Task<Token> _token;

    internal CreateDelegationTokenResult(Task<Token> token)
    {
        _token = token;
    }

    /// <summary>The issued token — Java's <c>delegationToken()</c> (<c>:36</c>).</summary>
    /// <returns>An awaitable over the token.</returns>
    public Task<Token> DelegationToken() => _token;
}
