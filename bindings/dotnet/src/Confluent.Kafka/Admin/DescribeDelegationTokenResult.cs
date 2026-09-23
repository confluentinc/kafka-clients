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
using System.Linq;
using System.Threading.Tasks;

using Token = Confluent.Kafka.DelegationToken;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.DescribeDelegationToken"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeDelegationTokenResult</c>: one awaitable over the
/// whole token list (<c>:29</c>). Java declares no <c>all()</c> here.
/// </summary>
public sealed class DescribeDelegationTokenResult
{
    private readonly Task<IReadOnlyList<Token>> _tokens;

    internal DescribeDelegationTokenResult(Task<IReadOnlyCollection<Token>> tokens)
    {
        _tokens = Project(tokens);
    }

    /// <summary>The described tokens — Java's <c>delegationTokens()</c> (<c>:38</c>).</summary>
    /// <returns>An awaitable over the token list, in the order the ABI delivered it.</returns>
    public Task<IReadOnlyList<Token>> DelegationTokens() => _tokens;

    // Java's accessor is List-typed (:38) while the collection walker produces
    // IReadOnlyCollection, so the one list it built is re-presented rather than rebuilt.
    private static async Task<IReadOnlyList<Token>> Project(Task<IReadOnlyCollection<Token>> tokens)
    {
        IReadOnlyCollection<Token> described = await tokens.ConfigureAwait(false);
        return described as IReadOnlyList<Token> ?? described.ToList();
    }
}
