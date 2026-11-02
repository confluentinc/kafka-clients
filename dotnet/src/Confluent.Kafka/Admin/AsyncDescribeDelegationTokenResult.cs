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
using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;
using Token = Confluent.Kafka.Common.Security.Token.Delegation.DelegationToken;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.DescribeDelegationToken"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeDelegationTokenResult</c>: one future over the
/// whole token list. Java declares no <c>all()</c> here.
/// </summary>
/// <seealso cref="DescribeDelegationTokenResult"/>
public sealed class AsyncDescribeDelegationTokenResult
{
    private readonly KafkaFutureImpl<IReadOnlyList<Token>> _tokens;
    private readonly SafeAdminResultHandle _native;

    private AsyncDescribeDelegationTokenResult(KafkaFutureImpl<IReadOnlyList<Token>> tokens, SafeAdminResultHandle native)
    {
        _tokens = tokens;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>The described tokens — Java's <c>delegationTokens()</c>.</summary>
    /// <returns>A future over the token list, in the order the core delivered it.</returns>
    /// <remarks>Every call returns an equal future, as Java's accessor returns its stored field.</remarks>
    public AsyncKafkaFuture<IReadOnlyList<Token>> DelegationTokens() =>
        new AsyncKafkaFuture<IReadOnlyList<Token>>(_tokens);

    internal static AsyncDescribeDelegationTokenResult OfClient(
        (KafkaFutureImpl<IReadOnlyList<Token>> Tokens, SafeAdminResultHandle Result) native) =>
        new AsyncDescribeDelegationTokenResult(native.Tokens, native.Result);
}
