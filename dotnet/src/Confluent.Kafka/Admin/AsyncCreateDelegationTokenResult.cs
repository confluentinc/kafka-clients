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

using Confluent.Kafka.Common;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;
using Token = Confluent.Kafka.Common.Security.Token.Delegation.DelegationToken;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAsyncAdmin.CreateDelegationToken"/> — Java's
/// <c>org.apache.kafka.clients.admin.CreateDelegationTokenResult</c>: one future over the
/// issued token.
/// </summary>
/// <seealso cref="CreateDelegationTokenResult"/>
public sealed class AsyncCreateDelegationTokenResult
{
    private readonly KafkaFutureImpl<Token> _token;
    private readonly SafeAdminResultHandle _native;

    private AsyncCreateDelegationTokenResult(KafkaFutureImpl<Token> token, SafeAdminResultHandle native)
    {
        _token = token;
        _native = native;
    }

    internal SafeAdminResultHandle Native => _native;

    /// <summary>The issued token — Java's <c>delegationToken()</c>.</summary>
    /// <returns>A future over the token.</returns>
    /// <remarks>Every call returns an equal future, as Java's accessor returns its stored field.</remarks>
    public AsyncKafkaFuture<Token> DelegationToken() => new AsyncKafkaFuture<Token>(_token);

    internal static AsyncCreateDelegationTokenResult OfClient(
        (KafkaFutureImpl<Token> Token, SafeAdminResultHandle Result) native) =>
        new AsyncCreateDelegationTokenResult(native.Token, native.Result);
}
