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

using Confluent.Kafka.Internal;

using Token = Confluent.Kafka.DelegationToken;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.CreateDelegationToken"/> — Java's
/// <c>org.apache.kafka.clients.admin.CreateDelegationTokenResult</c>: one future over the
/// issued token (<c>:27</c>).
/// </summary>
/// <remarks>
/// <b>Every <c>Get()</c> blocks the calling thread</b> on a managed latch until the token has been issued, and
/// needs no thread-pool thread: the future here is the operation's own latch, never a <c>Task</c>.
/// </remarks>
/// <seealso cref="AsyncCreateDelegationTokenResult"/>
public sealed class CreateDelegationTokenResult
{
    private readonly KafkaFuture<Token> _token;

    internal CreateDelegationTokenResult(KafkaFuture<Token> token)
    {
        _token = token;
    }

    /// <summary>
    /// Builds this result over an <see cref="AdminFlavour.Sync"/> operation's latch: the one place it is built
    /// from an admin operation (M15/P14).
    /// </summary>
    internal static CreateDelegationTokenResult From(SingleAdminOperation<DelegationToken> operation) =>
        new CreateDelegationTokenResult(new KafkaFuture<Token>(operation.SyncSource));

    /// <summary>The issued token — Java's <c>delegationToken()</c> (<c>:36</c>).</summary>
    /// <returns>A future over the token.</returns>
    /// <remarks>
    /// This is a future over the operation's own latch, so every call returns an <b>equal</b> future — no second
    /// completion.
    /// </remarks>
    public KafkaFuture<Token> DelegationToken() => _token;
}
