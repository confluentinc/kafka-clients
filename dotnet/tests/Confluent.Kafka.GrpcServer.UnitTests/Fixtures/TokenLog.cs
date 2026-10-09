// Copyright 2026 Confluent Inc.
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

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Confluent.Kafka.GrpcServer.UnitTests.Fixtures;

/// <summary>
/// Every <see cref="CancellationToken"/> the async servicer handed a client double, by operation
/// (T12's async half, D6 / D7). A token is recorded by whether it <b>can</b> fire
/// (<see cref="CancellationToken.CanBeCanceled"/>), not by whether it did: the rule is that no
/// token that can fire reaches the client at all, so the check does not depend on a stop
/// happening to land inside a call.
/// </summary>
internal sealed class TokenLog
{
    private readonly ConcurrentQueue<(string Operation, bool CanBeCanceled)> _calls =
        new ConcurrentQueue<(string Operation, bool CanBeCanceled)>();

    /// <summary>Every operation that was handed a token, once each.</summary>
    internal IReadOnlyCollection<string> Operations => _calls.Select(c => c.Operation).Distinct().ToList();

    /// <summary>The operations, one entry per call, that were handed a token that can fire.</summary>
    internal IReadOnlyList<string> Cancelable => _calls.Where(c => c.CanBeCanceled).Select(c => c.Operation).ToList();

    /// <summary>Records the token one call received.</summary>
    internal void Record(string operation, CancellationToken token) => _calls.Enqueue((operation, token.CanBeCanceled));
}
