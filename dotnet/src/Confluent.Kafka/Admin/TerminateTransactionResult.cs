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
/// The result of <see cref="IAdmin.ForceTerminateTransaction"/> — Java's
/// <c>org.apache.kafka.clients.admin.TerminateTransactionResult</c> (<c>:25</c>).
/// </summary>
/// <remarks>
/// ⚠ The sole member is <see cref="Result"/>, <b>not</b> <c>All()</c>: Java names it
/// <c>result()</c> (<c>:36</c>), and it wraps a scalar <c>KafkaFuture&lt;Void&gt;</c> rather
/// than a per-key map. The five sibling P8 results do answer <c>All()</c>; that asymmetry is
/// Java's and is deliberately kept.
/// </remarks>
public sealed class TerminateTransactionResult
{
    private readonly Task _result;

    internal TerminateTransactionResult(Task result)
    {
        _result = result;
    }

    /// <summary>
    /// Whether the transaction was terminated — Java's <c>result()</c> (<c>:36</c>).
    /// </summary>
    /// <returns>An awaitable that completes when the termination has.</returns>
    public Task Result() => _result;
}
