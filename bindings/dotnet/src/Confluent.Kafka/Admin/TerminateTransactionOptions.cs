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

using System.Globalization;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for force-terminating a transaction — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.TerminateTransactionOptions</c>
/// (<c>TerminateTransactionOptions.java:23</c>).
/// </summary>
/// <remarks>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks/para[1]"/>
/// <para>
/// Java declares no option of its own; the timeout is all there is. ⚠ Java has no
/// <c>ForceTerminateTransactionOptions</c> — this class is named for the result, not for the
/// <c>forceTerminateTransaction</c> RPC that takes it.
/// </para>
/// </remarks>
public sealed class TerminateTransactionOptions
{
    /// <inheritdoc cref="CreateTopicsOptions.TimeoutMs"/>
    public int? TimeoutMs { get; set; }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:26</c>).</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "TerminateTransactionOptions{{timeoutMs={0}}}",
            TimeoutMs.HasValue ? TimeoutMs.Value.ToString(CultureInfo.InvariantCulture) : "null");
}
