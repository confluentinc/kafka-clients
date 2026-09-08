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

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for <see cref="IAdmin.CreateTopics"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.CreateTopicsOptions</c>.
/// </summary>
/// <remarks>
/// <para>
/// A plain settable POCO rather than Java's fluent builder: C# object-initializer
/// syntax already reads the way Java's chained setters do, so a builder would add a
/// second spelling without adding meaning. Every default matches Java's, so
/// <c>options: null</c> at a call site behaves exactly like a freshly constructed
/// instance.
/// </para>
/// <para>
/// ⚠ The ABI has <b>no</b> options handle — it flattened every option to scalar
/// parameters — so this type is destructured at the P/Invoke site. Restoring it is the
/// binding's job: baking the ABI's flattening into the public surface is precisely what
/// the binding layer exists to undo.
/// </para>
/// </remarks>
public sealed class CreateTopicsOptions
{
    /// <summary>
    /// The per-request timeout in milliseconds, or <see langword="null"/> to leave it
    /// unset so the client's <c>default.api.timeout.ms</c> applies — Java's
    /// <c>AbstractOptions.timeoutMs()</c>, an <c>Integer</c> that is likewise nullable.
    /// </summary>
    /// <remarks>
    /// Must not be negative. The ABI reads a negative <c>timeout_ms</c> as <em>unset</em>,
    /// so passing one would silently mean "use the client default" rather than the
    /// timeout asked for; <see cref="IAdmin.CreateTopics"/> rejects it with
    /// <see cref="System.ArgumentOutOfRangeException"/> before any native call
    /// (ffi §B5). <see langword="null"/> is how you ask for the default.
    /// </remarks>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// Validate the request without actually creating the topics — Java's
    /// <c>validateOnly()</c>. Defaults to <see langword="false"/>, as Java's does.
    /// </summary>
    public bool ValidateOnly { get; set; }

    /// <summary>
    /// Retry automatically when the broker reports a quota violation — Java's
    /// <c>retryOnQuotaViolation()</c>. Defaults to <b><see langword="true"/></b>, as
    /// Java's does; note this is the one option whose default is not the C# default for
    /// its type.
    /// </summary>
    public bool RetryOnQuotaViolation { get; set; } = true;
}
