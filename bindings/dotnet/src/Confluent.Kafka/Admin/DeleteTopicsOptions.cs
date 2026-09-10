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
/// Options for <see cref="IAdmin.DeleteTopics"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.DeleteTopicsOptions</c>.
/// </summary>
/// <remarks>
/// A plain settable POCO rather than Java's fluent builder, and every default matches
/// Java's — the same shape and the same reasoning as
/// <see cref="CreateTopicsOptions"/>, which this type deliberately mirrors. The ABI has
/// no options handle, so this is destructured at the P/Invoke site.
/// </remarks>
public sealed class DeleteTopicsOptions
{
    /// <summary>
    /// The per-request timeout in milliseconds, or <see langword="null"/> to leave it
    /// unset so the client's <c>default.api.timeout.ms</c> applies — Java's
    /// <c>AbstractOptions.timeoutMs()</c>, an <c>Integer</c> that is likewise nullable.
    /// </summary>
    /// <remarks>
    /// Must not be negative. The ABI reads a negative <c>timeout_ms</c> as <em>unset</em>,
    /// so passing one would silently mean "use the client default" rather than the
    /// timeout asked for; <see cref="IAdmin.DeleteTopics"/> rejects it with
    /// <see cref="System.ArgumentOutOfRangeException"/> before any native call
    /// (ffi §B5). <see langword="null"/> is how you ask for the default.
    /// </remarks>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// Retry automatically when the broker reports a quota violation — Java's
    /// <c>retryOnQuotaViolation()</c>. Defaults to <b><see langword="true"/></b>, as
    /// Java's does; note this is the one option whose default is not the C# default for
    /// its type.
    /// </summary>
    public bool RetryOnQuotaViolation { get; set; } = true;
}
