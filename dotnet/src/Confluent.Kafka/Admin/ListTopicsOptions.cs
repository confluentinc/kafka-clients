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
/// Options for <see cref="IAdmin.ListTopics"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.ListTopicsOptions</c>.
/// </summary>
/// <inheritdoc cref="CreateTopicsOptions" path="/remarks"/>
public sealed class ListTopicsOptions
{
    /// <summary>
    /// The per-request timeout in milliseconds, or <see langword="null"/> to leave it
    /// unset so the client's <c>default.api.timeout.ms</c> applies — Java's
    /// <c>AbstractOptions.timeoutMs()</c>, an <c>Integer</c> that is likewise nullable.
    /// </summary>
    /// <remarks>
    /// Must not be negative. The ABI reads a negative <c>timeout_ms</c> as <em>unset</em>,
    /// so passing one would silently mean "use the client default" rather than the
    /// timeout asked for; <see cref="IAdmin.ListTopics"/> rejects it with
    /// <see cref="System.ArgumentOutOfRangeException"/> before any native call
    /// (ffi §B5).
    /// </remarks>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// Include internal topics such as <c>__consumer_offsets</c> — Java's
    /// <c>listInternal(boolean)</c> / <c>shouldListInternal()</c>. Defaults to
    /// <see langword="false"/>, as Java's does.
    /// </summary>
    public bool ListInternal { get; set; }
}
