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
/// Options for <see cref="IAdmin.DeleteAcls"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.DeleteAclsOptions</c>.
/// </summary>
/// <remarks>
/// A plain settable POCO rather than Java's fluent builder, mirroring
/// <see cref="CreateAclsOptions"/>. Java's type adds nothing to <c>AbstractOptions</c>
/// (<c>DeleteAclsOptions.java:25</c>), so the timeout is the whole surface.
/// </remarks>
public sealed class DeleteAclsOptions
{
    /// <summary>
    /// The per-request timeout in milliseconds, or <see langword="null"/> to leave it unset
    /// so the client's <c>default.api.timeout.ms</c> applies — Java's
    /// <c>AbstractOptions.timeoutMs()</c>.
    /// </summary>
    /// <remarks>
    /// Must not be negative: the ABI reads a negative <c>timeout_ms</c> as <em>unset</em>,
    /// so passing one would silently mean "use the client default".
    /// <see cref="IAdmin.DeleteAcls"/> rejects it with
    /// <see cref="System.ArgumentOutOfRangeException"/> before any native call (ffi §B5).
    /// </remarks>
    public int? TimeoutMs { get; set; }
}
