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
/// Options for <see cref="IAdmin.CreateAcls"/> — the .NET realization of Java's
/// <c>org.apache.kafka.clients.admin.CreateAclsOptions</c>.
/// </summary>
/// <remarks>
/// A plain settable POCO rather than Java's fluent builder, mirroring
/// <see cref="DeleteTopicsOptions"/>. Java's type adds nothing to
/// <c>AbstractOptions</c> (<c>CreateAclsOptions.java:25</c>), so the timeout is the whole
/// surface.
/// </remarks>
public sealed class CreateAclsOptions
{
    /// <summary>
    /// The per-request timeout in milliseconds, or <see langword="null"/> to leave it unset
    /// so the client's <c>default.api.timeout.ms</c> applies — Java's
    /// <c>AbstractOptions.timeoutMs()</c>.
    /// </summary>
    /// <remarks>
    /// For a negative value, see <see cref="CreateTopicsOptions.TimeoutMs"/>.
    /// </remarks>
    public int? TimeoutMs { get; set; }
}
