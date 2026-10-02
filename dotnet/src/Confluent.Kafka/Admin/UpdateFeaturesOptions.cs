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
/// Options for <see cref="IAdmin.UpdateFeatures"/> — Java's
/// <c>org.apache.kafka.clients.admin.UpdateFeaturesOptions</c>.
/// </summary>
public sealed class UpdateFeaturesOptions
{
    /// <summary>
    /// The per-request timeout in milliseconds, or <see langword="null"/> to leave it unset —
    /// Java's <c>AbstractOptions.timeoutMs()</c>. Must not be negative.
    /// </summary>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// Whether to validate the updates without applying them — Java's <c>validateOnly()</c>
    /// (<c>:27</c>), default <see langword="false"/>.
    /// </summary>
    public bool ValidateOnly { get; set; }
}
