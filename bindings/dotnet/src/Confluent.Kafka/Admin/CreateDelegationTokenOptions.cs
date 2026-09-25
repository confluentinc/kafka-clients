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

using System;
using System.Collections.Generic;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Options for <see cref="IAdmin.CreateDelegationToken"/> — Java's
/// <c>org.apache.kafka.clients.admin.CreateDelegationTokenOptions</c>.
/// </summary>
/// <remarks>
/// Java's deprecated-since-4.0 <c>maxlifeTimeMs</c> pair (<c>:56</c>, <c>:70</c> — lowercase
/// <c>l</c>, sharing the backing field with the correctly spelled members) is not bound: it is
/// a Java typo kept for compatibility.
/// </remarks>
public sealed class CreateDelegationTokenOptions
{
    /// <summary>
    /// The per-request timeout in milliseconds, or <see langword="null"/> to leave it unset —
    /// Java's <c>AbstractOptions.timeoutMs()</c>. Must not be negative.
    /// </summary>
    public int? TimeoutMs { get; set; }

    /// <summary>
    /// The principals allowed to renew the token — Java's <c>renewers()</c> (<c>:39</c>).
    /// ⚠ <b>Never null</b> in Java (<c>:31</c> initializes it to an empty list), unlike
    /// <see cref="DescribeDelegationTokenOptions.Owners"/>: here an empty renewer list is a
    /// meaningful request rather than "unset".
    /// </summary>
    public IReadOnlyList<KafkaPrincipal> Renewers { get; set; } = Array.Empty<KafkaPrincipal>();

    /// <summary>
    /// The token owner, or <see langword="null"/> to use the authenticated principal — Java's
    /// <c>owner()</c> (<c>:48</c>, an <c>Optional</c>).
    /// </summary>
    public KafkaPrincipal? Owner { get; set; }

    /// <summary>
    /// The token's maximum lifetime in milliseconds, or <c>-1</c> for the server default —
    /// Java's <c>maxLifetimeMs()</c> (<c>:74</c>), default <c>-1</c>.
    /// </summary>
    /// <remarks>
    /// ⚠ This is <b>Java's own sentinel on the options object</b> and is passed through to the
    /// ABI verbatim — it is unrelated to <see cref="TimeoutMs"/>, whose unset form is a
    /// negative <c>timeout_ms</c> supplied by the binding.
    /// </remarks>
    public long MaxLifetimeMs { get; set; } = -1L;
}
