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

using Confluent.Kafka.Admin;

namespace Confluent.Kafka.Internal;

/// <summary>
/// One row of the flattened <c>describeUserScramCredentials</c> table, copied out of the result
/// root before it is destroyed. The <c>DescribeClusterSnapshot</c> precedent, for a table.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Why a snapshot rather than one awaitable per key.</b> Java stores a single
/// <c>KafkaFuture&lt;DescribeUserScramCredentialsResponseData&gt;</c>
/// (<c>DescribeUserScramCredentialsResult.java:37</c>) — raw protocol data, not a map — and
/// derives all three accessors from it, each with its own treatment of a per-user error. Two
/// things follow: a per-key bridge cannot serve, because an empty user list describes
/// <b>every</b> user (<c>confluent_kafka.h:8784-8785</c>) so the keys are discovered from the
/// response rather than known before the submit; and the per-user error is row <em>data</em>
/// here, not a per-key fault, exactly as <c>electLeaders</c>' <c>get_error(i)</c> is a map
/// value. The Java return type decides the shape.
/// </para>
/// <para>
/// ⚠ <b>Known divergence (M15/P7 D44), carried to M15/P9.</b> Stated once, on the public
/// <see cref="Confluent.Kafka.Admin.DescribeUserScramCredentialsResult"/> — which is where a
/// user can read it.
/// </para>
/// </remarks>
internal sealed class UserScramCredentialEntry
{
    internal UserScramCredentialEntry(string user, KafkaException? error, UserScramCredentialsDescription description)
    {
        User = user;
        Error = error;
        Description = description;
    }

    /// <summary>The described user.</summary>
    internal string User { get; }

    /// <summary>That user's failure, or <see langword="null"/> if it was described.</summary>
    internal KafkaException? Error { get; }

    /// <summary>That user's credentials; empty when <see cref="Error"/> is set.</summary>
    internal UserScramCredentialsDescription Description { get; }
}
