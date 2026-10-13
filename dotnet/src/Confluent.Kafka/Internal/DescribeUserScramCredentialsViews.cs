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

using System.Collections.Generic;

using Confluent.Kafka.Admin;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The three <c>describeUserScramCredentials</c> views as the completion callback left
/// them: <c>all()</c> and <c>users()</c> copied out eagerly, and the retained native root
/// that serves <c>description(user)</c> on demand.
/// </summary>
/// <remarks>
/// <c>all()</c> and <c>users()</c> are precomputed C-side and are cheap copy-outs, so they
/// are taken in the callback; <c>description(user)</c> is evaluated per call against
/// <see cref="Root"/>, which is why the root is retained instead of destroyed there. It
/// cannot be pre-materialized: when <c>all()</c> faults its rows are unavailable and
/// <c>users()</c> excludes RESOURCE_NOT_FOUND users, so an eager pass has no way to
/// enumerate an RNF user — and answering "no such user" for one is exactly the divergence
/// this shape closes.
/// </remarks>
internal sealed class DescribeUserScramCredentialsViews
{
    /// <summary>Creates the payload handed to the single awaiter.</summary>
    /// <param name="allError">
    /// <c>all()</c>'s fault, or <see langword="null"/> when <c>all()</c> succeeded.
    /// </param>
    /// <param name="all">The <c>all()</c> rows; empty when <paramref name="allError"/> is set.</param>
    /// <param name="users">The <c>users()</c> names, in response order.</param>
    /// <param name="root">The retained native result root.</param>
    internal DescribeUserScramCredentialsViews(
        KafkaException? allError,
        IReadOnlyDictionary<string, UserScramCredentialsDescription> all,
        IReadOnlyList<string> users,
        SafeDescribeUserScramCredentialsResultHandle root)
    {
        AllError = allError;
        All = all;
        Users = users;
        Root = root;
    }

    /// <summary><c>all()</c>'s fault, or <see langword="null"/> on success.</summary>
    internal KafkaException? AllError { get; }

    /// <summary>The <c>all()</c> rows, keyed by user name.</summary>
    internal IReadOnlyDictionary<string, UserScramCredentialsDescription> All { get; }

    /// <summary>The <c>users()</c> names.</summary>
    internal IReadOnlyList<string> Users { get; }

    /// <summary>The retained root backing <c>description(user)</c>.</summary>
    internal SafeDescribeUserScramCredentialsResultHandle Root { get; }
}
