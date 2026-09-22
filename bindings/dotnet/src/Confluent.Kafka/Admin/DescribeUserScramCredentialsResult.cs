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
using System.Globalization;
using System.Threading.Tasks;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The result of <see cref="IAdmin.DescribeUserScramCredentials"/> — Java's
/// <c>org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult</c>: three accessors
/// derived from one underlying response (<c>:37</c>), each treating a per-user failure
/// differently.
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Known divergence (M15/P7 D44), carried to M15/P9 — the canonical statement.</b> The
/// ABI collapses Java's <c>RESOURCE_NOT_FOUND</c> case into "a successfully described user
/// with zero credentials" (<c>confluent_kafka.h:9411-9413</c>) and exposes <b>no</b>
/// discriminant, so a not-found user is indistinguishable here from one that genuinely has no
/// credentials. The binding ships the ABI's behaviour unchanged: a managed heuristic would be
/// a guess, and closing it properly needs a discriminant at the ABI (a Rust-core change).
/// </para>
/// <para>
/// <b>Two of the three accessors diverge, not all three.</b> <see cref="Users"/> includes a
/// user Java filters out (<c>:98-100</c>), and <see cref="Description"/> succeeds with zero
/// credentials where Java faults (<c>:128-130</c> — "RESOURCE_NOT_FOUND is included here").
/// ⚠ <see cref="All"/> <b>matches</b> Java: its <c>RESOURCE_NOT_FOUND</c> exclusion is only
/// from the <em>first-failure</em> scan (<c>:65-67</c>), after which the map is built from
/// <b>every</b> row (<c>:72-74</c>), so such a user is a key in Java's map too. Java's own
/// javadoc at <c>:60-64</c> says the opposite of its code; the code is the contract.
/// </para>
/// </remarks>
public sealed class DescribeUserScramCredentialsResult
{
    private readonly Task<IReadOnlyCollection<UserScramCredentialEntry>> _entries;

    internal DescribeUserScramCredentialsResult(Task<IReadOnlyCollection<UserScramCredentialEntry>> entries)
    {
        _entries = entries;
    }

    /// <summary>
    /// Every described user, keyed by name — Java's <c>all()</c> (<c>:54</c>). Faults with the
    /// <b>first</b> per-user failure if any user could not be described (<c>:68-69</c>).
    /// </summary>
    /// <returns>An awaitable over the whole map.</returns>
    public Task<IReadOnlyDictionary<string, UserScramCredentialsDescription>> All() => BuildAll();

    /// <summary>
    /// The users the response carried — Java's <c>users()</c> (<c>:92</c>).
    /// </summary>
    /// <remarks>
    /// ⚠ Deliberately does <b>not</b> fault on a per-user failure: Java's javadoc says the list
    /// "will include users that have a credential but that could not be described"
    /// (<c>:89-90</c>). Only a failure of the call itself faults it.
    /// </remarks>
    /// <returns>An awaitable over the user names.</returns>
    public Task<IReadOnlyList<string>> Users() => BuildUsers();

    /// <summary>
    /// One user's credentials — Java's <c>description(String)</c> (<c>:114</c>). Faults when
    /// the response carried no such user (<c>:124-125</c>) or when that user failed
    /// (<c>:128-131</c>).
    /// </summary>
    /// <param name="userName">The user to look up.</param>
    /// <returns>An awaitable over that user's description.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="userName"/> is null.</exception>
    public Task<UserScramCredentialsDescription> Description(string userName)
    {
        if (userName is null)
        {
            throw new ArgumentNullException(nameof(userName));
        }

        return BuildDescription(userName);
    }

    private async Task<IReadOnlyDictionary<string, UserScramCredentialsDescription>> BuildAll()
    {
        IReadOnlyCollection<UserScramCredentialEntry> entries = await _entries.ConfigureAwait(false);
        Dictionary<string, UserScramCredentialsDescription> described =
            new Dictionary<string, UserScramCredentialsDescription>(entries.Count, StringComparer.Ordinal);
        foreach (UserScramCredentialEntry entry in entries)
        {
            if (entry.Error is not null)
            {
                throw entry.Error;
            }

            described[entry.User] = entry.Description;
        }

        return described;
    }

    private async Task<IReadOnlyList<string>> BuildUsers()
    {
        IReadOnlyCollection<UserScramCredentialEntry> entries = await _entries.ConfigureAwait(false);
        List<string> users = new List<string>(entries.Count);
        foreach (UserScramCredentialEntry entry in entries)
        {
            users.Add(entry.User);
        }

        return users;
    }

    private async Task<UserScramCredentialsDescription> BuildDescription(string userName)
    {
        IReadOnlyCollection<UserScramCredentialEntry> entries = await _entries.ConfigureAwait(false);
        foreach (UserScramCredentialEntry entry in entries)
        {
            if (string.Equals(entry.User, userName, StringComparison.Ordinal))
            {
                return entry.Error is null ? entry.Description : throw entry.Error;
            }
        }

        throw new KafkaException(
            string.Format(CultureInfo.InvariantCulture, "No such user: {0}", userName));
    }
}
