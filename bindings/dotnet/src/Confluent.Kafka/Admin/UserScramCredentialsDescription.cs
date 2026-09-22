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
using System.Linq;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The SASL/SCRAM credentials described for one user — Java's
/// <c>org.apache.kafka.clients.admin.UserScramCredentialsDescription</c>.
/// </summary>
public sealed class UserScramCredentialsDescription
{
    private readonly ScramCredentialInfo[] _credentialInfos;

    /// <summary>Creates a description — Java's <c>:60</c>.</summary>
    /// <param name="name">The user name.</param>
    /// <param name="credentialInfos">The user's credential descriptions; copied.</param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public UserScramCredentialsDescription(
        string name, IEnumerable<ScramCredentialInfo> credentialInfos)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        if (credentialInfos is null)
        {
            throw new ArgumentNullException(nameof(credentialInfos));
        }

        _credentialInfos = credentialInfos.ToArray();
    }

    /// <summary>The user name — Java's <c>name()</c> (<c>:69</c>).</summary>
    public string Name { get; }

    /// <summary>
    /// The user's credential descriptions — Java's <c>credentialInfos()</c> (<c>:77</c>).
    /// </summary>
    public IReadOnlyList<ScramCredentialInfo> CredentialInfos => _credentialInfos;

    /// <summary>Value equality over both fields — Java's <c>equals</c> (<c>:34</c>).</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same user's credentials.</returns>
    public override bool Equals(object? obj) =>
        obj is UserScramCredentialsDescription other
        && string.Equals(Name, other.Name, StringComparison.Ordinal)
        && _credentialInfos.SequenceEqual(other._credentialInfos);

    /// <summary>The hash of both fields — Java's <c>hashCode</c> (<c>:43</c>).</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = StringComparer.Ordinal.GetHashCode(Name);
            foreach (ScramCredentialInfo info in _credentialInfos)
            {
                hash = (hash * 31) + info.GetHashCode();
            }

            return hash;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:48</c>).</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "UserScramCredentialsDescription{{name='{0}', credentialInfos=[{1}]}}",
            Name,
            string.Join(", ", _credentialInfos.Select(info => info.ToString())));
}
