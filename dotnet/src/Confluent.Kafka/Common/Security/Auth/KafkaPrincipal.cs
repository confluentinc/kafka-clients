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
using System.Globalization;

namespace Confluent.Kafka.Common.Security.Auth;

/// <summary>
/// A Kafka principal — Java's
/// <c>org.apache.kafka.common.security.auth.KafkaPrincipal</c>.
/// </summary>
/// <remarks>
/// Java's <c>SecurityUtils.parseKafkaPrincipal</c> string parser is not bound.
/// </remarks>
public sealed class KafkaPrincipal
{
    /// <summary>The <c>"User"</c> principal type — Java's <c>USER_TYPE</c>.</summary>
    public const string USER_TYPE = "User";

    private readonly string _principalType;
    private readonly string _name;
    private volatile bool _tokenAuthenticated;

    /// <summary>Creates a principal that was not token-authenticated — Java's two-argument constructor.</summary>
    /// <param name="principalType">The principal type, e.g. <see cref="USER_TYPE"/>.</param>
    /// <param name="name">The principal name.</param>
    /// <exception cref="ArgumentNullException">Either argument is null.</exception>
    public KafkaPrincipal(string principalType, string name)
        : this(principalType, name, false)
    {
    }

    /// <summary>Creates a principal — Java's three-argument constructor.</summary>
    /// <param name="principalType">The principal type, e.g. <see cref="USER_TYPE"/>.</param>
    /// <param name="name">The principal name.</param>
    /// <param name="tokenAuthenticated">Whether the principal was authenticated by a delegation token.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="principalType"/> or <paramref name="name"/> is null.
    /// </exception>
    public KafkaPrincipal(string principalType, string name, bool tokenAuthenticated)
    {
        _principalType = principalType
            ?? throw new ArgumentNullException(nameof(principalType), "Principal type cannot be null");
        _name = name ?? throw new ArgumentNullException(nameof(name), "Principal name cannot be null");
        _tokenAuthenticated = tokenAuthenticated;
    }

    /// <summary>The anonymous principal — Java's <c>ANONYMOUS</c>.</summary>
    public static KafkaPrincipal ANONYMOUS { get; } = new KafkaPrincipal(USER_TYPE, "ANONYMOUS");

    /// <summary>The principal type — Java's <c>getPrincipalType()</c>.</summary>
    /// <returns>The principal type.</returns>
    public string GetPrincipalType() => _principalType;

    /// <summary>The principal name — Java's <c>getName()</c>.</summary>
    /// <returns>The principal name.</returns>
    public string GetName() => _name;

    /// <summary>
    /// Records whether the principal was authenticated by a delegation token — Java's
    /// <c>tokenAuthenticated(boolean)</c>. Equality and the hash code do not read it.
    /// </summary>
    /// <param name="tokenAuthenticated">Whether the principal was authenticated by a delegation token.</param>
    public void TokenAuthenticated(bool tokenAuthenticated) => _tokenAuthenticated = tokenAuthenticated;

    /// <summary>
    /// Whether the principal was authenticated by a delegation token — Java's
    /// <c>tokenAuthenticated()</c>.
    /// </summary>
    /// <returns>Whether the principal was authenticated by a delegation token.</returns>
    public bool TokenAuthenticated() => _tokenAuthenticated;

    /// <summary>
    /// Value equality over <see cref="GetPrincipalType()"/> and <see cref="GetName()"/> only — Java's
    /// <c>equals</c>, which leaves out <c>tokenAuthenticated</c>.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two name the same principal.</returns>
    public override bool Equals(object? obj) =>
        obj is KafkaPrincipal other
        && string.Equals(GetPrincipalType(), other.GetPrincipalType(), StringComparison.Ordinal)
        && string.Equals(GetName(), other.GetName(), StringComparison.Ordinal);

    /// <summary>The hash of the same two fields — Java's <c>hashCode</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = StringComparer.Ordinal.GetHashCode(GetPrincipalType());
            return (hash * 31) + StringComparer.Ordinal.GetHashCode(GetName());
        }
    }

    /// <summary><c>"{type}:{name}"</c> — Java's <c>toString()</c>.</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(CultureInfo.InvariantCulture, "{0}:{1}", GetPrincipalType(), GetName());
}
