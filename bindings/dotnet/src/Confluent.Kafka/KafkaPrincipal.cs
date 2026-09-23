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

namespace Confluent.Kafka;

/// <summary>
/// A Kafka principal — Java's
/// <c>org.apache.kafka.common.security.auth.KafkaPrincipal</c>.
/// </summary>
/// <remarks>
/// Java's <c>tokenAuthenticated</c> field is mutable (<c>:92</c>); this type is immutable — a
/// binding-layer tightening (<c>definition-of-done.md</c> §7). Java's
/// <c>SecurityUtils.parseKafkaPrincipal</c> string parser is not bound.
/// </remarks>
public sealed class KafkaPrincipal
{
    /// <summary>The <c>"User"</c> principal type — Java's <c>USER_TYPE</c> (<c>:44</c>).</summary>
    public const string UserType = "User";

    /// <summary>Creates a principal that was not token-authenticated — Java's <c>:51</c>.</summary>
    /// <param name="principalType">The principal type, e.g. <see cref="UserType"/>.</param>
    /// <param name="name">The principal name.</param>
    /// <exception cref="ArgumentNullException">Either argument is null.</exception>
    public KafkaPrincipal(string principalType, string name)
        : this(principalType, name, false)
    {
    }

    /// <summary>Creates a principal — Java's <c>:55</c>.</summary>
    /// <param name="principalType">The principal type, e.g. <see cref="UserType"/>.</param>
    /// <param name="name">The principal name.</param>
    /// <param name="tokenAuthenticated">Whether the principal was authenticated by a delegation token.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="principalType"/> or <paramref name="name"/> is null.
    /// </exception>
    public KafkaPrincipal(string principalType, string name, bool tokenAuthenticated)
    {
        PrincipalType = principalType
            ?? throw new ArgumentNullException(nameof(principalType), "Principal type cannot be null");
        Name = name ?? throw new ArgumentNullException(nameof(name), "Principal name cannot be null");
        TokenAuthenticated = tokenAuthenticated;
    }

    /// <summary>The anonymous principal — Java's <c>ANONYMOUS</c> (<c>:45</c>).</summary>
    public static KafkaPrincipal Anonymous { get; } = new KafkaPrincipal(UserType, "ANONYMOUS");

    /// <summary>The principal type — Java's <c>getPrincipalType()</c> (<c>:88</c>).</summary>
    public string PrincipalType { get; }

    /// <summary>The principal name — Java's <c>getName()</c> (<c>:84</c>).</summary>
    public string Name { get; }

    /// <summary>
    /// Whether the principal was authenticated by a delegation token — Java's
    /// <c>tokenAuthenticated()</c> (<c>:96</c>).
    /// </summary>
    public bool TokenAuthenticated { get; }

    /// <summary>
    /// Value equality over <see cref="PrincipalType"/> and <see cref="Name"/> only — Java's
    /// <c>equals</c> (<c>:67</c>), which deliberately excludes <c>tokenAuthenticated</c>.
    /// </summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two name the same principal.</returns>
    public override bool Equals(object? obj) =>
        obj is KafkaPrincipal other
        && string.Equals(PrincipalType, other.PrincipalType, StringComparison.Ordinal)
        && string.Equals(Name, other.Name, StringComparison.Ordinal);

    /// <summary>The hash of the same two fields — Java's <c>hashCode</c> (<c>:77</c>).</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            int hash = StringComparer.Ordinal.GetHashCode(PrincipalType);
            return (hash * 31) + StringComparer.Ordinal.GetHashCode(Name);
        }
    }

    /// <summary><c>"{type}:{name}"</c> — Java's <c>toString()</c> (<c>:62</c>).</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(CultureInfo.InvariantCulture, "{0}:{1}", PrincipalType, Name);
}
