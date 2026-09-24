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
using System.Text;

namespace Confluent.Kafka.Admin;

/// <summary>
/// A request to insert or update a user's SASL/SCRAM credential — Java's
/// <c>org.apache.kafka.clients.admin.UserScramCredentialUpsertion</c>.
/// </summary>
/// <remarks>
/// <para>
/// ⚠⚠ <b><see cref="Salt"/> null and <see cref="Salt"/> empty are DIFFERENT requests, and this
/// is security-relevant.</b> Null selects Java's salt-generating constructors (<c>:43</c>,
/// <c>:54</c>) and the core generates a random salt; a non-null salt — <b>including a
/// zero-length one</b>, which Java's <c>requireNonNull</c> accepts — is used verbatim. A model
/// that maps empty to "generate" silently upgrades an explicit empty salt, and the reverse
/// stores a credential with no salt at all (<c>confluent_kafka.h:8865-8872</c>).
/// </para>
/// <para>
/// ⚠ <b>D42 — the binding does not generate a salt.</b> Java's three-argument constructors call
/// <c>ScramFormatter.secureRandomBytes</c> (<c>:98</c>); here that generation happens in the
/// core when the discriminant is cleared, so generating one managed-side as well would be the
/// binding adding behaviour and would make the discriminant unreachable. The shape a caller
/// writes is identical to Java's.
/// </para>
/// <para>
/// ⚠ <b><see cref="Password"/> and <see cref="Salt"/> are secret material.</b> Neither is ever
/// logged, traced or placed in an exception message, and this type deliberately does not
/// override <c>ToString()</c> — Java does not either, so the default type-name rendering is
/// already safe (compare <c>DelegationToken.toString()</c>, which masks its MAC).
/// </para>
/// </remarks>
public sealed class UserScramCredentialUpsertion : UserScramCredentialAlteration
{
    private readonly byte[] _password;
    private readonly byte[]? _salt;

    /// <summary>
    /// Creates an upsertion whose salt the core generates, with a UTF-8-encoded password —
    /// Java's <c>:43</c>.
    /// </summary>
    /// <param name="user">The user.</param>
    /// <param name="credentialInfo">The mechanism and iteration count.</param>
    /// <param name="password">The password; encoded as UTF-8, as Java does (<c>:44</c>).</param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public UserScramCredentialUpsertion(string user, ScramCredentialInfo credentialInfo, string password)
        : this(
            user,
            credentialInfo,
            Encoding.UTF8.GetBytes(password ?? throw new ArgumentNullException(nameof(password))))
    {
    }

    /// <summary>
    /// Creates an upsertion whose salt the core generates — Java's <c>:54</c>.
    /// </summary>
    /// <param name="user">The user.</param>
    /// <param name="credentialInfo">The mechanism and iteration count.</param>
    /// <param name="password">The password bytes.</param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public UserScramCredentialUpsertion(string user, ScramCredentialInfo credentialInfo, byte[] password)
        : this(user, credentialInfo, password, null)
    {
    }

    /// <summary>
    /// Creates an upsertion with an explicit salt — Java's <c>:66</c>.
    /// </summary>
    /// <param name="user">The user.</param>
    /// <param name="credentialInfo">The mechanism and iteration count.</param>
    /// <param name="password">The password bytes.</param>
    /// <param name="salt">
    /// The salt, used verbatim — including when zero-length. <see langword="null"/> asks the
    /// core to generate one; see the type remarks.
    /// </param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="user"/>, <paramref name="credentialInfo"/> or <paramref name="password"/>
    /// is null.
    /// </exception>
    public UserScramCredentialUpsertion(
        string user, ScramCredentialInfo credentialInfo, byte[] password, byte[]? salt)
        : base(user)
    {
        CredentialInfo = credentialInfo ?? throw new ArgumentNullException(nameof(credentialInfo));
        _password = password ?? throw new ArgumentNullException(nameof(password));
        _salt = salt;
    }

    /// <summary>
    /// The mechanism and iteration count — Java's <c>credentialInfo()</c> (<c>:77</c>).
    /// </summary>
    public ScramCredentialInfo CredentialInfo { get; }

    /// <summary>
    /// The explicit salt, or <see langword="null"/> to have the core generate one — Java's
    /// <c>salt()</c> (<c>:85</c>). ⚠ Secret material; never log it.
    /// </summary>
    public byte[]? Salt => _salt;

    /// <summary>
    /// The password bytes — Java's <c>password()</c> (<c>:93</c>). ⚠ Secret material; never
    /// log it.
    /// </summary>
    public byte[] Password => _password;
}
