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
/// A null <see cref="Salt"/> asks the core to generate a random salt, as Java's salt-generating
/// constructors do; any other salt, a zero-length one included, is used verbatim. The binding
/// generates no salt itself.
/// </para>
/// <para>
/// <see cref="Password"/> and <see cref="Salt"/> are secret material: neither is logged, traced or
/// put in an exception message, and <c>ToString()</c> is not overridden, as in Java.
/// </para>
/// </remarks>
public sealed class UserScramCredentialUpsertion : UserScramCredentialAlteration
{
    private readonly byte[] _password;
    private readonly byte[]? _salt;

    /// <summary>
    /// Creates an upsertion whose salt the core generates — Java's constructor taking the
    /// password as a <c>String</c>.
    /// </summary>
    /// <param name="user">The user.</param>
    /// <param name="credentialInfo">The mechanism and iteration count.</param>
    /// <param name="password">The password, encoded as UTF-8, as Java does.</param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public UserScramCredentialUpsertion(string user, ScramCredentialInfo credentialInfo, string password)
        : this(
            user,
            credentialInfo,
            Encoding.UTF8.GetBytes(password ?? throw new ArgumentNullException(nameof(password))))
    {
    }

    /// <summary>
    /// Creates an upsertion whose salt the core generates — Java's constructor taking the
    /// password as a <c>byte[]</c>.
    /// </summary>
    /// <param name="user">The user.</param>
    /// <param name="credentialInfo">The mechanism and iteration count.</param>
    /// <param name="password">The password bytes.</param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public UserScramCredentialUpsertion(string user, ScramCredentialInfo credentialInfo, byte[] password)
        : this(user, credentialInfo, password, null)
    {
    }

    /// <summary>Creates an upsertion with an explicit salt — Java's four-argument constructor.</summary>
    /// <param name="user">The user.</param>
    /// <param name="credentialInfo">The mechanism and iteration count.</param>
    /// <param name="password">The password bytes.</param>
    /// <param name="salt">
    /// The salt, used verbatim, a zero-length one included; <see langword="null"/> asks the core
    /// to generate one.
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

    /// <summary>The mechanism and iteration count — Java's <c>credentialInfo()</c>.</summary>
    public ScramCredentialInfo CredentialInfo { get; }

    /// <summary>
    /// The explicit salt, or <see langword="null"/> to have the core generate one — Java's
    /// <c>salt()</c>. Secret material: never log it.
    /// </summary>
    public byte[]? Salt => _salt;

    /// <summary>The password bytes — Java's <c>password()</c>. Secret material: never log it.</summary>
    public byte[] Password => _password;
}
