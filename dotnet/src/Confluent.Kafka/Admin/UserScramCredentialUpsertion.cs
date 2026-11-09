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

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Admin;

/// <summary>
/// A request to insert or update a user's SASL/SCRAM credential — Java's
/// <c>org.apache.kafka.clients.admin.UserScramCredentialUpsertion</c>.
/// </summary>
/// <remarks>
/// <para>
/// The three-argument constructors generate a random salt, as Java's do; the binding asks the
/// core for it (the core's salt-generating constructor) and generates none itself. An explicit
/// salt, a zero-length one included, is used verbatim. <see cref="Salt()"/> is never
/// <see langword="null"/>, as in Java.
/// </para>
/// <para>
/// <see cref="Password()"/> and <see cref="Salt()"/> are secret material: neither is logged, traced or
/// put in an exception message, and <c>ToString()</c> is not overridden, as in Java.
/// </para>
/// </remarks>
public sealed class UserScramCredentialUpsertion : UserScramCredentialAlteration
{
    private readonly byte[] _password;
    private readonly byte[] _salt;
    private readonly ScramCredentialInfo _credentialInfo;

    /// <summary>
    /// Creates an upsertion with a random salt — Java's constructor taking the password as a
    /// <c>String</c>.
    /// </summary>
    /// <param name="user">The user.</param>
    /// <param name="credentialInfo">The mechanism and iteration count.</param>
    /// <param name="password">The password, encoded as UTF-8, as Java does.</param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public UserScramCredentialUpsertion(string user, ScramCredentialInfo credentialInfo, string password)
        : this(
            user,
            credentialInfo,
            Utf8Marshal.QuestionMarkUtf8.GetBytes(password ?? throw new ArgumentNullException(nameof(password))))
    {
    }

    /// <summary>
    /// Creates an upsertion with a random salt — Java's constructor taking the password as a
    /// <c>byte[]</c>.
    /// </summary>
    /// <param name="user">The user.</param>
    /// <param name="credentialInfo">The mechanism and iteration count.</param>
    /// <param name="password">The password bytes.</param>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public UserScramCredentialUpsertion(string user, ScramCredentialInfo credentialInfo, byte[] password)
        : this(user, credentialInfo, password, UserScramCredentialMarshal.GenerateSalt(user, credentialInfo, password))
    {
    }

    /// <summary>Creates an upsertion with an explicit salt — Java's four-argument constructor.</summary>
    /// <param name="user">The user.</param>
    /// <param name="credentialInfo">The mechanism and iteration count.</param>
    /// <param name="password">The password bytes.</param>
    /// <param name="salt">The salt, used verbatim, a zero-length one included.</param>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="user"/>, <paramref name="credentialInfo"/>, <paramref name="password"/> or
    /// <paramref name="salt"/> is null.
    /// </exception>
    public UserScramCredentialUpsertion(
        string user, ScramCredentialInfo credentialInfo, byte[] password, byte[] salt)
        : base(user)
    {
        _credentialInfo = credentialInfo ?? throw new ArgumentNullException(nameof(credentialInfo));
        _password = password ?? throw new ArgumentNullException(nameof(password));
        _salt = salt ?? throw new ArgumentNullException(nameof(salt));
    }

    /// <summary>The mechanism and iteration count — Java's <c>credentialInfo()</c>.</summary>
    /// <returns>The mechanism and iteration count.</returns>
    public ScramCredentialInfo CredentialInfo() => _credentialInfo;

    /// <summary>The salt — Java's <c>salt()</c>. Secret material: never log it.</summary>
    /// <returns>The salt.</returns>
    public byte[] Salt() => _salt;

    /// <summary>The password bytes — Java's <c>password()</c>. Secret material: never log it.</summary>
    /// <returns>The password bytes.</returns>
    public byte[] Password() => _password;
}
