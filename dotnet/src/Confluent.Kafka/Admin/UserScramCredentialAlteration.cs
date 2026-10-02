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

namespace Confluent.Kafka.Admin;

/// <summary>
/// The base of the two SASL/SCRAM credential alterations — Java's abstract
/// <c>org.apache.kafka.clients.admin.UserScramCredentialAlteration</c> (<c>:27</c>). It is the
/// element type <see cref="IAdmin.AlterUserScramCredentials"/> accepts.
/// </summary>
/// <remarks>
/// A closed two-case hierarchy: <see cref="UserScramCredentialUpsertion"/> and
/// <see cref="UserScramCredentialDeletion"/>.
/// ⚠ <b>Narrower than Java:</b> the constructor is <c>private protected</c>, where Java's is
/// <c>protected</c>, so code outside this library cannot add a third case. Java lets a caller
/// subclass it but only ever sends those two cases (<c>KafkaAdminClient.java:4391-4512</c>),
/// so any other subclass just fails that user's future; here it could not be sent at all,
/// and closing the hierarchy keeps it from being written.
/// </remarks>
public abstract class UserScramCredentialAlteration
{
    /// <summary>
    /// Creates an alteration for one user — Java's protected <c>:34</c>, here
    /// <c>private protected</c> (see the class remarks).
    /// </summary>
    /// <param name="user">The user. Must not be null.</param>
    /// <exception cref="ArgumentNullException"><paramref name="user"/> is null.</exception>
    private protected UserScramCredentialAlteration(string user)
    {
        User = user ?? throw new ArgumentNullException(nameof(user));
    }

    /// <summary>The user — Java's <c>user()</c> (<c>:42</c>).</summary>
    public string User { get; }
}
