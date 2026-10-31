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
/// <c>org.apache.kafka.clients.admin.UserScramCredentialAlteration</c>. It is the
/// element type <see cref="IAdmin.AlterUserScramCredentials"/> accepts.
/// </summary>
/// <remarks>
/// A closed two-case hierarchy: <see cref="UserScramCredentialUpsertion"/> and
/// <see cref="UserScramCredentialDeletion"/>. The constructor is <c>private protected</c>, where
/// Java's is <c>protected</c>, a recorded deviation: Java sends only those two cases, so a third
/// could never be sent.
/// </remarks>
public abstract class UserScramCredentialAlteration
{
    private protected UserScramCredentialAlteration(string user)
    {
        User = user ?? throw new ArgumentNullException(nameof(user));
    }

    /// <summary>The user — Java's <c>user()</c>.</summary>
    public string User { get; }
}
