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

namespace Confluent.Kafka.Admin;

/// <summary>
/// A request to delete a user's SASL/SCRAM credential for one mechanism — Java's
/// <c>org.apache.kafka.clients.admin.UserScramCredentialDeletion</c>.
/// </summary>
public sealed class UserScramCredentialDeletion : UserScramCredentialAlteration
{
    /// <summary>Creates a deletion — Java's <c>:34</c>.</summary>
    /// <param name="user">The user.</param>
    /// <param name="mechanism">The mechanism whose credential is deleted.</param>
    /// <exception cref="System.ArgumentNullException"><paramref name="user"/> is null.</exception>
    public UserScramCredentialDeletion(string user, ScramMechanism mechanism)
        : base(user)
    {
        Mechanism = mechanism;
    }

    /// <summary>The mechanism — Java's <c>mechanism()</c> (<c>:43</c>).</summary>
    public ScramMechanism Mechanism { get; }
}
