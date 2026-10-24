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
/// A SASL/SCRAM mechanism — Java's <c>org.apache.kafka.clients.admin.ScramMechanism</c>.
/// </summary>
/// <remarks>
/// The admin enum, distinct from Java's internal <c>ScramMechanism</c>, which has no
/// <c>UNKNOWN</c>. The type codes are wire values and must not change.
/// </remarks>
public enum ScramMechanism : byte
{
    /// <summary>An unrecognized mechanism — Java's <c>UNKNOWN</c>.</summary>
    Unknown = 0,

    /// <summary><c>SCRAM-SHA-256</c> — Java's <c>SCRAM_SHA_256</c>.</summary>
    ScramSha256 = 1,

    /// <summary><c>SCRAM-SHA-512</c> — Java's <c>SCRAM_SHA_512</c>.</summary>
    ScramSha512 = 2,
}

/// <summary>
/// The members Java declares on the <see cref="ScramMechanism"/> enum itself, which a C# enum
/// cannot carry.
/// </summary>
public static class ScramMechanisms
{
    /// <summary>
    /// The SASL mechanism name — Java's <c>mechanismName()</c>: the enum constant with
    /// <c>'_'</c> replaced by <c>'-'</c>.
    /// </summary>
    /// <param name="mechanism">The mechanism.</param>
    /// <returns>The mechanism name, or <c>"UNKNOWN"</c>.</returns>
    public static string MechanismName(this ScramMechanism mechanism) =>
        mechanism switch
        {
            ScramMechanism.ScramSha256 => "SCRAM-SHA-256",
            ScramMechanism.ScramSha512 => "SCRAM-SHA-512",
            _ => "UNKNOWN",
        };

    /// <summary>
    /// The mechanism for a type code — Java's <c>fromType(byte)</c>. Falls back to
    /// <see cref="ScramMechanism.Unknown"/>; never throws.
    /// </summary>
    /// <param name="type">The type code.</param>
    /// <returns>The mechanism.</returns>
    public static ScramMechanism FromType(byte type) =>
        type switch
        {
            1 => ScramMechanism.ScramSha256,
            2 => ScramMechanism.ScramSha512,
            _ => ScramMechanism.Unknown,
        };

    /// <summary>
    /// The mechanism for a SASL mechanism name — Java's <c>fromMechanismName(String)</c>. Falls
    /// back to <see cref="ScramMechanism.Unknown"/>; never throws.
    /// </summary>
    /// <param name="mechanismName">The mechanism name.</param>
    /// <returns>The mechanism.</returns>
    public static ScramMechanism FromMechanismName(string mechanismName) =>
        string.Equals(mechanismName, "SCRAM-SHA-256", StringComparison.Ordinal)
            ? ScramMechanism.ScramSha256
            : string.Equals(mechanismName, "SCRAM-SHA-512", StringComparison.Ordinal)
                ? ScramMechanism.ScramSha512
                : ScramMechanism.Unknown;
}
