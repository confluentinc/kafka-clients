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

using System.Globalization;

namespace Confluent.Kafka.Admin;

/// <summary>
/// The mechanism and iteration count of one SASL/SCRAM credential — Java's
/// <c>org.apache.kafka.clients.admin.ScramCredentialInfo</c>.
/// </summary>
public sealed class ScramCredentialInfo
{
    /// <summary>Creates a credential description — Java's <c>:36</c>.</summary>
    /// <param name="mechanism">The mechanism.</param>
    /// <param name="iterations">The iteration count used when creating the credential.</param>
    public ScramCredentialInfo(ScramMechanism mechanism, int iterations)
    {
        Mechanism = mechanism;
        Iterations = iterations;
    }

    /// <summary>The mechanism — Java's <c>mechanism()</c> (<c>:45</c>).</summary>
    public ScramMechanism Mechanism { get; }

    /// <summary>The iteration count — Java's <c>iterations()</c> (<c>:53</c>).</summary>
    public int Iterations { get; }

    /// <summary>Value equality over both fields — Java's <c>equals</c> (<c>:66</c>).</summary>
    /// <param name="obj">The object to compare with.</param>
    /// <returns>Whether the two describe the same credential.</returns>
    public override bool Equals(object? obj) =>
        obj is ScramCredentialInfo other
        && Mechanism == other.Mechanism
        && Iterations == other.Iterations;

    /// <summary>The hash of both fields — Java's <c>hashCode</c> (<c>:75</c>).</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        unchecked
        {
            return (((int)Mechanism * 31) + 1) * 31 + Iterations;
        }
    }

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c> (<c>:58</c>).</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(
            CultureInfo.InvariantCulture,
            "ScramCredentialInfo{{mechanism={0}, iterations={1}}}",
            Mechanism,
            Iterations);
}
