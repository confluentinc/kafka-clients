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

using System.Collections.Generic;
using System.Collections.ObjectModel;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common.Config;

/// <summary>
/// Describes whether the server should require or request client authentication — Java's
/// <c>org.apache.kafka.common.config.SslClientAuth</c>.
/// </summary>
/// <remarks>
/// <c>ToString()</c> gives the Java constant name (<c>REQUIRED</c>), not Java's <c>toString()</c> text
/// (<c>"required"</c>): a recorded deviation.
/// </remarks>
public enum SslClientAuth
{
    /// <summary>The server requires client authentication — Java's <c>REQUIRED</c>.</summary>
    REQUIRED,

    /// <summary>The server requests client authentication — Java's <c>REQUESTED</c>.</summary>
    REQUESTED,

    /// <summary>The server does not authenticate clients — Java's <c>NONE</c>.</summary>
    NONE,
}

/// <summary>
/// The static members Java declares on the <see cref="SslClientAuth"/> enum itself, which a C# enum cannot carry.
/// </summary>
public static class SslClientAuths
{
    /// <summary>The constants, in Java's declaration order — Java's <c>VALUES</c>.</summary>
    public static IReadOnlyList<SslClientAuth> VALUES { get; } = new ReadOnlyCollection<SslClientAuth>(new[]
    {
        SslClientAuth.REQUIRED,
        SslClientAuth.REQUESTED,
        SslClientAuth.NONE,
    });

    /// <summary>The constant named <paramref name="key"/>, in any case — Java's <c>forConfig(String)</c>.</summary>
    /// <param name="key">The configured value.</param>
    /// <returns>
    /// <see cref="SslClientAuth.NONE"/> for <see langword="null"/>, the matching constant, or
    /// <see langword="null"/> when no constant has that name.
    /// </returns>
    public static SslClientAuth? ForConfig(string? key)
    {
        if (key is null)
        {
            return SslClientAuth.NONE;
        }

        return JavaText.ToUpperRoot(key) switch
        {
            nameof(SslClientAuth.REQUIRED) => SslClientAuth.REQUIRED,
            nameof(SslClientAuth.REQUESTED) => SslClientAuth.REQUESTED,
            nameof(SslClientAuth.NONE) => SslClientAuth.NONE,
            _ => null,
        };
    }
}
