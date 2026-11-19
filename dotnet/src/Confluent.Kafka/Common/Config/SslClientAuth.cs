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
using System.Globalization;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common.Config;

/// <summary>
/// Describes whether the server should require or request client authentication — Java's
/// <c>org.apache.kafka.common.config.SslClientAuth</c>.
/// </summary>
/// <remarks>
/// <c>ToString()</c> gives the Java constant name (<c>REQUIRED</c>), not Java's <c>toString()</c> text
/// (<c>"required"</c>): a recorded deviation. Java's text is <c>SslClientAuth.ToString(auth)</c> on C# 14, and
/// <see cref="SslClientAuthMethods.ToString(SslClientAuth)"/> on older C#.
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
/// From C# 14 its methods are called on the enum (<c>SslClientAuth.ForConfig(…)</c>); older C# calls them on this
/// class. <see cref="VALUES"/> is a field of this class on every C# version.
/// </summary>
public static class SslClientAuthMethods
{
    /// <summary>The constants, in Java's declaration order — Java's <c>VALUES</c>.</summary>
    public static readonly IReadOnlyList<SslClientAuth> VALUES = new ReadOnlyCollection<SslClientAuth>(new[]
    {
        SslClientAuth.REQUIRED,
        SslClientAuth.REQUESTED,
        SslClientAuth.NONE,
    });

    extension(SslClientAuth)
    {
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

        /// <summary>
        /// Java's <c>toString()</c> text of <paramref name="auth"/>: the constant name in lower case, such as
        /// <c>"required"</c>.
        /// </summary>
        /// <param name="auth">The client authentication setting.</param>
        /// <returns>The text, or the number for a value that is no constant.</returns>
        public static string ToString(SslClientAuth auth) =>
            auth switch
            {
                SslClientAuth.REQUIRED => "required",
                SslClientAuth.REQUESTED => "requested",
                SslClientAuth.NONE => "none",
                _ => ((int)auth).ToString(CultureInfo.InvariantCulture),
            };
    }
}
