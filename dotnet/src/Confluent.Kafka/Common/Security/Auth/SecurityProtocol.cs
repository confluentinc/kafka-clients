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
using System.Collections.Generic;
using System.Collections.ObjectModel;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common.Security.Auth;

/// <summary>
/// A security protocol — Java's <c>org.apache.kafka.common.security.auth.SecurityProtocol</c>. The value is
/// Java's <c>id</c>, and <c>ToString()</c> is Java's <c>name</c>.
/// </summary>
public enum SecurityProtocol : short
{
    /// <summary>Un-authenticated, non-encrypted channel — Java's <c>PLAINTEXT</c>.</summary>
    PLAINTEXT = 0,

    /// <summary>SSL channel — Java's <c>SSL</c>.</summary>
    SSL = 1,

    /// <summary>SASL authenticated, non-encrypted channel — Java's <c>SASL_PLAINTEXT</c>.</summary>
    SASL_PLAINTEXT = 2,

    /// <summary>SASL authenticated, SSL channel — Java's <c>SASL_SSL</c>.</summary>
    SASL_SSL = 3,
}

/// <summary>
/// The static methods Java declares on the <see cref="SecurityProtocol"/> enum itself, which a C# enum cannot
/// carry. From C# 14 they are called on the enum (<c>SecurityProtocol.ForName(…)</c>); older C# calls them on this
/// class.
/// </summary>
public static class SecurityProtocolMethods
{
    private const string JavaClass = "org.apache.kafka.common.security.auth.SecurityProtocol";

    private static readonly IReadOnlyList<string> s_names = new ReadOnlyCollection<string>(new[]
    {
        nameof(SecurityProtocol.PLAINTEXT),
        nameof(SecurityProtocol.SSL),
        nameof(SecurityProtocol.SASL_PLAINTEXT),
        nameof(SecurityProtocol.SASL_SSL),
    });

    extension(SecurityProtocol)
    {
        /// <summary>The names of the protocols, in Java's declaration order — Java's <c>names()</c>.</summary>
        /// <returns>A read-only list, the same one on every call.</returns>
        public static IReadOnlyList<string> Names() => s_names;

        /// <summary>The protocol whose id is <paramref name="id"/> — Java's <c>forId(short)</c>.</summary>
        /// <param name="id">The protocol id.</param>
        /// <returns>The protocol, or <see langword="null"/> when no protocol has that id.</returns>
        public static SecurityProtocol? ForId(short id) =>
            id switch
            {
                (short)SecurityProtocol.PLAINTEXT => SecurityProtocol.PLAINTEXT,
                (short)SecurityProtocol.SSL => SecurityProtocol.SSL,
                (short)SecurityProtocol.SASL_PLAINTEXT => SecurityProtocol.SASL_PLAINTEXT,
                (short)SecurityProtocol.SASL_SSL => SecurityProtocol.SASL_SSL,
                _ => null,
            };

        /// <summary>
        /// The protocol named <paramref name="name"/>, in any case — Java's <c>forName(String)</c>.
        /// </summary>
        /// <param name="name">The protocol name.</param>
        /// <returns>The protocol.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
        /// <exception cref="IllegalArgumentException">
        /// No protocol has the upper-cased name, with Java's message
        /// "No enum constant org.apache.kafka.common.security.auth.SecurityProtocol.&lt;NAME&gt;".
        /// </exception>
        public static SecurityProtocol ForName(string name)
        {
            if (name is null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            string upper = JavaText.ToUpperRoot(name);
            return upper switch
            {
                nameof(SecurityProtocol.PLAINTEXT) => SecurityProtocol.PLAINTEXT,
                nameof(SecurityProtocol.SSL) => SecurityProtocol.SSL,
                nameof(SecurityProtocol.SASL_PLAINTEXT) => SecurityProtocol.SASL_PLAINTEXT,
                nameof(SecurityProtocol.SASL_SSL) => SecurityProtocol.SASL_SSL,
                _ => throw new IllegalArgumentException("No enum constant " + JavaClass + "." + upper),
            };
        }
    }
}
