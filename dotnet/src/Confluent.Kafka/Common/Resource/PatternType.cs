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

namespace Confluent.Kafka.Common.Resource;

/// <summary>
/// How a resource pattern's name is matched against resource names — Java's
/// <c>org.apache.kafka.common.resource.PatternType</c>. Each member's value is the Kafka wire
/// code (Java's <c>code()</c>), not an ordinal.
/// </summary>
public enum PatternType
{
    /// <summary>A pattern type this client does not recognize — Java's <c>UNKNOWN</c> (code 0).</summary>
    UNKNOWN = 0,

    /// <summary>
    /// In a filter, matches any pattern type — Java's <c>ANY</c> (code 1). Rejected by
    /// <see cref="ResourcePattern"/>'s constructor; legal on <see cref="ResourcePatternFilter"/>.
    /// </summary>
    ANY = 1,

    /// <summary>
    /// In a filter, matches every pattern that would itself match the supplied name —
    /// Java's <c>MATCH</c> (code 2). Filter-only, like <see cref="ANY"/>.
    /// </summary>
    MATCH = 2,

    /// <summary>The name is matched literally — Java's <c>LITERAL</c> (code 3).</summary>
    LITERAL = 3,

    /// <summary>The name is a prefix — Java's <c>PREFIXED</c> (code 4).</summary>
    PREFIXED = 4,
}

/// <summary>
/// The members Java declares on the <see cref="PatternType"/> enum itself, which a C# enum
/// cannot carry. From C# 14 the static ones are called on the enum
/// (<c>PatternType.FromString(…)</c>); older C# calls them on this class.
/// </summary>
public static class PatternTypeMethods
{
    extension(PatternType)
    {
        /// <summary>
        /// Return the <see cref="PatternType"/> with the provided code or <see cref="PatternType.UNKNOWN"/> if one
        /// cannot be found — Java's <c>fromCode(byte)</c>.
        /// </summary>
        /// <param name="code">The code.</param>
        /// <returns>The pattern type; never throws.</returns>
        public static PatternType FromCode(byte code) =>
            code <= (byte)PatternType.PREFIXED ? (PatternType)code : PatternType.UNKNOWN;

        /// <summary>
        /// Return the <see cref="PatternType"/> with the provided name or <see cref="PatternType.UNKNOWN"/> if one
        /// cannot be found — Java's <c>fromString(String)</c>. The name is matched exactly, case included.
        /// </summary>
        /// <param name="name">The constant name.</param>
        /// <returns>
        /// The pattern type; <see cref="PatternType.UNKNOWN"/> for <see langword="null"/>, never throws.
        /// </returns>
        public static PatternType FromString(string name) =>
            name switch
            {
                nameof(PatternType.UNKNOWN) => PatternType.UNKNOWN,
                nameof(PatternType.ANY) => PatternType.ANY,
                nameof(PatternType.MATCH) => PatternType.MATCH,
                nameof(PatternType.LITERAL) => PatternType.LITERAL,
                nameof(PatternType.PREFIXED) => PatternType.PREFIXED,
                _ => PatternType.UNKNOWN,
            };
    }

    extension(PatternType patternType)
    {
        /// <summary>The code of this resource — Java's <c>code()</c>.</summary>
        /// <returns>The code, which is the member's value.</returns>
        public byte Code() => unchecked((byte)patternType);

        /// <summary>Whether this resource pattern type is UNKNOWN — Java's <c>isUnknown()</c>.</summary>
        /// <returns>Whether this pattern type is <see cref="PatternType.UNKNOWN"/>.</returns>
        public bool IsUnknown() => patternType == PatternType.UNKNOWN;

        /// <summary>
        /// Whether this resource pattern type is a concrete type, rather than UNKNOWN or one of the filter types —
        /// Java's <c>isSpecific()</c>.
        /// </summary>
        /// <returns>
        /// Whether this pattern type is neither <see cref="PatternType.UNKNOWN"/>, <see cref="PatternType.ANY"/>
        /// nor <see cref="PatternType.MATCH"/>.
        /// </returns>
        public bool IsSpecific() =>
            patternType != PatternType.UNKNOWN && patternType != PatternType.ANY && patternType != PatternType.MATCH;
    }
}
