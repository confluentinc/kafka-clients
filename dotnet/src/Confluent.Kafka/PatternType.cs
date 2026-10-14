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

namespace Confluent.Kafka;

/// <summary>
/// How a resource pattern's name is matched against resource names — the .NET realization of
/// Java's <c>org.apache.kafka.common.resource.PatternType</c>. Each member's value is the
/// Kafka <b>wire</b> code (Java's <c>code()</c>), not a C#-assigned ordinal.
/// </summary>
public enum PatternType
{
    /// <summary>A pattern type this client does not recognize — Java's <c>UNKNOWN</c> (code 0).</summary>
    Unknown = 0,

    /// <summary>
    /// In a filter, matches any pattern type — Java's <c>ANY</c> (code 1). Rejected by
    /// <see cref="ResourcePattern"/>'s constructor; legal on <see cref="ResourcePatternFilter"/>.
    /// </summary>
    Any = 1,

    /// <summary>
    /// In a filter, matches every pattern that would itself match the supplied name —
    /// Java's <c>MATCH</c> (code 2). Filter-only, like <see cref="Any"/>.
    /// </summary>
    Match = 2,

    /// <summary>The name is matched literally — Java's <c>LITERAL</c> (code 3).</summary>
    Literal = 3,

    /// <summary>The name is a prefix — Java's <c>PREFIXED</c> (code 4).</summary>
    Prefixed = 4,
}
