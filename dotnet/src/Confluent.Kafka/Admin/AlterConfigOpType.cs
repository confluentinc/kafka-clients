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
/// What an <see cref="AlterConfigOp"/> does to a configuration entry — the .NET
/// realization of Java's nested <c>AlterConfigOp.OpType</c>
/// (<c>AlterConfigOp.java:46-86</c>).
/// </summary>
/// <remarks>
/// <para>
/// ⚠ <b>Every member's numeric value is Java's <c>OpType.id()</c></b>
/// (<c>AlterConfigOp.java:50, :54, :60, :67</c>), not a C#-assigned ordinal — the ids
/// cross the ABI as bare <c>int32_t</c> in the <c>op_types</c> array, and the header
/// states them: "<c>op_types</c> hold <c>AlterConfigOp.OpType.id()</c> codes (0 = SET,
/// 1 = DELETE, 2 = APPEND, 3 = SUBTRACT)". They happen to coincide with declaration order,
/// which is exactly why they are written out: an auto-assigned value that agrees today
/// would silently stop agreeing if Java ever reordered them.
/// </para>
/// <para>
/// ⚠ <b>Contrast <see cref="ConfigEntry.ConfigSource"/> and
/// <see cref="ConfigEntry.ConfigType"/> in this same stage</b>, which cross as Java's enum
/// constant <em>names</em> because they have no numeric id in Java. Two conventions
/// coexist here; which one applies is decided by the header per accessor.
/// </para>
/// <para>
/// <b>Flattened out of Java's nesting, and the flattening is FORCED.</b> Java nests this
/// as <c>AlterConfigOp.OpType</c> and names its accessor <c>opType()</c> — legal in Java,
/// where types and methods occupy different namespaces. In C# they do not:
/// <see cref="AlterConfigOp.OpType"/> is a property under M15/P3 decision D18, and a
/// nested type of the same name would be <c>CS0102</c> ("the type already contains a
/// definition for 'OpType'"). D18 names <c>CS0102</c> as the one thing that overrides the
/// default shape, so the enum moves out rather than the accessor changing form. Contrast
/// D16's <see cref="ConfigEntry.ConfigSource"/> / <see cref="ConfigEntry.ConfigType"/>,
/// which stay nested precisely because Java's accessors there are <c>source()</c> and
/// <c>type()</c> — different names from the nested types, so nothing collides.
/// </para>
/// </remarks>
public enum AlterConfigOpType
{
    /// <summary>Set the entry's value — Java's <c>SET</c> (id 0).</summary>
    Set = 0,

    /// <summary>
    /// Revert the entry to its default value, possibly null — Java's <c>DELETE</c> (id 1).
    /// ⚠ This is the operation whose <c>ConfigEntry.Value</c> is meaningfully
    /// <see langword="null"/>, and the null must reach the broker as a null.
    /// </summary>
    Delete = 1,

    /// <summary>
    /// For list-valued entries, add the given values to the current value — Java's
    /// <c>APPEND</c> (id 2).
    /// </summary>
    Append = 2,

    /// <summary>
    /// For list-valued entries, remove the given values from the current value — Java's
    /// <c>SUBTRACT</c> (id 3).
    /// </summary>
    Subtract = 3,
}
