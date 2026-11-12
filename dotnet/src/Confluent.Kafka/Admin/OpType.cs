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
/// What an <see cref="AlterConfigOp"/> does to a configuration entry — Java's nested
/// <c>AlterConfigOp.OpType</c>.
/// </summary>
/// <remarks>
/// <para>
/// Each member's value is Java's <c>OpType.id()</c>, written out rather than assigned by order.
/// </para>
/// <para>
/// It is not nested, a recorded deviation: <see cref="AlterConfigOp.OpType()"/> is a method, and a
/// nested type of the same name would not compile (<c>CS0102</c>).
/// </para>
/// </remarks>
public enum OpType
{
    /// <summary>Set the entry's value — Java's <c>SET</c> (id 0).</summary>
    SET = 0,

    /// <summary>
    /// Revert the entry to its default value, possibly null — Java's <c>DELETE</c> (id 1). Its
    /// entry's value may be <see langword="null"/>, and is sent as a null.
    /// </summary>
    DELETE = 1,

    /// <summary>
    /// For list-valued entries, add the given values to the current value — Java's
    /// <c>APPEND</c> (id 2).
    /// </summary>
    APPEND = 2,

    /// <summary>
    /// For list-valued entries, remove the given values from the current value — Java's
    /// <c>SUBTRACT</c> (id 3).
    /// </summary>
    SUBTRACT = 3,
}

/// <summary>
/// The members Java declares on the <see cref="OpType"/> enum itself, which a C# enum cannot carry. From
/// C# 14 the static one is called on the enum (<c>OpType.ForId(…)</c>); older C# calls it on this class.
/// </summary>
public static class OpTypeMethods
{
    extension(OpType)
    {
        /// <summary>
        /// Return the operation type with the given id, or <see langword="null"/> if no type has it — Java's
        /// <c>forId(byte)</c>.
        /// </summary>
        /// <param name="id">The id.</param>
        /// <returns>The operation type, or <see langword="null"/>; never throws.</returns>
        public static OpType? ForId(byte id) => id switch
        {
            (byte)OpType.SET => OpType.SET,
            (byte)OpType.DELETE => OpType.DELETE,
            (byte)OpType.APPEND => OpType.APPEND,
            (byte)OpType.SUBTRACT => OpType.SUBTRACT,
            _ => null,
        };
    }

    extension(OpType opType)
    {
        /// <summary>Return the id of this operation type — Java's <c>id()</c>.</summary>
        /// <returns>
        /// The id, which is the member's value. Of a value no member has, only the low byte is kept:
        /// <c>((OpType)259).Id()</c> is 3.
        /// </returns>
        public byte Id() => unchecked((byte)opType);
    }
}
