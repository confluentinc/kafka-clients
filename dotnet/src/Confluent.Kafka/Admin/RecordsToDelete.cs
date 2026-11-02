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
using System.Globalization;

namespace Confluent.Kafka.Admin;

/// <summary>
/// Describes the records to delete from one partition in a call to
/// <see cref="IAdmin.DeleteRecords"/> — Java's
/// <c>org.apache.kafka.clients.admin.RecordsToDelete</c>.
/// </summary>
public sealed class RecordsToDelete : IEquatable<RecordsToDelete>
{
    private readonly long _offset;

    private RecordsToDelete(long offset)
    {
        _offset = offset;
    }

    /// <summary>
    /// Deletes all the records before <paramref name="offset"/> — Java's
    /// <c>beforeOffset(long)</c>.
    /// </summary>
    /// <param name="offset">
    /// The offset before which all records will be deleted. Use <c>-1</c> to truncate to
    /// the high watermark.
    /// </param>
    /// <returns>The request entry.</returns>
    public static RecordsToDelete BeforeOffset(long offset) => new RecordsToDelete(offset);

    /// <summary>
    /// The offset before which all records will be deleted — Java's instance
    /// <c>beforeOffset()</c>. Use <c>-1</c> to truncate to the high watermark.
    /// </summary>
    /// <returns>The offset.</returns>
    /// <remarks>
    /// A method, as Java's: C# forbids a property named like the
    /// <see cref="BeforeOffset(long)"/> factory.
    /// </remarks>
    public long BeforeOffset() => _offset;

    /// <summary>
    /// Value equality on the offset — Java's <c>equals</c>.
    /// </summary>
    /// <param name="other">The instance to compare with.</param>
    /// <returns><see langword="true"/> if the offsets are equal.</returns>
    public bool Equals(RecordsToDelete? other) => other is not null && _offset == other._offset;

    /// <inheritdoc/>
    public override bool Equals(object? obj) => Equals(obj as RecordsToDelete);

    /// <summary>The offset truncated to <see langword="int"/> — Java's <c>hashCode()</c>.</summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode() => unchecked((int)_offset);

    /// <summary>A diagnostic rendering matching Java's <c>toString()</c>.</summary>
    /// <returns>The rendering.</returns>
    public override string ToString() =>
        string.Format(CultureInfo.InvariantCulture, "(beforeOffset = {0})", _offset);
}
