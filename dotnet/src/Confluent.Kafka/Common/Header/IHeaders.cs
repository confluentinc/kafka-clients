// Copyright 2026 Confluent Inc.
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

namespace Confluent.Kafka.Common.Header;

/// <summary>
/// A mutable ordered collection of <see cref="IHeader"/> objects. Note that multiple headers may have the same
/// <see cref="IHeader.Key()">key</see>. The order of headers is preserved in the order they were added.
/// </summary>
public interface IHeaders : IEnumerable<IHeader>
{
    /// <summary>Adds a header (key inside), to the end, returning if the operation succeeded.</summary>
    /// <param name="header">The header to be added.</param>
    /// <returns>This instance of the headers, once the header is added.</returns>
    /// <exception cref="IllegalStateException">The headers are in a read-only state.</exception>
    IHeaders Add(IHeader header);

    /// <summary>Creates and adds a header, to the end, returning if the operation succeeded.</summary>
    /// <param name="key">The key of the header to be added; must not be <see langword="null"/>.</param>
    /// <param name="value">The value of the header to be added; may be <see langword="null"/>.</param>
    /// <returns>This instance of the headers, once the header is added.</returns>
    /// <exception cref="IllegalStateException">The headers are in a read-only state.</exception>
    IHeaders Add(string key, byte[]? value);

    /// <summary>
    /// Removes all headers for the given key returning if the operation succeeded, while preserving the insertion order
    /// of the remaining headers.
    /// </summary>
    /// <param name="key">The key to remove all headers for; must not be <see langword="null"/>.</param>
    /// <returns>This instance of the headers, once the header is removed.</returns>
    /// <exception cref="IllegalStateException">The headers are in a read-only state.</exception>
    IHeaders Remove(string key);

    /// <summary>Returns just one (the very last) header for the given key, if present.</summary>
    /// <param name="key">The key to get the last header for; must not be <see langword="null"/>.</param>
    /// <returns>The last header matching the given key, or <see langword="null"/> if not present.</returns>
    IHeader? LastHeader(string key);

    /// <summary>Returns all headers for the given key, in the order they were added in, if present.</summary>
    /// <param name="key">The key to return the headers for; must not be <see langword="null"/>.</param>
    /// <returns>
    /// All headers for the given key, in the order they were added in; if no headers are present, an empty sequence.
    /// </returns>
    IEnumerable<IHeader> Headers(string key);

    /// <summary>Returns all headers as an array, in the order they were added in.</summary>
    /// <returns>
    /// The headers as an array; mutating this array will not affect the headers. If no headers are present, an empty
    /// array.
    /// </returns>
    IHeader[] ToArray();
}
