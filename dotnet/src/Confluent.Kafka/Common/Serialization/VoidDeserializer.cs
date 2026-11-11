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

using System;

namespace Confluent.Kafka.Common.Serialization;

/// <summary>
/// Java's <c>VoidDeserializer</c>, over <see cref="object"/> since C# has no <c>Void</c> type argument. Java's
/// rejects any data that is not <see langword="null"/>, and a span is never <see langword="null"/>, so every call
/// throws; a consumer calls it only for a record with a key or a value.
/// </summary>
public sealed class VoidDeserializer : IDeserializer<object?>
{
    /// <inheritdoc/>
    public object? Deserialize(string topic, ReadOnlySpan<byte> data) =>
        throw new IllegalArgumentException("Data should be null for a VoidDeserializer.");
}
