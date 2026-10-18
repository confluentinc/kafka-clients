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
using System.Text;

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Internal.Serialization;

/// <summary>
/// The serde behind <see cref="Serdes.String"/>: UTF-8 strings, as Java's <c>StringSerializer</c> and
/// <c>StringDeserializer</c>.
/// </summary>
internal sealed class StringSerde : ISerde<string>
{
    public byte[]? Serialize(string topic, string data) =>
        data is null ? null : Encoding.UTF8.GetBytes(data);

    public string Deserialize(string topic, ReadOnlySpan<byte> data) =>
        Utf8Marshal.GetString(data);
}
