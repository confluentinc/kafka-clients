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
using Confluent.Kafka.Common.Header;
using Confluent.Kafka.Common.Record;

namespace Confluent.Kafka.Common.Errors;

/// <summary>
/// A fetched record whose key or value failed to deserialize, Java's <c>RecordDeserializationException</c>: the
/// record's partition, offset, timestamp, bytes and headers, with the deserializer's exception as
/// <see cref="Exception.InnerException"/>. Its <see cref="KafkaException.Code"/> is 0.
/// </summary>
public class RecordDeserializationException : SerializationException
{
    /// <summary>
    /// Initializes an exception for the record at <paramref name="offset"/> of <paramref name="partition"/>, with
    /// its timestamp, key and value bytes and headers, a message and a cause.
    /// </summary>
    public RecordDeserializationException(
        DeserializationExceptionOrigin? origin,
        TopicPartition partition,
        long offset,
        long timestamp,
        TimestampType timestampType,
        byte[]? keyBuffer,
        byte[]? valueBuffer,
        Headers? headers,
        string? message,
        Exception? cause)
        : base(message, cause)
    {
        Origin = origin;
        TopicPartition = partition;
        Offset = offset;
        TimestampType = timestampType;
        Timestamp = timestamp;
        KeyBuffer = keyBuffer;
        ValueBuffer = valueBuffer;
        Headers = headers;
    }

    /// <summary>Which part of the record failed to deserialize, Java's <c>DeserializationExceptionOrigin</c>.</summary>
    public enum DeserializationExceptionOrigin
    {
        /// <summary>The key.</summary>
        Key,

        /// <summary>The value.</summary>
        Value,
    }

    /// <summary>Whether the key or the value failed, or <see langword="null"/> when the constructor was given none.</summary>
    public DeserializationExceptionOrigin? Origin { get; }

    /// <summary>The record's partition.</summary>
    public TopicPartition TopicPartition { get; }

    /// <summary>The record's offset.</summary>
    public long Offset { get; }

    /// <summary>The record's timestamp type.</summary>
    public TimestampType TimestampType { get; }

    /// <summary>The record's timestamp.</summary>
    public long Timestamp { get; }

    /// <summary>A copy of the record's key bytes, or <see langword="null"/> for a null key.</summary>
    public byte[]? KeyBuffer { get; }

    /// <summary>A copy of the record's value bytes, or <see langword="null"/> for a null value.</summary>
    public byte[]? ValueBuffer { get; }

    /// <summary>The record's headers, or <see langword="null"/> when the constructor was given none.</summary>
    public Headers? Headers { get; }
}
