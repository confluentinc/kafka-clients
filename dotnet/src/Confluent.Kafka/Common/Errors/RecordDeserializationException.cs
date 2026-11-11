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
/// <see cref="Exception.InnerException"/>.
/// </summary>
public class RecordDeserializationException : SerializationException
{
    private readonly DeserializationExceptionOrigin? _origin;
    private readonly TopicPartition _partition;
    private readonly long _offset;
    private readonly TimestampType _timestampType;
    private readonly long _timestamp;
    private readonly byte[]? _keyBuffer;
    private readonly byte[]? _valueBuffer;
    private readonly Headers? _headers;

    /// <summary>
    /// Initializes an exception for the record at <paramref name="offset"/> of <paramref name="partition"/>, with
    /// its timestamp, key and value bytes and headers, a message and an inner exception.
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
        Exception? innerException)
        : base(message, innerException)
    {
        _origin = origin;
        _partition = partition;
        _offset = offset;
        _timestampType = timestampType;
        _timestamp = timestamp;
        _keyBuffer = keyBuffer;
        _valueBuffer = valueBuffer;
        _headers = headers;
    }

    /// <summary>Which part of the record failed to deserialize, Java's <c>DeserializationExceptionOrigin</c>.</summary>
    public enum DeserializationExceptionOrigin
    {
        /// <summary>The key.</summary>
        KEY,

        /// <summary>The value.</summary>
        VALUE,
    }

    /// <summary>Whether the key or the value failed, Java's <c>origin()</c>.</summary>
    /// <returns>Whether the key or the value failed, or <see langword="null"/> when the constructor was given
    /// none.</returns>
    public DeserializationExceptionOrigin? Origin() => _origin;

    /// <summary>The record's partition, Java's <c>topicPartition()</c>.</summary>
    /// <returns>The record's partition.</returns>
    public TopicPartition TopicPartition() => _partition;

    /// <summary>The record's offset, Java's <c>offset()</c>.</summary>
    /// <returns>The record's offset.</returns>
    public long Offset() => _offset;

    /// <summary>The record's timestamp type, Java's <c>timestampType()</c>.</summary>
    /// <returns>The record's timestamp type.</returns>
    public TimestampType TimestampType() => _timestampType;

    /// <summary>The record's timestamp, Java's <c>timestamp()</c>.</summary>
    /// <returns>The record's timestamp.</returns>
    public long Timestamp() => _timestamp;

    /// <summary>The record's key bytes, Java's <c>keyBuffer()</c>.</summary>
    /// <returns>A copy of the record's key bytes, or <see langword="null"/> for a null key.</returns>
    public byte[]? KeyBuffer() => _keyBuffer;

    /// <summary>The record's value bytes, Java's <c>valueBuffer()</c>.</summary>
    /// <returns>A copy of the record's value bytes, or <see langword="null"/> for a null value.</returns>
    public byte[]? ValueBuffer() => _valueBuffer;

    /// <summary>The record's headers, Java's <c>headers()</c>.</summary>
    /// <returns>The record's headers, or <see langword="null"/> when the constructor was given none.</returns>
    public Headers? Headers() => _headers;

    internal override int FfiId => -27;
}
