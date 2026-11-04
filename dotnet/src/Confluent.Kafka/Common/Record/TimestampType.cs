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

using Confluent.Kafka.Consumer;

namespace Confluent.Kafka.Common.Record;

/// <summary>
/// The kind of a <see cref="ConsumerRecord{TKey, TValue}"/>'s timestamp, Java's <c>TimestampType</c>, each
/// value its Java id.
/// </summary>
/// <remarks>
/// <c>ToString()</c> gives the Java constant name (<c>CREATE_TIME</c>), not Java's <c>toString()</c>
/// text (<c>"CreateTime"</c>): a recorded deviation.
/// </remarks>
public enum TimestampType
{
    /// <summary>No timestamp: Java's <c>NO_TIMESTAMP_TYPE</c>.</summary>
    NO_TIMESTAMP_TYPE = -1,

    /// <summary>The time the record was produced: Java's <c>CREATE_TIME</c>.</summary>
    CREATE_TIME = 0,

    /// <summary>The time the broker appended the record to the log: Java's <c>LOG_APPEND_TIME</c>.</summary>
    LOG_APPEND_TIME = 1,
}
