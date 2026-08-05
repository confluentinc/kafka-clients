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
/// The kind of timestamp carried by a <see cref="ConsumerRecord"/> — the .NET
/// realization of Java's <c>org.apache.kafka.common.record.TimestampType</c>. The
/// numeric values map directly onto the <c>int</c> the C ABI's
/// <c>ConsumerRecord_timestamp_type</c> returns.
/// </summary>
public enum TimestampType
{
    /// <summary>
    /// The record carries no timestamp (the ABI sentinel <c>-1</c>);
    /// <see cref="ConsumerRecord.Timestamp"/> is <c>-1</c> (<c>NO_TIMESTAMP</c>).
    /// </summary>
    NoTimestampType = -1,

    /// <summary>
    /// The timestamp is the time the record was produced (Kafka <c>CreateTime</c>,
    /// the ABI value <c>0</c>).
    /// </summary>
    CreateTime = 0,

    /// <summary>
    /// The timestamp is the time the broker appended the record to the log (Kafka
    /// <c>LogAppendTime</c>, the ABI value <c>1</c>).
    /// </summary>
    LogAppendTime = 1,
}
