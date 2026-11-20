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
using System.Collections.Generic;
using Confluent.Kafka.Common;
using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Consumer;

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// What an awaiting copy-out of a poll batch gives: the batch's owner once there is one, the records, and the
/// failure of a record with the positions to seek back to.
/// </summary>
internal sealed class AsyncCopyOut<TKey, TValue>
{
    // Owns the native batch from the moment it is set, also when the copy-out throws.
    internal SafeConsumerRecordsHandle? Batch { get; set; }

    // Null when the copy-out stopped because its token fired.
    internal ConsumerRecords<TKey, TValue>? Records { get; set; }

    internal RecordDeserializationException? Error { get; set; }

    internal IReadOnlyList<KeyValuePair<TopicPartition, long>> Rewind { get; set; } =
        Array.Empty<KeyValuePair<TopicPartition, long>>();
}
