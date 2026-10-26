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
using System.Threading.Tasks;

namespace Confluent.Kafka;

/// <summary>
/// The async form of a producer's delivery callback: the record's future completes, and the producer's later
/// completions are handled, only after the task <see cref="OnCompletion"/> returns has ended.
/// </summary>
public interface IAsyncDeliveryCallback
{
    /// <summary>
    /// Called on the producer's pump thread when the record's delivery completes.
    /// </summary>
    /// <param name="metadata">The record's metadata; on failure, the placeholder with offset -1.</param>
    /// <param name="exception">The failure, or <c>null</c> when the record was delivered.</param>
    /// <returns>A task whose end lets the producer handle its next completion.</returns>
    Task OnCompletion(RecordMetadata metadata, Exception? exception);
}
