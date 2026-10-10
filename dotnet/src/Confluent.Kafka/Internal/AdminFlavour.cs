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

namespace Confluent.Kafka.Internal;

/// <summary>
/// Which public admin surface an admin operation completes for (M15/P14 PLAN §3.1): the sync one, whose futures are
/// <see cref="KafkaFuture"/> / <see cref="KafkaFuture{T}"/> over <see cref="SyncCompletion{T}"/> latches, or the
/// async one, whose futures are <c>Task</c>s over <see cref="TaskAdminCompletion{T}"/>.
/// </summary>
/// <remarks>
/// The flavour is chosen once, when the operation is built, and decides only which per-key completion it creates
/// (<see cref="IAdminCompletion{T}"/>). The submit, the rooting, the countdown and the callbacks are the same code for
/// both.
/// </remarks>
internal enum AdminFlavour
{
    /// <summary>Per-key <see cref="SyncCompletion{T}"/> latches, read through a blocking <c>Get()</c>.</summary>
    Sync,

    /// <summary>Per-key <see cref="TaskAdminCompletion{T}"/> sources, read through their <c>Task</c>s.</summary>
    Async,
}
