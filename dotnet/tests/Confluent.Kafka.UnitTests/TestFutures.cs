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

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Builds the done and failed futures a test hands to a Result's constructor.
/// </summary>
internal static class TestFutures
{
    internal static KafkaFutureImpl<object?> Done() => KafkaFutureImpl.CompletedFuture<object?>(null);

    internal static KafkaFutureImpl<object?> Failed(Exception error) => Failed<object?>(error);

    internal static KafkaFutureImpl<T> Failed<T>(Exception error)
    {
        KafkaFutureImpl<T> future = new KafkaFutureImpl<T>();
        Assert.True(future.CompleteWithError(error));
        return future;
    }
}
