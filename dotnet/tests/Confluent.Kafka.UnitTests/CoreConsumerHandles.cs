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

using System.Reflection;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Makes the core's <c>kafka_consumer_ConsumerHandle_t</c> over one of the four public consumers, through its private
/// <see cref="NativeConsumer"/>, for the tests of the handle the consumer routes its in-callback calls to.
/// </summary>
internal static class CoreConsumerHandles
{
    internal static NativeConsumerHandle Of(object consumer) => NativeConsumerHandle.Create(NativeOf(consumer));

    internal static NativeConsumer NativeOf(object consumer) =>
        (NativeConsumer)consumer.GetType()
            .GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(consumer)!;
}
