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

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Pins the async send path of the producers a test creates (send-approach-2 POC), through
/// <see cref="NativeProducer.DirectSendOverride"/> — an <c>AsyncLocal</c>, so a parallel test is not
/// affected. The path is fixed when the producer is constructed, so only construction needs to run
/// inside the scope.
/// </summary>
/// <remarks>
/// Tests of the accumulator / pump machinery itself pin <see cref="Pump{T}"/>; tests of the direct
/// path pin <see cref="Direct{T}"/>. Every other producer test is path-agnostic and is meant to pass
/// on both: the suite runs as-is (direct, the default) and again with
/// <c>CONFLUENT_KAFKA_PRODUCER_ASYNC_SEND_PATH=pump</c>.
/// </remarks>
internal static class ProducerSendPath
{
    /// <summary>Creates a producer on the accumulator + pump path.</summary>
    internal static T Pump<T>(Func<T> create) => With(directSend: false, create);

    /// <summary>Creates a producer on the direct path.</summary>
    internal static T Direct<T>(Func<T> create) => With(directSend: true, create);

    private static T With<T>(bool directSend, Func<T> create)
    {
        bool? previous = NativeProducer.DirectSendOverride.Value;
        NativeProducer.DirectSendOverride.Value = directSend;
        try
        {
            return create();
        }
        finally
        {
            NativeProducer.DirectSendOverride.Value = previous;
        }
    }
}
