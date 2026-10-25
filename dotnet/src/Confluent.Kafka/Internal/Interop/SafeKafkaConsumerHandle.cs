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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// The handle of a <c>kafka_consumer_KafkaConsumer_t</c>, held as its <c>Consumer</c> view and destroyed with
/// <c>kafka_consumer_KafkaConsumer_destroy_err</c>.
/// </summary>
internal sealed class SafeKafkaConsumerHandle : SafeConsumerHandle
{
    internal SafeKafkaConsumerHandle(IntPtr view, IntPtr classPointer)
        : base(view, classPointer)
    {
    }

    private protected override void DestroyClass(IntPtr classPointer)
    {
        IntPtr outErr = IntPtr.Zero;
        NativeMethods.KafkaConsumerDestroy(classPointer, ref outErr);
        KafkaException.ThrowIfFfiFailed(ref outErr);
    }
}
