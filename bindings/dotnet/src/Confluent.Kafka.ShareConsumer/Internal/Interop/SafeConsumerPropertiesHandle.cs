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

namespace Confluent.Kafka.ShareConsumer.Internal.Interop;

/// <summary>
/// Owned handle over a <c>kafka_consumer_ConsumerProperties_t</c> — the
/// short-lived config builder passed to
/// <see cref="NativeMethods.KafkaConsumerNew"/>. The caller retains ownership
/// across that call (the ABI does not consume it), so the binding disposes it
/// immediately afterward (ffi §B2, Category 1). Passing it typed as a
/// <see cref="System.Runtime.InteropServices.SafeHandle"/> lets the marshaller
/// keep it alive across the P/Invoke (PLAN D6).
/// </summary>
internal sealed class SafeConsumerPropertiesHandle : SafeHandleZeroIsInvalid
{
    private SafeConsumerPropertiesHandle()
    {
    }

    /// <summary>
    /// Allocates a new, empty properties handle via
    /// <c>kafka_consumer_ConsumerProperties_new</c>.
    /// </summary>
    internal static SafeConsumerPropertiesHandle Create()
    {
        IntPtr raw = NativeMethods.ConsumerPropertiesNew();
        var handle = new SafeConsumerPropertiesHandle();
        handle.SetHandle(raw);
        return handle;
    }

    /// <inheritdoc/>
    protected override bool ReleaseHandle()
    {
        NativeMethods.ConsumerPropertiesDestroy(handle);
        return true;
    }
}
