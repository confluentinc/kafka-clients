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

namespace Confluent.Kafka.Internal.Interop;

/// <summary>
/// Owned handle over a <c>kafka_producer_ProducerProperties_t</c> — the
/// short-lived config builder passed to
/// <see cref="NativeMethods.KafkaProducerNew"/>. The caller retains ownership
/// across that call (the ABI does not consume it — the header states the caller
/// "must free it separately"), so the binding disposes it immediately afterward
/// (ffi §A2, Category 1). Passing it typed as a
/// <see cref="System.Runtime.InteropServices.SafeHandle"/> lets the marshaller
/// keep it alive across the P/Invoke.
/// </summary>
internal sealed class SafeProducerPropertiesHandle : SafeHandleZeroIsInvalid
{
    private SafeProducerPropertiesHandle()
    {
    }

    /// <summary>
    /// Allocates a new, empty properties handle via
    /// <c>kafka_producer_ProducerProperties_new</c>. The interop marshaller invokes
    /// the private parameterless ctor and sets the handle atomically on return
    /// (M2/P2), so no raw pointer is ever exposed to managed code and there is no
    /// create→<c>SetHandle</c> allocation-gap window in which the native handle could
    /// leak on an async abort / OOM.
    /// </summary>
    internal static SafeProducerPropertiesHandle Create() => NativeMethods.ProducerPropertiesNew();

    /// <inheritdoc/>
    protected override bool ReleaseHandle()
    {
        NativeMethods.ProducerPropertiesDestroy(handle);
        return true;
    }
}
