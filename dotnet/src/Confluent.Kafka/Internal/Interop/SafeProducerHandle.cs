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
/// Owned handle over a <c>kafka_producer_Producer_t</c> — the client handle returned
/// <b>directly</b> by <c>kafka_producer_KafkaProducer_new</c> / <c>MockProducer_new</c>
/// (ffi §A2, Category 1). The interop marshaller invokes the private parameterless
/// ctor and sets the handle atomically on return (M2/P2), so the binding never wraps
/// a raw pointer itself and there is no create→<c>SetHandle</c> allocation-gap window;
/// a null native return simply yields an <c>IsInvalid</c> handle whose
/// <see cref="ReleaseHandle"/> is skipped (no spurious <c>Producer_destroy</c>).
/// </summary>
/// <remarks>
/// <see cref="ReleaseHandle"/> calls <c>Producer_destroy</c>, which <b>blocks</b>: it
/// drops the runtime and joins the background Sender task (ffi §A2). This is why the
/// producer closes via <see cref="System.IDisposable.Dispose"/> and never the
/// finalizer — a blocking destroy is wrong on the finalizer thread. The owning lifecycle
/// wrapper's teardown now routes through the graceful <c>Producer_close</c> first, plus
/// the pending-send flush and the completion-pump join (added with the send/flush phases;
/// ffi §A2/§A7) — the M11/P1-era "straight through this bare destroy" description is
/// obsolete.
/// <para>
/// <b>Release is ref-counted, so the destroy is not always immediate or on the disposing
/// thread</b> (M11/P8, the surviving doc tail of the dropped Major 6). Every async op holds
/// a span-the-op count and every sync call holds the marshaller's call-scoped count, so
/// <see cref="ReleaseHandle"/> → <c>Producer_destroy</c> runs only when the count reaches
/// zero — which, if an op's completion callback releases the last count, is the core's
/// <b>dispatcher thread</b>, not the caller's. That is safe by construction: the core
/// explicitly <em>detaches</em> its dispatcher rather than joining it (a join would hang,
/// per its own comment), and the completion channel is unbounded, so in-flight callbacks
/// cannot block the destroy either. No deadlock, no use-after-free, no leak — do not
/// re-file it.
/// </para>
/// </remarks>
internal sealed class SafeProducerHandle : SafeHandleZeroIsInvalid
{
    private SafeProducerHandle()
    {
    }

    /// <inheritdoc/>
    protected override bool ReleaseHandle()
    {
        NativeMethods.ProducerDestroy(handle);
        return true;
    }
}
