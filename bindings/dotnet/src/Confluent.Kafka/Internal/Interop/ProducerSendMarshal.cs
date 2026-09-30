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
/// The <c>unsafe</c> send-path marshalling for the producer's inline pull-pump (ffi §A4,
/// PLAN §3 Option C): pins the key / value bytes <b>call-scoped</b> (the core copies them
/// synchronously during <c>Producer_send</c>, verified <c>src/ffi/producer.rs</c>), applies the
/// absent / empty / present sentinel logic, and returns the future handle — or throws a
/// <see cref="KafkaException"/> on a synchronous <c>out_error</c>. Lives in
/// <c>Internal/Interop/</c> because it is the only send-path code that needs <c>unsafe</c>
/// (the <c>fixed</c> pins + raw pointers), keeping <c>unsafe</c> quarantined here (CLAUDE.md §2).
/// </summary>
internal static class ProducerSendMarshal
{
    /// <summary>
    /// Sends one record synchronously through <c>kafka_producer_Producer_send</c> and returns the
    /// resulting <c>FutureRecordMetadata_t</c> handle. The topic and the key / value buffers are
    /// pinned only for the duration of the P/Invoke — the core copies them into the batch buffer
    /// during the call, so the pins are released the moment this returns (ffi §A4). Sentinels
    /// (§A4): an <b>absent</b> (<see langword="null"/>) key/value passes
    /// <see cref="IntPtr.Zero"/> + <c>len -1</c>; an <b>empty</b> (zero-length) one passes a
    /// <b>non-null stack sentinel</b> + <c>len 0</c> (a <c>fixed</c> over an empty span yields a
    /// null pointer, which the core rejects for a non-negative length); a <b>present</b> one
    /// passes the pinned pointer + its length.
    /// </summary>
    /// <param name="producer">
    /// The owned producer handle. Passed as the <see cref="SafeProducerHandle"/> (not a raw
    /// <see cref="IntPtr"/>) so the P/Invoke marshaler auto-<c>DangerousAddRef</c>/<c>Release</c>s it
    /// around the synchronous <c>Producer_send</c> — the call-scoped guard against a concurrent
    /// <c>Producer_destroy</c> (ffi §A2 sync-op form). A closed handle marshals to
    /// <see cref="ObjectDisposedException"/>.
    /// </param>
    /// <param name="topic">The destination topic (non-null; validated by the caller).</param>
    /// <param name="partition">The target partition, or <c>-1</c> for no hint.</param>
    /// <param name="timestamp">The timestamp in ms, or <c>-1</c> to let the producer stamp it.</param>
    /// <param name="key">The record key, or <see langword="null"/> for no key.</param>
    /// <param name="value">The record value, or <see langword="null"/> for a tombstone.</param>
    /// <returns>A non-null <c>FutureRecordMetadata_t</c> handle on success.</returns>
    /// <exception cref="KafkaException">The core reported a synchronous send failure.</exception>
    internal static unsafe IntPtr Send(
        SafeProducerHandle producer,
        string topic,
        int partition,
        long timestamp,
        ReadOnlyMemory<byte>? key,
        ReadOnlyMemory<byte>? value)
    {
        // Call-scoped topic pin: Producer_send copies the topic synchronously (ffi §A3).
        using Utf8Marshal.PinnedUtf8String topicPin = Utf8Marshal.Pin(topic);

        ReadOnlySpan<byte> keySpan = key.HasValue ? key.Value.Span : default;
        ReadOnlySpan<byte> valueSpan = value.HasValue ? value.Value.Span : default;

        // A non-null stack sentinel for the empty (Length == 0) case: `fixed` over an empty span
        // yields a NULL pointer, and the core rejects (null, len >= 0). Its address is guaranteed
        // non-null, so it distinguishes empty (non-null ptr, len 0) from absent (null ptr, len -1).
        byte emptySentinel = 0;

        fixed (byte* keyPtr = keySpan)
        fixed (byte* valuePtr = valueSpan)
        {
            byte* keyArg;
            int keyLen;
            if (!key.HasValue)
            {
                keyArg = null;              // absent
                keyLen = -1;
            }
            else if (keySpan.Length == 0)
            {
                keyArg = &emptySentinel;    // empty: non-null pointer, length 0
                keyLen = 0;
            }
            else
            {
                keyArg = keyPtr;            // present
                keyLen = keySpan.Length;
            }

            byte* valueArg;
            int valueLen;
            if (!value.HasValue)
            {
                valueArg = null;            // absent (tombstone)
                valueLen = -1;
            }
            else if (valueSpan.Length == 0)
            {
                valueArg = &emptySentinel;  // empty: non-null pointer, length 0
                valueLen = 0;
            }
            else
            {
                valueArg = valuePtr;        // present
                valueLen = valueSpan.Length;
            }

            IntPtr future = NativeMethods.ProducerSend(
                producer,
                topicPin.Pointer,
                partition,
                timestamp,
                (IntPtr)keyArg,
                keyLen,
                (IntPtr)valueArg,
                valueLen,
                out IntPtr error);

            // Synchronous validation / buffer failure: null future + non-null out_error. FromHandle
            // frees the error exactly once (null-safe) and returns null on success.
            KafkaException? failure = KafkaException.FromHandle(error);
            if (failure is not null)
            {
                throw failure;
            }

            return future;
        }
    }

    /// <summary>
    /// The direct async send (send-approach-2 POC): one
    /// <c>kafka_producer_Producer_send_with_callback_cancellable</c> call on the caller's thread,
    /// with the key / value pinned <b>call-scoped</b> exactly as <see cref="Send"/> pins them — the
    /// core consumes both before it returns (ffi §A4), so nothing is borrowed past this call and the
    /// deferred-send "do not mutate after Send" window does not exist on this path. The same
    /// absent / empty / present sentinels as <see cref="Send"/>.
    /// </summary>
    /// <remarks>
    /// Returns the raw results rather than throwing, because the caller must look at
    /// <paramref name="callbackPending"/> and the error code together before deciding who owns
    /// <paramref name="userData"/> and which exception to raise.
    /// </remarks>
    /// <param name="producer">The producer, as the <see cref="SafeProducerHandle"/> (call-scoped auto-ref).</param>
    /// <param name="topic">A NUL-terminated UTF-8 topic buffer, valid for the duration of the call.</param>
    /// <param name="partition">The target partition, or <c>-1</c>.</param>
    /// <param name="timestamp">The timestamp in ms, or <c>-1</c>.</param>
    /// <param name="key">The key, or <see langword="null"/>.</param>
    /// <param name="value">The value, or <see langword="null"/>.</param>
    /// <param name="userData">The rooted <see cref="DirectSendCompletion"/> handle.</param>
    /// <param name="cancel">A <c>SendCancelToken_t</c>, or <see cref="IntPtr.Zero"/> for a non-cancellable send.</param>
    /// <param name="callbackPending">Whether the delivery callback owns <paramref name="userData"/>.</param>
    /// <param name="error">The synchronous error handle (owned by the caller), or <see cref="IntPtr.Zero"/>.</param>
    /// <returns>The future handle on success (owned by the caller), or <see cref="IntPtr.Zero"/>.</returns>
    internal static unsafe IntPtr SendWithCallback(
        SafeProducerHandle producer,
        IntPtr topic,
        int partition,
        long timestamp,
        ReadOnlyMemory<byte>? key,
        ReadOnlyMemory<byte>? value,
        IntPtr userData,
        IntPtr cancel,
        out bool callbackPending,
        out IntPtr error)
    {
        ReadOnlySpan<byte> keySpan = key.HasValue ? key.Value.Span : default;
        ReadOnlySpan<byte> valueSpan = value.HasValue ? value.Value.Span : default;

        // Non-null stack sentinel for a present-but-empty buffer (see Send).
        byte emptySentinel = 0;

        fixed (byte* keyPtr = keySpan)
        fixed (byte* valuePtr = valueSpan)
        {
            SelectArgument(key.HasValue, keySpan.Length, keyPtr, &emptySentinel, out byte* keyArg, out int keyLen);
            SelectArgument(value.HasValue, valueSpan.Length, valuePtr, &emptySentinel, out byte* valueArg, out int valueLen);

            return NativeMethods.ProducerSendWithCallbackCancellable(
                producer,
                topic,
                partition,
                timestamp,
                (IntPtr)keyArg,
                keyLen,
                (IntPtr)valueArg,
                valueLen,
                ProducerCallbacks.Send,
                userData,
                cancel,
                out callbackPending,
                out error);
        }
    }

    /// <summary>
    /// The ABI's key / value sentinel for one buffer (ffi §A4): absent → null + <c>-1</c>; empty →
    /// the non-null <paramref name="emptySentinel"/> + <c>0</c>; present → the pinned pointer + length.
    /// </summary>
    private static unsafe void SelectArgument(
        bool hasValue,
        int length,
        byte* pinned,
        byte* emptySentinel,
        out byte* argument,
        out int argumentLength)
    {
        if (!hasValue)
        {
            argument = null;
            argumentLength = -1;
        }
        else if (length == 0)
        {
            argument = emptySentinel;
            argumentLength = 0;
        }
        else
        {
            argument = pinned;
            argumentLength = length;
        }
    }
}
