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

using Confluent.Kafka.ShareConsumer.Internal.Interop;

namespace Confluent.Kafka.ShareConsumer;

/// <summary>
/// The exception raised for an <b>operational</b> Kafka error surfaced by the
/// Rust core across the C ABI — the .NET realization of the Java client's
/// <c>org.apache.kafka.common.KafkaException</c> hierarchy.
/// </summary>
/// <remarks>
/// <para>
/// This maps the ABI's <c>kafka_common_KafkaError_t</c> handle onto a managed
/// exception: <see cref="Code"/>, <see cref="IsRetriable"/>, and
/// <see cref="IsFatal"/> carry the error's classification, and
/// <see cref="Exception.Message"/> carries its message. It is raised only for
/// errors that <em>originate in the core</em>; programmer errors (a null or
/// out-of-range argument, use after disposal) are validated in the binding
/// before the native call and raise the standard .NET exceptions
/// (<see cref="ArgumentNullException"/>, <see cref="ArgumentException"/>,
/// <see cref="ObjectDisposedException"/>, …) instead — never this type.
/// </para>
/// <para>
/// The type is intentionally <b>flat</b>: the ABI exposes only a code plus the
/// retriable / fatal flags, so a single exception carrying those is the faithful
/// shape today. A Java-style hierarchy of typed subclasses <em>may</em> be
/// introduced later, deriving from this same base — so <c>catch (KafkaException)</c>
/// keeps working unchanged. That is a purely additive, non-breaking evolution;
/// no timeline is implied.
/// </para>
/// </remarks>
public class KafkaException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaException"/> class with a
    /// default message and no error classification (<see cref="Code"/> is 0,
    /// <see cref="IsRetriable"/> and <see cref="IsFatal"/> are <see langword="false"/>).
    /// </summary>
    public KafkaException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaException"/> class with the
    /// specified message and no error classification (<see cref="Code"/> is 0,
    /// <see cref="IsRetriable"/> and <see cref="IsFatal"/> are <see langword="false"/>).
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public KafkaException(string? message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaException"/> class with the
    /// specified message and a reference to the inner exception that is the cause of
    /// this exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">
    /// The exception that is the cause of the current exception, or
    /// <see langword="null"/> if none is specified.
    /// </param>
    public KafkaException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// Initializes a new instance from the values copied out of a
    /// <c>kafka_common_KafkaError_t</c> handle. Used by
    /// <see cref="FromHandle(IntPtr)"/> — the only place a classified
    /// <see cref="KafkaException"/> is constructed.
    /// </summary>
    internal KafkaException(int code, string? message, bool isRetriable, bool isFatal)
        : base(message)
    {
        Code = code;
        IsRetriable = isRetriable;
        IsFatal = isFatal;
    }

    /// <summary>
    /// The numeric error code, mirroring the Kafka protocol error code carried by
    /// the core error (<c>kafka_common_KafkaError_code</c>).
    /// </summary>
    public int Code { get; }

    /// <summary>
    /// Whether the failed operation may succeed if retried
    /// (<c>kafka_common_KafkaError_is_retriable</c>).
    /// </summary>
    public bool IsRetriable { get; }

    /// <summary>
    /// Whether the error is fatal — unrecoverable at the client level
    /// (<c>kafka_common_KafkaError_is_fatal</c>).
    /// </summary>
    public bool IsFatal { get; }

    /// <summary>
    /// Builds a <see cref="KafkaException"/> from a <c>kafka_common_KafkaError_t</c>
    /// handle returned by the ABI, then <b>frees the handle exactly once</b>.
    /// </summary>
    /// <param name="error">
    /// The error handle from an <c>out_error</c> slot or an error-returning
    /// function. <see cref="IntPtr.Zero"/> means success.
    /// </param>
    /// <returns>
    /// <see langword="null"/> when <paramref name="error"/> is
    /// <see cref="IntPtr.Zero"/> (success); otherwise the mapped exception, which
    /// the caller typically throws.
    /// </returns>
    /// <remarks>
    /// The message is read <b>before</b> the handle is freed (the borrowed
    /// <c>const char*</c> dies with the handle), all values are copied out, and the
    /// handle is destroyed in a <c>finally</c> so it is freed exactly once even if
    /// construction throws. The returned exception holds only copied values — never
    /// the handle (ffi §A5/§B5).
    /// </remarks>
    internal static KafkaException? FromHandle(IntPtr error)
    {
        if (error == IntPtr.Zero)
        {
            return null;
        }

        try
        {
            // Message BEFORE free: the borrowed const char* is valid only while the
            // handle lives, so copy it out (and every other value) before _destroy.
            int code = NativeMethods.Code(error);
            string? message = Utf8Marshal.PtrToString(NativeMethods.Message(error));
            bool isRetriable = NativeMethods.IsRetriable(error);
            bool isFatal = NativeMethods.IsFatal(error);

            return new KafkaException(code, message, isRetriable, isFatal);
        }
        finally
        {
            NativeMethods.ErrorDestroy(error);
        }
    }
}
