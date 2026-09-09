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

using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka;

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
            // Read every value out BEFORE the free (the message is a borrowed const
            // char* that dies with the handle). The read itself is shared with
            // FromBorrowedHandle so the two overloads cannot drift; the ONLY
            // difference between them is this method's finally.
            return FromBorrowedHandle(error);
        }
        finally
        {
            NativeMethods.ErrorDestroy(error);
        }
    }

    /// <summary>
    /// Builds a <see cref="KafkaException"/> from a <b>borrowed</b>
    /// <c>kafka_common_KafkaError_t</c> pointer — the non-destroying sibling of
    /// <see cref="FromHandle(IntPtr)"/>. Reads the accessors and copies every value
    /// out; the handle is <b>never</b> freed, because this side of the ownership line
    /// does not own it.
    /// </summary>
    /// <param name="error">
    /// A borrowed error pointer, valid only until its owning root is destroyed.
    /// <see cref="IntPtr.Zero"/> means "no error" (success).
    /// </param>
    /// <returns>
    /// <see langword="null"/> when <paramref name="error"/> is
    /// <see cref="IntPtr.Zero"/>; otherwise the mapped exception, holding only copied
    /// values.
    /// </returns>
    /// <remarks>
    /// <para>
    /// ⚠ <b>Which overload to use is decided by const-ness in the header, and the same
    /// C type appears on both sides of that line.</b> An admin per-key error
    /// (<c>kafka_admin_*Result_get_error(result, i)</c>) is returned as
    /// <c>const kafka_common_KafkaError_t*</c> and the header says verbatim: "The
    /// pointer is borrowed from the result handle — read it with the
    /// <c>kafka_common_KafkaError_*</c> accessors, but do <b>not</b> destroy it." It
    /// dies with the result root, which the completion trampoline destroys exactly
    /// once. Using <see cref="FromHandle(IntPtr)"/> there frees it a second time — a
    /// double free, i.e. a process abort that <em>no managed assertion can catch</em>.
    /// </para>
    /// <para>
    /// The mirror-image mistake costs a leak: an admin completion callback's
    /// <c>error</c> <b>parameter</b> is a <em>non-const</em>
    /// <c>kafka_common_KafkaError_t*</c> that the callback <b>owns and must free</b>,
    /// so that one takes <see cref="FromHandle(IntPtr)"/>. Read the accessor's
    /// signature, never the type name.
    /// </para>
    /// </remarks>
    internal static KafkaException? FromBorrowedHandle(IntPtr error)
    {
        if (error == IntPtr.Zero)
        {
            return null;
        }

        int code = NativeMethods.Code(error);
        string? message = Utf8Marshal.PtrToString(NativeMethods.Message(error));
        bool isRetriable = NativeMethods.IsRetriable(error);
        bool isFatal = NativeMethods.IsFatal(error);

        return new KafkaException(code, message, isRetriable, isFatal);
    }
}
