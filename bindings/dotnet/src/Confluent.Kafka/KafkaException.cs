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
/// This maps the ABI's <c>kafka_common_Error_t</c> handle onto a managed
/// exception: <see cref="Code"/> and <see cref="IsRetriable"/> carry the
/// error's classification, and <see cref="Exception.Message"/> carries its
/// message. It is raised only for errors that <em>originate in the core</em>;
/// programmer errors (a null or out-of-range argument, use after disposal)
/// are validated in the binding before the native call and raise the
/// standard .NET exceptions (<see cref="ArgumentNullException"/>,
/// <see cref="ArgumentException"/>, <see cref="ObjectDisposedException"/>, …)
/// instead — never this type.
/// </para>
/// <para>
/// The type is intentionally <b>flat</b>: the ABI exposes only a code plus the
/// retriable flag, so a single exception carrying those is the faithful
/// shape today. A Java-style hierarchy of typed subclasses <em>may</em> be
/// introduced later, deriving from this same base — so <c>catch (KafkaException)</c>
/// keeps working unchanged. That is a purely additive, non-breaking evolution;
/// no timeline is implied.
/// </para>
/// <para>
/// <b><see cref="Exception.InnerException"/> is Java's <c>getCause()</c></b> (M15/P13.3,
/// decision D11). Where the core keeps a cause — wherever Java passes one to the
/// exception's constructor, for example <c>KafkaException("Failed to create new
/// KafkaAdminClient", exc)</c>, the <c>removeMembersFromConsumerGroup</c> remove-all wrap, or
/// a timeout that records the last error seen before its deadline — the exception built from
/// the native error carries it as an inner <see cref="KafkaException"/>, read through
/// <c>kafka_common_Error_cause</c>, with its own <see cref="Code"/>, message and cause in
/// turn. An error without a cause has a <see langword="null"/>
/// <see cref="Exception.InnerException"/>, as Java's <c>getCause()</c> is null.
/// </para>
/// <para>
/// There is no <c>IsFatal</c> flag: the Rust core's error redesign
/// (<c>a8205c5c</c>, "Error redesign") deliberately does not expose fatality
/// on the ABI's error handle — fatality is contextual (the same error class is
/// fatal in one call path and recoverable in another), not a property of the
/// error's type, so the core keeps it as a free function with a single
/// internal caller (CLAUDE.md §10.4) rather than a queryable flag. The
/// binding cannot recover a faithful value here and does not attempt to fake
/// one.
/// </para>
/// </remarks>
public class KafkaException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaException"/> class with a
    /// default message and no error classification (<see cref="Code"/> is 0,
    /// <see cref="IsRetriable"/> is <see langword="false"/>).
    /// </summary>
    public KafkaException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaException"/> class with the
    /// specified message and no error classification (<see cref="Code"/> is 0,
    /// <see cref="IsRetriable"/> is <see langword="false"/>).
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
    /// Initializes a new instance with an error classification and no cause. Used by the
    /// admin absent-key error (<c>AdminCallbacks.AbsentKey</c>), whose code <c>-4</c> no
    /// <c>kafka_common_Error_t</c> can carry — <c>kafka_common_Error_new</c> maps a
    /// non-protocol code to <c>UnknownServerError</c>. An exception built from a native
    /// error goes through the four-argument form instead, which also carries the cause.
    /// </summary>
    internal KafkaException(int code, string? message, bool isRetriable)
        : this(code, message, isRetriable, innerException: null)
    {
    }

    /// <summary>
    /// Initializes a new instance from the values copied out of a
    /// <c>kafka_common_Error_t</c> handle together with the exception built from its cause —
    /// Java's <c>KafkaException(String message, Throwable cause)</c>. Used by
    /// <see cref="FromBorrowedHandle(IntPtr)"/>, which reads the cause with
    /// <c>kafka_common_Error_cause</c>.
    /// </summary>
    /// <param name="code">The numeric error code.</param>
    /// <param name="message">The error message.</param>
    /// <param name="isRetriable">Whether the failed operation may succeed if retried.</param>
    /// <param name="innerException">
    /// The exception built from the core error's cause, or <see langword="null"/> when it has
    /// none (Java's null <c>getCause()</c>).
    /// </param>
    internal KafkaException(int code, string? message, bool isRetriable, Exception? innerException)
        : base(message, innerException)
    {
        Code = code;
        IsRetriable = isRetriable;
    }

    /// <summary>
    /// The numeric error code, mirroring the Kafka protocol error code carried by
    /// the core error (<c>kafka_common_Error_code</c>).
    /// </summary>
    public int Code { get; }

    /// <summary>
    /// Whether the failed operation may succeed if retried
    /// (<c>kafka_common_Error_is_retriable_error</c>).
    /// </summary>
    public bool IsRetriable { get; }

    /// <summary>
    /// Builds a <see cref="KafkaException"/> from a <c>kafka_common_Error_t</c>
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
    /// the handle (ffi §A5/§B5). The error's cause, if any, becomes its
    /// <see cref="Exception.InnerException"/> (see <see cref="FromBorrowedHandle(IntPtr)"/>).
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
    /// <para>
    /// ⚠ <b>The cause is freed here, even from a borrowed <paramref name="error"/>.</b> <c>kafka_common_Error_cause</c> returns an <em>owned</em>
    /// copy of the cause (or null), not a view into <paramref name="error"/>, so it is built
    /// with <see cref="FromHandle(IntPtr)"/> — which frees it exactly once, also when building
    /// it throws — and becomes the result's <see cref="Exception.InnerException"/>. Walking a
    /// longer chain is that same call recursing. <paramref name="error"/> itself is never
    /// freed here.
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

        // Java's getCause(). ⚠ The cause is an OWNED copy even when `error` is borrowed, so
        // it goes straight into FromHandle — no managed code runs between the P/Invoke
        // returning it and FromHandle's try, so nothing can throw and leak it — and
        // FromHandle frees it in its finally, including when building the inner exception
        // throws. That throw then propagates out of here, and the caller's own ownership rule
        // for `error` still holds: FromHandle's finally frees an owned `error`, a borrowed
        // one is left to its root. Recursion walks the chain until the core returns null.
        KafkaException? cause = FromHandle(NativeMethods.ErrorCause(error));

        return new KafkaException(code, message, isRetriable, cause);
    }
}
