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

using Confluent.Kafka.Common.Errors;
using Confluent.Kafka.Internal;
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Common;

/// <summary>
/// A Kafka error, Java's <c>KafkaException</c>: the root of the Kafka error classes, each of which a core error
/// arrives as, so the class is the category (<c>catch (RetriableException)</c>), and
/// <see cref="Exception.InnerException"/> is Java's <c>getCause()</c>.
/// </summary>
/// <remarks>
/// A Java built-in exception is its Java-named class in the <c>Confluent.Kafka.Java</c> namespaces, and a
/// .NET-only precondition throws the standard .NET exception.
/// </remarks>
public class KafkaException : Exception, IJavaThrowable
{
    private const int UnknownServerError = -1;

    // Java's getMessage(), null included; Message is then empty.
    private readonly string? _javaMessage;

    /// <summary>
    /// Initializes an exception with no message, so <see cref="Exception.Message"/> is empty; <see cref="Code"/> is
    /// <c>-1</c>.
    /// </summary>
    public KafkaException()
        : base(string.Empty)
    {
    }

    /// <summary>
    /// Initializes an exception with a message, an empty <see cref="Exception.Message"/> for
    /// <see langword="null"/>; <see cref="Code"/> is <c>-1</c>.
    /// </summary>
    public KafkaException(string? message)
        : base(message ?? string.Empty)
    {
        _javaMessage = message;
    }

    /// <summary>
    /// Initializes an exception with a message, an empty <see cref="Exception.Message"/> for
    /// <see langword="null"/>, and a cause; <see cref="Code"/> is <c>-1</c>.
    /// </summary>
    public KafkaException(string? message, Exception? innerException)
        : base(message ?? string.Empty, innerException)
    {
        _javaMessage = message;
    }

    /// <summary>
    /// Initializes an exception with a cause, whose Java <c>toString()</c>, its type's full name followed by
    /// <c>": "</c> and its message when it has one, becomes the message, or no message for a
    /// <see langword="null"/> cause; <see cref="Code"/> is <c>-1</c>.
    /// </summary>
    public KafkaException(Exception? innerException)
        : this(innerException is null ? null : JavaText.ToString(innerException), innerException)
    {
    }

    internal KafkaException(CoreError _, string? message, Exception? innerException)
        : this(message, innerException)
    {
    }

    /// <summary>
    /// The C ABI's id for the error's Java exception class; <c>-1</c>, the id of a bare Java
    /// <c>KafkaException</c>, for a class without an id of its own.
    /// </summary>
    public int Code => FfiId;

    internal virtual int FfiId => UnknownServerError;

    internal virtual string JavaToString() => JavaText.ThrowableToString(this, _javaMessage);

    string IJavaThrowable.JavaToString() => JavaToString();

    internal static KafkaException WrapAtPoll(Exception cause) => cause as KafkaException ?? new KafkaException(cause);

    internal static KafkaException WrapAtClose(Exception cause) =>
        new KafkaException("Failed to close kafka consumer", cause);

    /// <summary>
    /// Builds the exception of a core error: the managed exception it carries, or the class of its id, its source
    /// read the same way as the cause. An owned error is freed; a borrowed one is left to its owner.
    /// </summary>
    internal static Exception? FromFfiError(IntPtr error, bool owned)
    {
        if (error == IntPtr.Zero)
        {
            return null;
        }

        try
        {
            Exception? carried = ManagedExceptionCarrier.Carried(error);
            if (carried is not null)
            {
                return carried;
            }

            IntPtr outErr = IntPtr.Zero;
            int code = NativeMethods.ErrorEnum(error, ref outErr);
            ThrowIfFfiFailed(ref outErr);
            string? message = Utf8Marshal.PtrToString(NativeMethods.ErrorMessage(error, ref outErr));
            ThrowIfFfiFailed(ref outErr);
            bool isApiError = false;
            if (code == UnknownServerError)
            {
                isApiError = NativeMethods.ErrorIsApiError(error, ref outErr) != 0;
                ThrowIfFfiFailed(ref outErr);
            }

            IntPtr source = NativeMethods.ErrorSource(error, ref outErr);
            ThrowIfFfiFailed(ref outErr);
            Exception? inner = FromFfiError(source, owned: false);

            // A core error that wraps a callback's exception ("LocalCallbackError: <message>") is Java's
            // new KafkaException(t): its message is t.toString(), and a KafkaException t is thrown as it is.
            if (inner is not null && WrapsItsSource(message, source))
            {
                return inner as KafkaException ?? OfClass(code, isApiError, JavaText.ToString(inner), inner, error);
            }

            return OfClass(code, isApiError, message, inner, error);
        }
        finally
        {
            if (owned)
            {
                NativeMethods.ErrorDestroy(error);
            }
        }
    }

    /// <summary>
    /// The class of a core error's id. Id <c>-1</c> is both a bare <c>KafkaException</c> and
    /// <see cref="UnknownServerException"/>, told apart by the core's <c>is_api_error</c>; an id this binding does
    /// not know falls back to a bare <see cref="KafkaException"/>.
    /// </summary>
    internal static Exception OfClass(int code, bool isApiError, string? message, Exception? innerException, IntPtr error)
    {
        if (code == UnknownServerError)
        {
            return isApiError
                ? new UnknownServerException(default(CoreError), message, innerException)
                : new KafkaException(default(CoreError), message, innerException);
        }

        return ErrorClasses.Factories.TryGetValue(code, out Func<string?, Exception?, IntPtr, Exception>? factory)
            ? factory(message, innerException, error)
            : new KafkaException(default(CoreError), message, innerException);
    }

    private static bool WrapsItsSource(string? message, IntPtr source)
    {
        if (ManagedExceptionCarrier.Carried(source) is null)
        {
            return false;
        }

        IntPtr outErr = IntPtr.Zero;
        string? sourceMessage = Utf8Marshal.PtrToString(NativeMethods.ErrorMessage(source, ref outErr));
        ThrowIfFfiFailed(ref outErr);
        return message == "LocalCallbackError: " + sourceMessage;
    }

    internal static void ThrowIfFfiFailed(ref IntPtr outErr)
    {
        IntPtr error = outErr;
        outErr = IntPtr.Zero;
        if (error != IntPtr.Zero)
        {
            throw FromFfiError(error, owned: true)!;
        }
    }
}
