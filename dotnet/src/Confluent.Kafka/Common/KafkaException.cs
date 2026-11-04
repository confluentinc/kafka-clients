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
using Confluent.Kafka.Internal.Interop;

namespace Confluent.Kafka.Common;

/// <summary>
/// A Kafka error, Java's <c>KafkaException</c>, as one flat type: <see cref="Code"/> and
/// <see cref="IsRetriable"/> classify it, and <see cref="Exception.InnerException"/> is Java's <c>getCause()</c>.
/// </summary>
/// <remarks>A failed precondition throws the standard .NET exception instead.</remarks>
public class KafkaException : Exception, IJavaThrowable
{
    private const int UnknownServerError = -1;

    private const int LocalConcurrentModification = -2;

    private const int LocalIllegalArgument = -3;

    private const int LocalIllegalState = -4;

    private const int LocalTimeout = -5;

    private const int LocalUnsupportedOperation = -31;

    private readonly int? _storedCode;

    // Java's getMessage(), null included; Message is then empty.
    private readonly string? _javaMessage;

    /// <summary>
    /// Initializes an exception with no message, so <see cref="Exception.Message"/> is empty; <see cref="Code"/> is
    /// <c>-1</c> and <see cref="IsRetriable"/> is <see langword="false"/>.
    /// </summary>
    public KafkaException()
        : base(string.Empty)
    {
    }

    /// <summary>
    /// Initializes an exception with a message, an empty <see cref="Exception.Message"/> for
    /// <see langword="null"/>; <see cref="Code"/> is <c>-1</c> and <see cref="IsRetriable"/> is
    /// <see langword="false"/>.
    /// </summary>
    public KafkaException(string? message)
        : base(message ?? string.Empty)
    {
        _javaMessage = message;
    }

    /// <summary>
    /// Initializes an exception with a message, an empty <see cref="Exception.Message"/> for
    /// <see langword="null"/>, and a cause; <see cref="Code"/> is <c>-1</c> and <see cref="IsRetriable"/> is
    /// <see langword="false"/>.
    /// </summary>
    public KafkaException(string? message, Exception? innerException)
        : base(message ?? string.Empty, innerException)
    {
        _javaMessage = message;
    }

    /// <summary>
    /// Initializes an exception with a cause, whose Java <c>toString()</c>, its type's full name followed by
    /// <c>": "</c> and its message when it has one, becomes the message, or no message for a
    /// <see langword="null"/> cause; <see cref="Code"/> is <c>-1</c> and <see cref="IsRetriable"/> is
    /// <see langword="false"/>.
    /// </summary>
    public KafkaException(Exception? innerException)
        : this(innerException is null ? null : JavaText.ToString(innerException), innerException)
    {
    }

    internal KafkaException(CoreError _, string? message, Exception? innerException)
        : this(message, innerException)
    {
    }

    internal KafkaException(int code, string? message, bool isRetriable)
        : this(code, message, isRetriable, innerException: null)
    {
    }

    internal KafkaException(int code, string? message, bool isRetriable, Exception? innerException)
        : this(message, innerException)
    {
        _storedCode = code;
        IsRetriable = isRetriable;
    }

    /// <summary>
    /// The C ABI's id for the error's Java exception class; <c>-1</c>, the id of a bare Java
    /// <c>KafkaException</c>, for a class without an id of its own.
    /// </summary>
    public int Code => FfiId;

    /// <summary>Whether the failed operation may succeed if retried.</summary>
    public bool IsRetriable { get; }

    internal virtual int FfiId => _storedCode ?? UnknownServerError;

    internal virtual string JavaToString() => JavaText.ThrowableToString(this, _javaMessage);

    string IJavaThrowable.JavaToString() => JavaToString();

    internal static KafkaException WrapAtPoll(Exception cause) => cause as KafkaException ?? new KafkaException(cause);

    internal static KafkaException WrapAtClose(Exception cause) =>
        new KafkaException(UnknownServerError, "Failed to close kafka consumer", false, cause);

    internal static KafkaException? FromHandle(IntPtr error)
    {
        if (error == IntPtr.Zero)
        {
            return null;
        }

        try
        {
            return FromBorrowedHandle(error);
        }
        finally
        {
            NativeMethods.ErrorDestroy(error);
        }
    }

    internal static KafkaException? FromBorrowedHandle(IntPtr error)
    {
        if (error == IntPtr.Zero)
        {
            return null;
        }

        IntPtr outErr = IntPtr.Zero;
        int code = NativeMethods.ErrorEnum(error, ref outErr);
        ThrowIfFailed(ref outErr);
        string? message = Utf8Marshal.PtrToString(NativeMethods.ErrorMessage(error, ref outErr));
        ThrowIfFailed(ref outErr);
        bool isRetriable = NativeMethods.ErrorIsRetriableError(error, ref outErr) != 0;
        ThrowIfFailed(ref outErr);
        IntPtr source = NativeMethods.ErrorSource(error, ref outErr);
        ThrowIfFailed(ref outErr);

        return new KafkaException(code, message, isRetriable, FromBorrowedHandle(source));
    }

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
            bool isRetriable = NativeMethods.ErrorIsRetriableError(error, ref outErr) != 0;
            ThrowIfFfiFailed(ref outErr);
            IntPtr source = NativeMethods.ErrorSource(error, ref outErr);
            ThrowIfFfiFailed(ref outErr);
            Exception? inner = FromFfiError(source, owned: false);

            // A core error that wraps a callback's exception ("LocalCallbackError: <message>") is Java's
            // new KafkaException(t): its message is t.toString(), and a KafkaException t is thrown as it is.
            if (inner is not null && WrapsItsSource(message, source))
            {
                return inner as KafkaException ?? new KafkaException(code, JavaText.ToString(inner), isRetriable, inner);
            }

            return code switch
            {
                LocalConcurrentModification or LocalIllegalState => new InvalidOperationException(message, inner),
                LocalIllegalArgument => new ArgumentException(message, inner),
                LocalTimeout => new TimeoutException(message, inner),
                LocalUnsupportedOperation => new NotSupportedException(message, inner),
                _ => new KafkaException(code, message, isRetriable, inner),
            };
        }
        finally
        {
            if (owned)
            {
                NativeMethods.ErrorDestroy(error);
            }
        }
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

    internal static void ThrowIfFailed(ref IntPtr outErr)
    {
        IntPtr error = outErr;
        outErr = IntPtr.Zero;
        if (error != IntPtr.Zero)
        {
            throw FromHandle(error)!;
        }
    }
}
