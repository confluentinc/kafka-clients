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
/// A Kafka error, Java's <c>KafkaException</c>, as one flat type: <see cref="Code"/> and
/// <see cref="IsRetriable"/> classify it, and <see cref="Exception.InnerException"/> is Java's <c>getCause()</c>.
/// </summary>
/// <remarks>A failed precondition throws the standard .NET exception instead.</remarks>
public class KafkaException : Exception
{
    private const int LocalConcurrentModification = -2;

    private const int LocalIllegalArgument = -3;

    private const int LocalIllegalState = -4;

    private const int LocalTimeout = -5;

    /// <summary>
    /// Initializes an exception with a default message; <see cref="Code"/> is 0 and <see cref="IsRetriable"/>
    /// is <see langword="false"/>.
    /// </summary>
    public KafkaException()
    {
    }

    /// <summary>
    /// Initializes an exception with a message; <see cref="Code"/> is 0 and <see cref="IsRetriable"/> is
    /// <see langword="false"/>.
    /// </summary>
    public KafkaException(string? message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes an exception with a message and a cause; <see cref="Code"/> is 0 and
    /// <see cref="IsRetriable"/> is <see langword="false"/>.
    /// </summary>
    public KafkaException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    internal KafkaException(int code, string? message, bool isRetriable)
        : this(code, message, isRetriable, innerException: null)
    {
    }

    internal KafkaException(int code, string? message, bool isRetriable, Exception? innerException)
        : base(message, innerException)
    {
        Code = code;
        IsRetriable = isRetriable;
    }

    /// <summary>The error's code; for an error the broker reported, its Kafka protocol error code.</summary>
    public int Code { get; }

    /// <summary>Whether the failed operation may succeed if retried.</summary>
    public bool IsRetriable { get; }

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

            return code switch
            {
                LocalConcurrentModification or LocalIllegalState => new InvalidOperationException(message, inner),
                LocalIllegalArgument => new ArgumentException(message, inner),
                LocalTimeout => new TimeoutException(message, inner),
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
