// Copyright 2026 Confluent Inc.
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

// GENERATED, DO NOT EDIT. Produced from the Java source by `cargo xtask generate-error-codes`
// and checked for staleness by `cargo xtask check-generated`.

using System;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Java.Util;

/// <summary>Java's built-in <c>java.util.ConcurrentModificationException</c>.</summary>
public class ConcurrentModificationException : global::System.InvalidOperationException, IJavaThrowable
{
    private readonly string? _javaMessage;

    /// <summary>Initializes the exception, as Java's <c>ConcurrentModificationException()</c>.</summary>
    public ConcurrentModificationException()
        : this(default(CoreError), null, null)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>ConcurrentModificationException(String message)</c>.</summary>
    public ConcurrentModificationException(string? message)
        : this(default(CoreError), message, null)
    {
    }

    /// <summary>Initializes the exception, as Java's <c>ConcurrentModificationException(Throwable cause)</c>.</summary>
    public ConcurrentModificationException(Exception? innerException)
        : this(default(CoreError), innerException is null ? null : JavaText.ToString(innerException), innerException)
    {
    }

    /// <summary>
    /// Initializes the exception, as Java's <c>ConcurrentModificationException(String message, Throwable cause)</c>.
    /// </summary>
    public ConcurrentModificationException(string? message, Exception? innerException)
        : this(default(CoreError), message, innerException)
    {
    }

    internal ConcurrentModificationException(CoreError _, string? message, Exception? innerException)
        : base(message ?? string.Empty, innerException)
    {
        _javaMessage = message;
    }

    string IJavaThrowable.JavaToString() => JavaText.ThrowableToString(this, _javaMessage);
}
