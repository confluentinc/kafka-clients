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
using System.Diagnostics;
using System.Globalization;
using System.Threading;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The key and value serializers or deserializers a client was given, disposed once, key then value: each one that is
/// <see cref="IDisposable"/> is disposed, and a <c>Dispose</c> that throws is written to one trace warning and never
/// rethrown.
/// </summary>
internal sealed class SerdeDisposal
{
    private const string DisposeFailedMessage = "Exception while closing serde {0}: {1}";

    private readonly object _key;

    private readonly object _value;

    private int _disposed;

    internal SerdeDisposal(object key, object value)
    {
        _key = key;
        _value = value;
    }

    internal void DisposeOnce()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        DisposeIfDisposable(_key);
        DisposeIfDisposable(_value);
    }

    // Only IDisposable: a serde that is only IAsyncDisposable is left alone, on every path.
    internal static void DisposeIfDisposable(object serde)
    {
        if (serde is not IDisposable disposable)
        {
            return;
        }

        try
        {
            disposable.Dispose();
        }
        catch (Exception exception)
        {
            TraceDisposeFailure(serde, exception);
        }
    }

    private static void TraceDisposeFailure(object serde, Exception exception)
    {
        string message = string.Format(
            CultureInfo.InvariantCulture, DisposeFailedMessage, SerdeText(serde), ExceptionText(exception));
        try
        {
            Trace.TraceWarning(message);
        }
        catch (Exception)
        {
            // A throwing TraceListener must not escape the client's teardown.
        }
    }

    // A ToString that throws falls back to the type's name, so the failure is still traced, as Python's logging still
    // reports a record it cannot format.
    private static string SerdeText(object serde)
    {
        try
        {
            return serde.ToString() ?? string.Empty;
        }
        catch (Exception)
        {
            return TypeName(serde);
        }
    }

    private static string ExceptionText(Exception exception)
    {
        try
        {
            return exception.ToString();
        }
        catch (Exception)
        {
            string type = TypeName(exception);
            try
            {
                return type + ": " + exception.Message;
            }
            catch (Exception)
            {
                return type;
            }
        }
    }

    private static string TypeName(object value) => value.GetType().FullName ?? value.GetType().Name;
}
