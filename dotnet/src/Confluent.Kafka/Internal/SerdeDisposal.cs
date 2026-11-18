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
using System.Threading.Tasks;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The key and value serializers or deserializers a client was given, disposed once, key then value: each one that is
/// <see cref="IDisposable"/> is disposed, and a <c>Dispose</c> that throws is written to one trace warning and never
/// rethrown. The serdes of an asynchronous client are disposed through <see cref="IAsyncDisposable"/> when they have
/// it, and through <see cref="IDisposable"/> otherwise.
/// </summary>
internal sealed class SerdeDisposal
{
    private const string DisposeFailedMessage = "Exception while closing serde {0}: {1}";

    private readonly object _key;

    private readonly object _value;

    private readonly bool _asynchronous;

    private int _disposed;

    internal SerdeDisposal(object key, object value)
        : this(key, value, asynchronous: false)
    {
    }

    private SerdeDisposal(object key, object value, bool asynchronous)
    {
        _key = key;
        _value = value;
        _asynchronous = asynchronous;
    }

    internal static SerdeDisposal OfAsyncSerdes(object key, object value) =>
        new SerdeDisposal(key, value, asynchronous: true);

    // The blocking form, for a teardown that runs on a thread it may block: an asynchronous serde's DisposeAsync is
    // waited for here, so the serde is disposed when the client's Dispose returns, as it is when DisposeAsync does.
    internal void DisposeOnce()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        DisposeBlocking(_key);
        DisposeBlocking(_value);
    }

    internal ValueTask DisposeOnceAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return default;
        }

        if (!_asynchronous)
        {
            DisposeIfDisposable(_key);
            DisposeIfDisposable(_value);
            return default;
        }

        return DisposeBothAsync();
    }

    private async ValueTask DisposeBothAsync()
    {
        await DisposeAsync(_key).ConfigureAwait(false);
        await DisposeAsync(_value).ConfigureAwait(false);
    }

    private static async ValueTask DisposeAsync(object serde)
    {
        if (serde is not IAsyncDisposable disposable)
        {
            DisposeIfDisposable(serde);
            return;
        }

        try
        {
            await Start(disposable).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            TraceDisposeFailure(serde, exception);
        }
    }

    // A serde's DisposeAsync starts with no synchronization context and with the default task scheduler current. A
    // synchronous Dispose of the client waits for it on the calling thread, which could never run a continuation that
    // an await in the serde posted to that thread's own context or scheduler. The call stays on the calling thread, so
    // the wait needs no pool thread of its own.
    private static ValueTask Start(IAsyncDisposable disposable)
    {
        SynchronizationContext? context = SynchronizationContext.Current;
        if (context is null && TaskScheduler.Current == TaskScheduler.Default)
        {
            return disposable.DisposeAsync();
        }

        SynchronizationContext.SetSynchronizationContext(null);
        try
        {
            Task<ValueTask> start = new Task<ValueTask>(disposable.DisposeAsync);
            start.RunSynchronously(TaskScheduler.Default);
            return start.GetAwaiter().GetResult();
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(context);
        }
    }

    private void DisposeBlocking(object serde)
    {
        if (!_asynchronous || serde is not IAsyncDisposable disposable)
        {
            DisposeIfDisposable(serde);
            return;
        }

        try
        {
            ValueTask disposed = Start(disposable);
            if (disposed.IsCompleted)
            {
                disposed.GetAwaiter().GetResult();
            }
            else
            {
                disposed.AsTask().GetAwaiter().GetResult();
            }
        }
        catch (Exception exception)
        {
            TraceDisposeFailure(serde, exception);
        }
    }

    // Only IDisposable: a synchronous client leaves a serde that is only IAsyncDisposable alone, on every path.
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
