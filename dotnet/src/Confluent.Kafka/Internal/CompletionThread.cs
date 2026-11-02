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
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;

namespace Confluent.Kafka.Internal;

/// <summary>
/// A client's drain thread: woken by the client's notify hook, it calls the client's <c>__execute_callbacks</c> until
/// a call runs nothing, runs the actions posted to it in order after each call, and runs a teardown requested from a
/// callback once the drain has returned, then ends.
/// </summary>
internal sealed class CompletionThread : IDisposable
{
    /// <summary>The notify hook the core calls each time the client's callback queue turns non-empty.</summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void NotifyCallback(IntPtr opaque);

    internal static readonly NotifyCallback NotifyHook = OnNotify;

    private readonly Func<int> _drain;

    private readonly Thread _thread;

    private readonly IntPtr _opaque;

    private readonly object _gate = new object();

    private readonly Queue<Action> _posted = new Queue<Action>();

    private bool _signaled;

    private bool _stopping;

    private bool _stopped;

    private bool _draining;

    private Action? _teardown;

    private int _opaqueReleased;

    internal CompletionThread(string name, Func<int> drain)
    {
        _drain = drain;
        _opaque = GCHandle.ToIntPtr(GCHandle.Alloc(this));
        _thread = new Thread(Run) { IsBackground = true, Name = name };
        _thread.Start();
    }

    internal IntPtr NotifyOpaque => _opaque;

    internal bool IsCurrentThread => Thread.CurrentThread == _thread;

    internal bool IsDraining => IsCurrentThread && _draining;

    internal void Post(Action action)
    {
        lock (_gate)
        {
            if (!_stopped)
            {
                _posted.Enqueue(action);
                _signaled = true;
                Monitor.Pulse(_gate);
                return;
            }
        }

        RunGuarded(action, "a posted action");
    }

    internal void PostAfterDrain(Action action) =>
        Post(() =>
        {
            Drain();
            action();
        });

    internal void RequestTeardown(Action teardown)
    {
        lock (_gate)
        {
            _teardown ??= teardown;
            Monitor.Pulse(_gate);
        }
    }

    internal void ReleaseNotifyOpaque()
    {
        if (Interlocked.Exchange(ref _opaqueReleased, 1) == 0)
        {
            GCHandle.FromIntPtr(_opaque).Free();
        }
    }

    public void Dispose()
    {
        lock (_gate)
        {
            _stopping = true;
            Monitor.Pulse(_gate);
        }

        if (!IsCurrentThread)
        {
            _thread.Join();
        }
    }

    private static void OnNotify(IntPtr opaque)
    {
        try
        {
            ((CompletionThread)GCHandle.FromIntPtr(opaque).Target!).Signal();
        }
        catch (Exception)
        {
            // Never unwind into native.
        }
    }

    private void Signal()
    {
        lock (_gate)
        {
            _signaled = true;
            Monitor.Pulse(_gate);
        }
    }

    private void Run()
    {
        while (WaitForWork())
        {
            int ran;
            do
            {
                ran = Drain();
                RunPosted();
            }
            while (ran > 0 && !StopOrTeardownRequested());
        }

        Action? teardown;
        lock (_gate)
        {
            teardown = _teardown;
        }

        if (teardown != null)
        {
            RunGuarded(teardown, "the teardown");
        }

        FinishPosted();
    }

    private bool WaitForWork()
    {
        lock (_gate)
        {
            while (!_signaled && !_stopping && _teardown == null)
            {
                Monitor.Wait(_gate);
            }

            _signaled = false;
            return !_stopping && _teardown == null;
        }
    }

    private bool StopOrTeardownRequested()
    {
        lock (_gate)
        {
            return _stopping || _teardown != null;
        }
    }

    // A drain that throws counts as one that ran nothing: the core has put back what it left unrun, and the hook has
    // fired for it.
    private int Drain()
    {
        _draining = true;
        try
        {
            return _drain();
        }
        catch (Exception exception)
        {
            TraceSwallowed("the callbacks", exception);
            return 0;
        }
        finally
        {
            _draining = false;
        }
    }

    private void RunPosted()
    {
        Action? action;
        while ((action = TakePosted()) != null)
        {
            RunGuarded(action, "a posted action");
        }
    }

    private Action? TakePosted()
    {
        lock (_gate)
        {
            return _posted.Count == 0 ? null : _posted.Dequeue();
        }
    }

    private void FinishPosted()
    {
        while (true)
        {
            Action action;
            lock (_gate)
            {
                if (_posted.Count == 0)
                {
                    _stopped = true;
                    return;
                }

                action = _posted.Dequeue();
            }

            RunGuarded(action, "a posted action");
        }
    }

    // What throws is logged and the thread goes on, as Python's admin pump and event loop do.
    private void RunGuarded(Action action, string what)
    {
        try
        {
            action();
        }
        catch (Exception exception)
        {
            TraceSwallowed(what, exception);
        }
    }

    private void TraceSwallowed(string what, Exception exception)
    {
        try
        {
            Trace.TraceError(
                "Confluent.Kafka: error running {0} of '{1}'; the exception was swallowed. Details: {2}",
                what,
                _thread.Name,
                exception);
        }
        catch (Exception)
        {
            // A throwing TraceListener must not end the thread.
        }
    }
}
