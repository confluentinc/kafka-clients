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
using System.Threading;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// One call that may block — a sync future's <c>Get()</c> or <c>Wait()</c> — run on a <b>dedicated</b>
/// <see cref="Thread"/> and joined with a deadline, so a hang fails the test instead of hanging the run (M15/P14
/// PLAN §7.1, "bounded waits"). A thread-pool thread is deliberately avoided: the call blocks by design, and the futures
/// under test must not need the pool. The call's outcome is its return value, <see cref="Returned"/> for a
/// <see langword="void"/> call that returned, or the exception it threw.
/// </summary>
internal sealed class BlockingCall
{
    /// <summary>How long a call may take to return once it can.</summary>
    internal static readonly TimeSpan Deadline = TimeSpan.FromSeconds(30);

    /// <summary>How long a blocked call must stay blocked to count as "still waiting".</summary>
    internal static readonly TimeSpan StillBlocked = TimeSpan.FromMilliseconds(100);

    /// <summary>The outcome of a <see langword="void"/> call that returned normally.</summary>
    internal static readonly object Returned = new object();

    private readonly Thread _thread;
    private object? _outcome;
    private int _callingThreadId;

    private BlockingCall(Func<object?> call)
    {
        _thread = new Thread(() =>
        {
            _callingThreadId = Environment.CurrentManagedThreadId;
            try
            {
                _outcome = call();
            }
            catch (Exception exception)
            {
                _outcome = exception;
            }
        })
        {
            IsBackground = true,
        };
        _thread.Start();
    }

    /// <summary>The managed id of the dedicated thread the call runs on (valid once <see cref="Join"/> returned).</summary>
    internal int CallingThreadId => _callingThreadId;

    /// <summary>Starts a value-returning call on a dedicated thread.</summary>
    internal static BlockingCall StartGet(Func<object?> call) => new BlockingCall(call);

    /// <summary>Starts a <see langword="void"/> call on a dedicated thread.</summary>
    internal static BlockingCall StartWait(Action call) => new BlockingCall(() =>
    {
        call();
        return Returned;
    });

    /// <summary>Runs a value-returning call on a dedicated thread and returns its outcome.</summary>
    internal static object? RunGet(Func<object?> call) => StartGet(call).Join();

    /// <summary>Runs a <see langword="void"/> call on a dedicated thread and returns its outcome.</summary>
    internal static object? RunWait(Action call) => StartWait(call).Join();

    /// <summary>
    /// Asserts the call has parked (its thread is in a wait) and is still running <see cref="StillBlocked"/> later:
    /// it is waiting for something, not merely slow to start.
    /// </summary>
    internal void AssertBlocked()
    {
        // Stop spinning as soon as the call parks OR returns: a call that returned is exactly the failure this asserts
        // against, and the Join below reports it at once instead of after the whole deadline.
        Assert.True(
            SpinWait.SpinUntil(
                () => !_thread.IsAlive || (_thread.ThreadState & ThreadState.WaitSleepJoin) != 0,
                Deadline),
            "The call neither blocked nor returned.");
        Assert.False(_thread.Join(StillBlocked), "The call returned before what it waits on had completed.");
    }

    /// <summary>Waits for the call to return, within <see cref="Deadline"/>, and returns its outcome.</summary>
    internal object? Join()
    {
        Assert.True(_thread.Join(Deadline), $"The call did not return within {Deadline}.");
        return _outcome;
    }
}
