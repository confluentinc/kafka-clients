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

using Confluent.Kafka.Common;
using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Runs one call that may block on a dedicated thread, not the pool, and joins it with a deadline, so a hang fails the
/// test instead of hanging the run.
/// </summary>
internal sealed class BlockingCall
{
    internal static readonly TimeSpan Deadline = TimeSpan.FromSeconds(30);

    internal static readonly TimeSpan StillBlocked = TimeSpan.FromMilliseconds(100);

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

    internal int CallingThreadId => _callingThreadId;

    internal static BlockingCall StartGet(Func<object?> call) => new BlockingCall(call);

    internal static BlockingCall StartWait(Action call) => new BlockingCall(() =>
    {
        call();
        return Returned;
    });

    internal static object? RunGet(Func<object?> call) => StartGet(call).Join();

    internal static object? RunWait(Action call) => StartWait(call).Join();

    internal static KafkaException AssertThrew(object? outcome, int code, string message)
    {
        KafkaException failure = Assert.IsType<KafkaException>(outcome);
        Assert.Equal(code, failure.Code);
        Assert.Equal(message, failure.Message);
        return failure;
    }

    internal void AssertBlocked()
    {
        Assert.True(
            SpinWait.SpinUntil(
                () => !_thread.IsAlive || (_thread.ThreadState & ThreadState.WaitSleepJoin) != 0,
                Deadline),
            "The call neither blocked nor returned.");
        Assert.False(_thread.Join(StillBlocked), "The call returned before what it waits on had completed.");
    }

    internal object? Join()
    {
        Assert.True(_thread.Join(Deadline), $"The call did not return within {Deadline}.");
        return _outcome;
    }
}
