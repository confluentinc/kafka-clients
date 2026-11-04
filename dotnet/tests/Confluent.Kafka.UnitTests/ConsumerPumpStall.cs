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
using System.Reflection;
using System.Threading;

using Confluent.Kafka.Internal;

using Xunit;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// Holds a consumer's pump thread inside a posted action until disposed, and reads its queue and signal.
/// </summary>
internal sealed class ConsumerPumpStall : IDisposable
{
    private static readonly TimeSpan s_deadline = TimeSpan.FromSeconds(30);

    private readonly CompletionThread _pump;

    private readonly object _gate;

    private readonly ManualResetEventSlim _started = new ManualResetEventSlim();

    private readonly ManualResetEventSlim _release = new ManualResetEventSlim();

    internal ConsumerPumpStall(object consumer)
    {
        _pump = PumpOf(consumer);
        _gate = Field("_gate");
        _pump.Post(() =>
        {
            lock (_gate)
            {
                SetField("_signaled", false);
            }

            _started.Set();
            _release.Wait();
        });
        Assert.True(_started.Wait(s_deadline));
    }

    internal int Posted
    {
        get
        {
            lock (_gate)
            {
                return ((Queue<Action>)Field("_posted")).Count;
            }
        }
    }

    internal void WaitUntilSignaled()
    {
        Stopwatch waited = Stopwatch.StartNew();
        while (!Signaled())
        {
            Assert.True(waited.Elapsed < s_deadline, "The pump was never signaled.");
            Thread.Sleep(1);
        }
    }

    public void Dispose() => _release.Set();

    internal static void WaitForThePostedActions(object consumer)
    {
        using ManualResetEventSlim ran = new ManualResetEventSlim();
        PumpOf(consumer).Post(ran.Set);
        Assert.True(ran.Wait(s_deadline), "The pump never ran the actions posted before.");
    }

    private static CompletionThread PumpOf(object consumer)
    {
        object native = consumer.GetType()
            .GetField("_native", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(consumer)!;
        return (CompletionThread)typeof(NativeConsumer)
            .GetField("_pump", BindingFlags.NonPublic | BindingFlags.Instance)!
            .GetValue(native)!;
    }

    private bool Signaled()
    {
        lock (_gate)
        {
            return (bool)Field("_signaled");
        }
    }

    private object Field(string name) =>
        typeof(CompletionThread).GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)!.GetValue(_pump)!;

    private void SetField(string name, object value) =>
        typeof(CompletionThread).GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)!.SetValue(_pump, value);
}
