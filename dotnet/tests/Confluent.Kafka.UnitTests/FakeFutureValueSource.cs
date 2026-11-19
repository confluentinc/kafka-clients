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

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.UnitTests;

/// <summary>
/// A value source the test finishes or fails by hand, counting how often the future reads its value.
/// </summary>
internal sealed class FakeFutureValueSource<TValue> : IFutureValueSource<TValue>
{
    private volatile bool _done;

    private TValue _value = default!;

    private Exception? _error;

    private int _reads;

    public bool IsDone => _done;

    internal int Reads => Volatile.Read(ref _reads);

    public bool TryRead(out TValue value, out Exception? error)
    {
        Interlocked.Increment(ref _reads);
        if (!_done)
        {
            value = default!;
            error = null;
            return false;
        }

        value = _value;
        error = _error;
        return true;
    }

    internal void Finish(TValue value)
    {
        _value = value;
        _done = true;
    }

    internal void Fail(Exception error)
    {
        _error = error;
        _done = true;
    }
}
