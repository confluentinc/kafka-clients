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

using Confluent.Kafka.ShareConsumer.Internal;

using Xunit;

namespace Confluent.Kafka.ShareConsumer.UnitTests;

/// <summary>
/// The one-operation-in-flight access guard (ffi-marshalling.md §B5). This
/// mirror of the core guard is a pure managed component, so its rejection-type
/// matrix is tested here <b>deterministically</b> — a concurrent <b>async op</b>
/// throws <see cref="KafkaException"/> (Java <c>ConcurrentModificationException</c>
/// semantics), while a concurrent <b>sync state read</b> throws
/// <see cref="InvalidOperationException"/> — for every combination. (Forcing a
/// genuine op overlap on the near-instant Mock ops is non-deterministic, so the
/// exception-type contract is proven at this component level; the wiring into
/// <c>NativeConsumer</c> is exercised by the operation / group-metadata tests.)
/// </summary>
public sealed class ConsumerAccessGuardTests
{
    [Fact]
    public void ConcurrentAsyncOp_WhileOpHeld_ThrowsKafkaException()
    {
        ConsumerAccessGuard guard = new ConsumerAccessGuard();
        guard.EnterOperation();

        KafkaException ex = Assert.Throws<KafkaException>(() => guard.EnterOperation());
        Assert.Contains("multi-threaded access", ex.Message, StringComparison.Ordinal);

        guard.Release();
    }

    [Fact]
    public void StateRead_WhileOpHeld_ThrowsInvalidOperation()
    {
        ConsumerAccessGuard guard = new ConsumerAccessGuard();
        guard.EnterOperation();

        InvalidOperationException ex =
            Assert.Throws<InvalidOperationException>(() => guard.EnterStateRead());
        Assert.Contains("multi-threaded access", ex.Message, StringComparison.Ordinal);

        guard.Release();
    }

    [Fact]
    public void AsyncOp_WhileStateReadHeld_ThrowsKafkaException()
    {
        ConsumerAccessGuard guard = new ConsumerAccessGuard();
        guard.EnterStateRead();

        Assert.Throws<KafkaException>(() => guard.EnterOperation());

        guard.Release();
    }

    [Fact]
    public void ConcurrentStateRead_WhileStateReadHeld_ThrowsInvalidOperation()
    {
        ConsumerAccessGuard guard = new ConsumerAccessGuard();
        guard.EnterStateRead();

        Assert.Throws<InvalidOperationException>(() => guard.EnterStateRead());

        guard.Release();
    }

    [Fact]
    public void Release_MakesGuardReusable()
    {
        ConsumerAccessGuard guard = new ConsumerAccessGuard();

        guard.EnterOperation();
        guard.Release();

        // Reusable after release — a fresh op and a fresh state read both succeed.
        guard.EnterStateRead();
        guard.Release();

        guard.EnterOperation();
        guard.Release();
    }
}
