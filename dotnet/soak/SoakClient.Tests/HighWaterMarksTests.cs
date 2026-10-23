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

using Xunit;

namespace Confluent.Kafka.Soak.Tests;

/// <summary>The duplicate / gap accounting — the part of the soak a two-week run is judged by.</summary>
public sealed class HighWaterMarksTests
{
    private const string Key = "t-0";

    [Theory]
    // (description, prior observations, next offset, expected duplicates, expected missed)
    [InlineData("first message at offset 0", new long[0], 0L, 0L, 0L)]
    // The first offset seen only establishes the mark: consumption starts at an arbitrary
    // committed position, which is not a gap.
    [InlineData("first message mid-partition", new long[0], 5L, 0L, 0L)]
    [InlineData("in-order", new long[] { 5 }, 6L, 0L, 0L)]
    [InlineData("in-order after a long run", new long[] { 1, 2, 3, 4 }, 5L, 0L, 0L)]
    [InlineData("duplicate of the mark itself", new long[] { 10 }, 10L, 1L, 0L)]
    [InlineData("replay of three", new long[] { 10 }, 8L, 3L, 0L)]
    [InlineData("gap of one", new long[] { 10 }, 12L, 0L, 1L)]
    [InlineData("gap of four", new long[] { 10 }, 15L, 0L, 4L)]
    public void Accounting(string description, long[] marks, long offset, long expectedDuplicates, long expectedMissed)
    {
        var hwmarks = new HighWaterMarks();
        foreach (long mark in marks)
        {
            hwmarks.Observe(Key, mark);
        }

        (long duplicates, long missed) = hwmarks.Observe(Key, offset);

        Assert.Equal(expectedDuplicates, duplicates);
        Assert.Equal(expectedMissed, missed);
        Assert.NotEmpty(description);
    }

    /// <summary>
    /// Documents a defect inherited from the reference soak — it does NOT bless it.
    /// <para>
    /// The marks map defaults to <c>0</c>, so "never seen" and "last seen at offset 0" are
    /// the same state, and the <c>hw &gt; 0</c> guard therefore skips the check for exactly
    /// one transition per partition. A duplicate of offset 0, or a gap immediately after
    /// it, is not counted.
    /// </para>
    /// <para>
    /// This is a faithful port and is kept for fidelity; the blast radius is ~2 records,
    /// at the start of the first run against a fresh topic only (every later restart
    /// begins at a committed non-zero offset). The correct fix is a <c>-1</c> sentinel.
    /// <b>If someone changes the sentinel, this test SHOULD fail — that is the point of
    /// its name.</b>
    /// </para>
    /// </summary>
    [Theory]
    [InlineData("a real duplicate of offset 0 is missed", 0L, 0L)]
    [InlineData("a real gap after offset 0 is missed", 0L, 7L)]
    public void OffsetZeroBlindSpotIsAKnownLimitation(string description, long firstMark, long offset)
    {
        var hwmarks = new HighWaterMarks();
        hwmarks.Observe(Key, firstMark);

        (long duplicates, long missed) = hwmarks.Observe(Key, offset);

        Assert.Equal(0, duplicates);
        Assert.Equal(0, missed);
        Assert.NotEmpty(description);
    }

    [Fact]
    public void MarksArePerPartition()
    {
        var hwmarks = new HighWaterMarks();
        hwmarks.Observe("t-0", 100);
        hwmarks.Observe("t-1", 5);

        // A low offset on a different partition is not a duplicate.
        Assert.Equal((0L, 0L), hwmarks.Observe("t-1", 6));
        Assert.Equal((0L, 0L), hwmarks.Observe("t-0", 101));
        Assert.Equal(2, hwmarks.Count);
    }

    /// <summary>
    /// A rebalance replay must report the replayed records exactly once: the mark is set
    /// unconditionally (not advance-only), so the single jump back accounts for the whole
    /// replay and the records that follow are in order.
    /// </summary>
    [Fact]
    public void ReplayCountsEachRecordOnce()
    {
        var hwmarks = new HighWaterMarks();
        for (long offset = 0; offset <= 200; offset++)
        {
            hwmarks.Observe(Key, offset);
        }

        (long duplicates, long missed) = hwmarks.Observe(Key, 100);
        Assert.Equal(101, duplicates);
        Assert.Equal(0, missed);

        long totalExtra = 0;
        for (long offset = 101; offset <= 200; offset++)
        {
            (long dup, long miss) = hwmarks.Observe(Key, offset);
            totalExtra += dup + miss;
        }

        Assert.Equal(0, totalExtra);
        Assert.Equal((0L, 0L), hwmarks.Observe(Key, 201));
    }

    [Fact]
    public void GapThenRecovery()
    {
        var hwmarks = new HighWaterMarks();
        hwmarks.Observe(Key, 1);
        Assert.Equal((0L, 8L), hwmarks.Observe(Key, 10));
        Assert.Equal((0L, 0L), hwmarks.Observe(Key, 11));
    }

    /// <summary>
    /// The counters the consume loop forwards must be the REAL counts, not a flat 1 — a
    /// dashboard built on <c>consumer.msgdup</c> / <c>consumer.missedmsg</c> alone would
    /// otherwise read as far fewer than actually occurred. The loop passes these values
    /// through unmodified, so pinning them here pins the counter.
    /// </summary>
    [Fact]
    public void ObserveReportsTheActualCounts()
    {
        var hwmarks = new HighWaterMarks();
        hwmarks.Observe(Key, 10);
        Assert.Equal((3L, 0L), hwmarks.Observe(Key, 8));

        var second = new HighWaterMarks();
        second.Observe(Key, 1);
        Assert.Equal((0L, 8L), second.Observe(Key, 10));
    }

    [Fact]
    public void MarksSnapshotIsACopy()
    {
        var hwmarks = new HighWaterMarks();
        hwmarks.Observe(Key, 7);

        System.Collections.Generic.IReadOnlyDictionary<string, long> snapshot = hwmarks.Marks();
        hwmarks.Observe(Key, 8);

        Assert.Equal(7, snapshot[Key]);
        Assert.Equal(8, hwmarks.Marks()[Key]);
    }
}
