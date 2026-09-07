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
using System.Globalization;

namespace Confluent.Kafka.Internal;

/// <summary>
/// The async send accumulator's tuning constants (M11/P3.1 §3.2). <b>Every value with a Python
/// counterpart takes Python's value, and keeps Python's name</b>
/// (<c>bindings/python/_confluentkafka.c:19-27</c> and the bare <c>10 ms</c> literal in
/// <c>Producer_send_thread</c>), so the two bindings can be diffed line for line. Read <b>once, at
/// construction</b>, from environment variables that exist only as an escape hatch — the defaults
/// are the shipped behavior.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why environment variables and not config keys.</b> There is no <c>ProducerConfig</c> type in
/// this binding — config is a <c>KeyValuePair&lt;string,string&gt;</c> dictionary consumed by the
/// Rust core — so inventing a config-dict key the core does not know would be new public surface
/// for a knob that exists only to make a future performance result actionable. Environment
/// variables avoid that, following the recorded precedent of
/// <c>CONFLUENT_KAFKA_PRODUCER_MAX_INFLIGHT_SENDS</c> ("read once at construction; not a Kafka
/// config-dict key").
/// </para>
/// <para>
/// <b>The 10 ms window is an accepted cost, not an oversight (§3.3).</b> The batch thread's timer is
/// <b>free-running</b> — the deadline is taken from the loop's own clock at the top of each
/// iteration, unrelated to when a record arrived — so a sub-threshold batch waits
/// <b>0–<see cref="BatchWindowMs"/> ms uniformly</b>, mean ~half the window, not the full window.
/// That is Python's shape and it is kept deliberately; a test asserting stage-1 timing must
/// therefore assert a <em>bound</em>, never an expected value. A "first record starts the timer"
/// variant would change the mechanism as well as the magnitude and is explicitly out of scope.
/// </para>
/// <para>
/// <b>An unparseable or out-of-range override is ignored</b> rather than throwing: these are
/// operator escape hatches read during producer construction, and failing to build a producer
/// because of a typo in an optional environment variable would be a worse outcome than running
/// with the Python-parity default.
/// </para>
/// </remarks>
internal readonly struct SendAccumulatorSettings
{
    /// <summary><c>PRODUCER_RECORD_SLOT_THRESHOLD</c> (<c>_confluentkafka.c:19</c>).</summary>
    internal const int DefaultSlotThreshold = 1000;

    /// <summary>
    /// The gap Python adds on top of the threshold to size a node:
    /// <c>PRODUCER_RECORD_SLOT_CAPACITY (PRODUCER_RECORD_SLOT_THRESHOLD + 100)</c>
    /// (<c>_confluentkafka.c:20</c>). Kept as the same derivation, not as a second literal.
    /// </summary>
    internal const int SlotCapacityHeadroom = 100;

    /// <summary>The bare <c>10 ms</c> literal in Python's send-thread wait loop.</summary>
    internal const int DefaultBatchWindowMs = 10;

    internal const string ThresholdVariable = "CONFLUENT_KAFKA_PRODUCER_BATCH_THRESHOLD";
    internal const string MaxAccumulatedVariable = "CONFLUENT_KAFKA_PRODUCER_MAX_ACCUMULATED";
    internal const string WindowVariable = "CONFLUENT_KAFKA_PRODUCER_BATCH_WINDOW_MS";
    internal const string ChunkVariable = "CONFLUENT_KAFKA_PRODUCER_BATCH_CHUNK";

    /// <summary>
    /// Builds a settings value from explicit numbers — the "compose" half that
    /// <see cref="FromEnvironment"/>'s "read and validate" half feeds. Validation lives entirely in
    /// <see cref="FromEnvironment"/>, so this constructor takes the values as given (the derived
    /// <see cref="SlotCapacity"/> and the chunk clamp still apply).
    /// </summary>
    internal SendAccumulatorSettings(int slotThreshold, int maxAccumulatedRecords, int batchWindowMs, int batchChunk)
    {
        SlotThreshold = slotThreshold;
        SlotCapacity = slotThreshold + SlotCapacityHeadroom;
        MaxAccumulatedRecords = maxAccumulatedRecords;
        BatchWindowMs = batchWindowMs;

        // A chunk larger than a node is indistinguishable from a full node, because a chunk never
        // spans two nodes (§3.4). Clamping keeps the ceil(count / chunk) formula honest instead of
        // letting an over-large override read as a different mode.
        BatchChunk = Math.Min(batchChunk, SlotCapacity);
    }

    /// <summary>
    /// The record count at which an appending <c>Send</c> wakes the batch thread early rather than
    /// letting the window run out (Python <c>PRODUCER_RECORD_SLOT_THRESHOLD</c>, 1000).
    /// </summary>
    internal int SlotThreshold { get; }

    /// <summary>
    /// A node's fixed capacity — <see cref="SlotThreshold"/> + <see cref="SlotCapacityHeadroom"/>
    /// (Python <c>PRODUCER_RECORD_SLOT_CAPACITY</c>, 1100). A node never holds more, so this is
    /// also the largest batch a single <c>send_batch</c> can carry.
    /// </summary>
    internal int SlotCapacity { get; }

    /// <summary>
    /// The backpressure bound: records appended but not yet taken by the batch thread (Python
    /// <c>PRODUCER_MAX_ACCUMULATED_RECORDS</c>, which is <em>defined as</em> the threshold). Python's
    /// own comment is the Java-faithfulness argument for having a stage-1 bound at all: <i>"once this
    /// many records are accumulated but not yet taken by the send task, the producer is 'full' and
    /// further enqueuing should wait until the send task drains a batch. One complete batch beyond
    /// the one being filled — mirrors Java's <c>send()</c> blocking once <c>buffer.memory</c> is
    /// full, applied here at batch granularity in front of the Rust accumulator."</i>
    /// </summary>
    internal int MaxAccumulatedRecords { get; }

    /// <summary>The free-running linger window in milliseconds (Python's bare 10 ms literal).</summary>
    internal int BatchWindowMs { get; }

    /// <summary>
    /// The maximum number of records in one <c>send_batch</c> call. Defaults to
    /// <see cref="SlotCapacity"/> — Python's <b>effective</b> per-call maximum, since it issues one
    /// call per node and a node fills to exactly <c>SLOT_CAPACITY</c>. Python has no name for it;
    /// this is the phase's only net-new constant name, and its default value is Python's, so at the
    /// defaults it changes nothing (<c>ceil(count / chunk)</c> is always 1). It exists as the single
    /// knob that trades batching efficiency against how long one <c>send_batch</c> holds the core's
    /// coarse producer mutex (§3.4).
    /// </summary>
    internal int BatchChunk { get; }

    /// <summary>
    /// Builds the settings for one producer, reading each override <b>once</b>. Called from the
    /// accumulator's constructor, so a process can host producers with different settings and a
    /// test can change an override between constructions.
    /// </summary>
    internal static SendAccumulatorSettings FromEnvironment()
    {
        int threshold = ReadPositive(ThresholdVariable, DefaultSlotThreshold);

        // Python couples the bound to the threshold (PRODUCER_MAX_ACCUMULATED_RECORDS is *defined*
        // as PRODUCER_RECORD_SLOT_THRESHOLD), so the default follows the effective threshold rather
        // than the constant — otherwise lowering only the threshold would silently decouple them.
        int maxAccumulated = ReadPositive(MaxAccumulatedVariable, threshold);
        // ReadPositive, not ReadNonNegative: a zero window would make the batch thread's wait loop
        // expire instantly on every iteration, i.e. a spin loop burning a core while idle. Python's
        // window is a compile-time 10 ms and has no zero form to be faithful to.
        int window = ReadPositive(WindowVariable, DefaultBatchWindowMs);
        int chunk = ReadPositive(ChunkVariable, threshold + SlotCapacityHeadroom);

        return new SendAccumulatorSettings(threshold, maxAccumulated, window, chunk);
    }

    private static int ReadPositive(string variable, int fallback)
    {
        int value = Read(variable, fallback);
        return value >= 1 ? value : fallback;
    }

    private static int Read(string variable, int fallback)
    {
        string? raw;
        try
        {
            raw = Environment.GetEnvironmentVariable(variable);
        }
        catch (System.Security.SecurityException)
        {
            // A restricted host can deny environment access; fall back rather than fail the
            // producer's construction over an optional knob.
            return fallback;
        }

        return int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsed)
            ? parsed
            : fallback;
    }
}
