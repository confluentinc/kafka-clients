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
/// <para>
/// ⚠ <b>Two of these values have NO Python counterpart, and are the M11/P3.3 admission bound</b>
/// (§7 option A): <see cref="MaxAdmittedRecords"/> and <see cref="MaxBlockMs"/>. Python needs
/// neither, because its <c>send()</c> blocks the calling OS thread and so bounds the accepted
/// population for free; .NET's <c>Send</c> returns a <see cref="System.Threading.Tasks.Task"/>
/// and the caller does not await admission, so the bound has to be explicit. They are
/// <b>deliberately not</b> a reuse of <see cref="MaxAccumulatedRecords"/> (decision D3): that one
/// bounds records <em>appended but not yet taken</em>, this one bounds records <em>accepted but
/// not yet appended-and-taken</em>, and a sibling branch measured Python's 1000 starving .NET to
/// 96.9k msg/s when the two were coupled.
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

    /// <summary>
    /// ⚠ <b>PROVISIONAL — M11/P3.3 slice S2 replaces this with a value measured on this branch</b>
    /// (PLAN §8.3). It is <em>not</em> a measured knee here, and must not be cited as one.
    /// </summary>
    /// <remarks>
    /// The number comes from a sibling branch's sweep of the <em>same shape</em> of bound over a
    /// <em>different</em> send path (M11/P7: cap 1000 → 96.9k msg/s "too tight — starves the
    /// pipeline"; cap 5000 → 591.6k msg/s, p50 7 ms, 127 MiB; cap 10000 → 635.2k msg/s but p50
    /// 13 ms). That branch sent one record per inline <c>Producer_send</c>; this one defers to a
    /// batch thread, so its knee is unknown and the transferable part of that result is the
    /// <em>shape</em> (a record count in the low thousands, an order of magnitude above Python's
    /// 1000), not the value.
    /// </remarks>
    internal const int DefaultMaxAdmittedRecords = 5000;

    /// <summary>
    /// The Kafka default for <c>max.block.ms</c> — Java's
    /// <c>ProducerConfig.MAX_BLOCK_MS_CONFIG</c> default, which is what
    /// <c>KafkaProducer.send()</c> blocks up to once the accumulator is full.
    /// </summary>
    internal const int DefaultMaxBlockMs = 60_000;

    internal const string ThresholdVariable = "CONFLUENT_KAFKA_PRODUCER_BATCH_THRESHOLD";
    internal const string MaxAccumulatedVariable = "CONFLUENT_KAFKA_PRODUCER_MAX_ACCUMULATED";
    internal const string WindowVariable = "CONFLUENT_KAFKA_PRODUCER_BATCH_WINDOW_MS";
    internal const string ChunkVariable = "CONFLUENT_KAFKA_PRODUCER_BATCH_CHUNK";

    /// <summary>
    /// The admission bound's own override — <b>separately named</b> from
    /// <see cref="MaxAccumulatedVariable"/> because the two bound different populations (D3).
    /// </summary>
    internal const string MaxAdmittedVariable = "CONFLUENT_KAFKA_PRODUCER_MAX_ADMITTED";

    /// <summary>
    /// The Java dotted config key the admission wait is bounded by. <b>A config-dict key, not an
    /// environment variable</b> — unlike every other value here — because it is a real Kafka
    /// producer config the core already knows, so reading it from the user's own config map is
    /// honouring an existing knob rather than inventing new surface.
    /// </summary>
    internal const string MaxBlockMsKey = "max.block.ms";

    /// <summary>
    /// Builds a settings value from explicit numbers — the "compose" half that
    /// <see cref="FromEnvironment"/>'s "read and validate" half feeds. Validation lives entirely in
    /// <see cref="FromEnvironment"/>, so this constructor takes the values as given (the derived
    /// <see cref="SlotCapacity"/> and the chunk clamp still apply).
    /// </summary>
    /// <remarks>
    /// The two M11/P3.3 parameters are <b>optional</b>, defaulting to the shipped values, so the
    /// ~35 existing test constructions keep expressing exactly what they express today (a test that
    /// says nothing about admission gets production's admission bound). A test that needs the bound
    /// to saturate passes <paramref name="maxAdmittedRecords"/> explicitly.
    /// </remarks>
    internal SendAccumulatorSettings(
        int slotThreshold,
        int maxAccumulatedRecords,
        int batchWindowMs,
        int batchChunk,
        int maxAdmittedRecords = DefaultMaxAdmittedRecords,
        int maxBlockMs = DefaultMaxBlockMs)
    {
        SlotThreshold = slotThreshold;
        SlotCapacity = slotThreshold + SlotCapacityHeadroom;
        MaxAccumulatedRecords = maxAccumulatedRecords;
        BatchWindowMs = batchWindowMs;
        MaxAdmittedRecords = maxAdmittedRecords;
        MaxBlockMs = maxBlockMs;

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
    /// <b>The admission bound (M11/P3.3):</b> how many records may be <em>accepted by</em>
    /// <c>Send</c> — so returned to the caller — while still waiting to be handed to the batch
    /// thread. It covers <b>both</b> submission routes (the inline append and the FIFO submission
    /// queue), because capping one container only relocates the pile-up into the other: with
    /// M11/P3.2's queue path bypassed the identical bloat reappeared in the node chain
    /// (M11/P3.3 §2.3, measured). Exceeding it makes the calling thread <b>wait</b>, bounded by
    /// <see cref="MaxBlockMs"/> — Java's own shape, since <c>KafkaProducer.send()</c> blocks up to
    /// <c>max.block.ms</c> once the accumulator is full.
    /// </summary>
    /// <remarks>
    /// Distinct from <see cref="MaxAccumulatedRecords"/> in both quantity and purpose, and the
    /// separation is decision D3 rather than an accident — see the type's remarks. Default
    /// <see cref="DefaultMaxAdmittedRecords"/> (<b>provisional</b>).
    /// </remarks>
    internal int MaxAdmittedRecords { get; }

    /// <summary>
    /// How long a <c>Send</c> blocked on the admission bound waits before failing with a
    /// <see cref="KafkaException"/> — the user's <c>max.block.ms</c>
    /// (<see cref="MaxBlockMsKey"/>), default <see cref="DefaultMaxBlockMs"/>. Zero is legal and
    /// means "never block": the fast path still admits when capacity is free, and a saturated
    /// bound fails immediately.
    /// </summary>
    internal int MaxBlockMs { get; }

    /// <summary>
    /// Builds the settings for one producer, reading each override <b>once</b>. Called from the
    /// accumulator's constructor, so a process can host producers with different settings and a
    /// test can change an override between constructions.
    /// </summary>
    /// <param name="config">
    /// The producer's own config map, or <see langword="null"/> for a producer built without one (a
    /// <c>MockProducer</c>). Only <see cref="MaxBlockMsKey"/> is read from it; every other value
    /// here comes from the environment.
    /// </param>
    internal static SendAccumulatorSettings FromEnvironment(
        IReadOnlyDictionary<string, string>? config = null)
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

        // DELIBERATELY not derived from `threshold`, unlike `maxAccumulated` above: the admission
        // bound answers a different question and coupling it to Python's 1000 is what the sibling
        // branch's sweep measured as a throughput cliff (D3). Its own constant, its own override.
        int maxAdmitted = ReadPositive(MaxAdmittedVariable, DefaultMaxAdmittedRecords);
        int maxBlockMs = ReadMaxBlockMs(config);

        return new SendAccumulatorSettings(
            threshold, maxAccumulated, window, chunk, maxAdmitted, maxBlockMs);
    }

    /// <summary>
    /// Reads <see cref="MaxBlockMsKey"/> out of the producer's config map, falling back to
    /// <see cref="DefaultMaxBlockMs"/> when it is absent, unparseable or negative.
    /// </summary>
    /// <remarks>
    /// The same ignore-rather-than-throw policy as the environment overrides above, for a stronger
    /// reason: the core reads this key too, so a value it rejects will fail producer construction
    /// there, with the core's own message — the binding must not pre-empt that with a worse one.
    /// Non-negative rather than positive: Java's <c>max.block.ms</c> is <c>atLeast(0)</c>, and zero
    /// is a meaningful "never block" rather than a typo.
    /// </remarks>
    private static int ReadMaxBlockMs(IReadOnlyDictionary<string, string>? config)
    {
        if (config is null || !config.TryGetValue(MaxBlockMsKey, out string? raw))
        {
            return DefaultMaxBlockMs;
        }

        // long, then clamp: max.block.ms is a Java `long` config, so a value above int.MaxValue is
        // legal there and must not read as a parse failure here. Clamping to int.MaxValue ms
        // (~24 days) is indistinguishable from the unbounded wait the user asked for.
        if (!long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed)
            || parsed < 0)
        {
            return DefaultMaxBlockMs;
        }

        return (int)Math.Min(parsed, int.MaxValue);
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
