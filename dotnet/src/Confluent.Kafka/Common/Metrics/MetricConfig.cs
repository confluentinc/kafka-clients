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

using System;
using System.Collections.Generic;

namespace Confluent.Kafka.Common.Metrics;

/// <summary>
/// Configuration values for metrics, Java's <c>org.apache.kafka.common.metrics.MetricConfig</c>. Each setter returns
/// this instance, as Java's does, so calls can be chained.
/// </summary>
/// <remarks>
/// A <see cref="KafkaMetric"/> gives a copy of its configuration and copies the one it is given, so changing a
/// configuration after <see cref="KafkaMetric.Config()"/> or <see cref="KafkaMetric.Config(MetricConfig)"/> does not
/// change the metric, where Java's metric holds the object itself.
/// </remarks>
public class MetricConfig
{
    /// <summary>The number of samples a new configuration keeps, Java's <c>DEFAULT_NUM_SAMPLES</c>.</summary>
    public const int DEFAULT_NUM_SAMPLES = 2;

    private Quota? _quota;
    private int _samples;
    private long _eventWindow;
    private long _timeWindowMs;
    private IReadOnlyDictionary<string, string>? _tags;
    private Sensor.RecordingLevel _recordingLevel;

    /// <summary>
    /// Initializes a configuration with Java's defaults: no quota, <see cref="DEFAULT_NUM_SAMPLES"/> samples, an event
    /// window of <see cref="long.MaxValue"/>, a time window of 30 seconds, no tags and the <c>INFO</c> recording level.
    /// </summary>
    public MetricConfig()
    {
        _quota = null;
        _samples = DEFAULT_NUM_SAMPLES;
        _eventWindow = long.MaxValue;
        _timeWindowMs = 30000;
        _tags = new Dictionary<string, string>(StringComparer.Ordinal);
        _recordingLevel = Sensor.RecordingLevel.INFO;
    }

    // The values a native config holds, read back as they are, without the setters' checks: the core keeps a time
    // window in milliseconds, which a TimeSpan may not hold. The number of samples is at least 1, as the core's
    // set_samples panics below 1, as Java's samples(int) throws.
    internal MetricConfig(
        Quota? quota,
        int samples,
        long eventWindow,
        long timeWindowMs,
        IReadOnlyDictionary<string, string> tags,
        Sensor.RecordingLevel recordingLevel)
    {
        _quota = quota;
        _samples = samples;
        _eventWindow = eventWindow;
        _timeWindowMs = timeWindowMs;
        _tags = tags;
        _recordingLevel = recordingLevel;
    }

    /// <summary>The quota.</summary>
    /// <returns>The quota, or <see langword="null"/> for none.</returns>
    public Quota? Quota() => _quota;

    /// <summary>Sets the quota.</summary>
    /// <param name="quota">The quota, or <see langword="null"/> for none.</param>
    /// <returns>This configuration.</returns>
    public MetricConfig Quota(Quota? quota)
    {
        _quota = quota;
        return this;
    }

    /// <summary>The number of events in a sample window.</summary>
    /// <returns>The event window.</returns>
    public long EventWindow() => _eventWindow;

    /// <summary>Sets the number of events in a sample window.</summary>
    /// <param name="window">The event window.</param>
    /// <returns>This configuration.</returns>
    public MetricConfig EventWindow(long window)
    {
        _eventWindow = window;
        return this;
    }

    /// <summary>The time window, in milliseconds.</summary>
    /// <returns>The time window, in milliseconds.</returns>
    public long TimeWindowMs() => _timeWindowMs;

    /// <summary>
    /// Sets the time window, Java's <c>timeWindow(long window, TimeUnit unit)</c>: whole milliseconds, a fraction of a
    /// millisecond dropped toward zero, as Java's <c>TimeUnit.convert</c> drops it.
    /// </summary>
    /// <param name="window">The time window.</param>
    /// <returns>This configuration.</returns>
    public MetricConfig TimeWindow(TimeSpan window)
    {
        _timeWindowMs = window.Ticks / TimeSpan.TicksPerMillisecond;
        return this;
    }

    /// <summary>The tags.</summary>
    /// <returns>The tags, held by reference, or <see langword="null"/> when they were set to <see langword="null"/>.</returns>
    public IReadOnlyDictionary<string, string>? Tags() => _tags;

    /// <summary>Sets the tags.</summary>
    /// <param name="tags">The tags, held by reference, as Java holds its map; or <see langword="null"/>.</param>
    /// <returns>This configuration.</returns>
    /// <remarks>
    /// A <see langword="null"/> map, key or value is accepted here, as Java accepts it. The core cannot hold one:
    /// <see cref="KafkaMetric.Config(MetricConfig)"/> sets no tags for a <see langword="null"/> map, so the metric's
    /// configuration then has none, and throws <see cref="ArgumentNullException"/> for a <see langword="null"/> key or
    /// value. A key or value holding a NUL character or an unpaired surrogate is accepted here too, and
    /// <see cref="KafkaMetric.Config(MetricConfig)"/> throws <see cref="ArgumentException"/> for it: a NUL would cut
    /// the string, and an unpaired surrogate would come back as <c>?</c>, where Java keeps the string. A valid
    /// surrogate pair is kept.
    /// </remarks>
    public MetricConfig Tags(IReadOnlyDictionary<string, string>? tags)
    {
        _tags = tags;
        return this;
    }

    /// <summary>The number of samples.</summary>
    /// <returns>The number of samples.</returns>
    public int Samples() => _samples;

    /// <summary>Sets the number of samples.</summary>
    /// <param name="samples">The number of samples, at least 1.</param>
    /// <returns>This configuration.</returns>
    /// <exception cref="IllegalArgumentException">
    /// <paramref name="samples"/> is below 1: "The number of samples must be at least 1.".
    /// </exception>
    public MetricConfig Samples(int samples)
    {
        if (samples < 1)
        {
            throw new IllegalArgumentException("The number of samples must be at least 1.");
        }

        _samples = samples;
        return this;
    }

    /// <summary>The recording level.</summary>
    /// <returns>The recording level.</returns>
    public Sensor.RecordingLevel RecordLevel() => _recordingLevel;

    /// <summary>Sets the recording level.</summary>
    /// <param name="recordingLevel">The recording level.</param>
    /// <returns>This configuration.</returns>
    public MetricConfig RecordLevel(Sensor.RecordingLevel recordingLevel)
    {
        _recordingLevel = recordingLevel;
        return this;
    }
}
