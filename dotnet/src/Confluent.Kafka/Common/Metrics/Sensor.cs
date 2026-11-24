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
using System.Globalization;

using Confluent.Kafka.Internal;

namespace Confluent.Kafka.Common.Metrics;

/// <summary>
/// A sensor applies a continuous sequence of numerical values to a set of associated metrics, Java's
/// <c>org.apache.kafka.common.metrics.Sensor</c>. For example a sensor on message size would record a sequence of
/// message sizes and would maintain a set of metrics about request sizes such as the average or max.
/// </summary>
/// <remarks>
/// The binding hands out no sensor: the clients' sensors live in the core's metrics registry, which .NET does not
/// reach, and the class has no public constructor, as Java's has none. It holds <see cref="RecordingLevel"/>, the
/// level a <see cref="MetricConfig"/> records at.
/// </remarks>
public sealed class Sensor
{
    private Sensor()
    {
    }

    /// <summary>
    /// A recording level, Java's <c>Sensor.RecordingLevel</c>. The value is Java's <c>id</c>, and <c>ToString()</c> is
    /// Java's <c>name</c>. Java's methods are in <see cref="RecordingLevelMethods"/>.
    /// </summary>
    public enum RecordingLevel : short
    {
        /// <summary>Java's <c>INFO</c>.</summary>
        INFO = 0,

        /// <summary>Java's <c>DEBUG</c>.</summary>
        DEBUG = 1,

        /// <summary>Java's <c>TRACE</c>.</summary>
        TRACE = 2,
    }
}

/// <summary>
/// The members Java declares on the <see cref="Sensor.RecordingLevel"/> enum itself, which a C# enum cannot carry. From
/// C# 14 the static ones are called on the enum (<c>Sensor.RecordingLevel.ForId(…)</c>); older C# calls them on this
/// class.
/// </summary>
public static class RecordingLevelMethods
{
    private const string JavaClass = "org.apache.kafka.common.metrics.Sensor.RecordingLevel";

    private const int MinRecordingLevelKey = 0;

    /// <summary>The largest recording level id, Java's <c>MAX_RECORDING_LEVEL_KEY</c>.</summary>
    public static readonly int MAX_RECORDING_LEVEL_KEY = (int)Sensor.RecordingLevel.TRACE;

    extension(Sensor.RecordingLevel)
    {
        /// <summary>The recording level whose id is <paramref name="id"/>, Java's <c>forId(int)</c>.</summary>
        /// <param name="id">The id.</param>
        /// <returns>The recording level.</returns>
        /// <exception cref="IllegalArgumentException">
        /// <paramref name="id"/> is outside 0 to <see cref="MAX_RECORDING_LEVEL_KEY"/>, with Java's message
        /// "Unexpected RecordLevel id `&lt;id&gt;`, it should be between `0` and `2` (inclusive)".
        /// </exception>
        public static Sensor.RecordingLevel ForId(int id) => id switch
        {
            (int)Sensor.RecordingLevel.INFO => Sensor.RecordingLevel.INFO,
            (int)Sensor.RecordingLevel.DEBUG => Sensor.RecordingLevel.DEBUG,
            (int)Sensor.RecordingLevel.TRACE => Sensor.RecordingLevel.TRACE,
            _ => throw UnexpectedId(id),
        };

        /// <summary>
        /// The recording level named <paramref name="name"/>, in any case, Java's <c>forName(String)</c>: a case
        /// insensitive lookup by name.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns>The recording level.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> is <see langword="null"/>.</exception>
        /// <exception cref="IllegalArgumentException">
        /// No recording level has the upper-cased name, with Java's message
        /// "No enum constant org.apache.kafka.common.metrics.Sensor.RecordingLevel.&lt;NAME&gt;".
        /// </exception>
        public static Sensor.RecordingLevel ForName(string name)
        {
            if (name is null)
            {
                throw new ArgumentNullException(nameof(name));
            }

            string upper = JavaText.ToUpperRoot(name);
            return upper switch
            {
                nameof(Sensor.RecordingLevel.INFO) => Sensor.RecordingLevel.INFO,
                nameof(Sensor.RecordingLevel.DEBUG) => Sensor.RecordingLevel.DEBUG,
                nameof(Sensor.RecordingLevel.TRACE) => Sensor.RecordingLevel.TRACE,
                _ => throw new IllegalArgumentException("No enum constant " + JavaClass + "." + upper),
            };
        }
    }

    extension(Sensor.RecordingLevel level)
    {
        /// <summary>
        /// Whether a sensor at this level records when the configured level's id is <paramref name="configId"/>,
        /// Java's <c>shouldRecord(int)</c>: at <c>INFO</c> only <c>INFO</c> records, at <c>DEBUG</c> <c>INFO</c> and
        /// <c>DEBUG</c> do, and at <c>TRACE</c> every level does.
        /// </summary>
        /// <param name="configId">The id of the configured recording level.</param>
        /// <returns>Whether this level records.</returns>
        /// <exception cref="IllegalStateException">
        /// <paramref name="configId"/> is no level's id, with Java's message
        /// "Did not recognize recording level &lt;configId&gt;".
        /// </exception>
        public bool ShouldRecord(int configId)
        {
            short id = (short)level;
            if (configId == (short)Sensor.RecordingLevel.INFO)
            {
                return id == (short)Sensor.RecordingLevel.INFO;
            }

            if (configId == (short)Sensor.RecordingLevel.DEBUG)
            {
                return id == (short)Sensor.RecordingLevel.INFO || id == (short)Sensor.RecordingLevel.DEBUG;
            }

            if (configId == (short)Sensor.RecordingLevel.TRACE)
            {
                return true;
            }

            throw new IllegalStateException(
                "Did not recognize recording level " + configId.ToString(CultureInfo.InvariantCulture));
        }
    }

    // A level no member has, which Java cannot hold, gets forId's message.
    internal static Sensor.RecordingLevel RequireDefined(Sensor.RecordingLevel level) =>
        Sensor.RecordingLevel.ForId((short)level);

    private static IllegalArgumentException UnexpectedId(int id) =>
        new IllegalArgumentException(
            "Unexpected RecordLevel id `" + id.ToString(CultureInfo.InvariantCulture) + "`, it should be between `"
            + MinRecordingLevelKey.ToString(CultureInfo.InvariantCulture) + "` and `"
            + MAX_RECORDING_LEVEL_KEY.ToString(CultureInfo.InvariantCulture) + "` (inclusive)");
}
