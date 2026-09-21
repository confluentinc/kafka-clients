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
using System.Globalization;
using System.IO;
using System.Linq;

namespace Confluent.Kafka.Soak;

/// <summary>
/// Client-configuration routing, validation and file parsing — the port of
/// <c>soakclient.py</c>'s <c>filter_config</c> / <c>route_shared_config</c> /
/// <c>validate_config</c> / <c>parse_config_file</c>.
/// <para>
/// ⚠ <c>stringify_config</c> is deliberately NOT ported. It exists in Python because the
/// C extension raises <c>TypeError</c> on a non-string configuration value, so an int
/// <c>linger.ms</c> from a profile would abort startup. Here the parsed configuration is
/// <c>IReadOnlyDictionary&lt;string, string&gt;</c> by construction — both the file
/// parser and <c>KafkaProducer</c>/<c>KafkaConsumer</c>/<c>KafkaAdminClient</c> take
/// string values — so a coercion helper would have nothing to coerce.
/// </para>
/// <para>
/// ⚠ The JAAS helpers (<c>jaas_field</c> / <c>jaas_credentials</c> /
/// <c>librdkafka_admin_config</c>, ~85 lines plus their tests) are deliberately NOT
/// ported either (PLAN D6). They exist in Python only because its topic creation goes
/// through librdkafka's AdminClient, whose configuration namespace has no
/// <c>sasl.jaas.config</c> and errors on unknown keys. This binding's
/// <c>KafkaAdminClient</c> takes the SAME Java-shaped config the producer and consumer
/// take, so <c>sasl.jaas.config</c> flows through untouched and there is nothing to
/// translate.
/// </para>
/// </summary>
internal static class SoakConfig
{
    // -----------------------------------------------------------------------------
    // Accepted client configuration keys.
    //
    // The Rust client only *warns* on an unknown configuration key
    // (src/producer/producer_config.rs, src/consumer/consumer_config.rs, both end their
    // from_properties() match with a `warn!("Unknown ... key")` arm), so a typo would
    // silently start a soak with the default value — e.g. an unauthenticated PLAINTEXT
    // connection. A multi-day run must not begin that way, so the soak validates its own
    // configuration up front and refuses to start on an unknown key.
    //
    // Keep these in sync with the two Rust files named above.
    // -----------------------------------------------------------------------------

    /// <summary>Configuration keys the Rust producer recognises.</summary>
    internal static readonly IReadOnlyCollection<string> ProducerConfigKeys = new HashSet<string>(StringComparer.Ordinal)
    {
        "acks",
        "batch.size",
        "bootstrap.servers",
        "buffer.memory",
        "client.id",
        "compression.type",
        "connections.max.idle.ms",
        "delivery.timeout.ms",
        "enable.idempotence",
        "linger.ms",
        "max.block.ms",
        "max.in.flight.requests.per.connection",
        "max.request.size",
        "metadata.max.age.ms",
        "metadata.max.idle.ms",
        "partitioner.adaptive.partitioning.enable",
        "partitioner.availability.timeout.ms",
        "partitioner.ignore.keys",
        "receive.buffer.bytes",
        "reconnect.backoff.max.ms",
        "reconnect.backoff.ms",
        "request.timeout.ms",
        "retries",
        "retry.backoff.max.ms",
        "retry.backoff.ms",
        "sasl.jaas.config",
        "sasl.mechanism",
        "security.protocol",
        "send.buffer.bytes",
        "transaction.timeout.ms",
        "transactional.id",
    };

    /// <summary>Configuration keys the Rust consumer recognises.</summary>
    internal static readonly IReadOnlyCollection<string> ConsumerConfigKeys = new HashSet<string>(StringComparer.Ordinal)
    {
        "allow.auto.create.topics",
        "auto.commit.interval.ms",
        "auto.offset.reset",
        "bootstrap.servers",
        "check.crcs",
        "client.dns.lookup",
        "client.id",
        "client.rack",
        "config.providers",
        "connections.max.idle.ms",
        "default.api.timeout.ms",
        "enable.auto.commit",
        "enable.metrics.push",
        "exclude.internal.topics",
        "fetch.max.bytes",
        "fetch.max.wait.ms",
        "fetch.min.bytes",
        "group.id",
        "group.instance.id",
        "group.protocol",
        "group.remote.assignor",
        "heartbeat.interval.ms",
        "interceptor.classes",
        "internal.throw.on.fetch.stable.offset.unsupported",
        "isolation.level",
        "key.deserializer",
        "max.partition.fetch.bytes",
        "max.poll.interval.ms",
        "max.poll.records",
        "metadata.max.age.ms",
        "metadata.recovery.rebootstrap.trigger.ms",
        "metadata.recovery.strategy",
        "metric.reporters",
        "metrics.num.samples",
        "metrics.recording.level",
        "metrics.sample.window.ms",
        "partition.assignment.strategy",
        "receive.buffer.bytes",
        "reconnect.backoff.max.ms",
        "reconnect.backoff.ms",
        "request.timeout.ms",
        "retry.backoff.max.ms",
        "retry.backoff.ms",
        "sasl.jaas.config",
        "sasl.mechanism",
        "security.protocol",
        "security.providers",
        "send.buffer.bytes",
        "session.timeout.ms",
        "share.acknowledgement.mode",
        "share.acquire.mode",
        "socket.connection.setup.timeout.max.ms",
        "socket.connection.setup.timeout.ms",
        "value.deserializer",
    };

    /// <summary>
    /// Both configs route every <c>ssl.*</c> key to <c>apply_ssl_config_key()</c> rather
    /// than listing them individually, so the validator accepts the prefix wholesale.
    /// </summary>
    internal static readonly IReadOnlyList<string> AcceptedConfigPrefixes = new[] { "ssl." };

    /// <summary>
    /// Routes a flat config map to one client: drops every key starting with one of
    /// <paramref name="filterOut"/>, and strips <paramref name="stripPrefix"/> from the
    /// keys that carry it.
    /// </summary>
    internal static Dictionary<string, string> FilterConfig(
        IReadOnlyDictionary<string, string> conf,
        IReadOnlyCollection<string> filterOut,
        string stripPrefix)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, string> entry in conf)
        {
            if (filterOut.Any(prefix => entry.Key.StartsWith(prefix, StringComparison.Ordinal)))
            {
                continue;
            }

            string key = entry.Key.StartsWith(stripPrefix, StringComparison.Ordinal)
                ? entry.Key.Substring(stripPrefix.Length)
                : entry.Key;
            result[key] = entry.Value;
        }

        return result;
    }

    /// <summary>
    /// Drops unprefixed keys that belong to the <i>other</i> client.
    /// <para>
    /// A single shared config file naturally carries <c>group.id</c> (consumer-only)
    /// alongside <c>linger.ms</c> (producer-only). The reference soak hands both to every
    /// client and relies on librdkafka ignoring what it does not want; validating
    /// strictly (see <see cref="ValidateConfig"/>) would instead reject them. So a key
    /// unknown to this client but known to the other one is routed away rather than
    /// rejected — anything unknown to <b>both</b> still fails startup.
    /// </para>
    /// </summary>
    /// <returns>The kept configuration, and the sorted names routed away.</returns>
    internal static (Dictionary<string, string> Kept, IReadOnlyList<string> Routed) RouteSharedConfig(
        IReadOnlyDictionary<string, string> conf,
        IReadOnlyCollection<string> ownKeys,
        IReadOnlyCollection<string> otherKeys)
    {
        var routed = conf.Keys
            .Where(key => !ownKeys.Contains(key) && otherKeys.Contains(key))
            .OrderBy(key => key, StringComparer.Ordinal)
            .ToList();

        var kept = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, string> entry in conf)
        {
            if (!routed.Contains(entry.Key))
            {
                kept[entry.Key] = entry.Value;
            }
        }

        return (kept, routed);
    }

    /// <summary>
    /// Throws <see cref="ArgumentException"/> naming every key the Rust client would
    /// ignore. See the comment on <see cref="ProducerConfigKeys"/> for why silent
    /// acceptance is not survivable on a multi-day run.
    /// </summary>
    internal static void ValidateConfig(
        IReadOnlyDictionary<string, string> conf,
        IReadOnlyCollection<string> accepted,
        string what)
    {
        var unknown = conf.Keys
            .Where(key => !accepted.Contains(key)
                && !AcceptedConfigPrefixes.Any(prefix => key.StartsWith(prefix, StringComparison.Ordinal)))
            .OrderBy(key => key, StringComparer.Ordinal)
            .ToList();

        if (unknown.Count == 0)
        {
            return;
        }

        throw new ArgumentException(string.Format(
            CultureInfo.InvariantCulture,
            "unknown {0} configuration key(s): {1}. The Rust client only logs a warning "
            + "for unrecognised keys, so this would silently run with the default value. "
            + "Fix the key, or prefix it for another client (producer./consumer./admin.). "
            + "Accepted {2} keys: {3}",
            what,
            string.Join(", ", unknown),
            what,
            string.Join(", ", accepted.OrderBy(key => key, StringComparer.Ordinal))));
    }

    /// <summary>Parses a <c>key=value</c> client configuration file.</summary>
    /// <exception cref="ArgumentException">A non-comment, non-blank line has no <c>=</c>.</exception>
    internal static Dictionary<string, string> ParseConfigFile(TextReader reader)
    {
        var conf = new Dictionary<string, string>(StringComparer.Ordinal);
        string? line;
        while ((line = reader.ReadLine()) is not null)
        {
            line = line.Trim();
            if (line.Length == 0 || line[0] == '#')
            {
                continue;
            }

            // Everything after the FIRST '=' is the value, so a JAAS string containing
            // '=' needs no escaping.
            int separator = line.IndexOf('=');
            if (separator <= 0)
            {
                throw new ArgumentException(string.Format(
                    CultureInfo.InvariantCulture,
                    "Configuration lines must be `name=value..`, not {0}",
                    line));
            }

            conf[line.Substring(0, separator)] = line.Substring(separator + 1);
        }

        return conf;
    }
}
