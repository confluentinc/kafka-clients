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
/// ⚠ <c>librdkafka_admin_config</c> (the config <i>translator</i>) is deliberately NOT
/// ported (PLAN D6). It exists in Python only because its topic creation used to go
/// through librdkafka's AdminClient, whose configuration namespace had no
/// <c>sasl.jaas.config</c> and errored on unknown keys. This binding's
/// <c>KafkaAdminClient</c> — like the Python soak's admin client today — takes the SAME
/// Java-shaped config the producer and consumer take, so <c>sasl.jaas.config</c> flows
/// through untouched and there is nothing to translate.
/// </para>
/// <para>
/// <c>JaasField</c> / <c>JaasCredentials</c> / <c>CheckAdminCredentials</c>, by
/// contrast, ARE ported: they don't translate anything, they <i>validate</i> that a
/// SASL-configured admin config actually carries usable credentials — the same
/// fail-fast the Python soak added once it moved onto the Rust-backed admin client,
/// which silently defaults to PLAINTEXT on a protocol/credential mismatch rather than
/// rejecting it.
/// </para>
/// </summary>
internal static class SoakConfig
{
    // -----------------------------------------------------------------------------
    // Accepted client configuration keys.
    //
    // The Rust client only *warns* on an unknown configuration key
    // (src/producer/producer_config.rs, src/consumer/consumer_config.rs, both end their
    // new() match with a `warn!("Unknown ... key")` arm), so a typo would
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

    /// <summary>
    /// Extracts one field's value from a Java JAAS login-module string.
    /// <para>
    /// Accepts the spacing and quoting variants a JAAS string legally carries:
    /// <c>username="k"</c>, <c>username = "k"</c>, <c>username='k'</c> and bare
    /// <c>username=k</c>. Returns <c>null</c> only when the field is genuinely absent.
    /// </para>
    /// <para>
    /// This mirrors the Rust parser (<c>SaslConfig::parse_jaas_option</c>) byte for byte
    /// so the client and this validator never disagree: the key is recognized only at
    /// an <b>option start</b> (beginning of string or after whitespace), quoted regions
    /// are skipped wholesale <b>honoring backslash escapes</b>, and the value may be
    /// double-quoted, single-quoted, or bare. A <c>name=</c> sequence living inside
    /// another option's quoted value is therefore never mistaken for the option (e.g.
    /// <c>password="username=x" username="right"</c> resolves <c>username</c> to
    /// <c>right</c>, not <c>x</c>) — a naive scan could make that mistake and let a
    /// missing-username config pass the fast-fail. The returned value is the raw inner
    /// content between the quotes (escapes are honored for boundary detection but not
    /// expanded), matching the Rust parser.
    /// </para>
    /// </summary>
    internal static string? JaasField(string jaasConfig, string name)
    {
        string s = jaasConfig;
        int n = s.Length;

        int idx = 0;
        while (idx < n)
        {
            char c = s[idx];
            if (c == '"' || c == '\'')
            {
                idx = SkipQuoted(s, idx);
                continue;
            }

            bool atOptionStart = idx == 0 || char.IsWhiteSpace(s[idx - 1]);
            if (atOptionStart && StartsWithAt(s, idx, name))
            {
                int j = idx + name.Length;
                while (j < n && char.IsWhiteSpace(s[j]))
                {
                    j += 1;
                }

                if (j < n && s[j] == '=')
                {
                    j += 1;
                    while (j < n && char.IsWhiteSpace(s[j]))
                    {
                        j += 1;
                    }

                    if (j >= n)
                    {
                        return null;
                    }

                    char quote = s[j];
                    if (quote == '"' || quote == '\'')
                    {
                        int start = j + 1;
                        int k = start;
                        while (k < n)
                        {
                            if (s[k] == '\\')
                            {
                                k += 2;
                                continue;
                            }

                            if (s[k] == quote)
                            {
                                return s.Substring(start, k - start);
                            }

                            k += 1;
                        }

                        return null; // unterminated quote — malformed
                    }

                    // Bare value: read until whitespace or ';'.
                    int bareEnd = j;
                    while (bareEnd < n && !char.IsWhiteSpace(s[bareEnd]) && s[bareEnd] != ';')
                    {
                        bareEnd += 1;
                    }

                    return s.Substring(j, bareEnd - j);
                }
            }

            idx += 1;
        }

        return null;
    }

    /// <summary>Advances past a quoted region opening at <c>s[i]</c>, honoring '\' escapes.</summary>
    private static int SkipQuoted(string s, int i)
    {
        int n = s.Length;
        char quote = s[i];
        i += 1;
        while (i < n)
        {
            if (s[i] == '\\')
            {
                i += 2;
                continue;
            }

            if (s[i] == quote)
            {
                return i + 1;
            }

            i += 1;
        }

        return i;
    }

    /// <summary>Whether <paramref name="s"/> starts with <paramref name="value"/> at <paramref name="index"/>.</summary>
    private static bool StartsWithAt(string s, int index, string value)
    {
        if (index + value.Length > s.Length)
        {
            return false;
        }

        return string.CompareOrdinal(s, index, value, 0, value.Length) == 0;
    }

    /// <summary>
    /// Extracts <c>(username, password)</c> from a Java JAAS login-module string. Either
    /// element is <c>null</c> when that field is absent. Used only to <i>validate</i> the
    /// soak's admin credentials at startup (<see cref="CheckAdminCredentials"/>) — the
    /// Rust admin client takes <c>sasl.jaas.config</c> verbatim and parses it itself, so
    /// these values are never forwarded anywhere.
    /// </summary>
    internal static (string? Username, string? Password) JaasCredentials(string jaasConfig)
    {
        return (JaasField(jaasConfig, "username"), JaasField(jaasConfig, "password"));
    }

    /// <summary>
    /// Fails fast at startup when SASL is configured but credentials cannot be recovered
    /// from <c>sasl.jaas.config</c>.
    /// <para>
    /// The Rust admin client parses <c>sasl.jaas.config</c> itself, so this does NOT
    /// translate the config — it only <i>validates</i> that a username and password are
    /// present. Silently proceeding with no usable credentials is the one outcome worth
    /// refusing: it turns a typo into an authentication error from the broker minutes
    /// later, or — with an unauthenticated listener — into a soak that runs for two weeks
    /// against the wrong thing. PLAINTEXT (no SASL) requires no credentials and passes
    /// untouched.
    /// </para>
    /// <para>
    /// A second, symmetric misconfiguration is refused just as fast: SASL credentials or
    /// a mechanism are configured, but <c>security.protocol</c> is not a SASL protocol
    /// (does not contain <c>"SASL"</c>). The Rust client defaults to PLAINTEXT, so it
    /// would connect <i>unauthenticated</i> while the operator believes SASL is in force
    /// — exactly the "runs for two weeks against the wrong thing" failure, in the
    /// opposite direction. Naming it at startup beats discovering it from an
    /// unauthenticated listener later.
    /// </para>
    /// </summary>
    /// <exception cref="SoakFatalStartupException">
    /// A restart cannot fix a bad credential or a protocol mismatch, so it names the
    /// missing piece. Maps to <see cref="SoakExitCodes.Fatal"/>.
    /// </exception>
    internal static void CheckAdminCredentials(IReadOnlyDictionary<string, string> conf)
    {
        string protocol = conf.TryGetValue("security.protocol", out string? protocolValue) ? protocolValue : string.Empty;
        bool protocolIsSasl = protocol.IndexOf("SASL", StringComparison.OrdinalIgnoreCase) >= 0;
        string mechanism = conf.TryGetValue("sasl.mechanism", out string? mechanismValue) ? mechanismValue : string.Empty;
        string? jaas = conf.TryGetValue("sasl.jaas.config", out string? jaasValue) ? jaasValue : null;

        (string? username, string? password) = !string.IsNullOrEmpty(jaas)
            ? JaasCredentials(jaas)
            : (null, null);
        bool saslCredsPresent = username is not null || password is not null;
        bool saslConfigured = mechanism.Length > 0 || saslCredsPresent;

        // SASL is configured, but the protocol would not actually use it: the client
        // connects as PLAINTEXT (the Rust default) — silently unauthenticated. A restart
        // cannot fix a protocol mismatch, so refuse before the run begins.
        if (saslConfigured && !protocolIsSasl)
        {
            throw new SoakFatalStartupException(string.Format(
                CultureInfo.InvariantCulture,
                "sasl.mechanism='{0}' / sasl.jaas.config {1}, but security.protocol='{2}' is not a "
                + "SASL protocol, so the client would connect WITHOUT SASL (PLAINTEXT, the Rust "
                + "default) — silently unauthenticated. Set security.protocol to a SASL protocol "
                + "(e.g. SASL_SSL or SASL_PLAINTEXT). Restarting will not fix this.",
                mechanism,
                saslCredsPresent ? "has credentials" : "has no credentials",
                protocol));
        }

        bool saslExpected = protocolIsSasl || mechanism.Length > 0;
        if (!saslExpected)
        {
            return;
        }

        if (username is not null && password is not null)
        {
            return;
        }

        var missing = new List<string>();
        if (username is null)
        {
            missing.Add("username");
        }

        if (password is null)
        {
            missing.Add("password");
        }

        throw new SoakFatalStartupException(string.Format(
            CultureInfo.InvariantCulture,
            "security.protocol='{0}' / sasl.mechanism='{1}' require credentials, but "
            + "sasl.jaas.config {2}: could not extract {3}. Expected Java JAAS form: "
            + "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required "
            + "username=\"KEY\" password=\"SECRET\"; Restarting will not fix this.",
            protocol,
            mechanism,
            string.IsNullOrEmpty(jaas) ? "is not set" : "is set but unparseable",
            string.Join(" and ", missing)));
    }
}
