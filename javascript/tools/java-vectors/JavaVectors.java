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

// Writes test/java_vectors.json: values computed by Apache Kafka's own
// kafka-clients (the release in kafka/), which the unit tests compare the
// JavaScript client against, so that no test needs Java to run. It runs
// outside CI, by hand, after a Kafka upgrade:
//
//   (cd kafka && ./gradlew clients:jar -x test)
//   java -cp "kafka/clients/build/libs/*:<slf4j-api jar>" \
//     javascript/tools/java-vectors/JavaVectors.java > javascript/test/java_vectors.json
//
// The inputs are fixed here, so a run with the same Kafka release writes the
// same file.

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;

public class JavaVectors {
    private static final StringBuilder OUT = new StringBuilder();

    private static String str(String s) {
        if (s == null) return "null";
        StringBuilder b = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            if (c == '"' || c == '\\') b.append('\\').append(c);
            else if (c < 0x20 || c > 0x7e) b.append(String.format("\\u%04x", (int) c));
            else b.append(c);
        }
        return b.append('"').toString();
    }

    private static String hex(byte[] bytes) {
        if (bytes == null) return "null";
        StringBuilder b = new StringBuilder("\"");
        for (byte x : bytes) b.append(String.format("%02x", x & 0xff));
        return b.append('"').toString();
    }

    /** The keys whose placement the partition tests check: 200 ASCII keys, then non-ASCII ones. */
    static List<String> keys() {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 200; i++) keys.add("key-" + i);
        keys.add("été");
        keys.add("🚀");
        keys.add("日本");
        keys.add(" ");
        return keys;
    }

    public static void main(String[] args) {
        OUT.append("{\n  \"kafka\": ").append(str(org.apache.kafka.common.utils.internals.AppInfoParser.getVersion())).append(",\n");

        // BuiltInPartitioner.partitionForKey: toPositive(murmur2(key)) % numPartitions.
        OUT.append("  \"murmur2\": [\n");
        List<String> keys = keys();
        for (int i = 0; i < keys.size(); i++) {
            byte[] key = keys.get(i).getBytes(StandardCharsets.UTF_8);
            OUT.append("    {\"key\": ").append(str(keys.get(i)))
                .append(", \"murmur2\": ").append(Utils.murmur2(key))
                .append(", \"partition12\": ").append(Utils.toPositive(Utils.murmur2(key)) % 12)
                .append(", \"partition7\": ").append(Utils.toPositive(Utils.murmur2(key)) % 7)
                .append("}").append(i + 1 < keys.size() ? ",\n" : "\n");
        }
        OUT.append("  ],\n");

        // StringSerializer bytes, for each standard charset.
        String[] strings = {"", "abc", "été", "🚀", "a\ud800b", "\udc00", "x\ud83dy", "١٢ rtl", "é", "\u0000nul"};
        String[] charsets = {"UTF-8", "UTF-16", "UTF-16BE", "UTF-16LE", "ISO-8859-1", "US-ASCII"};
        OUT.append("  \"stringSerializer\": [\n");
        List<String> rows = new ArrayList<>();
        try (StringSerializer serializer = new StringSerializer()) {
            for (String charset : charsets) {
                serializer.configure(java.util.Map.of("serializer.encoding", charset), false);
                for (String s : strings) {
                    rows.add("    {\"encoding\": " + str(charset) + ", \"data\": " + str(s) + ", \"bytes\": " + hex(serializer.serialize("t", s)) + "}");
                }
            }
        }
        OUT.append(String.join(",\n", rows)).append("\n  ],\n");

        // toString() and hashCode() of records, metadata and headers.
        rows.clear();
        RecordHeaders headers = new RecordHeaders(new RecordHeader[] {new RecordHeader("h", "v".getBytes(StandardCharsets.UTF_8)), new RecordHeader("n", (byte[]) null)});
        Object[][] objects = {
            {"ProducerRecord(t, v)", new ProducerRecord<String, String>("t", "v")},
            {"ProducerRecord(t, 3, 42, k, v, headers)", new ProducerRecord<>("t", 3, 42L, "k", "v", headers)},
            {"ProducerRecord(t, null, null, null, null)", new ProducerRecord<String, String>("t", null, (Long) null, null, null)},
            {"RecordMetadata(t-1, 5, 7, 1000, 2, 3)", new RecordMetadata(new TopicPartition("t", 1), 5, 7, 1000L, 2, 3)},
            {"RecordMetadata(t-0, -1, 0, -1, -1, -1)", new RecordMetadata(new TopicPartition("t", 0), -1, 0, -1L, -1, -1)},
            {"RecordHeader(h, v)", new RecordHeader("h", "v".getBytes(StandardCharsets.UTF_8))},
            {"RecordHeader(n, null)", new RecordHeader("n", (byte[]) null)},
            {"RecordHeaders(h=v, n=null)", headers},
        };
        OUT.append("  \"objects\": [\n");
        for (Object[] o : objects) {
            Object value = o[1];
            boolean hashed = !(value instanceof RecordMetadata);
            rows.add("    {\"what\": " + str((String) o[0]) + ", \"toString\": " + str(value.toString())
                + (hashed ? ", \"hashCode\": " + value.hashCode() : "") + "}");
        }
        OUT.append(String.join(",\n", rows)).append("\n  ],\n");

        // The messages of errors built from their payloads.
        rows.clear();
        rows.add("    {\"what\": \"TopicAuthorizationException(Set.of(t))\", \"message\": " + str(new TopicAuthorizationException(Set.of("t")).getMessage()) + "}");
        rows.add("    {\"what\": \"InvalidTopicException(Set.of(t))\", \"message\": " + str(new InvalidTopicException(Set.of("t")).getMessage()) + "}");
        OUT.append("  \"errorMessages\": [\n").append(String.join(",\n", rows)).append("\n  ]\n}\n");
        System.out.print(OUT);
        if (Charset.defaultCharset() == null) throw new IllegalStateException();
    }
}
