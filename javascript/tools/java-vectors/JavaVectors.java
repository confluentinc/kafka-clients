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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.BooleanDeserializer;
import org.apache.kafka.common.serialization.BooleanSerializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.apache.kafka.common.serialization.VoidDeserializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.apache.kafka.common.utils.Utils;

/** Writes the values the unit tests compare the client against. */
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

    private static String hexOf(byte[] bytes) {
        StringBuilder b = new StringBuilder();
        for (byte x : bytes) b.append(String.format("%02x", x & 0xff));
        return b.toString();
    }

    private static byte[] unhex(String s) {
        byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < out.length; i++) out[i] = (byte) Integer.parseInt(s.substring(2 * i, 2 * i + 2), 16);
        return out;
    }

    /** A serializer's row: its class, the data given (as JSON) and the bytes it wrote. */
    private static String serialized(String cls, String data, byte[] bytes) {
        return "    {\"class\": " + str(cls) + ", \"data\": " + data + ", \"bytes\": " + hex(bytes) + "}";
    }

    /** A deserializer's row: its class, the bytes given, and the value it read (as JSON) or what it threw. */
    private static String deserialized(String cls, String bytes, Function<ByteBuffer, String> read) {
        String row = "    {\"class\": " + str(cls) + ", \"bytes\": " + str(bytes);
        try {
            return row + ", \"value\": " + read.apply(ByteBuffer.wrap(unhex(bytes))) + "}";
        } catch (RuntimeException e) {
            return row + ", \"error\": {\"class\": " + str(e.getClass().getSimpleName()) + ", \"message\": " + str(e.getMessage()) + "}}";
        }
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

        // Java's built-in serializers: the bytes each writes for a value.
        OUT.append("  \"serializers\": [\n");
        List<String> rows = new ArrayList<>();
        String[] strings = {"", "my string", "abc", "\u00e9t\u00e9", "\ud83d\ude80", "a\ud800b", "\udc00", "x\ud83dy", "\u0661\u0662 rtl", "\u0000nul"};
        for (String v : strings) rows.add(serialized("StringSerializer", str(v), new StringSerializer().serialize("t", v)));
        for (short v : new short[] {32767, -32768, 0, 1, -1, 256}) rows.add(serialized("ShortSerializer", Short.toString(v), new ShortSerializer().serialize("t", v)));
        for (int v : new int[] {423412424, -41243432, Integer.MAX_VALUE, Integer.MIN_VALUE, 0, -1}) {
            rows.add(serialized("IntegerSerializer", Integer.toString(v), new IntegerSerializer().serialize("t", v)));
        }
        for (long v : new long[] {922337203685477580L, -922337203685477581L, Long.MAX_VALUE, Long.MIN_VALUE, 0L, -1L, (1L << 53) - 1, 1L << 53}) {
            rows.add(serialized("LongSerializer", str(Long.toString(v)), new LongSerializer().serialize("t", v)));
        }
        int[] floatBits = {Float.floatToRawIntBits(5678567.12312f), Float.floatToRawIntBits(-5678567.12341f), 0, 0x80000000, 0x7f800000, 0xff800000,
            0x7fc00000, 0x7f800001, 0x7f800002, 1, 0x7f7fffff};
        for (int bits : floatBits) rows.add(serialized("FloatSerializer", str(String.format("%08x", bits)), new FloatSerializer().serialize("t", Float.intBitsToFloat(bits))));
        long[] doubleBits = {Double.doubleToRawLongBits(5678567.12312d), Double.doubleToRawLongBits(-5678567.12341d), 0L, 0x8000000000000000L,
            0x7ff0000000000000L, 0xfff0000000000000L, 0x7ff8000000000000L, 0x7ff0000000000001L, 0xfff8000000000001L, 1L, 0x7fefffffffffffffL};
        for (long bits : doubleBits) {
            rows.add(serialized("DoubleSerializer", str(String.format("%016x", bits)), new DoubleSerializer().serialize("t", Double.longBitsToDouble(bits))));
        }
        for (boolean v : new boolean[] {true, false}) rows.add(serialized("BooleanSerializer", Boolean.toString(v), new BooleanSerializer().serialize("t", v)));
        for (String v : new String[] {"123e4567-e89b-12d3-a456-426614174000", "123E4567-E89B-12D3-A456-426614174000", "00000000-0000-0000-0000-000000000000"}) {
            rows.add(serialized("UUIDSerializer", str(v), new UUIDSerializer().serialize("t", UUID.fromString(v))));
        }
        for (String v : new String[] {"", "6d7920737472696e67"}) rows.add(serialized("ByteArraySerializer", str(v), new ByteArraySerializer().serialize("t", unhex(v))));
        rows.add(serialized("VoidSerializer", "null", new VoidSerializer().serialize("t", null)));
        OUT.append(String.join(",\n", rows)).append("\n  ],\n");

        // Java's built-in deserializers, as the consumer calls them
        // (deserialize(topic, headers, ByteBuffer)): the value each reads,
        // or the class and message of what it throws.
        rows.clear();
        String[] stringBytes = {"", "6d7920737472696e67", "c3a974c3a9", "f09f9a80", "613f62", "ff", "c3", "e282", "f09f98", "eda080", "edbfbf", "eda000", "eda0ff", "edbfc1", "f18fedbf90", "61eda08062", "c0af", "61ff62", "f4908080"};
        for (String b : stringBytes) rows.add(deserialized("StringDeserializer", b, (d) -> str(new StringDeserializer().deserialize("t", new RecordHeaders(), d))));
        for (String b : new String[] {"7fff", "8000", "0000", "0100", "ffff", "", "01", "010203"}) {
            rows.add(deserialized("ShortDeserializer", b, (d) -> Short.toString(new ShortDeserializer().deserialize("t", new RecordHeaders(), d))));
        }
        for (String b : new String[] {"193ce3c8", "fd8aaed8", "7fffffff", "80000000", "", "000000", "0000000000"}) {
            rows.add(deserialized("IntegerDeserializer", b, (d) -> Integer.toString(new IntegerDeserializer().deserialize("t", new RecordHeaders(), d))));
        }
        for (String b : new String[] {"0ccccccccccccccc", "f333333333333333", "7fffffffffffffff", "8000000000000000", "ffffffffffffffff", "", "00000000000000", "000000000000000000"}) {
            rows.add(deserialized("LongDeserializer", b, (d) -> str(Long.toString(new LongDeserializer().deserialize("t", new RecordHeaders(), d)))));
        }
        for (String b : new String[] {"4aad4dce", "cacd4dce", "00000000", "80000000", "7f800000", "7fc00000", "7f800001", "", "000000", "0000000000"}) {
            rows.add(deserialized("FloatDeserializer", b,
                (d) -> str(String.format("%08x", Float.floatToRawIntBits(new FloatDeserializer().deserialize("t", new RecordHeaders(), d))))));
        }
        for (String b : new String[] {"4155a939c7e8e68a", "c155a939c7e8e68a", "0000000000000000", "7ff0000000000000", "7ff8000000000000", "0000000000000001", "", "00000000000000"}) {
            rows.add(deserialized("DoubleDeserializer", b,
                (d) -> str(String.format("%016x", Double.doubleToRawLongBits(new DoubleDeserializer().deserialize("t", new RecordHeaders(), d))))));
        }
        for (String b : new String[] {"01", "00", "02", "ff", "", "0000"}) {
            rows.add(deserialized("BooleanDeserializer", b, (d) -> Boolean.toString(new BooleanDeserializer().deserialize("t", new RecordHeaders(), d))));
        }
        for (String v : new String[] {"123e4567-e89b-12d3-a456-426614174000", "123E4567-E89B-12D3-A456-426614174000", "not-a-uuid", "", "123e4567-e89b-12d3-a456-4266141740000"}) {
            rows.add(deserialized("UUIDDeserializer", hexOf(v.getBytes(StandardCharsets.UTF_8)),
                (d) -> str(new UUIDDeserializer().deserialize("t", new RecordHeaders(), d).toString())));
        }
        for (String b : new String[] {"", "6d7920737472696e67"}) {
            rows.add(deserialized("ByteArrayDeserializer", b, (d) -> str(hexOf(new ByteArrayDeserializer().deserialize("t", new RecordHeaders(), d)))));
        }
        rows.add(deserialized("VoidDeserializer", "0000000000", (d) -> String.valueOf(new VoidDeserializer().deserialize("t", new RecordHeaders(), d))));
        OUT.append("  \"deserializers\": [\n").append(String.join(",\n", rows)).append("\n  ],\n");

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
