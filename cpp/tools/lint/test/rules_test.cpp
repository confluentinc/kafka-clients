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

// The rules on cpp/CLAUDE.md's own examples, over small Java sources.

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "data_files.hpp"
#include "expected.hpp"
#include "names.hpp"
#include "scope.hpp"
#include "types.hpp"

namespace {

using cklint::java::Model;
using cklint::rules::Decision;
using cklint::rules::ExpectedFunction;
using cklint::rules::ExpectedType;
using cklint::rules::FunctionKind;
using cklint::rules::Position;
using cklint::rules::TypeShape;

Model model_of(std::vector<std::pair<std::string, std::string>> sources) {
    auto model = cklint::java::build_model(sources);
    for (const auto& d : model.diagnostics) {
        ADD_FAILURE() << d.location.file << ':' << d.location.line << ": " << d.message;
    }
    return model;
}

/// As model_of, every type marked @InterfaceAudience.Public as the real ones are.
Model public_model_of(std::vector<std::pair<std::string, std::string>> sources) {
    auto model = model_of(std::move(sources));
    for (auto& [name, t] : model.types) {
        t.audience_public = true;
    }
    return model;
}

/// The method named `name` (Java's name) of `type` in `model`.
const cklint::java::Method& java_method(const Model& model, const std::string& type, const std::string& name) {
    const auto* t = model.find(type);
    EXPECT_NE(t, nullptr) << type;
    for (const auto& m : t->methods) {
        if (m.name == name) {
            return m;
        }
    }
    ADD_FAILURE() << "no method " << name;
    static const cklint::java::Method none;
    return none;
}

std::vector<std::string> names_of(const ExpectedType& e, FunctionKind kind) {
    std::vector<std::string> out;
    for (const auto& f : e.functions) {
        if (f.kind == kind) {
            out.push_back(f.name);
        }
    }
    return out;
}

const ExpectedFunction& function(const ExpectedType& e, const std::string& name, std::size_t index = 0) {
    std::size_t seen = 0;
    for (const auto& f : e.functions) {
        if (f.name == name && seen++ == index) {
            return f;
        }
    }
    ADD_FAILURE() << "no function " << name << " #" << index;
    static const ExpectedFunction none;
    return none;
}

std::string default_type(const cklint::rules::Mapping& m) {
    return m.ok() ? m.shapes.front().type.to_string() : "<" + m.unmapped + ">";
}

// ---- §2 names

TEST(NamesTest, SnakeCasesAsTheRustXtaskDoes) {
    EXPECT_EQ(cklint::rules::snake_case("getAPIVersion"), "get_api_version");
    EXPECT_EQ(cklint::rules::snake_case("offsetsForTimes"), "offsets_for_times");
    EXPECT_EQ(cklint::rules::snake_case("ByteArraySerializer"), "byte_array_serializer");
    EXPECT_EQ(cklint::rules::snake_case("UUIDSerializer"), "uuid_serializer");
}

TEST(NamesTest, TranslatesExceptionToErrorOnly) {
    // §2.2
    EXPECT_EQ(cklint::rules::type_name("TopicAuthorizationException"), "TopicAuthorizationError");
    EXPECT_EQ(cklint::rules::type_name("InvalidRegularExpression"), "InvalidRegularExpression");
    EXPECT_EQ(cklint::rules::type_name("SSLException"), "SSLError");
    EXPECT_EQ(cklint::rules::member_name("injectTimeoutException"), "inject_timeout_error");
    EXPECT_EQ(cklint::rules::member_name("sendException"), "send_error");
    EXPECT_EQ(cklint::rules::member_name("commitSync"), "commit_sync");
}

TEST(NamesTest, PlacesTypesInNamespacesAndHeaders) {
    // §2.1, §3.1
    EXPECT_EQ(cklint::rules::namespace_of("org.apache.kafka.clients.consumer"), "confluent::kafka::consumer");
    EXPECT_EQ(cklint::rules::namespace_of("org.apache.kafka.common.record"), "confluent::kafka::common::record");
    EXPECT_EQ(cklint::rules::umbrella_of("org.apache.kafka.clients.consumer"), "confluent/kafka/consumer.hpp");

    const auto model = model_of({{"org/apache/kafka/clients/consumer/CloseOptions.java", R"(
        package org.apache.kafka.clients.consumer;
        public class CloseOptions { public enum GroupMembershipOperation { LEAVE_GROUP } }
    )"}});
    const auto& nested = *model.find("org.apache.kafka.clients.consumer.CloseOptions$GroupMembershipOperation");
    EXPECT_EQ(cklint::rules::qualified_name(model, nested),
              "confluent::kafka::consumer::CloseOptions::GroupMembershipOperation");
    EXPECT_EQ(cklint::rules::header_of(model, nested), "confluent/kafka/consumer/close_options.hpp");
}

TEST(NamesTest, DropsGetOnlyFromAParameterlessGetter) {
    // §2.3
    const auto model = model_of({{"a/A.java", R"(
        package a;
        public class A {
            public long getMostSignificantBits() { return 0; }
            public String getName() { return null; }
            public Object get() { return null; }
            public Object getNow(Object valueIfAbsent) { return null; }
            public int getInt(String key) { return 0; }
            public int getShort() { return 0; }
            public void getReady() {}
            public boolean isEmpty() { return true; }
            public boolean hasRack() { return true; }
        }
    )"}});
    auto name = [&](const std::string& java) { return cklint::rules::method_name(java_method(model, "a.A", java)); };
    EXPECT_EQ(name("getMostSignificantBits"), "most_significant_bits");
    EXPECT_EQ(name("getName"), "name");
    EXPECT_EQ(name("get"), "get");
    EXPECT_EQ(name("getNow"), "get_now");
    EXPECT_EQ(name("getInt"), "get_int");      // takes a parameter
    EXPECT_EQ(name("getShort"), "get_short");  // `short` is a keyword
    EXPECT_EQ(name("getReady"), "get_ready");  // returns nothing
    EXPECT_EQ(name("isEmpty"), "is_empty");
    EXPECT_EQ(name("hasRack"), "has_rack");
}

TEST(NamesTest, NamesParametersAfterTheInterface) {
    // §2.3: Consumer.subscribe(topics, callback), KafkaConsumer.subscribe(topics, listener).
    const auto model = model_of({{"a/Consumer.java", R"(
        package a;
        import java.util.Collection;
        public interface Consumer { void subscribe(Collection<String> topics, Runnable callback); }
    )"},
                                 {"a/KafkaConsumer.java", R"(
        package a;
        import java.util.Collection;
        public class KafkaConsumer implements Consumer {
            public void subscribe(Collection<String> topics, Runnable listener) {}
            public void own(String someName) {}
        }
    )"}});
    const auto& consumer = *model.find("a.KafkaConsumer");
    EXPECT_EQ(cklint::rules::parameter_names(model, consumer, java_method(model, "a.KafkaConsumer", "subscribe")),
              (std::vector<std::string>{"topics", "callback"}));
    EXPECT_EQ(cklint::rules::parameter_names(model, consumer, java_method(model, "a.KafkaConsumer", "own")),
              (std::vector<std::string>{"some_name"}));
}

// ---- §5 types

class TypesTest : public ::testing::Test {
protected:
    TypesTest()
        : model_(model_of(
              {{"org/apache/kafka/common/Holder.java", R"(
            package org.apache.kafka.common;
            import java.time.Duration;
            import java.nio.ByteBuffer;
            import java.util.*;
            public class Holder<T> {
                public int i; public long l; public byte b; public boolean z; public char c;
                public String s; public byte[] bytes; public ByteBuffer buffer;
                public Optional<Integer> epoch; public List<String> list; public Set<String> set;
                public Collection<String> collection; public Map<String, Integer> map;
                public Duration duration; public Object object; public Integer timeoutMs;
                public KafkaFuture<Void> future; public Runnable runnable; public T t;
                public Map<String, ?> wildcard; public Holder<String> self; public Kind kind;
                public long[] longs;
            }
        )"},
               {"org/apache/kafka/common/KafkaFuture.java",
                "package org.apache.kafka.common; public abstract class KafkaFuture<T> {}"},
               {"org/apache/kafka/common/Kind.java", "package org.apache.kafka.common; public enum Kind { A }"},
               {"org/apache/kafka/common/annotation/InterfaceAudience.java", R"(
            package org.apache.kafka.common.annotation;
            public final class InterfaceAudience { public @interface Public {} }
        )"}})) {
        // Make the three Kafka types Public, as the real ones are.
        for (auto& [name, t] : model_.types) {
            if (name.rfind("org.apache.kafka.common.", 0) == 0) {
                t.audience_public = true;
            }
        }
    }

    std::string at(const std::string& field, Position position, std::string_view parameter_name = {}) const {
        const auto& holder = *model_.find("org.apache.kafka.common.Holder");
        const auto it =
            std::find_if(holder.fields.begin(), holder.fields.end(), [&](const auto& f) { return f.name == field; });
        return default_type(cklint::rules::map_type(model_, it->type, position, parameter_name));
    }

    cklint::rules::Mapping mapping(const std::string& field, Position position) const {
        const auto& holder = *model_.find("org.apache.kafka.common.Holder");
        const auto it =
            std::find_if(holder.fields.begin(), holder.fields.end(), [&](const auto& f) { return f.name == field; });
        return cklint::rules::map_type(model_, it->type, position);
    }

    Model model_;
};

TEST_F(TypesTest, MapsTheTypeTable) {
    EXPECT_EQ(at("i", Position::PARAMETER), "int32_t");
    EXPECT_EQ(at("l", Position::RETURN), "int64_t");
    EXPECT_EQ(at("b", Position::PARAMETER), "int8_t");
    EXPECT_EQ(at("z", Position::PARAMETER), "bool");
    EXPECT_EQ(at("s", Position::PARAMETER), "std::string_view");
    EXPECT_EQ(at("s", Position::RETURN), "std::string");
    EXPECT_EQ(at("s", Position::TYPE_ARGUMENT), "std::string");
    EXPECT_EQ(at("bytes", Position::PARAMETER), "std::span<const std::byte>");
    EXPECT_EQ(at("buffer", Position::PARAMETER), "std::span<const std::byte>");
    EXPECT_EQ(at("epoch", Position::RETURN), "std::optional<int32_t>");
    EXPECT_EQ(at("list", Position::PARAMETER), "std::span<const std::string>");
    EXPECT_EQ(at("list", Position::RETURN), "std::vector<std::string>");
    EXPECT_EQ(at("set", Position::RETURN), "std::unordered_set<std::string>");
    EXPECT_EQ(at("collection", Position::RETURN), "std::vector<std::string>");
    EXPECT_EQ(at("map", Position::PARAMETER), "const std::unordered_map<std::string, int32_t>&");
    EXPECT_EQ(at("map", Position::RETURN), "std::unordered_map<std::string, int32_t>");
    EXPECT_EQ(at("duration", Position::PARAMETER), "confluent::kafka::Duration");
    EXPECT_EQ(at("future", Position::RETURN), "confluent::kafka::common::KafkaFuture<void>");
    EXPECT_EQ(at("runnable", Position::PARAMETER), "std::function<void()>");
    EXPECT_EQ(at("t", Position::PARAMETER), "const T&");
    EXPECT_EQ(at("t", Position::RETURN), "T");
    EXPECT_EQ(at("self", Position::PARAMETER), "const confluent::kafka::common::Holder<std::string>&");
    EXPECT_EQ(at("kind", Position::PARAMETER), "confluent::kafka::common::Kind");
    EXPECT_EQ(at("longs", Position::RETURN), "std::vector<int64_t>");
    // §4.8: an Integer timeoutMs is optional milliseconds.
    EXPECT_EQ(at("timeoutMs", Position::PARAMETER, "timeoutMs"), "std::optional<int32_t>");
    EXPECT_EQ(at("timeoutMs", Position::PARAMETER, "other"), "int32_t");
}

TEST_F(TypesTest, ReportsShapesWithoutARow) {
    EXPECT_EQ(at("object", Position::PARAMETER), "<java.lang.Object does not appear on the surface (§5)>");
    EXPECT_EQ(at("c", Position::PARAMETER), "<no §5 row for char>");
    EXPECT_EQ(at("wildcard", Position::PARAMETER), "<an unbounded wildcard has no §5 row>");
}

TEST_F(TypesTest, NamesTheDecisionEachAlternativeNeeds) {
    // §4.6 and §5's TreeSet, TreeMap and Collection rows.
    const auto s = mapping("s", Position::PARAMETER);
    ASSERT_EQ(s.shapes.size(), 2U);
    EXPECT_EQ(s.shapes[1].type.to_string(), "std::optional<std::string_view>");
    EXPECT_EQ(s.shapes[1].decision, Decision::NULLABLE);
    const auto set = mapping("set", Position::RETURN);
    ASSERT_EQ(set.shapes.size(), 2U);
    EXPECT_EQ(set.shapes[1].type.to_string(), "std::set<std::string>");
    EXPECT_EQ(set.shapes[1].decision, Decision::ORDERED);
    const auto collection = mapping("collection", Position::RETURN);
    EXPECT_EQ(collection.shapes[1].type.to_string(), "std::unordered_set<std::string>");
    EXPECT_EQ(collection.shapes[1].decision, Decision::SET);
    const auto map = mapping("map", Position::PARAMETER);
    EXPECT_EQ(map.shapes.size(), 1U);  // one overload per Java overload (§5, §4.1)
}

// ---- expected shapes

TEST(ExpectedTest, AnEnumWithOneIntegerFieldTakesItsValues) {
    // §3.3: TimestampType : int32_t { CREATE_TIME = 0 }.
    const auto model = public_model_of({{"org/apache/kafka/common/record/TimestampType.java", R"(
        package org.apache.kafka.common.record;
        public enum TimestampType {
            NO_TIMESTAMP_TYPE(-1, "NoTimestampType"), CREATE_TIME(0, "CreateTime");
            public final int id;
            public final String name;
            TimestampType(int id, String name) { this.id = id; this.name = name; }
            public static TimestampType forName(String name) { return null; }
            @Override public String toString() { return name; }
        }
    )"},
                                        {"org/apache/kafka/common/record/Op.java",
                                         "package org.apache.kafka.common.record; public enum Op { SET, DELETE }"},
                                        {"org/apache/kafka/common/record/Type.java", R"(
        package org.apache.kafka.common.record;
        public enum Type { A((byte) 32), B((byte) 2); private final byte id; Type(byte id) { this.id = id; } }
    )"}});
    const auto e = cklint::rules::expected(model, *model.find("org.apache.kafka.common.record.TimestampType"));
    EXPECT_TRUE(e.problems.empty());
    EXPECT_EQ(e.shape, TypeShape::ENUM_CLASS);
    ASSERT_TRUE(e.underlying.has_value());
    EXPECT_EQ(e.underlying->to_string(), "int32_t");
    ASSERT_EQ(e.enumerators.size(), 2U);
    EXPECT_EQ(e.enumerators[0].value, "-1");
    EXPECT_EQ(e.enumerators[1].value, "0");
    EXPECT_EQ(names_of(e, FunctionKind::FREE_FUNCTION),
              (std::vector<std::string>{"timestamp_type_for_name", "to_string", "operator<<", "timestamp_type_id",
                                        "timestamp_type_name"}));

    // §3.3: an enum with no integer field takes the C header's values.
    const auto op = cklint::rules::expected(model, *model.find("org.apache.kafka.common.record.Op"));
    EXPECT_FALSE(op.underlying.has_value());
    EXPECT_EQ(op.enumerators.at(0).value, "");

    // Java's casts are dropped from the values.
    const auto type = cklint::rules::expected(model, *model.find("org.apache.kafka.common.record.Type"));
    EXPECT_EQ(type.underlying->to_string(), "int8_t");
    EXPECT_EQ(type.enumerators.at(0).value, "32");
}

TEST(ExpectedTest, TranslatesConstantsAndFields) {
    // §2.4
    const auto model = public_model_of({{"org/apache/kafka/common/Uuid.java", R"(
        package org.apache.kafka.common;
        import java.util.Set;
        public class Uuid {
            public static final Uuid ZERO_UUID = new Uuid();
            public static final Set<Uuid> RESERVED = Set.of(ZERO_UUID);
            public static final String TYPE = "User";
            public static final Set<String> NAMES = Set.of("a");
            public RuntimeException sendException = null;
            public final long bits = 0;
        }
    )"}});
    const auto e = cklint::rules::expected(model, *model.find("org.apache.kafka.common.Uuid"));
    EXPECT_TRUE(e.problems.empty());
    ASSERT_EQ(e.constants.size(), 2U);
    EXPECT_EQ(e.constants[0].name, "TYPE");
    EXPECT_EQ(default_type(e.constants[0].mapping), "std::string_view");
    EXPECT_EQ(default_type(e.constants[1].mapping), "std::array<std::string_view>");

    const auto& zero = function(e, "ZERO_UUID");
    EXPECT_EQ(zero.kind, FunctionKind::CONSTANT_FUNCTION);
    EXPECT_EQ(default_type(zero.returns), "const confluent::kafka::common::Uuid&");
    EXPECT_EQ(default_type(function(e, "RESERVED").returns), "const std::array<confluent::kafka::common::Uuid>&");

    const auto& setter = function(e, "send_error");
    EXPECT_EQ(setter.kind, FunctionKind::METHOD);
    ASSERT_EQ(setter.parameters.size(), 1U);
    EXPECT_EQ(default_type(setter.parameters[0].mapping), "std::exception_ptr");
    EXPECT_EQ(default_type(setter.returns), "void");
    EXPECT_TRUE(function(e, "bits").parameters.empty());
}

TEST(ExpectedTest, AppliesTheJdkInterfaceRows) {
    // §5: Comparable gives operator<=> and operator==, no compare_to; Principal is dropped.
    const auto model =
        public_model_of({{"org/apache/kafka/common/Uuid.java", R"(
        package org.apache.kafka.common;
        public class Uuid implements Comparable<Uuid>, java.security.Principal {
            public int compareTo(Uuid o) { return 0; }
            public String getName() { return null; }
            @Override public boolean equals(Object o) { return true; }
            @Override public int hashCode() { return 0; }
        }
    )"},
                         {"org/apache/kafka/common/Odd.java",
                          "package org.apache.kafka.common; public class Odd implements java.util.RandomAccess {}"}});
    const auto e = cklint::rules::expected(model, *model.find("org.apache.kafka.common.Uuid"));
    EXPECT_TRUE(e.problems.empty());
    const auto free = names_of(e, FunctionKind::FREE_FUNCTION);
    EXPECT_EQ(std::count(free.begin(), free.end(), "operator=="), 1);
    EXPECT_EQ(std::count(free.begin(), free.end(), "operator<=>"), 1);
    EXPECT_EQ(std::count(free.begin(), free.end(), "std::hash"), 1);
    EXPECT_EQ(names_of(e, FunctionKind::METHOD), (std::vector<std::string>{"name"}));

    const auto odd = cklint::rules::expected(model, *model.find("org.apache.kafka.common.Odd"));
    ASSERT_EQ(odd.problems.size(), 1U);
    EXPECT_EQ(odd.problems[0], "JDK supertype java.util.RandomAccess has no row in §5");
}

TEST(ExpectedTest, TranslatesInterfacesAsAbstractClasses) {
    // §5 "Interfaces Java declares".
    const auto model = public_model_of({{"org/apache/kafka/clients/producer/Callback.java", R"(
        package org.apache.kafka.clients.producer;
        @FunctionalInterface
        public interface Callback {
            void onCompletion(String metadata);
            default void close() {}
            static Callback none() { return null; }
        }
    )"}});
    const auto e = cklint::rules::expected(model, *model.find("org.apache.kafka.clients.producer.Callback"));
    EXPECT_TRUE(e.problems.empty());
    EXPECT_EQ(e.shape, TypeShape::ABSTRACT_CLASS);
    const auto& on = function(e, "on_completion");
    EXPECT_TRUE(on.is_virtual);
    EXPECT_TRUE(on.is_pure);
    const auto& close = function(e, "close");
    EXPECT_TRUE(close.is_virtual);
    EXPECT_FALSE(close.is_pure);
    EXPECT_EQ(function(e, "none").kind, FunctionKind::STATIC_METHOD);
    EXPECT_EQ(function(e, "of").kind, FunctionKind::STATIC_METHOD);  // §5.3
}

TEST(ExpectedTest, MergesOverloadsWithTheSameCppTypes) {
    // §4.1: overloads mapping to the same C++ types merge, named after the earliest declared.
    const auto model = public_model_of({{"org/apache/kafka/common/A.java", R"(
        package org.apache.kafka.common;
        import java.util.*;
        public class A {
            public void f(Collection<String> topics) {}
            public void f(List<String> names) {}
            public void g(int x) {}
            public void g(long x) {}
        }
    )"}});
    const auto e = cklint::rules::expected(model, *model.find("org.apache.kafka.common.A"));
    const auto methods = names_of(e, FunctionKind::METHOD);
    EXPECT_EQ(std::count(methods.begin(), methods.end(), "f"), 1);
    EXPECT_EQ(function(e, "f").parameters.at(0).name, "topics");
    EXPECT_NE(function(e, "f").note.find("merges org.apache.kafka.common.A#f(List)"), std::string::npos);
    EXPECT_EQ(std::count(methods.begin(), methods.end(), "g"), 2);  // int32_t and int64_t stay distinct
}

// ---- data files

TEST(DataFilesTest, ParsesAndValidatesDecisions) {
    const auto file = cklint::rules::parse_decisions("decisions.txt", R"(# comment
org.apache.kafka.common.Node#rack() return nullable body Node(id, host, port) passes null for it

org.apache.kafka.common.Node#rack() return maybe body the decision is not one of the three
org.apache.kafka.common.Node return nullable body no member
org.apache.kafka.common.Node#rack() return nullable body
)");
    ASSERT_EQ(file.lines.size(), 3U);
    EXPECT_EQ(file.lines[0].line, 2U);
    EXPECT_EQ(file.lines[0].fields[0], "org.apache.kafka.common.Node#rack()");
    EXPECT_EQ(file.lines[0].fields[4], "Node(id, host, port) passes null for it");
    ASSERT_EQ(file.errors.size(), 3U);
    // In line order, whichever check found them.
    EXPECT_EQ(file.errors[0], "decisions.txt:4: decision `maybe` is not one of nullable, ordered, set");
    EXPECT_EQ(file.errors[1],
              "decisions.txt:5: `org.apache.kafka.common.Node` is not a Java member `<binary name>#<signature>`");
    EXPECT_EQ(file.errors[2], "decisions.txt:6: missing the reason");
}

TEST(ScopeTest, FollowsSignaturesFromTheSeedsAndHonoursExclusions) {
    const auto model = public_model_of({
        {"org/apache/kafka/clients/producer/Producer.java", R"(
            package org.apache.kafka.clients.producer;
            import org.apache.kafka.common.internals.Hidden;
            public interface Producer {
                Record send(Record r);
                Excluded excluded();
                Hidden hidden();
                @Deprecated Old old();
                void take(Base b);
            }
        )"},
        {"org/apache/kafka/clients/producer/Record.java",
         "package org.apache.kafka.clients.producer; public class Record { public static class Inner {} }"},
        {"org/apache/kafka/clients/producer/Excluded.java",
         "package org.apache.kafka.clients.producer; public class Excluded {}"},
        {"org/apache/kafka/clients/producer/Old.java",
         "package org.apache.kafka.clients.producer; public class Old {}"},
        {"org/apache/kafka/clients/producer/Base.java",
         "package org.apache.kafka.clients.producer; public abstract class Base {}"},
        {"org/apache/kafka/clients/producer/Derived.java",
         "package org.apache.kafka.clients.producer; public class Derived extends Base {}"},
        {"org/apache/kafka/clients/producer/Unrelated.java",
         "package org.apache.kafka.clients.producer; public class Unrelated {}"},
        {"org/apache/kafka/common/internals/Hidden.java",
         "package org.apache.kafka.common.internals; public class Hidden {}"},
    });
    const auto excluded = cklint::rules::parse_scope_excluded("scope-excluded.txt", R"(
org.apache.kafka.clients.producer.Excluded §1.4 not translated
org.apache.kafka.clients.producer.Nothing §1.4 matches nothing
)");
    const auto scope = cklint::rules::compute_scope(model, excluded);
    auto in = [&](const std::string& n) { return scope.types.count("org.apache.kafka.clients.producer." + n) != 0; };
    EXPECT_TRUE(in("Producer"));
    EXPECT_TRUE(in("Record"));
    EXPECT_TRUE(in("Record$Inner"));
    EXPECT_TRUE(in("Base"));
    EXPECT_TRUE(in("Derived"));  // a public subclass of an abstract class a method takes
    EXPECT_FALSE(in("Excluded"));
    EXPECT_FALSE(in("Old"));  // reached only through a deprecated member
    EXPECT_FALSE(in("Unrelated"));
    EXPECT_EQ(scope.types.count("org.apache.kafka.common.internals.Hidden"), 0U);
    EXPECT_EQ(scope.types.at("org.apache.kafka.clients.producer.Record"),
              "org.apache.kafka.clients.producer.Producer#send(Record)");
    EXPECT_EQ(scope.used_exclusions, (std::set<std::size_t>{2}));
}

}  // namespace
