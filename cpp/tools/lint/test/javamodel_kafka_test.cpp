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

// The Java model of the kafka/ submodule's clients sources, checked against
// the two lists the Rust lints read (the @InterfaceAudience.Public types and
// the deprecated API), and the rules on it: the scope of cpp/CLAUDE.md §1.4
// and the expected shapes of the eleven verification classes.

#include <gtest/gtest.h>

#include <algorithm>
#include <fstream>
#include <memory>
#include <set>
#include <string>
#include <thread>

#include "data_files.hpp"
#include "expected.hpp"
#include "java_model.hpp"
#include "scope.hpp"

namespace {

using cklint::java::Model;

/// The non-comment lines of a list under design/current.
std::set<std::string> read_list(const std::string& file) {
    std::ifstream in(std::string(CK_LINT_DESIGN_DIR) + "/" + file);
    EXPECT_TRUE(in.good()) << "cannot read " << file;
    std::set<std::string> out;
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty() && line.front() != '#') {
            out.insert(line);
        }
    }
    return out;
}

/// The lines of `a` not in `b`, indented, one per line.
std::string difference(const std::set<std::string>& a, const std::set<std::string>& b) {
    std::string out;
    for (const auto& s : a) {
        if (b.count(s) == 0) {
            out += "  " + s + "\n";
        }
    }
    return out;
}

class JavaModelKafkaTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        model_ = std::make_unique<Model>(
            cklint::java::load_model(CK_LINT_KAFKA_JAVA_ROOT, std::max(1U, std::thread::hardware_concurrency())));
    }
    static void TearDownTestSuite() { model_.reset(); }

    static std::unique_ptr<Model> model_;
};

std::unique_ptr<Model> JavaModelKafkaTest::model_;

TEST_F(JavaModelKafkaTest, ParsesAndResolvesEverySource) {
    EXPECT_GT(model_->files, 1000U);
    for (const auto& d : model_->diagnostics) {
        ADD_FAILURE() << d.location.file << ':' << d.location.line << ": " << d.message;
    }
}

TEST_F(JavaModelKafkaTest, PublicAudienceMatchesTheCheckedInList) {
    std::set<std::string> parsed;
    for (const auto& [name, t] : model_->types) {
        if (t.enclosing.empty() && t.has_annotation(cklint::java::AUDIENCE_PUBLIC)) {
            parsed.insert(name);
        }
    }
    const auto listed = read_list("interface-audience-public-4.4.txt");
    EXPECT_EQ(parsed, listed) << "annotated but not listed:\n"
                              << difference(parsed, listed) << "listed but not annotated:\n"
                              << difference(listed, parsed);
}

TEST_F(JavaModelKafkaTest, EveryDeprecationIsInTheDeprecatedList) {
    // The list covers 4.3.1 too, so only this direction holds for the 4.4.0
    // sources. A deprecated class covers its members and nested classes.
    std::set<std::string> parsed;
    for (const auto& [name, t] : model_->types) {
        if (t.has_annotation(cklint::java::DEPRECATED)) {
            const auto* outer = t.enclosing.empty() ? nullptr : model_->find(t.enclosing);
            if (outer == nullptr || !outer->deprecated) {
                parsed.insert(name);
            }
            continue;
        }
        if (t.deprecated) {
            continue;
        }
        auto deprecated = [](const auto& annotations) {
            return std::any_of(annotations.begin(), annotations.end(),
                               [](const auto& a) { return a.name == cklint::java::DEPRECATED; });
        };
        for (const auto* list : {&t.constructors, &t.methods}) {
            for (const auto& m : *list) {
                if (deprecated(m.annotations)) {
                    parsed.insert(name + "#" + m.signature());
                }
            }
        }
        for (const auto& f : t.fields) {
            if (deprecated(f.annotations)) {
                parsed.insert(name + "#" + f.name);
            }
        }
        for (const auto& c : t.enum_constants) {
            if (deprecated(c.annotations)) {
                parsed.insert(name + "#" + c.name);
            }
        }
    }
    const auto listed = read_list("java-deprecated.txt");
    EXPECT_FALSE(parsed.empty());
    EXPECT_EQ(difference(parsed, listed), "") << "deprecated in the sources but not in java-deprecated.txt";
}

TEST_F(JavaModelKafkaTest, ReadsTheVerificationClasses) {
    const auto* tp = model_->find("org.apache.kafka.common.TopicPartition");
    ASSERT_NE(tp, nullptr);
    EXPECT_TRUE(tp->audience_public);
    ASSERT_EQ(tp->implements.size(), 1U);
    EXPECT_EQ(tp->implements[0].name, "java.io.Serializable");

    const auto* type = model_->find("org.apache.kafka.common.config.ConfigResource$Type");
    ASSERT_NE(type, nullptr);
    EXPECT_EQ(type->kind, cklint::java::TypeKind::ENUM);
    EXPECT_TRUE(type->audience_public);

    const auto* principal = model_->find("org.apache.kafka.common.security.auth.KafkaPrincipal");
    ASSERT_NE(principal, nullptr);
    ASSERT_EQ(principal->implements.size(), 1U);
    EXPECT_EQ(principal->implements[0].name, "java.security.Principal");

    const auto* consumer = model_->find("org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer");
    ASSERT_NE(consumer, nullptr);
    EXPECT_FALSE(consumer->audience_public);
}

TEST_F(JavaModelKafkaTest, ScopesTheClientFamilies) {
    const auto excluded = cklint::rules::read_scope_excluded(std::string(CK_LINT_DATA_DIR) + "/scope-excluded.txt");
    EXPECT_TRUE(excluded.errors.empty());
    const auto scope = cklint::rules::compute_scope(*model_, excluded);
    EXPECT_TRUE(scope.missing_seeds.empty());
    // Every line of scope-excluded.txt excludes something.
    EXPECT_EQ(scope.used_exclusions.size(), excluded.lines.size());
    for (const char* name : {
             "org.apache.kafka.common.TopicPartition",
             "org.apache.kafka.common.record.TimestampType",
             "org.apache.kafka.common.ElectionType",
             "org.apache.kafka.common.acl.AclPermissionType",
             "org.apache.kafka.clients.consumer.OffsetAndTimestamp",
             "org.apache.kafka.common.Node",
             "org.apache.kafka.common.Uuid",
             "org.apache.kafka.common.config.ConfigResource",
             "org.apache.kafka.common.config.ConfigResource$Type",
             "org.apache.kafka.common.security.auth.KafkaPrincipal",
             "org.apache.kafka.common.MetricName",
             "org.apache.kafka.common.Configurable",
             "org.apache.kafka.common.serialization.StringSerializer",
             "org.apache.kafka.common.utils.Bytes",
         }) {
        EXPECT_EQ(scope.types.count(name), 1U) << name;
    }
    for (const char* name : {
             "org.apache.kafka.common.config.ConfigDef",
             "org.apache.kafka.common.config.AbstractConfig",
             "org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer",
             "org.apache.kafka.clients.CommonClientConfigs",
             "org.apache.kafka.common.errors.TopicAuthorizationException",
             "org.apache.kafka.clients.consumer.OffsetResetStrategy",
         }) {
        EXPECT_EQ(scope.types.count(name), 0U) << name;
    }
}

TEST_F(JavaModelKafkaTest, DerivesTheVerificationClasses) {
    auto shape = [&](const std::string& name) { return cklint::rules::expected(*model_, *model_->find(name)); };
    for (const char* name : {
             "org.apache.kafka.common.TopicPartition",
             "org.apache.kafka.common.record.TimestampType",
             "org.apache.kafka.common.ElectionType",
             "org.apache.kafka.common.acl.AclPermissionType",
             "org.apache.kafka.clients.consumer.OffsetAndTimestamp",
             "org.apache.kafka.common.Node",
             "org.apache.kafka.common.Uuid",
             "org.apache.kafka.common.config.ConfigResource",
             "org.apache.kafka.common.config.ConfigResource$Type",
             "org.apache.kafka.common.security.auth.KafkaPrincipal",
             "org.apache.kafka.common.MetricName",
         }) {
        const auto e = shape(name);
        EXPECT_TRUE(e.problems.empty()) << name << ": " << (e.problems.empty() ? "" : e.problems.front());
    }
    // The plan expects Configurable to stop on its Map<String, ?> until a row is decided.
    const auto configurable = shape("org.apache.kafka.common.Configurable");
    ASSERT_EQ(configurable.problems.size(), 1U);
    EXPECT_NE(configurable.problems[0].find("an unbounded wildcard has no §5 row"), std::string::npos);

    const auto timestamp = shape("org.apache.kafka.common.record.TimestampType");
    EXPECT_EQ(timestamp.header, "confluent/kafka/common/record/timestamp_type.hpp");
    EXPECT_EQ(timestamp.underlying->to_string(), "int32_t");
    EXPECT_EQ(timestamp.enumerators.at(1).name, "CREATE_TIME");
    EXPECT_EQ(timestamp.enumerators.at(1).value, "0");

    const auto node = shape("org.apache.kafka.common.Node");
    EXPECT_EQ(std::count_if(node.functions.begin(), node.functions.end(),
                            [](const auto& f) { return f.kind == cklint::rules::FunctionKind::CONSTRUCTOR; }),
              3);  // the protected six-argument constructor is not public

    const auto uuid = shape("org.apache.kafka.common.Uuid");
    const auto zero =
        std::find_if(uuid.functions.begin(), uuid.functions.end(), [](const auto& f) { return f.name == "ZERO_UUID"; });
    ASSERT_NE(zero, uuid.functions.end());
    EXPECT_EQ(zero->kind, cklint::rules::FunctionKind::CONSTANT_FUNCTION);
    EXPECT_EQ(zero->returns.shapes.front().type.to_string(), "const confluent::kafka::common::Uuid&");

    const auto offsets = shape("org.apache.kafka.clients.consumer.OffsetAndTimestamp");
    EXPECT_EQ(offsets.cpp_name, "confluent::kafka::consumer::OffsetAndTimestamp");
}

}  // namespace
