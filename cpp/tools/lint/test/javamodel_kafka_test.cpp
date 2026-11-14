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
// the two lists the Rust lints read: the @InterfaceAudience.Public types and
// the deprecated API.

#include <gtest/gtest.h>

#include <algorithm>
#include <fstream>
#include <memory>
#include <set>
#include <string>
#include <thread>

#include "java_model.hpp"

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

}  // namespace
