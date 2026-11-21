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

// Node (org.apache.kafka.common.Node).
//
// Java has no NodeTest; these are the Rust core's tests of its translation
// (rust/src/common/node.rs), as cpp/CLAUDE.md §11.1 says, plus the null host
// and rack that Java's isEmpty, hasRack and toString give a meaning to. The
// core's test_node_with_explicit_id_string exercises Java's protected
// constructor, which is not public and so not translated.

#include <confluent/kafka/common.hpp>

#include <gtest/gtest.h>

#include <optional>
#include <sstream>
#include <string>
#include <unordered_set>

namespace {

using confluent::kafka::common::Node;

TEST(NodeTest, Creation) {
    const Node node(1, "localhost", 9092);
    EXPECT_EQ(node.id(), 1);
    EXPECT_EQ(node.host(), "localhost");
    EXPECT_EQ(node.port(), 9092);
    EXPECT_EQ(node.id_string(), "1");
    EXPECT_FALSE(node.has_rack());
    EXPECT_EQ(node.rack(), std::nullopt);
    EXPECT_FALSE(node.is_fenced());
}

TEST(NodeTest, WithRack) {
    const Node node(1, "localhost", 9092, "rack1");
    EXPECT_TRUE(node.has_rack());
    EXPECT_EQ(node.rack(), "rack1");
}

TEST(NodeTest, WithFenced) {
    const Node node(1, "localhost", 9092, std::nullopt, true);
    EXPECT_TRUE(node.is_fenced());
}

TEST(NodeTest, NoNode) {
    const Node& node = Node::no_node();
    EXPECT_EQ(node.id(), -1);
    EXPECT_TRUE(node.is_empty());
    EXPECT_EQ(&node, &Node::no_node());
}

TEST(NodeTest, IsEmpty) {
    EXPECT_TRUE(Node(-1, "", -1).is_empty());
    EXPECT_TRUE(Node(0, "host", -1).is_empty());
    EXPECT_FALSE(Node(0, "host", 9092).is_empty());
    EXPECT_TRUE(Node(0, std::nullopt, 9092).is_empty());
}

TEST(NodeTest, Equality) {
    const Node n1(1, "host", 9092);
    const Node n2(1, "host", 9092);
    const Node n3(2, "host", 9092);
    EXPECT_EQ(n1, n2);
    EXPECT_NE(n1, n3);
    EXPECT_NE(n1, Node(1, "host", 9092, "rack"));
    EXPECT_NE(n1, Node(1, "host", 9092, std::nullopt, true));
    EXPECT_NE(n1, Node(1, std::nullopt, 9092));
    EXPECT_EQ(Node(1, std::nullopt, 9092), Node(1, std::nullopt, 9092));
}

TEST(NodeTest, Hash) {
    std::unordered_set<Node> set;
    set.insert(Node(1, "host", 9092));
    set.insert(Node(1, "host", 9092));
    EXPECT_EQ(set.size(), 1U);
    set.insert(Node(1, std::nullopt, 9092, std::nullopt, true));
    EXPECT_EQ(set.size(), 2U);
}

TEST(NodeTest, Display) {
    const Node node(1, "localhost", 9092, "rack1");
    EXPECT_EQ(to_string(node), "localhost:9092 (id: 1 rack: rack1 isFenced: false)");
    std::ostringstream out;
    out << Node(2, std::nullopt, 9093, std::nullopt, true);
    EXPECT_EQ(out.str(), "null:9093 (id: 2 rack: null isFenced: true)");
}

}  // namespace
