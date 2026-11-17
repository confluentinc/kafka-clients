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

// ClusterResource (org.apache.kafka.common.ClusterResource).
//
// Java has no ClusterResourceTest; these are the Rust core's tests of its
// translation (rust/src/common/cluster_resource.rs), as cpp/CLAUDE.md §11.1
// says, plus toString and the hash.

#include <confluent/kafka/common.hpp>

#include <gtest/gtest.h>

#include <optional>
#include <sstream>
#include <unordered_set>

namespace {

using confluent::kafka::common::ClusterResource;

TEST(ClusterResourceTest, ClusterResource) {
    const ClusterResource cr("test-cluster");
    EXPECT_EQ(cr.cluster_id(), "test-cluster");
}

TEST(ClusterResourceTest, ClusterResourceNone) {
    const ClusterResource cr(std::nullopt);
    EXPECT_EQ(cr.cluster_id(), std::nullopt);
}

TEST(ClusterResourceTest, ClusterResourceEquality) {
    const ClusterResource cr1("id");
    const ClusterResource cr2("id");
    const ClusterResource cr3(std::nullopt);
    EXPECT_EQ(cr1, cr2);
    EXPECT_NE(cr1, cr3);
    EXPECT_EQ(cr3, ClusterResource(std::nullopt));
}

TEST(ClusterResourceTest, ToStringAndHash) {
    EXPECT_EQ(to_string(ClusterResource("id")), "ClusterResource(clusterId=id)");
    std::ostringstream out;
    out << ClusterResource(std::nullopt);
    EXPECT_EQ(out.str(), "ClusterResource(clusterId=null)");
    const std::unordered_set<ClusterResource> set{ClusterResource("id"), ClusterResource("id"),
                                                  ClusterResource(std::nullopt)};
    EXPECT_EQ(set.size(), 2U);
}

}  // namespace
