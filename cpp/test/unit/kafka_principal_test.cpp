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

// KafkaPrincipal (org.apache.kafka.common.security.auth.KafkaPrincipal).
//
// Java's KafkaPrincipalTest, plus the getters, the token flag, toString and
// ANONYMOUS.

#include <confluent/kafka/common/security/auth.hpp>

#include <gtest/gtest.h>

#include <sstream>
#include <string>
#include <utility>

namespace {

using confluent::kafka::common::security::auth::KafkaPrincipal;

TEST(KafkaPrincipalTest, EqualsAndHashCode) {
    const std::string name = "KafkaUser";
    const KafkaPrincipal principal1(KafkaPrincipal::USER_TYPE, name);
    const KafkaPrincipal principal2(KafkaPrincipal::USER_TYPE, name);

    EXPECT_EQ(std::hash<KafkaPrincipal>{}(principal1), std::hash<KafkaPrincipal>{}(principal2));
    EXPECT_EQ(principal1, principal2);
}

TEST(KafkaPrincipalTest, KeepsTypeAndName) {
    const KafkaPrincipal principal("Group", "admins");
    EXPECT_EQ(principal.principal_type(), "Group");
    EXPECT_EQ(principal.name(), "admins");
    EXPECT_FALSE(principal.token_authenticated());
    EXPECT_NE(principal, KafkaPrincipal("User", "admins"));
    EXPECT_NE(principal, KafkaPrincipal("Group", "users"));
}

TEST(KafkaPrincipalTest, TokenFlagIsSetInPlaceAndNotCompared) {
    KafkaPrincipal principal(KafkaPrincipal::USER_TYPE, "alice", true);
    EXPECT_TRUE(principal.token_authenticated());
    const KafkaPrincipal copy = principal;
    principal.token_authenticated(false);
    EXPECT_FALSE(principal.token_authenticated());
    EXPECT_TRUE(copy.token_authenticated());
    EXPECT_EQ(principal, copy);

    KafkaPrincipal assigned("Group", "g");
    assigned = principal;
    EXPECT_EQ(assigned, principal);
    EXPECT_FALSE(assigned.token_authenticated());
    const KafkaPrincipal moved = std::move(assigned);
    EXPECT_EQ(moved.name(), "alice");
}

TEST(KafkaPrincipalTest, ToStringAndAnonymous) {
    EXPECT_EQ(to_string(KafkaPrincipal("User", "bob")), "User:bob");
    std::ostringstream out;
    out << KafkaPrincipal::ANONYMOUS();
    EXPECT_EQ(out.str(), "User:ANONYMOUS");
    EXPECT_EQ(&KafkaPrincipal::ANONYMOUS(), &KafkaPrincipal::ANONYMOUS());
}

}  // namespace
