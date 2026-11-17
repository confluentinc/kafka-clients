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

// AclPermissionType (org.apache.kafka.common.acl.AclPermissionType), from
// Java's AclPermissionTypeTest.

#include <confluent/kafka/common/acl.hpp>

#include <gtest/gtest.h>

#include <array>
#include <string>

namespace {

using confluent::kafka::common::acl::AclPermissionType;
namespace acl_permission_type = confluent::kafka::common::acl::acl_permission_type;

struct AclPermissionTypeTestInfo {
    AclPermissionType ty;
    int code;
    std::string name;
    bool unknown;
};

const std::array<AclPermissionTypeTestInfo, 4> INFOS{{
    {AclPermissionType::UNKNOWN, 0, "unknown", true},
    {AclPermissionType::ANY, 1, "any", false},
    {AclPermissionType::DENY, 2, "deny", false},
    {AclPermissionType::ALLOW, 3, "allow", false},
}};

TEST(AclPermissionTypeTest, testIsUnknown) {
    for (const auto& info : INFOS) {
        EXPECT_EQ(info.unknown, is_unknown(info.ty))
            << info.name << " was supposed to have unknown == " << info.unknown;
    }
}

TEST(AclPermissionTypeTest, testCode) {
    for (const auto& info : INFOS) {
        EXPECT_EQ(info.code, code(info.ty)) << info.name << " was supposed to have code == " << info.code;
        EXPECT_EQ(info.ty, acl_permission_type::from_code(static_cast<int8_t>(info.code)))
            << "AclPermissionType.fromCode(" << info.code << ") was supposed to be " << info.name;
    }
    EXPECT_EQ(AclPermissionType::UNKNOWN, acl_permission_type::from_code(static_cast<int8_t>(120)));
}

TEST(AclPermissionTypeTest, testName) {
    for (const auto& info : INFOS) {
        EXPECT_EQ(info.ty, acl_permission_type::from_string(info.name))
            << "AclPermissionType.fromString(" << info.name << ") was supposed to be " << info.name;
    }
    EXPECT_EQ(AclPermissionType::UNKNOWN, acl_permission_type::from_string("something"));
}

// Java compares INFOS with AclPermissionType.values(); the compiler's values()
// is not translated (cpp/CLAUDE.md §3.3), so the codes walk the enumerators.
TEST(AclPermissionTypeTest, testExhaustive) {
    for (std::size_t i = 0; i < INFOS.size(); ++i) {
        EXPECT_EQ(INFOS[i].ty, acl_permission_type::from_code(static_cast<int8_t>(i)));
    }
    EXPECT_EQ(acl_permission_type::from_code(static_cast<int8_t>(INFOS.size())), AclPermissionType::UNKNOWN);
}

}  // namespace
