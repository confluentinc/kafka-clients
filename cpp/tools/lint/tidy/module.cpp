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

// The clang-tidy module of the translation linter:
//   clang-tidy-21 -load libConfluentKafkaTidy.so -checks='-*,confluent-kafka-*' ...

#include "checks.hpp"
#include "clang-tidy/ClangTidyModule.h"
#include "clang-tidy/ClangTidyModuleRegistry.h"

namespace cklint::tidy {
namespace {

class ConfluentKafkaModule : public clang::tidy::ClangTidyModule {
public:
    void addCheckFactories(clang::tidy::ClangTidyCheckFactories& factories) override {
        factories.registerCheck<JavaClassCheck>("confluent-kafka-java-class");
        factories.registerCheck<JavaInheritanceCheck>("confluent-kafka-java-inheritance");
        factories.registerCheck<JavaEnumCheck>("confluent-kafka-java-enum");
        factories.registerCheck<JavaMembersCheck>("confluent-kafka-java-members");
        factories.registerCheck<JavaSignatureCheck>("confluent-kafka-java-signature");
        factories.registerCheck<SurfaceCheck>("confluent-kafka-surface");
        factories.registerCheck<DocCheck>("confluent-kafka-doc");
        factories.registerCheck<AbiCheck>("confluent-kafka-abi");
        factories.registerCheck<InventoryCheck>("confluent-kafka-inventory");
        factories.registerCheck<HeaderCheck>("confluent-kafka-header");
    }
};

}  // namespace
}  // namespace cklint::tidy

// NOLINTNEXTLINE(cert-err58-cpp): the registry is clang-tidy's own pattern.
static const clang::tidy::ClangTidyModuleRegistry::Add<cklint::tidy::ConfluentKafkaModule> registration(
    "confluent-kafka-module", "The C++ binding's translation checks against the Java client.");
