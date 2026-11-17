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

#include "session.hpp"

#include <algorithm>
#include <filesystem>
#include <set>
#include <thread>

#include "clang/AST/ASTContext.h"
#include "clang/AST/Decl.h"
#include "clang/AST/DeclCXX.h"
#include "clang/AST/DeclTemplate.h"
#include "clang/Basic/SourceManager.h"
#include "names.hpp"

namespace cklint::tidy {
namespace {

std::string canonical(std::string_view path) {
    std::error_code ec;
    auto p = std::filesystem::weakly_canonical(std::filesystem::path(path), ec);
    return ec ? std::string(path) : p.generic_string();
}

class Walker {
public:
    Walker(clang::ASTContext& context, const Settings& settings, std::vector<CppType>& out)
        : context_(context), settings_(settings), out_(out) {}

    void run() {
        for (const auto* d : context_.getTranslationUnitDecl()->decls()) {
            const auto* confluent = llvm::dyn_cast<clang::NamespaceDecl>(d);
            if (confluent == nullptr || confluent->getName() != "confluent") {
                continue;
            }
            for (const auto* inner : confluent->decls()) {
                const auto* kafka = llvm::dyn_cast<clang::NamespaceDecl>(inner);
                if (kafka != nullptr && kafka->getName() == "kafka") {
                    walk_namespace(kafka);
                }
            }
        }
    }

private:
    void walk_namespace(const clang::DeclContext* dc) {
        for (const auto* d : dc->decls()) {
            if (const auto* ns = llvm::dyn_cast<clang::NamespaceDecl>(d)) {
                if (ns->getName() != "detail") {  // §2.1: internal, undocumented
                    walk_namespace(ns);
                }
            } else {
                visit(d, /*nested=*/false);
            }
        }
    }

    void visit(const clang::Decl* d, bool nested) {
        const clang::TagDecl* tag = nullptr;
        if (const auto* ctd = llvm::dyn_cast<clang::ClassTemplateDecl>(d)) {
            tag = ctd->getTemplatedDecl();
        } else if (llvm::isa<clang::ClassTemplateSpecializationDecl>(d)) {
            return;
        } else if (const auto* rd = llvm::dyn_cast<clang::CXXRecordDecl>(d)) {
            if (rd->isImplicit() || rd->isLambda()) {
                return;
            }
            tag = rd;
        } else if (const auto* ed = llvm::dyn_cast<clang::EnumDecl>(d)) {
            tag = ed;
        }
        if (tag == nullptr || !tag->isThisDeclarationADefinition() || tag->getIdentifier() == nullptr) {
            return;
        }
        if (nested && tag->getAccess() != clang::AS_public) {
            return;
        }
        const auto& sm = context_.getSourceManager();
        const auto loc = sm.getSpellingLoc(tag->getLocation());
        const auto entry = sm.getFileEntryRefForID(sm.getFileID(loc));
        if (!entry) {
            return;
        }
        auto header = relative_to_include_root(entry->getName(), settings_);
        if (header.rfind("confluent/kafka/", 0) != 0) {
            return;
        }
        out_.push_back(CppType{tag, tag->getQualifiedNameAsString(), std::move(header)});
        if (const auto* rd = llvm::dyn_cast<clang::CXXRecordDecl>(tag)) {
            for (const auto* member : rd->decls()) {
                visit(member, /*nested=*/true);
            }
        }
    }

    clang::ASTContext& context_;
    const Settings& settings_;
    std::vector<CppType>& out_;
};

}  // namespace

Settings Settings::of(const clang::tidy::ClangTidyCheck::OptionsView& options) {
    Settings s;
    s.java_root = options.getLocalOrGlobal("ConfluentKafkaJavaRoot", CK_LINT_KAFKA_JAVA_ROOT).str();
    s.data_dir = options.getLocalOrGlobal("ConfluentKafkaDataDir", CK_LINT_DATA_DIR).str();
    s.include_root = options.getLocalOrGlobal("ConfluentKafkaIncludeRoot", CK_LINT_INCLUDE_ROOT).str();
    return s;
}

const JavaSide& JavaSide::get(const Settings& settings) {
    static std::mutex lock;
    static std::map<std::string, std::unique_ptr<JavaSide>> cache;
    const std::lock_guard<std::mutex> guard(lock);
    auto& slot = cache[settings.key()];
    if (!slot) {
        slot.reset(new JavaSide(settings));
    }
    return *slot;
}

JavaSide::JavaSide(const Settings& settings)
    : model_(java::load_model(settings.java_root, std::max(1U, std::thread::hardware_concurrency()))),
      excluded_(rules::read_scope_excluded(settings.data_dir + "/scope-excluded.txt")),
      decisions_(rules::read_decisions(settings.data_dir + "/decisions.txt")),
      baseline_(rules::read_baseline(settings.data_dir + "/baseline.txt")),
      scope_(rules::compute_scope(model_, excluded_)) {
    for (const auto& [binary_name, type] : model_.types) {
        by_cpp_name_.emplace(rules::qualified_name(model_, type), binary_name);
    }
}

const java::Type* JavaSide::by_cpp_name(std::string_view cpp_name) const {
    const auto it = by_cpp_name_.find(cpp_name);
    return it == by_cpp_name_.end() ? nullptr : model_.find(it->second);
}

const rules::ExpectedType& JavaSide::expected(const java::Type& type) const {
    const std::lock_guard<std::mutex> guard(expected_lock_);
    auto it = expected_.find(type.binary_name);
    if (it == expected_.end()) {
        it = expected_.emplace(type.binary_name, rules::expected(model_, type)).first;
    }
    return it->second;
}

std::vector<CppType> public_types(clang::ASTContext& context, const Settings& settings) {
    std::vector<CppType> out;
    Walker(context, settings, out).run();
    return out;
}

std::string relative_to_include_root(std::string_view file, const Settings& settings) {
    const auto root = canonical(settings.include_root) + "/";
    const auto path = canonical(file);
    return path.rfind(root, 0) == 0 ? path.substr(root.size()) : std::string();
}

bool is_java_builtin_error(std::string_view qualified_name) {
    static const std::set<std::string, std::less<>> names = {
        "confluent::kafka::IllegalArgumentError",
        "confluent::kafka::NullPointerError",
        "confluent::kafka::IndexOutOfBoundsError",
        "confluent::kafka::IllegalStateError",
        "confluent::kafka::ConcurrentModificationError",
        "confluent::kafka::UnsupportedOperationError",
        "confluent::kafka::TimeoutError",
        "confluent::kafka::InterruptedError",
        "confluent::kafka::ExecutionError",
        "confluent::kafka::NoSuchElementError",
    };
    return names.count(qualified_name) != 0;
}

}  // namespace cklint::tidy
