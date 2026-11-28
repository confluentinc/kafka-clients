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

// confluent-kafka-java-class, -java-inheritance and -java-enum.

#include <algorithm>
#include <set>
#include <string>

#include "checks.hpp"
#include "clang/AST/ASTContext.h"
#include "clang/AST/DeclCXX.h"
#include "clang/AST/PrettyPrinter.h"
#include "clang/AST/QualTypeNames.h"
#include "clang/ASTMatchers/ASTMatchFinder.h"
#include "types.hpp"

namespace cklint::tidy {
namespace {

using clang::ast_matchers::MatchFinder;

const char* kind_of(rules::TypeShape shape) {
    switch (shape) {
        case rules::TypeShape::CLASS:
            return "class";
        case rules::TypeShape::ABSTRACT_CLASS:
            return "interface or abstract class";
        case rules::TypeShape::ENUM_CLASS:
            return "enum";
        case rules::TypeShape::ERROR_CLASS:
            return "exception";
    }
    return "type";
}

clang::PrintingPolicy qualified_policy(const clang::ASTContext& context) {
    clang::PrintingPolicy policy(context.getLangOpts());
    policy.SuppressTagKeyword = true;
    policy.SuppressUnwrittenScope = true;
    policy.FullyQualifiedName = true;
    return policy;
}

/// Whether `record`, or its namespace, declares an `operator<=>` taking it.
bool has_spaceship(const clang::CXXRecordDecl& record) {
    for (const auto* m : record.methods()) {
        if (m->getOverloadedOperator() == clang::OO_Spaceship) {
            return true;
        }
    }
    for (const auto* friend_decl : record.friends()) {
        if (const auto* f = llvm::dyn_cast_or_null<clang::FunctionDecl>(friend_decl->getFriendDecl())) {
            if (f->getOverloadedOperator() == clang::OO_Spaceship) {
                return true;
            }
        }
    }
    const auto* ns = record.getEnclosingNamespaceContext();
    for (const auto* d : ns->decls()) {
        const auto* f = llvm::dyn_cast<clang::FunctionDecl>(d);
        if (const auto* ft = llvm::dyn_cast<clang::FunctionTemplateDecl>(d)) {
            f = ft->getTemplatedDecl();
        }
        if (f == nullptr || f->getOverloadedOperator() != clang::OO_Spaceship || f->getNumParams() == 0) {
            continue;
        }
        const auto* param = f->getParamDecl(0)->getType().getNonReferenceType()->getAsCXXRecordDecl();
        if (param != nullptr && param->getCanonicalDecl() == record.getCanonicalDecl()) {
            return true;
        }
    }
    return false;
}

/// Whether `record` is abstract with a virtual destructor. Clang computes
/// neither on an uninstantiated class template, so both are read from the
/// declared methods.
bool is_abstract_with_virtual_destructor(const clang::CXXRecordDecl& record) {
    bool pure = record.isAbstract();
    bool virtual_destructor = false;
    for (const auto* m : record.methods()) {
        pure = pure || m->isPureVirtual();
        if (llvm::isa<clang::CXXDestructorDecl>(m)) {
            virtual_destructor = m->isVirtual();
        }
    }
    return pure && virtual_destructor;
}

}  // namespace

void TranslationUnitCheck::registerMatchers(MatchFinder* finder) {
    finder->addMatcher(clang::ast_matchers::translationUnitDecl().bind("tu"), this);
}

void JavaClassCheck::check(const MatchFinder::MatchResult& result) {
    const auto& java = JavaSide::get(settings());
    for (const auto& t : public_types(*result.Context, settings())) {
        const auto* jt = java.by_cpp_name(t.qualified_name);
        if (jt == nullptr) {
            if (!is_java_builtin_error(t.qualified_name)) {
                diag(t.decl->getLocation(),
                     "%0 translates no Java class: a public type is the translation of a Java class "
                     "(cpp/CLAUDE.md §1.4), anything else belongs in confluent::kafka::detail")
                    << t.decl;
            }
            continue;
        }
        if (!rules::is_public_api(*jt)) {
            diag(t.decl->getLocation(),
                 "%0 translates %1, which is not Java's public API (@InterfaceAudience.Public, outside internals "
                 "and unsupported packages): it belongs in confluent::kafka::detail (cpp/CLAUDE.md §1.4)")
                << t.decl << jt->binary_name;
            continue;
        }
        if (jt->deprecated) {
            diag(t.decl->getLocation(),
                 "%0 translates %1, which Java deprecates: it is generated only when the previous release has it "
                 "(cpp/CLAUDE.md §12.5)")
                << t.decl << jt->binary_name;
        }
        const auto& e = java.expected(*jt);
        if (t.header != e.header) {
            diag(t.decl->getLocation(), "%0 is declared in <%1>; cpp/CLAUDE.md §3.1 puts it in <%2>")
                << t.decl << t.header << e.header;
        }
        const auto* as_enum = llvm::dyn_cast<clang::EnumDecl>(t.decl);
        const auto* as_record = llvm::dyn_cast<clang::CXXRecordDecl>(t.decl);
        bool right_kind = true;
        switch (e.shape) {
            case rules::TypeShape::ENUM_CLASS:
                right_kind = as_enum != nullptr && as_enum->isScoped();
                break;
            case rules::TypeShape::CLASS:
            case rules::TypeShape::ERROR_CLASS:
                right_kind = as_record != nullptr && !as_record->isUnion();
                break;
            case rules::TypeShape::ABSTRACT_CLASS:
                right_kind = as_record != nullptr && is_abstract_with_virtual_destructor(*as_record);
                break;
        }
        if (!right_kind) {
            static const char* const shapes[] = {"a class", "an abstract class with a virtual destructor",
                                                 "an enum class", "a class"};
            diag(t.decl->getLocation(), "%0 translates the Java %1 %2: it is %3 (cpp/CLAUDE.md %4)")
                << t.decl << kind_of(e.shape) << jt->binary_name << shapes[static_cast<int>(e.shape)]
                << (e.shape == rules::TypeShape::ENUM_CLASS ? "§3.3" : "§5");
        }
    }
}

void JavaInheritanceCheck::check(const MatchFinder::MatchResult& result) {
    const auto& java = JavaSide::get(settings());
    const auto policy = qualified_policy(*result.Context);
    for (const auto& t : public_types(*result.Context, settings())) {
        const auto* record = llvm::dyn_cast<clang::CXXRecordDecl>(t.decl);
        const auto* jt = java.by_cpp_name(t.qualified_name);
        if (record == nullptr || jt == nullptr || !rules::is_public_api(*jt)) {
            continue;
        }
        const auto& e = java.expected(*jt);
        if (e.shape == rules::TypeShape::ERROR_CLASS || e.shape == rules::TypeShape::ENUM_CLASS) {
            continue;  // §6: the error emitter's hierarchy test checks errors
        }
        std::set<std::string> actual;
        for (const auto& base : record->bases()) {
            if (base.getAccessSpecifier() == clang::AS_public) {
                actual.insert(clang::TypeName::getFullyQualifiedName(base.getType(), *result.Context, policy));
            }
        }
        const std::set<std::string> wanted(e.bases.begin(), e.bases.end());
        for (const auto& b : wanted) {
            if (actual.count(b) == 0) {
                diag(record->getLocation(),
                     "%0 does not derive publicly from %1, the translation of a supertype of Java's %2 "
                     "(cpp/CLAUDE.md §5)")
                    << record << b << jt->binary_name;
            }
        }
        for (const auto& b : actual) {
            if (wanted.count(b) == 0) {
                diag(record->getLocation(),
                     "%0 derives publicly from %1, which translates no supertype of Java's %2 (cpp/CLAUDE.md §5)")
                    << record << b << jt->binary_name;
            }
        }
        const bool comparable = std::any_of(e.jdk_interfaces.begin(), e.jdk_interfaces.end(),
                                            [](const auto& row) { return row.rfind("Comparable", 0) == 0; });
        if (comparable) {
            if (!has_spaceship(*record)) {
                diag(record->getLocation(),
                     "%0 translates %1, which implements Comparable: it has operator<=> and operator== "
                     "(cpp/CLAUDE.md §5)")
                    << record << jt->binary_name;
            }
            for (const auto* m : record->methods()) {
                if (m->getIdentifier() != nullptr && m->getName() == "compare_to") {
                    diag(m->getLocation(),
                         "compare_to is not generated where Java implements Comparable; operator<=> replaces it "
                         "(cpp/CLAUDE.md §5)");
                }
            }
        }
    }
}

void JavaEnumCheck::check(const MatchFinder::MatchResult& result) {
    const auto& java = JavaSide::get(settings());
    const clang::PrintingPolicy policy(result.Context->getLangOpts());
    for (const auto& t : public_types(*result.Context, settings())) {
        const auto* en = llvm::dyn_cast<clang::EnumDecl>(t.decl);
        const auto* jt = java.by_cpp_name(t.qualified_name);
        if (en == nullptr || jt == nullptr || jt->kind != java::TypeKind::ENUM || !rules::is_public_api(*jt)) {
            continue;
        }
        const auto& e = java.expected(*jt);
        // An enum class always has a fixed type (int by default); the rule is
        // that it is written, so it is chosen and frozen on purpose (§3.3, §12.3).
        const auto* integer_type = en->getIntegerTypeSourceInfo();
        if (integer_type == nullptr) {
            diag(en->getLocation(), "%0 does not write its underlying type (cpp/CLAUDE.md §3.3, §12.3)") << en;
        } else if (e.underlying) {
            const auto written = integer_type->getType().getAsString(policy);
            if (written != e.underlying->to_string()) {
                diag(en->getLocation(),
                     "%0's underlying type is %1; Java's constructor sets one %2 field, so it is %2 "
                     "(cpp/CLAUDE.md §3.3)")
                    << en << written << e.underlying->to_string();
            }
        }
        std::vector<const clang::EnumConstantDecl*> actual(en->enumerator_begin(), en->enumerator_end());
        std::vector<std::string> wanted;
        for (const auto& x : e.enumerators) {
            wanted.push_back(x.name);
        }
        for (const auto& name : wanted) {
            const bool present =
                std::any_of(actual.begin(), actual.end(), [&](const auto* c) { return c->getName() == name; });
            if (!present) {
                diag(en->getLocation(), "%0 has no enumerator %1, a constant of Java's %2 (cpp/CLAUDE.md §3.3)")
                    << en << name << jt->binary_name;
            }
        }
        std::vector<std::string> common;
        for (const auto* c : actual) {
            const auto it = std::find(wanted.begin(), wanted.end(), c->getName().str());
            if (it == wanted.end()) {
                diag(c->getLocation(), "%0 is not a constant of Java's %1 (cpp/CLAUDE.md §3.3)")
                    << c << jt->binary_name;
                continue;
            }
            common.push_back(*it);
            const auto& value = e.enumerators[static_cast<std::size_t>(it - wanted.begin())].value;
            if (!value.empty()) {
                const long long java_value = std::stoll(value, nullptr, 0);
                if (c->getInitVal().getSExtValue() != java_value) {
                    diag(c->getLocation(), "%0 is %1; Java gives it %2 (cpp/CLAUDE.md §3.3)")
                        << c << std::to_string(c->getInitVal().getSExtValue()) << value;
                }
            }
        }
        // §3.3: Java's declaration order, new constants appended.
        std::vector<std::string> wanted_common;
        for (const auto& w : wanted) {
            if (std::find(common.begin(), common.end(), w) != common.end()) {
                wanted_common.push_back(w);
            }
        }
        for (std::size_t i = 0; i < common.size(); ++i) {
            if (common[i] != wanted_common[i]) {
                const auto* c = *std::find_if(actual.begin(), actual.end(),
                                              [&](const auto* x) { return x->getName() == common[i]; });
                diag(c->getLocation(),
                     "%0 is out of Java's declaration order: Java declares %1 here (cpp/CLAUDE.md §3.3)")
                    << c << wanted_common[i];
                break;
            }
        }
    }
}

}  // namespace cklint::tidy
