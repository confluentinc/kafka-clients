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

// confluent-kafka-java-members, -java-signature and -surface.

#include <algorithm>
#include <fstream>
#include <set>
#include <string>

#include "checks.hpp"
#include "clang/AST/ASTContext.h"
#include "clang/AST/DeclCXX.h"
#include "clang/ASTMatchers/ASTMatchFinder.h"
#include "clang/Basic/SourceManager.h"
#include "pairing.hpp"
#include "type_reader.hpp"

namespace cklint::tidy {
namespace {

using clang::ast_matchers::MatchFinder;

const char* what(const rules::ExpectedFunction& f) {
    switch (f.kind) {
        case rules::FunctionKind::CONSTRUCTOR:
            return "constructor";
        case rules::FunctionKind::METHOD:
            return "method";
        case rules::FunctionKind::STATIC_METHOD:
            return "static method";
        case rules::FunctionKind::FREE_FUNCTION:
            return "free function";
        case rules::FunctionKind::CONSTANT_FUNCTION:
            return "constant function";
    }
    return "function";
}

std::string spelled(const rules::ExpectedFunction& f) {
    return f.qualifier.empty() ? f.name : f.qualifier + "::" + f.name;
}

std::string allowed(const rules::Mapping& m) {
    std::string out;
    for (const auto& s : m.shapes) {
        out += (out.empty() ? "" : " or ") + s.type.to_string();
    }
    return out;
}

/// `<Java member> <parameter|return> <tag>`, the key of a decisions.txt line.
std::string decision_key(const std::string& member, const std::string& position, rules::Decision d) {
    return member + " " + position + " " + rules::decision_name(d);
}

std::set<std::string> decision_keys(const rules::DataFile& decisions) {
    std::set<std::string> out;
    for (const auto& line : decisions.lines) {
        out.insert(line.fields[0] + " " + line.fields[1] + " " + line.fields[2]);
    }
    return out;
}

/// The decisions.txt keys the pairs need, with where each is needed.
std::vector<std::pair<std::string, const clang::Decl*>> needed_decisions(const Pairing& pairing,
                                                                         const clang::ASTContext& context) {
    std::vector<std::pair<std::string, const clang::Decl*>> out;
    for (const auto& tp : pairing.types) {
        for (const auto& p : tp.pairs) {
            for (std::size_t i = 0; i < p.expected->parameters.size() && i < p.cpp->getNumParams(); ++i) {
                const auto& ep = p.expected->parameters[i];
                const auto* shape = matching_shape(ep.mapping, read_type(p.cpp->getParamDecl(i)->getType(), context));
                if (shape != nullptr && shape->decision != rules::Decision::NONE && !ep.java_name.empty()) {
                    out.emplace_back(decision_key(p.expected->java_member, ep.java_name, shape->decision),
                                     p.cpp->getParamDecl(i));
                }
            }
            if (!llvm::isa<clang::CXXConstructorDecl>(p.cpp) && !p.expected->returns.shapes.empty()) {
                const auto* shape = matching_shape(p.expected->returns, read_type(p.cpp->getReturnType(), context));
                if (shape != nullptr && shape->decision != rules::Decision::NONE) {
                    out.emplace_back(decision_key(p.expected->java_member, "return", shape->decision), p.cpp);
                }
            }
        }
    }
    return out;
}

bool is_comparable_convention(const rules::ExpectedFunction& f) {
    return f.java_member.empty() && f.note == "§5 Comparable";
}

clang::SourceLocation main_file_start(const clang::SourceManager& sm) {
    return sm.getLocForStartOfFile(sm.getMainFileID());
}

}  // namespace

void JavaMembersCheck::registerMatchers(MatchFinder* finder) {
    TranslationUnitCheck::registerMatchers(finder);
    using namespace clang::ast_matchers;
    // §1.3: a _cb function is never called.
    finder->addMatcher(declRefExpr(to(functionDecl(matchesName("_cb$")))).bind("cb"), this);
}

void JavaMembersCheck::check(const MatchFinder::MatchResult& result) {
    if (const auto* cb = result.Nodes.getNodeAs<clang::DeclRefExpr>("cb")) {
        const auto& sm = *result.SourceManager;
        const auto entry = sm.getFileEntryRefForID(sm.getFileID(sm.getSpellingLoc(cb->getLocation())));
        if (entry && !relative_to_include_root(entry->getName(), settings()).empty()) {
            diag(cb->getLocation(),
                 "%0 is a _cb function: the binding calls only the own-name fallible functions and the _err twins "
                 "(cpp/CLAUDE.md §1.3)")
                << cb->getDecl();
        }
        return;
    }
    const auto& java = JavaSide::get(settings());
    const auto pairing = pair_all(*result.Context, settings(), java);
    for (const auto& tp : pairing.types) {
        const auto* decl = tp.type->decl;
        // Comparable's operators, and a compare_to beside them, are the
        // inheritance check's findings (§5 JDK-interface rows).
        const bool comparable = std::any_of(tp.expected->functions.begin(), tp.expected->functions.end(),
                                            [](const auto& f) { return is_comparable_convention(f); });
        for (const auto* f : tp.missing) {
            if (is_comparable_convention(*f)) {
                continue;
            }
            if (f->java_member.empty()) {
                diag(decl->getLocation(), "%0 has no %1, the %2 of Java's %3 (cpp/CLAUDE.md §5)")
                    << decl << f->name << f->note << tp.java->binary_name;
            } else {
                diag(decl->getLocation(), "%0 has no %1 %2 translating Java's %3 (cpp/CLAUDE.md §4.1)")
                    << decl << what(*f) << spelled(*f) << f->java_member;
            }
        }
        for (const auto* f : tp.extra) {
            if (comparable && f->getIdentifier() != nullptr && f->getName() == "compare_to") {
                continue;
            }
            diag(f->getLocation(),
                 "%0 translates nothing of Java's %1: a public member is a Java member's "
                 "translation or a §5 convention (cpp/CLAUDE.md §1.4)")
                << f << tp.java->binary_name;
        }
        for (const auto* field : tp.fields) {
            diag(field->getLocation(),
                 "%0 is a public data member: no Java field stays a data member (cpp/CLAUDE.md §2.4, §12.3)")
                << field;
        }
        for (const auto& c : tp.expected->constants) {
            const auto it = std::find_if(tp.constants.begin(), tp.constants.end(), [&](const CppConstant& k) {
                return k.decl->getName() == c.name && k.qualifier == c.qualifier;
            });
            if (it == tp.constants.end()) {
                diag(decl->getLocation(), "%0 has no constant %1, Java's %2 (cpp/CLAUDE.md §2.4)")
                    << decl << c.name << c.java_member;
            } else if (!it->decl->isConstexpr()) {
                diag(it->decl->getLocation(), "%0 is Java's constant %1: it is constexpr (cpp/CLAUDE.md §2.4)")
                    << it->decl << c.java_member;
            }
        }
        for (const auto& k : tp.constants) {
            const bool expected = std::any_of(tp.expected->constants.begin(), tp.expected->constants.end(),
                                              [&](const auto& c) { return k.decl->getName() == c.name; });
            if (!expected) {
                diag(k.decl->getLocation(),
                     "%0 is a public variable translating no constant of Java's %1 "
                     "(cpp/CLAUDE.md §2.4, §12.3)")
                    << k.decl << tp.java->binary_name;
            }
        }
    }
    for (const auto* f : pairing.unowned) {
        diag(f->getLocation(),
             "%0 translates no Java member: a free function belongs to the type it takes first, or to an enum's "
             "statics namespace (cpp/CLAUDE.md §3.3)")
            << f;
    }
}

void JavaSignatureCheck::check(const MatchFinder::MatchResult& result) {
    const auto& java = JavaSide::get(settings());
    const auto pairing = pair_all(*result.Context, settings(), java);
    const auto have = decision_keys(java.decisions());
    for (const auto& [key, where] : needed_decisions(pairing, *result.Context)) {
        if (have.count(key) == 0) {
            diag(where->getLocation(),
                 "a non-default shape needs its line in tools/lint/data/decisions.txt: `%0 <source> <why>` "
                 "(cpp/CLAUDE.md §4.6)")
                << key;
        }
    }
    for (const auto& tp : pairing.types) {
        for (const auto& p : tp.pairs) {
            const auto& e = *p.expected;
            const auto* f = p.cpp;
            // An override's types are its base's, which is checked where it is
            // declared: `send(const int32_t&)` overrides `Producer<K, V>::send(const K&)`.
            const auto* method = llvm::dyn_cast<clang::CXXMethodDecl>(f);
            const bool overrides = method != nullptr && method->size_overridden_methods() > 0;
            for (std::size_t i = 0; i < e.parameters.size() && i < f->getNumParams(); ++i) {
                const auto& ep = e.parameters[i];
                const auto* param = f->getParamDecl(i);
                if (!ep.mapping.ok()) {
                    diag(param->getLocation(),
                         "Java's %0 has no shape in cpp/CLAUDE.md §5 (%1): it is not generated "
                         "until a row is decided")
                        << e.java_member << ep.mapping.unmapped;
                    continue;
                }
                const auto actual = read_type(param->getType(), *result.Context);
                if (!overrides && matching_shape(ep.mapping, actual) == nullptr) {
                    diag(param->getLocation(), "parameter %0 of %1 is %2; Java's %3 gives %4 (cpp/CLAUDE.md §5)")
                        << param << f << actual.to_string() << e.java_member << allowed(ep.mapping);
                }
                if (!ep.name.empty() && param->getName() != ep.name) {
                    diag(param->getLocation(), "parameter %0 of %1 is named %2 in Java (cpp/CLAUDE.md §2.3)")
                        << param << f << ep.name;
                }
                if (param->hasDefaultArg()) {
                    diag(param->getLocation(),
                         "parameter %0 has a default argument: a shorter Java overload is a C++ overload "
                         "(cpp/CLAUDE.md §4.1)")
                        << param;
                }
            }
            if (!llvm::isa<clang::CXXConstructorDecl>(f) && !e.returns.shapes.empty()) {
                const auto actual = read_type(f->getReturnType(), *result.Context);
                if (!overrides && matching_shape(e.returns, actual) == nullptr) {
                    diag(f->getLocation(), "%0 returns %1; Java's %2 gives %3 (cpp/CLAUDE.md §5, §4.7)")
                        << f << actual.to_string() << e.java_member << allowed(e.returns);
                }
            } else if (!e.returns.ok()) {
                diag(f->getLocation(), "Java's %0 has no shape in cpp/CLAUDE.md §5 (%1)")
                    << e.java_member << e.returns.unmapped;
            }
            if (const auto* m = llvm::dyn_cast<clang::CXXMethodDecl>(f); m != nullptr && e.qualifier.empty()) {
                const bool want_static =
                    e.kind == rules::FunctionKind::STATIC_METHOD || e.kind == rules::FunctionKind::CONSTANT_FUNCTION;
                if (!llvm::isa<clang::CXXConstructorDecl>(m) && m->isStatic() != want_static) {
                    diag(m->getLocation(), want_static ? "%0 is not static; Java's %1 is static (cpp/CLAUDE.md §2.4)"
                                                       : "%0 is static; Java's %1 is not (cpp/CLAUDE.md §2.4)")
                        << m << e.java_member;
                }
                if (e.is_pure && !m->isPureVirtual()) {
                    diag(m->getLocation(), "%0 translates Java's abstract %1: it is pure virtual (cpp/CLAUDE.md §5)")
                        << m << e.java_member;
                } else if (e.is_virtual && !m->isVirtual()) {
                    diag(m->getLocation(), "%0 translates Java's default method %1: it is virtual (cpp/CLAUDE.md §5)")
                        << m << e.java_member;
                }
            }
        }
    }
}

void SurfaceCheck::check(const MatchFinder::MatchResult& result) {
    const auto& java = JavaSide::get(settings());
    const auto& sm = *result.SourceManager;
    const auto where = main_file_start(sm);
    const auto pairing = pair_all(*result.Context, settings(), java);

    std::set<std::string> translated;
    for (const auto& t : pairing.cpp_types) {
        if (const auto* jt = java.by_cpp_name(t.qualified_name)) {
            translated.insert(jt->binary_name);
        }
    }
    std::set<std::string> missing;
    for (const auto& [name, reason] : java.scope().types) {
        if (translated.count(name) == 0) {
            missing.insert(name);
        }
    }
    if (Options.getLocalOrGlobal("ConfluentKafkaWriteBaseline", "false") == "true") {
        // `make -C cpp lint-baseline`: the baseline becomes what is missing now.
        std::ofstream out(settings().data_dir + "/baseline.txt");
        out << "# The in-scope Java classes not translated yet (cpp/CLAUDE.md §1.4), written by\n"
               "# `make -C cpp lint-baseline`. It only shrinks: a translated class's line goes.\n";
        for (const auto& name : missing) {
            out << "missing-class " << name << '\n';
        }
        return;
    }
    const auto& baseline = java.baseline();
    std::set<std::string> listed;
    for (const auto& line : baseline.lines) {
        listed.insert(line.fields[1]);
        if (translated.count(line.fields[1]) != 0) {
            diag(where, "%0:%1: %2 is translated: remove its baseline line")
                << baseline.path << static_cast<unsigned>(line.line) << line.fields[1];
        } else if (java.scope().types.count(line.fields[1]) == 0) {
            diag(where, "%0:%1: %2 is not in scope (cpp/CLAUDE.md §1.4): remove its baseline line")
                << baseline.path << static_cast<unsigned>(line.line) << line.fields[1];
        }
    }
    for (const auto& name : missing) {
        if (listed.count(name) == 0) {
            diag(where,
                 "Java's %0 is in scope (reached by %1) but not translated: translate it, or add "
                 "`missing-class %0` to tools/lint/data/baseline.txt (cpp/CLAUDE.md §1.4)")
                << name << java.scope().types.at(name);
        }
    }
    // A decisions.txt line nothing uses.
    std::set<std::string> needed;
    for (const auto& [key, decl] : needed_decisions(pairing, *result.Context)) {
        needed.insert(key);
    }
    const auto& decisions = java.decisions();
    for (const auto& line : decisions.lines) {
        const auto key = line.fields[0] + " " + line.fields[1] + " " + line.fields[2];
        if (needed.count(key) == 0) {
            diag(where, "%0:%1: no header needs `%2`: remove the line")
                << decisions.path << static_cast<unsigned>(line.line) << key;
        }
    }
    for (const auto* file : {&baseline, &decisions}) {
        for (const auto& error : file->errors) {
            diag(where, "%0") << error;
        }
    }
}

}  // namespace cklint::tidy
