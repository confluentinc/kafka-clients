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

// confluent-kafka-doc (cpp/CLAUDE.md §12.1, §12.2; root documentation.md §2-§6).

#include <algorithm>
#include <fstream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "checks.hpp"
#include "clang/AST/ASTContext.h"
#include "clang/AST/DeclCXX.h"
#include "clang/AST/DeclFriend.h"
#include "clang/AST/DeclTemplate.h"
#include "clang/Basic/SourceManager.h"
#include "doxygen.hpp"
#include "javadoc.hpp"
#include "names.hpp"
#include "pairing.hpp"
#include "public_decls.hpp"

namespace cklint::tidy {
namespace {

using clang::ast_matchers::MatchFinder;

/// What a documented declaration translates, when it translates something.
struct Origin {
    std::string marker;  ///< `<binary name>` or `<binary name>#<member>`
    const rules::ExpectedFunction* expected = nullptr;
    std::string title;  ///< its ledger title (§12.2)
};

std::string qualified(const clang::NamedDecl& decl) {
    std::string out;
    llvm::raw_string_ostream os(out);
    decl.printQualifiedName(os);
    return out;
}

/// The parameter list of a marker's member, `(int,String)`, or empty.
std::string java_parameters(const std::string& marker) {
    const auto open = marker.find('(', marker.find('#') == std::string::npos ? 0 : marker.find('#'));
    return open == std::string::npos ? std::string() : marker.substr(open);
}

std::string read_file(const std::string& path) {
    std::ifstream in(path);
    std::ostringstream text;
    text << in.rdbuf();
    return text.str();
}

}  // namespace

void DocCheck::check(const MatchFinder::MatchResult& result) {
    auto& context = *result.Context;
    const auto& sm = context.getSourceManager();
    const auto& java = JavaSide::get(settings());
    const auto pairing = pair_all(context, settings(), java);

    // What each declaration translates.
    std::map<const clang::Decl*, Origin> origins;
    for (const auto& tp : pairing.types) {
        origins[tp.type->decl] = Origin{tp.java->binary_name, nullptr, tp.type->qualified_name};
        for (const auto& p : tp.pairs) {
            origins[p.cpp] = Origin{p.expected->java_member, p.expected,
                                    qualified(*p.cpp) + java_parameters(p.expected->java_member)};
        }
        for (const auto& k : tp.constants) {
            for (const auto& c : tp.expected->constants) {
                if (k.decl->getName() == c.name && k.qualifier == c.qualifier) {
                    origins[k.decl] = Origin{c.java_member, nullptr, qualified(*k.decl)};
                }
            }
        }
        if (const auto* en = llvm::dyn_cast<clang::EnumDecl>(tp.type->decl)) {
            for (const auto* c : en->enumerators()) {
                origins[c] = Origin{tp.java->binary_name + "#" + c->getName().str(), nullptr, qualified(*c)};
            }
        }
    }

    const auto ledger = rules::parse_ledger(read_file(settings().ledger));
    std::map<std::string, const rules::LedgerEntry*> by_title;
    for (const auto& e : ledger.entries) {
        by_title[e.title] = &e;
    }
    std::set<std::string> warned;

    for (const auto* decl : public_declarations(context, settings())) {
        const auto* raw = context.getRawCommentForDeclNoCache(decl);
        if (raw == nullptr) {
            diag(decl->getLocation(), "%0 has no Doxygen comment (cpp/CLAUDE.md §12.1)") << decl;
            continue;
        }
        const auto text = rules::comment_text(raw->getRawText(sm));
        const auto doc = rules::parse_doxygen(text);
        for (const auto& leftover : rules::javadoc_leftovers(text)) {
            diag(decl->getLocation(), "%0's comment has %1 (cpp/CLAUDE.md §12.1)") << decl << leftover;
        }
        if (doc.notes.size() > 1) {
            diag(decl->getLocation(),
                 "%0's comment has %1 `@par Translation notes`; there is one, after Java's "
                 "contract (cpp/CLAUDE.md §12.1)")
                << decl << static_cast<unsigned>(doc.notes.size());
        }
        if (doc.warnings_outside_notes > 0) {
            diag(decl->getLocation(),
                 "%0's comment has a `WARNING:` outside `@par Translation notes` (cpp/CLAUDE.md §12.2)")
                << decl;
        }

        // §12.1: every parameter documented, nothing else; a value returned.
        const auto* function = llvm::dyn_cast<clang::FunctionDecl>(decl);
        if (function != nullptr) {
            std::set<std::string> names;
            for (const auto* p : function->parameters()) {
                if (!p->getName().empty()) {
                    names.insert(p->getName().str());
                    const bool documented = std::any_of(doc.params.begin(), doc.params.end(),
                                                        [&](const auto& e) { return e.name == p->getName(); });
                    if (!documented) {
                        diag(p->getLocation(), "parameter %0 of %1 has no @param (cpp/CLAUDE.md §12.1)")
                            << p << function;
                    }
                }
            }
            for (const auto& e : doc.params) {
                if (names.count(e.name) == 0) {
                    diag(decl->getLocation(), "%0's @param %1 names no parameter (cpp/CLAUDE.md §12.1)")
                        << decl << e.name;
                }
            }
            const bool returns_value = !llvm::isa<clang::CXXConstructorDecl>(function) &&
                                       !llvm::isa<clang::CXXConversionDecl>(function) &&
                                       !function->getReturnType()->isVoidType();
            if (returns_value && !doc.returns) {
                diag(decl->getLocation(), "%0 returns a value and has no @return (cpp/CLAUDE.md §12.1)") << decl;
            }
        }

        const auto origin = origins.find(decl);
        const auto javadocs = origin == origins.end() || origin->second.marker.empty()
                                  ? std::vector<std::string>{}
                                  : rules::followed_javadocs(java.model(), origin->second.marker);
        if (javadocs.empty()) {
            if (!doc.notes.empty()) {
                diag(decl->getLocation(),
                     "%0 translates nothing with a Javadoc, so its comment has no "
                     "`@par Translation notes` (root documentation.md §2)")
                    << decl;
            }
        } else {
            // The Javadoc's tags, its parameters named as in C++.
            std::vector<rules::JavaTags> tags;
            for (const auto& d : javadocs) {
                rules::JavaTags t;
                for (const auto& tag : rules::parse_javadoc(d).tags) {
                    if (tag.name == "param") {
                        std::string name = rules::snake_case(tag.arg);
                        if (origin->second.expected != nullptr) {
                            for (const auto& p : origin->second.expected->parameters) {
                                if (p.java_name == tag.arg) {
                                    name = p.name;
                                }
                            }
                        }
                        t.params.insert(name);
                    } else if (tag.name == "return") {
                        t.has_return = true;
                    } else if (tag.name == "throws" || tag.name == "exception") {
                        auto simple =
                            tag.arg.substr(tag.arg.rfind('.') == std::string::npos ? 0 : tag.arg.rfind('.') + 1);
                        t.throws.push_back(simple.substr(0, simple.find('<')));
                    }
                }
                tags.push_back(std::move(t));
            }
            // §12.1: `@throws XError` per Javadoc `@throws` (NullPointerException is
            // the one boundary rule, not per item, as in the Rust lint).
            for (const auto& thrown : tags.front().throws) {
                if (thrown == "NullPointerException") {
                    continue;
                }
                const auto wanted = rules::type_name(thrown);
                const bool documented = std::any_of(doc.throws.begin(), doc.throws.end(), [&](const auto& e) {
                    const auto colon = e.name.rfind(':');
                    return (colon == std::string::npos ? e.name : e.name.substr(colon + 1)) == wanted;
                });
                if (!documented) {
                    diag(decl->getLocation(),
                         "%0 has no `@throws %1` for the Javadoc's @throws %2 "
                         "(cpp/CLAUDE.md §12.1)")
                        << decl << wanted << thrown;
                }
            }
            // §12.1: at most 150% of the Javadoc's words, against the one it fits best.
            std::size_t best_own = 0;
            std::size_t best_java = 0;
            long best = 0;
            for (std::size_t i = 0; i < javadocs.size(); ++i) {
                const auto own = rules::doxygen_size_words(doc, tags[i]);
                const auto java_words = rules::word_count(rules::javadoc_markdown(javadocs[i]));
                const long score = static_cast<long>(own * 2) - static_cast<long>(java_words * 3);
                if (i == 0 || score < best) {
                    best = score;
                    best_own = own;
                    best_java = java_words;
                }
            }
            if (best_own > best_java * 3 / 2) {
                diag(decl->getLocation(),
                     "%0's comment has %1 words, more than 150%% of the Javadoc's %2 (at most "
                     "%3) (cpp/CLAUDE.md §12.1)")
                    << decl << static_cast<unsigned>(best_own) << static_cast<unsigned>(best_java)
                    << static_cast<unsigned>(best_java * 3 / 2);
            }
        }

        // §12.2: a WARNING needs an accepted ledger entry under its title.
        if (doc.warnings_in_notes > 0) {
            const auto title = origin != origins.end() ? origin->second.title : qualified(*decl);
            warned.insert(title);
            const auto entry = by_title.find(title);
            if (entry == by_title.end() || entry->second->accepted.empty()) {
                diag(decl->getLocation(),
                     entry == by_title.end()
                         ? "%0 changes Java's contract (`WARNING:`) with no entry `%1` in "
                           "cpp/design/current/doc-contract-changes.md (cpp/CLAUDE.md §12.2)"
                         : "%0 changes Java's contract (`WARNING:`); its ledger entry `%1` is not accepted yet "
                           "(cpp/CLAUDE.md §12.2)")
                    << decl << title;
            }
        }
    }

    const auto start = sm.getLocForStartOfFile(sm.getMainFileID());
    for (const auto& error : ledger.errors) {
        diag(start, "%0:%1 (root documentation.md §6)") << settings().ledger << error;
    }
    for (const auto& e : ledger.entries) {
        if (!e.accepted.empty() && warned.count(e.title) == 0) {
            diag(start,
                 "%0:%1: the accepted entry `%2` matches no item with a `WARNING:`; remove it "
                 "(root documentation.md §6)")
                << settings().ledger << static_cast<unsigned>(e.line) << e.title;
        }
    }
}

}  // namespace cklint::tidy
