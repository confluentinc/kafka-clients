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

#pragma once

// The confluent-kafka-* checks. Each matches the translation unit once and
// compares the binding's public types with what cpp/CLAUDE.md derives from
// the Java sources; they run on the ABI probe, which includes every header.

#include "clang-tidy/ClangTidyCheck.h"
#include "session.hpp"

namespace cklint::tidy {

/// The base of every check: a C++20 translation unit, matched once.
class TranslationUnitCheck : public clang::tidy::ClangTidyCheck {
public:
    TranslationUnitCheck(llvm::StringRef name, clang::tidy::ClangTidyContext* context)
        : ClangTidyCheck(name, context), settings_(Settings::of(Options)) {}

    bool isLanguageVersionSupported(const clang::LangOptions& options) const override { return options.CPlusPlus20; }
    void registerMatchers(clang::ast_matchers::MatchFinder* finder) override;

protected:
    const Settings& settings() const { return settings_; }

private:
    Settings settings_;
};

/// confluent-kafka-java-class: a public type translates a Java class that is
/// part of Java's public API and not deprecated, lives in the header §3.1
/// gives it, and has the kind §3.3 / §5 give its Java kind.
class JavaClassCheck : public TranslationUnitCheck {
public:
    using TranslationUnitCheck::TranslationUnitCheck;
    void check(const clang::ast_matchers::MatchFinder::MatchResult& result) override;
};

/// confluent-kafka-java-inheritance: a class's public bases are the
/// translations of Java's supertypes, and the JDK interfaces have their §5 row.
class JavaInheritanceCheck : public TranslationUnitCheck {
public:
    using TranslationUnitCheck::TranslationUnitCheck;
    void check(const clang::ast_matchers::MatchFinder::MatchResult& result) override;
};

/// confluent-kafka-java-enum: an enum's underlying type, enumerators, their
/// order and values are Java's (§3.3).
class JavaEnumCheck : public TranslationUnitCheck {
public:
    using TranslationUnitCheck::TranslationUnitCheck;
    void check(const clang::ast_matchers::MatchFinder::MatchResult& result) override;
};

/// confluent-kafka-java-members: every public Java member has its C++
/// translation and every public C++ member translates one (§1.4, §4.1); no
/// public data member (§2.4, §12.3); Java's constants are constexpr.
class JavaMembersCheck : public TranslationUnitCheck {
public:
    using TranslationUnitCheck::TranslationUnitCheck;
    void check(const clang::ast_matchers::MatchFinder::MatchResult& result) override;
};

/// confluent-kafka-java-signature: a translation's parameter and return types
/// are shapes §5 allows, a non-default one with its decisions.txt line (§4.6);
/// parameters keep Java's names (§2.3) and have no default (§4.1); static and
/// virtual as Java's.
class JavaSignatureCheck : public TranslationUnitCheck {
public:
    using TranslationUnitCheck::TranslationUnitCheck;
    void check(const clang::ast_matchers::MatchFinder::MatchResult& result) override;
};

/// confluent-kafka-surface: every in-scope Java class is translated or listed
/// in baseline.txt; no data-file line is stale. With the option
/// `ConfluentKafkaWriteBaseline: true` it writes baseline.txt instead.
class SurfaceCheck : public TranslationUnitCheck {
public:
    using TranslationUnitCheck::TranslationUnitCheck;
    void check(const clang::ast_matchers::MatchFinder::MatchResult& result) override;
};

/// confluent-kafka-doc: every public declaration has a Doxygen comment with
/// an `@param` per parameter (and none other) and an `@return` for a value;
/// one translating a Java member has an `@throws` per Javadoc `@throws`, at
/// most 150% of the Javadoc's words, at most one `@par Translation notes`
/// and none without a Javadoc, a `WARNING:` only in the notes and only with
/// an accepted ledger entry, and no Java file:line reference, inline tag or
/// HTML left (cpp/CLAUDE.md §12.1, §12.2).
class DocCheck : public TranslationUnitCheck {
public:
    using TranslationUnitCheck::TranslationUnitCheck;
    void check(const clang::ast_matchers::MatchFinder::MatchResult& result) override;
};

/// confluent-kafka-abi: no construct that freezes a later change (§12.3): a
/// class with data holds one pointer and is not passed in registers nor an
/// aggregate; no bit-field, virtual inheritance or unscoped enum, an enum's
/// type written; a virtual only in an interface translation, never an
/// overloaded virtual name.
class AbiCheck : public TranslationUnitCheck {
public:
    using TranslationUnitCheck::TranslationUnitCheck;
    void check(const clang::ast_matchers::MatchFinder::MatchResult& result) override;
};

/// confluent-kafka-library: a public header defines only a constexpr or
/// deleted function, a template or an interface's constructor; every class and
/// free function the library defines is marked CONFLUENT_KAFKA_CPP_API (§3.2).
class LibraryCheck : public TranslationUnitCheck {
public:
    using TranslationUnitCheck::TranslationUnitCheck;
    void registerMatchers(clang::ast_matchers::MatchFinder* finder) override;
    void check(const clang::ast_matchers::MatchFinder::MatchResult& result) override;
};

/// confluent-kafka-source: the library's sources (src/, run on each of them,
/// not on the probe) call no _cb function (§1.3).
class SourceCheck : public TranslationUnitCheck {
public:
    using TranslationUnitCheck::TranslationUnitCheck;
    void registerMatchers(clang::ast_matchers::MatchFinder* finder) override;
    void check(const clang::ast_matchers::MatchFinder::MatchResult& result) override;
};

/// confluent-kafka-inventory: writes the API inventory to the file named by
/// the option ConfluentKafkaInventoryFile, for `make -C cpp abi-check`
/// (§12.4); reports nothing, and does nothing without the option.
class InventoryCheck : public TranslationUnitCheck {
public:
    using TranslationUnitCheck::TranslationUnitCheck;
    void check(const clang::ast_matchers::MatchFinder::MatchResult& result) override;
};

/// confluent-kafka-header: every public header has the Apache 2.0 header and
/// `#pragma once`, never includes the C header or src/detail/ffi.hpp, is
/// included by its module header, and has no TODO, FIXME or silenced check.
class HeaderCheck : public TranslationUnitCheck {
public:
    using TranslationUnitCheck::TranslationUnitCheck;
    void check(const clang::ast_matchers::MatchFinder::MatchResult& result) override;
};

}  // namespace cklint::tidy
