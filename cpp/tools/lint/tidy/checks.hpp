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

/// confluent-kafka-header: every public header has the Apache 2.0 header and
/// `#pragma once`, reaches the C header only through detail/ffi.hpp, is
/// included by its module header, and has no TODO, FIXME or silenced check.
class HeaderCheck : public TranslationUnitCheck {
public:
    using TranslationUnitCheck::TranslationUnitCheck;
    void check(const clang::ast_matchers::MatchFinder::MatchResult& result) override;
};

}  // namespace cklint::tidy
