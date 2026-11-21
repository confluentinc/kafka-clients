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

// The C++ side of the doc check (cpp/CLAUDE.md §12.1): a header's Doxygen
// comment split into what root .claude/rules/documentation.md §2 and §5 talk
// about, and the words of it that count against the Javadoc's.

#include <cstddef>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <vector>

namespace cklint::rules {

/// One `@param`, `@throws` entry: what it names and its text.
struct DoxygenEntry {
    std::string name;
    std::string text;
};

/// A Doxygen comment, split.
struct DoxygenDoc {
    /// The text that is no block command, and every `@par` but the notes:
    /// Java's description.
    std::string description;
    std::vector<DoxygenEntry> params;
    std::optional<std::string> returns;
    std::vector<DoxygenEntry> throws;
    /// The text of each `@par Translation notes`.
    std::vector<std::string> notes;
    /// `WARNING:` lines inside and outside the notes.
    std::size_t warnings_in_notes = 0;
    std::size_t warnings_outside_notes = 0;
};

/// The text of a comment as written (`///` lines, a `/** */` block), its
/// comment markers removed.
std::string comment_text(std::string_view raw);

/// Splits a comment's text (`@` or `\` commands).
DoxygenDoc parse_doxygen(std::string_view text);

/// What a Javadoc documents, for the words that count against it.
struct JavaTags {
    /// The C++ names of the parameters Java has an `@param` for.
    std::set<std::string> params;
    bool has_return = false;
    /// The simple names of the classes of Java's `@throws`.
    std::vector<std::string> throws;
};

/// The words of `doc` that count against a Javadoc with `tags`
/// (documentation.md §5): the description, the notes, each `@param` Java
/// documents, `@return` when Java has one, each `@throws` naming a Java throw.
std::size_t doxygen_size_words(const DoxygenDoc& doc, const JavaTags& tags);

/// The leftovers a translated doc must not keep: a Java file:line reference,
/// a Javadoc inline tag, an HTML tag. Each is a short description.
std::vector<std::string> javadoc_leftovers(std::string_view text);

/// One entry of the contract-change ledger (cpp/CLAUDE.md §12.2, root
/// documentation.md §6).
struct LedgerEntry {
    std::string title;  ///< the C++ name plus Java's parameter list
    std::string java_member;
    std::string change;
    std::string why;
    std::string accepted;  ///< empty: proposed
    std::size_t line = 0;
};

struct Ledger {
    std::vector<LedgerEntry> entries;
    /// `<line>: <problem>` for each malformed entry.
    std::vector<std::string> errors;
};

/// The entries under the ledger's `## Entries` heading.
Ledger parse_ledger(std::string_view text);

}  // namespace cklint::rules
