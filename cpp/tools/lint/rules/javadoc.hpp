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

// The Javadoc side of the doc check (cpp/CLAUDE.md §12.1, root
// .claude/rules/documentation.md §3 and §5): what a Javadoc says and how many
// words it has, counted as `cargo xtask javadoc --words` counts them.
//
// The count is a port of rust/xtask/src/javadoc.rs (`parse`, `Translator`'s
// blocks and render, `word_count`) and java.rs (`clean_javadoc`,
// `overload_doc`), and of the pointer following of
// rust/xtask/src/lint_custom/api_docs.rs. The Rust translator also renames
// (`Exception` to `Error`, a link to a snake_cased path, `null` to `None`);
// a rename never changes a word count, so the port leaves them out. The
// parity test (`javadoc_words_parity`) holds the two counts equal.

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include "java_model.hpp"

namespace cklint::rules {

/// One block tag: `@param name text`, `@throws X text`, `@return text`, ...
struct BlockTag {
    std::string name;  ///< without its `@`
    std::string arg;   ///< the parameter of `@param`, the class of `@throws`; else empty
    std::string text;
};

/// A Javadoc split into its main description and its block tags.
struct ParsedJavadoc {
    std::string body;
    std::vector<BlockTag> tags;
};

/// The text of a `/** ... */` comment: the delimiters, each line's leading
/// `*` and one space after it removed, blank edge lines dropped.
std::string clean_javadoc(std::string_view comment);

/// Splits a cleaned Javadoc into its body and block tags; a block tag starts
/// a line, outside `<pre>`.
ParsedJavadoc parse_javadoc(std::string_view cleaned);

/// The Markdown a cleaned Javadoc translates to, structurally: paragraphs,
/// lists, headings, code blocks and tables from the HTML, inline tags
/// rendered, then sections for `@param`, `@return`, `@throws` and `@see`.
std::string javadoc_markdown(std::string_view cleaned);

/// The words of a Markdown text: runs of letters, digits and `_` after links
/// are reduced to their text, a code span counting as one; heading and fence
/// lines left out.
std::size_t word_count(std::string_view markdown);

/// `own` with each `{@inheritDoc}` replaced by `inherited`'s matching part,
/// and the `@param`, `@return` and `@throws` it leaves out inherited.
std::string inherit_doc(std::string_view own, std::string_view inherited);

/// The references of a Javadoc that only points elsewhere (its text is "See"
/// and links, its tags `@see` and `@since`), else empty.
std::vector<std::string> pointer_refs(std::string_view cleaned);

/// The cleaned Javadocs a doc translating `marker` is measured against: the
/// member's (or the class's, for a marker without `#`), `{@inheritDoc}`
/// resolved from the supertypes, a missing one inherited, and a pointer-only
/// one replaced by what it points to. `marker` is `<binary name>` or
/// `<binary name>#<signature or name>`.
std::vector<std::string> followed_javadocs(const java::Model& model, std::string_view marker);

/// The word count of each of `followed_javadocs(model, marker)`.
std::vector<std::size_t> javadoc_words(const java::Model& model, std::string_view marker);

}  // namespace cklint::rules
