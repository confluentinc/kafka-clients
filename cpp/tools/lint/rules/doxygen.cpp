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

#include "doxygen.hpp"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <regex>
#include <set>

#include "javadoc.hpp"
#include "names.hpp"

namespace cklint::rules {
namespace {

bool is_space(char c) {
    return c == ' ' || c == '\t' || c == '\r';
}

std::string_view trim(std::string_view s) {
    while (!s.empty() && is_space(s.front())) {
        s.remove_prefix(1);
    }
    while (!s.empty() && is_space(s.back())) {
        s.remove_suffix(1);
    }
    return s;
}

bool starts_with(std::string_view s, std::string_view p) {
    return s.substr(0, p.size()) == p;
}

std::vector<std::string_view> lines_of(std::string_view s) {
    std::vector<std::string_view> out;
    std::size_t start = 0;
    while (start <= s.size()) {
        const auto nl = s.find('\n', start);
        out.push_back(s.substr(start, nl == std::string_view::npos ? std::string_view::npos : nl - start));
        if (nl == std::string_view::npos) {
            break;
        }
        start = nl + 1;
    }
    return out;
}

/// The command a line starts with (`param` for `@param` or `\param`), or empty.
std::string_view command_of(std::string_view trimmed) {
    if (trimmed.size() < 2 || (trimmed[0] != '@' && trimmed[0] != '\\') ||
        std::isalpha(static_cast<unsigned char>(trimmed[1])) == 0) {
        return {};
    }
    std::size_t end = 1;
    while (end < trimmed.size() && std::isalpha(static_cast<unsigned char>(trimmed[end])) != 0) {
        ++end;
    }
    return trimmed.substr(1, end - 1);
}

std::pair<std::string_view, std::string_view> split_word(std::string_view s) {
    s = trim(s);
    const auto end = s.find_first_of(" \t");
    if (end == std::string_view::npos) {
        return {s, {}};
    }
    return {s.substr(0, end), trim(s.substr(end))};
}

bool is_warning(std::string_view trimmed) {
    while (!trimmed.empty() && (trimmed.front() == '-' || trimmed.front() == '*' || trimmed.front() == ' ')) {
        trimmed.remove_prefix(1);
    }
    return starts_with(trimmed, "WARNING:");
}

}  // namespace

std::string comment_text(std::string_view raw) {
    std::string out;
    bool block = false;
    for (auto line : lines_of(raw)) {
        line = trim(line);
        if (starts_with(line, "///") || starts_with(line, "//!")) {
            line.remove_prefix(3);
        } else if (starts_with(line, "/**") || starts_with(line, "/*!")) {
            line.remove_prefix(3);
            block = true;
        } else if (block && starts_with(line, "*") && !starts_with(line, "*/")) {
            line.remove_prefix(1);
        }
        if (block && line.size() >= 2 && line.substr(line.size() - 2) == "*/") {
            line.remove_suffix(2);
            block = false;
        }
        if (starts_with(line, " ")) {
            line.remove_prefix(1);
        }
        out += std::string(line);
        out += '\n';
    }
    while (!out.empty() && (out.back() == '\n' || is_space(out.back()))) {
        out.pop_back();
    }
    return out;
}

DoxygenDoc parse_doxygen(std::string_view text) {
    DoxygenDoc doc;
    enum class Into : std::uint8_t { DESCRIPTION, PARAM, RETURN, THROWS, NOTES };
    Into into = Into::DESCRIPTION;
    bool fence = false;
    const auto append = [](std::string& to, std::string_view line) {
        if (!to.empty()) {
            to += '\n';
        }
        to += line;
    };
    for (const auto line : lines_of(text)) {
        const auto trimmed = trim(line);
        if (starts_with(trimmed, "```")) {
            fence = !fence;
        } else if (starts_with(trimmed, "@code") || starts_with(trimmed, "\\code")) {
            fence = true;
        } else if (starts_with(trimmed, "@endcode") || starts_with(trimmed, "\\endcode")) {
            fence = false;
        }
        const auto command = fence ? std::string_view{} : command_of(trimmed);
        if (!fence && is_warning(trimmed)) {
            (into == Into::NOTES ? doc.warnings_in_notes : doc.warnings_outside_notes) += 1;
        }
        if (command == "param" || command == "tparam") {
            auto rest = trim(trimmed.substr(command.size() + 1));
            if (starts_with(rest, "[")) {
                rest = trim(rest.substr(rest.find(']') == std::string_view::npos ? rest.size() : rest.find(']') + 1));
            }
            const auto [name, body] = split_word(rest);
            if (command == "param") {
                doc.params.push_back({std::string(name), std::string(body)});
                into = Into::PARAM;
            } else {
                append(doc.description, line);
                into = Into::DESCRIPTION;
            }
            continue;
        }
        if (command == "return" || command == "returns") {
            doc.returns = std::string(trim(trimmed.substr(command.size() + 1)));
            into = Into::RETURN;
            continue;
        }
        if (command == "throws" || command == "throw" || command == "exception") {
            const auto [name, body] = split_word(trimmed.substr(command.size() + 1));
            doc.throws.push_back({std::string(name), std::string(body)});
            into = Into::THROWS;
            continue;
        }
        if (command == "par") {
            const auto title = trim(trimmed.substr(4));
            if (title == "Translation notes") {
                doc.notes.emplace_back();
                into = Into::NOTES;
            } else {
                append(doc.description, title);
                into = Into::DESCRIPTION;
            }
            continue;
        }
        if (!command.empty()) {
            // @note, @see, ...: Java's description.
            append(doc.description, trimmed.substr(command.size() + 1));
            into = Into::DESCRIPTION;
            continue;
        }
        if (trimmed.empty() && !fence && into != Into::DESCRIPTION) {
            // A blank line ends a command's paragraph.
            into = Into::DESCRIPTION;
            continue;
        }
        switch (into) {
            case Into::DESCRIPTION:
                append(doc.description, line);
                break;
            case Into::PARAM:
                append(doc.params.back().text, trimmed);
                break;
            case Into::RETURN:
                append(*doc.returns, trimmed);
                break;
            case Into::THROWS:
                append(doc.throws.back().text, trimmed);
                break;
            case Into::NOTES:
                append(doc.notes.back(), line);
                break;
        }
    }
    return doc;
}

std::size_t doxygen_size_words(const DoxygenDoc& doc, const JavaTags& tags) {
    std::size_t words = word_count(doc.description);
    for (const auto& n : doc.notes) {
        words += word_count(n);
    }
    for (const auto& p : doc.params) {
        if (tags.params.count(p.name) != 0) {
            words += 1 + word_count(p.text);  // the name, as Rust's `- `name` - text` entry
        }
    }
    if (doc.returns && tags.has_return) {
        words += word_count(*doc.returns);
    }
    for (const auto& t : doc.throws) {
        const auto simple = t.name.substr(t.name.rfind(':') == std::string::npos ? 0 : t.name.rfind(':') + 1);
        const bool java = std::any_of(tags.throws.begin(), tags.throws.end(),
                                      [&](const std::string& j) { return type_name(j) == simple; });
        if (java) {
            words += 1 + word_count(t.text);
        }
    }
    return words;
}

std::vector<std::string> javadoc_leftovers(std::string_view text) {
    std::vector<std::string> out;
    static const std::regex java_line(R"([A-Za-z0-9_]+\.java:\d+)");
    static const std::regex short_line(R"(`:\d+`)");
    static const std::regex inline_tag(R"(\{@(link|linkplain|code|literal|value|inheritDoc)\b)");
    static const std::regex html(
        R"(</?(p|b|i|em|strong|code|tt|pre|ul|ol|li|br|a|table|tr|td|th|h[1-6]|dl|dt|dd|div|blockquote)\b[^>]*>)",
        std::regex::icase);
    const std::string s(text);
    std::smatch m;
    if (std::regex_search(s, m, java_line) || std::regex_search(s, m, short_line)) {
        out.push_back("a Java file:line reference (" + m.str() + "); name the member instead");
    }
    if (std::regex_search(s, m, inline_tag)) {
        out.push_back("a Javadoc inline tag (" + m.str() + "); translate it");
    }
    if (std::regex_search(s, m, html)) {
        out.push_back("an HTML tag (" + m.str() + "); translate it");
    }
    return out;
}

Ledger parse_ledger(std::string_view text) {
    Ledger ledger;
    static const std::regex title(R"(^\d+\.\s+`([^`]+)`\s*$)");
    static const std::regex field(R"(^\s+-\s+(Java member|Contract change|Why|Accepted):\s*(.*)$)");
    static const char* const order[] = {"Java member", "Contract change", "Why", "Accepted"};
    bool entries = false;
    std::size_t number = 0;
    std::size_t next_field = 4;
    std::set<std::string> titles;
    const auto close = [&] {
        if (!ledger.entries.empty() && next_field < 4) {
            ledger.errors.push_back(std::to_string(ledger.entries.back().line) + ": `" + ledger.entries.back().title +
                                    "` lacks `- " + order[next_field] + ":`");
        }
    };
    for (const auto line : lines_of(text)) {
        ++number;
        if (starts_with(line, "## ")) {
            close();
            next_field = 4;
            entries = trim(line.substr(3)) == "Entries";
            continue;
        }
        if (!entries || trim(line).empty()) {
            continue;
        }
        const std::string l(line);
        std::smatch m;
        if (std::regex_match(l, m, title)) {
            close();
            LedgerEntry e;
            e.title = m[1];
            e.line = number;
            if (!titles.insert(e.title).second) {
                ledger.errors.push_back(std::to_string(number) + ": `" + e.title + "` is listed twice");
            }
            ledger.entries.push_back(std::move(e));
            next_field = 0;
        } else if (std::regex_match(l, m, field) && !ledger.entries.empty()) {
            if (next_field >= 4 || m[1] != order[next_field]) {
                ledger.errors.push_back(std::to_string(number) + ": `- " + m[1].str() +
                                        ":` is out of order; the "
                                        "sub-items are Java member, Contract change, Why, Accepted");
                continue;
            }
            auto& e = ledger.entries.back();
            auto value = std::string(trim(m[2].str()));
            if (next_field == 0) {
                if (value.size() >= 2 && value.front() == '`' && value.back() == '`') {
                    value = value.substr(1, value.size() - 2);
                }
                e.java_member = value;
            } else if (next_field == 1) {
                e.change = value;
            } else if (next_field == 2) {
                e.why = value;
            } else {
                e.accepted = value;
            }
            ++next_field;
        } else if (trim(line) != "(none yet)") {
            ledger.errors.push_back(std::to_string(number) + ": `" + std::string(trim(line)) +
                                    "` is neither an entry title nor one of its four sub-items");
        }
    }
    close();
    return ledger;
}

}  // namespace cklint::rules
