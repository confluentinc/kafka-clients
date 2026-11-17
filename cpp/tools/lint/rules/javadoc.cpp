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

#include "javadoc.hpp"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <locale>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <utility>

namespace cklint::rules {
namespace {

// ---- text helpers (Rust's str methods, on bytes)

bool is_space(char c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v';
}

/// Rust's `char::is_alphanumeric`, or `_`: Unicode letters and digits, read
/// through a local C.UTF-8 facet so the process locale is left alone. Where
/// that locale is missing, a non-ASCII character counts as a letter.
bool is_word_char(char32_t c) {
    if (c < 0x80) {
        return std::isalnum(static_cast<int>(c)) != 0 || c == U'_';
    }
    static const std::ctype<wchar_t>* const facet = []() -> const std::ctype<wchar_t>* {
        try {
            static const std::locale utf8("C.UTF-8");
            return &std::use_facet<std::ctype<wchar_t>>(utf8);
        } catch (const std::runtime_error&) {
            return nullptr;
        }
    }();
    return facet == nullptr || facet->is(std::ctype_base::alnum, static_cast<wchar_t>(c));
}

/// The code point of the UTF-8 sequence at `s[i]`, advancing `i` past it; a
/// malformed byte is returned as itself.
char32_t next_code_point(std::string_view s, std::size_t& i) {
    const auto b0 = static_cast<unsigned char>(s[i]);
    const std::size_t length = b0 < 0x80 ? 1 : (b0 >> 5) == 0x6 ? 2 : (b0 >> 4) == 0xE ? 3 : (b0 >> 3) == 0x1E ? 4 : 1;
    if (length == 1 || i + length > s.size()) {
        ++i;
        return b0;
    }
    char32_t c = length == 2 ? (b0 & 0x1FU) : length == 3 ? (b0 & 0x0FU) : (b0 & 0x07U);
    for (std::size_t k = 1; k < length; ++k) {
        c = (c << 6U) | (static_cast<unsigned char>(s[i + k]) & 0x3FU);
    }
    i += length;
    return c;
}

std::string_view trim_start(std::string_view s) {
    while (!s.empty() && is_space(s.front())) {
        s.remove_prefix(1);
    }
    return s;
}

std::string_view trim_end(std::string_view s) {
    while (!s.empty() && is_space(s.back())) {
        s.remove_suffix(1);
    }
    return s;
}

std::string_view trim(std::string_view s) {
    return trim_end(trim_start(s));
}

bool starts_with(std::string_view s, std::string_view p) {
    return s.substr(0, p.size()) == p;
}

std::string lower(std::string_view s) {
    std::string out(s);
    std::transform(out.begin(), out.end(), out.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return out;
}

/// Rust's `str::lines`: split at `\n`, a trailing `\r` dropped, no final empty line.
std::vector<std::string_view> lines_of(std::string_view s) {
    std::vector<std::string_view> out;
    while (!s.empty()) {
        const auto nl = s.find('\n');
        auto line = s.substr(0, nl);
        if (!line.empty() && line.back() == '\r') {
            line.remove_suffix(1);
        }
        out.push_back(line);
        if (nl == std::string_view::npos) {
            break;
        }
        s.remove_prefix(nl + 1);
    }
    return out;
}

std::string join(const std::vector<std::string>& parts, std::string_view sep) {
    std::string out;
    for (std::size_t i = 0; i < parts.size(); ++i) {
        if (i > 0) {
            out += sep;
        }
        out += parts[i];
    }
    return out;
}

std::size_t count(std::string_view s, std::string_view what) {
    std::size_t n = 0;
    for (auto at = s.find(what); at != std::string_view::npos; at = s.find(what, at + what.size())) {
        ++n;
    }
    return n;
}

/// `s` with its whitespace runs collapsed to one space, trimmed.
std::string collapse(std::string_view s) {
    std::string out;
    bool space = false;
    for (const char c : s) {
        if (is_space(c)) {
            space = true;
            continue;
        }
        if (space && !out.empty()) {
            out += ' ';
        }
        space = false;
        out += c;
    }
    return out;
}

/// The first whitespace-separated word of `s` and the rest, trimmed.
std::pair<std::string_view, std::string_view> split_word(std::string_view s) {
    s = trim_start(s);
    const auto end = std::find_if(s.begin(), s.end(), is_space);
    if (end == s.end()) {
        return {s, {}};
    }
    const auto at = static_cast<std::size_t>(end - s.begin());
    return {s.substr(0, at), trim(s.substr(at))};
}

void append_utf8(std::string& out, std::uint32_t code) {
    if (code < 0x80) {
        out += static_cast<char>(code);
    } else if (code < 0x800) {
        out += static_cast<char>(0xC0 | (code >> 6));
        out += static_cast<char>(0x80 | (code & 0x3F));
    } else if (code < 0x10000) {
        out += static_cast<char>(0xE0 | (code >> 12));
        out += static_cast<char>(0x80 | ((code >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (code & 0x3F));
    } else {
        out += static_cast<char>(0xF0 | (code >> 18));
        out += static_cast<char>(0x80 | ((code >> 12) & 0x3F));
        out += static_cast<char>(0x80 | ((code >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (code & 0x3F));
    }
}

// ---- inline tags, HTML, entities

struct InlineTag {
    std::string_view tag;
    std::string_view content;
    std::size_t end;  ///< just past the closing brace
};

/// The inline tag `{@tag content}` at `start`; braces inside nest.
std::optional<InlineTag> inline_tag(std::string_view text, std::size_t start) {
    if (!starts_with(text.substr(start), "{@")) {
        return std::nullopt;
    }
    const auto rest = text.substr(start + 2);
    int depth = 1;
    std::size_t end = std::string_view::npos;
    for (std::size_t k = 0; k < rest.size(); ++k) {
        if (rest[k] == '{') {
            ++depth;
        } else if (rest[k] == '}' && --depth == 0) {
            end = k;
            break;
        }
    }
    if (end == std::string_view::npos) {
        return std::nullopt;
    }
    const auto inside = rest.substr(0, end);
    const auto ws = std::find_if(inside.begin(), inside.end(), is_space);
    const auto at = static_cast<std::size_t>(ws - inside.begin());
    return InlineTag{inside.substr(0, at), inside.substr(at), start + 2 + end + 1};
}

/// A `{@link}` content split into its reference and label; the reference ends
/// at the first space outside parentheses.
std::pair<std::string_view, std::string_view> split_link(std::string_view content) {
    content = trim(content);
    int depth = 0;
    for (std::size_t k = 0; k < content.size(); ++k) {
        const char c = content[k];
        if (c == '(') {
            ++depth;
        } else if (c == ')') {
            --depth;
        } else if (is_space(c) && depth == 0) {
            return {content.substr(0, k), trim(content.substr(k))};
        }
    }
    return {content, {}};
}

struct HtmlTag {
    std::string name;
    bool closing = false;
    std::size_t length = 0;
};

/// The HTML tag at the start of `s`, or nothing when `<` starts none (`a < b`).
std::optional<HtmlTag> html_tag(std::string_view s) {
    if (!starts_with(s, "<")) {
        return std::nullopt;
    }
    const auto inner = s.substr(1);
    const bool closing = starts_with(inner, "/");
    const auto name_start = closing ? inner.substr(1) : inner;
    const bool alpha = !name_start.empty() && std::isalpha(static_cast<unsigned char>(name_start[0])) != 0;
    if (!alpha && !starts_with(inner, "!")) {
        return std::nullopt;
    }
    const auto end = s.find('>');
    if (end == std::string_view::npos) {
        return std::nullopt;
    }
    std::string name;
    for (const char c : name_start) {
        if (std::isalnum(static_cast<unsigned char>(c)) == 0) {
            break;
        }
        name += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return HtmlTag{name, closing, end + 1};
}

/// The HTML entity at the start of `s`, decoded, and its length.
std::optional<std::pair<std::string, std::size_t>> entity(std::string_view s) {
    const auto end = s.substr(0, std::min<std::size_t>(10, s.size())).find(';');
    if (end == std::string_view::npos || end < 1) {
        return std::nullopt;
    }
    const auto name = s.substr(1, end - 1);
    std::string decoded;
    if (name == "lt") {
        decoded = "<";
    } else if (name == "gt") {
        decoded = ">";
    } else if (name == "amp") {
        decoded = "&";
    } else if (name == "quot") {
        decoded = "\"";
    } else if (name == "apos") {
        decoded = "'";
    } else if (name == "nbsp") {
        decoded = " ";
    } else if (starts_with(name, "#")) {
        auto digits = name.substr(1);
        int base = 10;
        if (starts_with(digits, "x") || starts_with(digits, "X")) {
            digits.remove_prefix(1);
            base = 16;
        }
        if (digits.empty()) {
            return std::nullopt;
        }
        std::uint32_t code = 0;
        for (const char c : digits) {
            const int v = std::isdigit(static_cast<unsigned char>(c)) != 0 ? c - '0'
                          : base == 16 && std::isxdigit(static_cast<unsigned char>(c)) != 0
                              ? std::tolower(static_cast<unsigned char>(c)) - 'a' + 10
                              : -1;
            if (v < 0) {
                return std::nullopt;
            }
            code = code * static_cast<std::uint32_t>(base) + static_cast<std::uint32_t>(v);
            if (code > 0x10FFFF) {
                return std::nullopt;
            }
        }
        if (code >= 0xD800 && code <= 0xDFFF) {
            return std::nullopt;
        }
        append_utf8(decoded, code);
    } else {
        return std::nullopt;
    }
    return std::make_pair(decoded, end + 1);
}

/// The lines of a `<pre>` block: a `{@code ..}` wrapper unwrapped, entities
/// decoded, the common indentation and blank edges removed.
std::string pre_text(std::string_view raw) {
    std::string text(trim(raw));
    if (starts_with(text, "{@code")) {
        const auto t = trim_end(std::string_view(text).substr(6));
        if (!t.empty() && t.back() == '}') {
            text = std::string(t.substr(0, t.size() - 1));
        }
    }
    std::string decoded;
    for (std::size_t i = 0; i < text.size();) {
        if (text[i] == '&') {
            if (const auto e = entity(std::string_view(text).substr(i))) {
                decoded += e->first;
                i += e->second;
                continue;
            }
        }
        decoded += text[i++];
    }
    const auto lines = lines_of(decoded);
    std::size_t indent = std::string_view::npos;
    for (const auto l : lines) {
        if (!trim(l).empty()) {
            indent = std::min(indent, l.size() - trim_start(l).size());
        }
    }
    if (indent == std::string_view::npos) {
        indent = 0;
    }
    std::vector<std::string> kept;
    for (const auto l : lines) {
        kept.emplace_back(l.size() >= indent ? trim_end(l.substr(indent)) : std::string_view{});
    }
    std::size_t first = 0;
    while (first < kept.size() && kept[first].empty()) {
        ++first;
    }
    std::size_t last = kept.size();
    while (last > first && kept[last - 1].empty()) {
        --last;
    }
    return join(std::vector<std::string>(kept.begin() + static_cast<std::ptrdiff_t>(first),
                                         kept.begin() + static_cast<std::ptrdiff_t>(last)),
                "\n");
}

std::string code_span(std::string_view code) {
    if (code.find('`') != std::string_view::npos) {
        return "`` " + std::string(code) + " ``";
    }
    return "`" + std::string(code) + "`";
}

// ---- blocks and Markdown

enum class BlockKind : std::uint8_t { PARA, ITEM, HEADING, CODE, TABLE };

struct Block {
    BlockKind kind = BlockKind::PARA;
    std::string marker;  ///< an item's `- ` or `1. `
    std::string text;
    std::vector<std::vector<std::string>> rows;  ///< a table's, the first the header
};

std::vector<Block> blocks(std::string_view text);

std::string inline_text(std::string_view text) {
    std::vector<std::string> parts;
    for (const auto& b : blocks(text)) {
        switch (b.kind) {
            case BlockKind::PARA:
            case BlockKind::HEADING:
                parts.push_back(b.text);
                break;
            case BlockKind::ITEM:
                parts.push_back(b.marker + b.text);
                break;
            case BlockKind::CODE:
                parts.push_back("`" + collapse(b.text) + "`");
                break;
            case BlockKind::TABLE: {
                std::vector<std::string> rows;
                for (const auto& r : b.rows) {
                    rows.push_back(join(r, " | "));
                }
                parts.push_back(join(rows, "; "));
                break;
            }
        }
    }
    return join(parts, " ");
}

/// A Java reference as the Rust translator prints it, minus its renames: one
/// code span, never empty (an empty class is `Self`).
std::string reference_span(std::string_view reference) {
    return "`" + (reference.empty() ? std::string("Self") : std::string(reference)) + "`";
}

std::string render_inline_tag(std::string_view tag, std::string_view content) {
    if (tag == "code") {
        auto c = trim(content);
        if (starts_with(c, "#")) {
            c.remove_prefix(1);
        }
        return code_span(c);
    }
    if (tag == "literal") {
        return std::string(trim(content));
    }
    if (tag == "link" || tag == "linkplain") {
        const auto [reference, label] = split_link(content);
        return label.empty() ? reference_span(reference) : inline_text(label);
    }
    if (tag == "value") {
        const auto reference = trim(content);
        return reference.empty() ? std::string() : reference_span(reference);
    }
    if (tag == "inheritDoc" || tag == "docRoot") {
        return {};
    }
    return std::string(trim(content));
}

std::vector<Block> blocks(std::string_view text) {
    std::vector<Block> out;
    std::string cur;
    BlockKind open = BlockKind::PARA;
    std::string open_marker;
    std::vector<std::pair<bool, int>> lists;
    std::optional<std::vector<std::vector<std::string>>> table;
    bool in_cell = false;
    const auto cell = [&] {
        if (std::exchange(in_cell, false)) {
            auto t = collapse(cur);
            cur.clear();
            if (table) {
                if (table->empty()) {
                    table->emplace_back();
                }
                table->back().push_back(std::move(t));
            }
        }
    };
    const auto flush = [&] {
        auto t = collapse(cur);
        cur.clear();
        const auto kind = std::exchange(open, BlockKind::PARA);
        auto marker = std::exchange(open_marker, std::string());
        if (t.empty()) {
            return;
        }
        Block b;
        b.kind = kind;
        b.marker = std::move(marker);
        b.text = std::move(t);
        out.push_back(std::move(b));
    };
    const auto push_table = [&](std::vector<std::vector<std::string>> rows) {
        rows.erase(std::remove_if(rows.begin(), rows.end(), [](const auto& r) { return r.empty(); }), rows.end());
        if (!rows.empty()) {
            Block b;
            b.kind = BlockKind::TABLE;
            b.rows = std::move(rows);
            out.push_back(std::move(b));
        }
    };
    std::size_t i = 0;
    while (i < text.size()) {
        const auto rest = text.substr(i);
        const char c = rest[0];
        if (c == '<') {
            if (const auto tag = html_tag(rest)) {
                const auto& name = tag->name;
                const bool closing = tag->closing;
                if (name == "table") {
                    cell();
                    flush();
                    if (closing) {
                        if (table) {
                            push_table(std::move(*table));
                            table.reset();
                        }
                    } else {
                        table.emplace();
                    }
                } else if (name == "tr" && table) {
                    cell();
                    flush();
                    if (!closing) {
                        table->emplace_back();
                    }
                } else if ((name == "th" || name == "td") && table) {
                    cell();
                    if (!closing) {
                        cur.clear();
                        in_cell = true;
                    }
                } else if ((name == "p" || name == "br" || name == "div" || name == "tr" || name == "blockquote" ||
                            name == "dl") &&
                           !in_cell) {
                    flush();
                } else if (name == "ul" || name == "ol") {
                    flush();
                    if (closing) {
                        if (!lists.empty()) {
                            lists.pop_back();
                        }
                    } else {
                        lists.emplace_back(name == "ol", 0);
                    }
                } else if (name == "li" || name == "dt" || name == "dd") {
                    flush();
                    if (!closing) {
                        std::string marker = "- ";
                        if (!lists.empty() && lists.back().first) {
                            marker = std::to_string(++lists.back().second) + ". ";
                        }
                        open = BlockKind::ITEM;
                        open_marker = marker;
                    }
                } else if (name.size() == 2 && name[0] == 'h' && name[1] >= '1' && name[1] <= '6') {
                    flush();
                    if (!closing) {
                        open = BlockKind::HEADING;
                    }
                } else if (name == "pre" && !closing) {
                    flush();
                    const auto body = rest.substr(tag->length);
                    const auto close_at = lower(body).find("</pre");
                    const auto close = close_at == std::string::npos ? body.size() : close_at;
                    Block b;
                    b.kind = BlockKind::CODE;
                    b.text = pre_text(body.substr(0, close));
                    out.push_back(std::move(b));
                    const auto after = body.substr(close);
                    const auto gt = after.find('>');
                    i += tag->length + close + (gt == std::string_view::npos ? after.size() : gt + 1);
                    continue;
                } else if (name == "b" || name == "strong") {
                    cur += "**";
                } else if (name == "i" || name == "em") {
                    cur += '*';
                } else if (name == "code" || name == "tt") {
                    cur += '`';
                }
                i += tag->length;
                continue;
            }
        } else if (c == '{' && starts_with(rest, "{@")) {
            if (const auto t = inline_tag(text, i)) {
                cur += render_inline_tag(t->tag, t->content);
                i = t->end;
                continue;
            }
        } else if (c == '&') {
            if (const auto e = entity(rest)) {
                cur += e->first;
                i += e->second;
                continue;
            }
        }
        cur += c == '\n' ? ' ' : c;
        ++i;
    }
    cell();
    flush();
    if (table && std::any_of(table->begin(), table->end(), [](const auto& r) { return !r.empty(); })) {
        push_table(std::move(*table));
    }
    return out;
}

std::string render(const std::vector<Block>& all) {
    std::string out;
    bool prev_item = false;
    for (const auto& b : all) {
        const bool is_item = b.kind == BlockKind::ITEM;
        if (!out.empty()) {
            out += is_item && prev_item ? "\n" : "\n\n";
        }
        switch (b.kind) {
            case BlockKind::PARA:
                out += b.text;
                break;
            case BlockKind::ITEM:
                out += b.marker + b.text;
                break;
            case BlockKind::HEADING:
                out += "# " + b.text;
                break;
            case BlockKind::CODE:
                out += "```java\n" + b.text + "\n```";
                break;
            case BlockKind::TABLE: {
                std::size_t width = 0;
                for (const auto& r : b.rows) {
                    width = std::max(width, r.size());
                }
                const auto line = [&](const std::vector<std::string>& cells) {
                    std::string l = "|";
                    for (std::size_t k = 0; k < width; ++k) {
                        l += ' ';
                        l += k < cells.size() ? cells[k] : std::string();
                        l += " |";
                    }
                    return l;
                };
                out += line(b.rows[0]) + "\n|";
                for (std::size_t k = 0; k < width; ++k) {
                    out += " --- |";
                }
                for (std::size_t r = 1; r < b.rows.size(); ++r) {
                    out += "\n" + line(b.rows[r]);
                }
                break;
            }
        }
        prev_item = is_item;
    }
    return out;
}

Block item(std::string text) {
    Block b;
    b.kind = BlockKind::ITEM;
    b.marker = "- ";
    b.text = std::string(trim_end(text));
    return b;
}

std::string see_ref(std::string_view text) {
    text = trim(text);
    if (starts_with(text, "\"")) {
        text.remove_prefix(1);
        while (!text.empty() && text.back() == '"') {
            text.remove_suffix(1);
        }
        return std::string(text);
    }
    if (starts_with(text, "<")) {
        return inline_text(text);
    }
    const auto [reference, label] = split_link(text);
    return label.empty() ? reference_span(reference) : inline_text(label);
}

/// `markdown` with each link `[label](target)` replaced by its label.
std::string strip_links(std::string_view text) {
    std::string out;
    auto rest = text;
    while (true) {
        const auto open = rest.find('[');
        if (open == std::string_view::npos) {
            break;
        }
        out += rest.substr(0, open);
        const auto after = rest.substr(open + 1);
        const auto close = after.find(']');
        if (close == std::string_view::npos) {
            out += rest.substr(open);
            return out;
        }
        out += after.substr(0, close);
        auto tail = after.substr(close + 1);
        if (starts_with(tail, "(")) {
            const auto end = tail.find(')');
            if (end != std::string_view::npos) {
                tail = tail.substr(end + 1);
            }
        }
        rest = tail;
    }
    out += rest;
    return out;
}

/// `<pkg>.Outer$Inner` and `name(T1,T2)` or `name` from a marker.
std::pair<std::string_view, std::string_view> split_marker(std::string_view marker) {
    const auto hash = marker.find('#');
    if (hash == std::string_view::npos) {
        return {marker, {}};
    }
    return {marker.substr(0, hash), marker.substr(hash + 1)};
}

std::vector<std::string> erased_parameters(const java::Method& m) {
    std::vector<std::string> out;
    for (const auto& p : m.parameters) {
        out.push_back(p.type.erased_simple_name() + (p.varargs ? "[]" : ""));
    }
    return out;
}

class Follower {
public:
    explicit Follower(const java::Model& model) : model_(model) {}

    std::vector<std::string> followed(const std::string& marker, std::set<std::string>& seen) const {
        if (!seen.insert(marker).second) {
            return {};
        }
        std::vector<std::string> out;
        for (const auto& doc : marker_javadocs(marker)) {
            std::vector<std::string> through;
            for (const auto& reference : pointer_refs(doc)) {
                if (const auto target = see_marker(marker, reference)) {
                    const auto docs = followed(*target, seen);
                    through.insert(through.end(), docs.begin(), docs.end());
                }
            }
            if (through.empty()) {
                out.push_back(doc);
            } else {
                out.insert(out.end(), through.begin(), through.end());
            }
        }
        return out;
    }

private:
    static std::optional<std::string> own(const std::optional<java::Javadoc>& javadoc) {
        if (!javadoc) {
            return std::nullopt;
        }
        return clean_javadoc(javadoc->text);
    }

    std::vector<std::string> marker_javadocs(const std::string& marker) const {
        const auto [class_name, member] = split_marker(marker);
        const auto* type = model_.find(class_name);
        if (type == nullptr) {
            return {};
        }
        std::vector<std::string> docs;
        const auto add = [&](std::optional<std::string> d) {
            if (d && std::find(docs.begin(), docs.end(), *d) == docs.end()) {
                docs.push_back(std::move(*d));
            }
        };
        if (member.empty()) {
            add(own(type->javadoc));
            return docs;
        }
        const bool exact = member.find('(') != std::string_view::npos;
        const auto name = member.substr(0, member.find('('));
        for (const auto* list : {&type->constructors, &type->methods}) {
            for (const auto& m : *list) {
                if (m.implicit) {
                    continue;
                }
                if (exact ? m.signature() == member : m.name == name) {
                    add(overload_doc(*type, m));
                }
            }
        }
        if (!exact) {
            for (const auto& f : type->fields) {
                if (f.name == name) {
                    add(own(f.javadoc));
                }
            }
            for (const auto& c : type->enum_constants) {
                if (c.name == name) {
                    add(own(c.javadoc));
                }
            }
        }
        return docs;
    }

    std::optional<std::string> overload_doc(const java::Type& type, const java::Method& m) const {
        std::set<std::string> seen;
        return overload_doc_from(type, m, own(m.javadoc), seen);
    }

    std::optional<std::string> overload_doc_from(const java::Type& type, const java::Method& m,
                                                 std::optional<std::string> own_doc,
                                                 std::set<std::string>& seen) const {
        if (!seen.insert(type.binary_name).second) {
            return own_doc;
        }
        if (own_doc && own_doc->find("{@inheritDoc}") == std::string::npos) {
            return own_doc;
        }
        std::optional<std::string> inherited;
        std::vector<const java::TypeRef*> supers;
        for (const auto& s : type.extends) {
            supers.push_back(&s);
        }
        for (const auto& s : type.implements) {
            supers.push_back(&s);
        }
        const auto params = erased_parameters(m);
        for (const auto* s : supers) {
            const auto* sup = model_.find(s->name);
            if (sup == nullptr) {
                continue;
            }
            const java::Method* found = nullptr;
            for (const auto& o : sup->methods) {
                if (o.name == m.name && erased_parameters(o) == params) {
                    found = &o;
                    break;
                }
            }
            if (found == nullptr) {
                const java::Method* only = nullptr;
                int same_arity = 0;
                for (const auto& o : sup->methods) {
                    if (o.name == m.name && o.parameters.size() == m.parameters.size()) {
                        only = &o;
                        ++same_arity;
                    }
                }
                if (same_arity == 1) {
                    found = only;
                }
            }
            inherited = found != nullptr ? overload_doc_from(*sup, *found, own(found->javadoc), seen)
                                         : overload_doc_from(*sup, m, std::nullopt, seen);
            if (inherited) {
                break;
            }
        }
        if (own_doc && inherited) {
            return inherit_doc(*own_doc, *inherited);
        }
        return own_doc ? own_doc : inherited;
    }

    std::optional<std::string> see_marker(const std::string& from, std::string_view reference) const {
        std::string squashed;
        for (const char c : reference) {
            if (!is_space(c)) {
                squashed += c;
            }
        }
        const auto [class_part, member] = split_marker(squashed);
        const auto* from_type = model_.find(split_marker(from).first);
        if (from_type == nullptr) {
            return std::nullopt;
        }
        const java::Type* type = from_type;
        if (!class_part.empty()) {
            const auto dot = class_part.rfind('.');
            const auto simple = dot == std::string_view::npos ? class_part : class_part.substr(dot + 1);
            const java::Type* best = nullptr;
            for (const auto& [name, t] : model_.types) {
                if (t.simple_name != simple) {
                    continue;
                }
                if (best == nullptr || (best->package != from_type->package && t.package == from_type->package)) {
                    best = &t;
                }
            }
            if (best == nullptr) {
                return std::nullopt;
            }
            type = best;
        }
        if (member.empty()) {
            return type->binary_name;
        }
        const auto name = member.substr(0, member.find('('));
        bool names_overload = false;
        if (member.find('(') != std::string_view::npos) {
            for (const auto* list : {&type->constructors, &type->methods}) {
                for (const auto& m : *list) {
                    names_overload = names_overload || m.signature() == member;
                }
            }
        }
        return type->binary_name + "#" + std::string(names_overload ? member : name);
    }

    const java::Model& model_;
};

}  // namespace

std::string clean_javadoc(std::string_view comment) {
    auto inner = trim(comment);
    if (starts_with(inner, "/**")) {
        inner.remove_prefix(3);
    }
    if (inner.size() >= 2 && inner.substr(inner.size() - 2) == "*/") {
        inner.remove_suffix(2);
    }
    std::vector<std::string> lines;
    for (auto line : lines_of(inner)) {
        line = trim_start(line);
        if (starts_with(line, "*")) {
            line.remove_prefix(1);
        }
        if (starts_with(line, " ")) {
            line.remove_prefix(1);
        }
        lines.emplace_back(trim_end(line));
    }
    std::size_t first = 0;
    while (first < lines.size() && lines[first].empty()) {
        ++first;
    }
    std::size_t last = lines.size();
    while (last > first && lines[last - 1].empty()) {
        --last;
    }
    return join(std::vector<std::string>(lines.begin() + static_cast<std::ptrdiff_t>(first),
                                         lines.begin() + static_cast<std::ptrdiff_t>(last)),
                "\n");
}

ParsedJavadoc parse_javadoc(std::string_view cleaned) {
    ParsedJavadoc out;
    std::vector<std::string> body;
    std::size_t pre = 0;
    for (const auto line : lines_of(cleaned)) {
        const auto trimmed = trim(line);
        const bool starts_tag = pre == 0 && trimmed.size() > 1 && trimmed[0] == '@' &&
                                std::isalpha(static_cast<unsigned char>(trimmed[1])) != 0;
        if (starts_tag) {
            const auto [name, rest] = split_word(trimmed.substr(1));
            BlockTag tag;
            tag.name = std::string(name);
            if (name == "param" || name == "throws" || name == "exception") {
                const auto [arg, text] = split_word(rest);
                tag.arg = std::string(arg);
                tag.text = std::string(text);
            } else {
                tag.text = std::string(rest);
            }
            out.tags.push_back(std::move(tag));
        } else if (!out.tags.empty()) {
            out.tags.back().text += '\n';
            out.tags.back().text += line;
        } else {
            body.emplace_back(line);
        }
        const auto l = lower(line);
        pre += count(l, "<pre");
        const auto closes = count(l, "</pre");
        pre = pre > closes ? pre - closes : 0;
    }
    for (auto& tag : out.tags) {
        tag.text = std::string(trim(tag.text));
    }
    out.body = std::string(trim(join(body, "\n")));
    return out;
}

std::string javadoc_markdown(std::string_view cleaned) {
    const auto parsed = parse_javadoc(cleaned);
    auto body = blocks(parsed.body);
    std::vector<Block> arguments;
    std::vector<Block> returns;
    std::vector<Block> throws;
    std::vector<Block> see;
    for (const auto& tag : parsed.tags) {
        if (tag.name == "param") {
            std::string name = tag.arg;
            if (name.size() >= 2 && name.front() == '<' && name.back() == '>') {
                name = name.substr(1, name.size() - 2);
            }
            arguments.push_back(item("`" + name + "` - " + inline_text(tag.text)));
        } else if (tag.name == "return") {
            const auto found = blocks(tag.text);
            returns.insert(returns.end(), found.begin(), found.end());
        } else if (tag.name == "throws" || tag.name == "exception") {
            const auto dot = tag.arg.rfind('.');
            auto cls = dot == std::string::npos ? tag.arg : tag.arg.substr(dot + 1);
            cls = cls.substr(0, cls.find('<'));
            throws.push_back(item("`" + cls + "` " + inline_text(tag.text)));
        } else if (tag.name == "see") {
            see.push_back(item(see_ref(tag.text)));
        } else if (tag.name == "apiNote" || tag.name == "implNote" || tag.name == "implSpec") {
            const auto found = blocks(tag.text);
            body.insert(body.end(), found.begin(), found.end());
        }
    }
    std::vector<Block> all = body;
    for (auto& [heading, list] : std::vector<std::pair<std::string, std::vector<Block>*>>{
             {"Arguments", &arguments}, {"Returns", &returns}, {"Errors", &throws}, {"See also", &see}}) {
        if (!list->empty()) {
            Block h;
            h.kind = BlockKind::HEADING;
            h.text = heading;
            all.push_back(std::move(h));
            all.insert(all.end(), list->begin(), list->end());
        }
    }
    return render(all);
}

std::size_t word_count(std::string_view markdown) {
    bool fence = false;
    std::vector<std::string> kept;
    for (const auto line : lines_of(markdown)) {
        const auto trimmed = trim_start(line);
        if (starts_with(trimmed, "```")) {
            fence = !fence;
            continue;
        }
        std::size_t hashes = 0;
        while (hashes < trimmed.size() && trimmed[hashes] == '#') {
            ++hashes;
        }
        if (!fence && hashes > 0 && starts_with(trimmed.substr(hashes), " ")) {
            continue;
        }
        kept.emplace_back(line);
    }
    const auto text = strip_links(join(kept, "\n"));
    // A code span is one word: a run of N backticks opens it and the next run
    // of exactly N closes it, as CommonMark reads it (so `` a`b `` is one span);
    // a run nothing closes is plain text.
    const auto run_at = [&](std::size_t at) {
        std::size_t n = 0;
        while (at + n < text.size() && text[at + n] == '`') {
            ++n;
        }
        return n;
    };
    std::size_t words = 0;
    bool in_word = false;
    for (std::size_t i = 0; i < text.size();) {
        if (text[i] == '`') {
            const auto run = run_at(i);
            std::size_t close = std::string::npos;
            for (std::size_t j = i + run; j < text.size();) {
                if (text[j] != '`') {
                    ++j;
                    continue;
                }
                const auto other = run_at(j);
                if (other == run) {
                    close = j;
                    break;
                }
                j += other;
            }
            in_word = false;
            if (close == std::string::npos) {
                i += run;
                continue;
            }
            words += trim(std::string_view(text).substr(i + run, close - i - run)).empty() ? 0 : 1;
            i = close + run;
            continue;
        }
        const bool w = is_word_char(next_code_point(text, i));
        if (w && !in_word) {
            ++words;
        }
        in_word = w;
    }
    return words;
}

std::string inherit_doc(std::string_view own_text, std::string_view inherited_text) {
    auto own = parse_javadoc(own_text);
    const auto inherited = parse_javadoc(inherited_text);
    const auto replace_all = [](std::string& s, std::string_view what, std::string_view with) {
        for (auto at = s.find(what); at != std::string::npos; at = s.find(what, at + with.size())) {
            s.replace(at, what.size(), with);
        }
    };
    replace_all(own.body, "{@inheritDoc}", inherited.body);
    for (auto& tag : own.tags) {
        if (tag.text.find("{@inheritDoc}") != std::string::npos) {
            std::string parent;
            for (const auto& t : inherited.tags) {
                if (t.name == tag.name && t.arg == tag.arg) {
                    parent = t.text;
                    break;
                }
            }
            replace_all(tag.text, "{@inheritDoc}", parent);
        }
    }
    for (const auto& tag : inherited.tags) {
        const bool inheritable =
            tag.name == "param" || tag.name == "return" || tag.name == "throws" || tag.name == "exception";
        const bool present = std::any_of(own.tags.begin(), own.tags.end(),
                                         [&](const auto& t) { return t.name == tag.name && t.arg == tag.arg; });
        if (inheritable && !present) {
            own.tags.push_back(tag);
        }
    }
    std::string out = own.body;
    for (const auto& tag : own.tags) {
        out += "\n@" + tag.name + " " + tag.arg + " " + tag.text;
    }
    return out;
}

std::vector<std::string> pointer_refs(std::string_view cleaned) {
    const auto parsed = parse_javadoc(cleaned);
    if (!std::all_of(parsed.tags.begin(), parsed.tags.end(),
                     [](const auto& t) { return t.name == "see" || t.name == "since"; })) {
        return {};
    }
    std::vector<std::string> refs;
    std::string rest;
    std::string_view body = parsed.body;
    while (true) {
        const auto start = body.find("{@link");
        if (start == std::string_view::npos) {
            break;
        }
        rest += body.substr(0, start);
        const auto len = body.substr(start).find('}');
        if (len == std::string_view::npos) {
            return {};
        }
        const auto inner = body.substr(start + 1, len - 1);
        std::istringstream words{std::string(inner)};
        std::string first;
        std::string target;
        words >> first >> target;
        refs.push_back(target);
        body.remove_prefix(start + len + 1);
    }
    rest += body;
    bool in_word = false;
    std::string word;
    for (std::size_t i = 0; i <= rest.size(); ++i) {
        const bool w = i < rest.size() && std::isalnum(static_cast<unsigned char>(rest[i])) != 0;
        if (w) {
            word += rest[i];
        } else if (in_word) {
            if (lower(word) != "see") {
                return {};
            }
            word.clear();
        }
        in_word = w;
    }
    for (const auto& t : parsed.tags) {
        if (t.name != "see") {
            continue;
        }
        const auto end = t.text.find(')');
        if (end != std::string::npos && t.text.substr(0, end).find('(') != std::string::npos) {
            refs.push_back(t.text.substr(0, end + 1));
        } else {
            refs.emplace_back(split_word(t.text).first);
        }
    }
    return refs;
}

std::vector<std::string> followed_javadocs(const java::Model& model, std::string_view marker) {
    std::set<std::string> seen;
    return Follower(model).followed(std::string(marker), seen);
}

std::vector<std::size_t> javadoc_words(const java::Model& model, std::string_view marker) {
    std::vector<std::size_t> out;
    for (const auto& doc : followed_javadocs(model, marker)) {
        out.push_back(word_count(javadoc_markdown(doc)));
    }
    return out;
}

}  // namespace cklint::rules
