// Copyright 2025 Confluent Inc.
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

//! Static arity checker for the hand-written CPython extension module.
//!
//! `Py_BuildValue` and `PyArg_ParseTuple` / `PyArg_ParseTupleAndKeywords` are
//! variadic: a format string with one unit too few (or too many) for the
//! arguments supplied compiles cleanly, is not diagnosed by any compiler
//! warning, and reads a garbage pointer at run time. Worse, the mismatch is
//! frequently *unreachable from the test suite*: for every admin RPC that
//! Java's own `MockAdminClient` leaves unsupported, the success path of the
//! corresponding `*_drain` is dead code in the unit tests, so only the error
//! branch is ever executed.
//!
//! `PyObject_CallFunction` takes the same `Py_BuildValue` grammar and is the
//! same defect class; it is scanned too, and it matters more than its call
//! count suggests, because every site is a **callback trampoline** — the code
//! path hardest to reach from a unit test, which is this checker's whole
//! rationale.
//!
//! This checker parses every call site in `bindings/python/_confluentkafka.c`,
//! counts the format units in the (string-literal) format argument, and
//! compares that against the number of top-level arguments actually passed.
//! Any mismatch fails the build.
//!
//! What it does **not** check, so that the gate is not read as broader than it
//! is:
//!
//! * **Argument order.** A transposed pair of same-typed fields is statically
//!   undetectable here and has to be caught by a test or by review.
//! * **Argument types.** A `long` passed under `i`, or an `int64_t` under `i`,
//!   is an arity match and a run-time defect.
//! * **`N` versus `O` reference stealing** in `Py_BuildValue`. `N` steals the
//!   reference and `O` increments it; using the wrong one leaks or
//!   double-frees, and the counter cannot tell them apart.
//! * **`PyArg_ParseTupleAndKeywords` kwlist length.** The kwlist must hold one
//!   `NULL`-terminated name per non-positional unit; a short one reads past the
//!   array.
//! * `PyErr_Format`, whose format is printf grammar rather than either of
//!   these two, so it needs a separate counter.

use std::fmt::Write as _;
use std::path::Path;

/// Default subject of the check.
pub const DEFAULT_SOURCE: &str = "bindings/python/_confluentkafka.c";

/// The four variadic CPython entry points we can check, and how many fixed
/// (non-format-unit) arguments each takes.
///
/// * `Py_BuildValue(format, ...)` — 1 fixed argument (the format itself).
/// * `PyObject_CallFunction(callable, format, ...)` — 2, and the same
///   `Py_BuildValue` grammar for the rest.
/// * `PyArg_ParseTuple(args, format, ...)` — 2 fixed arguments.
/// * `PyArg_ParseTupleAndKeywords(args, kwargs, format, kwlist, ...)` — 4.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kind {
    BuildValue,
    CallFunction,
    ParseTuple,
    ParseTupleAndKeywords,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::BuildValue => "Py_BuildValue",
            Kind::CallFunction => "PyObject_CallFunction",
            Kind::ParseTuple => "PyArg_ParseTuple",
            Kind::ParseTupleAndKeywords => "PyArg_ParseTupleAndKeywords",
        }
    }

    /// Zero-based index of the format string within the argument list.
    fn format_index(self) -> usize {
        match self {
            Kind::BuildValue => 0,
            Kind::CallFunction => 1,
            Kind::ParseTuple => 1,
            Kind::ParseTupleAndKeywords => 2,
        }
    }

    /// Number of arguments that are not consumed by format units.
    fn fixed_args(self) -> usize {
        match self {
            Kind::BuildValue => 1,
            Kind::CallFunction => 2,
            Kind::ParseTuple => 2,
            Kind::ParseTupleAndKeywords => 4,
        }
    }

    /// `Py_BuildValue` and `PyObject_CallFunction` read their arguments;
    /// `PyArg_Parse*` writes through pointers. The two format grammars differ
    /// (`|`, `$`, `:`, `;`, `O!`, `es#`, `s*` are parse-only), so the unit
    /// counter needs to know which.
    fn is_parse(self) -> bool {
        !matches!(self, Kind::BuildValue | Kind::CallFunction)
    }
}

/// One problem found at one call site.
#[derive(Debug, PartialEq, Eq)]
pub struct Finding {
    /// 1-based line number of the call in the source file.
    pub line: usize,
    /// Which CPython function.
    pub function: &'static str,
    /// Human-readable description of the problem.
    pub detail: String,
}

/// Scans `source` (the full text of a C file) and returns every arity problem.
///
/// Comments are stripped first, and string literals are tracked, so a call
/// mentioned inside a comment or a string is not scanned.
pub fn scan(source: &str) -> Vec<Finding> {
    let (code, in_literal) = strip_comments(source);
    let bytes = code.as_bytes();
    let mut findings = Vec::new();

    for kind in [
        Kind::BuildValue,
        Kind::CallFunction,
        Kind::ParseTupleAndKeywords,
        Kind::ParseTuple,
    ] {
        let needle = kind.name();
        let mut from = 0usize;
        while let Some(rel) = code[from..].find(needle) {
            let at = from + rel;
            from = at + needle.len();

            // Skip matches inside a string literal, and matches that are part
            // of a longer identifier. `PyArg_ParseTuple` is a prefix of
            // `PyArg_ParseTupleAndKeywords`, so the right-boundary check is
            // what keeps the two from double-counting (the longer name is
            // scanned first, but each scan is independent).
            if in_literal[at] {
                continue;
            }
            if at > 0 && is_ident_byte(bytes[at - 1]) {
                continue;
            }
            let after = at + needle.len();
            if after < bytes.len() && is_ident_byte(bytes[after]) {
                continue;
            }

            // Skip whitespace between the name and its `(`. A declaration or a
            // function-pointer mention with no call parenthesis is ignored.
            let mut i = after;
            while i < bytes.len() && (bytes[i] as char).is_whitespace() {
                i += 1;
            }
            if i >= bytes.len() || bytes[i] != b'(' {
                continue;
            }

            let line = line_of(source, at);
            match split_arguments(&code, &in_literal, i) {
                None => findings.push(Finding {
                    line,
                    function: kind.name(),
                    detail: "unterminated argument list (unbalanced parentheses?)".to_string(),
                }),
                Some((args, end)) => {
                    from = end;
                    findings.extend(check_call(kind, line, &code, &in_literal, &args));
                },
            }
        }
    }

    findings.sort_by_key(|f| (f.line, f.function));
    findings
}

/// Checks one call site whose top-level arguments have already been split.
fn check_call(kind: Kind, line: usize, code: &str, in_literal: &[bool], args: &[(usize, usize)]) -> Vec<Finding> {
    let fmt_index = kind.format_index();
    let Some(&(fmt_start, fmt_end)) = args.get(fmt_index) else {
        return vec![Finding {
            line,
            function: kind.name(),
            detail: format!(
                "call has {} argument(s) but the format string is argument {}",
                args.len(),
                fmt_index + 1
            ),
        }];
    };

    let Some(format) = string_literal(&code[fmt_start..fmt_end], &in_literal[fmt_start..fmt_end]) else {
        // A non-literal format cannot be checked. That is a hole in the gate,
        // so it is reported rather than silently skipped: either make the
        // format a literal, or the mismatch class this checker exists to
        // catch is unguarded at that site.
        return vec![Finding {
            line,
            function: kind.name(),
            detail: format!(
                "format argument is not a string literal ({}) — arity cannot be checked statically",
                code[fmt_start..fmt_end].trim()
            ),
        }];
    };

    let units = match count_units(&format, kind.is_parse()) {
        Ok(units) => units,
        Err(err) => {
            return vec![Finding { line, function: kind.name(), detail: format!("format \"{format}\": {err}") }];
        },
    };

    let expected = kind.fixed_args() + units;
    if args.len() == expected {
        return Vec::new();
    }
    let mut detail = String::new();
    let _ = write!(
        detail,
        "format \"{}\" needs {} format unit(s), so the call takes {} argument(s), but {} were passed",
        format,
        units,
        expected,
        args.len()
    );
    vec![Finding { line, function: kind.name(), detail }]
}

/// Counts the arguments a CPython format string consumes.
///
/// `parse` selects the `PyArg_Parse*` grammar (pointer targets, `|`/`$`
/// separators, `:`/`;` terminators) over the `Py_BuildValue` one.
///
/// Structural characters (`()[]{}`, `,`, whitespace, `:` in build mode for
/// dict keys) consume nothing.
fn count_units(format: &str, parse: bool) -> Result<usize, String> {
    let chars: Vec<char> = format.chars().collect();
    let mut i = 0usize;
    let mut units = 0usize;

    // `?` after a unit means "or None" in some third-party conventions but is
    // not CPython; every character below is from the documented grammar.
    while i < chars.len() {
        let c = chars[i];
        i += 1;
        let peek = |i: usize| chars.get(i).copied();

        match c {
            // Structural / separators — no argument consumed.
            '(' | ')' | '[' | ']' | '{' | '}' | ',' | ' ' | '\t' | '\n' => {},
            '|' | '$' if parse => {},
            ':' if !parse => {},
            // In PyArg_Parse* a `:` or `;` ends the format; what follows is a
            // function name (for error messages) or a custom error message.
            ':' | ';' if parse => break,

            // Strings and buffers.
            's' | 'z' | 'y' => {
                if peek(i) == Some('#') {
                    i += 1;
                    units += 2;
                } else if parse && peek(i) == Some('*') {
                    i += 1;
                    units += 1;
                } else {
                    units += 1;
                }
            },
            'u' | 'Z' => {
                if peek(i) == Some('#') {
                    i += 1;
                    units += 2;
                } else {
                    units += 1;
                }
            },
            'U' => {
                // `U#` existed in old Py_BuildValue formats; harmless to accept.
                if !parse && peek(i) == Some('#') {
                    i += 1;
                    units += 2;
                } else {
                    units += 1;
                }
            },
            'w' if parse => {
                if peek(i) == Some('*') || peek(i) == Some('#') {
                    let two = peek(i) == Some('#');
                    i += 1;
                    units += if two { 2 } else { 1 };
                } else {
                    return Err("`w` must be followed by `*` (or the deprecated `#`)".to_string());
                }
            },
            'e' if parse => {
                // `es`/`et` take (encoding, char **buffer); with `#` also a length.
                match peek(i) {
                    Some('s') | Some('t') => i += 1,
                    _ => return Err("`e` must be followed by `s` or `t`".to_string()),
                }
                if peek(i) == Some('#') {
                    i += 1;
                    units += 3;
                } else {
                    units += 2;
                }
            },

            // Objects.
            'O' => match peek(i) {
                Some('&') => {
                    i += 1;
                    units += 2;
                },
                Some('!') if parse => {
                    i += 1;
                    units += 2;
                },
                _ => units += 1,
            },
            'S' | 'N' => units += 1,
            'Y' if parse => units += 1,

            // Scalars.
            'b' | 'B' | 'h' | 'H' | 'i' | 'I' | 'l' | 'k' | 'L' | 'K' | 'n' | 'c' | 'C' | 'd' | 'f' | 'D' | 'p' => {
                units += 1
            },

            other => return Err(format!("unknown format unit `{other}`")),
        }
    }

    Ok(units)
}

/// Splits the argument list of a call whose `(` is at byte offset `open`.
///
/// Returns the `(start, end)` byte range of each top-level argument and the
/// offset just past the closing `)`. Nested calls, casts, array subscripts,
/// braced initializers and string/char literals are all skipped, so only
/// commas at nesting depth zero split.
///
/// A call written `f()` yields zero arguments; `f("")` yields one.
fn split_arguments(code: &str, in_literal: &[bool], open: usize) -> Option<(Vec<(usize, usize)>, usize)> {
    let bytes = code.as_bytes();
    debug_assert_eq!(bytes[open], b'(');
    let mut depth = 0i32;
    let mut args = Vec::new();
    let mut start = open + 1;

    let mut i = open;
    while i < bytes.len() {
        if in_literal[i] {
            i += 1;
            continue;
        }
        match bytes[i] {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => {
                depth -= 1;
                if depth == 0 {
                    let last = &code[start..i];
                    if !(args.is_empty() && last.trim().is_empty()) {
                        args.push((start, i));
                    }
                    return Some((args, i + 1));
                }
            },
            b',' if depth == 1 => {
                args.push((start, i));
                start = i + 1;
            },
            _ => {},
        }
        i += 1;
    }
    None
}

/// Decodes a C expression that is one or more adjacent string literals into
/// the concatenated string it denotes. Returns `None` when the expression is
/// anything else (an identifier, a macro, a ternary, ...).
fn string_literal(expr: &str, in_literal: &[bool]) -> Option<String> {
    let bytes = expr.as_bytes();
    let mut out = String::new();
    let mut i = 0usize;
    let mut saw_one = false;

    while i < bytes.len() {
        let b = bytes[i];
        if (b as char).is_whitespace() {
            i += 1;
            continue;
        }
        if b != b'"' || !in_literal[i] {
            return None;
        }
        i += 1; // opening quote
        while i < bytes.len() && bytes[i] != b'"' {
            if bytes[i] == b'\\' {
                // Keep the escape's *effect* on unit counting minimal: the
                // only escapes that could appear in a CPython format string
                // are `\"` and `\\`, neither of which is a format unit.
                i += 1;
                if i < bytes.len() {
                    out.push(bytes[i] as char);
                    i += 1;
                }
                continue;
            }
            out.push(bytes[i] as char);
            i += 1;
        }
        if i >= bytes.len() {
            return None; // unterminated
        }
        i += 1; // closing quote
        saw_one = true;
    }

    if saw_one {
        Some(out)
    } else {
        None
    }
}

/// Replaces every comment byte with a space (newlines preserved so line
/// numbers do not move) and returns, alongside the rewritten text, a mask
/// marking every byte that lies inside a string or character literal
/// (including the delimiting quotes).
fn strip_comments(source: &str) -> (String, Vec<bool>) {
    let bytes = source.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut mask = Vec::with_capacity(bytes.len());
    let mut i = 0usize;

    while i < bytes.len() {
        let b = bytes[i];
        let next = bytes.get(i + 1).copied();

        if b == b'/' && next == Some(b'/') {
            while i < bytes.len() && bytes[i] != b'\n' {
                out.push(b' ');
                mask.push(false);
                i += 1;
            }
            continue;
        }
        if b == b'/' && next == Some(b'*') {
            let mut j = i;
            while j < bytes.len() {
                let end = bytes[j] == b'*' && bytes.get(j + 1) == Some(&b'/');
                out.push(if bytes[j] == b'\n' { b'\n' } else { b' ' });
                mask.push(false);
                j += 1;
                if end {
                    out.push(b' ');
                    mask.push(false);
                    j += 1;
                    break;
                }
            }
            i = j;
            continue;
        }
        if b == b'"' || b == b'\'' {
            let quote = b;
            out.push(b);
            mask.push(true);
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' {
                    out.push(bytes[i]);
                    mask.push(true);
                    i += 1;
                    if i < bytes.len() {
                        out.push(bytes[i]);
                        mask.push(true);
                        i += 1;
                    }
                    continue;
                }
                let closing = bytes[i] == quote;
                out.push(bytes[i]);
                mask.push(true);
                i += 1;
                if closing {
                    break;
                }
            }
            continue;
        }

        out.push(b);
        mask.push(false);
        i += 1;
    }

    // The byte-for-byte rewrite keeps every non-ASCII byte untouched, so the
    // result is valid UTF-8 whenever the input was.
    (String::from_utf8(out).expect("comment stripping is byte-preserving"), mask)
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

fn line_of(source: &str, offset: usize) -> usize {
    source[..offset].bytes().filter(|&b| b == b'\n').count() + 1
}

/// Runs the check over one file, printing a per-file summary.
///
/// Returns the number of call sites inspected on success.
pub fn check_file(path: &Path) -> anyhow::Result<()> {
    let source = std::fs::read_to_string(path).map_err(|e| anyhow::anyhow!("cannot read {}: {e}", path.display()))?;
    let findings = scan(&source);
    let (build_sites, call_sites, parse_sites) = count_sites(&source);

    println!(
        "   {}: {} Py_BuildValue, {} PyObject_CallFunction and {} PyArg_Parse* call site(s) inspected",
        path.display(),
        build_sites,
        call_sites,
        parse_sites
    );

    if findings.is_empty() {
        return Ok(());
    }
    for f in &findings {
        eprintln!("{}:{}: {}: {}", path.display(), f.line, f.function, f.detail);
    }
    anyhow::bail!("{} CPython format-arity problem(s) in {}", findings.len(), path.display())
}

/// Counts inspected call sites, for the summary line: `Py_BuildValue`,
/// `PyObject_CallFunction` and `PyArg_Parse*` respectively.
fn count_sites(source: &str) -> (usize, usize, usize) {
    let (code, in_literal) = strip_comments(source);
    let bytes = code.as_bytes();
    let mut build = 0usize;
    let mut call = 0usize;
    let mut parse = 0usize;

    for kind in [
        Kind::BuildValue,
        Kind::CallFunction,
        Kind::ParseTupleAndKeywords,
        Kind::ParseTuple,
    ] {
        let needle = kind.name();
        let mut from = 0usize;
        while let Some(rel) = code[from..].find(needle) {
            let at = from + rel;
            from = at + needle.len();
            if in_literal[at] || (at > 0 && is_ident_byte(bytes[at - 1])) {
                continue;
            }
            let after = at + needle.len();
            if after < bytes.len() && is_ident_byte(bytes[after]) {
                continue;
            }
            let mut i = after;
            while i < bytes.len() && (bytes[i] as char).is_whitespace() {
                i += 1;
            }
            if i >= bytes.len() || bytes[i] != b'(' {
                continue;
            }
            match kind {
                Kind::BuildValue => build += 1,
                Kind::CallFunction => call += 1,
                Kind::ParseTuple | Kind::ParseTupleAndKeywords => parse += 1,
            }
        }
    }
    (build, call, parse)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn details(src: &str) -> Vec<String> {
        scan(src).into_iter().map(|f| format!("{}:{}", f.line, f.detail)).collect()
    }

    #[test]
    fn a_matching_build_value_call_is_clean() {
        assert!(scan(r#"x = Py_BuildValue("(sON)", a, b, c);"#).is_empty());
    }

    #[test]
    fn one_unit_short_is_reported_with_both_counts() {
        // The shape of the real defect fixed in 761da3b2: ten units, eleven
        // arguments.
        let src = r#"x = Py_BuildValue("(sONsssNNNN)", a, b, c, d, e, f, g, h, i, j, k);"#;
        let found = details(src);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(
            found[0].contains("needs 10 format unit(s), so the call takes 11 argument(s), but 12 were passed"),
            "{found:?}"
        );
    }

    #[test]
    fn one_unit_too_many_is_reported() {
        let found = details(r#"Py_BuildValue("(ss)", a);"#);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("needs 2 format unit(s)"), "{found:?}");
    }

    #[test]
    fn two_argument_units_are_counted_as_two() {
        assert!(scan(r#"Py_BuildValue("(s#O&)", buf, len, conv, obj);"#).is_empty());
        assert_eq!(details(r#"Py_BuildValue("(s#)", buf);"#).len(), 1);
    }

    #[test]
    fn nested_structure_characters_consume_nothing() {
        assert!(scan(r#"Py_BuildValue("{s:i,s:i}", a, b, c, d);"#).is_empty());
        assert!(scan(r#"Py_BuildValue("[(si)(si)]", a, b, c, d);"#).is_empty());
    }

    #[test]
    fn empty_format_takes_no_arguments() {
        assert!(scan(r#"Py_BuildValue("");"#).is_empty());
        assert_eq!(details(r#"Py_BuildValue("", x);"#).len(), 1);
    }

    #[test]
    fn nested_calls_and_commas_inside_them_do_not_split_arguments() {
        assert!(
            scan(r#"Py_BuildValue("(sN)", topic, make_tuple(a, b, c));"#).is_empty(),
            "{:?}",
            details(r#"Py_BuildValue("(sN)", topic, make_tuple(a, b, c));"#)
        );
        // Braced initializer and subscript, likewise.
        assert!(scan(r#"Py_BuildValue("(ii)", arr[i, j], (int){1, 2});"#).is_empty());
    }

    #[test]
    fn a_comma_inside_a_string_or_char_literal_does_not_split() {
        assert!(scan(r#"Py_BuildValue("(ss)", "a,b", "c,d");"#).is_empty());
        assert!(scan(r#"Py_BuildValue("(ci)", ',', n);"#).is_empty());
    }

    #[test]
    fn adjacent_string_literals_are_concatenated() {
        assert!(scan(r#"Py_BuildValue("(s" "s)", a, b);"#).is_empty());
        assert_eq!(details(r#"Py_BuildValue("(s" "s)", a);"#).len(), 1);
    }

    #[test]
    fn parse_tuple_counts_its_two_fixed_arguments() {
        assert!(scan(r#"PyArg_ParseTuple(args, "sO", &a, &b);"#).is_empty());
        let found = details(r#"PyArg_ParseTuple(args, "sO", &a);"#);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("takes 4 argument(s), but 3 were passed"), "{found:?}");
    }

    #[test]
    fn parse_tuple_and_keywords_counts_its_four_fixed_arguments() {
        assert!(scan(r#"PyArg_ParseTupleAndKeywords(args, kwargs, "sO", kwlist, &a, &b);"#).is_empty());
        assert_eq!(
            details(r#"PyArg_ParseTupleAndKeywords(args, kwargs, "sO", kwlist, &a);"#).len(),
            1
        );
    }

    #[test]
    fn parse_only_grammar_is_honoured() {
        // `|` and `$` consume nothing; `:name` terminates the format.
        assert!(scan(r#"PyArg_ParseTuple(args, "s|i:fn", &a, &b);"#).is_empty());
        assert!(scan(r#"PyArg_ParseTupleAndKeywords(args, kw, "s|$i", kwlist, &a, &b);"#).is_empty());
        // `O!` and `O&` take two pointers, `s*` one, `es#` three.
        assert!(scan(r#"PyArg_ParseTuple(args, "O!O&s*", &t, &o, &c, &o2, &buf);"#).is_empty());
        assert!(scan(r#"PyArg_ParseTuple(args, "es#", &enc, &buf, &len);"#).is_empty());
    }

    #[test]
    fn the_terminator_is_not_a_terminator_for_py_build_value() {
        // `:` is a dict separator in a build format, not an end marker.
        assert!(scan(r#"Py_BuildValue("{s:s}", k, v);"#).is_empty());
    }

    #[test]
    fn calls_inside_comments_and_strings_are_ignored() {
        assert!(scan("// Py_BuildValue(\"(ss)\", a);\n").is_empty());
        assert!(scan("/* Py_BuildValue(\"(ss)\", a);\n   more */\n").is_empty());
        assert!(scan(r#"const char *doc = "Py_BuildValue(\"(ss)\", a)";"#).is_empty());
    }

    #[test]
    fn a_longer_identifier_is_not_a_match() {
        assert!(scan(r#"my_Py_BuildValue("(ss)", a);"#).is_empty());
        assert!(scan(r#"Py_BuildValueX("(ss)", a);"#).is_empty());
    }

    #[test]
    fn parse_tuple_is_not_double_counted_as_the_keywords_variant() {
        // `PyArg_ParseTuple` is a strict prefix of
        // `PyArg_ParseTupleAndKeywords`; a wrong boundary check would either
        // scan the keywords call twice or charge it the wrong fixed count.
        let src = r#"PyArg_ParseTupleAndKeywords(args, kwargs, "s", kwlist, &a);"#;
        assert!(scan(src).is_empty(), "{:?}", details(src));
        let (build, call, parse) = count_sites(src);
        assert_eq!((build, call, parse), (0, 0, 1));
    }

    #[test]
    fn a_non_literal_format_is_reported_rather_than_skipped() {
        let found = details(r#"Py_BuildValue(fmt, a, b);"#);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("not a string literal"), "{found:?}");
    }

    #[test]
    fn an_unknown_format_unit_is_reported() {
        let found = details(r#"Py_BuildValue("(sQ)", a, b);"#);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("unknown format unit `Q`"), "{found:?}");
    }

    #[test]
    fn line_numbers_survive_comment_stripping_and_wrapping() {
        let src = "/* a\n   multi-line\n   comment */\nPy_BuildValue(\n    \"(ss)\",\n    a);\n";
        let found = scan(src);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].line, 4);
    }

    #[test]
    fn site_counting_matches_the_scanned_calls() {
        let src = r#"
            Py_BuildValue("(s)", a);
            PyObject_CallFunction(cb, "K", n);
            PyArg_ParseTuple(args, "s", &a);
            PyArg_ParseTupleAndKeywords(args, kw, "s", kwlist, &a);
        "#;
        assert_eq!(count_sites(src), (1, 1, 2));
        assert!(scan(src).is_empty());
    }

    #[test]
    fn a_matching_call_function_is_clean_and_a_short_one_is_reported() {
        // Two fixed arguments (the callable and the format), then one per unit.
        assert!(scan(r#"PyObject_CallFunction(cb, "KK", a, b);"#).is_empty());
        assert!(scan(r#"PyObject_CallFunction(cb, "LisL", a, b, c, d);"#).is_empty());
        let found = details(r#"PyObject_CallFunction(cb, "KK", a);"#);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(
            found[0].contains("needs 2 format unit(s), so the call takes 4 argument(s), but 3 were passed"),
            "{found:?}"
        );
    }

    #[test]
    fn call_function_uses_the_build_value_grammar_not_the_parse_one() {
        // `|` is parse-only: under the build grammar it is an unknown unit, so
        // a `PyObject_CallFunction` misclassified as a parse call would go
        // unreported here.
        let found = details(r#"PyObject_CallFunction(cb, "s|i", a, b);"#);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("unknown format unit `|`"), "{found:?}");
    }

    #[test]
    fn call_function_obj_args_is_not_scanned_as_call_function() {
        // `PyObject_CallFunction` is a strict prefix of
        // `PyObject_CallFunctionObjArgs`, which is NULL-terminated and takes no
        // format at all -- scanning it would report its first argument as a
        // non-literal format.
        let src = r#"PyObject_CallFunctionObjArgs(cb, a, b, NULL);"#;
        assert!(scan(src).is_empty(), "{:?}", details(src));
        assert_eq!(count_sites(src), (0, 0, 0));
    }

    #[test]
    fn an_unbalanced_call_is_reported_not_silently_dropped() {
        let found = details("Py_BuildValue(\"(ss)\", a, b\n");
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("unterminated"), "{found:?}");
    }
}
