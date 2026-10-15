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

//! A minimal JSON reader for the producer harnesses' `results.json`.
//!
//! xtask has no JSON dependency (`chaos_matrix` reads cargo's JSON by hand
//! too), and the summaries are small, flat objects written by four harnesses
//! in two layouts (one line per key, and Python's `json.dump(indent=2)`), so a
//! full parser of standard JSON is simpler than matching either layout.

use anyhow::bail;

/// A parsed JSON value. Object members keep their order.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Value {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<Value>),
    Object(Vec<(String, Value)>),
}

impl Value {
    /// The member `key` of an object; `None` for a missing key or a non-object.
    pub(crate) fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Object(members) => members.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub(crate) fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Number(n) => Some(*n),
            _ => None,
        }
    }

    pub(crate) fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }
}

/// Parse one JSON document; anything but whitespace after it is an error.
pub(crate) fn parse(text: &str) -> anyhow::Result<Value> {
    let mut parser = Parser { bytes: text.as_bytes(), pos: 0 };
    let value = parser.value(0)?;
    parser.skip_whitespace();
    if parser.pos != parser.bytes.len() {
        bail!("unexpected trailing data at byte {}", parser.pos);
    }
    Ok(value)
}

/// Deeper nesting than any summary has; stops a hostile file from
/// overflowing the stack.
const MAX_DEPTH: usize = 64;

struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Parser<'_> {
    fn value(&mut self, depth: usize) -> anyhow::Result<Value> {
        if depth > MAX_DEPTH {
            bail!("nesting deeper than {MAX_DEPTH} levels");
        }
        self.skip_whitespace();
        match self.peek() {
            Some(b'{') => self.object(depth),
            Some(b'[') => self.array(depth),
            Some(b'"') => Ok(Value::String(self.string()?)),
            Some(b't') => self.literal("true", Value::Bool(true)),
            Some(b'f') => self.literal("false", Value::Bool(false)),
            Some(b'n') => self.literal("null", Value::Null),
            Some(b'-' | b'0'..=b'9') => self.number(),
            Some(c) => bail!("unexpected `{}` at byte {}", c as char, self.pos),
            None => bail!("unexpected end of input"),
        }
    }

    fn object(&mut self, depth: usize) -> anyhow::Result<Value> {
        self.pos += 1; // '{'
        let mut members = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(Value::Object(members));
        }
        loop {
            self.skip_whitespace();
            if self.peek() != Some(b'"') {
                bail!("expected a string key at byte {}", self.pos);
            }
            let key = self.string()?;
            self.skip_whitespace();
            self.expect(b':')?;
            let value = self.value(depth + 1)?;
            members.push((key, value));
            self.skip_whitespace();
            match self.next() {
                Some(b',') => continue,
                Some(b'}') => return Ok(Value::Object(members)),
                _ => bail!("expected `,` or `}}` at byte {}", self.pos.saturating_sub(1)),
            }
        }
    }

    fn array(&mut self, depth: usize) -> anyhow::Result<Value> {
        self.pos += 1; // '['
        let mut items = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(b']') {
            self.pos += 1;
            return Ok(Value::Array(items));
        }
        loop {
            items.push(self.value(depth + 1)?);
            self.skip_whitespace();
            match self.next() {
                Some(b',') => continue,
                Some(b']') => return Ok(Value::Array(items)),
                _ => bail!("expected `,` or `]` at byte {}", self.pos.saturating_sub(1)),
            }
        }
    }

    fn string(&mut self) -> anyhow::Result<String> {
        self.pos += 1; // '"'
        let mut out = String::new();
        loop {
            let start = self.pos;
            while let Some(c) = self.peek() {
                if c == b'"' || c == b'\\' || c < 0x20 {
                    break;
                }
                self.pos += 1;
            }
            // The input is a &str and the run stops only at ASCII bytes, so
            // the slice is valid UTF-8.
            out.push_str(std::str::from_utf8(&self.bytes[start..self.pos])?);
            match self.next() {
                Some(b'"') => return Ok(out),
                Some(b'\\') => out.push(self.escape()?),
                Some(_) => bail!("control character in a string at byte {}", self.pos - 1),
                None => bail!("unterminated string"),
            }
        }
    }

    fn escape(&mut self) -> anyhow::Result<char> {
        Ok(match self.next() {
            Some(b'"') => '"',
            Some(b'\\') => '\\',
            Some(b'/') => '/',
            Some(b'b') => '\u{8}',
            Some(b'f') => '\u{c}',
            Some(b'n') => '\n',
            Some(b'r') => '\r',
            Some(b't') => '\t',
            Some(b'u') => {
                let unit = self.hex4()?;
                if (0xD800..0xDC00).contains(&unit) {
                    // A high surrogate must be followed by `\u` and a low one.
                    if self.next() != Some(b'\\') || self.next() != Some(b'u') {
                        bail!("unpaired surrogate at byte {}", self.pos);
                    }
                    let low = self.hex4()?;
                    if !(0xDC00..0xE000).contains(&low) {
                        bail!("invalid low surrogate at byte {}", self.pos);
                    }
                    let code = 0x10000 + ((unit - 0xD800) << 10) + (low - 0xDC00);
                    char::from_u32(code).ok_or_else(|| anyhow::anyhow!("invalid code point"))?
                } else {
                    char::from_u32(unit).ok_or_else(|| anyhow::anyhow!("unpaired surrogate at byte {}", self.pos))?
                }
            },
            _ => bail!("invalid escape at byte {}", self.pos.saturating_sub(1)),
        })
    }

    fn hex4(&mut self) -> anyhow::Result<u32> {
        let end = self.pos + 4;
        let digits = self
            .bytes
            .get(self.pos..end)
            .ok_or_else(|| anyhow::anyhow!("truncated \\u escape"))?;
        let text = std::str::from_utf8(digits)?;
        let unit = u32::from_str_radix(text, 16).map_err(|_| anyhow::anyhow!("invalid \\u escape `{text}`"))?;
        self.pos = end;
        Ok(unit)
    }

    fn number(&mut self) -> anyhow::Result<Value> {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if matches!(c, b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9') {
                self.pos += 1;
            } else {
                break;
            }
        }
        let text = std::str::from_utf8(&self.bytes[start..self.pos])?;
        // Rust's float grammar accepts a few forms JSON does not (`1.`,
        // `+1`, `.5`); a summary never contains them, so the looser check
        // costs nothing. `inf` / `NaN` cannot get here: they start with a
        // letter.
        let n: f64 = text
            .parse()
            .map_err(|_| anyhow::anyhow!("invalid number `{text}` at byte {start}"))?;
        Ok(Value::Number(n))
    }

    fn literal(&mut self, word: &str, value: Value) -> anyhow::Result<Value> {
        if self.bytes[self.pos..].starts_with(word.as_bytes()) {
            self.pos += word.len();
            Ok(value)
        } else {
            bail!("invalid literal at byte {}", self.pos)
        }
    }

    fn expect(&mut self, byte: u8) -> anyhow::Result<()> {
        if self.next() == Some(byte) {
            Ok(())
        } else {
            bail!("expected `{}` at byte {}", byte as char, self.pos.saturating_sub(1))
        }
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn next(&mut self) -> Option<u8> {
        let c = self.peek()?;
        self.pos += 1;
        Some(c)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_one_line_per_key_summary() {
        // The layout the Rust, C and Java harnesses write.
        let text = "{\n  \"test\": \"producer\",\n  \"client\": \"librdkafka\",\n  \"throughput_msg_s\": 10000.25,\n  \
                    \"latency_ms\": {\"min\": 1, \"avg\": 2.50, \"p50\": 2, \"p99\": 9, \"max\": 31.00},\n  \
                    \"cpu_avg_pct\": 12.00\n}\n";
        let v = parse(text).unwrap();
        assert_eq!(v.get("client").and_then(Value::as_str), Some("librdkafka"));
        assert_eq!(v.get("throughput_msg_s").and_then(Value::as_f64), Some(10000.25));
        let latency = v.get("latency_ms").unwrap();
        assert_eq!(latency.get("p99").and_then(Value::as_f64), Some(9.0));
        assert_eq!(latency.get("max").and_then(Value::as_f64), Some(31.0));
        assert_eq!(v.get("missing"), None);
    }

    #[test]
    fn parses_pythons_indented_layout() {
        let text = "{\n  \"test\": \"producer\",\n  \"latency_ms\": {\n    \"min\": 0,\n    \"p999\": 12\n  },\n  \
                    \"rss_avg_kib\": 1024.5\n}";
        let v = parse(text).unwrap();
        assert_eq!(
            v.get("latency_ms").and_then(|l| l.get("p999")).and_then(Value::as_f64),
            Some(12.0)
        );
        assert_eq!(v.get("rss_avg_kib").and_then(Value::as_f64), Some(1024.5));
    }

    #[test]
    fn parses_scalars_arrays_and_escapes() {
        let v = parse(r#"{"a": [true, false, null, -1.5e2], "s": "x\"y\\z\n\u00e9\ud83d\ude00", "e": {}}"#).unwrap();
        assert_eq!(
            v.get("a"),
            Some(&Value::Array(vec![
                Value::Bool(true),
                Value::Bool(false),
                Value::Null,
                Value::Number(-150.0)
            ]))
        );
        assert_eq!(v.get("s").and_then(Value::as_str), Some("x\"y\\z\né😀"));
        assert_eq!(v.get("e"), Some(&Value::Object(Vec::new())));
        assert_eq!(parse("[]").unwrap(), Value::Array(Vec::new()));
    }

    #[test]
    fn rejects_malformed_documents() {
        for bad in [
            "",
            "{",
            "{\"a\" 1}",
            "{\"a\": 1,}",
            "[1 2]",
            "{\"a\": 1} x",
            "\"unterminated",
            "{\"a\": tru}",
            "\"\\x\"",
            "\"\\ud83d\"",
            "{1: 2}",
            "\"a\nb\"",
        ] {
            assert!(parse(bad).is_err(), "accepted {bad:?}");
        }
    }

    #[test]
    fn rejects_runaway_nesting() {
        let deep = "[".repeat(MAX_DEPTH + 2);
        let err = parse(&deep).unwrap_err().to_string();
        assert!(err.contains("nesting deeper than"), "{err}");
    }
}
