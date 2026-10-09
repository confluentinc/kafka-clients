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

//! The `ConfigDef` type of every producer and consumer config key, read from
//! the Java sources.
//!
//! The Python clients coerce a `str`, `int`, `float` or `bool` config value to
//! its key's `ConfigDef` type before the core sees it (CLAUDE.md, Python Binding
//! Conventions, Configuration). The key sets and types are those of Java's `ProducerConfig`
//! and `ConsumerConfig`: every `define(name, Type.X, …)` /
//! `defineInternal(name, Type.X, …)` of the class, plus the ones
//! `withClientSslSupport()` / `withClientSaslSupport()` add from `SslConfigs` /
//! `SaslConfigs`. A key name is a `static final String` constant, resolved
//! through the class's imports; anything this reader cannot resolve stops
//! generation instead of being guessed.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::java_parse::{parse_expr, strip_comments, Expr};

type Res<T> = anyhow::Result<T>;

const JAVA_ROOT: &str = "kafka/clients/src/main/java";
const OUTPUT: &str = "python/confluent_kafka/_config_types.py";
const SSL_CONFIGS: &str = "org.apache.kafka.common.config.SslConfigs";
const SASL_CONFIGS: &str = "org.apache.kafka.common.config.SaslConfigs";

/// The `ConfigDef` types a key can have (Java's `ConfigDef.Type`).
const TYPES: &[&str] = &[
    "BOOLEAN", "STRING", "INT", "SHORT", "LONG", "DOUBLE", "LIST", "CLASS", "PASSWORD",
];

/// A Java source file: its package, imports, superclass and `String`
/// constants.
struct Source {
    package: String,
    imports: BTreeMap<String, String>,
    /// `import static a.b.C.NAME;`: `NAME` -> `a.b.C`.
    static_imports: BTreeMap<String, String>,
    parent: Option<String>,
    constants: BTreeMap<String, String>,
    text: String,
}

struct Reader {
    root: PathBuf,
    cache: BTreeMap<String, Source>,
}

impl Reader {
    fn load(&mut self, fqn: &str) -> Res<&Source> {
        if !self.cache.contains_key(fqn) {
            let path = self.root.join(JAVA_ROOT).join(format!("{}.java", fqn.replace('.', "/")));
            let raw = fs::read_to_string(&path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
            self.cache.insert(fqn.to_string(), parse_source(&strip_comments(&raw)));
        }
        Ok(&self.cache[fqn])
    }

    /// The fully qualified name of the class `simple` names inside `from`.
    fn qualify(&mut self, from: &str, simple: &str) -> Res<String> {
        let src = self.load(from)?;
        if let Some(fqn) = src.imports.get(simple) {
            return Ok(fqn.clone());
        }
        Ok(format!("{}.{simple}", src.package))
    }

    /// The value of the `String` constant `name` seen from class `fqn`: its
    /// own, a static import, or its superclass's (`AbstractConfig`'s
    /// `CONFIG_PROVIDERS_CONFIG`).
    fn constant(&mut self, fqn: &str, name: &str) -> Res<String> {
        let src = self.load(fqn)?;
        if let Some(init) = src.constants.get(name).cloned() {
            let expr = parse_expr(&init).map_err(|e| anyhow::anyhow!("config types: `{fqn}.{name}`: {e}"))?;
            return self.eval(fqn, &expr);
        }
        if let Some(owner) = src.static_imports.get(name).cloned() {
            return self.constant(&owner, name);
        }
        if let Some(parent) = src.parent.clone() {
            let parent = self.qualify(fqn, &parent)?;
            return self.constant(&parent, name);
        }
        anyhow::bail!("config types: `{fqn}` has no String constant `{name}`")
    }

    fn eval(&mut self, fqn: &str, expr: &Expr) -> Res<String> {
        match expr {
            Expr::Str(s) => Ok(s.clone()),
            Expr::Add(a, b) => Ok(self.eval(fqn, a)? + &self.eval(fqn, b)?),
            Expr::Name(parts) if parts.len() == 1 => self.constant(fqn, &parts[0]),
            Expr::Name(parts) if parts.len() == 2 => {
                let owner = self.qualify(fqn, &parts[0])?;
                self.constant(&owner, &parts[1])
            },
            other => anyhow::bail!("config types: cannot resolve `{other:?}` in `{fqn}` to a key name"),
        }
    }

    /// The keys and types a `ConfigDef` chain in `text` (read in class `fqn`)
    /// defines, in order.
    fn defines(&mut self, fqn: &str, text: &str, out: &mut BTreeMap<String, String>) -> Res<()> {
        let mut i = 0;
        while let Some(at) = next_call(text, i) {
            let (method, args_start) = at;
            let args_end = closing_paren(text, args_start)?;
            let args = &text[args_start..args_end];
            match method {
                "define" | "defineInternal" => {
                    let parts = first_args(args, 2);
                    if parts.len() < 2 {
                        anyhow::bail!("config types: `{method}({args})` in `{fqn}` has no type argument");
                    }
                    let name = self.eval(fqn, &parse_expr(parts[0].trim())?)?;
                    let ty = parts[1].trim().rsplit('.').next().unwrap_or_default().to_string();
                    if !TYPES.contains(&ty.as_str()) {
                        anyhow::bail!("config types: `{name}` in `{fqn}` has unknown type `{}`", parts[1].trim());
                    }
                    out.insert(name, ty);
                },
                // ConfigDef.withClientSslSupport() / withClientSaslSupport() call
                // SslConfigs.addClientSslSupport(this) / SaslConfigs.addClientSaslSupport(this).
                "withClientSslSupport" => self.support_in(SSL_CONFIGS, "addClientSslSupport", out)?,
                "withClientSaslSupport" => self.support_in(SASL_CONFIGS, "addClientSaslSupport", out)?,
                _ => {},
            }
            i = args_end;
        }
        Ok(())
    }

    fn support_in(&mut self, fqn: &str, method: &str, out: &mut BTreeMap<String, String>) -> Res<()> {
        let text = self.load(fqn)?.text.clone();
        let marker = format!("static void {method}(");
        let at = text
            .find(&marker)
            .ok_or_else(|| anyhow::anyhow!("config types: `{fqn}` has no `{method}`"))?;
        let open = text[at..].find('{').map(|o| at + o).ok_or_else(|| anyhow::anyhow!("no body"))?;
        let close = closing_brace(&text, open + 1)?;
        let body = text[open + 1..close].to_string();
        self.defines(fqn, &body, out)
    }
}

fn parse_source(text: &str) -> Source {
    let mut package = String::new();
    let mut imports = BTreeMap::new();
    let mut static_imports = BTreeMap::new();
    let mut parent = None;
    for line in text.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("package ") {
            package = rest.trim_end_matches(';').trim().to_string();
        } else if let Some(rest) = line.strip_prefix("import static ") {
            let fqn = rest.trim_end_matches(';').trim();
            if let Some((owner, member)) = fqn.rsplit_once('.') {
                static_imports.insert(member.to_string(), owner.to_string());
            }
        } else if let Some(rest) = line.strip_prefix("import ") {
            let fqn = rest.trim_end_matches(';').trim();
            if let Some(simple) = fqn.rsplit('.').next() {
                imports.insert(simple.to_string(), fqn.to_string());
            }
        } else if parent.is_none() && line.starts_with("public class ") {
            parent = line
                .split_once(" extends ")
                .and_then(|(_, rest)| rest.split(|c: char| !(c.is_alphanumeric() || c == '_')).next())
                .map(str::to_string);
        }
    }
    let mut constants = BTreeMap::new();
    let marker = "static final String ";
    let mut i = 0;
    while let Some(at) = text[i..].find(marker) {
        let start = i + at + marker.len();
        let rest = &text[start..];
        let name_len = rest.find(|c: char| !(c.is_alphanumeric() || c == '_')).unwrap_or(rest.len());
        let name = rest[..name_len].to_string();
        let after = rest[name_len..].trim_start();
        if let Some(init) = after.strip_prefix('=') {
            let end = statement_end(init);
            constants.insert(name, init[..end].trim().to_string());
        }
        i = start + name_len;
    }
    Source { package, imports, static_imports, parent, constants, text: text.to_string() }
}

/// The byte offset of the `;` ending the statement that starts `s`.
fn statement_end(s: &str) -> usize {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'"' => i = skip_string(b, i),
            b';' => return i,
            _ => {},
        }
        i += 1;
    }
    b.len()
}

/// The index of the closing quote of the string literal opening at `i`.
fn skip_string(b: &[u8], mut i: usize) -> usize {
    let quote = b[i];
    i += 1;
    while i < b.len() && b[i] != quote {
        if b[i] == b'\\' {
            i += 1;
        }
        i += 1;
    }
    i
}

/// The next `.define(` / `.defineInternal(` / `.withClient…Support(` call at or
/// after `from`: the method name and the offset just after its `(`.
fn next_call(text: &str, from: usize) -> Option<(&'static str, usize)> {
    const METHODS: &[&str] = &[
        "define",
        "defineInternal",
        "withClientSslSupport",
        "withClientSaslSupport",
    ];
    let b = text.as_bytes();
    let mut i = from;
    while i < b.len() {
        match b[i] {
            b'"' => i = skip_string(b, i),
            b'.' => {
                for m in METHODS {
                    let end = i + 1 + m.len();
                    if text.get(i + 1..end) == Some(m) && b.get(end) == Some(&b'(') {
                        return Some((m, end + 1));
                    }
                }
            },
            _ => {},
        }
        i += 1;
    }
    None
}

/// The offset of the `)` closing the call whose arguments start at `start`.
fn closing_paren(text: &str, start: usize) -> Res<usize> {
    closing(text, start, b'(', b')')
}

fn closing_brace(text: &str, start: usize) -> Res<usize> {
    closing(text, start, b'{', b'}')
}

fn closing(text: &str, start: usize, open: u8, close: u8) -> Res<usize> {
    let b = text.as_bytes();
    let mut depth = 1;
    let mut i = start;
    while i < b.len() {
        let c = b[i];
        if c == b'"' || c == b'\'' {
            i = skip_string(b, i);
        } else if c == open {
            depth += 1;
        } else if c == close {
            depth -= 1;
            if depth == 0 {
                return Ok(i);
            }
        }
        i += 1;
    }
    anyhow::bail!("config types: unbalanced `{}`", open as char)
}

/// The first `n` top-level comma-separated arguments of `args`.
fn first_args(args: &str, n: usize) -> Vec<&str> {
    let b = args.as_bytes();
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;
    let mut i = 0;
    while i < b.len() && parts.len() < n {
        match b[i] {
            b'"' | b'\'' => i = skip_string(b, i),
            b'(' | b'{' | b'[' => depth += 1,
            b')' | b'}' | b']' => depth -= 1,
            b',' if depth == 0 => {
                parts.push(&args[start..i]);
                start = i + 1;
            },
            _ => {},
        }
        i += 1;
    }
    if parts.len() < n && start < args.len() {
        parts.push(&args[start..]);
    }
    parts
}

/// Every key of a client's `ConfigDef`, with its type.
fn client_types(reader: &mut Reader, fqn: &str) -> Res<BTreeMap<String, String>> {
    let text = reader.load(fqn)?.text.clone();
    let at = text
        .find("CONFIG = new ConfigDef()")
        .ok_or_else(|| anyhow::anyhow!("config types: `{fqn}` builds no `CONFIG`"))?;
    let end = at + statement_end(&text[at..]);
    let mut out = BTreeMap::new();
    reader.defines(fqn, &text[at..=end.min(text.len() - 1)], &mut out)?;
    if out.is_empty() {
        anyhow::bail!("config types: `{fqn}` defines no keys");
    }
    Ok(out)
}

fn render(producer: &BTreeMap<String, String>, consumer: &BTreeMap<String, String>) -> String {
    let mut out = String::from(
        "# Copyright 2025 Confluent Inc.\n\
         #\n\
         # Licensed under the Apache License, Version 2.0 (the \"License\");\n\
         # you may not use this file except in compliance with the License.\n\
         # You may obtain a copy of the License at\n\
         #\n\
         #     http://www.apache.org/licenses/LICENSE-2.0\n\
         #\n\
         # Unless required by applicable law or agreed to in writing, software\n\
         # distributed under the License is distributed on an \"AS IS\" BASIS,\n\
         # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n\
         # See the License for the specific language governing permissions and\n\
         # limitations under the License.\n\
         \n\
         # GENERATED by `cargo xtask generate-error-codes` from Java's ProducerConfig and\n\
         # ConsumerConfig -- DO NOT EDIT.\n\
         \n\
         \"\"\"The ``ConfigDef`` type of every producer and consumer config key.\n\
         \n\
         Java's ``ProducerConfig`` / ``ConsumerConfig`` define these keys, with\n\
         ``withClientSslSupport()`` / ``withClientSaslSupport()``; ``_config`` coerces\n\
         a value to its key's type and passes every other key to the core as given.\n\
         \"\"\"\n\
         \n\
         from __future__ import annotations\n\
         \n\
         __all__ = [\"CONSUMER\", \"PRODUCER\"]\n",
    );
    for (name, map) in [("PRODUCER", producer), ("CONSUMER", consumer)] {
        out.push_str(&format!("\n{name}: dict[str, str] = {{\n"));
        for (key, ty) in map {
            out.push_str(&format!("    \"{key}\": \"{ty}\",\n"));
        }
        out.push_str("}\n");
    }
    out
}

/// The generated file and its content.
pub fn generated_output(repo_root: &Path) -> Res<(PathBuf, String)> {
    let mut reader = Reader { root: repo_root.to_path_buf(), cache: BTreeMap::new() };
    let producer = client_types(&mut reader, "org.apache.kafka.clients.producer.ProducerConfig")?;
    let consumer = client_types(&mut reader, "org.apache.kafka.clients.consumer.ConsumerConfig")?;
    Ok((repo_root.join(OUTPUT), render(&producer, &consumer)))
}

pub fn generate(repo_root: &Path) -> Res<()> {
    let (path, content) = generated_output(repo_root)?;
    fs::write(&path, content)?;
    println!("✅ Wrote {}", path.display());
    Ok(())
}

/// The generated file when it is stale.
pub fn check_up_to_date(repo_root: &Path) -> Res<Option<PathBuf>> {
    let (path, expected) = generated_output(repo_root)?;
    match fs::read_to_string(&path) {
        Ok(actual) if actual == expected => Ok(None),
        _ => Ok(Some(path)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap()
            .to_path_buf()
    }

    #[test]
    fn test_reads_java_key_types() {
        let (_, content) = generated_output(&repo_root()).unwrap();
        for line in [
            "    \"bootstrap.servers\": \"LIST\",",
            "    \"acks\": \"STRING\",",
            "    \"buffer.memory\": \"LONG\",",
            "    \"enable.idempotence\": \"BOOLEAN\",",
            "    \"key.serializer\": \"CLASS\",",
            "    \"ssl.keystore.password\": \"PASSWORD\",",
            "    \"sasl.mechanism\": \"STRING\",",
            "    \"max.poll.records\": \"INT\",",
            "    \"internal.throw.on.fetch.stable.offset.unsupported\": \"BOOLEAN\",",
        ] {
            assert!(content.contains(line), "missing {line}");
        }
    }

    #[test]
    fn test_first_args_and_calls() {
        assert_eq!(first_args("A, Type.INT, between(0, 1), \"a,b\"", 2), vec!["A", " Type.INT"]);
        let text = "x.define(A, Type.INT).defineInternal(B, Type.LIST) \".define(\"";
        let (m, at) = next_call(text, 0).unwrap();
        assert_eq!((m, &text[at..at + 1]), ("define", "A"));
        let (m, _) = next_call(text, at).unwrap();
        assert_eq!(m, "defineInternal");
    }
}
