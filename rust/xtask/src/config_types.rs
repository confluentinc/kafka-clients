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

//! The `ConfigDef` type of every producer, consumer and admin config key, read
//! from the Java sources.
//!
//! The Python clients check a config value's Python type against
//! its key's `ConfigDef` type before the core sees it (CLAUDE.md, Python Binding
//! Conventions, Configuration). The key sets and types are those of Java's `ProducerConfig`,
//! `ConsumerConfig` and `AdminClientConfig`: every `define(name, Type.X, …)` /
//! `defineInternal(name, Type.X, …)` of the class, plus the ones
//! `withClientSslSupport()` / `withClientSaslSupport()` add from `SslConfigs` /
//! `SaslConfigs`. A key name is a `static final String` constant, resolved
//! through the class's imports; anything this reader cannot resolve stops
//! generation instead of being guessed.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::config_consts;
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

/// One key of a `ConfigDef`: its type, the class whose source defines it and
/// its validator's source, if any.
struct KeyDef {
    ty: String,
    class: String,
    validator: Option<String>,
}

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
    fn defines(&mut self, fqn: &str, text: &str, out: &mut BTreeMap<String, KeyDef>) -> Res<()> {
        let mut i = 0;
        while let Some(at) = next_call(text, i) {
            let (method, args_start) = at;
            let args_end = closing_paren(text, args_start)?;
            let args = &text[args_start..args_end];
            match method {
                "define" | "defineInternal" => {
                    let parts = first_args(args, usize::MAX);
                    if parts.len() < 2 {
                        anyhow::bail!("config types: `{method}({args})` in `{fqn}` has no type argument");
                    }
                    let name = self.eval(fqn, &parse_expr(parts[0].trim())?)?;
                    let ty = parts[1].trim().rsplit('.').next().unwrap_or_default().to_string();
                    if !TYPES.contains(&ty.as_str()) {
                        anyhow::bail!("config types: `{name}` in `{fqn}` has unknown type `{}`", parts[1].trim());
                    }
                    // `ConfigDef.define(name, type, [default, [validator,]] importance, ...)`:
                    // the validator is the argument between the default and the importance.
                    let importance = parts
                        .iter()
                        .position(|p| p.contains("Importance."))
                        .ok_or_else(|| anyhow::anyhow!("config types: `{name}` in `{fqn}` has no importance"))?;
                    let validator = match importance {
                        2 | 3 => None,
                        4 => Some(parts[3].split_whitespace().collect::<Vec<_>>().join(" ")),
                        _ => anyhow::bail!("config types: `{name}` in `{fqn}` has an unknown `{method}` form"),
                    };
                    out.insert(name, KeyDef { ty, class: fqn.to_string(), validator });
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

    fn support_in(&mut self, fqn: &str, method: &str, out: &mut BTreeMap<String, KeyDef>) -> Res<()> {
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
                // Java allows whitespace after the `.` (`AdminClientConfig` breaks
                // a chain as `...DOC).` then `define(...` on the next line).
                let mut name = i + 1;
                while b.get(name).is_some_and(u8::is_ascii_whitespace) {
                    name += 1;
                }
                for m in METHODS {
                    let end = name + m.len();
                    if text.get(name..end) == Some(m) && b.get(end) == Some(&b'(') {
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
fn client_types(reader: &mut Reader, fqn: &str) -> Res<BTreeMap<String, KeyDef>> {
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

/// What Java's `ConfigDef.parseValue` does with an explicit `null` for a key:
/// it runs the key's validator on it (CLAUDE.md, Python Binding Conventions,
/// Configuration).
#[derive(Clone, Debug, PartialEq)]
enum NullRule {
    /// No validator, or one that accepts `null`: Java holds `null`.
    Allowed,
    /// `ConfigException(name, null, message)`.
    Named { kind: &'static str, message: String },
    /// `ConfigException(message)` (the one-argument form).
    Plain { kind: &'static str, message: String },
}

/// The body of the first `{...}` block after `marker` in `text`.
fn block_after<'a>(text: &'a str, marker: &str) -> Res<&'a str> {
    let at = text
        .find(marker)
        .ok_or_else(|| anyhow::anyhow!("config types: no `{marker}`"))?;
    let open = text[at..]
        .find('{')
        .map(|o| at + o)
        .ok_or_else(|| anyhow::anyhow!("config types: no body after `{marker}`"))?;
    let close = closing_brace(text, open + 1)?;
    Ok(&text[open + 1..close])
}

/// The strings an `in(...)` argument list evaluates to: each argument a
/// string, or an array of them (`Utils.enumOptions(X.class)`).
fn valid_strings(consts: &mut config_consts::Reader, class: &str, args: &str) -> Res<Vec<String>> {
    let mut out = Vec::new();
    for a in first_args(args, usize::MAX) {
        match consts.eval_src(class, a.trim())? {
            config_consts::Value::Str(s) => out.push(s),
            config_consts::Value::List(items) => {
                for i in items {
                    let config_consts::Value::Str(s) = i else {
                        anyhow::bail!("config types: `in({args})` holds a non-string")
                    };
                    out.push(s);
                }
            },
            v => anyhow::bail!("config types: `in({args})` holds {v:?}"),
        }
    }
    Ok(out)
}

/// The argument list of `call(...)` when `src` is exactly that call.
fn call_args<'a>(src: &'a str, call: &str) -> Option<&'a str> {
    let rest = src.strip_prefix(call)?.trim_start().strip_prefix('(')?;
    rest.strip_suffix(')')
}

/// The iteration order of a Java `HashSet` built by adding `values` in order
/// (default capacity 16, doubled while the size passes three quarters of it):
/// by bucket (`hash ^ (hash >>> 16)` masked by the table size, `hash` being
/// `String.hashCode` over UTF-16), insertion order within a bucket. The same
/// order as the core's `common::config::config_def::java_hash_set_order`.
fn java_hash_set_order(values: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut distinct: Vec<String> = Vec::new();
    for v in values {
        if !distinct.contains(&v) {
            distinct.push(v);
        }
    }
    let mut capacity: usize = 16;
    while distinct.len() * 4 > capacity * 3 {
        capacity *= 2;
    }
    let bucket = |s: &String| {
        let h = s
            .encode_utf16()
            .fold(0i32, |h, unit| h.wrapping_mul(31).wrapping_add(i32::from(unit)));
        let spread = (h ^ ((h as u32) >> 16) as i32) as u32;
        spread as usize & (capacity - 1)
    };
    distinct.sort_by_key(bucket);
    distinct
}

/// The null rule of a key from its validator's source, read as Java's
/// `ConfigDef` validators treat `null`. An unknown validator kind stops
/// generation.
fn null_rule(reader: &mut Reader, consts: &mut config_consts::Reader, key: &str, def: &KeyDef) -> Res<NullRule> {
    let Some(src) = def.validator.as_deref() else {
        return Ok(NullRule::Allowed);
    };
    let src = src.trim();
    let bare = src.replace("ConfigDef.", "").replace(" .", ".");
    let bare = bare.as_str();
    if bare == "null" {
        return Ok(NullRule::Allowed);
    }
    // `Range.atLeast` / `Range.between` (also statically imported).
    for call in ["atLeast", "between", "Range.atLeast", "Range.between"] {
        if call_args(bare, call).is_some() {
            return Ok(NullRule::Named { kind: "Range", message: "Value must be non-null".into() });
        }
    }
    // `ValidString.in(...)` (statically imported as `in`).
    for call in ["in", "ValidString.in"] {
        if let Some(args) = call_args(bare, call) {
            let strings = valid_strings(consts, &def.class, args)?;
            return Ok(NullRule::Named {
                kind: "ValidString",
                message: format!("String must be one of: {}", strings.join(", ")),
            });
        }
    }
    if let Some(args) = call_args(bare, "CaseInsensitiveValidString.in") {
        // Java collects the upper-cased strings with `Collectors.toSet()`, a
        // `HashSet`, and joins them in its iteration order (`ConfigDef.java`
        // `CaseInsensitiveValidString`), as the core's `java_hash_set_order` does.
        let strings = java_hash_set_order(valid_strings(consts, &def.class, args)?.iter().map(|s| s.to_uppercase()));
        return Ok(NullRule::Named {
            kind: "CaseInsensitiveValidString",
            message: format!("String must be one of (case insensitive): {}", strings.join(", ")),
        });
    }
    if let Some(args) = call_args(bare, "ValidList.anyNonDuplicateValues") {
        let flags = first_args(args, usize::MAX);
        let null_allowed = match flags.get(1).map(|f| f.trim()) {
            Some("true") => true,
            Some("false") => false,
            other => anyhow::bail!("config types: `{key}`: ValidList null flag {other:?}"),
        };
        return Ok(if null_allowed {
            NullRule::Allowed
        } else {
            NullRule::Plain {
                kind: "ValidList",
                message: format!("Configuration '{key}' values must not be null."),
            }
        });
    }
    if call_args(bare, "ValidList.in").is_some() {
        return Ok(NullRule::Plain {
            kind: "ValidList",
            message: format!("Configuration '{key}' values must not be null."),
        });
    }
    if bare == "new NonEmptyString()" {
        return Ok(NullRule::Allowed);
    }
    // `CompressionType.X.levelValidator()`: the constant's own validator, a
    // `Range` or a lambda refusing `null` as `Range` does.
    if let Some(rest) = bare.strip_suffix(".levelValidator()") {
        if let Some((owner, constant)) = rest.rsplit_once('.') {
            let fqn = reader.qualify(&def.class, owner)?;
            let text = reader.load(&fqn)?.text.clone();
            let body = block_after(&text, &format!("{constant}("))?;
            let method = block_after(body, "levelValidator()")?;
            let range = method.contains("between(") || method.contains("atLeast(");
            let lambda = method.contains("if (value == null)")
                && method.contains("throw new ConfigException(name, null, \"Value must be non-null\")");
            if range || lambda {
                return Ok(NullRule::Named { kind: "Range", message: "Value must be non-null".into() });
            }
        }
        anyhow::bail!("config types: `{key}`: unknown level validator `{src}`")
    }
    // `new X.Validator()`: an enum's validator that parses the string in a
    // `try` and throws `ConfigException(name, value, message)` from the `catch`;
    // `null` reaches the `catch` when the parser rejects `null`.
    if let Some(owner) = bare.strip_prefix("new ").and_then(|r| r.strip_suffix(".Validator()")) {
        let fqn = reader.qualify(&def.class, owner)?;
        let text = reader.load(&fqn)?.text.clone();
        let validator = block_after(&text, "class Validator")?;
        let ensure = block_after(validator, "ensureValid(String name, Object value)")?;
        let stmts: Vec<&str> = ensure.split(';').map(str::trim).collect();
        let local = stmts
            .first()
            .and_then(|s| s.strip_prefix("String "))
            .and_then(|s| s.strip_suffix("= (String) value").or_else(|| s.split_once('=').map(|(l, _)| l)))
            .map(str::trim)
            .ok_or_else(|| anyhow::anyhow!("config types: `{fqn}.Validator` does not cast the value"))?;
        let try_body = block_after(ensure, "try")?;
        let parser = try_body.trim().split('(').next().unwrap_or_default().trim().to_string();
        let catch = block_after(ensure, "catch (Exception e)")?;
        let throw = catch
            .trim()
            .strip_prefix("throw new ConfigException(name, value,")
            .and_then(|r| r.trim_end().strip_suffix(");"))
            .ok_or_else(|| anyhow::anyhow!("config types: `{fqn}.Validator` throws an unknown form"))?;
        // The parser must reject null with an explicit check.
        let parser_body = block_after(&text, &format!(" {parser}("))?;
        let first = parser_body.trim();
        let rejects_null = first.starts_with("if (")
            && first.split_once(')').is_some_and(|(cond, rest)| {
                cond.trim_start_matches("if (").trim().ends_with("== null")
                    && rest.trim_start().trim_start_matches('{').trim_start().starts_with("throw ")
            });
        if !rejects_null {
            anyhow::bail!("config types: `{fqn}.{parser}` does not reject null");
        }
        // The message as Python builds it, `null` spelled `None` (Types).
        let message = consts.eval_src_with(&fqn, throw.trim(), &[(local, "None"), ("name", key)])?;
        return Ok(NullRule::Named { kind: "custom Validator", message });
    }
    anyhow::bail!("config types: `{key}` in `{}` has an unknown validator `{src}`", def.class)
}

/// The clients whose tables are generated: the Python table name and the Java config class.
const CLIENTS: [(&str, &str); 3] = [
    ("PRODUCER", "org.apache.kafka.clients.producer.ProducerConfig"),
    ("CONSUMER", "org.apache.kafka.clients.consumer.ConsumerConfig"),
    ("ADMIN", "org.apache.kafka.clients.admin.AdminClientConfig"),
];

type Table = BTreeMap<String, (String, NullRule)>;

fn render(tables: &[(&str, Table)]) -> String {
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
         # GENERATED by `cargo xtask generate-error-codes` from Java's ProducerConfig,\n\
         # ConsumerConfig and AdminClientConfig -- DO NOT EDIT.\n\
         \n\
         \"\"\"The ``ConfigDef`` type of every producer, consumer and admin config key,\n\
         and what its validator does with ``null``.\n\
         \n\
         Java's ``ProducerConfig`` / ``ConsumerConfig`` / ``AdminClientConfig`` define\n\
         these keys, with ``withClientSslSupport()`` / ``withClientSaslSupport()``;\n\
         ``_config`` checks a value's Python type against its key's type and passes\n\
         every other key to the core as given.\n\
         \n\
         ``<CLIENT>_NULL`` maps each key whose ``ConfigDef`` validator rejects an explicit\n\
         ``null`` (``ConfigDef.parseValue`` runs the validator on it) to the\n\
         ``ConfigException`` Java throws: ``(\"named\", message)`` for\n\
         ``ConfigException(name, null, message)``, ``(\"plain\", message)`` for\n\
         ``ConfigException(message)``. A key that is not there holds ``null`` in Java.\n\
         \"\"\"\n\
         \n\
         from __future__ import annotations\n\
         \n\
         from typing import Literal\n\
         \n\
         __all__ = [\"ADMIN\", \"ADMIN_NULL\", \"CONSUMER\", \"CONSUMER_NULL\", \"PRODUCER\", \"PRODUCER_NULL\"]\n",
    );
    for (name, map) in tables {
        out.push_str(&format!("\n{name}: dict[str, str] = {{\n"));
        for (key, (ty, _)) in map {
            out.push_str(&format!("    \"{key}\": \"{ty}\",\n"));
        }
        out.push_str("}\n");
    }
    for (name, map) in tables {
        out.push_str(&format!(
            "\n{name}_NULL: dict[str, tuple[Literal[\"named\", \"plain\"], str]] = {{\n"
        ));
        for (key, (_, rule)) in map {
            let (form, kind, message) = match rule {
                NullRule::Allowed => continue,
                NullRule::Named { kind, message } => ("named", kind, message),
                NullRule::Plain { kind, message } => ("plain", kind, message),
            };
            out.push_str(&format!(
                "    \"{key}\": (\"{form}\", {}),  # {kind}\n",
                config_consts::py_str(message)
            ));
        }
        out.push_str("}\n");
    }
    out
}

/// The generated file and its content.
pub fn generated_output(repo_root: &Path) -> Res<(PathBuf, String)> {
    let mut reader = Reader { root: repo_root.to_path_buf(), cache: BTreeMap::new() };
    let mut consts = config_consts::Reader::new(repo_root);
    let mut tables = Vec::with_capacity(CLIENTS.len());
    for (name, fqn) in CLIENTS {
        let mut table = Table::new();
        for (key, def) in client_types(&mut reader, fqn)? {
            let rule = null_rule(&mut reader, &mut consts, &key, &def)?;
            table.insert(key, (def.ty, rule));
        }
        tables.push((name, table));
    }
    Ok((repo_root.join(OUTPUT), render(&tables)))
}

/// Every generated file: the key-type tables and the config classes'
/// constants (`config_consts`).
fn all_outputs(repo_root: &Path) -> Res<Vec<(PathBuf, String)>> {
    let mut out = vec![generated_output(repo_root)?];
    out.extend(crate::config_consts::outputs(repo_root)?);
    Ok(out)
}

pub fn generate(repo_root: &Path) -> Res<()> {
    for (path, content) in all_outputs(repo_root)? {
        fs::write(&path, content)?;
        println!("✅ Wrote {}", path.display());
    }
    Ok(())
}

/// The generated files that are stale.
pub fn check_up_to_date(repo_root: &Path) -> Res<Vec<PathBuf>> {
    let mut stale = Vec::new();
    for (path, expected) in all_outputs(repo_root)? {
        match fs::read_to_string(&path) {
            Ok(actual) if actual == expected => {},
            _ => stale.push(path),
        }
    }
    Ok(stale)
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
            "    \"bootstrap.controllers\": \"LIST\",",
            "ADMIN: dict[str, str] = {",
        ] {
            assert!(content.contains(line), "missing {line}");
        }
    }

    #[test]
    fn test_reads_java_validators_null_rules() {
        let (_, content) = generated_output(&repo_root()).unwrap();
        for line in [
            "    \"acks\": (\"named\", \"String must be one of: all, -1, 0, 1\"),  # ValidString",
            "    \"linger.ms\": (\"named\", \"Value must be non-null\"),  # Range",
            "    \"compression.gzip.level\": (\"named\", \"Value must be non-null\"),  # Range",
            "    \"bootstrap.servers\": (\"plain\", \"Configuration 'bootstrap.servers' values must not be null.\"),  # ValidList",
            "    \"security.protocol\": (\"named\", \"String must be one of (case insensitive): SASL_SSL, PLAINTEXT, SSL, SASL_PLAINTEXT\"),  # CaseInsensitiveValidString",
        ] {
            assert!(content.contains(line), "missing {line}");
        }
        // NonEmptyString and keys without a validator accept null.
        let producer_null = content.split("\nPRODUCER_NULL:").nth(1).unwrap().split("\n}").next().unwrap();
        assert!(!producer_null.contains("\"transactional.id\""));
        assert!(!producer_null.contains("\"client.id\""));
    }

    #[test]
    fn test_first_args_and_calls() {
        assert_eq!(first_args("A, Type.INT, between(0, 1), \"a,b\"", 2), vec!["A", " Type.INT"]);
        let text = "x.define(A, Type.INT).defineInternal(B, Type.LIST) \".define(\"";
        let (m, at) = next_call(text, 0).unwrap();
        assert_eq!((m, &text[at..at + 1]), ("define", "A"));
        let (m, _) = next_call(text, at).unwrap();
        assert_eq!(m, "defineInternal");
        let text = "x.define(A, Type.LIST).\n    define(B, Type.LIST)";
        let (_, at) = next_call(text, 0).unwrap();
        let (m, at) = next_call(text, at).unwrap();
        assert_eq!((m, &text[at..at + 1]), ("define", "B"));
    }
}
