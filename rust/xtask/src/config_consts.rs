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

//! The `public static final` constants of Java's config classes, evaluated from
//! the Java sources (CLAUDE.md, Python Binding Conventions, Scope: the config
//! classes are translated for their constants).
//!
//! A small reader of Java classes (fields, nested types, enum constants with
//! their constructor arguments, `toString` overrides) and an evaluator of the
//! constant expressions those classes use: literals, `+` / `-` / `*`, casts,
//! qualified names, enum constants, `.name()` / `.toString()` /
//! `.toLowerCase(Locale.ROOT)`, `List.of` / `Set.of`, `Utils.enumOptions(X.class)`.
//! Anything it cannot evaluate stops generation instead of being guessed.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::java_parse::strip_comments;

type Res<T> = anyhow::Result<T>;

const JAVA_ROOT: &str = "kafka/clients/src/main/java";

/// JDK calls whose value is the JVM's at run time, read as OpenJDK's defaults
/// (the values the Rust core uses: `rust/src/common/config/ssl_configs.rs`).
const JDK_RUNTIME: &[(&str, &str)] = &[
    ("javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm", "SunX509"),
    ("javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm", "PKIX"),
];

/// A Java constant value.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Str(String),
    Int(i64),
    Double(f64),
    Bool(bool),
    Null,
    /// A `List` (ordered).
    List(Vec<Value>),
    /// A `Set`.
    Set(Vec<Value>),
    /// A class reference (`X` in `X.FOO`, or `X.class`).
    Class(String),
    /// An enum constant.
    Enum {
        class: String,
        name: String,
    },
}

impl Value {
    /// Java's `String.valueOf(value)`, for string concatenation.
    fn java_string(&self, cx: &mut Reader) -> Res<String> {
        Ok(match self {
            Value::Str(s) => s.clone(),
            Value::Int(n) => n.to_string(),
            Value::Double(d) => java_double(*d)?,
            Value::Bool(b) => b.to_string(),
            Value::Null => "null".into(),
            Value::Enum { class, name } => cx.enum_to_string(class, name)?,
            other => anyhow::bail!("config constants: cannot concatenate {other:?}"),
        })
    }
}

/// Java's `Double.toString` for the plain decimal range.
fn java_double(d: f64) -> Res<String> {
    let a = d.abs();
    if !(a == 0.0 || (1e-3..1e7).contains(&a)) {
        anyhow::bail!("config constants: double {d} outside Double.toString's plain range");
    }
    let s = format!("{d}");
    Ok(if s.contains('.') { s } else { s + ".0" })
}

/// A Java field: its declared type, initializer and modifiers.
#[derive(Clone, Debug)]
pub struct Field {
    pub name: String,
    pub init: Option<String>,
    pub public: bool,
    pub static_final: bool,
}

/// An enum constant: its name, its constructor arguments' sources.
#[derive(Clone, Debug)]
struct EnumConst {
    name: String,
    args: Vec<String>,
}

/// One Java class, enum or interface.
#[derive(Clone, Debug, Default)]
pub struct JClass {
    pub fqn: String,
    pub is_enum: bool,
    pub parent: Option<String>,
    /// Fields in declaration order.
    pub fields: Vec<Field>,
    nested: BTreeMap<String, JClass>,
    constants: Vec<EnumConst>,
    /// The first constructor's parameter names.
    ctor_params: Vec<String>,
    /// `this.FIELD = expr;` in that constructor.
    ctor_assign: BTreeMap<String, String>,
    /// The `return` expression of `toString()`, if overridden.
    to_string: Option<String>,
    /// The public methods and constructors, as `name(T1, T2)`.
    pub public_methods: Vec<String>,
}

struct Unit {
    package: String,
    imports: BTreeMap<String, String>,
    static_imports: BTreeMap<String, String>,
}

pub struct Reader {
    root: PathBuf,
    units: BTreeMap<String, Unit>,
    classes: BTreeMap<String, JClass>,
}

impl Reader {
    pub fn new(root: &Path) -> Self {
        Reader { root: root.to_path_buf(), units: BTreeMap::new(), classes: BTreeMap::new() }
    }

    /// The source file of a top-level class `fqn`, if it exists.
    fn file_of(&self, fqn: &str) -> PathBuf {
        self.root.join(JAVA_ROOT).join(format!("{}.java", fqn.replace('.', "/")))
    }

    /// Load the top-level class `top` and its nested classes.
    fn load_top(&mut self, top: &str) -> Res<()> {
        if self.units.contains_key(top) {
            return Ok(());
        }
        let path = self.file_of(top);
        let raw = fs::read_to_string(&path).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
        let text = strip_comments(&raw);
        let mut unit = Unit {
            package: String::new(),
            imports: BTreeMap::new(),
            static_imports: BTreeMap::new(),
        };
        for line in text.lines() {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("package ") {
                unit.package = rest.trim_end_matches(';').trim().to_string();
            } else if let Some(rest) = line.strip_prefix("import static ") {
                let f = rest.trim_end_matches(';').trim();
                if let Some((owner, member)) = f.rsplit_once('.') {
                    unit.static_imports.insert(member.to_string(), owner.to_string());
                }
            } else if let Some(rest) = line.strip_prefix("import ") {
                let f = rest.trim_end_matches(';').trim();
                if let Some((_, simple)) = f.rsplit_once('.') {
                    unit.imports.insert(simple.to_string(), f.to_string());
                }
            }
        }
        let simple = top.rsplit('.').next().unwrap_or(top);
        let class = parse_type(&text, simple, top)?;
        let mut stack = vec![class];
        while let Some(c) = stack.pop() {
            stack.extend(c.nested.values().cloned());
            self.classes.insert(c.fqn.clone(), c);
        }
        self.units.insert(top.to_string(), unit);
        Ok(())
    }

    /// The class `fqn` (a nested one as `a.b.Outer.Inner`).
    pub fn class(&mut self, fqn: &str) -> Res<JClass> {
        if !self.classes.contains_key(fqn) {
            // Find the top-level class: the longest prefix with a source file.
            let parts: Vec<&str> = fqn.split('.').collect();
            let mut loaded = false;
            for n in (1..=parts.len()).rev() {
                let top = parts[..n].join(".");
                if self.file_of(&top).is_file() {
                    self.load_top(&top)?;
                    loaded = true;
                    break;
                }
            }
            if !loaded {
                anyhow::bail!("config constants: no Java source for `{fqn}`");
            }
        }
        self.classes
            .get(fqn)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("config constants: no class `{fqn}`"))
    }

    /// The top-level class `fqn` is declared in.
    fn top_of(&self, fqn: &str) -> String {
        let parts: Vec<&str> = fqn.split('.').collect();
        for n in 1..=parts.len() {
            let top = parts[..n].join(".");
            if self.units.contains_key(&top) {
                return top;
            }
        }
        fqn.to_string()
    }

    /// Whether `fqn` names a class with a Java source (or a nested class of one).
    fn class_exists(&mut self, fqn: &str) -> bool {
        self.class(fqn).is_ok()
    }

    /// The class the simple name `name` refers to inside class `from`.
    fn resolve_class(&mut self, from: &str, name: &str) -> Res<Option<String>> {
        // A nested class of `from` or of an enclosing class.
        let mut scope = Some(from.to_string());
        while let Some(s) = scope {
            let c = self.class(&s)?;
            if c.nested.contains_key(name) {
                return Ok(Some(format!("{s}.{name}")));
            }
            scope = s
                .rsplit_once('.')
                .map(|(p, _)| p.to_string())
                .filter(|p| self.classes.contains_key(p));
        }
        let top = self.top_of(from);
        let unit = &self.units[&top];
        if let Some(f) = unit.imports.get(name).cloned() {
            return Ok(Some(f));
        }
        let same = format!("{}.{name}", unit.package);
        if self.file_of(&same).is_file() {
            return Ok(Some(same));
        }
        for (j, f) in [
            ("Integer", "java.lang.Integer"),
            ("Long", "java.lang.Long"),
            ("Short", "java.lang.Short"),
            ("String", "java.lang.String"),
            ("List", "java.util.List"),
            ("Set", "java.util.Set"),
        ] {
            if name == j {
                return Ok(Some(f.to_string()));
            }
        }
        Ok(None)
    }

    /// The value of constant `name` seen from class `fqn`: its own field, its
    /// enum constant, a static import, an enclosing class's, or a superclass's.
    fn constant(&mut self, fqn: &str, name: &str) -> Res<Option<Value>> {
        let c = self.class(fqn)?;
        if let Some(f) = c.fields.iter().find(|f| f.name == name && f.static_final) {
            let init = f
                .init
                .clone()
                .ok_or_else(|| anyhow::anyhow!("config constants: `{fqn}.{name}` has no initializer"))?;
            return Ok(Some(self.eval_src(fqn, &init)?));
        }
        if c.is_enum && c.constants.iter().any(|k| k.name == name) {
            return Ok(Some(Value::Enum { class: fqn.to_string(), name: name.to_string() }));
        }
        let top = self.top_of(fqn);
        if let Some(owner) = self.units[&top].static_imports.get(name).cloned() {
            return self.constant(&owner, name);
        }
        if let Some((outer, _)) = fqn.rsplit_once('.') {
            if self.classes.contains_key(outer) {
                if let Some(v) = self.constant(outer, name)? {
                    return Ok(Some(v));
                }
            }
        }
        if let Some(parent) = c.parent.clone() {
            if let Some(p) = self.resolve_class(fqn, &parent)? {
                if self.class_exists(&p) {
                    return self.constant(&p, name);
                }
            }
        }
        Ok(None)
    }

    /// Evaluate the `String` expression `src` in class `fqn` with string locals.
    pub fn eval_src_with(&mut self, fqn: &str, src: &str, locals: &[(&str, &str)]) -> Res<String> {
        let ast = parse(src)?;
        let locals = locals.iter().map(|(k, v)| (k.to_string(), Value::Str(v.to_string()))).collect();
        match self.eval(fqn, &ast, &locals)? {
            Value::Str(s) => Ok(s),
            v => anyhow::bail!("config constants: `{src}` is {v:?}, not a string"),
        }
    }

    /// Evaluate the Java expression `src` in class `fqn`.
    pub fn eval_src(&mut self, fqn: &str, src: &str) -> Res<Value> {
        let ast = parse(src)?;
        self.eval(fqn, &ast, &BTreeMap::new())
    }

    fn eval(&mut self, fqn: &str, ast: &Ast, locals: &BTreeMap<String, Value>) -> Res<Value> {
        match ast {
            Ast::Lit(v) => Ok(v.clone()),
            Ast::Neg(a) => match self.eval(fqn, a, locals)? {
                Value::Int(n) => Ok(Value::Int(-n)),
                Value::Double(d) => Ok(Value::Double(-d)),
                v => anyhow::bail!("config constants: cannot negate {v:?}"),
            },
            Ast::Cast(ty, a) => {
                let v = self.eval(fqn, a, locals)?;
                match (ty.as_str(), v) {
                    ("short" | "byte" | "int" | "long", Value::Int(n)) => Ok(Value::Int(n)),
                    ("double" | "float", Value::Int(n)) => Ok(Value::Double(n as f64)),
                    ("double" | "float", Value::Double(d)) => Ok(Value::Double(d)),
                    (t, v) => anyhow::bail!("config constants: cannot cast {v:?} to {t}"),
                }
            },
            Ast::Bin(op, a, b) => {
                let a = self.eval(fqn, a, locals)?;
                let b = self.eval(fqn, b, locals)?;
                match (op, &a, &b) {
                    ('+', Value::Str(_), _) | ('+', _, Value::Str(_)) => {
                        Ok(Value::Str(a.java_string(self)? + &b.java_string(self)?))
                    },
                    (_, Value::Int(x), Value::Int(y)) => Ok(Value::Int(match op {
                        '+' => x + y,
                        '-' => x - y,
                        '*' => x * y,
                        _ => anyhow::bail!("config constants: operator {op}"),
                    })),
                    _ => anyhow::bail!("config constants: cannot apply {op} to {a:?}, {b:?}"),
                }
            },
            Ast::Ident(name) => {
                if let Some(v) = locals.get(name) {
                    return Ok(v.clone());
                }
                if let Some(v) = self.constant(fqn, name)? {
                    return Ok(v);
                }
                if let Some(c) = self.resolve_class(fqn, name)? {
                    return Ok(Value::Class(c));
                }
                // A package prefix of a fully qualified name (`java.util.X`).
                Ok(Value::Class(name.clone()))
            },
            Ast::Field(target, name) => {
                let t = self.eval(fqn, target, locals)?;
                match t {
                    Value::Class(c) if name == "class" => Ok(Value::Class(c)),
                    Value::Class(c) => {
                        match c.as_str() {
                            "java.lang.Integer" if name == "MAX_VALUE" => return Ok(Value::Int(i32::MAX as i64)),
                            "java.lang.Long" if name == "MAX_VALUE" => return Ok(Value::Int(i64::MAX)),
                            "java.lang.Short" if name == "MAX_VALUE" => return Ok(Value::Int(i16::MAX as i64)),
                            _ => {},
                        }
                        if self.class_exists(&c) {
                            if let Some(v) = self.constant(&c, name)? {
                                return Ok(v);
                            }
                            let nested = format!("{c}.{name}");
                            if self.class_exists(&nested) {
                                return Ok(Value::Class(nested));
                            }
                            anyhow::bail!("config constants: `{c}` has no constant `{name}`")
                        }
                        // Still a package prefix.
                        Ok(Value::Class(format!("{c}.{name}")))
                    },
                    Value::Enum { class, name: k } => self.enum_field(&class, &k, name),
                    v => anyhow::bail!("config constants: no field `{name}` on {v:?}"),
                }
            },
            Ast::Call(target, name, args) => {
                let t = self.eval(fqn, target, locals)?;
                let mut vals = Vec::with_capacity(args.len());
                for a in args {
                    vals.push(self.eval(fqn, a, locals)?);
                }
                self.call(t, name, vals)
            },
        }
    }

    fn call(&mut self, target: Value, name: &str, args: Vec<Value>) -> Res<Value> {
        match (&target, name) {
            (Value::Class(c), "of") if c == "java.util.List" => Ok(Value::List(args)),
            (Value::Class(c), "of") if c == "java.util.Set" => Ok(Value::Set(args)),
            (Value::Class(c), "enumOptions") if c == "org.apache.kafka.common.utils.Utils" => {
                let [Value::Class(e)] = args.as_slice() else {
                    anyhow::bail!("config constants: Utils.enumOptions({args:?})")
                };
                let class = self.class(e)?;
                let mut out = Vec::new();
                for k in &class.constants {
                    out.push(Value::Str(self.enum_to_string(e, &k.name)?));
                }
                Ok(Value::List(out))
            },
            (Value::Class(c), m) => {
                let key = format!("{c}.{m}");
                if let Some((_, v)) = JDK_RUNTIME.iter().find(|(k, _)| *k == key) {
                    if args.is_empty() {
                        return Ok(Value::Str((*v).to_string()));
                    }
                }
                anyhow::bail!("config constants: cannot evaluate the call `{key}`")
            },
            (Value::Enum { class, name: k }, "name") => {
                let _ = class;
                Ok(Value::Str(k.clone()))
            },
            (Value::Enum { class, name: k }, "toString") => Ok(Value::Str(self.enum_to_string(class, k)?)),
            (Value::Str(s), "toLowerCase") => Ok(Value::Str(s.to_lowercase())),
            (Value::Str(s), "toUpperCase") => Ok(Value::Str(s.to_uppercase())),
            (Value::Str(s), "toString") => Ok(Value::Str(s.clone())),
            (t, m) => anyhow::bail!("config constants: cannot evaluate `{m}` on {t:?}"),
        }
    }

    /// The value of the instance field `field` of enum constant `class.name`.
    fn enum_field(&mut self, class: &str, name: &str, field: &str) -> Res<Value> {
        let c = self.class(class)?;
        let k = c
            .constants
            .iter()
            .find(|k| k.name == name)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("config constants: `{class}` has no constant `{name}`"))?;
        let expr = c
            .ctor_assign
            .get(field)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("config constants: `{class}`'s constructor sets no `{field}`"))?;
        if k.args.len() != c.ctor_params.len() {
            anyhow::bail!(
                "config constants: `{class}.{name}` has {} arguments for {} parameters",
                k.args.len(),
                c.ctor_params.len()
            );
        }
        let mut locals = BTreeMap::new();
        for (p, a) in c.ctor_params.iter().zip(&k.args) {
            let v = self.eval_src(class, a)?;
            locals.insert(p.clone(), v);
        }
        let ast = parse(&expr)?;
        self.eval(class, &ast, &locals)
    }

    /// Java's `toString()` of enum constant `class.name`.
    pub fn enum_to_string(&mut self, class: &str, name: &str) -> Res<String> {
        let c = self.class(class)?;
        let Some(body) = c.to_string.clone() else {
            return Ok(name.to_string());
        };
        let ast = parse(&body)?;
        let v = self.eval_enum_expr(class, name, &ast)?;
        match v {
            Value::Str(s) => Ok(s),
            v => anyhow::bail!("config constants: `{class}.toString()` gave {v:?}"),
        }
    }

    /// An expression inside an enum instance method: `super.toString()` and
    /// `name()` are the constant's name, a bare or `this.` field its value.
    fn eval_enum_expr(&mut self, class: &str, name: &str, ast: &Ast) -> Res<Value> {
        match ast {
            Ast::Call(t, m, args)
                if args.is_empty() && matches!(&**t, Ast::Ident(s) if s == "super") && m == "toString" =>
            {
                Ok(Value::Str(name.to_string()))
            },
            Ast::Call(t, m, args) => {
                let target = self.eval_enum_expr(class, name, t)?;
                let mut vals = Vec::new();
                for a in args {
                    vals.push(self.eval(class, a, &BTreeMap::new())?);
                }
                self.call(target, m, vals)
            },
            Ast::Ident(f) | Ast::Field(_, f) if self.class(class)?.ctor_assign.contains_key(f) => {
                self.enum_field(class, name, f)
            },
            Ast::Ident(s) if s == "this" => Ok(Value::Enum { class: class.to_string(), name: name.to_string() }),
            other => self.eval(class, other, &BTreeMap::new()),
        }
    }
}

// ---------------------------------------------------------------------------
// Class parsing
// ---------------------------------------------------------------------------

/// Skip the string or char literal opening at `i`; the index after it.
fn skip_lit(b: &[u8], mut i: usize) -> usize {
    let q = b[i];
    i += 1;
    while i < b.len() && b[i] != q {
        if b[i] == b'\\' {
            i += 1;
        }
        i += 1;
    }
    i + 1
}

/// The index of the bracket closing the one at `open`.
fn matching(b: &[u8], open: usize) -> Res<usize> {
    let (o, c) = (
        b[open],
        match b[open] {
            b'(' => b')',
            b'{' => b'}',
            b'[' => b']',
            _ => anyhow::bail!("not a bracket"),
        },
    );
    let mut depth = 0;
    let mut i = open;
    while i < b.len() {
        match b[i] {
            b'"' | b'\'' => {
                i = skip_lit(b, i);
                continue;
            },
            x if x == o => depth += 1,
            x if x == c => {
                depth -= 1;
                if depth == 0 {
                    return Ok(i);
                }
            },
            _ => {},
        }
        i += 1;
    }
    anyhow::bail!("config constants: unbalanced `{}`", o as char)
}

/// Split `s` at top-level occurrences of `sep` (`<...>` counted as brackets).
fn split_top(s: &str, sep: u8) -> Vec<String> {
    split_at(s, sep, true)
}

/// Split `s` at top-level occurrences of `sep`; `generics` counts `<...>`.
fn split_at(s: &str, sep: u8, generics: bool) -> Vec<String> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'"' | b'\'' => {
                i = skip_lit(b, i);
                continue;
            },
            b'(' | b'{' | b'[' => depth += 1,
            b')' | b'}' | b']' => depth -= 1,
            b'<' if generics => depth += 1,
            b'>' if generics => depth -= 1,
            x if x == sep && depth == 0 => {
                out.push(s[start..i].trim().to_string());
                start = i + 1;
            },
            _ => {},
        }
        i += 1;
    }
    let last = s[start..].trim();
    if !last.is_empty() {
        out.push(last.to_string());
    }
    out
}

/// Drop leading annotations (`@Deprecated`, `@SuppressWarnings("x")`).
fn strip_annotations(mut s: &str) -> &str {
    loop {
        s = s.trim_start();
        let Some(rest) = s.strip_prefix('@') else { return s };
        let n = rest
            .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '.'))
            .unwrap_or(rest.len());
        let mut after = &rest[n..];
        if after.trim_start().starts_with('(') {
            let t = after.trim_start();
            let off = after.len() - t.len();
            if let Ok(close) = matching(after.as_bytes(), off) {
                after = &after[close + 1..];
            }
        }
        s = after;
    }
}

/// Parse the type `simple` declared in `text` (its first declaration).
fn parse_type(text: &str, simple: &str, fqn: &str) -> Res<JClass> {
    let b = text.as_bytes();
    for kw in ["class ", "enum ", "interface "] {
        let pat = format!("{kw}{simple}");
        let mut from = 0;
        while let Some(at) = text[from..].find(&pat) {
            let at = from + at;
            let end = at + pat.len();
            let boundary_before = at == 0 || !(b[at - 1].is_ascii_alphanumeric() || b[at - 1] == b'_');
            let boundary_after = b.get(end).is_none_or(|c| !(c.is_ascii_alphanumeric() || *c == b'_'));
            if boundary_before && boundary_after {
                let open = text[end..]
                    .find('{')
                    .map(|o| end + o)
                    .ok_or_else(|| anyhow::anyhow!("no body"))?;
                let header = &text[end..open];
                let parent = header
                    .split_once(" extends ")
                    .and_then(|(_, r)| r.split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '.')).next())
                    .map(str::to_string);
                let close = matching(b, open)?;
                let mut c = parse_body(&text[open + 1..close], simple, fqn, kw == "enum ")?;
                c.parent = parent;
                return Ok(c);
            }
            from = end;
        }
    }
    anyhow::bail!("config constants: no type `{simple}` in its source")
}

fn parse_body(body: &str, simple: &str, fqn: &str, is_enum: bool) -> Res<JClass> {
    let mut c = JClass { fqn: fqn.to_string(), is_enum, ..JClass::default() };
    let b = body.as_bytes();
    let mut i = 0;
    if is_enum {
        // The constants run to the first top-level `;` (or the end).
        let mut j = 0;
        let mut end = body.len();
        while j < b.len() {
            match b[j] {
                b'"' | b'\'' => {
                    j = skip_lit(b, j);
                    continue;
                },
                b'(' | b'{' => {
                    j = matching(b, j)? + 1;
                    continue;
                },
                b';' => {
                    end = j;
                    break;
                },
                _ => {},
            }
            j += 1;
        }
        for item in split_at(&body[..end], b',', false) {
            let item = strip_annotations(&item).trim();
            if item.is_empty() {
                continue;
            }
            let n = item.find(|ch: char| !(ch.is_alphanumeric() || ch == '_')).unwrap_or(item.len());
            let name = item[..n].to_string();
            let rest = item[n..].trim_start();
            let args = if rest.starts_with('(') {
                let close = matching(rest.as_bytes(), 0)?;
                split_at(&rest[1..close], b',', false)
            } else {
                Vec::new()
            };
            c.constants.push(EnumConst { name, args });
        }
        i = (end + 1).min(body.len());
    }
    // Members: statements ending in `;` or blocks `{...}` at the top level.
    let mut start = i;
    while i < b.len() {
        match b[i] {
            b'"' | b'\'' => {
                i = skip_lit(b, i);
                continue;
            },
            b'(' => {
                i = matching(b, i)? + 1;
                continue;
            },
            b';' => {
                member(&body[start..i], None, simple, fqn, &mut c)?;
                i += 1;
                start = i;
                continue;
            },
            b'{' => {
                let close = matching(b, i)?;
                let head = &body[start..i];
                // `= {...}` array initializers and `= new X() {...}` stay in the statement.
                if head.contains('=') && !head.trim_end().ends_with(')') || head.trim_end().ends_with('=') {
                    i = close + 1;
                    continue;
                }
                member(head, Some(&body[i + 1..close]), simple, fqn, &mut c)?;
                i = close + 1;
                start = i;
                continue;
            },
            _ => {},
        }
        i += 1;
    }
    Ok(c)
}

/// One member: a field statement (`block` none), or a method / constructor /
/// nested type / initializer with its block.
fn member(head: &str, block: Option<&str>, simple: &str, fqn: &str, c: &mut JClass) -> Res<()> {
    let head = strip_annotations(head).trim();
    if head.is_empty() {
        return Ok(());
    }
    let words: Vec<&str> = head.split_whitespace().collect();
    if let Some(block) = block {
        for kw in ["class", "enum", "interface"] {
            if let Some(p) = words.iter().position(|w| *w == kw) {
                let name = words.get(p + 1).copied().unwrap_or_default();
                let name: String = name.chars().take_while(|ch| ch.is_alphanumeric() || *ch == '_').collect();
                let nested_fqn = format!("{fqn}.{name}");
                let header = head;
                let mut n = parse_body(block, &name, &nested_fqn, kw == "enum")?;
                n.parent = header
                    .split_once(" extends ")
                    .and_then(|(_, r)| r.split(|ch: char| !(ch.is_alphanumeric() || ch == '_' || ch == '.')).next())
                    .map(str::to_string);
                c.nested.insert(name, n);
                return Ok(());
            }
        }
        let Some(paren) = head.find('(') else { return Ok(()) };
        let before = head[..paren].trim();
        let name = before.rsplit(|ch: char| ch.is_whitespace()).next().unwrap_or_default();
        let params_src = &head[paren + 1..head.rfind(')').unwrap_or(head.len())];
        if before.split_whitespace().any(|w| w == "public") {
            let types: Vec<String> = split_top(params_src, b',')
                .iter()
                .filter_map(|p| {
                    let ws: Vec<&str> = p.split_whitespace().filter(|w| *w != "final").collect();
                    (ws.len() >= 2).then(|| ws[..ws.len() - 1].join(" "))
                })
                .collect();
            c.public_methods.push(format!("{name}({})", types.join(", ")));
        }
        if name == simple && c.ctor_params.is_empty() {
            c.ctor_params = split_top(params_src, b',')
                .iter()
                .filter_map(|p| p.split_whitespace().last().map(str::to_string))
                .collect();
            for stmt in split_at(block, b';', false) {
                let stmt = stmt.trim();
                if let Some(rest) = stmt.strip_prefix("this.") {
                    if let Some((f, e)) = rest.split_once('=') {
                        c.ctor_assign.insert(f.trim().to_string(), e.trim().to_string());
                    }
                }
            }
        } else if name == "toString" && params_src.trim().is_empty() {
            let stmt = block.trim();
            if let Some(ret) = stmt.strip_prefix("return ") {
                if let Some(e) = ret.trim().strip_suffix(';') {
                    if !e.contains(';') {
                        c.to_string = Some(e.trim().to_string());
                    }
                }
            }
        }
        return Ok(());
    }
    // A field: `modifiers Type NAME = init` (or several, rejected) or `Type NAME`.
    let (decl, init) = match split_at(head, b'=', false).as_slice() {
        [d] => (d.clone(), None),
        [d, rest @ ..] => (d.clone(), Some(rest.join("="))),
        [] => return Ok(()),
    };
    let toks: Vec<&str> = decl.split_whitespace().collect();
    if toks.len() < 2 || decl.contains('(') {
        return Ok(());
    }
    let name = toks[toks.len() - 1].to_string();
    let mods = &toks[..toks.len() - 1];
    let ty_start = mods
        .iter()
        .position(|w| {
            !matches!(
                *w,
                "public" | "private" | "protected" | "static" | "final" | "volatile" | "transient"
            )
        })
        .unwrap_or(mods.len());
    let has = |m: &str| mods[..ty_start].contains(&m);
    c.fields
        .push(Field { name, init, public: has("public"), static_final: has("static") && has("final") });
    Ok(())
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum Ast {
    Lit(Value),
    Ident(String),
    Field(Box<Ast>, String),
    Call(Box<Ast>, String, Vec<Ast>),
    Neg(Box<Ast>),
    Cast(String, Box<Ast>),
    Bin(char, Box<Ast>, Box<Ast>),
}

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Str(String),
    Int(i64),
    Double(f64),
    Ident(String),
    Punct(char),
}

fn tokenize(s: &str) -> Res<Vec<Tok>> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c.is_ascii_whitespace() {
            i += 1;
        } else if c == b'"' {
            let mut v = String::new();
            i += 1;
            while i < b.len() && b[i] != b'"' {
                if b[i] == b'\\' {
                    i += 1;
                    match b.get(i) {
                        Some(b'n') => v.push('\n'),
                        Some(b't') => v.push('\t'),
                        Some(b'r') => v.push('\r'),
                        Some(b'"') => v.push('"'),
                        Some(b'\'') => v.push('\''),
                        Some(b'\\') => v.push('\\'),
                        other => anyhow::bail!("config constants: escape {other:?} in `{s}`"),
                    }
                } else {
                    v.push(b[i] as char);
                }
                i += 1;
            }
            i += 1;
            out.push(Tok::Str(v));
        } else if c.is_ascii_digit() || (c == b'.' && b.get(i + 1).is_some_and(u8::is_ascii_digit)) {
            let start = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'.' || b[i] == b'_') {
                i += 1;
            }
            let lit = s[start..i].replace('_', "");
            let lower = lit.to_ascii_lowercase();
            if lower.starts_with("0x") {
                let digits = lower.trim_start_matches("0x").trim_end_matches('l');
                out.push(Tok::Int(i64::from_str_radix(digits, 16)?));
            } else if lower.contains('.') || lower.ends_with('f') || lower.ends_with('d') || lower.contains('e') {
                out.push(Tok::Double(lower.trim_end_matches(['f', 'd']).parse()?));
            } else {
                out.push(Tok::Int(lower.trim_end_matches('l').parse()?));
            }
        } else if c.is_ascii_alphabetic() || c == b'_' || c == b'$' {
            let start = i;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'_' || b[i] == b'$') {
                i += 1;
            }
            out.push(Tok::Ident(s[start..i].to_string()));
        } else if b"+-*().,".contains(&c) {
            out.push(Tok::Punct(c as char));
            i += 1;
        } else {
            anyhow::bail!("config constants: unsupported character `{}` in `{s}`", c as char);
        }
    }
    Ok(out)
}

struct P {
    toks: Vec<Tok>,
    pos: usize,
    src: String,
}

fn parse(s: &str) -> Res<Ast> {
    let mut p = P { toks: tokenize(s)?, pos: 0, src: s.to_string() };
    let e = p.additive()?;
    if p.pos != p.toks.len() {
        anyhow::bail!("config constants: trailing tokens in `{s}`");
    }
    Ok(e)
}

impl P {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }
    fn punct(&self, c: char) -> bool {
        self.peek() == Some(&Tok::Punct(c))
    }
    fn expect(&mut self, c: char) -> Res<()> {
        if !self.punct(c) {
            anyhow::bail!("config constants: expected `{c}` in `{}`", self.src);
        }
        self.pos += 1;
        Ok(())
    }
    fn additive(&mut self) -> Res<Ast> {
        let mut left = self.multiplicative()?;
        while self.punct('+') || self.punct('-') {
            let Some(Tok::Punct(op)) = self.peek().cloned() else {
                unreachable!()
            };
            self.pos += 1;
            let right = self.multiplicative()?;
            left = Ast::Bin(op, Box::new(left), Box::new(right));
        }
        Ok(left)
    }
    fn multiplicative(&mut self) -> Res<Ast> {
        let mut left = self.unary()?;
        while self.punct('*') {
            self.pos += 1;
            let right = self.unary()?;
            left = Ast::Bin('*', Box::new(left), Box::new(right));
        }
        Ok(left)
    }
    fn unary(&mut self) -> Res<Ast> {
        if self.punct('-') {
            self.pos += 1;
            return Ok(Ast::Neg(Box::new(self.unary()?)));
        }
        // A cast to a primitive type: `(short) 60`.
        if self.punct('(') {
            if let (Some(Tok::Ident(t)), Some(Tok::Punct(')'))) =
                (self.toks.get(self.pos + 1), self.toks.get(self.pos + 2))
            {
                if matches!(t.as_str(), "short" | "byte" | "int" | "long" | "double" | "float") {
                    let t = t.clone();
                    self.pos += 3;
                    return Ok(Ast::Cast(t, Box::new(self.unary()?)));
                }
            }
        }
        self.postfix()
    }
    fn postfix(&mut self) -> Res<Ast> {
        let mut e = self.primary()?;
        while self.punct('.') {
            self.pos += 1;
            let Some(Tok::Ident(n)) = self.peek().cloned() else {
                anyhow::bail!("config constants: expected a name after `.` in `{}`", self.src)
            };
            self.pos += 1;
            if self.punct('(') {
                let args = self.args()?;
                e = Ast::Call(Box::new(e), n, args);
            } else {
                e = Ast::Field(Box::new(e), n);
            }
        }
        Ok(e)
    }
    fn args(&mut self) -> Res<Vec<Ast>> {
        self.expect('(')?;
        let mut out = Vec::new();
        if self.punct(')') {
            self.pos += 1;
            return Ok(out);
        }
        loop {
            out.push(self.additive()?);
            if self.punct(',') {
                self.pos += 1;
                continue;
            }
            self.expect(')')?;
            return Ok(out);
        }
    }
    fn primary(&mut self) -> Res<Ast> {
        match self.peek().cloned() {
            Some(Tok::Str(s)) => {
                self.pos += 1;
                Ok(Ast::Lit(Value::Str(s)))
            },
            Some(Tok::Int(n)) => {
                self.pos += 1;
                Ok(Ast::Lit(Value::Int(n)))
            },
            Some(Tok::Double(d)) => {
                self.pos += 1;
                Ok(Ast::Lit(Value::Double(d)))
            },
            Some(Tok::Punct('(')) => {
                self.pos += 1;
                let e = self.additive()?;
                self.expect(')')?;
                Ok(e)
            },
            Some(Tok::Ident(id)) => {
                self.pos += 1;
                Ok(match id.as_str() {
                    "null" => Ast::Lit(Value::Null),
                    "true" => Ast::Lit(Value::Bool(true)),
                    "false" => Ast::Lit(Value::Bool(false)),
                    _ if self.punct('(') => {
                        // An unqualified call (`name()` inside an enum).
                        let args = self.args()?;
                        Ast::Call(Box::new(Ast::Ident("this".into())), id, args)
                    },
                    _ => Ast::Ident(id),
                })
            },
            other => anyhow::bail!("config constants: unexpected {other:?} in `{}`", self.src),
        }
    }
}

// ---------------------------------------------------------------------------
// The generated Python classes
// ---------------------------------------------------------------------------

/// How a config class is translated (CLAUDE.md, Python Binding Conventions,
/// Scope).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// Constants only; Java's constructors parse a configuration through the
    /// `ConfigDef` machinery, which is not translated, so constructing raises.
    ConstantsOnly,
    /// Constants only; Java's implicit no-argument constructor is kept.
    Constructible,
}

/// One generated class.
struct Spec {
    java: &'static str,
    /// Its file under `python/`.
    file: &'static str,
    kind: Kind,
    /// Java superclasses whose constants are inherited but not translated as
    /// classes: their constants are added to this class.
    flatten: &'static [&'static str],
}

const ABSTRACT_CONFIG: &str = "org.apache.kafka.common.config.AbstractConfig";

const SPECS: &[Spec] = &[
    Spec {
        java: "org.apache.kafka.clients.producer.ProducerConfig",
        file: "python/confluent_kafka/producer/producer_config.py",
        kind: Kind::ConstantsOnly,
        flatten: &[ABSTRACT_CONFIG],
    },
    Spec {
        java: "org.apache.kafka.clients.consumer.ConsumerConfig",
        file: "python/confluent_kafka/consumer/consumer_config.py",
        kind: Kind::ConstantsOnly,
        flatten: &[ABSTRACT_CONFIG],
    },
    Spec {
        java: "org.apache.kafka.clients.admin.AdminClientConfig",
        file: "python/confluent_kafka/admin/admin_client_config.py",
        kind: Kind::ConstantsOnly,
        flatten: &[ABSTRACT_CONFIG],
    },
    Spec {
        java: "org.apache.kafka.common.config.TopicConfig",
        file: "python/confluent_kafka/common/config/topic_config.py",
        kind: Kind::Constructible,
        flatten: &[],
    },
    Spec {
        java: "org.apache.kafka.common.config.SaslConfigs",
        file: "python/confluent_kafka/common/config/sasl_configs.py",
        kind: Kind::Constructible,
        flatten: &[],
    },
    Spec {
        java: "org.apache.kafka.common.config.SslConfigs",
        file: "python/confluent_kafka/common/config/ssl_configs.py",
        kind: Kind::Constructible,
        flatten: &[],
    },
    Spec {
        java: "org.apache.kafka.common.config.SecurityConfig",
        file: "python/confluent_kafka/common/config/security_config.py",
        kind: Kind::Constructible,
        flatten: &[],
    },
    Spec {
        java: "org.apache.kafka.common.config.LogLevelConfig",
        file: "python/confluent_kafka/common/config/log_level_config.py",
        kind: Kind::Constructible,
        flatten: &[],
    },
];

/// The checked-in list of the Java API deprecated in `DEPRECATION_REFS`.
const DEPRECATED_LIST: &str = "design/current/java-deprecated.txt";

const LICENSE: &str = "# Copyright 2026 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the \"License\");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an \"AS IS\" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
";

/// A Python string literal.
pub fn py_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// A constant's Python value and its stub type.
fn py_value(v: &Value) -> Res<(String, String)> {
    Ok(match v {
        Value::Str(s) => (py_str(s), "str".into()),
        Value::Int(n) => (n.to_string(), "int".into()),
        Value::Double(d) => {
            let s = format!("{d:?}");
            (s, "float".into())
        },
        Value::Bool(b) => ((if *b { "True" } else { "False" }).into(), "bool".into()),
        Value::Null => ("None".into(), "None".into()),
        // A constant `Set` is a frozenset, a constant `List` a tuple in Java's order
        // (Class family).
        Value::List(items) | Value::Set(items) => {
            let mut parts = Vec::new();
            let mut ty = None;
            for i in items {
                let (p, t) = py_value(i)?;
                if ty.as_ref().is_some_and(|x| *x != t) {
                    anyhow::bail!("config constants: a collection of mixed types {items:?}");
                }
                ty = Some(t);
                parts.push(p);
            }
            let ty = ty.unwrap_or_else(|| "str".into());
            if matches!(v, Value::List(_)) {
                let comma = if parts.len() == 1 { "," } else { "" };
                (format!("({}{comma})", parts.join(", ")), format!("tuple[{ty}, ...]"))
            } else if parts.is_empty() {
                ("frozenset()".into(), format!("frozenset[{ty}]"))
            } else {
                (format!("frozenset({{{}}})", parts.join(", ")), format!("frozenset[{ty}]"))
            }
        },
        other => anyhow::bail!("config constants: no Python form for {other:?}"),
    })
}

/// One class's constants, in declaration order: `(name, value)`; the
/// deprecated ones left out.
fn constants_of(reader: &mut Reader, fqn: &str, deprecated: &[String]) -> Res<Vec<(String, Value)>> {
    let class = reader.class(fqn)?;
    let mut out = Vec::new();
    for f in class.fields.iter().filter(|f| f.public && f.static_final) {
        if deprecated.iter().any(|d| *d == format!("{fqn}#{}", f.name)) {
            continue;
        }
        let init = f
            .init
            .clone()
            .ok_or_else(|| anyhow::anyhow!("config constants: `{fqn}.{}` has no initializer", f.name))?;
        let v = reader
            .eval_src(fqn, &init)
            .map_err(|e| anyhow::anyhow!("`{fqn}.{}`: {e}", f.name))?;
        out.push((f.name.clone(), v));
    }
    Ok(out)
}

/// The simple name of `fqn`.
fn simple(fqn: &str) -> &str {
    fqn.rsplit('.').next().unwrap_or(fqn)
}

/// Wrap `text` to lines of at most `width`, each prefixed by `indent`.
fn wrap(text: &str, width: usize, indent: &str) -> String {
    let mut lines = Vec::new();
    let mut line = String::new();
    for w in text.split_whitespace() {
        if !line.is_empty() && indent.len() + line.len() + 1 + w.len() > width {
            lines.push(format!("{indent}{line}"));
            line.clear();
        }
        if !line.is_empty() {
            line.push(' ');
        }
        line.push_str(w);
    }
    if !line.is_empty() {
        lines.push(format!("{indent}{line}"));
    }
    lines.join("\n")
}

/// The generated config-class modules: `(path, content)`, `.py` and `.pyi` each.
pub fn outputs(repo_root: &Path) -> Res<Vec<(PathBuf, String)>> {
    let deprecated: Vec<String> = fs::read_to_string(repo_root.join(DEPRECATED_LIST))
        .map_err(|e| anyhow::anyhow!("{DEPRECATED_LIST}: {e}"))?
        .lines()
        .filter(|l| !l.starts_with('#') && !l.trim().is_empty())
        .map(|l| l.trim().to_string())
        .collect();
    let mut reader = Reader::new(repo_root);
    let mut out = Vec::new();
    for spec in SPECS {
        let name = simple(spec.java);
        let class = reader.class(spec.java)?;
        let mut consts = constants_of(&mut reader, spec.java, &deprecated)?;
        for parent in spec.flatten {
            for (n, v) in constants_of(&mut reader, parent, &deprecated)? {
                if !consts.iter().any(|(m, _)| *m == n) {
                    consts.push((n, v));
                }
            }
        }
        let mut skipped: Vec<String> = class
            .public_methods
            .iter()
            .filter(|m| !m.starts_with("toString("))
            .map(|m| format!("``{m}``"))
            .collect();
        skipped.dedup();
        let mut about = String::from(
            "Every ``public static final`` field of Java's class, evaluated from the Java source, is a \
             class attribute of the same name; a deprecated one is left out (CLAUDE.md §3).",
        );
        if !spec.flatten.is_empty() {
            about.push_str(
                " The constants Java's class inherits from ``AbstractConfig`` \
                 (``CONFIG_PROVIDERS_CONFIG``, ...) are attributes of the class too.",
            );
        }
        match spec.kind {
            Kind::ConstantsOnly => about.push_str(&format!(
                " The ``ConfigDef`` machinery is not translated *(deviation: a configuration is a ``dict`` \
                 of these keys, and the ``ConfigDef`` types are the types Configuration checks)*, so \
                 ``{name}`` is not instantiable: constructing it raises ``TypeError``."
            )),
            Kind::Constructible => {},
        }
        if !skipped.is_empty() {
            about.push_str(&format!(
                " Not generated: {}, the ``ConfigDef`` machinery or methods that take a type the \
                 binding does not translate (``ConfigDef``, ``AbstractConfig``).",
                skipped.join(", ")
            ));
        }
        let header = format!(
            "{LICENSE}\n# GENERATED by `cargo xtask generate-error-codes` from Java's {} -- DO NOT EDIT.\n",
            spec.java
        );
        let doc = format!(
            "\"\"\"``{name}``: the constants of Java's ``{}``.\n\n{}\n\"\"\"\n",
            spec.java,
            wrap(&about, 88, "")
        );
        let mut py = format!("{header}\n{doc}\nfrom __future__ import annotations\n\n");
        let mut pyi = format!("{header}\n");
        let needs_any = spec.kind == Kind::ConstantsOnly;
        let typing = if needs_any {
            "from typing import Any, Final\n"
        } else {
            "from typing import Final\n"
        };
        py.push_str(typing);
        pyi.push_str(typing);
        py.push_str(&format!("\n__all__ = [\"{name}\"]\n\n\nclass {name}:\n"));
        pyi.push_str(&format!("\n__all__ = [\"{name}\"]\n\nclass {name}:\n"));
        py.push_str(&format!("    \"\"\"Java: ``{}``.\"\"\"\n\n    __slots__ = ()\n", spec.java));
        if !consts.is_empty() {
            py.push('\n');
        }
        for (n, v) in &consts {
            let (val, ty) = py_value(v)?;
            py.push_str(&format!("    {n}: Final = {val}\n"));
            pyi.push_str(&format!("    {n}: Final[{ty}]\n"));
        }
        if spec.kind == Kind::ConstantsOnly {
            py.push_str(&format!(
                "\n    def __init__(self, *args: Any, **kwargs: Any) -> None:\n        \
                 raise TypeError(\"{name} is not instantiable: its ConfigDef machinery is not \"\n                        \
                 \"translated; a configuration is a dict of its keys\")\n"
            ));
            pyi.push_str("    def __init__(self, *args: Any, **kwargs: Any) -> None: ...\n");
        } else if consts.is_empty() {
            pyi.push_str("    ...\n");
        }
        let path = repo_root.join(spec.file);
        out.push((path.with_extension("pyi"), pyi));
        out.push((path, py));
    }
    Ok(out)
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
    fn test_evaluates_java_constants() {
        let mut r = Reader::new(&repo_root());
        let cases: &[(&str, &str, Value)] = &[
            (
                "org.apache.kafka.clients.producer.ProducerConfig",
                "BOOTSTRAP_SERVERS_CONFIG",
                Value::Str("bootstrap.servers".into()),
            ),
            (
                "org.apache.kafka.clients.consumer.ConsumerConfig",
                "DEFAULT_FETCH_MAX_BYTES",
                Value::Int(50 * 1024 * 1024),
            ),
            (
                "org.apache.kafka.clients.consumer.ConsumerConfig",
                "DEFAULT_GROUP_PROTOCOL",
                Value::Str("classic".into()),
            ),
            (
                "org.apache.kafka.clients.consumer.ConsumerConfig",
                "DEFAULT_ISOLATION_LEVEL",
                Value::Str("read_uncommitted".into()),
            ),
            (
                "org.apache.kafka.clients.consumer.ConsumerConfig",
                "DEFAULT_GROUP_REMOTE_ASSIGNOR",
                Value::Null,
            ),
            (
                "org.apache.kafka.clients.CommonClientConfigs",
                "DEFAULT_METADATA_RECOVERY_STRATEGY",
                Value::Str("rebootstrap".into()),
            ),
            (
                "org.apache.kafka.clients.CommonClientConfigs",
                "RETRY_BACKOFF_JITTER",
                Value::Double(0.2),
            ),
            (
                "org.apache.kafka.clients.CommonClientConfigs",
                "SEND_BUFFER_LOWER_BOUND",
                Value::Int(-1),
            ),
            (
                "org.apache.kafka.common.config.SslConfigs",
                "DEFAULT_SSL_TRUSTMANAGER_ALGORITHM",
                Value::Str("PKIX".into()),
            ),
            (
                "org.apache.kafka.common.config.SaslConfigs",
                "DEFAULT_LOGIN_REFRESH_MIN_PERIOD_SECONDS",
                Value::Int(60),
            ),
        ];
        for (class, name, want) in cases {
            let got = r.constant(class, name).unwrap().unwrap();
            assert_eq!(&got, want, "{class}.{name}");
        }
        assert_eq!(
            r.enum_to_string("org.apache.kafka.clients.ClientDnsLookup", "USE_ALL_DNS_IPS")
                .unwrap(),
            "use_all_dns_ips"
        );
    }

    #[test]
    fn test_deprecated_constants_left_out() {
        let outputs = outputs(&repo_root()).unwrap();
        let producer = outputs.iter().find(|(p, _)| p.ends_with("producer_config.py")).unwrap();
        assert!(producer
            .1
            .contains("    PARTITIONER_ADAPTIVE_PARTITIONING_ENABLE_CONFIG: Final ="));
        assert!(!producer.1.contains("PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG"));
        assert!(producer.1.contains("    CONFIG_PROVIDERS_CONFIG: Final = \"config.providers\""));
        let topic = outputs.iter().find(|(p, _)| p.ends_with("topic_config.py")).unwrap();
        assert!(!topic.1.contains("MESSAGE_DOWNCONVERSION_ENABLE_CONFIG"));
    }
}
