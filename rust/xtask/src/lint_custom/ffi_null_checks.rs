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

//! `check-ffi-null-checks`: no C argument reaches Rust unchecked for NULL
//! (CLAUDE.md §4: "Never cause a segmentation fault from the C call").
//!
//! A mandatory pointer goes through the `ffi::precondition` helpers
//! (`require_ref`, `require_mut`, `require_str`, `require_list`, `require_map`,
//! `require_fn`), which panic with Java's error on NULL. The rule reports, per
//! function under `ffi` (outside `ffi::precondition` and `#[cfg(test)]` code),
//! every place that reads through a raw pointer without such a check:
//!
//! - `deref`: a unary `*` on a raw-pointer parameter or `let`, on a cast to a
//!   raw-pointer type, or on pointer arithmetic over one (`p.add(i)`);
//! - `from-ptr`: `CStr::from_ptr(p)`;
//! - `from-raw-parts`: `slice::from_raw_parts(_mut)(p, ..)`;
//! - `read`: `p.read()` or `ptr::read(p)`;
//! - `unwrap`: `p.as_ref()` / `p.as_mut()` followed by `.unwrap()` or
//!   `.expect(..)`, a NULL check panicking without Java's error;
//! - `fn-call`: a call through a C export's parameter whose type is a bare `fn`.
//!
//! A pointer the function tests with `p.is_null()` counts as checked, since the
//! test is how a nullable parameter is read. Outside a C export, a pointer to a
//! Rust type (`Self`, a `dyn` trait, or a type whose name does not end in `_t`)
//! is the crate's own, not a C argument, so it is not reported: the export
//! that turned the C argument into it is where the check belongs.
//! `Box::from_raw` / `Arc::from_raw` are not reported: they take back what
//! Rust handed out, as `_destroy` does.
//!
//! Second, every fn-pointer typedef must be nullable: `bare-typedef` for a
//! `pub type X = unsafe extern "C" fn(..)` under `ffi`, `bare-fn-param` for an
//! export parameter typed as a bare `fn`, `bare-fn-field` for a public field of
//! one. C can pass `NULL` for any of them, which a bare `fn` cannot hold.

use std::collections::{BTreeMap, BTreeSet};

use syn::visit::{self, Visit};

use super::{is_cfg_test, Crate, Rule};

/// One unchecked read, or one non-nullable fn-pointer type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Finding {
    kind: &'static str,
    /// The crate path of the function or typedef, e.g. `ffi::common::kafka_x`.
    subject: String,
    file: String,
    detail: String,
}

impl Finding {
    #[cfg(test)]
    pub(super) fn key(&self) -> String {
        format!("{} {}", self.kind, self.subject)
    }

    fn message(&self) -> String {
        format!("{}: {} `{}`: {}", self.file, self.kind, self.subject, self.detail)
    }
}

/// The rule.
pub(super) struct FfiNullChecks;

impl FfiNullChecks {
    /// Every finding, and how many functions were inspected.
    pub(super) fn findings(&self, krate: &Crate) -> (Vec<Finding>, usize) {
        let mut out = Vec::new();
        let mut checked = 0;
        for (path, module) in &krate.modules {
            if path.first().map(String::as_str) != Some("ffi")
                || path.get(1).map(String::as_str) == Some("precondition")
            {
                continue;
            }
            let prefix = path.join("::");
            let file = module.file.display().to_string();
            for item in &module.items {
                checked += check_item(item, &prefix, &file, &mut out);
            }
        }
        out.sort();
        out.dedup();
        (out, checked)
    }
}

impl Rule for FfiNullChecks {
    fn name(&self) -> &'static str {
        "check-ffi-null-checks"
    }

    fn check(&self, krate: &Crate, findings: &mut Vec<String>) -> usize {
        let (raw, checked) = self.findings(krate);
        findings.extend(raw.iter().map(Finding::message));
        checked
    }

    fn hint(&self) -> &'static str {
        "   A C argument is checked for NULL before Rust reads it (CLAUDE.md §4):
   use `ffi::precondition::require_ref` / `require_mut` / `require_str` /
   `require_list` / `require_map` / `require_fn`, which panic with Java's error,
   or test `p.is_null()` where Java gives null a meaning. A fn-pointer typedef
   is `Option<unsafe extern \"C\" fn(..)>`, unwrapped with `require_fn`."
    }
}

/// Checks one module item, returning how many functions it inspected.
fn check_item(item: &syn::Item, prefix: &str, file: &str, out: &mut Vec<Finding>) -> usize {
    match item {
        syn::Item::Fn(f) if !is_cfg_test(&f.attrs) => {
            let subject = format!("{prefix}::{}", f.sig.ident);
            if is_export(&f.attrs) {
                for (name, ty) in params(&f.sig) {
                    if matches!(ty, syn::Type::BareFn(_)) {
                        push(
                            out,
                            "bare-fn-param",
                            &subject,
                            file,
                            format!("parameter `{name}` is a bare `fn`; make its typedef `Option<..>`"),
                        );
                    }
                }
            }
            check_fn(&f.sig, &f.block, is_export(&f.attrs), &subject, file, out);
            1
        },
        syn::Item::Impl(imp) if !is_cfg_test(&imp.attrs) => {
            let self_ty = type_name(&imp.self_ty);
            let mut n = 0;
            for it in &imp.items {
                if let syn::ImplItem::Fn(f) = it {
                    if is_cfg_test(&f.attrs) {
                        continue;
                    }
                    let subject = format!("{prefix}::{self_ty}::{}", f.sig.ident);
                    check_fn(&f.sig, &f.block, is_export(&f.attrs), &subject, file, out);
                    n += 1;
                }
            }
            n
        },
        syn::Item::Type(t) if matches!(t.vis, syn::Visibility::Public(_)) && !is_cfg_test(&t.attrs) => {
            if matches!(&*t.ty, syn::Type::BareFn(_)) {
                push(
                    out,
                    "bare-typedef",
                    &format!("{prefix}::{}", t.ident),
                    file,
                    "a fn-pointer typedef C can pass NULL for is `Option<unsafe extern \"C\" fn(..)>`".to_string(),
                );
            }
            0
        },
        syn::Item::Struct(s) if matches!(s.vis, syn::Visibility::Public(_)) && !is_cfg_test(&s.attrs) => {
            for field in &s.fields {
                if matches!(field.vis, syn::Visibility::Public(_)) && matches!(field.ty, syn::Type::BareFn(_)) {
                    let name = field.ident.as_ref().map_or_else(|| "_".to_string(), ToString::to_string);
                    push(
                        out,
                        "bare-fn-field",
                        &format!("{prefix}::{}", s.ident),
                        file,
                        format!("field `{name}` is a bare `fn`; make it `Option<..>`"),
                    );
                }
            }
            0
        },
        _ => 0,
    }
}

fn push(out: &mut Vec<Finding>, kind: &'static str, subject: &str, file: &str, detail: String) {
    out.push(Finding { kind, subject: subject.to_string(), file: file.to_string(), detail });
}

/// Whether `attrs` mark a C export (`#[unsafe(no_mangle)]` or `#[no_mangle]`).
fn is_export(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|a| {
        a.path().is_ident("no_mangle")
            || (a.path().is_ident("unsafe") && a.parse_args::<syn::Path>().is_ok_and(|p| p.is_ident("no_mangle")))
    })
}

/// The named parameters of `sig` with their types.
fn params(sig: &syn::Signature) -> impl Iterator<Item = (String, &syn::Type)> {
    sig.inputs.iter().filter_map(|arg| match arg {
        syn::FnArg::Typed(pt) => match &*pt.pat {
            syn::Pat::Ident(id) => Some((id.ident.to_string(), &*pt.ty)),
            _ => None,
        },
        syn::FnArg::Receiver(_) => None,
    })
}

fn type_name(ty: &syn::Type) -> String {
    match ty {
        syn::Type::Path(p) => p.path.segments.last().map_or_else(String::new, |s| s.ident.to_string()),
        _ => "_".to_string(),
    }
}

/// Whether `ty` is a raw pointer to a Rust type: `Self`, a `dyn` trait, or a
/// named type whose name does not end in `_t` and is not a primitive or a
/// `std::ffi` C type, as a C argument's pointee always is.
fn is_rust_pointer(ty: &syn::Type) -> bool {
    let syn::Type::Ptr(p) = ty else { return false };
    match &*p.elem {
        syn::Type::TraitObject(_) => true,
        syn::Type::Paren(inner) => matches!(&*inner.elem, syn::Type::TraitObject(_)),
        syn::Type::Path(path) => path.path.segments.last().is_some_and(|seg| {
            let name = seg.ident.to_string();
            name == "Self" || (name.starts_with(|c: char| c.is_ascii_uppercase()) && !name.ends_with("_t"))
        }),
        _ => false,
    }
}

/// Collects the kinds of unchecked reads in one function body; `export` says
/// whether every pointer parameter is a C argument (see [`is_rust_pointer`]).
fn check_fn(sig: &syn::Signature, block: &syn::Block, export: bool, subject: &str, file: &str, out: &mut Vec<Finding>) {
    let mut v = BodyVisitor { export, ..BodyVisitor::default() };
    for (name, ty) in params(sig) {
        match ty {
            syn::Type::Ptr(_) if !export && is_rust_pointer(ty) => {},
            syn::Type::Ptr(_) => {
                v.raw.insert(name);
            },
            // A Rust `fn` value is never NULL: outside an export it is a fn item or
            // what `require_fn` returned, and an export's own is `bare-fn-param`.
            syn::Type::BareFn(_) if export => {
                v.bare_fns.insert(name);
            },
            _ => {},
        }
    }
    // Two passes: the first learns the raw `let`s and the `is_null()` tests,
    // wherever they sit in the body, the second reports.
    v.learning = true;
    v.visit_block(block);
    v.learning = false;
    v.visit_block(block);
    for (kind, count) in v.found {
        let detail = match kind {
            "deref" => "dereferences a raw pointer not checked for NULL",
            "from-ptr" => "`CStr::from_ptr` on a pointer not checked for NULL",
            "from-raw-parts" => "`from_raw_parts` on a pointer not checked for NULL",
            "read" => "reads through a raw pointer not checked for NULL",
            "unwrap" => "`as_ref()`/`as_mut()` unwrapped: a NULL panics without Java's error",
            _ => "calls a bare `fn` parameter C may pass as NULL",
        };
        push(
            out,
            kind,
            subject,
            file,
            format!("{detail} ({count} site(s)); use the `ffi::precondition` helpers"),
        );
    }
}

#[derive(Default)]
struct BodyVisitor {
    learning: bool,
    /// Whether the function is a C export, where every pointer is a C argument.
    export: bool,
    /// Names bound to raw pointers: parameters and `let`s.
    raw: BTreeSet<String>,
    /// Names tested with `.is_null()`.
    null_checked: BTreeSet<String>,
    /// Parameters of a bare `fn` type.
    bare_fns: BTreeSet<String>,
    /// Reported kinds and how many sites each.
    found: BTreeMap<&'static str, usize>,
}

impl BodyVisitor {
    fn report(&mut self, kind: &'static str) {
        if !self.learning {
            *self.found.entry(kind).or_default() += 1;
        }
    }

    /// Whether reading through `e` may read NULL: `e` is, or derives by
    /// arithmetic or a cast from, a raw pointer not tested with `is_null()`.
    fn unchecked_raw(&self, e: &syn::Expr) -> bool {
        match e {
            syn::Expr::Paren(p) => self.unchecked_raw(&p.expr),
            syn::Expr::Group(g) => self.unchecked_raw(&g.expr),
            syn::Expr::Cast(c) => {
                if let Some(name) = root_name(&c.expr) {
                    if self.null_checked.contains(&name) {
                        return false;
                    }
                }
                let c_pointer = matches!(&*c.ty, syn::Type::Ptr(_)) && (self.export || !is_rust_pointer(&c.ty));
                c_pointer || self.unchecked_raw(&c.expr)
            },
            syn::Expr::MethodCall(m) if is_pointer_arith(&m.method.to_string()) => self.unchecked_raw(&m.receiver),
            syn::Expr::Path(p) => p
                .path
                .get_ident()
                .map(ToString::to_string)
                .is_some_and(|n| self.raw.contains(&n) && !self.null_checked.contains(&n)),
            _ => false,
        }
    }
}

fn is_pointer_arith(method: &str) -> bool {
    matches!(
        method,
        "add" | "offset" | "sub" | "byte_add" | "cast" | "cast_mut" | "cast_const" | "wrapping_add"
    )
}

/// The variable a pointer expression is computed from, if it is one.
fn root_name(e: &syn::Expr) -> Option<String> {
    match e {
        syn::Expr::Paren(p) => root_name(&p.expr),
        syn::Expr::Group(g) => root_name(&g.expr),
        syn::Expr::Cast(c) => root_name(&c.expr),
        syn::Expr::MethodCall(m) if is_pointer_arith(&m.method.to_string()) => root_name(&m.receiver),
        syn::Expr::Path(p) => p.path.get_ident().map(ToString::to_string),
        _ => None,
    }
}

/// The last two segments of a called path, e.g. `("CStr", "from_ptr")`.
fn call_tail(func: &syn::Expr) -> Option<(String, String)> {
    let syn::Expr::Path(p) = func else { return None };
    let segs: Vec<String> = p.path.segments.iter().map(|s| s.ident.to_string()).collect();
    let last = segs.last()?.clone();
    let prev = if segs.len() > 1 {
        segs[segs.len() - 2].clone()
    } else {
        String::new()
    };
    Some((prev, last))
}

impl<'ast> Visit<'ast> for BodyVisitor {
    fn visit_local(&mut self, local: &'ast syn::Local) {
        if self.learning {
            let (name, ty) = match &local.pat {
                syn::Pat::Type(pt) => match &*pt.pat {
                    syn::Pat::Ident(id) => (Some(id.ident.to_string()), Some(&*pt.ty)),
                    _ => (None, None),
                },
                syn::Pat::Ident(id) => (Some(id.ident.to_string()), None),
                _ => (None, None),
            };
            if let Some(name) = name {
                let c_pointer = |t: &syn::Type| matches!(t, syn::Type::Ptr(_)) && (self.export || !is_rust_pointer(t));
                let typed_raw = ty.is_some_and(c_pointer);
                let cast_raw = local.init.as_ref().is_some_and(|init| match &*init.expr {
                    syn::Expr::Cast(c) => c_pointer(&c.ty),
                    syn::Expr::MethodCall(m) => {
                        is_pointer_arith(&m.method.to_string())
                            && root_name(&m.receiver).is_some_and(|r| self.raw.contains(&r))
                    },
                    _ => false,
                });
                if typed_raw || cast_raw {
                    self.raw.insert(name);
                }
            }
        }
        visit::visit_local(self, local);
    }

    fn visit_expr_unary(&mut self, u: &'ast syn::ExprUnary) {
        if matches!(u.op, syn::UnOp::Deref(_)) && self.unchecked_raw(&u.expr) {
            self.report("deref");
        }
        visit::visit_expr_unary(self, u);
    }

    fn visit_expr_call(&mut self, call: &'ast syn::ExprCall) {
        if let Some((prev, last)) = call_tail(&call.func) {
            let first = call.args.first();
            let unchecked = first.is_some_and(|a| self.unchecked_raw(a));
            match (prev.as_str(), last.as_str()) {
                ("CStr", "from_ptr") if unchecked => self.report("from-ptr"),
                (_, "from_raw_parts" | "from_raw_parts_mut") if unchecked => self.report("from-raw-parts"),
                ("ptr", "read" | "read_unaligned") if unchecked => self.report("read"),
                ("", name) if self.bare_fns.contains(name) => self.report("fn-call"),
                _ => {},
            }
        }
        visit::visit_expr_call(self, call);
    }

    fn visit_expr_method_call(&mut self, m: &'ast syn::ExprMethodCall) {
        let method = m.method.to_string();
        if self.learning && method == "is_null" {
            if let Some(name) = root_name(&m.receiver) {
                self.null_checked.insert(name);
            }
        }
        if matches!(method.as_str(), "read" | "read_unaligned") && self.unchecked_raw(&m.receiver) {
            self.report("read");
        }
        if matches!(method.as_str(), "unwrap" | "expect") {
            if let syn::Expr::MethodCall(inner) = &*m.receiver {
                let im = inner.method.to_string();
                if matches!(im.as_str(), "as_ref" | "as_mut") && self.unchecked_raw(&inner.receiver) {
                    self.report("unwrap");
                }
            }
        }
        visit::visit_expr_method_call(self, m);
    }

    // A nested fn or closure with its own parameters is a different scope;
    // closures share the enclosing names, which is what the body means.
    fn visit_item_fn(&mut self, _: &'ast syn::ItemFn) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn run(name: &str, ffi: &str) -> Vec<String> {
        let dir: PathBuf = std::env::temp_dir().join(format!("xtask-ffi-null-checks-{name}-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let lib = format!(
            "pub mod ffi {{\n use std::ffi::{{c_char, c_void, CStr}};\n{ffi}\n pub mod precondition {{ \
             pub unsafe fn require_ref(p: *const u8) -> &'static u8 {{ &*p }} }}\n}}\n"
        );
        fs::write(dir.join("lib.rs"), lib).unwrap();
        let krate = Crate::load(&dir.join("lib.rs")).unwrap();
        let (findings, _) = FfiNullChecks.findings(&krate);
        fs::remove_dir_all(&dir).unwrap();
        findings.iter().map(Finding::key).collect()
    }

    #[test]
    fn test_unchecked_reads_are_reported_per_kind() {
        let keys = run(
            "kinds",
            r#"
            pub struct X;
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn kafka_a(self_: *const X, s: *const c_char, data: *const u8, len: i32) -> i32 {
                let _x = &*self_;
                let _s = CStr::from_ptr(s);
                let _d = std::slice::from_raw_parts(data, len as usize);
                let _r = data.read();
                0
            }
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn kafka_b(p: *mut c_void) {
                let _x = &*(p as *const X);
                let _y = (p as *const X).as_ref().unwrap();
            }
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn kafka_c(items: *const *mut c_void, i: usize) {
                let _x = *items.add(i);
            }
            "#,
        );
        assert_eq!(
            keys,
            [
                "deref ffi::kafka_a",
                "deref ffi::kafka_b",
                "deref ffi::kafka_c",
                "from-ptr ffi::kafka_a",
                "from-raw-parts ffi::kafka_a",
                "read ffi::kafka_a",
                "unwrap ffi::kafka_b",
            ]
        );
    }

    #[test]
    fn test_null_tested_pointers_and_precondition_module_pass() {
        let keys = run(
            "checked",
            r#"
            pub struct X;
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn kafka_a(p: *const X, q: *mut c_void) -> i32 {
                if p.is_null() { return -1; }
                let _x = &*p;
                let _y = (q as *const X).as_ref();
                let _b = Box::from_raw(q as *mut X);
                0
            }
            "#,
        );
        assert!(keys.is_empty(), "{keys:?}");
    }

    #[test]
    fn test_rust_pointers_outside_exports_pass() {
        let keys = run(
            "internal",
            r#"
            pub struct X;
            pub struct kafka_y_t;
            pub trait T {}
            pub unsafe fn helper(this: *mut X, t: *const dyn T, y: *const kafka_y_t, v: *mut c_void) {
                let _x = &*this;
                let _t = &*t;
                let _z = &*(v as *const X);
                let _y = &*y;
            }
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn kafka_a(self_: *const X, v: *mut c_void) {
                let _x = &*self_;
                let _z = &*(v as *const X);
            }
            pub unsafe fn element(v: usize) -> i32 {
                *(v as *const i32)
            }
            "#,
        );
        assert_eq!(keys, ["deref ffi::element", "deref ffi::helper", "deref ffi::kafka_a"]);
    }

    #[test]
    fn test_bare_fn_types_are_reported() {
        let keys = run(
            "bare",
            r#"
            pub type kafka_a_fn_t = unsafe extern "C" fn(opaque: *mut c_void);
            pub type kafka_b_fn_t = Option<unsafe extern "C" fn(opaque: *mut c_void)>;
            pub struct S { pub f: unsafe extern "C" fn(), pub g: Option<unsafe extern "C" fn()> }
            #[unsafe(no_mangle)]
            pub unsafe extern "C" fn kafka_c(cb: unsafe extern "C" fn(opaque: *mut c_void), opaque: *mut c_void) {
                cb(opaque);
            }
            "#,
        );
        assert_eq!(
            keys,
            [
                "bare-fn-field ffi::S",
                "bare-fn-param ffi::kafka_c",
                "bare-typedef ffi::kafka_a_fn_t",
                "fn-call ffi::kafka_c",
            ]
        );
    }
}
