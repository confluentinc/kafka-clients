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

//! A burn-down baseline: a file listing the finding keys a rule still waives,
//! shared by `check-ffi-translation` (`ffi-baseline.txt`) and
//! `check-api-docs` (`doc-baseline.txt`).
//!
//! Each line is a finding key (`<kind> <subject>`), or `prefix:<p>` waiving
//! every finding whose subject starts with `p`; `#` starts a comment. A line
//! that waives nothing is itself a finding, so the file only shrinks.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

/// A finding a [`Baseline`] can waive.
pub(super) trait Waivable: Sized {
    /// The baseline line that waives it.
    fn key(&self) -> String;
    /// What `prefix:` lines match against.
    fn subject(&self) -> &str;
    /// The finding reporting that baseline line `line` of `file` is wrong.
    fn baseline_finding(file: &str, line: String, detail: &str) -> Self;
}

/// The keys a baseline file waives.
pub(super) struct Baseline {
    pub(super) file: String,
    pub(super) keys: BTreeSet<String>,
    /// The `prefix:` lines: every subject starting with one is waived.
    pub(super) prefixes: Vec<String>,
    /// Lines listed twice.
    pub(super) duplicates: Vec<String>,
}

impl Baseline {
    /// Reads `path`; a missing file waives nothing.
    pub(super) fn load(path: &Path) -> Self {
        let text = fs::read_to_string(path).unwrap_or_default();
        Self::parse(&path.display().to_string(), &text)
    }

    /// The baseline of `text`, read from `file`.
    pub(super) fn parse(file: &str, text: &str) -> Self {
        let mut keys = BTreeSet::new();
        let mut prefixes = Vec::new();
        let mut duplicates = Vec::new();
        for line in text.lines().map(str::trim).filter(|l| !l.is_empty() && !l.starts_with('#')) {
            if let Some(prefix) = line.strip_prefix("prefix:") {
                if prefixes.iter().any(|p| p == prefix) {
                    duplicates.push(line.to_string());
                } else {
                    prefixes.push(prefix.to_string());
                }
            } else if !keys.insert(line.to_string()) {
                duplicates.push(line.to_string());
            }
        }
        Baseline { file: file.to_string(), keys, prefixes, duplicates }
    }

    /// Drops the waived findings and adds one per stale or duplicated line.
    pub(super) fn apply<F: Waivable>(&self, findings: Vec<F>) -> Vec<F> {
        let mut used_keys = BTreeSet::new();
        let mut used_prefixes = BTreeSet::new();
        let mut kept = Vec::new();
        for f in findings {
            let key = f.key();
            if self.keys.contains(&key) {
                used_keys.insert(key);
                continue;
            }
            if let Some(p) = self.prefixes.iter().find(|p| f.subject().starts_with(p.as_str())) {
                used_prefixes.insert(p.clone());
                continue;
            }
            kept.push(f);
        }
        for key in self.keys.difference(&used_keys) {
            kept.push(F::baseline_finding(
                &self.file,
                key.clone(),
                "stale entry: no finding matches it any more; delete the line",
            ));
        }
        for prefix in self.prefixes.iter().filter(|p| !used_prefixes.contains(*p)) {
            kept.push(F::baseline_finding(
                &self.file,
                format!("prefix:{prefix}"),
                "stale entry: no finding starts with it any more; delete the line",
            ));
        }
        for line in &self.duplicates {
            kept.push(F::baseline_finding(&self.file, line.clone(), "listed twice; delete one"));
        }
        kept
    }
}
