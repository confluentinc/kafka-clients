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

//! The changelog step of `bump-version`: the `## [Unreleased]` section of the
//! changelog of each package whose version moves becomes
//! `## [<version>] - <date>`, the date in PST (UTC-8), and an agent first
//! fills in the entries the section is missing.
//!
//! The release of a pre-release (`1.1.0` after `1.1.0-rc.1`) makes no new
//! section: the pre-release's section is renamed to the release's, dated
//! today, the `[Unreleased]` entries folded into its subsections and the
//! `[Unreleased]` section removed. Without a section for the pre-release, the
//! `[Unreleased]` section is released as for any other version.
//!
//! The agent is a command taking Claude Code's options, as for the test
//! selection (`--agent`, `--model`, `--timeout`, by default a local Claude Code
//! with the same read-only tools): it reads the commits since the package's
//! previous release and answers with the section's new content, which keeps
//! every line the section had, in its order. Without an agent (`--no-agent`)
//! the section is renamed as it is.
//!
//! Every edit is returned, not written, so `bump-version` writes nothing when
//! the agent fails or its answer is refused.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context as _};

use crate::test_selection::{run_with_deadline, AGENT_BUILTIN_TOOLS, AGENT_DENIED_TOOLS, AGENT_TOOLS};

/// The heading of the section that collects the changes not yet released.
const UNRELEASED: &str = "## [Unreleased]";

/// How long the agent may take by default: it reads every commit since the
/// previous release.
pub(super) const DEFAULT_AGENT_TIMEOUT_SECS: u64 = 1200;

/// Above this size the commit list is left for the agent to read with git.
const MAX_INLINE_LOG: usize = 64 * 1024;

/// A changelog released with a version.
pub(super) struct Release {
    /// The changelog, relative to the repository root.
    pub(super) file: &'static str,
    /// What the changelog documents, for the agent.
    pub(super) package: &'static str,
    /// The prefix of the package's release tags (`rust/v` for `rust/v1.0.0`).
    pub(super) tag_prefix: &'static str,
    /// The new version, in the form the changelog's headings use.
    pub(super) version: String,
    /// The pre-release, in the same form, whose section the new version's
    /// replaces, when the new version is its release.
    pub(super) pre_release: Option<String>,
}

/// The agent that fills in the missing entries, or none.
pub(super) struct Agent<'a> {
    pub(super) command: &'a str,
    pub(super) model: &'a str,
    pub(super) timeout: Duration,
}

/// A `## ` section of a changelog: the byte offsets of its heading, of its
/// first body line and of the end of its body.
#[derive(Debug, PartialEq, Eq)]
struct Section {
    start: usize,
    body: usize,
    end: usize,
}

/// One changelog as read, with what the prompt needs.
struct Changelog<'r> {
    release: &'r Release,
    path: PathBuf,
    text: String,
    unreleased: Option<Section>,
    /// The pre-release's section the release renames, when there is one.
    renamed: Option<Section>,
    /// The section's body before the agent fills it in: the `[Unreleased]`
    /// entries, folded into the renamed section's when there is one.
    body: String,
    previous: Option<String>,
}

impl Changelog<'_> {
    /// The section the release starts from, as the prompt names it.
    fn source(&self) -> String {
        match (&self.renamed, &self.release.pre_release) {
            (Some(_), Some(pre)) => format!("[{pre}]"),
            _ => "[Unreleased]".to_string(),
        }
    }
}

/// The edits that release `releases`' changelogs at today's PST date, the
/// missing entries filled in by `agent` when given.
pub(super) fn edits(
    root: &Path,
    releases: &[Release],
    agent: Option<&Agent>,
) -> anyhow::Result<Vec<(PathBuf, String)>> {
    let date = pst_date(SystemTime::now());
    let mut changelogs = Vec::new();
    for release in releases {
        let path = root.join(release.file);
        let text = super::read(&path)?;
        let unreleased = find_section(&text, |heading| heading == UNRELEASED);
        let renamed = release.pre_release.as_deref().and_then(|pre| version_section(&text, pre));
        if let (Some(pre), None) = (&release.pre_release, &renamed) {
            println!("   {} has no [{pre}] section to rename: [Unreleased] is released", release.file);
        }
        let unreleased_body = unreleased.as_ref().map_or("", |section| &text[section.body..section.end]);
        let (body, previous) = match &renamed {
            Some(section) => (
                merge_bodies(&text[section.body..section.end], unreleased_body),
                release.pre_release.clone(),
            ),
            None => (unreleased_body.to_string(), previous_version(&text)),
        };
        changelogs.push(Changelog { release, path, unreleased, renamed, body, previous, text });
    }
    let bodies = match agent {
        Some(agent) if !changelogs.is_empty() => ask(root, agent, &changelogs, &date)?,
        _ => changelogs.iter().map(|changelog| changelog.body.clone()).collect(),
    };

    let mut edits = Vec::new();
    for (changelog, body) in changelogs.iter().zip(bodies) {
        let Changelog { release, path, text, unreleased, renamed, .. } = changelog;
        // An empty section is released too, so every version the package
        // moves to has its section.
        if body.trim().is_empty() {
            println!(
                "⚠️  {}: [{}] has no entries: add them by hand if the release has changes",
                release.file, release.version
            );
        }
        let released = match renamed {
            Some(section) => rename_section(text, section, unreleased.as_ref(), &release.version, &date, &body),
            None => release_section(text, unreleased.as_ref(), &release.version, &date, &body),
        };
        println!(
            "📝 {}: {} becomes [{}] - {date}",
            release.file,
            changelog.source(),
            release.version
        );
        edits.push((path.clone(), released));
    }
    Ok(edits)
}

/// Asks the agent for each changelog's new section body, checked to keep
/// every line the section had.
fn ask(root: &Path, agent: &Agent, changelogs: &[Changelog], date: &str) -> anyhow::Result<Vec<String>> {
    let words: Vec<&str> = agent.command.split_whitespace().collect();
    let Some((program, agent_args)) = words.split_first() else {
        bail!("bump-version: --agent is empty; pass --no-agent to release the changelogs as they are");
    };
    let mut ranges = Vec::new();
    for changelog in changelogs {
        ranges.push(release_base(root, changelog)?);
    }
    let prompt = prompt(root, changelogs, &ranges, date)?;
    eprintln!(
        "bump-version: asking `{}` for the missing changelog entries of {}...",
        agent.command,
        changelogs
            .iter()
            .map(|changelog| changelog.release.file)
            .collect::<Vec<_>>()
            .join(", ")
    );
    let mut command = Command::new(program);
    command
        .args(agent_args)
        .args(["--model", agent.model])
        .args(["--restricted", "--strict-mcp-config", "--tools", AGENT_BUILTIN_TOOLS])
        .args(["--allowedTools", AGENT_TOOLS, "--disallowedTools", AGENT_DENIED_TOOLS])
        .args(["-p", &prompt])
        .current_dir(root);
    let (status, answer) = run_with_deadline("bump-version", &mut command, agent.timeout)?;
    if !status.success() {
        bail!("bump-version: the agent failed ({status}):\n{answer}");
    }
    changelogs
        .iter()
        .map(|changelog| {
            let file = changelog.release.file;
            let body = answer_body(&answer, file).with_context(|| format!("the agent's answer:\n{answer}"))?;
            check_body(&changelog.body, &body).with_context(|| format!("{file}: the agent's section:\n{body}"))?;
            Ok(body)
        })
        .collect()
}

/// The commit the changelog's previous release was cut from, and how it was
/// found: the merge base of its release tag and HEAD (a release may be tagged
/// on a branch of its own), or else the last commit that changed the
/// previous release's heading. A changelog with no release yet starts from
/// the package's latest release tag before HEAD.
fn release_base(root: &Path, changelog: &Changelog) -> anyhow::Result<(String, String)> {
    let file = changelog.release.file;
    let prefix = changelog.release.tag_prefix;
    let Some(previous) = &changelog.previous else {
        let tag = git(
            root,
            &[
                "describe",
                "--tags",
                "--abbrev=0",
                "--match",
                &format!("{prefix}*"),
                "HEAD",
            ],
        )
        .with_context(|| format!("{file} has no release, and no {prefix}* tag is before HEAD; pass --no-agent"))?;
        let base = git(root, &["rev-parse", &format!("{tag}^{{commit}}")])?;
        return Ok((
            base,
            format!("the latest release tag before HEAD, {tag}: {file} has no release section yet"),
        ));
    };
    let tag = format!("{prefix}{previous}");
    if let Ok(base) = git(root, &["merge-base", &format!("refs/tags/{tag}"), "HEAD"]) {
        return Ok((base, format!("the merge base of the tag {tag} and HEAD")));
    }
    let heading = format!("## [{previous}] - ");
    let base = git(root, &["log", "-1", "--format=%H", "-S", &heading, "--", file])?;
    if base.is_empty() {
        bail!("{file}: neither the tag {tag} nor the commit that added [{previous}] is found; pass --no-agent");
    }
    Ok((
        base,
        format!("the last commit that changed the [{previous}] heading (no tag {tag} here)"),
    ))
}

fn prompt(root: &Path, changelogs: &[Changelog], ranges: &[(String, String)], date: &str) -> anyhow::Result<String> {
    let mut prompt = String::from(
        "You prepare a release of Confluent's Kafka clients (this repository). Each changelog below has a \
         section that becomes the section of the new version: its `## [Unreleased]` section or, for the \
         release of a pre-release, the pre-release's section, into which the [Unreleased] entries are \
         already folded; the version heading is written for you. Your task is to fill in the entries the \
         section is missing.\n\n\
         Read every commit in the changelog's range (`git log`, `git show`, `git diff`), and for each change a \
         user of the package the changelog documents can see, and that no entry covers yet, add an entry: new or \
         changed API, configuration keys and defaults, behaviour, errors and their messages, fixed bugs, \
         supported platforms and dependencies, packaging, breaking changes. Leave out what users cannot see: \
         tests, CI, tooling (xtask, Makefiles, git hooks, scripts), internal refactoring, agent rules, design \
         notes, and changes to the other packages of the repository, which have changelogs or release notes of \
         their own.\n\n\
         Rules for the section you return:\n\
         - Keep every line the section has exactly as it is, in its order: add entries, never reword, \
         reorder, merge or remove one. Keep each entry whole and in its subsection: add a new entry only \
         between two entries, and a new subsection only between two subsections.\n\
         - Put a new entry under the `###` subsection it belongs to, reusing the subsections the changelog \
         already uses (read the file); add a subsection only when none fits.\n\
         - Write entries as the changelog writes them: a `- ` bullet, lines wrapped at 79 columns with a \
         two-space continuation indent, plain statements of what changed for the user, names in backticks, \
         `**Breaking:**` before a change that breaks existing code.\n\
         - Write no `#` or `##` heading.\n\
         - You only read files and run git: change no file.\n\n",
    );
    prompt.push_str(&format!("The release date is {date}.\n\n"));
    for (changelog, (base, how)) in changelogs.iter().zip(ranges) {
        let release = changelog.release;
        let previous = changelog.previous.as_deref().unwrap_or("none in this changelog");
        prompt.push_str(&format!(
            "## {file}\n\nIt documents {package}. The new version is {version}; the previous release is \
             {previous}. The range is {base}..HEAD, where {base} is {how}.\n\n",
            file = release.file,
            package = release.package,
            version = release.version,
        ));
        let log = git(root, &["log", "--no-merges", "--format=%h %s", &format!("{base}..HEAD")])?;
        if log.len() <= MAX_INLINE_LOG {
            prompt.push_str(&format!("Its commits, newest first:\n\n{log}\n\n"));
        } else {
            prompt.push_str("Its commits are too many to list here: read them with `git log`.\n\n");
        }
        if changelog.renamed.is_some() {
            prompt.push_str(&format!(
                "{version} is the release of the pre-release {previous}: no new section is made, the {previous} \
                 section becomes {version}'s, and what is missing is what changed since {previous}.\n\n",
                version = release.version,
            ));
        }
        match (&changelog.unreleased, &changelog.renamed) {
            (None, None) => prompt.push_str("It has no [Unreleased] section: every entry is missing.\n\n"),
            _ => prompt.push_str(&format!(
                "Its {} section now (between the markers):\n\n{}\n{}\n{}\n\n",
                changelog.source(),
                begin_marker(release.file),
                changelog.body.trim_matches('\n'),
                end_marker(release.file),
            )),
        }
    }
    prompt.push_str("Answer with the new content of each section, between its marker lines, as above:\n\n");
    for changelog in changelogs {
        let file = changelog.release.file;
        prompt.push_str(&format!(
            "{}\n<the section's lines>\n{}\n",
            begin_marker(file),
            end_marker(file)
        ));
    }
    Ok(prompt)
}

fn begin_marker(file: &str) -> String {
    format!("=== BEGIN {file} ===")
}

fn end_marker(file: &str) -> String {
    format!("=== END {file} ===")
}

/// The lines between the last pair of `file`'s markers in `answer`. Markers,
/// unlike a fenced block, cannot clash with the Markdown of an entry.
fn answer_body(answer: &str, file: &str) -> anyhow::Result<String> {
    let begin = begin_marker(file);
    let end = end_marker(file);
    let start = answer
        .rfind(&begin)
        .with_context(|| format!("no `{begin}` line in the answer"))?;
    let after = &answer[start + begin.len()..];
    let stop = after.find(&end).with_context(|| format!("no `{end}` line after `{begin}`"))?;
    Ok(after[..stop].trim_matches('\n').to_string())
}

/// Refuses a section that holds a heading above `###`, or that does not keep
/// every entry the section had: each entry whole (its first line and its
/// continuation lines, nothing added between them), in the subsection it was
/// in, in its order, and the subsections in their order. New entries and
/// subsections may go only between whole entries and subsections.
fn check_body(old: &str, new: &str) -> anyhow::Result<()> {
    if let Some(heading) = new.lines().find(|line| line.starts_with("# ") || line.starts_with("## ")) {
        bail!("the section holds the heading `{heading}`");
    }
    let new_groups = entry_groups(new);
    for (at, (heading, _)) in new_groups.iter().enumerate() {
        if heading.is_some() && new_groups[..at].iter().any(|(other, _)| other == heading) {
            bail!("the section has the subsection `{}` twice", heading.unwrap_or_default());
        }
    }
    let mut next_group = 0;
    for (heading, entries) in entry_groups(old) {
        let name = heading.unwrap_or("before the first subsection");
        let Some(at) = new_groups.iter().position(|(other, _)| *other == heading) else {
            bail!("the section no longer has the subsection `{name}`");
        };
        if at < next_group {
            bail!("the subsection `{name}` moved before one that came before it");
        }
        next_group = at + 1;
        let mut kept = new_groups[at].1.iter();
        for entry in entries {
            if !kept.any(|new_entry| *new_entry == entry) {
                bail!(
                    "the subsection `{name}` no longer has, whole and where it was, the entry:\n{}",
                    entry.join("\n")
                );
            }
        }
    }
    Ok(())
}

/// A section body's subsections, the lines before the first `### ` heading
/// under `None`, each as its entries: an entry is a line at the left margin
/// and the lines that continue it, indented ones (after a blank line too) and,
/// for a paragraph that is not a list item, the lines that follow it with no
/// blank line between. Lines are compared without trailing blanks. A group
/// with no entry is kept only when it has a heading.
fn entry_groups(body: &str) -> Vec<(Option<&str>, Vec<Vec<&str>>)> {
    let is_item = |line: &str| {
        line.starts_with("- ")
            || line.starts_with("* ")
            || line
                .split_once(". ")
                .is_some_and(|(n, _)| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
    };
    let mut groups: Vec<(Option<&str>, Vec<Vec<&str>>)> = vec![(None, Vec::new())];
    let mut after_blank = false;
    for line in body.lines().map(str::trim_end) {
        if let Some(heading) = line.strip_prefix("### ").map(|_| line) {
            groups.push((Some(heading), Vec::new()));
            after_blank = false;
            continue;
        }
        if line.is_empty() {
            after_blank = true;
            continue;
        }
        let Some((_, entries)) = groups.last_mut() else {
            continue;
        };
        let indented = line.starts_with(' ') || line.starts_with('\t');
        let continues = match entries.last() {
            Some(entry) if indented => !entry.is_empty(),
            Some(entry) => !after_blank && !is_item(line) && !entry.first().is_some_and(|first| is_item(first)),
            None => false,
        };
        match entries.last_mut() {
            Some(entry) if continues => entry.push(line),
            _ => entries.push(vec![line]),
        }
        after_blank = false;
    }
    groups.retain(|(heading, entries)| heading.is_some() || !entries.is_empty());
    groups
}

/// The first section of `text` whose heading line `is_heading` accepts, up
/// to the next `## ` heading or the end.
fn find_section(text: &str, is_heading: impl Fn(&str) -> bool) -> Option<Section> {
    let mut offset = 0;
    let mut section: Option<Section> = None;
    for line in text.split_inclusive('\n') {
        let heading = line.trim_end();
        if let Some(open) = section.as_mut() {
            if heading.starts_with("## ") {
                open.end = offset;
                return section;
            }
        } else if heading.starts_with("## ") && is_heading(heading) {
            section = Some(Section { start: offset, body: offset + line.len(), end: text.len() });
        }
        offset += line.len();
    }
    section
}

/// The section of `version`, `## [<version>] - <date>`.
fn version_section(text: &str, version: &str) -> Option<Section> {
    let heading = format!("## [{version}]");
    find_section(text, |line| {
        line.strip_prefix(heading.as_str())
            .is_some_and(|rest| rest.is_empty() || rest.starts_with(" - "))
    })
}

/// A section body's `### ` subsections, the lines before the first one under
/// `None`, each without its leading and trailing blank lines.
fn subsections(body: &str) -> Vec<(Option<&str>, Vec<&str>)> {
    let mut groups: Vec<(Option<&str>, Vec<&str>)> = vec![(None, Vec::new())];
    for line in body.lines() {
        if line.starts_with("### ") {
            groups.push((Some(line.trim_end()), Vec::new()));
        } else if let Some((_, lines)) = groups.last_mut() {
            lines.push(line);
        }
    }
    for (_, lines) in &mut groups {
        while lines.first().is_some_and(|line| line.trim().is_empty()) {
            lines.remove(0);
        }
        while lines.last().is_some_and(|line| line.trim().is_empty()) {
            lines.pop();
        }
    }
    groups
}

/// `base` with the entries of `extra` added: each subsection's at the end of
/// `base`'s subsection of the same heading, or as a new subsection after the
/// others.
fn merge_bodies(base: &str, extra: &str) -> String {
    let mut groups = subsections(base);
    for (heading, lines) in subsections(extra) {
        match groups.iter_mut().find(|(kept, _)| *kept == heading) {
            Some((_, kept)) => kept.extend(lines),
            None => groups.push((heading, lines)),
        }
    }
    groups
        .into_iter()
        .filter_map(|(heading, lines)| {
            let lines = lines.join("\n");
            match heading {
                None if lines.is_empty() => None,
                None => Some(lines),
                Some(heading) if lines.is_empty() => Some(heading.to_string()),
                Some(heading) => Some(format!("{heading}\n\n{lines}")),
            }
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// `text` with `renamed`, a pre-release's section, renamed to
/// `## [<version>] - <date>` and holding `body`, and `unreleased`, whose
/// entries `body` holds, removed.
fn rename_section(
    text: &str,
    renamed: &Section,
    unreleased: Option<&Section>,
    version: &str,
    date: &str,
    body: &str,
) -> String {
    let mut section = section_text(version, date, body);
    if renamed.end < text.len() {
        section.push('\n');
    }
    let mut ranges = vec![(renamed.start, renamed.end, section)];
    if let Some(unreleased) = unreleased {
        ranges.push((unreleased.start, unreleased.end, String::new()));
    }
    ranges.sort_by_key(|(start, ..)| std::cmp::Reverse(*start));
    let mut out = text.to_string();
    for (start, end, replacement) in ranges {
        out.replace_range(start..end, &replacement);
    }
    while out.ends_with("\n\n") {
        out.pop();
    }
    out
}

/// The version of the first released section, `## [<version>] - <date>`.
fn previous_version(text: &str) -> Option<String> {
    text.lines()
        .filter_map(|line| line.strip_prefix("## ["))
        .filter_map(|rest| rest.split_once("] - "))
        .map(|(version, _)| version.to_string())
        .next()
}

/// `text` with `unreleased` (or, when there is none, a new section before the
/// first release) released as `## [<version>] - <date>` holding `body`.
fn release_section(text: &str, unreleased: Option<&Section>, version: &str, date: &str, body: &str) -> String {
    let mut section = section_text(version, date, body);
    let (start, end) = match unreleased {
        Some(Section { start, end, .. }) => (*start, *end),
        None => {
            let first = text.find("\n## [").map_or(text.len(), |at| at + 1);
            (first, first)
        },
    };
    // A blank line before the next heading, and before this one when it is
    // added after the last line.
    if end < text.len() {
        section.push('\n');
    }
    let before = &text[..start];
    let gap = match unreleased {
        None if start == text.len() && !before.ends_with("\n\n") => {
            if before.ends_with('\n') {
                "\n"
            } else {
                "\n\n"
            }
        },
        _ => "",
    };
    format!("{before}{gap}{section}{}", &text[end..])
}

/// A released section: its heading and, when it has one, its body.
fn section_text(version: &str, date: &str, body: &str) -> String {
    match body.trim_matches('\n') {
        "" => format!("## [{version}] - {date}\n"),
        body => format!("## [{version}] - {date}\n\n{body}\n"),
    }
}

/// Today's date, `YYYY-MM-DD`, in Pacific Standard Time (UTC-8).
fn pst_date(now: SystemTime) -> String {
    let secs = match now.duration_since(UNIX_EPOCH) {
        Ok(since) => since.as_secs() as i64,
        Err(before) => -(before.duration().as_secs() as i64),
    };
    let (year, month, day) = civil_from_days((secs - 8 * 3600).div_euclid(86_400));
    format!("{year:04}-{month:02}-{day:02}")
}

/// The proleptic Gregorian date of a day count from 1970-01-01 (Howard
/// Hinnant's `civil_from_days`).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

fn git(root: &Path, args: &[&str]) -> anyhow::Result<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .context("running git")?;
    if !output.status.success() {
        bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHANGELOG: &str = "# Changelog\n\nIntro.\n\n## [Unreleased]\n\n### Common\n\n- One.\n\n\
                             ## [1.0.0] - 2026-10-30\n\n- Old.\n";

    #[test]
    fn pst_date_is_utc_minus_eight() {
        let at = |secs: u64| pst_date(UNIX_EPOCH + Duration::from_secs(secs));
        assert_eq!(at(0), "1969-12-31");
        assert_eq!(at(8 * 3600), "1970-01-01");
        // 2000-03-01T00:00:00Z: a leap day before it.
        assert_eq!(at(951_868_800 + 8 * 3600), "2000-03-01");
        assert_eq!(at(951_868_800 + 8 * 3600 - 1), "2000-02-29");
        // 2026-11-27T07:59:59Z is still the 26th in PST.
        assert_eq!(at(1_795_766_399), "2026-11-26");
        assert_eq!(at(1_795_766_400), "2026-11-27");
    }

    #[test]
    fn the_unreleased_section_runs_to_the_next_heading() {
        let section = find_section(CHANGELOG, |heading| heading == UNRELEASED).unwrap();
        assert_eq!(&CHANGELOG[section.start..section.body], "## [Unreleased]\n");
        assert_eq!(&CHANGELOG[section.body..section.end], "\n### Common\n\n- One.\n\n");
        let last = "# C\n\n## [Unreleased]\n\n- One.\n";
        let section = unreleased(last).unwrap();
        assert_eq!(&last[section.body..section.end], "\n- One.\n");
        assert_eq!(unreleased("# C\n\n## [1.0.0] - 2026-10-30\n"), None);
    }

    #[test]
    fn the_previous_version_is_the_first_released_heading() {
        assert_eq!(previous_version(CHANGELOG).as_deref(), Some("1.0.0"));
        assert_eq!(previous_version("# C\n\n## [Unreleased]\n"), None);
    }

    #[test]
    fn the_unreleased_heading_becomes_the_version() {
        let section = unreleased(CHANGELOG);
        let body = "### Common\n\n- One.\n- Two.";
        assert_eq!(
            release_section(CHANGELOG, section.as_ref(), "1.1.0-rc.1", "2026-11-27", body),
            "# Changelog\n\nIntro.\n\n## [1.1.0-rc.1] - 2026-11-27\n\n### Common\n\n- One.\n- Two.\n\n\
             ## [1.0.0] - 2026-10-30\n\n- Old.\n"
        );
        let last = "# C\n\n## [Unreleased]\n\n- One.\n";
        assert_eq!(
            release_section(last, unreleased(last).as_ref(), "1.0.0", "2026-11-27", "- One."),
            "# C\n\n## [1.0.0] - 2026-11-27\n\n- One.\n"
        );
    }

    fn unreleased(text: &str) -> Option<Section> {
        find_section(text, |heading| heading == UNRELEASED)
    }

    /// A changelog at a pre-release, with entries not yet released.
    const AT_RC: &str = "# C\n\n## [Unreleased]\n\n### Common\n\n- Since rc.\n\n### Python\n\n- New subsection.\n\n\
                         ## [1.1.0-rc.1] - 2026-11-20\n\nIntro.\n\n### Common\n\n- In rc.\n\n### C FFI\n\n- FFI.\n\n\
                         ## [1.0.0] - 2026-10-30\n\n- Old.\n";

    #[test]
    fn a_version_section_is_found_by_its_exact_version() {
        let section = version_section(AT_RC, "1.1.0-rc.1").unwrap();
        assert!(AT_RC[section.start..].starts_with("## [1.1.0-rc.1] - 2026-11-20\n"));
        assert!(AT_RC[section.end..].starts_with("## [1.0.0]"));
        assert!(version_section(AT_RC, "1.1.0").is_none());
        assert!(version_section(AT_RC, "1.1.0-rc").is_none());
        assert!(version_section("## [1.1.0-rc.10] - 2026-11-20\n", "1.1.0-rc.1").is_none());
    }

    #[test]
    fn unreleased_entries_join_the_subsections_of_their_heading() {
        let rc = version_section(AT_RC, "1.1.0-rc.1").unwrap();
        let unreleased = unreleased(AT_RC).unwrap();
        assert_eq!(
            merge_bodies(&AT_RC[rc.body..rc.end], &AT_RC[unreleased.body..unreleased.end]),
            "Intro.\n\n### Common\n\n- In rc.\n- Since rc.\n\n### C FFI\n\n- FFI.\n\n### Python\n\n- New subsection."
        );
        assert_eq!(merge_bodies("\n### Common\n\n- In rc.\n\n", ""), "### Common\n\n- In rc.");
        assert_eq!(merge_bodies("", "- Loose.\n"), "- Loose.");
    }

    #[test]
    fn the_release_of_a_pre_release_renames_its_section() {
        let rc = version_section(AT_RC, "1.1.0-rc.1").unwrap();
        let unreleased = unreleased(AT_RC);
        let body = merge_bodies(
            &AT_RC[rc.body..rc.end],
            &AT_RC[unreleased.as_ref().unwrap().body..unreleased.as_ref().unwrap().end],
        );
        assert_eq!(
            rename_section(AT_RC, &rc, unreleased.as_ref(), "1.1.0", "2026-11-27", &body),
            "# C\n\n## [1.1.0] - 2026-11-27\n\nIntro.\n\n### Common\n\n- In rc.\n- Since rc.\n\n### C FFI\n\n- FFI.\n\n\
             ### Python\n\n- New subsection.\n\n## [1.0.0] - 2026-10-30\n\n- Old.\n"
        );
        let only_rc = "# C\n\n## [1.1.0-rc.1] - 2026-11-20\n\n- In rc.\n";
        let rc = version_section(only_rc, "1.1.0-rc.1").unwrap();
        assert_eq!(
            rename_section(only_rc, &rc, None, "1.1.0", "2026-11-27", "- In rc."),
            "# C\n\n## [1.1.0] - 2026-11-27\n\n- In rc.\n"
        );
    }

    #[test]
    fn without_an_unreleased_section_one_is_added_before_the_first_release() {
        let text = "# C\n\n## [1.0.0] - 2026-10-30\n\n- Old.\n";
        assert_eq!(
            release_section(text, None, "1.1.0", "2026-11-27", "- New."),
            "# C\n\n## [1.1.0] - 2026-11-27\n\n- New.\n\n## [1.0.0] - 2026-10-30\n\n- Old.\n"
        );
        assert_eq!(
            release_section("# C\n", None, "0.1.0", "2026-11-27", "- New."),
            "# C\n\n## [0.1.0] - 2026-11-27\n\n- New.\n"
        );
    }

    #[test]
    fn the_answer_body_is_between_the_last_markers() {
        let file = "rust/CHANGELOG.md";
        let answer = format!(
            "Here:\n{}\n<the section's lines>\n{}\nDone:\n{}\n### Common\n\n```\ncode\n```\n- One.\n{}\n",
            begin_marker(file),
            end_marker(file),
            begin_marker(file),
            end_marker(file)
        );
        assert_eq!(answer_body(&answer, file).unwrap(), "### Common\n\n```\ncode\n```\n- One.");
        assert!(answer_body("nothing", file).is_err());
        assert!(answer_body(&begin_marker(file), file).is_err());
    }

    #[test]
    fn the_agent_may_only_add_lines() {
        let old = "Intro,\non two lines.\n\n### Common\n\n- One,\n  wrapped.\n\n- Two.\n\n### C FFI\n\n- Three.\n";
        for good in [
            "Intro,\non two lines.\n\n### Common\n\n- One,\n  wrapped.\n- New.\n\n- Two.\n\n### C FFI\n\n- Three.",
            // A new subsection between two, or before the first.
            "Intro,\non two lines.\n\n### New\n\n- New.\n\n### Common\n\n- One,\n  wrapped.\n- Two.\n\n\
             ### Producer\n\n- New.\n\n### C FFI\n\n- Three.\n- New.",
        ] {
            check_body(old, good).unwrap();
        }
        for bad in [
            // A line or an entry dropped, reworded or reordered.
            "Intro,\non two lines.\n\n### Common\n\n- Two.\n- One,\n  wrapped.\n\n### C FFI\n\n- Three.",
            "Intro,\non two lines.\n\n### Common\n\n- One, wrapped.\n- Two.\n\n### C FFI\n\n- Three.",
            "Intro,\non two lines.\n\n### Common\n\n- One,\n  wrapped.\n\n### C FFI\n\n- Three.",
            "Intro.\n\n### Common\n\n- One,\n  wrapped.\n- Two.\n\n### C FFI\n\n- Three.",
            // A new entry between an entry and its continuation.
            "Intro,\non two lines.\n\n### Common\n\n- One,\n- New,\n  wrapped.\n- Two.\n\n### C FFI\n\n- Three.",
            "Intro,\n- New.\non two lines.\n\n### Common\n\n- One,\n  wrapped.\n- Two.\n\n### C FFI\n\n- Three.",
            // A new subsection taking an entry out of its own.
            "Intro,\non two lines.\n\n### Common\n\n- One,\n  wrapped.\n\n### Producer\n\n- Two.\n\n### C FFI\n\n- Three.",
            // Subsections reordered, dropped or repeated.
            "Intro,\non two lines.\n\n### C FFI\n\n- Three.\n\n### Common\n\n- One,\n  wrapped.\n- Two.",
            "Intro,\non two lines.\n\n### Common\n\n- One,\n  wrapped.\n- Two.\n- Three.",
            "Intro,\non two lines.\n\n### Common\n\n- One,\n  wrapped.\n- Two.\n\n### C FFI\n\n- Three.\n\n\
             ### Common\n\n- New.",
            "## [1.1.0]\n\nIntro,\non two lines.\n\n### Common\n\n- One,\n  wrapped.\n- Two.\n\n### C FFI\n\n- Three.",
        ] {
            assert!(check_body(old, bad).is_err(), "{bad:?}");
        }
        check_body("", "### Common\n\n- New.").unwrap();
    }

    #[test]
    fn entries_are_items_with_their_continuations() {
        assert_eq!(
            entry_groups(
                "Intro,\nmore.\n\n- Loose.\n\n### A\n\n- One,\n  two.\n\n  More of one.\n- Two.\n1. Three.\n\n### B\n"
            ),
            [
                (None, vec![vec!["Intro,", "more."], vec!["- Loose."]]),
                (
                    Some("### A"),
                    vec![
                        vec!["- One,", "  two.", "  More of one."],
                        vec!["- Two."],
                        vec!["1. Three."]
                    ]
                ),
                (Some("### B"), vec![]),
            ]
        );
    }

    #[test]
    fn an_empty_section_is_a_bare_heading() {
        let text = "# C\n\n## [Unreleased]\n\n## [1.0.0] - 2026-10-30\n\n- Old.\n";
        assert_eq!(
            release_section(text, unreleased(text).as_ref(), "1.1.0-rc.1", "2026-11-27", "\n"),
            "# C\n\n## [1.1.0-rc.1] - 2026-11-27\n\n## [1.0.0] - 2026-10-30\n\n- Old.\n"
        );
        let text = "# C\n\n## [1.0.0] - 2026-10-30\n\n- Old.\n";
        assert_eq!(
            release_section(text, None, "1.1.0-rc.1", "2026-11-27", ""),
            "# C\n\n## [1.1.0-rc.1] - 2026-11-27\n\n## [1.0.0] - 2026-10-30\n\n- Old.\n"
        );
    }
}
