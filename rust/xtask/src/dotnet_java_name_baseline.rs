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

//! `cargo xtask dotnet-java-name-baseline [--add]`: rewrites the .NET binding's
//! `dotnet/analyzers/java-name-baseline.txt` from a build. It compiles
//! `Confluent.Kafka` for each of its target frameworks without the baseline, so
//! CKRDA0013 reports every finding the allowlist does not hold, and writes those
//! findings, sorted, under the file's header.
//!
//! The baseline only shrinks: a finding the file does not hold yet stops the
//! command, which names it, unless `--add` is given (for a Kafka upgrade that adds
//! Java API, say). A line of the allowlist or the pairs that matches nothing
//! (CKRDA0014) stops it too: those two files are kept by hand.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::process::Command;

/// The library project, relative to `rust/`.
const PROJECT: &str = "../dotnet/src/Confluent.Kafka/Confluent.Kafka.csproj";

/// The build props that reference the analyzer for every project, relative to `rust/`.
const BUILD_PROPS: &str = "../dotnet/Directory.Build.props";

/// The analyzer project, built first so the compile runs the current analyzer.
const ANALYZER_PROJECT: &str = "../dotnet/analyzers/Confluent.Kafka.Analyzers/Confluent.Kafka.Analyzers.csproj";

/// The baseline, relative to `rust/`.
const BASELINE: &str = "../dotnet/analyzers/java-name-baseline.txt";

/// Where each compile writes its intermediate files and its log, emptied first so
/// the compiler always runs: an up-to-date compile would report no finding.
const WORK_DIR: &str = "target/dotnet-java-name-baseline";

const NAME_RULE: &str = "warning CKRDA0013: ";
const STALE_RULE: &str = "warning CKRDA0014: ";

/// An analyzer that threw (AD0001) or could not be created (CS8032): its findings
/// are missing, so the log says nothing about the baseline.
const ANALYZER_FAILURES: &[&str] = &["warning AD0001: ", "warning CS8032: "];

pub fn run(args: &[String]) -> anyhow::Result<()> {
    let add = match args {
        [] => false,
        [flag] if flag == "--add" => true,
        _ => anyhow::bail!("usage: cargo xtask dotnet-java-name-baseline [--add]"),
    };
    let found = build_findings()?;
    let old = fs::read_to_string(BASELINE).map_err(|e| anyhow::anyhow!("{BASELINE}: {e}"))?;
    let rewrite = rewrite(&old, &found);
    if !rewrite.added.is_empty() && !add {
        anyhow::bail!(
            "{} finding(s) are not in {BASELINE}. Fix them, allowlist them, or rerun with --add:\n  {}",
            rewrite.added.len(),
            rewrite.added.join("\n  ")
        );
    }
    fs::write(BASELINE, &rewrite.text).map_err(|e| anyhow::anyhow!("{BASELINE}: {e}"))?;
    println!(
        "✅ Wrote {} line(s) to {BASELINE}: {} removed, {} added",
        found.keys.len(),
        rewrite.removed,
        rewrite.added.len()
    );
    Ok(())
}

/// The findings of a compile of every target framework, the baseline left out.
fn build_findings() -> anyhow::Result<Findings> {
    let project = fs::read_to_string(PROJECT).map_err(|e| anyhow::anyhow!("{PROJECT}: {e}"))?;
    let props = fs::read_to_string(BUILD_PROPS).map_err(|e| anyhow::anyhow!("{BUILD_PROPS}: {e}"))?;
    // A build with no finding is a clean one, unless the analyzer does not run at all.
    check_wiring(&project, &props)?;
    let frameworks = target_frameworks(&project)?;
    let work = Path::new(WORK_DIR);
    if work.exists() {
        fs::remove_dir_all(work)?;
    }
    fs::create_dir_all(work)?;
    let work = fs::canonicalize(work)?;

    dotnet(&["build", ANALYZER_PROJECT, "-nologo", "-v:q"])?;
    // `msbuild -t:Compile` does not restore.
    dotnet(&["restore", PROJECT, "-nologo", "-v:q"])?;
    let mut all = Findings::default();
    for framework in &frameworks {
        let log = work.join(format!("{framework}.log"));
        let obj = work.join("obj").join(framework);
        dotnet(&[
            "msbuild",
            PROJECT,
            "-t:Compile",
            &format!("-p:TargetFramework={framework}"),
            "-p:TreatWarningsAsErrors=false",
            "-p:JavaNameBaseline=false",
            &format!("-p:IntermediateOutputPath={}/", obj.display()),
            "-nologo",
            "-noconsolelogger",
            "-fl",
            &format!("-flp:LogFile={};WarningsOnly;Verbosity=quiet", log.display()),
        ])?;
        let text = fs::read_to_string(&log).map_err(|e| anyhow::anyhow!("{}: {e}", log.display()))?;
        let findings = findings(&text);
        anyhow::ensure!(
            findings.failures.is_empty(),
            "the analyzer failed in the {framework} compile, so the baseline is left as it is:\n  {}",
            findings.failures.join("\n  ")
        );
        all.keys.extend(findings.keys);
        all.stale.extend(findings.stale);
    }
    anyhow::ensure!(
        all.stale.is_empty(),
        "the allowlist or the pairs hold lines that match nothing (CKRDA0014); delete them:\n  {}",
        all.stale.into_iter().collect::<Vec<_>>().join("\n  ")
    );
    Ok(all)
}

fn dotnet(args: &[&str]) -> anyhow::Result<()> {
    let status = Command::new("dotnet")
        .args(args)
        .status()
        .map_err(|e| anyhow::anyhow!("dotnet: {e}"))?;
    anyhow::ensure!(status.success(), "`dotnet {}` failed", args.join(" "));
    Ok(())
}

/// Fails unless the analyzer runs on the project: `Directory.Build.props` references
/// it, and the project gives it the Java list (without which it checks nothing).
fn check_wiring(project: &str, props: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        props.contains("Confluent.Kafka.Analyzers.csproj") && props.contains(r#"OutputItemType="Analyzer""#),
        "{BUILD_PROPS} does not reference the analyzer project as an analyzer"
    );
    anyhow::ensure!(
        project.contains(r#"<AdditionalFiles Include="../../analyzers/java-api.txt""#),
        "{PROJECT} does not give the analyzer ../../analyzers/java-api.txt"
    );
    Ok(())
}

/// The frameworks of the project's `<TargetFrameworks>`.
fn target_frameworks(project: &str) -> anyhow::Result<Vec<String>> {
    let start = project
        .find("<TargetFrameworks>")
        .ok_or_else(|| anyhow::anyhow!("{PROJECT} has no <TargetFrameworks>"))?
        + "<TargetFrameworks>".len();
    let end = start
        + project[start..]
            .find("</TargetFrameworks>")
            .ok_or_else(|| anyhow::anyhow!("{PROJECT}: <TargetFrameworks> is not closed"))?;
    Ok(project[start..end]
        .split(';')
        .map(str::trim)
        .filter(|f| !f.is_empty())
        .map(str::to_string)
        .collect())
}

/// What a build log reports: the CKRDA0013 keys, the CKRDA0014 messages, and any
/// analyzer failure.
#[derive(Debug, Default, PartialEq)]
struct Findings {
    keys: BTreeSet<String>,
    stale: BTreeSet<String>,
    failures: Vec<String>,
}

/// The findings of an MSBuild log. A CKRDA0013 message is `<key>: <explanation>`,
/// and a key holds no `": "`; a line ends with ` [<project>::TargetFramework=…]`.
fn findings(log: &str) -> Findings {
    let mut found = Findings::default();
    for line in log.lines() {
        let line = line.rsplit_once(" [").map_or(line, |(message, _)| message);
        if let Some((_, message)) = line.split_once(NAME_RULE) {
            let key = message.split_once(": ").map_or(message, |(key, _)| key);
            found.keys.insert(key.to_string());
        } else if let Some((_, message)) = line.split_once(STALE_RULE) {
            found.stale.insert(message.to_string());
        } else if ANALYZER_FAILURES.iter().any(|f| line.contains(f)) {
            found.failures.push(line.to_string());
        }
    }
    found
}

/// The rewritten baseline: its header kept, then the keys, sorted.
#[derive(Debug, PartialEq)]
struct Rewrite {
    text: String,
    removed: usize,
    added: Vec<String>,
}

fn rewrite(old: &str, found: &Findings) -> Rewrite {
    let header: Vec<&str> = old
        .lines()
        .take_while(|l| l.trim().is_empty() || l.trim_start().starts_with('#'))
        .collect();
    let before: BTreeSet<&str> = old
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .collect();
    let mut text = String::new();
    for line in &header {
        text.push_str(line);
        text.push('\n');
    }
    for key in &found.keys {
        text.push_str(key);
        text.push('\n');
    }
    Rewrite {
        text,
        removed: before.iter().filter(|k| !found.keys.contains(**k)).count(),
        added: found.keys.iter().filter(|k| !before.contains(k.as_str())).cloned().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(list: &[&str]) -> Findings {
        Findings { keys: list.iter().map(|s| s.to_string()).collect(), ..Findings::default() }
    }

    #[test]
    fn reads_the_frameworks_of_the_project() {
        let project = "<Project>\n  <TargetFrameworks>netstandard2.0;net8.0; net10.0</TargetFrameworks>\n</Project>";
        assert_eq!(target_frameworks(project).unwrap(), ["netstandard2.0", "net8.0", "net10.0"]);
        assert!(target_frameworks("<Project/>").is_err());
    }

    #[test]
    fn reads_keys_and_stale_lines_from_a_log() {
        let log = "\
/x/Node.cs(3,5): warning CKRDA0013: unmatched M:Confluent.Kafka.Common.Node.Frob: Java's Node has no method of this name [/x/Confluent.Kafka.csproj::TargetFramework=net8.0]
/x/Node.cs(3,5): warning CKRDA0013: missing T:Confluent.Kafka.Common.Node org.apache.kafka.common.Node#rack(): has no counterpart of Java's org.apache.kafka.common.Node#rack() [/x/Confluent.Kafka.csproj::TargetFramework=net8.0]
/x/java-name-pairs.txt(4,1): warning CKRDA0014: 'A.B org.x.B' in java-name-pairs.txt matches nothing; delete it [/x/Confluent.Kafka.csproj::TargetFramework=net8.0]
/x/Other.cs(1,1): warning CS0618: something else [/x/Confluent.Kafka.csproj::TargetFramework=net8.0]
";
        let found = findings(log);
        assert_eq!(
            found.keys.into_iter().collect::<Vec<_>>(),
            [
                "missing T:Confluent.Kafka.Common.Node org.apache.kafka.common.Node#rack()",
                "unmatched M:Confluent.Kafka.Common.Node.Frob",
            ]
        );
        assert_eq!(
            found.stale.into_iter().collect::<Vec<_>>(),
            ["'A.B org.x.B' in java-name-pairs.txt matches nothing; delete it"]
        );
    }

    #[test]
    fn reads_an_analyzer_failure_from_a_log() {
        let log = "CSC : warning AD0001: Analyzer 'Confluent.Kafka.Analyzers.JavaNameAnalyzer' threw an exception [/x/Confluent.Kafka.csproj::TargetFramework=net8.0]\n";
        let found = findings(log);
        assert!(found.keys.is_empty());
        assert_eq!(
            found.failures,
            ["CSC : warning AD0001: Analyzer 'Confluent.Kafka.Analyzers.JavaNameAnalyzer' threw an exception"]
        );
    }

    #[test]
    fn checks_that_the_analyzer_runs_on_the_project() {
        let props = r#"<ProjectReference Include="analyzers/Confluent.Kafka.Analyzers/Confluent.Kafka.Analyzers.csproj" OutputItemType="Analyzer" />"#;
        let project = r#"<AdditionalFiles Include="../../analyzers/java-api.txt" />"#;
        assert!(check_wiring(project, props).is_ok());
        assert!(check_wiring("<Project/>", props).is_err());
        assert!(check_wiring(project, "<Project/>").is_err());
    }

    #[test]
    fn a_clean_build_empties_the_baseline_to_its_header() {
        let rewrite = rewrite("# header\nunmatched T:A\n", &keys(&[]));
        assert_eq!(rewrite.text, "# header\n");
        assert_eq!((rewrite.removed, rewrite.added.len()), (1, 0));
    }

    #[test]
    fn keeps_the_header_and_writes_the_keys_sorted() {
        let old = "# header one\n# header two\nunmatched T:B\nunmatched T:A\n";
        let rewrite = rewrite(old, &keys(&["unmatched T:B", "unmatched T:A"]));
        assert_eq!(rewrite.text, "# header one\n# header two\nunmatched T:A\nunmatched T:B\n");
        assert_eq!((rewrite.removed, rewrite.added.len()), (0, 0));
    }

    #[test]
    fn counts_what_a_fix_removed_and_names_what_is_new() {
        let old = "# header\nunmatched T:A\nunmatched T:B\n";
        let rewrite = rewrite(old, &keys(&["unmatched T:B", "unmatched T:C"]));
        assert_eq!(rewrite.text, "# header\nunmatched T:B\nunmatched T:C\n");
        assert_eq!(rewrite.removed, 1);
        assert_eq!(rewrite.added, ["unmatched T:C"]);
    }
}
