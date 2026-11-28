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

//! Apache Kafka test jars that a broker container loads classes from.
//!
//! Some Java integration tests configure the broker with a class of Kafka's own
//! test sources: `ShareConsumerRackAwareTest` names the share-group assignor
//! `org.apache.kafka.clients.consumer.RackAwareTestAssignor`, and two
//! `ShareConsumerDLQTest` tests name the remote storage manager
//! `org.apache.kafka.server.log.remote.storage.LocalTieredStorage`. Java's test
//! kit runs the broker in the test JVM, whose classpath holds those classes; the
//! `apache/kafka` image does not ship them. They are published on Maven Central
//! as test jars, so [`TestJar::local_path`] downloads one on first use into a
//! cache under the gitignored `target/` directory, checks its pinned SHA-256,
//! and a [`super::cluster_config::ClusterConfig::broker_libs`] cluster copies it
//! into each node's `/opt/kafka/libs`, which the image's start script puts on
//! the classpath.
//!
//! The jars are of release 4.4.0 and are built against that broker's internal
//! APIs (the group-coordinator assignor API, the remote storage manager API), so
//! a test that needs one runs only on a 4.4+ broker.

use std::path::Path;
use std::path::PathBuf;
use std::sync::Mutex;

use aws_lc_rs::digest::SHA256;
use aws_lc_rs::digest::digest;

/// The Maven Central repository the jars are downloaded from.
const MAVEN_CENTRAL: &str = "https://repo1.maven.org/maven2";

/// The release of every [`TestJar`].
const KAFKA_TEST_JARS_VERSION: &str = "4.4.0";

/// Serializes downloads within a test process, so concurrent cluster starts do
/// not fetch the same jar twice.
/// Separate processes download to partial files of their own (see
/// [`TestJar::local_path`]).
static DOWNLOAD_LOCK: Mutex<()> = Mutex::new(());

/// A Kafka test jar a broker container can load classes from.
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum TestJar {
    /// `org.apache.kafka:kafka-clients-integration-tests:4.4.0:test`, with
    /// `RackAwareTestAssignor`.
    ClientsIntegrationTests,
    /// `org.apache.kafka:kafka-storage:4.4.0:test-fixtures`, with
    /// `LocalTieredStorage`.
    StorageTestFixtures,
    /// `org.apache.kafka:kafka-clients:4.4.0:test-fixtures`, with
    /// `org.apache.kafka.test.TestUtils`, which `LocalTieredStorage` calls
    /// (`TestUtils#tempDirectory`).
    ClientsTestFixtures,
}

impl TestJar {
    /// The Maven artifact id and classifier.
    fn artifact(self) -> (&'static str, &'static str) {
        match self {
            Self::ClientsIntegrationTests => ("kafka-clients-integration-tests", "test"),
            Self::StorageTestFixtures => ("kafka-storage", "test-fixtures"),
            Self::ClientsTestFixtures => ("kafka-clients", "test-fixtures"),
        }
    }

    /// The pinned SHA-256 of the jar, hex-encoded.
    fn sha256(self) -> &'static str {
        match self {
            Self::ClientsIntegrationTests => "0e4d94490e1665bf9af9dc745ddd85de8cad58f81890d915f2176c7fd3fdc983",
            Self::StorageTestFixtures => "ab62d0acf3ca64ba5cce7941c21c338d0399954b2c66c25efbf24a57241ff991",
            Self::ClientsTestFixtures => "39e2a3e997eb9fd8131f31e8d62535b7a2e89b826cf16a1fc25524ea0a160eb1",
        }
    }

    /// The jar's file name, e.g. `kafka-storage-4.4.0-test-fixtures.jar`.
    pub fn file_name(self) -> String {
        let (artifact, classifier) = self.artifact();
        format!("{artifact}-{KAFKA_TEST_JARS_VERSION}-{classifier}.jar")
    }

    /// The jar's Maven Central URL.
    fn url(self) -> String {
        let (artifact, _) = self.artifact();
        format!(
            "{MAVEN_CENTRAL}/org/apache/kafka/{artifact}/{KAFKA_TEST_JARS_VERSION}/{}",
            self.file_name()
        )
    }

    /// The jar in the local cache, downloaded first when it is not there yet.
    ///
    /// # Panics
    ///
    /// When the download fails, or the downloaded file does not have the pinned
    /// SHA-256 (the file is then deleted), so a test never runs against an
    /// unverified jar. A cached file is checked again on every call.
    pub fn local_path(self) -> PathBuf {
        let _guard = DOWNLOAD_LOCK.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let dir = cache_dir();
        std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("cannot create {}: {e}", dir.display()));
        let path = dir.join(self.file_name());
        if path.exists() && self.verify(&path).is_ok() {
            return path;
        }
        // A name of this process's own: two test processes downloading the
        // same jar at once never write one file; each renames its verified
        // copy over the target atomically, and every copy holds the same bytes.
        let partial = dir.join(format!("{}.partial.{}", self.file_name(), std::process::id()));
        let url = self.url();
        // Bounded, so a stalled network fails the cluster start instead of
        // holding `DOWNLOAD_LOCK` (and the start) for ever: 10 s to connect and
        // 120 s per attempt, for jars of at most a few hundred KB.
        let output = std::process::Command::new("curl")
            .args([
                "-fsSL",
                "--connect-timeout",
                "10",
                "--max-time",
                "120",
                "--retry",
                "5",
                "--retry-delay",
                "2",
                "--retry-all-errors",
                "-o",
            ])
            .arg(&partial)
            .arg(&url)
            .output()
            .unwrap_or_else(|e| panic!("cannot run curl to download {url}: {e}"));
        assert!(
            output.status.success(),
            "downloading {url} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
        if let Err(e) = self.verify(&partial) {
            let _ = std::fs::remove_file(&partial);
            panic!("{url}: {e}");
        }
        std::fs::rename(&partial, &path).unwrap_or_else(|e| panic!("cannot move {} into place: {e}", path.display()));
        path
    }

    /// Checks the file at `path` against the pinned SHA-256.
    fn verify(self, path: &Path) -> Result<(), String> {
        let bytes = std::fs::read(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
        let actual = hex(digest(&SHA256, &bytes).as_ref());
        if actual == self.sha256() {
            Ok(())
        } else {
            Err(format!("SHA-256 {actual} does not match the pinned {}", self.sha256()))
        }
    }
}

/// `<manifest dir>/target/test-jars`, inside the gitignored build directory.
fn cache_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("target").join("test-jars")
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

#[cfg(test)]
mod tests {
    use super::TestJar;

    #[test]
    fn file_names_and_urls_follow_the_maven_layout() {
        assert_eq!(
            "kafka-storage-4.4.0-test-fixtures.jar",
            TestJar::StorageTestFixtures.file_name()
        );
        assert_eq!(
            "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients-integration-tests/4.4.0/\
             kafka-clients-integration-tests-4.4.0-test.jar",
            TestJar::ClientsIntegrationTests.url()
        );
        assert_eq!(
            "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.4.0/kafka-clients-4.4.0-test-fixtures.jar",
            TestJar::ClientsTestFixtures.url()
        );
    }

    #[test]
    fn a_file_without_the_pinned_digest_is_rejected() {
        let dir = std::env::temp_dir().join(format!("test-jars-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("not-the-jar.jar");
        std::fs::write(&path, b"not the jar").expect("write");
        let error = TestJar::StorageTestFixtures.verify(&path).expect_err("a wrong digest");
        assert!(error.contains("does not match the pinned ab62d0ac"), "{error}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
