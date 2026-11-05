# Node.js client — the npm release pipeline

Status: **approved 2026-11-05** (revision 3 of the design: publishing to npm and the post-publish install test are manual steps). It mirrors the .NET release flow (`.semaphore/build-dotnet-native.sh`, `pack-nupkg.sh`, `test-nupkg.sh`, `release-nupkg.sh`): build per platform, pack, test on clean machines, record, then publish by hand.

## 1. Summary

A `javascript/v<version>` tag starts a tag build that

1. builds the native addon on each platform, with the Rust core statically linked;
2. packs the npm packages: one per platform, plus the main package;
3. installs those exact tarballs on a clean machine per platform and runs a smoke test;
4. records them as a release bundle: a zip holding the four tested tarballs, their checksums and `publish.sh`.

A person downloads the zip and runs `publish.sh`, entering an npm one-time password before each package. Afterwards the `javascript-post-install` Semaphore task, started by hand, installs the published version from npmjs.com across a matrix of distributions and Node.js versions. No machine in the pipeline holds an npm token.

## 2. Decisions

| # | Decision |
|---|---|
| R1 | Tag `javascript/v<semver>`, matching `^javascript/v(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)(-(alpha\|beta\|dev\|rc)\.(0\|[1-9][0-9]*))?$` everywhere it appears. The version is `"version"` in `javascript/package.json` (mirrored in `package-lock.json`); the major is at least 4, above the 1.x client that shares the npm name. |
| R2 | Platforms: `linux-x64-gnu`, `linux-arm64-gnu`, `darwin-arm64`. musl and Windows come later. |
| R3 | Publishing is manual, from the bundle, with the publisher's one-time password. Nothing in CI publishes. |
| R4 | Pre-releases go under the `preview` dist-tag; `latest`, `rc` and `alpha` stay with 1.x. `publish.sh` refuses a stable version until moving `latest` is agreed. |
| R5 | Names: `@confluentinc/kafka-javascript` and `@confluentinc/kafka-javascript-<platform>`. Tarballs are what `npm pack` names: `confluentinc-kafka-javascript[-<platform>]-<version>.tgz`. |
| R6 | Release addons link the core statically (`CONFLUENT_KAFKA_LINK=static` in `javascript/binding.gyp`); development builds, the tests and the gRPC image stay dynamic. |
| R7 | The repository's `package.json` stays `"private": true` with no dependencies; the pack step writes the published one (`check-npm.py manifest`): `private`, `devDependencies` and `scripts` removed, `optionalDependencies` naming the three platform packages at exactly the version. |
| R8 | Before publishing, the tarballs are installed from their files on each platform, plus an exact metadata check; after publishing, the manual post-install task installs from npmjs.com over the distribution × Node.js matrix. |

## 3. The tag build (`.semaphore/semaphore.yml`)

- **Release (JavaScript): native addon (Linux amd64 | Linux arm64 | macOS arm64)**, alongside CI: `build-javascript-native.sh` runs `build-ffi-library.sh` with `STAGE_STATIC_LIB=1` (which also stages `libconfluent_kafka.a`, an opt-in that leaves Python and .NET unchanged), then builds the addon against it: on Linux in the same `manylinux_2_28` image, with a pinned Node.js 22 tarball whose SHA-256 is checked and npm's bundled node-gyp, so the addon needs glibc 2.28 at most; on macOS on the runner with `MACOSX_DEPLOYMENT_TARGET=11.0`. `check-npm.py native` then requires the right ELF or Mach-O, only glibc / libgcc_s or system libraries loaded, no rpath, the glibc 2.28 or macOS 11.0 floor, at least 1 MiB, and exactly `napi_register_module_v1` and `node_api_module_get_api_version_v1` exported. The addon goes to `npm/native/<platform>`.
- **Release (JavaScript): pack**: the tag's version must be `check-npm.py version`'s; `pack-npm.sh` packs the four tarballs from the committed `javascript/` (it refuses uncommitted changes) and `check-npm.py package` checks each one's exact file list, its `package.json`, the absence of install scripts and build inputs, the platform packages' `os`, `cpu` and `libc`, and each addon. The tarballs, and nothing else, go to `npm/built`.
- **Release (JavaScript): package verification (<platform>)**, on a VM that built nothing: `test-npm.sh` hashes the tarballs, installs the main package and this platform's package from their files into a project outside the repository, with an empty cache, no npm configuration and an unreachable registry (npm skips the two other optional platform packages), checks the installed tree is exactly those two tarballs, runs `javascript/tools/package-smoke` (the platform package's addon is the one loaded, every `exports` entry point loads, a producer opens and closes, the versions match), and only then writes the checksums to `npm/tested/<platform>`.
- **Release (JavaScript): record**, after "Release: CI passed" and the three verifications: `release-npm.sh` refuses unless the tag still points at HEAD and HEAD is on master, the version is the manifest's and on npmjs.com for none of the four packages, every platform tested exactly the four packed tarballs, and `check-npm.py` accepts them. It writes `confluent-kafka-javascript-<version>-release.zip` (the tarballs, `SHA256SUMS`, `javascript/release/publish.sh` and `README.txt`), claims `npm/release/SHA256SUMS` without `--force`, so a repacked release can never replace a recorded one, pushes the zip, and prints every checksum.

The release blocks run only on tags, so they are not in `REQUIRED_CI_JOBS`. On every build the macOS `verify-javascript` job also runs `make package-check-javascript`: this machine's static addon packed, checked and installed as above.

## 4. Publishing and after

- `publish.sh`, on the publisher's machine: checks `SHA256SUMS`, `npm whoami` and that the version is a pre-release; skips a package already published with the same tarball (so a rerun completes an interrupted one) and stops on one published with another; dry-runs; then publishes the three platform packages and the main package, in that order, with `--access public --tag preview`, asking for a one-time password before each. It publishes the tarball files, so npm uploads exactly the tested bytes.
- `javascript-post-install` (service.yml task on master, parameter `JAVASCRIPT_VERSION`): `check-published-npm.py check` verifies the four packages, the `preview` dist-tag, the optional dependencies and each tarball against the registry's integrity and `check-npm.py package --published`; install jobs run `npm install @confluentinc/kafka-javascript@<version> --ignore-scripts` and the smoke program on Linux amd64 and arm64 in almalinux 8 and 9, rockylinux 9, debian bookworm and trixie, ubuntu 20.04, 22.04 and 24.04, with Node.js 18, 20, 22 and 24 (official tarballs checked against nodejs.org's `SHASUMS256.txt`), and on macOS arm64 with the same Node.js versions. It runs from master, so it cannot compare with the recorded bundle; `publish.sh` does, by reading each package back from npmjs.com after publishing and checking the registry serves exactly the bundle's tarball.

## 5. Versioning and tagging

- `cargo xtask bump-version ... --javascript-version <semver>` (`make bump-version JAVASCRIPT_VERSION=...`, and the bump-version task's optional parameter) writes the version into `package.json` and both copies in `package-lock.json`, in the semver form, 4.0.0 or higher, never lower.
- The "Create release tag" promotion takes `JAVASCRIPT_VERSION`: the version must match the pattern, equal `check-npm.py version`, be entered on master, have no tag yet and be on npmjs.com for none of the packages. GitHub sends no push event for a push of more than three tags, so the tags are pushed in groups of at most three, each atomic; if a later group fails, the earlier groups are deleted from origin again, which keeps the release all or nothing.
- `rfc/versioning.md` describes the JavaScript version line.

## 6. Out of scope

- Publishing from CI (it would need an npm token the package settings allow).
- Moving `latest` from 1.x, or publishing a stable version.
- musl and Windows packages.
- npm provenance, which npm supports only from GitHub Actions and GitLab CI.
