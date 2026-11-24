# The platform packages

One folder per platform package of `@confluentinc/kafka-javascript`, each
holding the `package.json` it is published with and its README. The preview
has two:

| Folder | Package | `os` / `cpu` / `libc` |
|---|---|---|
| `linux-x64-gnu/` | `@confluentinc/kafka-javascript-linux-x64-gnu` | `linux` / `x64` / `glibc` |
| `darwin-arm64/` | `@confluentinc/kafka-javascript-darwin-arm64` | `darwin` / `arm64` |

The main package pins each at exactly its own version in its
`optionalDependencies` (`../package.json`), so npm installs the one of the
user's machine; there is no install script and no download fallback.
`cargo xtask bump-version --javascript-version <v>` writes the version of
every folder's `package.json` and those pins together.

A package's `files` name `dist/` and `NOTICE`, as every JavaScript package's
do (a platform package has no `dist/`, so that entry matches nothing), the
addon and `THIRD_PARTY_LICENSES.txt`; npm adds the README and the LICENSE
itself. None of those three is committed
here: `.semaphore/pack-npm.sh` stages each folder with the addon of its
platform, the repository's `LICENSE`, `../NOTICE` and the third-party licence
file, which `.semaphore/third-party-licenses.sh` builds with cargo-about from
`about.toml` and `about.hbs` (the licences of the Rust crates linked into the
addon), and `.semaphore/check-npm.py package` checks each tarball's exact file
list.
