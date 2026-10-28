# Contributing to VanAvro

Thanks for your interest in improving `vanavro`. This document explains how to
propose changes and what a change needs before it can be merged.

`vanavro` generates code that is compiled into downstream crates and
round-tripped against the Apache Avro Java implementation. A subtle bug in the
generator ships silently into every consumer, so correctness carries more
weight here than anything else.

## Reporting issues

- **Bugs:** open a GitHub issue that includes the smallest `.avsc` / `.avdl`
  input that reproduces the problem, the generated output (or error), and what
  you expected instead. Note the `vanavro` version and the builder options or
  CLI flags you used.
- **Security vulnerabilities:** do **not** open a public issue. Report them
  privately through GitHub's
  [private vulnerability reporting](https://github.com/helsing-ai/vanavro/security/advisories/new).

## Making a change

1. For anything beyond a small fix, open an issue first so the approach can be
   agreed before you invest time in it.
2. Fork the repository and create a topic branch from `main`.
3. Make the change, following the requirements below.
4. Open a pull request against `main` and describe _what_ changed and _why_.
   Reference the issue and the SPEC sections involved.

Keep pull requests focused. A bug fix should not carry an unrelated refactor.

## `SPEC.md` is the source of truth

[`SPEC.md`](./SPEC.md) is the authoritative specification of the generator's
behaviour.

- Read the relevant section before changing code. Sections are numbered
  (e.g. §5.3, §15.9), and code comments cite them as `SPEC §X.Y`. Keep those
  citations accurate.
- If your change alters behaviour, update the governing section of `SPEC.md`
  **in the same pull request**, including tables and cross-references. A
  divergence between code and SPEC is treated as a bug.
- If the SPEC is wrong, ambiguous, or silent on something, pick the defensible
  behaviour, implement it, and record the decision and its rationale in the
  SPEC. Call it out in the pull request description.

## Requirements for a pull request

### Tests

Run the suite with [`cargo-nextest`](https://nexte.st/):

```bash
cargo nextest run
```

The suite includes Rust round-trips, determinism checks, fingerprint checks,
source snapshots, and Java-interop tests. The Java-interop tests in
`vanavro-tests/tests/java_interop.rs` need `avro-tools` (and a JRE) on `PATH`
to run in full. Without it, the live `avro-tools cat` step is skipped, so run
the full suite before submitting changes that affect encoding. See the
[README](./README.md#avro-tools-for-the-full-test-suite) for details.

New behaviour needs a test that fails without the change. That includes error
paths: assert on the `GeneratorError` variant and check that its message tells
the user what to do.

### Lints and formatting

```bash
cargo fmt --all
cargo clippy --all-targets --all-features
```

The build and clippy must be free of warnings.

### Snapshots

Generated source is pinned with [`insta`](https://insta.rs/) snapshots
(`vanavro-tests/tests/snapshots/`). If your change alters generated output:

```bash
cargo insta review
```

Read every snapshot diff before accepting it, and only commit snapshots whose
changes you can explain. Never commit pending `*.snap.new` files.

### Test fixtures

- **Schema corpus** (`vanavro-tests/fixtures/*.avsc`): when you add a fixture,
  wire it into `vanavro-tests/build.rs`, `vanavro-tests/src/lib.rs`, and the
  relevant tests (e.g. `snapshots.rs`, `roundtrip.rs`).
- **Java reference fixtures** (`vanavro-tests/fixtures/reference/`): these are
  produced by `vanavro-tests/fixtures/regenerate.sh` with `avro-tools`. To add
  one, add a `<stem>.input.json` datum next to the schema, append the stem to
  the script, rerun it, and commit the reviewed output. Never edit these files
  by hand.
- **IDL fixtures** (`vanavro/tests/fixtures/idl/`): most of these files are
  copied from the Apache Avro project under the Apache License 2.0 (see the
  `NOTICE` file in that directory). Keep upstream license headers intact. If
  you add or modify an upstream file, say so in `NOTICE` and at the top of the
  file.

### Changelog

Add an entry for user-visible changes under an `Unreleased` heading in
[`CHANGELOG.md`](./CHANGELOG.md), following
[Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

### Commits

Write commit messages in the imperative mood, with a short summary line and,
where useful, a body that explains the reasoning behind the change.

## Minimum supported Rust version

The MSRV is declared as `rust-version` in the workspace `Cargo.toml`. It is
currently set by the highest `rust-version` among the dependencies. Do not
use language or standard-library features newer than the MSRV. If a change
requires raising it, update `rust-version` and note the bump in the changelog.

## License

Unless you explicitly state otherwise, any contribution you intentionally
submit for inclusion in this project, as defined in the Apache-2.0 license,
is dual-licensed as described in the [README](./README.md#license), without
any additional terms or conditions.
