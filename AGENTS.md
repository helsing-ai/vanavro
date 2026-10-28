# AGENTS.md — Working agreement for agents in this repo

`vanavro` is an Apache Avro-to-Rust code generator. Treat it as
correctness-critical: generated code is compiled and round-tripped against the
JVM reference implementation.

## 1. SPEC.md is the source of truth

[`SPEC.md`](./SPEC.md) is the authoritative specification. Every change must
conform to it.

- **Read the relevant section before you touch code.** Sections are numbered
  (e.g. §4.3, §10.2). Code comments cite them as `SPEC §X.Y` — keep that
  convention when you add comments, and keep the citations accurate. Cite the
  SPEC only in plain `//` comments, never in `///` / `//!` doc comments or in
  `#[doc]` text emitted into generated code.
- **Conform to the SPEC.** If a requirement exists, implement it as written.
  Do not silently reinterpret it.
- **If the implementation must diverge from the SPEC, update the SPEC in the
  same change.** A divergence that is left undocumented is a bug. When you
  change behaviour, find the governing section and rewrite it so SPEC and code
  agree — including the numbered subsection, any tables, and cross-references.
  Note deliberate, justified deviations explicitly rather than leaving the
  reader to infer them.
- **If the SPEC is wrong, ambiguous, or silent** on something you need, resolve
  it explicitly: pick the defensible behaviour, implement it, and amend the
  SPEC to record the decision and its rationale. Flag it to the user.
- Never delete a SPEC requirement to make a test pass. Fix the code, or change
  the requirement deliberately with justification.

## 2. Review everything brutally

Hold every change — yours and the user's — to a brutal standard. Before
declaring anything done:

- **Hunt for correctness bugs first.** Wrong schema output, broken
  namespace/reference resolution, mis-mapped logical types, non-deterministic
  ordering, and round-trip failures are the bugs that matter here. Assume the
  happy path works and go looking for the cases that don't: recursion, forward
  refs, inline anonymous types (§10.3), unions, defaults, escaped keywords.
- **Be adversarial about your own diff.** Ask what input breaks it, what the
  SPEC says it should do, and whether the tests actually prove it. A passing
  test suite is necessary, not sufficient.
- **Reject dead code and misleading comments.** A feature flag that gates
  nothing, a comment that contradicts the code, an error path that produces an
  unactionable message — call it out and fix it, don't preserve it.
- **Prefer the smallest correct change** that the SPEC permits. Don't refactor
  opportunistically in a change whose purpose is a fix.
- For a deep, multi-perspective pass, check if the `brutal-review` skill is
  available — use it when the user asks for a "brutal review" or when a change
  is large enough to warrant it.

## 3. Tests and quality gates

- Run tests with `cargo nextest run` (not `cargo test`). The full suite —
  including Java-interop round-trips and source snapshots — must pass.
- Fix every warning surfaced by the build and by `cargo clippy --all-targets`.
- When you change generated output, update the snapshot fixtures deliberately
  and inspect the diff; never bless a snapshot you haven't read.
- New behaviour needs a test that would fail without the change. Error paths
  included — assert on the error variant and that its message is actionable.

## 4. Conventions

- Match the surrounding code's style, naming, comment density, and idiom.
- Do not add "Generated with Claude Code" / "Co-Authored-By" or similar trailers
  to commits.
- Do not add per-file copyright headers to source files.
