// Generate Rust types from the checked-in `.avsc` corpus into `OUT_DIR`.
//
// Each fixture is emitted to `OUT_DIR/<stem>.rs` and pulled into the crate by
// `src/lib.rs` via `include!`, mirroring the canonical `build.rs` integration
// pattern from SPEC §4.2 (generate into `OUT_DIR`, `include!` from source).
// Generation uses the split layout (SPEC §8.3), so each namespace
// module lands in `OUT_DIR/<stem>/…` and compiling this crate proves the
// `#[path]` wiring survives `include!`. The generated source carries no module
// prelude (no banner, no inner `#![allow(...)]`), so it can be `include!`d
// mid-file at the crate root; the crate-level lint allowances live in
// `src/lib.rs` instead.

use std::path::PathBuf;

use vanavro::{AvroType, CustomLogicalType, VanAvro};

fn main() {
    let manifest = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));
    let fixtures = manifest.join("fixtures");

    for stem in CORPUS {
        let src = fixtures.join(format!("{stem}.avsc"));
        println!("cargo::rerun-if-changed={}", src.display());
        let json =
            std::fs::read_to_string(&src).unwrap_or_else(|e| panic!("read {}: {e}", src.display()));
        VanAvro::builder()
            .schema_json(json)
            .generate_to_path(out_dir.join(format!("{stem}.rs")), true)
            .unwrap_or_else(|e| panic!("codegen {stem}: {e}"));
    }

    // The custom-logical fixture (SPEC §15) needs registered mappings, so it is
    // generated explicitly rather than through the plain corpus loop. The same
    // registrations are mirrored in `tests/snapshots.rs::custom_logical_*` so
    // the snapshot matches what is compiled here.
    let custom_src = fixtures.join("custom_logical.avsc");
    println!("cargo::rerun-if-changed={}", custom_src.display());
    let custom_json = std::fs::read_to_string(&custom_src)
        .unwrap_or_else(|e| panic!("read {}: {e}", custom_src.display()));
    VanAvro::builder()
        .schema_json(custom_json)
        // `url` → `::url::Url`, converterless (round-trips as a plain string).
        .with_logical_type(
            CustomLogicalType::builder("url", AvroType::String, "::url::Url").build(),
        )
        // `email` → `crate::Email`, routed through a `serde_with` converter.
        .with_logical_type(
            CustomLogicalType::builder("email", AvroType::String, "crate::Email")
                .converter("crate::EmailAsString")
                .build(),
        )
        // `money` → `crate::Money`, a *record-level* suppression (SPEC §15.3
        // case 2 / §12.1): the `Money` struct is not emitted, but `CustomLogical`
        // references it, so a `build_*` schema-builder for it *is* emitted into
        // the `custom_logical` module with no accompanying Rust type. Compiling
        // this fixture is the only place that path is exercised end to end.
        .with_logical_type(
            CustomLogicalType::builder("money", AvroType::Record, "crate::Money").build(),
        )
        // `cents` → `crate::Cents`, a record-level suppression *with* a converter
        // (SPEC §15.3 case 2 + §15.4). `crate::Cents` cannot round-trip on its
        // own — only `CentsAsRecord` gives it the record's wire shape — so a
        // reference that loses the converter fails to compile or to decode
        // instead of silently emitting the wrong bytes. It appears only in
        // multi-variant union arms (case 6), the position where the converter
        // used to be dropped.
        .with_logical_type(
            CustomLogicalType::builder("cents", AvroType::Record, "crate::Cents")
                .converter("crate::CentsAsRecord")
                .build(),
        )
        .generate_to_path(out_dir.join("custom_logical.rs"), true)
        .unwrap_or_else(|e| panic!("codegen custom_logical: {e}"));

    // The `collapsed_refs` fixture exercises references *to* a collapsed
    // single-union record (SPEC §5.6): `Outer` holds a `Scalar` field, and
    // `Tree` is a recursive collapsed record. Generated outside CORPUS and
    // has no Java reference fixture; a Rust-only roundtrip test in `tests/roundtrip.rs`
    // verifies the referenced and recursive shapes encode/decode.
    let collapsed_refs_src = fixtures.join("collapsed_refs.avsc");
    println!("cargo::rerun-if-changed={}", collapsed_refs_src.display());
    let collapsed_refs_json = std::fs::read_to_string(&collapsed_refs_src)
        .unwrap_or_else(|e| panic!("read {}: {e}", collapsed_refs_src.display()));
    VanAvro::builder()
        .schema_json(collapsed_refs_json)
        .generate_to_path(out_dir.join("collapsed_refs.rs"), true)
        .unwrap_or_else(|e| panic!("codegen collapsed_refs: {e}"));

    // The `uncollapsed_refs` fixture is the `collapsed_refs` shapes generated
    // with the SPEC §5.6 collapse *disabled* (`collapse_single_union(false)`).
    // It compiles the non-collapsed struct + `{Record}Union` output and, via
    // `tests/roundtrip.rs`, proves the wire shape is unchanged from the
    // collapsed form.
    let uncollapsed_refs_src = fixtures.join("uncollapsed_refs.avsc");
    println!("cargo::rerun-if-changed={}", uncollapsed_refs_src.display());
    let uncollapsed_refs_json = std::fs::read_to_string(&uncollapsed_refs_src)
        .unwrap_or_else(|e| panic!("read {}: {e}", uncollapsed_refs_src.display()));
    VanAvro::builder()
        .schema_json(uncollapsed_refs_json)
        .collapse_single_union(false)
        .generate_to_path(out_dir.join("uncollapsed_refs.rs"), true)
        .unwrap_or_else(|e| panic!("codegen uncollapsed_refs: {e}"));

    // The aliases fixture exercises record-, enum-, and fixed-*type* aliases
    // (plus field aliases). Type aliases have no serde representation but DO
    // survive in the structural `schema()` ctor, so compiling this fixture
    // guards the `Alias` emission against upstream API drift (e.g. the 0.22
    // removal of `Alias: From<&str>`). Rust-only; no Java-interop reference.
    let aliases_src = fixtures.join("aliases.avsc");
    println!("cargo::rerun-if-changed={}", aliases_src.display());
    let aliases_json = std::fs::read_to_string(&aliases_src)
        .unwrap_or_else(|e| panic!("read {}: {e}", aliases_src.display()));
    VanAvro::builder()
        .schema_json(aliases_json)
        .generate_to_path(out_dir.join("aliases.rs"), true)
        .unwrap_or_else(|e| panic!("codegen aliases: {e}"));

    println!("cargo::rerun-if-changed=build.rs");
}

/// Fixture stems under `fixtures/` generated with default options; kept in sync
/// with the `src/lib.rs` `include!`s, and each has a `tests/snapshots.rs` case. Java
/// reference coverage is the separate `stems` list in `fixtures/regenerate.sh`
/// (not `deep_ns` / `diamond`), and `defaults` has no fingerprint test.
const CORPUS: &[&str] = &[
    "primitives",
    "collections",
    "unions",
    "nested",
    "recursive",
    "logical",
    "defaults",
    // SPEC §5.6 single-union-record collapse. Both also have Java reference
    // fixtures, so the collapsed wire shape is validated against the JVM
    // reference (`tests/java_interop.rs`):
    // `scalar` is a flat collapsed record; `tree` is a recursive one whose
    // nested arms route through the `Serialize` impl.
    "scalar",
    "tree",
    // Multi-segment namespace (`a.b.c`) referencing a shallower one (`a`), so
    // the generated builder calls exercise a depth>=2 `super::` chain. Guards
    // the relative-module path arithmetic; a fingerprint assertion lives in
    // `tests/fingerprint.rs`.
    "deep_ns",
    // Diamond: `Root -> {L, R} -> Shared`, so `Shared` is referenced from two
    // distinct fields. The runtime `defined` set must expand `Shared` in full
    // on its first occurrence and emit a bare back-reference on the second —
    // the axis where insertion order matters. Fingerprint assertion in
    // `tests/fingerprint.rs`.
    "diamond",
    "named_unions",
];
