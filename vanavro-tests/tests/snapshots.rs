//! SPEC §14.1 — snapshot tests of the full generated source for the corpus.
//!
//! A snapshot diff fails the test; the reviewer decides whether to
//! accept the new output (`cargo insta review`) or fix the codegen.

use std::path::Path;

use rstest::rstest;
use vanavro::{AvroType, CustomLogicalType, VanAvro};

fn fixture(stem: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(format!("{stem}.avsc"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

#[rstest]
#[case("primitives")]
#[case("collections")]
#[case("unions")]
#[case("nested")]
#[case("recursive")]
#[case("logical")]
#[case("defaults")]
#[case("aliases")]
#[case("scalar")]
#[case("tree")]
#[case("collapsed_refs")]
#[case("deep_ns")]
#[case("diamond")]
#[case("named_unions")]
fn generated_source_snapshot(#[case] stem: &str) {
    let generated = VanAvro::builder()
        .schema_json(fixture(stem))
        .generate()
        .unwrap_or_else(|e| panic!("codegen {stem}: {e}"));
    insta::assert_snapshot!(stem, generated);
}

/// SPEC §5.6 — snapshot of the `uncollapsed_refs` fixture generated with the
/// single-union-record collapse *disabled*, mirroring `build.rs`. Locks down the
/// non-collapsed struct + `{Record}Union` output shape.
#[test]
fn uncollapsed_refs_source_snapshot() {
    let generated = VanAvro::builder()
        .schema_json(fixture("uncollapsed_refs"))
        .collapse_single_union(false)
        .generate()
        .unwrap_or_else(|e| panic!("codegen uncollapsed_refs: {e}"));
    insta::assert_snapshot!("uncollapsed_refs", generated);
}

/// SPEC §15 — snapshot of the custom-logical-type output. The registrations
/// mirror `build.rs` exactly so the snapshot tracks what the integration crate
/// actually compiles and round-trips.
#[test]
fn custom_logical_source_snapshot() {
    let generated = VanAvro::builder()
        .schema_json(fixture("custom_logical"))
        .with_logical_type(
            CustomLogicalType::builder("url", AvroType::String, "::url::Url").build(),
        )
        .with_logical_type(
            CustomLogicalType::builder("email", AvroType::String, "crate::Email")
                .converter("crate::EmailAsString")
                .build(),
        )
        .with_logical_type(
            CustomLogicalType::builder("money", AvroType::Record, "crate::Money").build(),
        )
        .with_logical_type(
            CustomLogicalType::builder("cents", AvroType::Record, "crate::Cents")
                .converter("crate::CentsAsRecord")
                .build(),
        )
        .generate()
        .unwrap_or_else(|e| panic!("codegen custom_logical: {e}"));
    insta::assert_snapshot!("custom_logical", generated);
}
