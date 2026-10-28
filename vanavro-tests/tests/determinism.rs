//! SPEC §14.2 — codegen determinism.
//!
//! Generating from the same input twice must produce byte-identical output;
//! catches accidental `HashMap`-iteration order leaking into the emitted source.

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
#[case("scalar")]
#[case("tree")]
#[case("collapsed_refs")]
#[case("deep_ns")]
#[case("diamond")]
#[case("named_unions")]
fn codegen_is_byte_identical_across_runs(#[case] stem: &str) {
    let json = fixture(stem);
    let first = VanAvro::builder()
        .schema_json(json.clone())
        .generate()
        .expect("first generate");
    let second = VanAvro::builder()
        .schema_json(json)
        .generate()
        .expect("second generate");
    assert_eq!(first, second, "codegen for `{stem}` diverged between runs");
}

/// SPEC §14.2 + §15 — custom-logical codegen is also byte-identical across
/// runs. Guards the `BTreeMap`-backed logical-type registry against any
/// nondeterministic iteration leaking into the emitted source.
#[test]
fn custom_logical_codegen_is_byte_identical_across_runs() {
    let json = fixture("custom_logical");
    let gen_once = || {
        VanAvro::builder()
            .schema_json(json.clone())
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
            .expect("generate")
    };
    assert_eq!(gen_once(), gen_once(), "custom-logical codegen diverged");
}
