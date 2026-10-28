// SPEC §14.3 — fingerprint correctness.
//
// Two layers of assurance:
//
// 1. **Internal consistency:** the structurally built `T::schema()` must yield
//    the same Rabin fingerprint as the schema re-parsed from JSON via
//    `Schema::parse_str`. This proves the hand-rolled structural constructor
//    (SPEC §4.1 Option A) reproduces the exact canonical form of the parsed
//    schema.
// 2. **Cross-language:** the Rust fingerprint must match the reference Java
//    implementation via `avro-tools fingerprint --fingerprint CRC-64-AVRO`
//    (Avro's "Rabin" 64-bit fingerprint is CRC-64-AVRO). Skipped when
//    `avro-tools` is not on `PATH` so the default test path stays hermetic.
//
// ## Known upstream caveat (logical types)
//
// `apache-avro` 0.22's `Schema::canonical_form()` emits a *nested* type object
// for logical-type fields (`"type":{"type":"int"}`) instead of the bare
// primitive (`"type":"int"`) mandated by the Avro Parsing Canonical Form and
// produced by Java. As a result the Rust Rabin fingerprint of any schema that
// uses a logical type does **not** match `avro-tools`. This affects
// `parse_str` and our structural `schema()` identically, so it is purely an
// upstream `canonical_form()` bug, not a vanavro defect. The `logical` fixture
// is therefore asserted as a *known mismatch*; the test will fail loudly if a
// future `apache-avro` release fixes the canonical form, prompting us to flip
// it to an equality assertion.

use std::path::{Path, PathBuf};
use std::process::Command;

use apache_avro::Schema;
use apache_avro::rabin::Rabin;
use apache_avro::schema::SchemaFingerprint;
use rstest::rstest;

use vanavro_tests::a::b::c::Deep;
use vanavro_tests::collections::Collections;
use vanavro_tests::diamond::Root as DiamondRoot;
use vanavro_tests::logical::Logical;
use vanavro_tests::named_unions::Catalogue;
use vanavro_tests::nested::Nested;
use vanavro_tests::primitives::Primitives;
use vanavro_tests::recursive::TreeNode;
use vanavro_tests::scalar::Scalar;
use vanavro_tests::tree::Tree;
use vanavro_tests::unions::Unions;

fn fixture_path(stem: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(format!("{stem}.avsc"))
}

fn hex(fp: &SchemaFingerprint) -> String {
    fp.bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// `T::fingerprint::<Rabin>()` must equal the fingerprint of the schema parsed
/// straight from the fixture JSON.
fn assert_matches_parsed(stem: &str, rust_fp: SchemaFingerprint) {
    let json = std::fs::read_to_string(fixture_path(stem)).expect("read fixture");
    let parsed = Schema::parse_str(&json).expect("parse fixture");
    let parsed_fp = parsed.fingerprint::<Rabin>();
    assert_eq!(
        hex(&rust_fp),
        hex(&parsed_fp),
        "structural schema() fingerprint for `{stem}` diverges from parse_str"
    );
}

#[test]
fn structural_schema_matches_parsed_primitives() {
    assert_matches_parsed("primitives", Primitives::fingerprint::<Rabin>());
}

#[test]
fn structural_schema_matches_parsed_collections() {
    assert_matches_parsed("collections", Collections::fingerprint::<Rabin>());
}

#[test]
fn structural_schema_matches_parsed_unions() {
    assert_matches_parsed("unions", Unions::fingerprint::<Rabin>());
}

#[test]
fn structural_schema_matches_parsed_nested() {
    assert_matches_parsed("nested", Nested::fingerprint::<Rabin>());
}

#[test]
fn structural_schema_matches_parsed_recursive() {
    assert_matches_parsed("recursive", TreeNode::fingerprint::<Rabin>());
}

#[test]
fn structural_schema_matches_parsed_logical() {
    // Internal consistency still holds for logical types — the upstream
    // canonical-form quirk applies equally to parse_str and schema().
    assert_matches_parsed("logical", Logical::fingerprint::<Rabin>());
}

#[test]
fn structural_schema_matches_parsed_scalar() {
    assert_matches_parsed("scalar", Scalar::fingerprint::<Rabin>());
}

#[test]
fn structural_schema_matches_parsed_tree() {
    assert_matches_parsed("tree", Tree::fingerprint::<Rabin>());
}

/// Multi-segment namespace. `a.b.c.Deep` references `a.Shared`, so the compiled
/// `schema()` builder call must climb a depth-2 `super::` chain; if the
/// relative-module arithmetic were off the crate would not compile, and this
/// fingerprint would diverge from the parsed schema.
#[test]
fn structural_schema_matches_parsed_deep_ns() {
    assert_matches_parsed("deep_ns", Deep::fingerprint::<Rabin>());
}

/// Diamond: `Root -> {L, R} -> Shared`. `Shared` is referenced from two
/// distinct fields, so the runtime `defined` set decides which occurrence is
/// the definition (full expansion) and which is the back-reference. A parse of
/// the same schema resolves the second textual occurrence to a `Schema::Ref`;
/// this fingerprint matching `parse_str` proves the builder reproduces that
/// first-occurrence/back-reference split exactly.
#[test]
fn structural_schema_matches_parsed_diamond() {
    assert_matches_parsed("diamond", DiamondRoot::fingerprint::<Rabin>());
}

#[test]
fn structural_schema_matches_parsed_named_unions() {
    assert_matches_parsed("named_unions", Catalogue::fingerprint::<Rabin>());
}

// The `"enum"` union-naming hint (SPEC §5.3) is vanavro-internal: it must be
// stripped from every record field's `custom_attributes` before the field
// reaches the emitted `schema()`, or it would perturb the canonical form.
#[test]
fn named_unions_schema_has_no_enum_attributes() {
    let Schema::Record(rec) = Catalogue::schema() else {
        panic!("named_unions fixture must produce a record schema");
    };
    // Guard against the check passing because there is nothing to check: the
    // fixture carries an `enum` hint on most of its fields.
    assert!(!rec.fields.is_empty(), "record has no fields to inspect");
    for field in &rec.fields {
        assert!(
            !field.custom_attributes.contains_key("enum"),
            "field `{}` leaked the internal `enum` union-naming hint into schema(): {:?}",
            field.name,
            field.custom_attributes,
        );
    }
}

// ---- cross-language (avro-tools) -------------------------------------------

/// Run `avro-tools fingerprint --fingerprint CRC-64-AVRO <file>` and return the
/// leading hex token, or `None` if the tool is unavailable.
fn avro_tools_crc64(stem: &str) -> Option<String> {
    let output = Command::new("avro-tools")
        .args(["fingerprint", "--fingerprint", "CRC-64-AVRO"])
        .arg(fixture_path(stem))
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8(output.stdout).ok()?;
    stdout
        .split_whitespace()
        .next()
        .map(str::to_string)
        .filter(|s| s.len() == 16 && s.bytes().all(|b| b.is_ascii_hexdigit()))
}

#[rstest]
#[case("primitives", Primitives::fingerprint::<Rabin>())]
#[case("collections", Collections::fingerprint::<Rabin>())]
#[case("unions", Unions::fingerprint::<Rabin>())]
#[case("nested", Nested::fingerprint::<Rabin>())]
#[case("recursive", TreeNode::fingerprint::<Rabin>())]
#[case("named_unions", Catalogue::fingerprint::<Rabin>())]
fn cross_language_fingerprint_matches_avro_tools(
    #[case] stem: &str,
    #[case] rust_fp: SchemaFingerprint,
) {
    let Some(java) = avro_tools_crc64(stem) else {
        eprintln!("skipping cross-language check for `{stem}`: avro-tools unavailable");
        return;
    };
    assert_eq!(
        hex(&rust_fp),
        java,
        "Rust Rabin fingerprint for `{stem}` disagrees with avro-tools CRC-64-AVRO"
    );
}

/// Logical types: assert the *known* upstream mismatch (see module docs). If
/// this assertion ever fails, `apache-avro` has likely fixed
/// `canonical_form()`; flip this to `assert_eq!` and drop the caveat.
#[test]
fn cross_language_fingerprint_logical_is_known_mismatch() {
    let Some(java) = avro_tools_crc64("logical") else {
        eprintln!("skipping: avro-tools unavailable");
        return;
    };
    let rust = hex(&Logical::fingerprint::<Rabin>());
    assert_ne!(
        rust, java,
        "logical-type fingerprints now AGREE — upstream canonical_form() may be \
         fixed; flip this test to assert_eq! and remove the §14.3 caveat"
    );
}
