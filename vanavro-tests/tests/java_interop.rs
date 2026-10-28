//! SPEC §7.2 / §7.4 — Java interoperability against checked-in reference
//! fixtures produced by the Apache `avro-tools` CLI.
//!
//! `fixtures/reference/<stem>.avro` is an Avro Object Container File written by
//! Java (`avro-tools fromjson`) with the schema embedded in its header. For
//! each type these tests assert:
//!
//! 1. **Decode (§7.4):** reading the Java-produced container with the generated
//!    Rust type yields the expected value.
//! 2. **Re-encode (§7.4):** serializing that value against the type's
//!    `schema()` and reading it back yields the same logical value (sync
//!    markers / framing differ between writers, so we compare values, not raw
//!    bytes).
//! 3. **Java roundtrip (§7.2):** piping the container through
//!    `avro-tools cat` (Java decodes + re-encodes) and reading the result back
//!    into Rust still yields the original value, and the Java re-encode used a
//!    fresh sync marker (proving it did not pass bytes through). Skipped when
//!    `avro-tools` is unavailable.
//!
//! ## Not implemented: the canonical-JSON assertion (§7.4)
//!
//! SPEC §7.4 also lists "deserialize the `.json` fixture via
//! `apache_avro::from_value` over the canonical Avro JSON form". `apache-avro`
//! 0.22 exposes **no schema-aware Avro-JSON decoder** — only binary
//! (`from_avro_datum`) and Object Container (`Reader`) paths. The generic
//! `Value::from(serde_json::Value)` route yields untyped `Map`s that
//! `from_value` rejects for `record` / `union` shapes, so this assertion cannot
//! be made with the current dependency. The Java-produced `reference/*.json`
//! files are retained anyway as human-readable, version-controlled
//! documentation of each container's contents (and as `regenerate.sh` output);
//! they are intentionally not consumed by a test.

use std::path::{Path, PathBuf};
use std::process::Command;

use apache_avro::{Reader, Schema, Writer, from_value};
use serde::{Serialize, de::DeserializeOwned};

use vanavro_tests::collections::Collections;
use vanavro_tests::defaults::{Defaults, Inner};
use vanavro_tests::logical::Logical;
use vanavro_tests::named_unions::{
    Alpha, Beta, Catalogue, CatalogueUnion, Kind, Many, Score, Single, Tag as CatalogueTag, Weight,
};
use vanavro_tests::nested::{Color, Elem, Inline, Nested, Tag};
use vanavro_tests::primitives::Primitives;
use vanavro_tests::recursive::TreeNode;
use vanavro_tests::scalar::Scalar;
use vanavro_tests::tree::Tree;
use vanavro_tests::unions::{Unions, UnionsUnion};

/// Process-wide, write-once human-readable serde flag (SPEC §3.3 / §7.2).
///
/// apache-avro 0.22 serializes `uuid`-on-string only when this global flag is
/// set (required for the `logical` corpus's `uuid` field). It is `OnceLock`-
/// backed: settable **only once per process** and never reverted, so we set it
/// deliberately at module scope rather than hiding the mutation inside each
/// assertion helper. See the matching declaration in `roundtrip.rs`.
static HUMAN_READABLE_SERDE: std::sync::LazyLock<()> = std::sync::LazyLock::new(|| {
    apache_avro::util::set_serde_human_readable(true);
});

fn reference(stem: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join("reference")
        .join(format!("{stem}.avro"))
}

/// Decode the single record from a container file into `T`.
fn decode_container<T: DeserializeOwned>(bytes: &[u8]) -> T {
    let reader = Reader::new(bytes).expect("reader from container header");
    let mut value = None;
    for record in reader {
        assert!(value.is_none(), "expected exactly one record");
        value = Some(from_value(&record.expect("read record")).expect("deserialize"));
    }
    value.expect("at least one record")
}

/// Full §7.4 + §7.2 assertion for one type.
fn assert_java_interop<T>(stem: &str, schema: &Schema, expected: &T)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    std::sync::LazyLock::force(&HUMAN_READABLE_SERDE);

    let container = std::fs::read(reference(stem)).expect("read reference container");

    // (1) Java-produced container decodes to the expected Rust value.
    let decoded: T = decode_container(&container);
    assert_eq!(&decoded, expected, "decode of `{stem}` reference mismatch");

    // (2) Rust re-encode against schema() decodes back to the same value.
    let mut writer = Writer::new(schema, Vec::new()).expect("writer");
    writer.append_ser(expected).expect("serialize");
    let reencoded = writer.into_inner().expect("flush");
    let redecoded: T = decode_container(&reencoded);
    assert_eq!(&redecoded, expected, "rust re-encode of `{stem}` mismatch");

    // (3) Java roundtrip via `avro-tools cat`.
    assert_java_cat_roundtrip(stem, &container, expected);
}

/// Pipe the container through `avro-tools cat` and re-read it; verify the value
/// survives and that Java actually re-encoded (sync marker changed).
fn assert_java_cat_roundtrip<T>(stem: &str, input: &[u8], expected: &T)
where
    T: DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let dir = std::env::temp_dir().join(format!("vanavro-interop-{stem}"));
    std::fs::create_dir_all(&dir).expect("temp dir");
    let in_path = dir.join("in.avro");
    let out_path = dir.join("out.avro");
    std::fs::write(&in_path, input).expect("write temp input");

    let status = Command::new("avro-tools")
        .arg("cat")
        .arg(&in_path)
        .arg(&out_path)
        .status();
    let Ok(status) = status else {
        eprintln!("skipping `avro-tools cat` for `{stem}`: tool unavailable");
        return;
    };
    assert!(status.success(), "avro-tools cat failed for `{stem}`");

    let out = std::fs::read(&out_path).expect("read cat output");
    let roundtripped: T = decode_container(&out);
    assert_eq!(
        &roundtripped, expected,
        "java cat roundtrip of `{stem}` mismatch"
    );

    // §7.2 sync-marker verification: the 16-byte sync marker (last 16 bytes of
    // the file header, before records) must differ, proving Java re-encoded
    // rather than copying bytes.
    assert_ne!(
        sync_marker(input),
        sync_marker(&out),
        "avro-tools cat for `{stem}` reused the sync marker (no re-encode?)"
    );
}

/// Extract the 16-byte sync marker that terminates the container file header.
/// The header is `Obj\x01`, a map of metadata, then the 16-byte marker; rather
/// than parse it we take the marker that also trails every data block — the
/// final 16 bytes of the file.
fn sync_marker(container: &[u8]) -> [u8; 16] {
    let n = container.len();
    assert!(n >= 16, "container too small to hold a sync marker");
    container[n - 16..].try_into().expect("16 bytes")
}

#[test]
fn java_interop_primitives() {
    let expected = Primitives {
        b: true,
        i: 42,
        l: 9_000_000_000,
        f: 1.5,
        d: 2.5,
        by: vec![0, 1, 2, 255],
        s: "hello 🦀 世界".into(),
    };
    assert_java_interop("primitives", Primitives::schema(), &expected);
}

#[test]
fn java_interop_collections() {
    let expected = Collections {
        list: vec![1, 2, 3, -4],
        dict: std::collections::HashMap::from([("a".into(), "x".into()), ("b".into(), "y".into())]),
        nested: vec![vec![1, 2], vec![], vec![3]],
    };
    assert_java_interop("collections", Collections::schema(), &expected);
}

#[test]
fn java_interop_unions() {
    let expected = Unions {
        maybe: Some("set".into()),
        choice: UnionsUnion::Int(7),
    };
    assert_java_interop("unions", Unions::schema(), &expected);
}

#[test]
fn java_interop_nested() {
    let expected = Nested {
        color: Color::Green,
        tag: Tag::from([1, 2, 3, 4]),
        inline: Inline { v: 99 },
        inlines: vec![Elem { w: 1 }, Elem { w: 2 }],
    };
    assert_java_interop("nested", Nested::schema(), &expected);
}

#[test]
fn java_interop_recursive() {
    let expected = TreeNode {
        value: 1,
        next: Some(Box::new(TreeNode {
            value: 2,
            next: Some(Box::new(TreeNode {
                value: 3,
                next: None,
            })),
        })),
    };
    assert_java_interop("recursive", TreeNode::schema(), &expected);
}

#[test]
fn java_interop_defaults() {
    // A record whose schema carries scalar, record-literal, and nested-optional
    // `default`s. The reference container was written by Java from a *fully
    // populated* datum, so this proves the generated `schema()` for a
    // record-with-defaults is byte-compatible with the Java writer/reader (the
    // defaults travel in the embedded schema but do not affect a complete
    // datum). Default *resolution* (a writer omitting a field) is apache-avro's
    // concern and is covered by the serde-default tests in `roundtrip.rs`.
    let expected = Defaults {
        count: 99,
        label: "set".into(),
        inner: Inner { v: 5, opt: Some(3) },
    };
    assert_java_interop("defaults", Defaults::schema(), &expected);
}

#[test]
fn java_interop_logical() {
    let expected = Logical {
        day: 19_000,
        ms_of_day: 3_600_000,
        us_of_day: 86_399_000_000,
        ts_ms: 1_700_000_000_000,
        ts_us: 1_700_000_000_000_000,
        id: uuid::uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8"),
    };
    assert_java_interop("logical", Logical::schema(), &expected);
}

#[test]
fn java_interop_scalar() {
    // SPEC §5.6: `Scalar` is a single-union-record record that collapses to a
    // bare `enum`, yet serializes as the original 1-field record. This proves
    // the collapsed Rust enum decodes the Java-written container and re-encodes
    // byte-compatibly against the *record* schema Java uses.
    let expected = Scalar::Int(7);
    assert_java_interop("scalar", Scalar::schema(), &expected);

    // Assert interop for the other arms (Null, String, Boolean) as well.
    assert_java_roundtrip_by_value("scalar", Scalar::schema(), &Scalar::Null(()));
    assert_java_roundtrip_by_value("scalar", Scalar::schema(), &Scalar::String("hello".into()));
    assert_java_roundtrip_by_value("scalar", Scalar::schema(), &Scalar::Boolean(true));
}

#[test]
fn java_interop_tree() {
    // SPEC §5.6: a recursive collapsed single-union record. Nested values route
    // through the public enum's `Serialize` impl / `try_from`
    // `Deserialize`, so this validates that the recursive wire shape stays
    // identical to the non-collapsed 1-field record across the JVM boundary.
    let expected = Tree::Tree(Box::new(Tree::Int(100)));
    assert_java_interop("tree", Tree::schema(), &expected);

    // Also assert interop for the Null arm of the recursive tree.
    assert_java_roundtrip_by_value("tree", Tree::schema(), &Tree::Null(()));
}

#[test]
fn java_interop_named_unions() {
    // SPEC §5.3: every `enum` union-naming shape in one record — the field-root
    // default (`CatalogueUnion`), names at the field root (`Single`, `Many`,
    // `Alpha`, `Beta`), names propagated into an `array` / `map` (`Tag`,
    // `Score`), and names propagated through a transparent `[null, T]` wrapper
    // (`Weight`, `Kind`). `schema()` strips the naming hints, yet the datum
    // wire shape stays identical across the JVM boundary.
    let expected = Catalogue {
        primary_key: CatalogueUnion::Long(123_456_789_012),
        nickname: Some("nick".into()),
        single: Single::String("solo".into()),
        many: Many::Boolean(true),
        tags: vec![
            CatalogueTag::String("a".into()),
            CatalogueTag::Null(()),
            CatalogueTag::Long(42),
        ],
        scores: std::collections::HashMap::from([
            ("x".into(), Score::Int(3)),
            ("y".into(), Score::Double(1.5)),
        ]),
        optional_scores: Some(std::collections::HashMap::from([(
            "w".into(),
            Weight::Double(2.5),
        )])),
        variant_array: Some(vec![Kind::Int(1), Kind::String("k".into())]),
        alpha: Alpha::String("al".into()),
        beta: Beta::Double(9.75),
    };
    assert_java_interop("named_unions", Catalogue::schema(), &expected);
}

fn assert_java_roundtrip_by_value<T>(stem: &str, schema: &Schema, expected: &T)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    // (1) Rust re-encode against schema() decodes back to the same value.
    let mut writer = Writer::new(schema, Vec::new()).expect("writer creation");
    writer.append_ser(expected).expect("serialize");
    let reencoded = writer.into_inner().expect("flush");
    let redecoded: T = decode_container(&reencoded);
    assert_eq!(&redecoded, expected, "rust re-encode of `{stem}` mismatch");

    // (2) Java roundtrip via `avro-tools cat`.
    assert_java_cat_roundtrip(stem, &reencoded, expected);
}
