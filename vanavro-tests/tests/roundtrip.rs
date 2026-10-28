//! SPEC §7.1 — Rust binary roundtrip tests.
//!
//! Construct generated struct instances (including edge cases), serialize with
//! `apache_avro::Writer` against the type's structurally built `schema()`, then
//! deserialize with `Reader` and assert equality.

use std::collections::HashMap;

use apache_avro::{
    AvroSchema, Reader, Schema, SpecificSingleObjectReader, SpecificSingleObjectWriter, Writer,
    from_value,
};
use rstest::rstest;
use serde::{Serialize, de::DeserializeOwned};

use vanavro_tests::collections::Collections;
use vanavro_tests::custom_logical::{CustomLogical, CustomLogicalUnion, Notification};
use vanavro_tests::defaults::{Defaults, Inner};
use vanavro_tests::diamond::{L, R, Root as DiamondRoot, Shared};
use vanavro_tests::logical::Logical;
use vanavro_tests::nested::{Color, Elem, Inline, Nested, Tag};
use vanavro_tests::primitives::Primitives;
use vanavro_tests::recursive::TreeNode;
use vanavro_tests::scalar::Scalar;
use vanavro_tests::unions::{Unions, UnionsUnion};
use vanavro_tests::{Cents, Email, Money};

/// Process-wide, write-once human-readable serde flag (SPEC §3.3 / §7.1).
///
/// apache-avro 0.22 distinguishes `uuid`-on-string from `uuid`-on-bytes: a
/// `uuid::Uuid` must serialize as a *string* to match a `Uuid(String)` schema,
/// which apache-avro gates behind this global flag. It is `OnceLock`-backed and
/// can be set **only once per process** and never reverted, so we set it
/// deliberately at module scope — visible to every reader — instead of hiding
/// the mutation inside each assertion helper. Other corpus field types are
/// unaffected (bytes / fixed go through their own helpers).
static HUMAN_READABLE_SERDE: std::sync::LazyLock<()> = std::sync::LazyLock::new(|| {
    apache_avro::util::set_serde_human_readable(true);
});

/// Serialize `value` against `schema`, read it back, and assert round-trip
/// equality through the generated type.
fn assert_roundtrip<T>(schema: &Schema, value: &T)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    std::sync::LazyLock::force(&HUMAN_READABLE_SERDE);

    let mut writer = Writer::new(schema, Vec::new()).expect("writer");
    writer.append_ser(value).expect("serialize");
    let bytes = writer.into_inner().expect("flush");

    // The container self-describes (writer schema in the header), so `Reader::new`
    // reads it back against that same schema.
    let reader = Reader::new(&bytes[..]).expect("reader");
    let mut count = 0;
    for record in reader {
        let decoded: T = from_value(&record.expect("read record")).expect("deserialize");
        assert_eq!(&decoded, value, "roundtrip mismatch");
        count += 1;
    }
    assert_eq!(count, 1, "expected exactly one record");
}

/// Serialize `value` with Avro single-object encoding (schema-driven, via
/// `T::get_schema()`), read it back, and assert round-trip equality.
///
/// This exercises apache-avro's *schema-aware* serde path, which — unlike the
/// `types::Value`-based container reader used by [`assert_roundtrip`] — matches
/// the Rust struct name against the schema record name. Collapsed single-union
/// records (SPEC §5.6) deserialize through a private `{Record}Repr` carrier, so
/// this guards that the carrier reports the record's Avro name, not `{Record}Repr`.
fn assert_soe_roundtrip<T>(value: &T)
where
    T: AvroSchema + Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    std::sync::LazyLock::force(&HUMAN_READABLE_SERDE);

    let writer = SpecificSingleObjectWriter::<T>::new().expect("writer");
    let mut bytes = Vec::new();
    writer.write_ref(value, &mut bytes).expect("serialize");

    let reader = SpecificSingleObjectReader::<T>::new().expect("reader");
    let decoded: T = reader.read(&mut bytes.as_slice()).expect("deserialize");
    assert_eq!(&decoded, value, "single-object roundtrip mismatch");
}

/// SPEC §5.6 — collapsed single-union records must also round-trip through the
/// schema-aware single-object encoding, not just the container `Reader`. Covers
/// the bare collapsed enum, a reference to one, a recursive one, and the
/// null-only shape.
#[test]
fn collapsed_union_single_object_roundtrip() {
    use vanavro_tests::collapsed_refs::{NullOnly, Outer, Scalar as RefScalar, Tree};

    assert_soe_roundtrip(&Scalar::Int(7));
    assert_soe_roundtrip(&Scalar::String("hi".into()));
    assert_soe_roundtrip(&Scalar::Null(()));
    assert_soe_roundtrip(&Outer {
        inner: RefScalar::Int(42),
    });
    assert_soe_roundtrip(&Tree::Tree(Box::new(Tree::Int(100))));
    assert_soe_roundtrip(&NullOnly::Null(()));
}

#[rstest]
#[case(Primitives {
    b: true, i: 42, l: 9_000_000_000, f: 1.5, d: 2.5,
    by: vec![0, 1, 2, 255], s: "hello".into(),
})]
#[case(Primitives {
    b: false, i: i32::MIN, l: i64::MAX, f: f32::MIN, d: f64::MAX,
    by: vec![], s: String::new(),
})]
#[case(Primitives {
    b: true, i: i32::MAX, l: i64::MIN, f: 0.0, d: -0.0,
    by: vec![7; 64], s: "unicode: 🦀 héllo 世界".into(),
})]
fn primitives_roundtrip(#[case] value: Primitives) {
    assert_roundtrip(Primitives::schema(), &value);
}

#[test]
fn collections_roundtrip() {
    let value = Collections {
        list: vec![1, 2, 3, -4],
        dict: HashMap::from([("a".into(), "x".into()), ("b".into(), "y".into())]),
        nested: vec![vec![1, 2], vec![], vec![3]],
    };
    assert_roundtrip(Collections::schema(), &value);
}

#[test]
fn collections_empty_roundtrip() {
    let value = Collections {
        list: vec![],
        dict: HashMap::new(),
        nested: vec![],
    };
    assert_roundtrip(Collections::schema(), &value);
}

#[rstest]
#[case(Unions { maybe: Some("set".into()), choice: UnionsUnion::Int(7) })]
#[case(Unions { maybe: None, choice: UnionsUnion::String("s".into()) })]
#[case(Unions { maybe: Some(String::new()), choice: UnionsUnion::Boolean(true) })]
fn unions_roundtrip(#[case] value: Unions) {
    assert_roundtrip(Unions::schema(), &value);
}

/// vanavro extension (SPEC §5.6) — a record whose lone field is a union (`Scalar`)
/// collapses to a single public enum that takes the record's own name.
#[rstest]
#[case(Scalar::Int(7))]
#[case(Scalar::String("hi".into()))]
#[case(Scalar::Boolean(true))]
#[case(Scalar::Null(()))]
fn scalar_roundtrip(#[case] value: Scalar) {
    assert_roundtrip(Scalar::schema(), &value);
}

/// vanavro extension (SPEC §5.6) — a record (`Outer`) referencing a collapsed
/// single-union record (`Scalar`) names the field by the collapsed enum's name,
/// and the nested value round-trips through `Outer`'s schema.
#[test]
fn collapsed_union_record_reference_roundtrip() {
    use vanavro_tests::collapsed_refs::{Outer, Scalar};
    let value = Outer {
        inner: Scalar::Int(42),
    };
    assert_roundtrip(Outer::schema(), &value);
}

/// vanavro extension (SPEC §5.6) — a recursive collapsed single-union record
/// (`Tree`) references itself in a boxed arm; nested values route through the
/// public enum's own serialize/deserialize and round-trip at depth.
#[test]
fn recursive_collapsed_union_record_roundtrip() {
    use vanavro_tests::collapsed_refs::Tree;
    let value = Tree::Tree(Box::new(Tree::Int(100)));
    assert_roundtrip(Tree::schema(), &value);
}

/// SPEC §5.6 — with the collapse *disabled* (`collapse_single_union(false)`),
/// the single-union record `Scalar` is an ordinary struct over its
/// `ScalarUnion` field; the referencing `Outer` and the recursive `Tree` still
/// round-trip through their own schemas, confirming the wire shape is unchanged.
#[test]
fn uncollapsed_union_record_roundtrips() {
    use vanavro_tests::uncollapsed_refs::{Outer, Scalar, ScalarUnion, Tree, TreeUnion};
    let scalar = Scalar {
        choice: ScalarUnion::Int(42),
    };
    assert_roundtrip(Scalar::schema(), &scalar);

    let outer = Outer {
        inner: Scalar {
            choice: ScalarUnion::String("hi".into()),
        },
    };
    assert_roundtrip(Outer::schema(), &outer);

    let tree = Tree {
        node: TreeUnion::Tree(Box::new(Tree {
            node: TreeUnion::Int(100),
        })),
    };
    assert_roundtrip(Tree::schema(), &tree);
}

/// vanavro extension (SPEC §5.6) — a null-only union (`["null"]`) still collapses
/// to a bare enum. Compiling this guards the degenerate one-arm `Serialize` /
/// carrier shape; the roundtrip confirms the wire shape.
#[test]
fn null_only_collapsed_union_record_roundtrip() {
    use vanavro_tests::collapsed_refs::NullOnly;
    let value = NullOnly::Null(());
    assert_roundtrip(NullOnly::schema(), &value);
}

#[test]
fn nested_roundtrip() {
    let value = Nested {
        color: Color::Green,
        tag: Tag::from([1, 2, 3, 4]),
        inline: Inline { v: 99 },
        inlines: vec![Elem { w: 1 }, Elem { w: 2 }],
    };
    assert_roundtrip(Nested::schema(), &value);
}

#[test]
fn diamond_roundtrip() {
    // `Shared` is referenced from both `L` and `R`: the definition rides `L`, the back-reference
    // `R`, and both decode to the same shape.
    let value = DiamondRoot {
        left: L {
            shared: Shared { id: 7 },
        },
        right: R {
            shared: Shared { id: 42 },
        },
    };
    assert_roundtrip(DiamondRoot::schema(), &value);
}

#[test]
fn recursive_roundtrip() {
    // A finite chain exercises the boxed self-reference.
    let value = TreeNode {
        value: 1,
        next: Some(Box::new(TreeNode {
            value: 2,
            next: Some(Box::new(TreeNode {
                value: 3,
                next: None,
            })),
        })),
    };
    assert_roundtrip(TreeNode::schema(), &value);
}

#[test]
fn logical_roundtrip() {
    let value = Logical {
        day: 19_000,
        ms_of_day: 3_600_000,
        us_of_day: 86_399_000_000,
        ts_ms: 1_700_000_000_000,
        ts_us: 1_700_000_000_000_000,
        id: uuid::uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8"),
    };
    assert_roundtrip(Logical::schema(), &value);
}

/// SPEC §15 — custom logical types round-trip through Avro binary, proving the
/// generated code (converterless `::url::Url`, converter-backed `Email` via
/// `#[serde_as]`, the `Vec<Url>` array shape, and the `Option<Url>` arm)
/// actually compiles, serialises, and deserialises against `schema()`.
#[test]
fn custom_logical_roundtrip() {
    let value = CustomLogical {
        homepage: "https://example.com/".parse().expect("valid url"),
        contact: Email("ops@example.com".into()),
        mirrors: vec![
            "https://mirror1.example.com/".parse().expect("url"),
            "https://mirror2.example.com/".parse().expect("url"),
        ],
        fallback: Some("https://fallback.example.com/".parse().expect("url")),
        // `price` is the suppressed-record mapping (`crate::Money`); round-tripping
        // it drives the emitted-but-typeless `build_custom_0logical_1Money` builder.
        price: Money {
            amount: 4200,
            currency: "EUR".into(),
        },
        channel: CustomLogicalUnion::String(Email("union@example.com".into())),
    };
    assert_roundtrip(CustomLogical::schema(), &value);
}

/// SPEC §15.3 case 6 — every arm shape of a multi-variant union carries its
/// custom logical mapping onto the wire: a converter-backed primitive arm
/// (`Email` via `#[serde_as]`), a converterless suppressed-record arm
/// (`crate::Money`), a converter-backed suppressed-record arm (`crate::Cents`,
/// which only reaches the record's wire shape *through* `CentsAsRecord`), an
/// unmapped primitive arm, and the `null` arm.
#[rstest]
#[case(CustomLogicalUnion::Null(()))]
#[case(CustomLogicalUnion::String(Email("arm@example.com".into())))]
#[case(CustomLogicalUnion::Money(Money { amount: -1, currency: "GBP".into() }))]
#[case(CustomLogicalUnion::Cents(Cents(1_234)))]
#[case(CustomLogicalUnion::Int(i32::MIN))]
fn custom_logical_union_arm_roundtrip(#[case] channel: CustomLogicalUnion) {
    let value = CustomLogical {
        homepage: "https://example.com/".parse().expect("valid url"),
        contact: Email("ops@example.com".into()),
        mirrors: vec![],
        fallback: None,
        price: Money {
            amount: 1,
            currency: "EUR".into(),
        },
        channel,
    };
    assert_roundtrip(CustomLogical::schema(), &value);
}

/// SPEC §15.3 case 6 × §5.6 — the same arm shapes through a *collapsed*
/// single-union record, whose hand-written `Serialize` routes converter-backed
/// payloads through `serde_with::ser::SerializeAsWrap` and whose `Deserialize`
/// routes them through the private carrier's `#[serde_as]`. Run through both
/// the container reader and the schema-aware single-object encoding, so a
/// mismatch between the two directions cannot hide.
#[rstest]
#[case(Notification::Null(()))]
#[case(Notification::String(Email("collapsed@example.com".into())))]
#[case(Notification::Money(Money { amount: 7, currency: "CHF".into() }))]
#[case(Notification::Cents(Cents(-99)))]
#[case(Notification::Int(0))]
fn collapsed_union_custom_logical_arm_roundtrip(#[case] value: Notification) {
    assert_roundtrip(Notification::schema(), &value);
    assert_soe_roundtrip(&value);
}

/// SPEC §15 — the optional custom-logical arm round-trips as `None`, and the
/// array shape round-trips empty.
#[test]
fn custom_logical_empty_and_none_roundtrip() {
    let value = CustomLogical {
        homepage: "https://example.com/".parse().expect("valid url"),
        contact: Email(String::new()),
        mirrors: vec![],
        fallback: None,
        price: Money {
            amount: 0,
            currency: String::new(),
        },
        channel: CustomLogicalUnion::Null(()),
    };
    assert_roundtrip(CustomLogical::schema(), &value);
}

/// SPEC §11.5 — a fully-populated value with record-literal and optional
/// defaults round-trips through Avro binary (proves the generated default
/// helpers and `schema()` actually compile and serialize).
#[test]
fn defaults_roundtrip() {
    let value = Defaults {
        count: 99,
        label: "set".into(),
        inner: Inner { v: 5, opt: Some(3) },
    };
    assert_roundtrip(Defaults::schema(), &value);
}

/// SPEC §11.5 — the heart of the feature: when the wire/JSON omits defaulted
/// fields, the generated `#[serde(default = "…")]` helpers fill them in. This
/// exercises both the scalar defaults (`count`, `label`) and the record-literal
/// default (`inner`), including its nested optional `opt` lowering to `None`
/// (the `null`-on-optional bug fix) — verifying generated code, not just text.
#[test]
fn defaults_fire_when_fields_omitted() {
    // `#[serde(default = "…")]` is a serde-framework mechanism: it fires when a
    // field is *absent* from the input. Avro *binary* always encodes every
    // field, so JSON (where keys can be omitted) is the natural trigger — this
    // proves the generated helper fns, not the Avro reader-schema-resolution
    // default path (that is apache-avro's concern, driven by the `default`
    // entries embedded in `schema()`, not by these serde attributes).
    let decoded: Defaults = serde_json::from_str("{}").expect("deserialize with defaults");
    assert_eq!(
        decoded,
        Defaults {
            count: 7,
            label: "anon".into(),
            inner: Inner { v: 1, opt: None },
        }
    );
}
