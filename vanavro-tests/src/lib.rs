#![allow(clippy::all, non_camel_case_types, dead_code, unused_imports)]
//! Integration-test crate for `vanavro` (SPEC §7 / §14).
//!
//! `build.rs` generates Rust types from the `.avsc` corpus under `fixtures/`
//! into `OUT_DIR`. Each fixture uses a distinct namespace, so the generated
//! `pub mod <ns>` blocks can be `include!`d at the crate root without clashing.
//! The generated code carries no inner `#![allow(...)]`; the crate-level one
//! above covers it. The tests under `tests/`
//! exercise:
//!
//! - Writer/Reader binary roundtrips (SPEC §7.1)
//! - deterministic codegen (SPEC §14.2)
//! - structural-vs-canonical and cross-language Rabin / CRC-64-AVRO
//!   fingerprints (SPEC §14.3)
//! - Java-interop roundtrips and checked-in reference fixtures via `avro-tools`
//!   (SPEC §7.2 / §7.4)
//! - `insta` snapshots of generated source (SPEC §14.1)

include!(concat!(env!("OUT_DIR"), "/primitives.rs"));
include!(concat!(env!("OUT_DIR"), "/collections.rs"));
include!(concat!(env!("OUT_DIR"), "/unions.rs"));
include!(concat!(env!("OUT_DIR"), "/nested.rs"));
include!(concat!(env!("OUT_DIR"), "/recursive.rs"));
include!(concat!(env!("OUT_DIR"), "/logical.rs"));
include!(concat!(env!("OUT_DIR"), "/defaults.rs"));
include!(concat!(env!("OUT_DIR"), "/custom_logical.rs"));
include!(concat!(env!("OUT_DIR"), "/aliases.rs"));
include!(concat!(env!("OUT_DIR"), "/scalar.rs"));
include!(concat!(env!("OUT_DIR"), "/tree.rs"));
include!(concat!(env!("OUT_DIR"), "/collapsed_refs.rs"));
include!(concat!(env!("OUT_DIR"), "/uncollapsed_refs.rs"));
include!(concat!(env!("OUT_DIR"), "/deep_ns.rs"));
include!(concat!(env!("OUT_DIR"), "/diamond.rs"));
include!(concat!(env!("OUT_DIR"), "/named_unions.rs"));

// ---- Custom logical type support (SPEC §15) -------------------------------
//
// The `custom_logical` fixture maps four `logicalType`s onto crate-local types:
//
// - `url`   → `::url::Url`   (converterless — `Url` serialises as a plain string
//             that Avro accepts directly).
// - `email` → [`Email`]      (converter-backed — routed through [`EmailAsString`],
//             a `serde_with` adapter that (de)serialises it as a plain string).
// - `money` → [`Money`]      (record-level suppression, SPEC §15.3 case 2 — the
//             `Money` *record* is not generated; references resolve to this type).
// - `cents` → [`Cents`]      (record-level suppression *and* a converter, routed
//             through [`CentsAsRecord`]).
//
// The last two are deliberately a matched pair. A record-level mapping reaches a
// reference — including a multi-variant union arm (§15.3 case 6) — by resolving
// the ref back to the annotated record, and the two halves of that resolution
// fail independently: `money` proves the mapped *type* is substituted, `cents`
// proves the mapping's *converter* comes with it. Before the case-6 fix an arm
// got the first and silently lost the second, so a fixture carrying only a
// converterless record-level mapping could not tell the two apart.
//
// All must be in scope at the crate root because the generated code references
// them by absolute path (`crate::Email` / `crate::EmailAsString` / `crate::Money`
// / `crate::Cents` / `crate::CentsAsRecord`).
//
// The `money` mapping is what makes this fixture exercise SPEC §12.1's sole
// collision-argument exception: because `CustomLogical.price` references the
// suppressed `custom_logical.Money`, a `build_custom_0logical_1Money` schema-
// builder is emitted into the `custom_logical` module with *no* accompanying
// generated Rust type. Compiling and round-tripping this crate is the only place
// that referenced-but-suppressed builder path is checked end to end.

/// A record-shaped custom logical type mapped from `money`. Its fields mirror
/// the suppressed `custom_logical.Money` record so it (de)serialises against the
/// schema that the generated `build_custom_0logical_1Money` builder embeds; it is
/// converterless, so this serde shape must match `{amount: long, currency: string}`
/// directly.
#[derive(Debug, Clone, PartialEq, Eq, ::serde::Serialize, ::serde::Deserialize)]
pub struct Money {
    pub amount: i64,
    pub currency: String,
}

/// A record-shaped custom logical type mapped from `cents`, the converter-backed
/// counterpart to [`Money`]. Unlike `Money` its Rust shape does *not* mirror the
/// suppressed `custom_logical.Cents` record — it is a bare newtype — so it can
/// only round-trip through [`CentsAsRecord`]. That is the point: if a generated
/// reference to it ever loses the converter, this fails to (de)serialise rather
/// than quietly producing the wrong bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Cents(pub i64);

/// `serde_with` adapter mapping a [`Cents`] onto the one-field Avro record the
/// `cents` `logicalType` decorates.
pub struct CentsAsRecord;

/// The wire shape of `custom_logical.Cents`. Named to match the Avro record, so
/// schema-aware deserializers that check the struct name against the schema
/// (apache-avro's single-object reader) resolve it.
#[derive(::serde::Serialize, ::serde::Deserialize)]
#[serde(rename = "Cents")]
struct CentsRepr {
    value: i64,
}

impl serde_with::SerializeAs<Cents> for CentsAsRecord {
    fn serialize_as<S: serde::Serializer>(value: &Cents, serializer: S) -> Result<S::Ok, S::Error> {
        ::serde::Serialize::serialize(&CentsRepr { value: value.0 }, serializer)
    }
}

impl<'de> serde_with::DeserializeAs<'de, Cents> for CentsAsRecord {
    fn deserialize_as<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Cents, D::Error> {
        let repr = <CentsRepr as ::serde::Deserialize>::deserialize(deserializer)?;
        Ok(Cents(repr.value))
    }
}

/// A wrapper over an email address, mapped from the custom `email` logical type
/// and (de)serialised through [`EmailAsString`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Email(pub String);

/// `serde_with` adapter (de)serialising an [`Email`] as a bare string, which is
/// exactly what the underlying Avro `string` carries on the wire.
pub struct EmailAsString;

impl serde_with::SerializeAs<Email> for EmailAsString {
    fn serialize_as<S: serde::Serializer>(value: &Email, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.0)
    }
}

impl<'de> serde_with::DeserializeAs<'de, Email> for EmailAsString {
    fn deserialize_as<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Email, D::Error> {
        let s = <String as serde::Deserialize>::deserialize(deserializer)?;
        Ok(Email(s))
    }
}
