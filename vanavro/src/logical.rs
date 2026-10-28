//! Custom logical types (SPEC §15).
//!
//! `apache-avro` drops unknown `logicalType` annotations on primitive types
//! when it parses a schema (it `continue`s past `logicalType` while collecting
//! `custom_attributes`). Custom logical types are therefore recovered from the
//! **raw `serde_json::Value`** of each input, retained alongside the parsed
//! `Schema` (SPEC §4.3).
//!
//! This module owns three things:
//!
//! - [`AvroType`] — the public enum naming the Avro shape an annotation
//!   decorates (re-exported from the crate root).
//! - [`LogicalTypeRegistry`] — the caller-supplied, insertion-keyed mapping
//!   from a `logicalType` string onto a Rust type (+ optional `serde_with`
//!   converter), populated via the builder methods in `lib.rs`.
//! - [`RawNameTable`] + the `get_*_logical_type` recovery helpers — the single,
//!   narrowly-scoped raw-JSON lookup (SPEC §4.3) that resolves type references
//!   through one shared routine ([`RawNameTable::resolve_ref`]) rather than re-implementing
//!   scope-aware lookup per call site.

use std::collections::BTreeMap;

use serde_json::Value;

/// The Avro types that may carry a `logicalType` annotation (SPEC §15.1).
///
/// A registration states which underlying Avro shape the annotation decorates.
/// Only record-level suppression (SPEC §15.3) checks it; field-level mappings
/// apply regardless of the annotated node's actual type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AvroType {
    /// Avro `null`.
    Null,
    /// Avro `boolean`.
    Boolean,
    /// Avro `int`.
    Int,
    /// Avro `long`.
    Long,
    /// Avro `float`.
    Float,
    /// Avro `double`.
    Double,
    /// Avro `bytes`.
    Bytes,
    /// Avro `string`.
    String,
    /// Avro `record`.
    Record,
    /// Avro `enum`.
    Enum,
    /// Avro `fixed`.
    Fixed,
}

/// A single registered custom-logical-type mapping (SPEC §15.2).
#[derive(Debug, Clone)]
pub(crate) struct LogicalTypeMapping {
    /// The Avro shape the annotation decorates (SPEC §15.1).
    pub underlying_type: AvroType,
    /// Rust type path emitted verbatim for any field carrying this
    /// `logicalType` (SPEC §15.2).
    pub rust_type: String,
    /// Optional `serde_with` adapter routing (de)serialisation (SPEC §15.2 /
    /// §15.4). `None` ⇒ the `rust_type` round-trips under `apache-avro` on its
    /// own; no `#[serde_as]` is emitted.
    pub converter_type: Option<String>,
    /// Optional `bon` `#[builder(with = …)]` text emitted verbatim on every
    /// field resolving to this mapping, **only** under `--builder` (SPEC §15.9).
    /// Purely a `bon::Builder` concern: it never touches serde, derives, or the
    /// wire schema, and is a silent no-op without `--builder`.
    pub builder_with: Option<String>,
    /// Caller's assertion that `rust_type` implements `Eq + Hash` (SPEC §11.1).
    /// vanavro can't see the opaque type's trait impls, so a field carrying this
    /// annotation disqualifies its owning record from the `Eq + Hash` uplift
    /// unless this is set. Defaults to `false` (the safe, conservative choice).
    pub eq_hash: bool,
}

/// Caller-supplied registry of custom logical types, keyed by `logicalType`
/// string (SPEC §15.1).
///
/// Insertion-keyed: registering the same name twice replaces the earlier entry
/// (last-write-wins). A `BTreeMap` keeps lookups and any iteration
/// deterministic, which matters for byte-identical output (SPEC §11.7).
#[derive(Debug, Clone, Default)]
pub(crate) struct LogicalTypeRegistry {
    by_name: BTreeMap<String, LogicalTypeMapping>,
}

impl LogicalTypeRegistry {
    /// Register (or replace) a mapping for `logical_type_name`.
    pub(crate) fn insert(
        &mut self,
        logical_type_name: String,
        underlying_type: AvroType,
        rust_type: String,
        converter_type: Option<String>,
        builder_with: Option<String>,
        eq_hash: bool,
    ) {
        self.by_name.insert(
            logical_type_name,
            LogicalTypeMapping {
                underlying_type,
                rust_type,
                converter_type,
                builder_with,
                eq_hash,
            },
        );
    }

    /// Whether any mapping has been registered.
    pub(crate) fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }

    /// Look up a mapping by `logicalType` string.
    pub(crate) fn get(&self, logical_type_name: &str) -> Option<&LogicalTypeMapping> {
        self.by_name.get(logical_type_name)
    }
}

/// The raw `serde_json::Value` of every top-level named definition, keyed by
/// fully-qualified Avro name (SPEC §4.3 / §10.1). Named types declared inline
/// inside another definition (SPEC §10.3) have no entry of their own.
///
/// This is the *only* mechanism that consults raw JSON; everything else in
/// codegen relies on the parsed `Schema` AST. Cross-references inside a value
/// are resolved through [`RawNameTable::resolve_ref`].
#[derive(Debug, Clone, Default)]
pub(crate) struct RawNameTable {
    by_fqn: BTreeMap<String, Value>,
}

impl RawNameTable {
    /// Construct from the FQN → raw-JSON map the schema layer assembles.
    pub(crate) fn new(by_fqn: BTreeMap<String, Value>) -> Self {
        Self { by_fqn }
    }

    /// Fetch the raw JSON of a named definition by FQN.
    pub(crate) fn get(&self, fqn: &str) -> Option<&Value> {
        self.by_fqn.get(fqn)
    }

    /// Resolve a schema-position JSON node to a concrete inline definition,
    /// chasing string type-references through the table (SPEC §15.3 — the
    /// single shared reference-resolution routine).
    ///
    /// `enclosing_ns` is the namespace in scope at the reference site so an
    /// unqualified ref resolves the same way Avro does. Returns the value
    /// unchanged when it is not a reference (already inline, primitive, etc.).
    pub(crate) fn resolve_ref<'a>(
        &'a self,
        value: &'a Value,
        enclosing_ns: Option<&str>,
    ) -> &'a Value {
        // A string in a schema position is either a primitive keyword
        // (`"int"`, `"string"`, …) or a name reference. Only the latter
        // resolves to a table entry.
        if let Value::String(s) = value {
            let fqn = fully_qualified(s, enclosing_ns);
            if let Some(target) = self.by_fqn.get(&fqn) {
                return target;
            }
            return value;
        }
        value
    }
}

/// The recovered logical-type string for a schema-position node, if it carries
/// a `logicalType`. Resolves through `table` so a bare type-reference to a named
/// record/enum/fixed picks up the annotation on the *target* definition.
fn node_logical_type<'a>(
    node: &'a Value,
    table: &'a RawNameTable,
    enclosing_ns: Option<&str>,
) -> Option<&'a str> {
    let resolved = table.resolve_ref(node, enclosing_ns);
    match resolved {
        Value::Object(obj) => obj.get("logicalType").and_then(Value::as_str),
        _ => None,
    }
}

/// The namespace in scope inside a named definition's body: its own explicit
/// `namespace`, else (when the `name` is dotted) the namespace split out of the
/// name, else the enclosing namespace.
fn body_namespace(
    obj: &serde_json::Map<String, Value>,
    enclosing_ns: Option<&str>,
) -> Option<String> {
    obj.get("namespace")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            obj.get("name")
                .and_then(Value::as_str)
                .and_then(|n| n.rsplit_once('.').map(|(ns, _)| ns.to_string()))
        })
        .or_else(|| enclosing_ns.map(str::to_string))
}

/// Recover the `logicalType` on a record field's type (SPEC §15.3 case 1).
///
/// `record_fqn` selects the owning record's raw JSON; `field_name` selects the
/// field. The field's `type` node is resolved through the table so a reference
/// to a named type carrying the annotation is also picked up.
pub(crate) fn get_field_logical_type<'a>(
    table: &'a RawNameTable,
    record_fqn: &str,
    field_name: &str,
) -> Option<&'a str> {
    let record = table.get(record_fqn)?;
    let Value::Object(rec_obj) = record else {
        return None;
    };
    let enclosing = body_namespace(rec_obj, None);
    let fields = rec_obj.get("fields")?.as_array()?;
    let field = fields.iter().find(|f| {
        f.as_object()
            .and_then(|o| o.get("name"))
            .and_then(Value::as_str)
            == Some(field_name)
    })?;
    let ty = field.as_object()?.get("type")?;
    node_logical_type(ty, table, enclosing.as_deref())
}

/// Recover a top-level `logicalType` on a *named record* itself (SPEC §15.3
/// case 2). When present the record's struct is suppressed and every reference
/// to it becomes the mapped `rust_type`.
pub(crate) fn get_record_logical_type<'a>(
    table: &'a RawNameTable,
    record_fqn: &str,
) -> Option<&'a str> {
    let record = table.get(record_fqn)?;
    let obj = record.as_object()?;
    // Only a `record` body counts — guard against an enum/fixed sharing helpers.
    if obj.get("type").and_then(Value::as_str) != Some("record") {
        return None;
    }
    obj.get("logicalType").and_then(Value::as_str)
}

/// Recover the `logicalType` on an array field's *item* type (SPEC §15.3 case 3).
pub(crate) fn get_array_item_logical_type<'a>(
    table: &'a RawNameTable,
    record_fqn: &str,
    field_name: &str,
) -> Option<&'a str> {
    let record = table.get(record_fqn)?;
    let rec_obj = record.as_object()?;
    let enclosing = body_namespace(rec_obj, None);
    let field_type = field_type_node(rec_obj, field_name)?;
    let array = table.resolve_ref(field_type, enclosing.as_deref());
    let items = array.as_object()?.get("items")?;
    node_logical_type(items, table, enclosing.as_deref())
}

/// Recover the `logicalType` on a map field's *value* type (SPEC §15.3 case 4).
pub(crate) fn get_map_value_logical_type<'a>(
    table: &'a RawNameTable,
    record_fqn: &str,
    field_name: &str,
) -> Option<&'a str> {
    let record = table.get(record_fqn)?;
    let rec_obj = record.as_object()?;
    let enclosing = body_namespace(rec_obj, None);
    let field_type = field_type_node(rec_obj, field_name)?;
    let map = table.resolve_ref(field_type, enclosing.as_deref());
    let values = map.as_object()?.get("values")?;
    node_logical_type(values, table, enclosing.as_deref())
}

/// Recover the `logicalType` on the non-null arm of an optional union field
/// (`["null", T]` / `[T, "null"]`) — SPEC §15.3 case 5.
pub(crate) fn get_union_variant_logical_type<'a>(
    table: &'a RawNameTable,
    record_fqn: &str,
    field_name: &str,
) -> Option<&'a str> {
    let record = table.get(record_fqn)?;
    let rec_obj = record.as_object()?;
    let enclosing = body_namespace(rec_obj, None);
    let field_type = field_type_node(rec_obj, field_name)?;
    let Value::Array(variants) = field_type else {
        return None;
    };
    if variants.len() != 2 {
        return None;
    }
    // Find the non-null arm.
    let non_null = variants
        .iter()
        .find(|v| !matches!(v, Value::String(s) if s == "null"))?;
    node_logical_type(non_null, table, enclosing.as_deref())
}

/// Recover the `logicalType` on one arm of a *multi-variant* union field
/// (SPEC §15.3 case 6).
///
/// `arm_index` indexes the union in declaration order, which the parsed
/// `UnionSchema` preserves. `arm_count` is the arity the parsed schema reports:
/// on a mismatch the raw JSON and the parsed union have drifted apart, and
/// recovery is abandoned rather than risking an annotation landing on the wrong
/// arm. The arm node is resolved through the table, so a reference to a named
/// record/enum/fixed picks up the annotation on the *target* definition — this
/// is what makes a record-level logical type (case 2) carry its converter into
/// a union arm.
pub(crate) fn get_union_arm_logical_type<'a>(
    table: &'a RawNameTable,
    record_fqn: &str,
    field_name: &str,
    arm_index: usize,
    arm_count: usize,
) -> Option<&'a str> {
    let record = table.get(record_fqn)?;
    let rec_obj = record.as_object()?;
    let enclosing = body_namespace(rec_obj, None);
    let field_type = field_type_node(rec_obj, field_name)?;
    let Value::Array(arms) = field_type else {
        return None;
    };
    if arms.len() != arm_count {
        return None;
    }
    node_logical_type(arms.get(arm_index)?, table, enclosing.as_deref())
}

/// The raw `type` node of `field_name` inside a record object.
fn field_type_node<'a>(
    rec_obj: &'a serde_json::Map<String, Value>,
    field_name: &str,
) -> Option<&'a Value> {
    let fields = rec_obj.get("fields")?.as_array()?;
    fields
        .iter()
        .find(|f| {
            f.as_object()
                .and_then(|o| o.get("name"))
                .and_then(Value::as_str)
                == Some(field_name)
        })?
        .as_object()?
        .get("type")
}

/// Compose a fully-qualified Avro name from a raw `name` and an optional
/// enclosing namespace; a name containing `.` is already qualified.
fn fully_qualified(name: &str, enclosing_ns: Option<&str>) -> String {
    if name.contains('.') {
        return name.to_string();
    }
    match enclosing_ns.filter(|s| !s.is_empty()) {
        Some(ns) => format!("{ns}.{name}"),
        None => name.to_string(),
    }
}
