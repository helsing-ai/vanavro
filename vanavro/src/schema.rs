//! Parse `.avsc` JSON and `.avdl` IDL inputs into a single, merged Avro name
//! table.
//!
//! Implements the IDL pipeline and multi-schema resolution: every
//! input contributes its top-level named types — record, enum, fixed — into a
//! global `BTreeMap` keyed by fully-qualified Avro name. Inline references that
//! point at another registered name are rewritten to string refs so a single
//! [`Schema::parse_list`] call resolves the full cross-reference graph.
//!
//! Returns the resolved schemas in fully-qualified-name order.

use std::collections::{BTreeMap, btree_map::Entry};
use std::path::Path;

use apache_avro::Schema;
use apache_avro::error::Details as AvroDetails;
use serde_json::{Map, Value};

use crate::GeneratorError;
use crate::logical::RawNameTable;

/// Result of merging and resolving every input: the parsed `Schema` list plus
/// the raw-JSON name table needed to recover custom `logicalType` annotations
/// that `apache-avro` drops on primitives.
pub(crate) struct Resolved {
    /// Resolved schemas in fully-qualified-name order.
    pub schemas: Vec<Schema>,
    /// Denormalised raw JSON of every top-level named definition, keyed by
    /// FQN, for logical-type recovery only.
    pub raw_names: RawNameTable,
}

/// Merge every input into a single named-schema registry and resolve all
/// cross-references in one pass.
///
/// `idl_path` is honoured only when the `idl` feature is enabled; otherwise
/// passing `Some` raises [`GeneratorError::UnsupportedSchema`].
pub(crate) fn parse(
    schema_jsons: &[String],
    idl_path: Option<&Path>,
) -> Result<Resolved, GeneratorError> {
    let mut registry: BTreeMap<String, RegistryEntry> = BTreeMap::new();

    for (idx, json) in schema_jsons.iter().enumerate() {
        let source = format!("<schemas[{idx}]>");
        let value = parse_json(json, &source)?;
        ingest_top_level(value, &source, &mut registry)?;
    }

    if let Some(path) = idl_path {
        ingest_idl(path, &mut registry)?;
    }

    denormalize_registry(&mut registry);

    // Surface dangling references with a pointer to the offending field before
    // `parse_list` collapses them into its opaque internal message (SPEC §10.2).
    check_references(&registry)?;

    // Snapshot the denormalised raw JSON keyed by FQN before handing the values
    // to `parse_list`. This is the single raw-JSON source consulted later for
    // custom logical-type recovery (SPEC §4.3); all other codegen relies on the
    // parsed `Schema` AST.
    let raw_names = RawNameTable::new(
        registry
            .iter()
            .map(|(k, entry)| (k.clone(), entry.value.clone()))
            .collect(),
    );

    let merged: Vec<String> = registry
        .values()
        .map(|entry| {
            serde_json::to_string(&entry.value)
                .expect("re-serialising a serde_json::Value never fails")
        })
        .collect();

    let schemas = Schema::parse_list(&merged).map_err(|inner| GeneratorError::SchemaParse {
        source: "<merged registry>".into(),
        inner,
    })?;

    Ok(Resolved { schemas, raw_names })
}

/// One slot in the global name table, retaining the source label for clear
/// duplicate-conflict diagnostics.
struct RegistryEntry {
    value: Value,
    source: String,
}

/// JSON-parse one input, wrapping any `serde_json` failure in the same
/// `apache_avro::Error` shape (`Details::ParseSchemaJson`) that `Schema::parse_str`
/// would have produced for the same input.
fn parse_json(json: &str, source: &str) -> Result<Value, GeneratorError> {
    serde_json::from_str(json).map_err(|err| GeneratorError::SchemaParse {
        source: source.to_string(),
        inner: AvroDetails::ParseSchemaJson(err).into(),
    })
}

/// Register every top-level named definition in `value`. A `.avsc` document is
/// either a single named type (object) or a flat array of named types;
/// a primitive top-level input or a non-object array entry is rejected.
fn ingest_top_level(
    mut value: Value,
    source: &str,
    registry: &mut BTreeMap<String, RegistryEntry>,
) -> Result<(), GeneratorError> {
    normalise_idl_extensions(&mut value);
    match value {
        Value::Object(_) => register_named(value, source, registry),
        Value::Array(items) => {
            for item in items {
                match item {
                    Value::Object(_) => register_named(item, source, registry)?,
                    _ => {
                        return Err(GeneratorError::UnsupportedSchema {
                            detail: format!(
                                "input {source} has a non-named entry inside its top-level array"
                            ),
                        });
                    }
                }
            }
            Ok(())
        }
        _ => Err(GeneratorError::UnsupportedSchema {
            detail: format!("input {source} is not a named record/enum/fixed schema"),
        }),
    }
}

/// Insert a single named-type JSON value into the registry, comparing against
/// any prior declaration to flag genuine conflicts versus harmless redeclarations.
fn register_named(
    value: Value,
    source: &str,
    registry: &mut BTreeMap<String, RegistryEntry>,
) -> Result<(), GeneratorError> {
    let Value::Object(ref obj) = value else {
        return Err(GeneratorError::UnsupportedSchema {
            detail: format!("input {source} is not a named record/enum/fixed schema"),
        });
    };

    // SPEC §10.1: only record / enum / fixed are named definitions. Anything
    // else carrying a `name` (e.g. `{"type":"array","name":"X",…}`) would be
    // accepted by `parse_list` and silently produce no output.
    match obj.get("type").and_then(Value::as_str) {
        Some("record" | "enum" | "fixed") => {}
        other => {
            let found = other.map_or_else(
                || "no string `type`".to_string(),
                |t| format!("`\"type\": \"{t}\"`"),
            );
            return Err(GeneratorError::UnsupportedSchema {
                detail: format!(
                    "input {source} is not a named record/enum/fixed schema (found {found}); \
                     top-level inputs must declare `\"type\"` as `record`, `enum` or `fixed`"
                ),
            });
        }
    }

    let Some(raw_name) = obj.get("name").and_then(Value::as_str).map(str::to_string) else {
        return Err(GeneratorError::UnsupportedSchema {
            detail: format!("input {source} has no top-level `name` field"),
        });
    };
    let local_ns = obj
        .get("namespace")
        .and_then(Value::as_str)
        .map(str::to_string);
    let fqn = fully_qualified(&raw_name, local_ns.as_deref());

    let normalised = with_explicit_namespace(value, local_ns.as_deref());

    match registry.entry(fqn.clone()) {
        Entry::Vacant(slot) => {
            slot.insert(RegistryEntry {
                value: normalised,
                source: source.to_string(),
            });
            Ok(())
        }
        Entry::Occupied(existing) => {
            if canonical_equal(&existing.get().value, &normalised) {
                Ok(())
            } else {
                Err(GeneratorError::DuplicateName {
                    name: fqn,
                    first: existing.get().source.clone(),
                    second: source.to_string(),
                })
            }
        }
    }
}

#[cfg(feature = "idl")]
fn ingest_idl(
    path: &Path,
    registry: &mut BTreeMap<String, RegistryEntry>,
) -> Result<(), GeneratorError> {
    let source = path.display().to_string();
    let output =
        avdl::Idl2Schemata::new()
            .extract(path)
            .map_err(|report| GeneratorError::IdlParse {
                source: source.clone(),
                // `report` is a `miette::Report`, which inverts the usual
                // convention: `Debug` renders the full graphical diagnostic
                // (source span, label, help) while `Display` yields only the
                // terse top-level message. We want the rich rendering here.
                inner: Box::<dyn std::error::Error + Send + Sync>::from(format!("{report:?}")),
            })?;
    for named in output.schemas {
        ingest_top_level(named.schema, &source, registry)?;
    }
    Ok(())
}

#[cfg(not(feature = "idl"))]
fn ingest_idl(
    _path: &Path,
    _registry: &mut BTreeMap<String, RegistryEntry>,
) -> Result<(), GeneratorError> {
    Err(GeneratorError::UnsupportedSchema {
        detail: "IDL parsing requires the `idl` feature to be enabled".into(),
    })
}

/// Rewrite IDL-only `"type": "error"` decorations into `"type": "record"`
/// recursively. Avro JSON has no `error` type — IDL errors are records that
/// the protocol's `errors` list flags specially. Since vanavro v1 doesn't
/// emit RPC scaffolding, the distinction collapses to "record"
/// for type generation.
fn normalise_idl_extensions(value: &mut Value) {
    match value {
        Value::Object(obj) => {
            if obj.get("type").and_then(Value::as_str) == Some("error") {
                obj.insert("type".to_string(), Value::String("record".to_string()));
            }
            for (_, v) in obj.iter_mut() {
                normalise_idl_extensions(v);
            }
        }
        Value::Array(items) => {
            for item in items {
                normalise_idl_extensions(item);
            }
        }
        _ => {}
    }
}

/// Stamp an explicit `namespace` field onto a named definition so reparsing it
/// later — possibly in a different enclosing scope — yields the same name.
fn with_explicit_namespace(value: Value, namespace: Option<&str>) -> Value {
    let Some(ns) = namespace.filter(|s| !s.is_empty()) else {
        return value;
    };
    let Value::Object(mut obj) = value else {
        return value;
    };
    obj.entry("namespace".to_string())
        .or_insert_with(|| Value::String(ns.to_string()));
    Value::Object(obj)
}

/// Walk each registered schema and replace any inline child whose
/// fully-qualified name also appears in the registry with a string reference.
///
/// Avro disallows redefining a named type once it has been declared in scope,
/// so without this pass `Schema::parse_list` would reject the redundant inline
/// copies that `avdl::Idl2Schemata` emits when it makes each schema
/// self-contained.
fn denormalize_registry(registry: &mut BTreeMap<String, RegistryEntry>) {
    let known: std::collections::BTreeSet<String> = registry.keys().cloned().collect();

    for (own_name, entry) in registry.iter_mut() {
        let ns = top_level_namespace(&entry.value);
        denormalize_children(&mut entry.value, &known, ns.as_deref(), own_name);
    }
}

fn top_level_namespace(value: &Value) -> Option<String> {
    let Value::Object(obj) = value else {
        return None;
    };
    obj.get("namespace")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            obj.get("name")
                .and_then(Value::as_str)
                .and_then(|n| n.rsplit_once('.').map(|(ns, _)| ns.to_string()))
        })
}

/// Recurse into the schema-bearing positions of `value` — `fields[].type`,
/// `items`, `values`, and union arrays — calling [`maybe_replace`] on each.
fn denormalize_children(
    value: &mut Value,
    known: &std::collections::BTreeSet<String>,
    enclosing_ns: Option<&str>,
    own_fqn: &str,
) {
    match value {
        Value::Object(obj) => {
            let local_ns = obj
                .get("namespace")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| enclosing_ns.map(str::to_string));
            let local_ns_ref = local_ns.as_deref();

            if let Some(Value::Array(fields)) = obj.get_mut("fields") {
                for field in fields {
                    if let Value::Object(field_obj) = field
                        && let Some(field_type) = field_obj.get_mut("type")
                    {
                        maybe_replace(field_type, known, local_ns_ref, own_fqn);
                    }
                }
            }
            if let Some(items) = obj.get_mut("items") {
                maybe_replace(items, known, local_ns_ref, own_fqn);
            }
            if let Some(values) = obj.get_mut("values") {
                maybe_replace(values, known, local_ns_ref, own_fqn);
            }
        }
        Value::Array(variants) => {
            for variant in variants {
                maybe_replace(variant, known, enclosing_ns, own_fqn);
            }
        }
        _ => {}
    }
}

/// If `value` is an inline named definition whose FQN is already known, swap
/// it for a string reference; otherwise recurse into it.
fn maybe_replace(
    value: &mut Value,
    known: &std::collections::BTreeSet<String>,
    enclosing_ns: Option<&str>,
    own_fqn: &str,
) {
    let candidate_fqn = inline_named_fqn(value, enclosing_ns);
    if let Some(fqn) = candidate_fqn
        && fqn != own_fqn
        && known.contains(&fqn)
    {
        *value = Value::String(fqn);
        return;
    }
    denormalize_children(value, known, enclosing_ns, own_fqn);
}

/// Extract the fully-qualified name of `value` if it is an inline named
/// definition (record / enum / fixed); otherwise `None`.
fn inline_named_fqn(value: &Value, enclosing_ns: Option<&str>) -> Option<String> {
    let Value::Object(obj) = value else {
        return None;
    };
    let type_str = obj.get("type").and_then(Value::as_str)?;
    if !matches!(type_str, "record" | "enum" | "fixed") {
        return None;
    }
    let raw_name = obj.get("name").and_then(Value::as_str)?;
    let ns = obj
        .get("namespace")
        .and_then(Value::as_str)
        .or(enclosing_ns);
    Some(fully_qualified(raw_name, ns))
}

/// Validate that every string type reference in the merged registry resolves to
/// an Avro primitive, a top-level name, or a name defined inline somewhere in
/// the registry. Failing here — instead of in `parse_list` — lets us point at
/// the referencing field rather than emit apache-avro's opaque message.
fn check_references(registry: &BTreeMap<String, RegistryEntry>) -> Result<(), GeneratorError> {
    // Everything resolvable in the merged set: top-level FQNs plus any
    // record/enum/fixed declared inline. Collecting inline names keeps valid
    // references to anonymous inline types (SPEC §10.3) from tripping the check.
    let mut known: std::collections::BTreeSet<String> = registry.keys().cloned().collect();
    for entry in registry.values() {
        let ns = top_level_namespace(&entry.value);
        collect_inline_names(&entry.value, ns.as_deref(), &mut known);
    }

    for (fqn, entry) in registry {
        let ns = top_level_namespace(&entry.value);
        let referrer = format!("record `{fqn}` ({})", entry.source);
        check_schema_refs(&entry.value, &known, ns.as_deref(), &referrer)?;
    }
    Ok(())
}

/// Record the fully-qualified name of every inline named definition reachable
/// from `value`, mirroring the schema-bearing positions [`denormalize_children`]
/// walks.
fn collect_inline_names(
    value: &Value,
    enclosing_ns: Option<&str>,
    out: &mut std::collections::BTreeSet<String>,
) {
    match value {
        Value::Object(obj) => {
            let local_ns = obj
                .get("namespace")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| enclosing_ns.map(str::to_string));
            let local_ns_ref = local_ns.as_deref();

            if let Some(type_str) = obj.get("type").and_then(Value::as_str)
                && matches!(type_str, "record" | "enum" | "fixed")
                && let Some(name) = obj.get("name").and_then(Value::as_str)
            {
                out.insert(fully_qualified(name, local_ns_ref));
            }

            if let Some(Value::Array(fields)) = obj.get("fields") {
                for field in fields {
                    if let Value::Object(field_obj) = field
                        && let Some(field_type) = field_obj.get("type")
                    {
                        collect_inline_names(field_type, local_ns_ref, out);
                    }
                }
            }
            if let Some(items) = obj.get("items") {
                collect_inline_names(items, local_ns_ref, out);
            }
            if let Some(values) = obj.get("values") {
                collect_inline_names(values, local_ns_ref, out);
            }
        }
        Value::Array(variants) => {
            for variant in variants {
                collect_inline_names(variant, enclosing_ns, out);
            }
        }
        _ => {}
    }
}

/// Recurse the schema-bearing positions of `value`, flagging any string
/// reference that does not resolve against `known`.
fn check_schema_refs(
    value: &Value,
    known: &std::collections::BTreeSet<String>,
    enclosing_ns: Option<&str>,
    referrer: &str,
) -> Result<(), GeneratorError> {
    match value {
        Value::String(name) => check_name_ref(name, known, enclosing_ns, referrer),
        Value::Object(obj) => {
            let local_ns = obj
                .get("namespace")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| enclosing_ns.map(str::to_string));
            let local_ns_ref = local_ns.as_deref();

            if let Some(Value::Array(fields)) = obj.get("fields") {
                for field in fields {
                    if let Value::Object(field_obj) = field
                        && let Some(field_type) = field_obj.get("type")
                    {
                        let field_name =
                            field_obj.get("name").and_then(Value::as_str).unwrap_or("?");
                        let loc = format!("field `{field_name}` of {referrer}");
                        check_schema_refs(field_type, known, local_ns_ref, &loc)?;
                    }
                }
            }
            if let Some(items) = obj.get("items") {
                check_schema_refs(items, known, local_ns_ref, referrer)?;
            }
            if let Some(values) = obj.get("values") {
                check_schema_refs(values, known, local_ns_ref, referrer)?;
            }
            Ok(())
        }
        Value::Array(variants) => {
            for variant in variants {
                check_schema_refs(variant, known, enclosing_ns, referrer)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// A bare string in a schema position is either an Avro primitive or a name
/// reference; reject the latter when it resolves to nothing known.
fn check_name_ref(
    name: &str,
    known: &std::collections::BTreeSet<String>,
    enclosing_ns: Option<&str>,
    referrer: &str,
) -> Result<(), GeneratorError> {
    const PRIMITIVES: [&str; 8] = [
        "null", "boolean", "int", "long", "float", "double", "bytes", "string",
    ];
    if PRIMITIVES.contains(&name) {
        return Ok(());
    }
    if known.contains(name) || known.contains(&fully_qualified(name, enclosing_ns)) {
        return Ok(());
    }
    Err(GeneratorError::UnresolvedReference {
        name: name.to_string(),
        referenced_from: referrer.to_string(),
    })
}

/// Compose a fully-qualified Avro name from a raw `name` and an optional
/// enclosing namespace; a name containing `.` is treated as already qualified
/// (Avro spec §names).
fn fully_qualified(name: &str, enclosing_ns: Option<&str>) -> String {
    if name.contains('.') {
        return name.to_string();
    }
    match enclosing_ns.filter(|s| !s.is_empty()) {
        Some(ns) => format!("{ns}.{name}"),
        None => name.to_string(),
    }
}

/// Structural equality after recursively sorting JSON object keys — accepts
/// two declarations that differ only in field ordering or whitespace while
/// still rejecting genuine schema conflicts.
fn canonical_equal(a: &Value, b: &Value) -> bool {
    sort_keys(a) == sort_keys(b)
}

fn sort_keys(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut sorted: BTreeMap<String, Value> = BTreeMap::new();
            for (k, v) in map {
                sorted.insert(k.clone(), sort_keys(v));
            }
            let mut out = Map::with_capacity(sorted.len());
            for (k, v) in sorted {
                out.insert(k, v);
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(sort_keys).collect()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test shim: run [`parse`] and return just the resolved schema list, so
    /// the existing assertions (which predate the `Resolved` wrapper) keep
    /// working unchanged.
    fn parse(
        schema_jsons: &[String],
        idl_path: Option<&Path>,
    ) -> Result<Vec<Schema>, GeneratorError> {
        super::parse(schema_jsons, idl_path).map(|r| r.schemas)
    }

    const RECORD_A: &str = r#"{
        "type": "record",
        "name": "A",
        "namespace": "com.example",
        "fields": [
            {"name": "id", "type": "long"}
        ]
    }"#;

    const RECORD_A_VARIANT: &str = r#"{
        "type": "record",
        "name": "A",
        "namespace": "com.example",
        "fields": [
            {"name": "id", "type": "string"}
        ]
    }"#;

    const RECORD_B_REFS_A: &str = r#"{
        "type": "record",
        "name": "B",
        "namespace": "com.example",
        "fields": [
            {"name": "child", "type": "com.example.A"}
        ]
    }"#;

    #[test]
    fn parses_single_record() {
        let out = parse(&[RECORD_A.to_string()], None).expect("parse succeeded");
        assert_eq!(out.len(), 1);
        let name = out[0].name().expect("named schema");
        assert_eq!(name.fullname(None), "com.example.A");
    }

    #[test]
    fn merges_two_records_with_cross_reference() {
        let out = parse(&[RECORD_A.to_string(), RECORD_B_REFS_A.to_string()], None)
            .expect("cross-input refs should resolve");
        assert_eq!(out.len(), 2);
        let names: Vec<String> = out
            .iter()
            .map(|s| s.name().unwrap().fullname(None))
            .collect();
        assert_eq!(names, vec!["com.example.A", "com.example.B"]);
    }

    #[test]
    fn identical_redeclaration_is_silent() {
        let out = parse(&[RECORD_A.to_string(), RECORD_A.to_string()], None)
            .expect("identical redeclaration is a no-op");
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn conflicting_redeclaration_raises_duplicate_name() {
        let err = parse(&[RECORD_A.to_string(), RECORD_A_VARIANT.to_string()], None)
            .expect_err("conflicting redeclaration must error");
        match err {
            GeneratorError::DuplicateName { name, .. } => {
                assert_eq!(name, "com.example.A");
            }
            other => panic!("expected DuplicateName, got {other:?}"),
        }
    }

    #[test]
    fn rejects_primitive_top_level_input() {
        let err =
            parse(&[r#""string""#.to_string()], None).expect_err("primitive top-level rejected");
        assert!(matches!(err, GeneratorError::UnsupportedSchema { .. }));
    }

    // Output schemas must come back sorted by fully-qualified Avro name even
    // when inputs are supplied in reverse alphabetical order (SPEC §11.7).
    #[test]
    fn output_is_sorted_by_fully_qualified_name() {
        let alpha = r#"{"type":"record","name":"Alpha","namespace":"x.y","fields":[]}"#;
        let beta = r#"{"type":"record","name":"Beta","namespace":"x.y","fields":[]}"#;
        let gamma = r#"{"type":"record","name":"Gamma","namespace":"x.y","fields":[]}"#;

        let out = parse(
            &[gamma.to_string(), alpha.to_string(), beta.to_string()],
            None,
        )
        .expect("parse succeeded");

        let names: Vec<String> = out
            .iter()
            .map(|s| s.name().unwrap().fullname(None))
            .collect();
        assert_eq!(names, vec!["x.y.Alpha", "x.y.Beta", "x.y.Gamma"]);
    }

    // A record that references a name no input declared must surface as
    // `UnresolvedReference`, naming the dangling reference and pointing at the
    // referencing field and source (SPEC §10.2 unresolved-ref path).
    #[test]
    fn unresolved_reference_surfaces_with_field_context() {
        let referring = r#"{
            "type": "record",
            "name": "Referrer",
            "namespace": "com.example",
            "fields": [{"name": "missing", "type": "com.example.Ghost"}]
        }"#;
        let err =
            parse(&[referring.to_string()], None).expect_err("unresolved reference must error");
        match err {
            GeneratorError::UnresolvedReference {
                name,
                referenced_from,
            } => {
                assert_eq!(name, "com.example.Ghost");
                assert!(
                    referenced_from.contains("missing")
                        && referenced_from.contains("com.example.Referrer"),
                    "referenced_from points at the field and record: {referenced_from}"
                );
            }
            other => panic!("expected UnresolvedReference, got {other:?}"),
        }
    }

    /// Avdl emits self-contained schemas: when one top-level schema inlines
    /// another's full definition, the denormalize pass must rewrite the inline
    /// copy to a string ref so `Schema::parse_list` doesn't reject the duplicate.
    #[test]
    fn denormalize_replaces_inline_duplicate_with_string_ref() {
        // `B` is registered top-level, but `A` also inlines `B`'s full definition
        // inside its field.type — mimicking `avdl::Idl2Schemata` output.
        let record_b = r#"{
            "type": "record",
            "name": "B",
            "namespace": "com.example",
            "fields": [{"name": "v", "type": "int"}]
        }"#;
        let record_a_inlines_b = r#"{
            "type": "record",
            "name": "A",
            "namespace": "com.example",
            "fields": [{
                "name": "child",
                "type": {
                    "type": "record",
                    "name": "B",
                    "namespace": "com.example",
                    "fields": [{"name": "v", "type": "int"}]
                }
            }]
        }"#;

        let out = parse(
            &[record_b.to_string(), record_a_inlines_b.to_string()],
            None,
        )
        .expect("denormalize should rewrite the inline copy to a name ref");
        assert_eq!(out.len(), 2);
        let names: Vec<String> = out
            .iter()
            .map(|s| s.name().unwrap().fullname(None))
            .collect();
        assert_eq!(names, vec!["com.example.A", "com.example.B"]);
    }

    // ---- recursion (SPEC §5.4) -------------------------------------------------

    /// A record whose field references itself must resolve without parse_list
    /// looping or rejecting the self-cycle.
    #[test]
    fn self_referential_record_resolves() {
        let node = r#"{
            "type": "record",
            "name": "Node",
            "namespace": "tree",
            "fields": [
                {"name": "value", "type": "int"},
                {"name": "next", "type": ["null", "tree.Node"], "default": null}
            ]
        }"#;
        let out = parse(&[node.to_string()], None).expect("self-ref must resolve");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name().unwrap().fullname(None), "tree.Node");
    }

    /// Two records each referencing the other (`A → B → A`) — neither can be
    /// parsed in isolation; only the merged-list pass can resolve them.
    #[test]
    fn mutually_recursive_records_resolve() {
        let a = r#"{
            "type": "record",
            "name": "A",
            "namespace": "m",
            "fields": [{"name": "b", "type": ["null", "m.B"], "default": null}]
        }"#;
        let b = r#"{
            "type": "record",
            "name": "B",
            "namespace": "m",
            "fields": [{"name": "a", "type": ["null", "m.A"], "default": null}]
        }"#;
        let out =
            parse(&[a.to_string(), b.to_string()], None).expect("mutual recursion must resolve");
        let names: Vec<String> = out
            .iter()
            .map(|s| s.name().unwrap().fullname(None))
            .collect();
        assert_eq!(names, vec!["m.A", "m.B"]);
    }

    // ---- per-type-kind registration (SPEC §10.1) -------------------------------

    /// A top-level `enum` schema must register under its FQN like any record.
    #[test]
    fn enum_registers_at_top_level() {
        let suit = r#"{
            "type": "enum",
            "name": "Suit",
            "namespace": "cards",
            "symbols": ["HEARTS", "DIAMONDS", "CLUBS", "SPADES"]
        }"#;
        let out = parse(&[suit.to_string()], None).expect("enum must register");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name().unwrap().fullname(None), "cards.Suit");
        assert!(matches!(out[0], Schema::Enum(_)));
    }

    /// A top-level `fixed` schema must register under its FQN.
    #[test]
    fn fixed_registers_at_top_level() {
        let md5 = r#"{"type":"fixed","name":"Md5","namespace":"crypto","size":16}"#;
        let out = parse(&[md5.to_string()], None).expect("fixed must register");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name().unwrap().fullname(None), "crypto.Md5");
        assert!(matches!(out[0], Schema::Fixed(_)));
    }

    /// A field typed as `array` of a named record must resolve the item ref
    /// against the registry.
    #[test]
    fn array_field_referencing_named_record_resolves() {
        let item = r#"{
            "type": "record",
            "name": "Item",
            "namespace": "store",
            "fields": [{"name": "sku", "type": "string"}]
        }"#;
        let basket = r#"{
            "type": "record",
            "name": "Basket",
            "namespace": "store",
            "fields": [{
                "name": "items",
                "type": {"type": "array", "items": "store.Item"}
            }]
        }"#;
        let out = parse(&[item.to_string(), basket.to_string()], None)
            .expect("array-of-named-record must resolve");
        assert_eq!(out.len(), 2);
    }

    /// A field typed as `map` of a named record must resolve the value ref.
    #[test]
    fn map_field_referencing_named_record_resolves() {
        let entry = r#"{
            "type": "record",
            "name": "Entry",
            "namespace": "kv",
            "fields": [{"name": "value", "type": "string"}]
        }"#;
        let bag = r#"{
            "type": "record",
            "name": "Bag",
            "namespace": "kv",
            "fields": [{
                "name": "by_key",
                "type": {"type": "map", "values": "kv.Entry"}
            }]
        }"#;
        let out = parse(&[entry.to_string(), bag.to_string()], None)
            .expect("map-of-named-record must resolve");
        assert_eq!(out.len(), 2);
    }

    /// A field typed as a union containing a named record must resolve the
    /// union's named arm against the registry.
    #[test]
    fn union_field_referencing_named_record_resolves() {
        let payload = r#"{
            "type": "record",
            "name": "Payload",
            "namespace": "wire",
            "fields": [{"name": "bytes", "type": "bytes"}]
        }"#;
        let envelope = r#"{
            "type": "record",
            "name": "Envelope",
            "namespace": "wire",
            "fields": [{
                "name": "body",
                "type": ["null", "wire.Payload"],
                "default": null
            }]
        }"#;
        let out = parse(&[payload.to_string(), envelope.to_string()], None)
            .expect("union-with-named-ref must resolve");
        assert_eq!(out.len(), 2);
    }

    // ---- denormalize across all structural positions ---------------------------

    /// Inline duplicate placed inside a `["null", T]` union variant must be
    /// rewritten to a string ref.
    #[test]
    fn denormalize_replaces_inline_dup_in_union_variant() {
        let b =
            r#"{"type":"record","name":"B","namespace":"n","fields":[{"name":"v","type":"int"}]}"#;
        let a_inline = r#"{
            "type": "record",
            "name": "A",
            "namespace": "n",
            "fields": [{
                "name": "maybe_b",
                "type": [
                    "null",
                    {"type":"record","name":"B","namespace":"n","fields":[{"name":"v","type":"int"}]}
                ],
                "default": null
            }]
        }"#;
        let out = parse(&[b.to_string(), a_inline.to_string()], None)
            .expect("denormalize must rewrite inline dup inside union");
        assert_eq!(out.len(), 2);
    }

    /// Inline duplicate placed as an array's `items` must be rewritten.
    #[test]
    fn denormalize_replaces_inline_dup_in_array_items() {
        let b =
            r#"{"type":"record","name":"B","namespace":"n","fields":[{"name":"v","type":"int"}]}"#;
        let a_inline = r#"{
            "type": "record",
            "name": "A",
            "namespace": "n",
            "fields": [{
                "name": "bs",
                "type": {
                    "type": "array",
                    "items": {"type":"record","name":"B","namespace":"n","fields":[{"name":"v","type":"int"}]}
                }
            }]
        }"#;
        let out = parse(&[b.to_string(), a_inline.to_string()], None)
            .expect("denormalize must rewrite inline dup inside array items");
        assert_eq!(out.len(), 2);
    }

    /// Inline duplicate placed as a map's `values` must be rewritten.
    #[test]
    fn denormalize_replaces_inline_dup_in_map_values() {
        let b =
            r#"{"type":"record","name":"B","namespace":"n","fields":[{"name":"v","type":"int"}]}"#;
        let a_inline = r#"{
            "type": "record",
            "name": "A",
            "namespace": "n",
            "fields": [{
                "name": "by_key",
                "type": {
                    "type": "map",
                    "values": {"type":"record","name":"B","namespace":"n","fields":[{"name":"v","type":"int"}]}
                }
            }]
        }"#;
        let out = parse(&[b.to_string(), a_inline.to_string()], None)
            .expect("denormalize must rewrite inline dup inside map values");
        assert_eq!(out.len(), 2);
    }

    // ---- namespace inheritance (SPEC §6) ---------------------------------------

    // A field whose type is an unqualified name must resolve against the
    // enclosing record's namespace (Avro spec §names).
    #[test]
    fn cross_record_unqualified_reference_inherits_enclosing_namespace() {
        let inner = r#"{
            "type": "record",
            "name": "Inner",
            "namespace": "outer.a",
            "fields": [{"name": "x", "type": "int"}]
        }"#;
        let outer = r#"{
            "type": "record",
            "name": "Outer",
            "namespace": "outer.a",
            "fields": [{"name": "i", "type": "Inner"}]
        }"#;
        let out = parse(&[inner.to_string(), outer.to_string()], None)
            .expect("unqualified ref must resolve via enclosing namespace");
        let names: Vec<String> = out
            .iter()
            .map(|s| s.name().unwrap().fullname(None))
            .collect();
        assert_eq!(names, vec!["outer.a.Inner", "outer.a.Outer"]);
    }

    // ---- canonical_equal known limitation --------------------------------------

    // Documents the v1 surface-syntax limitation noted in SPEC §10.2: two
    // declarations that differ only in whether the namespace is split out of
    // the `name` field are flagged as a conflict by sorted-keys equality.
    // When the limitation is tightened, flip this assertion to `expect(...)`.
    #[test]
    fn canonical_equal_known_false_positive_dotted_name_vs_split_namespace() {
        let split_ns = r#"{"type":"record","name":"R","namespace":"com.example","fields":[]}"#;
        let dotted_name = r#"{"type":"record","name":"com.example.R","fields":[]}"#;
        let err = parse(&[split_ns.to_string(), dotted_name.to_string()], None)
            .expect_err("v1 sorted-keys equality cannot see these as equivalent");
        assert!(matches!(err, GeneratorError::DuplicateName { .. }));
    }

    // ---- IDL surfaces (SPEC §9) ------------------------------------------------

    // ---- Apache Avro IDL corpus ------------------------------------------------
    //
    // Fixtures under `vanavro/tests/fixtures/idl/` are copied verbatim from the
    // Apache Avro Java test suite (see NOTICE there for provenance). Each test
    // below loads one fixture and asserts the named types it should produce —
    // grounded against the upstream reference so regressions are visible per
    // file.

    #[cfg(feature = "idl")]
    fn parse_fixture(rel: &str) -> Result<Vec<Schema>, GeneratorError> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/idl")
            .join(rel);
        parse(&[], Some(&path))
    }

    #[cfg(feature = "idl")]
    fn fqns(schemas: &[Schema]) -> Vec<String> {
        schemas
            .iter()
            .map(|s| s.name().unwrap().fullname(None))
            .collect()
    }

    /// `cycle.avdl` — two cycles: `SampleNode ↔ SamplePair` and `SelfRef → SelfRef`,
    /// plus a `Method` record referenced by `SamplePair` but in neither cycle.
    #[cfg(feature = "idl")]
    #[test]
    fn apache_corpus_cycle() {
        let names = fqns(&parse_fixture("cycle.avdl").expect("cycle.avdl parses"));
        for expected in [
            "org.apache.avro.gen.SampleNode",
            "org.apache.avro.gen.SamplePair",
            "org.apache.avro.gen.SelfRef",
            "org.apache.avro.gen.Method",
        ] {
            assert!(names.contains(&expected.to_string()), "missing {expected}");
        }
    }

    /// `namespaces.avdl` — a protocol whose decls each declare a distinct
    /// namespace via `@namespace(...)`, plus a record that references them all
    /// by fully-qualified name.
    #[cfg(feature = "idl")]
    #[test]
    fn apache_corpus_namespaces() {
        let names = fqns(&parse_fixture("namespaces.avdl").expect("namespaces.avdl parses"));
        for expected in [
            "avro.test.fixed.FixedInOtherNamespace",
            "avro.test.protocol.FixedInThisNamespace",
            "avro.test.record.RecordInOtherNamespace",
            "avro.test.error.ErrorInOtherNamespace",
            "avro.test.enum.EnumInOtherNamespace",
            "avro.test.protocol.RefersToOthers",
        ] {
            assert!(names.contains(&expected.to_string()), "missing {expected}");
        }
    }

    /// `union.avdl` — a record whose field is a union of two named records
    /// declared after the referring record (forward refs through a union).
    #[cfg(feature = "idl")]
    #[test]
    fn apache_corpus_union() {
        let names = fqns(&parse_fixture("union.avdl").expect("union.avdl parses"));
        for expected in [
            "org.apache.avro.gen.TestRecord",
            "org.apache.avro.gen.SR1",
            "org.apache.avro.gen.SR2",
        ] {
            assert!(names.contains(&expected.to_string()), "missing {expected}");
        }
    }

    /// `forward_ref.avdl` — record references an enum declared later in the
    /// protocol body. Inheritance of the enclosing `@namespace` must apply.
    #[cfg(feature = "idl")]
    #[test]
    fn apache_corpus_forward_ref() {
        let names = fqns(&parse_fixture("forward_ref.avdl").expect("forward_ref.avdl parses"));
        for expected in ["org.foo.ANameValue", "org.foo.ValueType"] {
            assert!(names.contains(&expected.to_string()), "missing {expected}");
        }
    }

    // `simple.avdl` — kitchen-sink protocol with records, enums, fixed,
    // error type, defaults, doc comments, and a `messages` block. SPEC §9.3
    // requires the `messages` to be parsed and silently ignored, with all
    // declared types still registered.
    #[cfg(feature = "idl")]
    #[test]
    fn apache_corpus_simple() {
        let names = fqns(&parse_fixture("simple.avdl").expect("simple.avdl parses"));
        for expected in [
            "org.apache.avro.test.Kind",
            "org.apache.avro.test.Status",
            "org.apache.avro.test.TestRecord",
            "org.apache.avro.test.MD5",
            "org.apache.avro.test.TestError",
        ] {
            assert!(names.contains(&expected.to_string()), "missing {expected}");
        }
    }

    // `logicalTypes.avdl` — a set of standard logical types plus a couple of
    // `@logicalType` annotations. The schema parser must accept them; codegen
    // will later map them per SPEC §3.3.
    #[cfg(feature = "idl")]
    #[test]
    fn apache_corpus_logical_types() {
        let names = fqns(&parse_fixture("logicalTypes.avdl").expect("logicalTypes.avdl parses"));
        assert!(
            names.contains(&"org.apache.avro.test.LogicalTypeFields".to_string()),
            "missing LogicalTypeFields, got {names:?}"
        );
    }

    /// `import_local.avdl` — trimmed variant of Apache's `import.avdl` that
    /// imports only the local `baz.avsc`, `foo.avsc`, and `bar.avpr`. Verifies
    /// IDL `import schema` / `import protocol` resolution through avdl into our
    /// registry.
    #[cfg(feature = "idl")]
    #[test]
    fn apache_corpus_import_local() {
        let names = fqns(&parse_fixture("import_local.avdl").expect("import_local.avdl parses"));
        for expected in ["ns.other.schema.Baz", "org.foo.Foo", "org.foo.Bar"] {
            assert!(names.contains(&expected.to_string()), "missing {expected}");
        }
    }

    // `optional_shorthand.avdl` — the IDL `T?` sugar (SPEC §9.4) for a union of
    // `null` with one other schema. Every field of `RecordWithUnion` — written
    // longhand as `union { null, string }`, with the `?` shorthand, with or
    // without a default — must lower to a 2-arm union of exactly `null` and
    // `string`. avdl preserves the *declared* arm order, so `string?` and
    // `union { null, string }` are null-first while `union { string, null }` is
    // string-first; the test asserts the arm *set*, not the order.
    #[cfg(feature = "idl")]
    #[test]
    fn apache_corpus_optional_shorthand() {
        let out = parse_fixture("optional_shorthand.avdl").expect("optional_shorthand.avdl parses");
        let rec = out
            .iter()
            .find_map(|s| match s {
                Schema::Record(r) if r.name.name() == "RecordWithUnion" => Some(r),
                _ => None,
            })
            .expect("RecordWithUnion present");
        assert_eq!(rec.fields.len(), 5, "five optional fields");
        for field in &rec.fields {
            let union = match &field.schema {
                Schema::Union(u) => u,
                other => panic!("field {} is not a union: {other:?}", field.name),
            };
            let arms = union.variants();
            assert_eq!(
                arms.len(),
                2,
                "field {} should be a 2-arm optional union, got {arms:?}",
                field.name
            );
            assert!(
                arms.iter().any(|v| matches!(v, Schema::Null)),
                "field {} should have a null arm, got {arms:?}",
                field.name
            );
            assert!(
                arms.iter().any(|v| matches!(v, Schema::String)),
                "field {} should have a string arm, got {arms:?}",
                field.name
            );
        }
    }

    // ---- multi-variant + non-nullable unions (SPEC §5.3) ----------------------

    // A union with 3 non-null variants must register and resolve (SPEC §5.3).
    #[test]
    fn multi_variant_non_null_union_with_three_arms_resolves() {
        let r = r#"{
            "type": "record",
            "name": "Mixed",
            "namespace": "u",
            "fields": [{"name": "value", "type": ["int", "string", "long"]}]
        }"#;
        let out = parse(&[r.to_string()], None).expect("3-arm union must parse");
        let rec = match &out[0] {
            Schema::Record(r) => r,
            other => panic!("expected record, got {other:?}"),
        };
        let union = match &rec.fields[0].schema {
            Schema::Union(u) => u,
            other => panic!("expected union, got {other:?}"),
        };
        assert_eq!(union.variants().len(), 3);
    }

    /// A 2-arm union without `null` is *not* the optional-T shape — it must
    /// still parse without being collapsed into an `Option`.
    #[test]
    fn non_nullable_two_arm_union_resolves() {
        let r = r#"{
            "type": "record",
            "name": "Either",
            "namespace": "u",
            "fields": [{"name": "value", "type": ["int", "string"]}]
        }"#;
        let out = parse(&[r.to_string()], None).expect("non-null 2-arm union must parse");
        let rec = match &out[0] {
            Schema::Record(r) => r,
            other => panic!("expected record, got {other:?}"),
        };
        let union = match &rec.fields[0].schema {
            Schema::Union(u) => u,
            other => panic!("expected union, got {other:?}"),
        };
        assert_eq!(union.variants().len(), 2);
        assert!(
            !union.variants().iter().any(|v| matches!(v, Schema::Null)),
            "no null arm"
        );
    }

    // ---- anonymous inline (SPEC §10.3) -----------------------------------------

    /// An inline named definition with no top-level twin must stay inline,
    /// not be promoted to the global registry.
    #[test]
    fn truly_anonymous_inline_record_stays_inline() {
        let outer = r#"{
            "type": "record",
            "name": "Outer",
            "namespace": "anon",
            "fields": [{
                "name": "child",
                "type": {
                    "type": "record",
                    "name": "Inner",
                    "fields": [{"name": "v", "type": "int"}]
                }
            }]
        }"#;
        let out = parse(&[outer.to_string()], None).expect("anonymous inline is valid");
        assert_eq!(out.len(), 1, "only Outer should be top-level");
        assert_eq!(out[0].name().unwrap().fullname(None), "anon.Outer");
        let rec = match &out[0] {
            Schema::Record(r) => r,
            other => panic!("expected record, got {other:?}"),
        };
        let inner = match &rec.fields[0].schema {
            Schema::Record(r) => r,
            other => panic!("inline child must remain a record, got {other:?}"),
        };
        assert_eq!(inner.name.fullname(None), "anon.Inner");
    }

    // ---- determinism (SPEC §14.2 for the parse stage) --------------------------

    /// Two `parse()` invocations on identical inputs must produce identical
    /// resolved schemas — guards against accidental `HashMap`-iteration leaks.
    #[test]
    fn parse_is_deterministic_across_runs() {
        let inputs = vec![
            r#"{"type":"record","name":"Gamma","namespace":"d","fields":[{"name":"x","type":"int"}]}"#
                .to_string(),
            r#"{"type":"record","name":"Alpha","namespace":"d","fields":[]}"#.to_string(),
            r#"{"type":"record","name":"Beta","namespace":"d","fields":[{"name":"y","type":"string"}]}"#
                .to_string(),
        ];
        let first = parse(&inputs, None).expect("first run");
        let second = parse(&inputs, None).expect("second run");
        assert_eq!(first.len(), second.len());
        for (a, b) in first.iter().zip(second.iter()) {
            assert_eq!(
                a.canonical_form(),
                b.canonical_form(),
                "schema diverged between runs"
            );
        }
    }

    // ---- error paths (SPEC §8.1) -----------------------------------------------

    /// Malformed JSON must surface as `SchemaParse` carrying an
    /// `apache_avro::Error` — not bare `serde_json::Error`.
    #[test]
    fn malformed_json_raises_schema_parse() {
        let bad = r#"{"type":"record","name":"Bad","fields":["#; // truncated
        let err = parse(&[bad.to_string()], None).expect_err("malformed JSON must error");
        match err {
            GeneratorError::SchemaParse { source, inner } => {
                assert_eq!(source, "<schemas[0]>");
                // Pin the variant so a future refactor that swaps the
                // serde_json round-trip for something weaker fails loudly.
                assert!(matches!(
                    inner.details(),
                    apache_avro::error::Details::ParseSchemaJson(_)
                ));
            }
            other => panic!("expected SchemaParse, got {other:?}"),
        }
    }

    /// An object without a top-level `name` field is not a valid named schema.
    #[test]
    fn missing_top_level_name_raises_unsupported_schema() {
        let bad = r#"{"type":"record","fields":[]}"#;
        let err = parse(&[bad.to_string()], None).expect_err("missing name must error");
        match err {
            GeneratorError::UnsupportedSchema { detail } => {
                assert!(detail.contains("name"), "detail mentions name: {detail}");
            }
            other => panic!("expected UnsupportedSchema, got {other:?}"),
        }
    }

    // SPEC §10.1: a named object whose `type` is not record / enum / fixed is
    // rejected, top-level or inside a top-level array, instead of yielding
    // empty output.
    #[test]
    fn non_named_type_with_name_raises_unsupported_schema() {
        for (bad, found) in [
            (
                r#"{"type":"array","name":"X","items":"int"}"#,
                r#""type": "array""#,
            ),
            (r#"{"type":"string","name":"X"}"#, r#""type": "string""#),
            (
                r#"[{"type":"map","name":"X","values":"int"}]"#,
                r#""type": "map""#,
            ),
            (
                r#"{"type":{"type":"record","name":"X","fields":[]},"name":"Y"}"#,
                "no string `type`",
            ),
            (r#"{"name":"X","fields":[]}"#, "no string `type`"),
        ] {
            let err = parse(&[bad.to_string()], None).expect_err("must error");
            match err {
                GeneratorError::UnsupportedSchema { detail } => {
                    assert!(detail.contains(found), "detail names the type: {detail}");
                    assert!(
                        detail.contains("record`, `enum` or `fixed"),
                        "detail says what is allowed: {detail}"
                    );
                }
                other => panic!("expected UnsupportedSchema for {bad}, got {other:?}"),
            }
        }
    }

    /// A `.avdl` file that fails to parse must surface as `IdlParse` with the
    /// source path embedded.
    #[cfg(feature = "idl")]
    #[test]
    fn idl_with_syntax_error_raises_idl_parse() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("broken.avdl");
        std::fs::write(&path, "this is not valid Avro IDL").expect("write");
        let err = parse(&[], Some(&path)).expect_err("malformed IDL must error");
        match err {
            GeneratorError::IdlParse { source, .. } => {
                assert!(
                    source.contains("broken.avdl"),
                    "source mentions filename: {source}"
                );
            }
            other => panic!("expected IdlParse, got {other:?}"),
        }
    }

    // ---- canonical_equal positive (complement to the known-limitation test) ----

    /// Two declarations of the same schema with reordered JSON keys must be
    /// recognised as identical and dedup silently.
    #[test]
    fn canonical_equal_accepts_key_reordered_identical_declarations() {
        let order_a =
            r#"{"type":"record","name":"R","namespace":"k","fields":[{"name":"x","type":"int"}]}"#;
        let order_b =
            r#"{"namespace":"k","fields":[{"type":"int","name":"x"}],"name":"R","type":"record"}"#;
        let out = parse(&[order_a.to_string(), order_b.to_string()], None)
            .expect("key-reordered identical schemas must dedup");
        assert_eq!(out.len(), 1);
    }

    // ---- content fidelity ------------------------------------------------------

    /// Doc strings, aliases, and field defaults must survive the
    /// JSON-roundtrip through our pipeline into `apache_avro::Schema`.
    #[test]
    fn doc_aliases_and_defaults_survive_to_parsed_schema() {
        let r = r#"{
            "type": "record",
            "name": "Fidelity",
            "namespace": "f",
            "doc": "Top-level doc.",
            "aliases": ["LegacyFidelity"],
            "fields": [{
                "name": "count",
                "doc": "How many.",
                "type": "int",
                "default": 7
            }]
        }"#;
        let out = parse(&[r.to_string()], None).expect("parse");
        let rec = match &out[0] {
            Schema::Record(r) => r,
            other => panic!("expected record, got {other:?}"),
        };
        assert_eq!(rec.doc.as_deref(), Some("Top-level doc."));
        let alias_names: Vec<String> = rec
            .aliases
            .as_ref()
            .expect("aliases preserved")
            .iter()
            .map(|a| a.fullname(None))
            .collect();
        assert!(
            alias_names.iter().any(|a| a.ends_with("LegacyFidelity")),
            "got aliases: {alias_names:?}"
        );

        let field = &rec.fields[0];
        assert_eq!(field.name, "count");
        assert_eq!(field.doc.as_deref(), Some("How many."));
        assert_eq!(
            field.default.as_ref().and_then(|v| v.as_i64()),
            Some(7),
            "default value preserved"
        );
    }

    /// Every IDL logical-type keyword in `logicalTypes.avdl` must lower to the
    /// matching `apache_avro::Schema` variant — proves the annotations survive
    /// the avdl → JSON → Schema lowering.
    #[cfg(feature = "idl")]
    #[test]
    fn logical_types_lower_to_specific_schema_variants() {
        use std::collections::HashMap;
        let out = parse_fixture("logicalTypes.avdl").expect("logicalTypes.avdl parses");
        let rec = out
            .iter()
            .find_map(|s| match s {
                Schema::Record(r)
                    if r.name.fullname(None) == "org.apache.avro.test.LogicalTypeFields" =>
                {
                    Some(r)
                }
                _ => None,
            })
            .expect("LogicalTypeFields record");
        let by_name: HashMap<&str, &Schema> = rec
            .fields
            .iter()
            .map(|f| (f.name.as_str(), &f.schema))
            .collect();

        assert!(matches!(by_name["aDate"], Schema::Date));
        assert!(matches!(by_name["aTime"], Schema::TimeMillis));
        assert!(matches!(by_name["aTimestamp"], Schema::TimestampMillis));
        assert!(matches!(
            by_name["aLocalTimestamp"],
            Schema::LocalTimestampMillis
        ));
        assert!(matches!(by_name["pocketMoney"], Schema::Decimal(_)));
        assert!(matches!(by_name["identifier"], Schema::Uuid(_)));
        assert!(matches!(
            by_name["anotherTimestamp"],
            Schema::TimestampMicros
        ));
        assert!(matches!(by_name["allowance"], Schema::Decimal(_)));
    }

    // IDL input flows through `avdl::Idl2Schemata` into the same registry as
    // JSON inputs and merges cleanly (SPEC §9 + §10).
    #[cfg(feature = "idl")]
    #[test]
    fn idl_input_merges_with_json_inputs() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let avdl_path = tmp.path().join("greetings.avdl");
        std::fs::write(
            &avdl_path,
            r#"
                @namespace("com.example.greet")
                protocol Greetings {
                    record Hello {
                        string subject;
                    }
                }
            "#,
        )
        .expect("write .avdl fixture");

        let extra_json = r#"{
            "type": "record",
            "name": "Goodbye",
            "namespace": "com.example.greet",
            "fields": [{"name": "subject", "type": "string"}]
        }"#;

        let out =
            parse(&[extra_json.to_string()], Some(&avdl_path)).expect("IDL + JSON merge succeeded");
        let names: Vec<String> = out
            .iter()
            .map(|s| s.name().unwrap().fullname(None))
            .collect();
        assert_eq!(
            names,
            vec!["com.example.greet.Goodbye", "com.example.greet.Hello"]
        );
    }
}
