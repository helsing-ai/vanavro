//! Walk a resolved `Vec<apache_avro::Schema>` and emit a single formatted
//! Rust source file mapping each named Avro type to a Rust definition.
//!
//! Implements:
//! - Records → `pub struct` with serde derives
//! - Enums (unit symbols) → `pub enum` with full derive set + `#[non_exhaustive]`
//! - Fixed → `pub type X = [u8; N];`
//! - Primitives, `Option<T>`, `Vec<T>`, `HashMap<String, T>`, named refs via
//!   `super::…` relative paths
//! - Bytes / fixed serde helpers and their `_opt` flavours
//! - Logical types: date / time / timestamp variants ride their
//!   underlying primitive; `uuid` → `::uuid::Uuid`; `decimal` → `::apache_avro::Decimal`;
//!   `duration` → `::apache_avro::Duration`
//! - Recursion: every record reference that participates in a
//!   direct + `Option` cycle is wrapped in `Box`. Cycles through `Vec` /
//!   `HashMap` are left alone since those types already heap-allocate.
//!   Note: v1 over-boxes — a minimal feedback-arc set would box one edge per
//!   cycle, but computing one is NP-hard; we accept the extra indirection.
//! - Namespaces → nested `pub mod` tree
//! - Keyword escaping: strict + reserved Rust keywords get the
//!   raw-identifier prefix (`r#…`); `self` / `Self` / `super` / `crate`
//!   take a trailing-underscore suffix; serde rename is emitted whenever
//!   escaping or case conversion changes the wire name
//! - Multi-variant unions: non-optional unions generate a
//!   sibling `pub enum`, one variant per arm, `#[serde(rename)]` per Avro tag.
//!   Named by the `"enum"` attribute on the *record field* (`{Record}Union`
//!   default if absent), which propagates inward through arrays / maps /
//!   optional wrappers; the attribute anywhere else is rejected, as is a
//!   nested union left with no name. A field may hold at most one
//!   enum-generating union; optional `[null, T]` unions generate none and are
//!   exempt. Names join the namespace-wide collision pass.
//! - Single-union-record collapse (vanavro extension): a record
//!   whose only field is a *multi-variant* union collapses to a single public
//!   enum that takes the record's own name `{Record}` (no wrapping struct),
//!   serialized as the original 1-field record with zero extra allocation
//!   (`#[serde(try_from)]` + a borrowing `Serialize` impl). References
//!   and recursive self-references resolve to `{Record}` naturally. See
//!   `emit_collapsed_union_record`. The collapse is on by default but gated on
//!   `CodegenOptions::collapse_single_union` (CLI `--no-collapse-single-union`);
//!   when disabled the record keeps its ordinary struct + `{Record}Union` shape.
//! - Optional single-field constructors (vanavro extension): a *separate*,
//!   minor helper for the uncommon `[null, T]` single-field shape, which stays
//!   a struct over `Option<T>` and gains fluent `Some`/`null` constructors via
//!   `emit_union_constructors`.
//! - Deterministic output via `BTreeMap` ordering on namespaces
//! - Doc comments: record / enum / fixed / field `doc` strings
//!   render as one outer `#[doc = " …"]` per line so `prettyplease` emits
//!   `///` comments
//! - Aliases: `#[serde(alias = "…")]` per alias on record fields
//!   only. Record/enum *type* aliases and fixed aliases are dropped — serde's
//!   `alias` is a field/variant attribute, not a container one, and a type
//!   name has no wire representation anyway; these aliases survive in the
//!   structural `Schema` exposed via `T::schema()` (or the sibling
//!   `{snake}_schema()` fn for a fixed).
//! - Default values: `null`/`[]`/`{}` lean on `#[serde(default)]`;
//!   scalar / non-empty literals (including record-object literals, lowered to
//!   struct literals) get `#[serde(default = "default_<rec>_<field>")]` plus a
//!   `pub(super)` helper fn. Decimal / uuid / duration literal defaults are not
//!   lowered in v1 and raise `UnsupportedSchema`.
//!
//! - Eq + Hash uplift: a record `struct` additionally derives
//!   `Eq, Hash` when every field's Rust type is itself `Eq + Hash`. Computed
//!   as a cycle-safe greatest fixpoint over the record graph
//!   (`compute_eq_hash_records`). Float/double, `HashMap` (map fields),
//!   `Decimal`/`BigDecimal`/`Duration`, and multi-variant union enum fields
//!   disqualify the owning record (`BigDecimal` conservatively: it does
//!   implement `Hash`).
//!
//! - `bon::Builder` opt-in: when `CodegenOptions::builder` is set
//!   (CLI `--builder`), record structs additionally derive `::bon::Builder`.
//!   `String` / `Option<String>` fields gain `#[builder(into)]` for ergonomic
//!   `impl Into<String>` setters; fields with an Avro default reuse the
//!   `default_<rec>_<field>` machinery via `#[builder(default = …)]` (bare
//!   `#[builder(default)]` for `[]` / `{}`). A custom-logical field whose
//!   mapping registered `builder_with` gains `#[builder(with =
//!   …)]` with the text emitted verbatim. All present parts (`into` / `with` /
//!   `default`) merge into a single `#[builder(…)]`; `into` and `with` are
//!   mutually exclusive by construction (only custom-logical fields carry
//!   `with`, and those never receive the String `into`).
//!     - Known gap: bon rejects `#[builder(default)]` on `Option<T>` fields
//!       (`Option` already implies `None`), so a *non-null* Avro default on an
//!       optional field is honoured by serde-deserialization but **not** by
//!       the builder, which yields `None`. `into` still applies to optional
//!       `String`.
//!
//! - Post-conversion collision detection: before any tokens are
//!   emitted, `check_name_collisions` fails clean with
//!   [`GeneratorError::NameCollision`] when two distinct Avro names fold onto
//!   the same Rust identifier after case conversion / keyword escaping —
//!   across record fields, enum symbols, per-namespace type names, and
//!   namespace-sibling module segments — and between a namespace's type names
//!   and its own child module segments, which Rust resolves in one namespace.
//!   (Union variant collisions are exempt; they self-heal via the
//!   `Variant{N}` fallback.)
//!
//! - Per-type API: every record / unit enum / union enum
//!   gets `impl T { fn schema() -> &'static Schema; fn fingerprint<D: Digest>()
//!   -> SchemaFingerprint; }`; fixed type aliases get sibling free fns
//!   `{snake}_schema()` / `{snake}_fingerprint::<D>()`. `schema()` is backed by
//!   a `LazyLock` whose initializer builds the `apache_avro::Schema`
//!   **structurally** (no runtime string parsing).
//!   The schema is denormalised/self-contained: each referenced named type is
//!   defined in full on first occurrence and back-referenced via `Schema::Ref`
//!   thereafter (so recursion terminates and `fingerprint()` matches the
//!   canonical form of the equivalent parsed schema). To keep generated output
//!   O(N) rather than O(N^2), this construction is factored into one runtime
//!   builder fn per named type, emitted **inside that type's own namespace
//!   module** beside the generated type (see `emit_schema_builder`); a builder
//!   symbol therefore collides across two `include!`d outputs exactly when the
//!   module-scoped types already would, never independently
//!   (bar a referenced suppressed record, which has a builder but no type).
//!   Each `schema()` calls its type's builder — bare when co-located, otherwise
//!   through the relative-module path used for type references — threading
//!   a fresh `HashSet<Name>` "defined" set that drives the first-occurrence/
//!   back-reference decision at run time. The materialised `Schema` (and its
//!   fingerprint) is identical to inlining it. Field defaults and
//!   custom attributes are preserved as `::serde_json::Value` literals, so
//!   downstream consumers of `schema()` must depend on `serde_json` (and on
//!   `digest` for `fingerprint`). Records / unit enums / union enums also
//!   `impl ::apache_avro::AvroSchema` (`get_schema()` clones the static), for
//!   idiomatic integration with `Writer` and the derive ecosystem; fixed type
//!   aliases keep only the sibling fns (a `pub type` can't host an `impl`).
//! - Formatting: `prettyplease` output is post-processed to insert a single
//!   blank line between sibling items (type defs, `impl` blocks) for
//!   readability (`space_sibling_items`).
//!
//! Unsupported schema shapes raise [`GeneratorError::UnsupportedSchema`].

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};

use apache_avro::Schema;
use apache_avro::schema::{
    EnumSchema, FixedSchema, Name, NamespaceRef, RecordField, RecordSchema, UnionSchema,
};
use heck::{ToPascalCase, ToSnakeCase};
use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::quote;

use crate::GeneratorError;
use crate::logical::{self, LogicalTypeRegistry, RawNameTable};

/// Knobs that the CLI / builder API exposes to control emitted code shape.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CodegenOptions {
    /// `true` keeps the default of `#[non_exhaustive]` on every
    /// generated unit enum; `false` strips it (CLI `--no-non-exhaustive`).
    pub non_exhaustive: bool,
    /// `true` emits `#[derive(::bon::Builder)]` plus per-field
    /// `#[builder(into / default)]` attributes on record structs
    /// (CLI `--builder`).
    pub builder: bool,
    /// `true` keeps the default of collapsing a single-field
    /// enum-generating-union record into a bare public enum; `false` keeps the
    /// record as an ordinary struct with a sibling `{Record}Union` enum
    /// (CLI `--no-collapse-single-union`).
    pub collapse_single_union: bool,
    /// `true` tolerates a `logicalType` that matches no standard logical type
    /// and no registered custom mapping, lowering the field to its base type
    /// instead of raising [`GeneratorError::UnknownLogicalType`]
    /// (CLI `--allow-unregistered-logical-types`). Defaults to `false`.
    pub allow_unregistered_logical_types: bool,
}

/// Shared, read-only context for custom logical types: the
/// caller-supplied registry, the raw-JSON name table for recovering
/// annotations `apache-avro` drops on primitives, and the set of *named
/// records* carrying a top-level `logicalType` whose
/// struct generation is suppressed in favour of the mapped Rust type.
#[derive(Clone, Copy)]
pub(crate) struct LogicalCtx<'a> {
    registry: &'a LogicalTypeRegistry,
    raw_names: &'a RawNameTable,
    /// Records mapped to a Rust type by a record-level `logicalType`; their
    /// `Name` ⇒ the mapping. References to these become the mapped type and
    /// the records themselves are never emitted.
    suppressed_records: &'a HashMap<Name, logical::LogicalTypeMapping>,
    /// When `true`, an unregistered non-standard `logicalType` is tolerated and
    /// the field lowers to its base type instead of raising
    /// [`GeneratorError::UnknownLogicalType`].
    allow_unregistered: bool,
    /// Union-enum collision pre-pass sink. When `Some`, each union enum resolved
    /// by [`union_type`] records its namespace + Rust ident here for
    /// [`check_name_collisions`]. `None` during real emission.
    union_sink: Option<&'a RefCell<Vec<UnionTypeInfo>>>,
}

impl LogicalCtx<'_> {
    /// Whether any custom logical type is registered. When `false`, all
    /// recovery is skipped so codegen without custom logical types is byte-for-byte unchanged.
    fn active(&self) -> bool {
        !self.registry.is_empty()
    }

    /// The record-level mapping for a named record, if its struct is
    /// suppressed.
    fn suppressed_record(&self, name: &Name) -> Option<&logical::LogicalTypeMapping> {
        self.suppressed_records.get(name)
    }
}

/// Walk the resolved schemas and return formatted single-file Rust source. The
/// unit tests' entry point; the public API goes through [`generate_file`].
#[cfg(test)]
pub(crate) fn generate(
    schemas: &[Schema],
    raw_names: &RawNameTable,
    logical_types: &LogicalTypeRegistry,
    opts: CodegenOptions,
) -> Result<String, GeneratorError> {
    let file = generate_file(schemas, raw_names, logical_types, opts)?;
    Ok(format_file(&file))
}

/// Walk the resolved schemas and return the generated output as a single
/// unformatted `syn::File`, namespace modules inline.
pub(crate) fn generate_file(
    schemas: &[Schema],
    raw_names: &RawNameTable,
    logical_types: &LogicalTypeRegistry,
    opts: CodegenOptions,
) -> Result<syn::File, GeneratorError> {
    // Flatten top-level schemas plus every *inline* named type (anonymous
    // records / enums / fixed declared inside a field, array, map, union, …)
    // into one list of named definitions. Without this, a field typed by an
    // inline record references a struct path (`pub child: Inner`) whose
    // definition is never emitted — a dangling reference that won't compile
    // downstream. Each inline named type carries its own FQN (it inherits the
    // enclosing namespace at parse time), so it slots into the module tree and
    // registry exactly like a top-level definition.
    let all_named = collect_named_schemas(schemas);

    // SPEC §15.3 case 2: a *named record* carrying a top-level `logicalType`
    // that matches a registered mapping is not generated at all — every
    // reference to it becomes the mapped Rust type. Compute that set first so
    // the records are dropped from emission and resolved at reference sites.
    let suppressed_records = collect_suppressed_records(&all_named, logical_types, raw_names);

    let logical = LogicalCtx {
        registry: logical_types,
        raw_names,
        suppressed_records: &suppressed_records,
        allow_unregistered: opts.allow_unregistered_logical_types,
        union_sink: None,
    };

    // The emitted definitions: every named type except the suppressed records.
    let named: Vec<&Schema> = all_named
        .iter()
        .copied()
        .filter(|s| match s.name() {
            Some(n) => !suppressed_records.contains_key(n),
            None => true,
        })
        .collect();

    // Build a lookup so `Schema::Ref { name }` field positions can resolve to
    // their target schema (needed e.g. to emit the `serde::fixed` helper
    // on fields that reference a top-level Fixed). The registry keeps *all*
    // named types (including suppressed records) so refs still resolve.
    let registry: HashMap<Name, &Schema> = all_named
        .iter()
        .filter_map(|s| s.name().map(|n| (n.clone(), *s)))
        .collect();

    // SPEC §5.3: a record field may hold at most one enum-generating union, and
    // may carry the `"enum"` union-name attribute only on the field itself.
    // Checked over `all_named`, not the emission set — both rules hold even for
    // a record §15.3 suppresses.
    check_field_union_rules(&all_named)?;

    // SPEC §5.3: resolve every extracted union enum's namespace + Rust ident for
    // the collision pass below.
    let union_types = collect_union_types(&named, &registry, opts, logical)?;

    // SPEC §5.2 / §5.3: fail clean if case conversion / keyword escaping folds
    // two distinct Avro names onto the same Rust identifier, rather than
    // emitting unparseable Rust downstream.
    check_name_collisions(&named, &union_types, opts, logical)?;

    // SPEC §11.1 / §15: reject a `force_eq_hash` mapping used where the field is
    // a `HashMap` (map value), which can never be `Hash` — the flag would be
    // silently ineffective there, so surface it instead.
    check_force_eq_hash_shapes(&named, logical)?;

    let cycle_partners = build_cycle_partners(&named, &registry);
    let eq_hash = compute_eq_hash_records(&named, &registry, logical);

    // SPEC §12.1: emit one shared `build_*` schema-builder fn per named type, so
    // the generated output is O(N), not O(N²). Emit a builder only for a type
    // reachable from an emitted type (an unreferenced suppressed record has no
    // Rust type and needs none — see `reachable_builders`) and place it in its
    // own namespace module (via `build_namespace_tree` / `emit_module_body`), so
    // a builder symbol collides across two `include!`d outputs exactly when the
    // module-scoped types already would — never independently of them, bar a
    // referenced suppressed record (builder without a type, SPEC §12.1).
    let reachable = reachable_builders(&named, &registry);
    let tree = build_namespace_tree(&named, &reachable)?;
    let body = emit_module_body(
        &tree,
        &[],
        opts,
        &registry,
        &cycle_partners,
        &eq_hash,
        logical,
    )?;

    // No module-level `#![allow(...)]` is emitted: the output is meant to be
    // `include!`d, and the including module applies whatever lint attributes
    // it needs at its own site.
    syn::parse2(body).map_err(GeneratorError::Format)
}

/// Pretty-print a generated file with blank lines between sibling items.
pub(crate) fn format_file(file: &syn::File) -> String {
    space_sibling_items(&prettyplease::unparse(file))
}

/// `prettyplease` packs sibling items with no blank lines (e.g. a `struct`
/// immediately followed by its `impl` blocks). Insert a single blank line
/// between adjacent sibling items for readability.
///
/// The rule is indentation-aware: a blank line is inserted before a line that
/// *starts a new item* (an `impl` / `pub` declaration, or its leading
/// attribute / doc comment) when the previous line is a lone closing brace at
/// the **same** indentation — i.e. the two lines are siblings, not a field /
/// statement nested inside the item that just closed. This never splits struct
/// fields or `impl`-body statements, which sit at a deeper indent under a
/// non-`}` previous line.
fn space_sibling_items(src: &str) -> String {
    let lines: Vec<&str> = src.lines().collect();
    let mut out: Vec<String> = Vec::with_capacity(lines.len());

    for (i, &line) in lines.iter().enumerate() {
        if i > 0 {
            let prev = lines[i - 1];
            // A split output's `pub mod x;` declarations (SPEC §8.3) end an
            // item just like a closing brace does.
            let ends_item = prev.trim() == "}"
                || (prev.trim_start().starts_with("pub mod ") && prev.ends_with(';'));
            if starts_new_item(line) && ends_item && indent_width(line) == indent_width(prev) {
                out.push(String::new());
            }
        }
        out.push(line.to_string());
    }

    let mut joined = out.join("\n");
    if src.ends_with('\n') {
        joined.push('\n');
    }
    joined
}

fn indent_width(line: &str) -> usize {
    line.len() - line.trim_start().len()
}

/// Whether a (formatted) line begins a new sibling item or the leading
/// attribute / doc comment that introduces one.
fn starts_new_item(line: &str) -> bool {
    let t = line.trim_start();
    t.starts_with("impl ")
        || t.starts_with("impl<")
        || t.starts_with("pub struct ")
        || t.starts_with("pub enum ")
        || t.starts_with("pub type ")
        || t.starts_with("pub fn ")
        || t.starts_with("pub mod ")
        || t.starts_with("pub(super) fn ")
        || t.starts_with("#[derive")
        || t.starts_with("#[non_exhaustive")
        || t.starts_with("#[serde")
        || t.starts_with("#[doc")
        || t.starts_with("#[path")
        || t.starts_with("///")
}

// =============================================================================
// Namespace tree
// =============================================================================

/// Tree of nested namespaces; each node holds the schemas declared at that
/// exact namespace, the schema-builder fns to emit there, plus children keyed
/// by the next segment.
#[derive(Debug, Default)]
struct ModuleNode<'a> {
    schemas: Vec<&'a Schema>,
    /// Names whose `build_*` schema-builder fn is emitted in this module (see
    /// [`emit_schema_builder`]). Builders live in their type's own namespace
    /// module so a `build_*` fn collides across two `include!`d outputs exactly
    /// when the module-scoped types would — never independently of them, bar a
    /// referenced suppressed record.
    builders: Vec<Name>,
    children: BTreeMap<String, ModuleNode<'a>>,
}

/// Flatten the top-level schemas plus every inline named definition (record /
/// enum / fixed declared inside a field, array, map, union, or decimal-inner
/// position) into a single deterministic list of named definitions.
///
/// `apache_avro::Schema::parse_list` resolves the *second* textual occurrence
/// of a name into a `Schema::Ref`, so each inline named type is defined exactly
/// once in the tree; back-references are `Schema::Ref` and contribute nothing
/// here. The schema layer (`schema::denormalize_registry`) further rewrites
/// any inline child whose FQN matches a top-level definition into a string ref,
/// so a name never appears both top-level and inline. The FQN dedup below is a
/// belt-and-braces guard that keeps the first occurrence in traversal order.
fn collect_named_schemas(schemas: &[Schema]) -> Vec<&Schema> {
    let mut out: Vec<&Schema> = Vec::new();
    let mut seen: HashSet<Name> = HashSet::new();
    for schema in schemas {
        collect_named_into(schema, &mut out, &mut seen);
    }
    out
}

fn collect_named_into<'a>(schema: &'a Schema, out: &mut Vec<&'a Schema>, seen: &mut HashSet<Name>) {
    match schema {
        Schema::Record(rec) => {
            push_named_if_new(schema, &rec.name, out, seen);
            for field in &rec.fields {
                collect_named_into(&field.schema, out, seen);
            }
        }
        Schema::Enum(en) => push_named_if_new(schema, &en.name, out, seen),
        Schema::Fixed(fx) => push_named_if_new(schema, &fx.name, out, seen),
        Schema::Array(arr) => collect_named_into(&arr.items, out, seen),
        Schema::Map(map) => collect_named_into(&map.types, out, seen),
        Schema::Union(u) => {
            for variant in u.variants() {
                collect_named_into(variant, out, seen);
            }
        }
        // A decimal's backing fixed lives in `InnerDecimalSchema::Fixed`, which
        // is no longer a standalone `Schema` node, so it introduces no
        // separately-named definition to collect here.
        // Refs and primitives/logical types introduce no new definitions.
        _ => {}
    }
}

/// Names whose schema-builder fn must be emitted: every emitted (non-suppressed)
/// named type, plus the transitive closure of named types their schemas
/// reference. A *referenced* suppressed record is included — its full definition
/// is still preserved in the embedded `schema()` — but a suppressed
/// record that nothing references produces no Rust type **and** no builder, so
/// it cannot introduce a symbol that collides across two `include!`d outputs.
fn reachable_builders(named: &[&Schema], registry: &HashMap<Name, &Schema>) -> HashSet<Name> {
    let mut reachable: HashSet<Name> = HashSet::new();
    let mut stack: Vec<Name> = named.iter().filter_map(|s| s.name().cloned()).collect();
    while let Some(name) = stack.pop() {
        if !reachable.insert(name.clone()) {
            continue;
        }
        if let Some(schema) = registry.get(&name) {
            collect_deps(schema, &mut stack);
        }
    }
    reachable
}

/// Push the names of every named type reachable through one type's *immediate*
/// body, without descending past any named boundary. A record's own fields are
/// the only place a fresh subtree hangs off a named node, so only records have
/// a body to walk; every dependency they name is resolved later when the caller
/// pops that name and calls back in through the registry. Keeping this to a
/// single level is what makes `reachable_builders` linear in the number of
/// named types rather than quadratic on deeply-nested schemas.
fn collect_deps(schema: &Schema, out: &mut Vec<Name>) {
    if let Schema::Record(rec) = schema {
        for field in &rec.fields {
            collect_ref_names(&field.schema, out);
        }
    }
}

/// Push the names of every named type (record / enum / fixed / bare ref)
/// referenced anywhere in `schema`, recursing through collections and unions.
/// Bodies are not followed past a named node — the name is pushed and the caller
/// resolves it through the registry (see [`collect_deps`]).
fn collect_ref_names(schema: &Schema, out: &mut Vec<Name>) {
    match schema {
        Schema::Record(rec) => out.push(rec.name.clone()),
        Schema::Enum(en) => out.push(en.name.clone()),
        Schema::Fixed(fx) => out.push(fx.name.clone()),
        Schema::Array(arr) => collect_ref_names(&arr.items, out),
        Schema::Map(map) => collect_ref_names(&map.types, out),
        Schema::Union(u) => {
            for variant in u.variants() {
                collect_ref_names(variant, out);
            }
        }
        Schema::Ref { name } => out.push(name.clone()),
        // Decimal / duration backing-fixeds are not standalone registry
        // entries (see [`collect_named_into`]), and primitives introduce no refs.
        _ => {}
    }
}

/// Record `schema` under `name` unless an earlier traversal already claimed
/// that FQN (keeps the first occurrence in traversal order).
fn push_named_if_new<'a>(
    schema: &'a Schema,
    name: &Name,
    out: &mut Vec<&'a Schema>,
    seen: &mut HashSet<Name>,
) {
    if seen.insert(name.clone()) {
        out.push(schema);
    }
}

/// Collect named records whose top-level `logicalType`
/// (recovered from raw JSON) matches a registered mapping. Such records are
/// not emitted; every reference resolves to the mapped Rust type.
///
/// A mismatched `underlying_type` (mapping registered for a non-`record`
/// shape) is ignored here — the record falls through to normal generation —
/// because the `LogicalTypeRegistry` keys only on the `logicalType` string and
/// the record position is the authority for the `record` shape.
fn collect_suppressed_records(
    named: &[&Schema],
    registry: &LogicalTypeRegistry,
    raw_names: &RawNameTable,
) -> HashMap<Name, logical::LogicalTypeMapping> {
    let mut out = HashMap::new();
    if registry.is_empty() {
        return out;
    }
    for &schema in named {
        let Schema::Record(rec) = schema else {
            continue;
        };
        let fqn = fqn_string(&rec.name);
        if let Some(lt) = logical::get_record_logical_type(raw_names, &fqn)
            && let Some(mapping) = registry.get(lt)
            && mapping.underlying_type == logical::AvroType::Record
        {
            out.insert(rec.name.clone(), mapping.clone());
        }
    }
    out
}

fn build_namespace_tree<'a>(
    schemas: &[&'a Schema],
    reachable: &HashSet<Name>,
) -> Result<ModuleNode<'a>, GeneratorError> {
    let mut root = ModuleNode::default();
    for schema in schemas {
        let name = schema
            .name()
            .ok_or_else(|| GeneratorError::UnsupportedSchema {
                detail: "top-level schema must be named (record / enum / fixed)".into(),
            })?;
        node_for_namespace(&mut root, name).schemas.push(*schema);
    }

    // Place each reachable type's schema-builder fn in its own namespace module
    // (SPEC §12.1). Sorted by FQN so the emitted builder order is deterministic
    // (SPEC §11.7) regardless of `HashSet` iteration order.
    let mut builder_names: Vec<&Name> = reachable.iter().collect();
    builder_names.sort_by_cached_key(|n| n.fullname(None));
    for name in builder_names {
        node_for_namespace(&mut root, name)
            .builders
            .push(name.clone());
    }
    Ok(root)
}

/// Descend into (creating as needed) the tree node for `name`'s namespace.
fn node_for_namespace<'a, 'b>(root: &'b mut ModuleNode<'a>, name: &Name) -> &'b mut ModuleNode<'a> {
    let mut node = root;
    for seg in ns_segments(name.namespace()) {
        node = node.children.entry(seg).or_default();
    }
    node
}

fn ns_segments<'a>(ns: NamespaceRef<'a>) -> Vec<String> {
    match ns {
        Some(s) if !s.is_empty() => s.split('.').map(str::to_string).collect(),
        _ => Vec::new(),
    }
}

// =============================================================================
// Module emission
// =============================================================================

#[allow(clippy::too_many_arguments)]
fn emit_module_body<'a>(
    node: &ModuleNode<'a>,
    current_ns: &[String],
    opts: CodegenOptions,
    registry: &HashMap<Name, &'a Schema>,
    cycle_partners: &CyclePartners,
    eq_hash: &EqHashRecords,
    logical: LogicalCtx<'_>,
) -> Result<TokenStream, GeneratorError> {
    let mut items: Vec<TokenStream> = Vec::new();

    for schema in &node.schemas {
        items.push(emit_named_type(
            schema,
            current_ns,
            opts,
            registry,
            cycle_partners,
            eq_hash,
            logical,
        )?);
    }

    // SPEC §12.1: the shared `build_*` schema-builder fns for types declared at
    // this namespace. They live beside their type so a builder symbol collides
    // across two `include!`d outputs exactly when the module-scoped types do.
    for name in &node.builders {
        items.push(emit_schema_builder(name, registry, current_ns)?);
    }

    for (seg, child) in &node.children {
        let mut child_ns = current_ns.to_vec();
        child_ns.push(seg.clone());
        let child_body = emit_module_body(
            child,
            &child_ns,
            opts,
            registry,
            cycle_partners,
            eq_hash,
            logical,
        )?;
        let mod_ident = rust_module_ident(seg);
        items.push(quote! {
            pub mod #mod_ident {
                #child_body
            }
        });
    }

    Ok(quote! { #( #items )* })
}

#[allow(clippy::too_many_arguments)]
fn emit_named_type<'a>(
    schema: &'a Schema,
    current_ns: &[String],
    opts: CodegenOptions,
    registry: &HashMap<Name, &'a Schema>,
    cycle_partners: &CyclePartners,
    eq_hash: &EqHashRecords,
    logical: LogicalCtx<'_>,
) -> Result<TokenStream, GeneratorError> {
    match schema {
        Schema::Record(rec) => emit_record(
            rec,
            current_ns,
            opts,
            registry,
            cycle_partners,
            eq_hash,
            logical,
        ),
        Schema::Enum(en) => emit_enum(en, opts, registry),
        Schema::Fixed(fx) => emit_fixed(fx, registry),
        other => Err(GeneratorError::UnsupportedSchema {
            detail: format!("top-level schema must be record / enum / fixed, got {other:?}"),
        }),
    }
}

// =============================================================================
// Record emission
// =============================================================================

/// Structural pre-pass over every named record's fields, enforcing
/// both union placement rules in one traversal:
///
/// 1. a field holds at most one *enum-generating* union, and
/// 2. the `"enum"` union-name attribute sits only on the record field.
///
/// Runs over *every* named record — suppressed ones included — since both
/// rules are properties of the schema, not of what gets emitted. Runs ahead of
/// the lowering passes so an over-budget nested union is not reported by
/// [`collect_union_types`] as merely unnamable.
fn check_field_union_rules(schemas: &[&Schema]) -> Result<(), GeneratorError> {
    for &schema in schemas {
        let Schema::Record(rec) = schema else {
            continue;
        };
        for field in &rec.fields {
            let mut unions = 0;
            walk_field_union_rules(&field.schema, &mut unions)?;
            if unions > 1 {
                return Err(GeneratorError::UnsupportedSchema {
                    detail: format!(
                        "field `{}` in record `{}` contains {} enum-generating unions, but \
                         at most one is allowed per field. An *optional* union \
                         (`[\"null\", T]`) is exempt — it lowers to `Option<T>` and \
                         generates no enum — so either reduce one of them to that shape, \
                         or move the inner union behind a named record and reference it \
                         (SPEC §5.3)",
                        field.name,
                        fqn_string(&rec.name),
                        unions
                    ),
                });
            }
        }
    }
    Ok(())
}

/// Walk one record field's type tree for [`check_field_union_rules`]: reject a
/// misplaced `"enum"` attribute in place, and accumulate the field's
/// enum-generating union count into `unions`.
///
/// Optional unions, arrays and maps are transparent: the walk continues through
/// them without spending budget, using the same [`optional_union_payload`]
/// predicate [`union_type`] lowers on. Traversal stops at `Record` / `Enum` /
/// `Fixed` / `Ref` — a union behind a named-type boundary is budgeted against
/// *that* type's field, and the caller walks every named type in its own right.
fn walk_field_union_rules(schema: &Schema, unions: &mut usize) -> Result<(), GeneratorError> {
    reject_misplaced_enum_attr(schema)?;
    match schema {
        Schema::Union(u) => match optional_union_payload(u) {
            Some(payload) => walk_field_union_rules(payload, unions)?,
            None => {
                *unions += 1;
                for variant in u.variants() {
                    walk_field_union_rules(variant, unions)?;
                }
            }
        },
        Schema::Array(arr) => walk_field_union_rules(&arr.items, unions)?,
        Schema::Map(map) => walk_field_union_rules(&map.types, unions)?,
        _ => {}
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn emit_record<'a>(
    rec: &'a RecordSchema,
    current_ns: &[String],
    opts: CodegenOptions,
    registry: &HashMap<Name, &'a Schema>,
    cycle_partners: &CyclePartners,
    eq_hash: &EqHashRecords,
    logical: LogicalCtx<'_>,
) -> Result<TokenStream, GeneratorError> {
    let ty_ident = rust_type_ident(rec.name.name());
    let ty_rename = type_rename(rec.name.name(), &ty_ident);
    let ty_doc = doc_attrs(&rec.doc);
    // SPEC §11.4: record-type aliases have no serde representation. Serde's
    // `alias` is a field/variant attribute, not a container one — emitting it
    // on the struct yields `unknown serde container attribute 'alias'`. A
    // struct serializes as an untagged JSON object anyway, so the type name
    // (and thus its aliases) never appears on the wire. The aliases survive in
    // the structural `Schema` exposed via `T::schema()`.
    let box_targets = cycle_partners.get(&rec.name).cloned().unwrap_or_default();
    let record_fqn = fqn_string(&rec.name);

    // Multi-variant union enums and default-helper fns emitted as siblings
    // end up here, ahead of the struct in the same module body.
    let mut sibling_items: Vec<TokenStream> = Vec::new();

    // SPEC §15.4: the struct gains `#[serde_as]` iff at least one field
    // resolves to a converter-backed custom logical type.
    let mut record_needs_serde_as = false;

    // vanavro extension: a `[null, T]` optional single-field record gains
    // fluent `Some`/`null` constructors (see `emit_union_constructors`).
    // Captured during the field loop so the union lowering / boxing logic is not duplicated.
    let mut single_field_info: Option<(Ident, &Schema, Option<String>)> = None;

    // vanavro extension (SPEC §5.6): a record whose lone field is an
    // enum-generating union collapses to a single public enum that takes the
    // record's own name `{Record}` (no wrapping struct). It serializes as the
    // original 1-field record. Optional `[null, T]` single-field records are
    // exempt — they stay a struct with an `Option<T>` field. A custom-logical
    // override on the field also opts out: the field type is then the opaque
    // mapped type, not a union. The collapse is a caller-toggled default
    // (`collapse_single_union`): when disabled the record keeps its ordinary
    // struct + sibling `{Record}Union` shape (§5.3).
    if opts.collapse_single_union && record_collapses(rec, logical)? {
        return emit_collapsed_union_record(
            rec,
            &ty_ident,
            &ty_doc,
            current_ns,
            &box_targets,
            registry,
            logical,
        );
    }

    let mut fields = Vec::with_capacity(rec.fields.len());
    for field in &rec.fields {
        let (field_ident, field_rename) = rust_field_ident(&field.name);
        let ctx = FieldContext::root(rec.name.name(), &record_fqn, &field.name);

        // SPEC §15.3: custom logical-type override. When a field (or its
        // array item / map value / optional-union arm) carries a registered
        // `logicalType`, the mapped Rust type replaces the standard lowering,
        // the bytes/fixed serde helper is suppressed, and a converter (if any)
        // emits `#[serde_as(as = …)]`.
        let logical_override = resolve_field_logical(field, &record_fqn, logical)?;

        let (field_type, serde_helper, serde_as_attr) = match &logical_override {
            Some(ov) => {
                if ov.uses_converter {
                    record_needs_serde_as = true;
                }
                (ov.field_type.clone(), None, ov.serde_as_attr.clone())
            }
            None => {
                // SPEC §5.3: field-level `"enum"` names this field's union.
                let field_name = custom_enum_name(&field.custom_attributes)?;
                let ft = schema_to_rust_type(
                    &field.schema,
                    current_ns,
                    registry,
                    &box_targets,
                    Some(ctx),
                    field_name,
                    &mut sibling_items,
                    logical,
                )?;
                let helper = field_serde_helper(&field.schema, registry);
                (ft, helper, None)
            }
        };

        // Capture the lone field for the named-constructor pass.
        // By this point a multi-variant single-union record has already been
        // collapsed and returned, so the only constructor-bearing shape that
        // reaches here is the exempt `[null, T]` optional field (→ `Some`/`null`
        // constructors). Skipped under a custom-logical override: `field_type`
        // is then the opaque mapped type, not the union the constructors target.
        if rec.fields.len() == 1 && logical_override.is_none() {
            single_field_info = Some((
                field_ident.clone(),
                &field.schema,
                custom_enum_name(&field.custom_attributes)?,
            ));
        }

        let rename_attr = field_rename.map(|original| {
            quote! { #[serde(rename = #original)] }
        });
        let field_doc = doc_attrs(&field.doc);
        let field_aliases = field_alias_attrs(&field.aliases);
        // SPEC §15 + §11.5: a custom-logical override replaces the field's Rust
        // type with an opaque `rust_type`. Its default goes through
        // `logical_field_default`, which lowers a non-empty literal via a
        // `Deserialize`-backed helper fn rather than a raw primitive literal.
        let default = if let Some(ov) = &logical_override {
            logical_field_default(
                rec.name.name(),
                &field.name,
                field.default.as_ref(),
                &field.schema,
                ov,
                &mut sibling_items,
            )?
        } else {
            field_default(
                rec.name.name(),
                &field.name,
                field.default.as_ref(),
                &field.schema,
                &field_type,
                current_ns,
                registry,
                &mut sibling_items,
            )?
        };
        let default_attr = default.serde_attr();

        // SPEC §2.1 / §15.9: per-field `bon::Builder` attributes, only when the
        // builder derive is active. `into` for String setters; `with = …` for a
        // custom-logical mapping's registered `builder_with`; `default` reuses
        // the serde default machinery (skipped on `Option` fields, which bon
        // already treats as optional). All present parts merge into one
        // `#[builder(…)]` so bon sees them together. `into` and `with` are
        // mutually exclusive by construction — `into` only fires when there is
        // no custom-logical override, and `with` only comes from one — so bon's
        // `into`+`with` conflict (SPEC §15.9) can never arise.
        let builder_attr = opts
            .builder
            .then(|| {
                let mut parts: Vec<TokenStream> = Vec::new();
                // A custom-logical override replaces the Rust type wholesale,
                // so the String-targeted `into` heuristic no longer applies.
                if logical_override.is_none() && builder_into_attr(&field.schema) {
                    parts.push(quote! { into });
                }
                if let Some(with) = logical_override
                    .as_ref()
                    .and_then(|ov| ov.builder_with.as_ref())
                {
                    parts.push(quote! { with = #with });
                }
                if let Some(d) = default.builder_default_inner(is_optional_field(&field.schema)) {
                    parts.push(d);
                }
                (!parts.is_empty()).then(|| quote! { #[builder(#( #parts ),*)] })
            })
            .flatten();

        fields.push(quote! {
            #field_doc
            #rename_attr
            #field_aliases
            #serde_as_attr
            #serde_helper
            #default_attr
            #builder_attr
            pub #field_ident: #field_type
        });
    }

    // SPEC §11.1: add `Eq + Hash` when every field's Rust type is `Eq + Hash`.
    let eq_hash_derives = eq_hash.contains(&rec.name).then(|| quote! { Eq, Hash, });
    // SPEC §2.1: opt-in `bon::Builder` derive, gated on the `builder` flag.
    let builder_derive = opts.builder.then(|| quote! { ::bon::Builder, });
    let derive = quote! {
        #[derive(
            Debug,
            Clone,
            PartialEq,
            #eq_hash_derives
            #builder_derive
            ::serde::Serialize,
            ::serde::Deserialize,
        )]
    };

    let ty_rename_attr = ty_rename.map(|original| quote! { #[serde(rename = #original)] });

    // SPEC §12: per-type `schema()` / `fingerprint()` API. The registry holds
    // the owning `&Schema` for this record.
    let schema_api = match registry.get(&rec.name) {
        Some(_) => emit_schema_api(&ty_ident, &rec.name)?,
        None => TokenStream::new(),
    };

    // SPEC §15.4: `#[serde_as]` must precede `#[derive(...)]` on the struct.
    let serde_as_attr = record_needs_serde_as.then(|| quote! { #[::serde_with::serde_as] });

    // vanavro extension: fluent `Some`/`null` constructors for an `[null, T]`
    // optional single-field record.
    let constructor_impl = match single_field_info {
        Some((field_ident, field_schema, field_enum_name)) => emit_union_constructors(
            &ty_ident,
            &field_ident,
            field_schema,
            field_enum_name,
            current_ns,
            registry,
            &box_targets,
            logical,
        )?,
        None => TokenStream::new(),
    };

    Ok(quote! {
        #( #sibling_items )*
        #ty_doc
        #serde_as_attr
        #derive
        #ty_rename_attr
        pub struct #ty_ident {
            #( #fields ),*
        }
        #schema_api
        #constructor_impl
    })
}

/// vanavro extension: emit fluent constructors on a record struct whose single
/// field is an `[null, T]` optional union (lowered to `Option<T>`): a
/// `fn <variant_snake>(val: T)` wrapping `Some`, plus a nullary `fn null()` for `None`.
/// This is a minor convenience for an uncommon shape, distinct from the
/// multi-variant single-union-record collapse.
///
/// The multi-variant single-union shape never reaches here — it is collapsed to
/// a bare enum by [`emit_collapsed_union_record`] before the field loop runs.
/// Returns an empty stream when the field is not an `[null, T]` optional union.
#[allow(clippy::too_many_arguments)]
fn emit_union_constructors(
    ty_ident: &Ident,
    field_ident: &Ident,
    field_schema: &Schema,
    field_enum_name: Option<String>,
    current_ns: &[String],
    registry: &HashMap<Name, &Schema>,
    box_targets: &HashSet<Name>,
    logical: LogicalCtx<'_>,
) -> Result<TokenStream, GeneratorError> {
    // Resolve through named-type references to reach the underlying union.
    let mut resolved = field_schema;
    while let Schema::Ref { name } = resolved {
        match registry.get(name) {
            Some(target) => resolved = target,
            None => break,
        }
    }

    // Only the `[null, T]` / `[T, null]` optional shape produces constructors
    // here; any other union is an enum-generating one that has already been
    // collapsed away (and so never carries a struct to hang constructors on).
    let Some(payload) = optional_payload(resolved) else {
        return Ok(TokenStream::new());
    };

    // Re-lower the payload only to name the constructor argument; its siblings
    // were already emitted in `emit_record`, so discard them here.
    // `field_enum_name` is threaded through so a named nested union resolves to
    // the same ident (SPEC §5.3).
    let mut discard: Vec<TokenStream> = Vec::new();
    let payload_type = schema_to_rust_type(
        payload,
        current_ns,
        registry,
        box_targets,
        None,
        field_enum_name,
        &mut discard,
        logical,
    )?;
    let naming = union_variant_naming((0, payload));
    let fn_ident = ctor_fn_ident(&naming.candidate_name, 0);
    Ok(quote! {
        impl #ty_ident {
            #[doc = "Construct with the non-null arm (wraps in `Some`)."]
            pub fn #fn_ident(val: #payload_type) -> Self {
                Self { #field_ident: Some(val) }
            }
            #[doc = "Construct the `null` arm (`None`)."]
            pub fn null() -> Self {
                Self { #field_ident: None }
            }
        }
    })
}

/// Build a `snake_case` constructor-fn identifier from a (PascalCase) variant
/// candidate. Degenerate candidates (e.g. an all-underscore name like `__`,
/// which `heck` collapses to the empty string) fall back to `variant{idx}` so
/// `escape_ident` never receives an empty string.
fn ctor_fn_ident(candidate: &str, idx: usize) -> Ident {
    let snake = candidate.to_snake_case();
    if snake.is_empty() {
        escape_ident(&format!("variant{idx}"))
    } else {
        escape_ident(&snake)
    }
}

fn record_collapses(rec: &RecordSchema, logical: LogicalCtx<'_>) -> Result<bool, GeneratorError> {
    if rec.fields.len() == 1 {
        let field = &rec.fields[0];
        let is_enum_union =
            matches!(field.schema, Schema::Union(_)) && optional_payload(&field.schema).is_none();
        if is_enum_union {
            // SPEC §5.6 *Scope*: a custom logical-type override (§15.3) blocks
            // the collapse, so resolve it first — the conflict below is only
            // real when the collapse actually happens.
            let record_fqn = fqn_string(&rec.name);
            if resolve_field_logical(field, &record_fqn, logical)?.is_some() {
                return Ok(false);
            }
            // SPEC §5.3 / §5.6: the collapse names the enum after the record, so
            // a field-level `"enum"` is a second name for it — a conflict.
            if let Some(custom) = custom_enum_name(&field.custom_attributes)? {
                // SPEC §5.3: reject an unusable name before offering the
                // escape hatch below, which would otherwise promise a type
                // that cannot be emitted.
                validate_custom_enum_name(&custom).map_err(|e| match e {
                    // SPEC §5.3: every diagnostic names the owning record and field.
                    GeneratorError::UnsupportedSchema { detail } => {
                        GeneratorError::UnsupportedSchema {
                            detail: format!(
                                "record `{}` field `{}`: {detail}",
                                rec.name.name(),
                                field.name
                            ),
                        }
                    }
                    other => other,
                })?;
                return Err(GeneratorError::UnsupportedSchema {
                    detail: format!(
                        "record `{record}` collapses to a bare enum named after the \
                         record (SPEC §5.6), but its lone union field `{field}` also \
                         carries an `enum` name attribute (`\"{name}\"`) — two \
                         conflicting names for the same enum. Either remove the \
                         attribute and take `{record}`, or disable the collapse \
                         (`--no-collapse-single-union` / `collapse_single_union(false)`) \
                         to keep the struct and get `pub enum {name}` (SPEC §5.3)",
                        record = rec.name.name(),
                        field = field.name,
                        name = custom,
                    ),
                });
            }
            return Ok(true);
        }
    }
    Ok(false)
}

/// vanavro extension: emit a record whose lone field is an
/// enum-generating union as a single public enum that takes the record's own
/// name `{Record}`, collapsing away the wrapping struct. Because the record
/// name maps directly to the enum, references to the record from elsewhere — and
/// recursive self-references inside the union's own arms — resolve to `{Record}`
/// through the ordinary type-lowering path with no special handling.
///
/// The on-the-wire encoding is unchanged — the type still serializes as the
/// original 1-field record. This is achieved without any extra allocation:
///
/// * **Deserialize** via `#[serde(try_from = "{Record}Repr")]`. The private
///   carrier `{Record}Repr` (a 1-field struct over the private `{Record}ReprUnion` enum)
///   deserializes exactly as the old struct did; `From` then *moves* the
///   payload into the public enum (serde's `try_from` resolves against the std
///   blanket `TryFrom`).
/// * **Serialize** via a borrowing `Serialize` impl that drives
///   `serialize_struct(.., 1)` + `serialize_field(<field>, …)` from `&self`,
///   with a nested `Arm<'a>(&'a {Record})` adapter that calls
///   `serialize_newtype_variant` per arm — byte-identical to serde-derive's
///   externally-tagged encoding of the non-collapsed `{Record}Union`, and with
///   no payload cloned. `#[serde(into)]` is deliberately *not* used: it would
///   force `self.clone()` on every serialize.
///
/// A recursive arm references the public `{Record}` enum in both positions
/// (public enum, deserialize carrier), so nested values route through the
/// public enum's own `Serialize` / `Deserialize` — keeping the wire shape
/// identical at every level.
///
/// The variant naming (and the indexed `Variant{N}` collision fallback) mirrors
/// [`emit_union_enum`] exactly, so the public enum, the `{Record}ReprUnion`
/// carrier enum, and the serde rename tags stay consistent.
#[allow(clippy::too_many_arguments)]
fn emit_collapsed_union_record(
    rec: &RecordSchema,
    ty_ident: &Ident,
    ty_doc: &Option<TokenStream>,
    current_ns: &[String],
    box_targets: &HashSet<Name>,
    registry: &HashMap<Name, &Schema>,
    logical: LogicalCtx<'_>,
) -> Result<TokenStream, GeneratorError> {
    let field = &rec.fields[0];
    let Schema::Union(union_schema) = &field.schema else {
        unreachable!("emit_collapsed_union_record called on a non-union field");
    };
    let variants = union_schema.variants();
    let record_fqn = fqn_string(&rec.name);

    let (variant_infos, _) = resolve_union_variants(variants)?;

    // The public enum takes the record's own name `{Record}` (SPEC §5.6).
    // Mapping the record name directly to the enum means every reference to the
    // record — from another type's field, or a recursive self-reference within
    // the union's own arms — resolves through the normal `relative_path` /
    // `schema_to_rust_type` machinery to this same `{Record}` ident, with no
    // context-sensitive rewriting. (Contrast the non-collapsed multi-field case,
    // where the sibling union enum is named `{Record}Union` because `{Record}`
    // is taken by the wrapping struct.)
    let pub_ident = ty_ident.clone();

    // Carrier type idents, derived from the record name with a `Repr` suffix.
    // They are module-private. A user-defined type literally named
    // `{Record}Repr` / `{Record}ReprUnion` is reported as a `NameCollision` by
    // `check_name_collisions` (SPEC §5.2).
    let repr_ident = escape_ident(&format!("{ty_ident}Repr"));
    let repr_union_ident = escape_ident(&format!("{ty_ident}ReprUnion"));

    // The carrier field keeps the original Avro field name (with a serde
    // rename when the Rust ident differs), so the wire object key is unchanged.
    let (field_ident, field_rename) = rust_field_ident(&field.name);
    let field_rename_attr = field_rename.map(|orig| quote! { #[serde(rename = #orig)] });

    // Carrier-private default helpers (and the union-default-unsupported error)
    // must behave exactly as the non-collapsed struct: a default targeting a
    // multi-variant union cannot be lowered and raises `UnsupportedSchema`
    // rather than being silently dropped (SPEC §11.5). Helpers land in
    // `carrier_siblings` (reuses the §11.5 `pub(super)` form for consistency).
    let mut carrier_siblings: Vec<TokenStream> = Vec::new();
    let field_default = field_default(
        rec.name.name(),
        &field.name,
        field.default.as_ref(),
        &field.schema,
        &quote! { #repr_union_ident },
        current_ns,
        registry,
        &mut carrier_siblings,
    )?;
    let field_default_attr = field_default.serde_attr();

    // Accumulators for each generated fragment.
    let mut pub_variants = Vec::with_capacity(variants.len());
    let mut repr_variants = Vec::with_capacity(variants.len());
    let mut serialize_arms = Vec::with_capacity(variants.len());
    let mut from_arms = Vec::with_capacity(variants.len());

    // Sibling items needed by payload lowering (e.g. boxed recursive refs).
    // The lone field's one enum-generating union (SPEC §5.3) is the collapsed
    // enum itself, so no further union enums arise here.
    let mut sibling_items: Vec<TokenStream> = Vec::new();

    // The record's Avro name, reused as serde's externally-tagged enum name in
    // the `serialize_newtype_variant` calls (cosmetic for the Avro serializer,
    // which resolves the union by schema). See `ty_name_str` below.
    let ty_name_str = rec.name.name().to_string();

    // SPEC §15.4: the deserialize carrier gains `#[serde_as]` iff at least one
    // arm resolves to a converter-backed custom logical type. The public enum
    // never needs it — its `Deserialize` routes through the carrier and its
    // `Serialize` is hand-written below.
    let mut carrier_needs_serde_as = false;

    for (idx, info) in variant_infos.iter().enumerate() {
        let variant_ident = &info.variant_ident;
        let tag = &info.avro_tag;
        let variant = &info.schema;
        // Positional variant index, matching serde-derive's externally-tagged
        // encoding of the non-collapsed `{Record}Union` (declaration order).
        let variant_idx = idx as u32;

        // SPEC §15.3 case 6: a registered `logicalType` on this arm replaces the
        // payload type in all three shapes below (public enum, carrier enum,
        // `Serialize` impl). An annotated `null` arm therefore leaves the
        // `(())` special case and lowers like any other payload.
        let arm_logical = resolve_union_arm_logical(
            variant,
            idx,
            variants.len(),
            &record_fqn,
            &field.name,
            logical,
        )?;

        if matches!(variant, Schema::Null) && arm_logical.is_none() {
            // The null arm carries a `()` payload (matching the old enum).
            // serde-derive treats `Null(())` as a *newtype* variant over `()`,
            // so it must serialize via `serialize_newtype_variant(.., &())` —
            // NOT `serialize_unit_variant` — to stay byte-identical (SPEC §5.6).
            pub_variants.push(quote! { #variant_ident(()) });
            repr_variants.push(quote! {
                #[serde(rename = #tag)]
                #variant_ident(())
            });
            serialize_arms.push(quote! {
                #pub_ident::#variant_ident(()) => serializer.serialize_newtype_variant(
                    #ty_name_str, #variant_idx, #tag, &(),
                ),
            });
            from_arms.push(quote! {
                #repr_union_ident::#variant_ident(()) => Self::#variant_ident(()),
            });
        } else {
            let (payload, serde_as_attr, converter_ty) = match &arm_logical {
                Some(ov) => {
                    if ov.converter_ty.is_some() {
                        carrier_needs_serde_as = true;
                    }
                    (
                        ov.payload_type.clone(),
                        ov.serde_as_attr.clone(),
                        ov.converter_ty.clone(),
                    )
                }
                None => (
                    schema_to_rust_type(
                        variant,
                        current_ns,
                        registry,
                        box_targets,
                        None,
                        None,
                        &mut sibling_items,
                        logical,
                    )?,
                    None,
                    None,
                ),
            };
            pub_variants.push(quote! { #variant_ident(#payload) });
            repr_variants.push(quote! {
                #[serde(rename = #tag)]
                #variant_ident(#serde_as_attr #payload)
            });
            // With a converter the payload cannot be handed to serde directly —
            // `SerializeAsWrap` borrows it and routes through the converter's
            // `SerializeAs`, keeping the hand-written impl byte-identical to the
            // `#[serde_as]`-annotated carrier (SPEC §15.4).
            let serialized = match &converter_ty {
                Some(conv) => quote! {
                    &::serde_with::ser::SerializeAsWrap::<#payload, #conv>::new(val)
                },
                None => quote! { val },
            };
            serialize_arms.push(quote! {
                #pub_ident::#variant_ident(val) => serializer.serialize_newtype_variant(
                    #ty_name_str, #variant_idx, #tag, #serialized,
                ),
            });
            from_arms.push(quote! {
                #repr_union_ident::#variant_ident(val) => Self::#variant_ident(val),
            });
        }
    }

    // `#[serde(try_from = "...")]` takes the carrier's type *name* as a string.
    let repr_str = repr_ident.to_string();
    // The field key must match the carrier's wire key, i.e. the original Avro
    // field name. (`ty_name_str`, serde's `serialize_struct`/enum name, is
    // computed above alongside the serialize arms.)
    let field_name_str = field.name.clone();

    // SPEC §15.4: `#[serde_as]` must precede `#[derive(...)]` on the carrier.
    let carrier_serde_as_attr =
        carrier_needs_serde_as.then(|| quote! { #[::serde_with::serde_as] });

    // §12 per-type API: schema()/fingerprint()/AvroSchema on the public enum.
    // The schema is the *record* schema, identical to the non-collapsed form,
    // so fingerprints and the wire layout are unchanged.
    let schema_api = match registry.get(&rec.name) {
        Some(_) => emit_schema_api(&pub_ident, &rec.name)?,
        None => TokenStream::new(),
    };

    Ok(quote! {
        #( #sibling_items )*

        #ty_doc
        // The derive set deliberately mirrors `emit_union_enum` — union enums
        // do not derive Eq/Hash in v1.
        #[derive(Debug, Clone, PartialEq, ::serde::Deserialize)]
        #[serde(try_from = #repr_str)]
        pub enum #pub_ident {
            #( #pub_variants ),*
        }

        // Hand-written, borrowing Serialize: reproduces the original 1-field
        // record encoding with zero payload clones (SPEC §5.6). The nested
        // `Arm` adapter borrows `&self` and drives serde's externally-tagged
        // `serialize_newtype_variant` per arm — byte-identical to the
        // `#[derive(Serialize)]` on the non-collapsed `{Record}Union`.
        impl ::serde::Serialize for #pub_ident {
            fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
            where
                S: ::serde::Serializer,
            {
                use ::serde::ser::SerializeStruct as _;
                struct Arm<'a>(&'a #pub_ident);
                impl ::serde::Serialize for Arm<'_> {
                    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
                    where
                        S: ::serde::Serializer,
                    {
                        match self.0 {
                            #( #serialize_arms )*
                        }
                    }
                }
                let mut st = serializer.serialize_struct(#ty_name_str, 1)?;
                st.serialize_field(#field_name_str, &Arm(self))?;
                st.end()
            }
        }

        #schema_api

        #( #carrier_siblings )*

        // Private carrier: deserialize target, byte-identical to the old struct.
        // The serde name is renamed to the record's Avro name (not `{Record}Repr`)
        // so schema-aware deserializers that match the struct name against the
        // schema record name (e.g. apache-avro's single-object reader, which calls
        // `deserialize_struct` with this name and requires `record.name.name() ==
        // name`) resolve the carrier against the `{Record}` schema. This mirrors
        // the `Serialize` impl, which already drives `serialize_struct`
        // with `#ty_name_str`.
        #[derive(::serde::Deserialize)]
        #[serde(rename = #ty_name_str)]
        struct #repr_ident {
            #field_rename_attr
            #field_default_attr
            #field_ident: #repr_union_ident,
        }

        #carrier_serde_as_attr
        #[derive(::serde::Deserialize)]
        enum #repr_union_ident {
            #( #repr_variants ),*
        }

        // The carrier→enum mapping is total, so this is a plain `From`. serde's
        // `#[serde(try_from = ...)]` resolves against the std blanket
        // `TryFrom<#repr_ident>` (`Error = Infallible`) supplied by this impl.
        impl ::std::convert::From<#repr_ident> for #pub_ident {
            fn from(repr: #repr_ident) -> Self {
                match repr.#field_ident {
                    #( #from_arms )*
                }
            }
        }
    })
}

/// The record field a lowering is descending from. Supplies the
/// default `{Record}Union` name — only at a field-root position — and keeps the
/// record / field attached across the descent for diagnostics.
///
/// Threaded as `None` only where no owning field is in scope: a union variant's
/// payload, which the one-union-per-field budget keeps from ever needing a name
/// of its own.
#[derive(Clone, Copy)]
struct FieldContext<'a> {
    record_name: &'a str,
    /// Fully-qualified name of the owning record. Together with `field_name`
    /// these are the coordinates a union arm needs to look its own
    /// `logicalType` up in the raw JSON.
    record_fqn: &'a str,
    field_name: &'a str,
    /// The innermost collection lowering has descended through, as an
    /// article-prefixed noun (``"an `array`"`` / ``"a `map`"``), or `None` at a
    /// field-root position. Transparent `[null, T]` wrappers are not a descent.
    nested_in: Option<&'static str>,
}

impl<'a> FieldContext<'a> {
    /// A field-root position on `field_name` of `record_name`.
    fn root(record_name: &'a str, record_fqn: &'a str, field_name: &'a str) -> Self {
        Self {
            record_name,
            record_fqn,
            field_name,
            nested_in: None,
        }
    }

    /// The same field, one collection deeper — no longer a field-root position.
    fn nested(&self, kind: &'static str) -> Self {
        Self {
            nested_in: Some(kind),
            ..*self
        }
    }

    /// The owning field itself, for a diagnostic about the field (rather than
    /// about a union somewhere beneath it).
    fn describe_field(&self) -> String {
        format!("record `{}` field `{}`", self.record_name, self.field_name)
    }

    /// Human-readable origin of a union reached from this field, for
    /// `NameCollision` and `UnsupportedSchema` diagnostics.
    fn describe_origin(&self) -> String {
        match self.nested_in {
            Some(kind) => format!("{}, nested in {kind}", self.describe_field()),
            None => self.describe_field(),
        }
    }
}

// =============================================================================
// Custom logical types — field-level override (SPEC §15.3 / §15.4)
// =============================================================================

/// A field whose type is replaced by a registered custom logical type mapping.
struct FieldLogicalOverride {
    /// The container-shaped Rust type (`RustTy`, `Vec<RustTy>`,
    /// `HashMap<String, RustTy>`, or `Option<RustTy>`), using the mapped
    /// `rust_type` in the leaf position.
    field_type: TokenStream,
    /// `#[serde_as(as = "…")]` when the mapping has a converter; `None` when
    /// the `rust_type` round-trips under `apache-avro` on its own.
    serde_as_attr: Option<TokenStream>,
    /// The converter type shaped to the field's container (`Conv`, `Vec<Conv>`,
    /// `Option<Conv>`, `HashMap<String, Conv>`) as a *type token* — the second
    /// type argument to `DeserializeAsWrap` when lowering a literal default.
    /// `None` when the mapping has no converter, in which
    /// case a default deserializes straight into `field_type`.
    default_serde_as_ty: Option<TokenStream>,
    /// Whether a converter is in play (drives the struct-level `#[serde_as]`).
    uses_converter: bool,
    /// Verbatim `bon` `#[builder(with = …)]` inner text, already parsed into
    /// tokens, when the mapping registered `builder_with`. Emitted
    /// only under `--builder`; independent of `converter` / `serde_as`.
    builder_with: Option<TokenStream>,
}

/// The container shape a field's schema presents, used to wrap the mapped
/// `rust_type` and to shape the `serde_as` converter expression.
#[derive(Clone, Copy)]
enum LogicalShape {
    /// Bare field: `Conv`.
    Bare,
    /// `array<T>` → `Vec<Conv>`.
    Array,
    /// `map<T>` → `HashMap<_, Conv>`.
    Map,
    /// `["null", T]` → `Option<Conv>`.
    OptionalUnion,
}

/// Recover the registered custom-logical mapping (and the field's container
/// shape) for one record field, if any applies — across the four field-position
/// shapes (bare / array / map / optional union). This does **no**
/// token parsing, so it is infallible and cheap: callers that only need to know
/// *whether* an override applies (e.g. the `Eq + Hash` fixpoint) use this
/// directly; [`resolve_field_logical`] builds on it.
fn field_logical_mapping<'a>(
    field: &RecordField,
    record_fqn: &str,
    logical: LogicalCtx<'a>,
) -> Option<(LogicalShape, &'a str, &'a logical::LogicalTypeMapping)> {
    if !logical.active() {
        return None;
    }
    let (shape, lt) = field_logical_position(field, record_fqn, logical.raw_names)?;
    let mapping = logical.registry.get(lt)?;
    Some((shape, lt, mapping))
}

/// Recover the field's container shape (bare / array item / map
/// value / optional-union arm) and the raw `logicalType` string at that exact
/// position, reading from the raw JSON. Returns `None` when the field carries no
/// `logicalType` there. Independent of whether any mapping is registered, so it
/// also backs unknown-annotation detection in [`resolve_field_logical`].
fn field_logical_position<'a>(
    field: &RecordField,
    record_fqn: &str,
    raw_names: &'a RawNameTable,
) -> Option<(LogicalShape, &'a str)> {
    // Determine the field's container shape from the *parsed* schema, then
    // recover the matching `logicalType` from raw JSON for that exact shape.
    let (shape, lt) = match &field.schema {
        Schema::Array(_) => (
            LogicalShape::Array,
            logical::get_array_item_logical_type(raw_names, record_fqn, &field.name),
        ),
        Schema::Map(_) => (
            LogicalShape::Map,
            logical::get_map_value_logical_type(raw_names, record_fqn, &field.name),
        ),
        Schema::Union(_) if optional_payload(&field.schema).is_some() => (
            LogicalShape::OptionalUnion,
            logical::get_union_variant_logical_type(raw_names, record_fqn, &field.name),
        ),
        // Everything else (primitive, named ref, record) is a "bare" position.
        _ => (
            LogicalShape::Bare,
            logical::get_field_logical_type(raw_names, record_fqn, &field.name),
        ),
    };

    Some((shape, lt?))
}

/// Names of the standard Avro logical types `apache-avro` recognizes. A raw
/// `logicalType` matching one of these is handled by standard lowering (or, when
/// malformed — e.g. an out-of-range `decimal` — deliberately falls back to its
/// base type); either way it is not an *unknown* annotation.
fn is_standard_logical_name(name: &str) -> bool {
    matches!(
        name,
        "decimal"
            | "big-decimal"
            | "uuid"
            | "date"
            | "time-millis"
            | "time-micros"
            | "timestamp-millis"
            | "timestamp-micros"
            | "timestamp-nanos"
            | "local-timestamp-millis"
            | "local-timestamp-micros"
            | "local-timestamp-nanos"
            | "duration"
    )
}

/// Recover and apply a custom logical-type mapping for one record field.
/// Returns `Ok(None)` when no registered annotation applies, leaving the field
/// to standard lowering; `Err(InvalidRustType)` when the registered `rust_type` /
/// `converter_type` does not parse as a Rust path or `builder_with` does not
/// tokenize (these strings come straight from the public builder, so a typo is a
/// clean error, never a panic); `Err(UnknownLogicalType)` for a non-standard,
/// unregistered annotation unless `allow_unregistered` is set.
fn resolve_field_logical(
    field: &RecordField,
    record_fqn: &str,
    logical: LogicalCtx<'_>,
) -> Result<Option<FieldLogicalOverride>, GeneratorError> {
    let Some((shape, lt, mapping)) = field_logical_mapping(field, record_fqn, logical) else {
        // SPEC §15.6: a `logicalType` that matches no standard logical type and
        // no registered custom mapping is surfaced, not silently lowered to the
        // base type, so typos in the schema or the registration are caught —
        // unless the caller opted into tolerating unknown annotations
        // (`allow_unregistered_logical_types`), in which case the field lowers to its
        // base type just as `apache-avro` does when it drops the annotation.
        if !logical.allow_unregistered
            && let Some((_, lt)) = field_logical_position(field, record_fqn, logical.raw_names)
            && !is_standard_logical_name(lt)
            && logical.registry.get(lt).is_none()
        {
            return Err(GeneratorError::UnknownLogicalType {
                logical_type: lt.to_string(),
                field: field.name.clone(),
                record: record_fqn.to_string(),
            });
        }
        return Ok(None);
    };

    let rust_path = parse_rust_path(&mapping.rust_type, "rust_type", lt)?;

    let field_type = match shape {
        LogicalShape::Bare => quote! { #rust_path },
        LogicalShape::Array => quote! { Vec<#rust_path> },
        LogicalShape::Map => quote! { ::std::collections::HashMap<String, #rust_path> },
        LogicalShape::OptionalUnion => quote! { Option<#rust_path> },
    };

    // `serde_with`'s `serde_as(as = "…")` takes a *string literal* spelling the
    // converter type, shaped to the field's container (SPEC §15.4). Build the
    // string from the registered converter path directly so the literal stays
    // clean (`Vec<Conv>`, not `Vec < Conv >` — `TokenStream::to_string` inserts
    // spaces around `::` and `<`/`>`). The map shape spells the fully-qualified
    // `::std::collections::HashMap` because `serde_as` resolves the string in
    // the generated module's scope, where bare `HashMap` is not in prelude.
    let serde_as_attr = mapping.converter_type.as_ref().map(|conv| {
        let spelled = match shape {
            LogicalShape::Bare => conv.clone(),
            LogicalShape::Array => format!("Vec<{conv}>"),
            LogicalShape::Map => format!("::std::collections::HashMap<_, {conv}>"),
            LogicalShape::OptionalUnion => format!("Option<{conv}>"),
        };
        quote! { #[serde_as(as = #spelled)] }
    });

    // Parallel to `serde_as_attr`, but as a *type token* for `DeserializeAsWrap`
    // when a literal default needs lowering (the `serde_as` form is a string).
    let default_serde_as_ty = match &mapping.converter_type {
        Some(conv) => {
            let conv_ty = parse_rust_path(conv, "converter_type", lt)?;
            Some(match shape {
                LogicalShape::Bare => quote! { #conv_ty },
                LogicalShape::Array => quote! { Vec<#conv_ty> },
                LogicalShape::Map => quote! { ::std::collections::HashMap<String, #conv_ty> },
                LogicalShape::OptionalUnion => quote! { Option<#conv_ty> },
            })
        }
        None => None,
    };

    // SPEC §15.9: verbatim `#[builder(with = …)]` text. Parsed into tokens now
    // (not at emit time) so a text that fails to even tokenize surfaces a clean
    // `InvalidRustType` rather than a downstream `Format` error. `--builder`
    // gating happens at the emission site; here we only carry the tokens.
    let builder_with = match &mapping.builder_with {
        Some(text) => Some(parse_builder_with(text, lt)?),
        None => None,
    };

    Ok(Some(FieldLogicalOverride {
        field_type,
        serde_as_attr,
        default_serde_as_ty,
        uses_converter: mapping.converter_type.is_some(),
        builder_with,
    }))
}

/// One arm of a multi-variant union whose payload type is replaced by a
/// registered custom logical type mapping.
struct ArmLogicalOverride {
    /// The mapped `rust_type`, used verbatim as the variant's payload. A union
    /// arm is always a bare position — there is no container to shape it into.
    payload_type: TokenStream,
    /// `#[serde_as(as = "Conv")]` for the variant's payload, when the mapping
    /// has a converter. `None` leaves the payload to (de)serialize on its own.
    serde_as_attr: Option<TokenStream>,
    /// The same converter as a *type token*. The collapsed single-union shape writes
    /// its `Serialize` by hand, where an attribute has no home and the payload
    /// must be wrapped in `serde_with::ser::SerializeAsWrap` instead.
    converter_ty: Option<TokenStream>,
}

/// Recover and apply a custom logical-type mapping for one arm of a
/// multi-variant union, reading the annotation off the
/// arm's own raw-JSON node.
///
/// This is the union-arm counterpart of [`resolve_field_logical`], and it
/// subsumes the record-level suppression (case 2) that `schema_to_rust_type`
/// performs for a bare reference: resolving the arm through the raw-name table
/// lands on the target record's `logicalType`, so the arm picks up the
/// mapping's *converter* as well as its `rust_type`. Without it a suppressed
/// record in a union arm would get the right type and lose its serde glue.
///
/// `array` / `map` arms are deliberately skipped. In field position the
/// annotation is read off the container's items / values rather than the
/// container node itself, and there is no equivalent
/// recovery for a container *nested inside* a union arm; mapping the arm node
/// here would invent a semantics that field position does not have.
fn resolve_union_arm_logical(
    arm: &Schema,
    arm_index: usize,
    arm_count: usize,
    record_fqn: &str,
    field_name: &str,
    logical: LogicalCtx<'_>,
) -> Result<Option<ArmLogicalOverride>, GeneratorError> {
    if matches!(arm, Schema::Array(_) | Schema::Map(_)) {
        return Ok(None);
    }

    let lt = logical::get_union_arm_logical_type(
        logical.raw_names,
        record_fqn,
        field_name,
        arm_index,
        arm_count,
    );

    let Some(mapping) = lt.and_then(|lt| logical.registry.get(lt)) else {
        // SPEC §15.6, as for a field: an annotation matching neither a standard
        // logical type nor a registered mapping is surfaced rather than
        // silently dropped, unless the caller opted into tolerating it.
        if !logical.allow_unregistered
            && let Some(lt) = lt
            && !is_standard_logical_name(lt)
        {
            return Err(GeneratorError::UnknownLogicalType {
                logical_type: lt.to_string(),
                field: field_name.to_string(),
                record: record_fqn.to_string(),
            });
        }
        return Ok(None);
    };
    let lt = lt.expect("mapping was looked up by the recovered logical type");

    let payload_type = parse_rust_path(&mapping.rust_type, "rust_type", lt)?;
    let serde_as_attr = mapping.converter_type.as_ref().map(|conv| {
        quote! { #[serde_as(as = #conv)] }
    });
    let converter_ty = match &mapping.converter_type {
        Some(conv) => Some(parse_rust_path(conv, "converter_type", lt)?),
        None => None,
    };

    Ok(Some(ArmLogicalOverride {
        payload_type,
        serde_as_attr,
        converter_ty,
    }))
}

/// Parse a caller-supplied `builder_with` string into verbatim tokens for a
/// `bon` `#[builder(with = <tokens>)]` attribute. vanavro does not
/// interpret the text — an infallible/fallible closure or a function path are
/// all valid — but it must at least *tokenize*, so a lexer failure maps to a
/// clean [`GeneratorError::InvalidRustType`] (detail prefixed `builder_with`) instead
/// of blowing up later as a `Format` error. `logical_type` is the annotation the
/// mapping was registered under.
fn parse_builder_with(text: &str, logical_type: &str) -> Result<TokenStream, GeneratorError> {
    text.parse::<TokenStream>()
        .map_err(|e| GeneratorError::InvalidRustType {
            logical_type: logical_type.to_string(),
            detail: format!("builder_with `{text}` is not a valid token stream: {e}"),
        })
}

/// Parse a caller-supplied `rust_type` / `converter_type` string into a type
/// token, mapping a parse failure to a clean [`GeneratorError::InvalidRustType`]
/// instead of panicking. `which` names which field of the mapping
/// is at fault; `logical_type` is the annotation it was registered under.
///
/// Parsed as a `syn::Type` (not a bare `TokenStream`): `TokenStream::from_str`
/// only rejects *lexer* errors, so garbage like `not a::path!!!` would lex fine
/// and only blow up later as a `Format` error. `syn::Type` rejects anything that
/// is not a syntactically valid Rust type up front.
fn parse_rust_path(
    path: &str,
    which: &str,
    logical_type: &str,
) -> Result<TokenStream, GeneratorError> {
    let ty: syn::Type = syn::parse_str(path).map_err(|e| GeneratorError::InvalidRustType {
        logical_type: logical_type.to_string(),
        detail: format!("{which} `{path}` is not a valid Rust type: {e}"),
    })?;
    Ok(quote! { #ty })
}

/// Parse the `rust_type` of a *record-level* suppressed mapping into a type token, mapping a parse failure to a clean
/// [`GeneratorError::InvalidRustType`] rather than panicking. `name` locates the
/// suppressed record in the diagnostic.
fn parse_suppressed_rust_type(
    mapping: &logical::LogicalTypeMapping,
    name: &Name,
) -> Result<TokenStream, GeneratorError> {
    let ty: syn::Type =
        syn::parse_str(&mapping.rust_type).map_err(|e| GeneratorError::InvalidRustType {
            logical_type: fqn_string(name),
            detail: format!(
                "rust_type `{}` for record-level logical type is not a valid Rust type: {e}",
                mapping.rust_type
            ),
        })?;
    Ok(quote! { #ty })
}

// =============================================================================
// Enum emission
// =============================================================================

fn emit_enum(
    en: &EnumSchema,
    opts: CodegenOptions,
    registry: &HashMap<Name, &Schema>,
) -> Result<TokenStream, GeneratorError> {
    let ty_ident = rust_type_ident(en.name.name());
    let ty_rename = type_rename(en.name.name(), &ty_ident);
    let ty_doc = doc_attrs(&en.doc);
    // SPEC §11.4: enum-type aliases have no serde representation. Serde's
    // `alias` is a field/variant attribute, not a container one — emitting it
    // on the enum yields `unknown serde container attribute 'alias'`. The Avro
    // enum-type name (and its aliases) never appears on the wire; only the
    // symbol does. The aliases survive in the structural `Schema` via
    // `T::schema()`.

    let variants = en.symbols.iter().map(|sym| {
        let (variant_ident, variant_rename) = rust_variant_ident(sym);
        let rename = variant_rename.map(|original| quote! { #[serde(rename = #original)] });
        quote! { #rename #variant_ident }
    });

    let non_exhaustive = opts.non_exhaustive.then(|| quote! { #[non_exhaustive] });
    let ty_rename_attr = ty_rename.map(|original| quote! { #[serde(rename = #original)] });

    // SPEC §12: per-type `schema()` / `fingerprint()` API.
    let schema_api = match registry.get(&en.name) {
        Some(_) => emit_schema_api(&ty_ident, &en.name)?,
        None => TokenStream::new(),
    };

    Ok(quote! {
        #ty_doc
        #[derive(
            Debug,
            Clone,
            Copy,
            PartialEq,
            Eq,
            Hash,
            PartialOrd,
            Ord,
            ::serde::Serialize,
            ::serde::Deserialize,
        )]
        #non_exhaustive
        #ty_rename_attr
        pub enum #ty_ident {
            #( #variants ),*
        }
        #schema_api
    })
}

// =============================================================================
// Fixed emission
// =============================================================================

fn emit_fixed(
    fx: &FixedSchema,
    registry: &HashMap<Name, &Schema>,
) -> Result<TokenStream, GeneratorError> {
    let ty_ident = rust_type_ident(fx.name.name());
    let size = Literal::usize_unsuffixed(fx.size);
    // A `pub type` alias can't carry `#[serde(alias)]` (it's not a serde
    // container), so fixed aliases are dropped here, but they DO survive in
    // the structural `Schema` exposed via the sibling `*_schema()` fn below.
    // Doc comments render on a type alias, so those are preserved.
    let ty_doc = doc_attrs(&fx.doc);
    // SPEC §12: a type alias can't host an `impl`, so emit sibling free fns.
    let schema_api = match registry.get(&fx.name) {
        Some(_) => emit_fixed_schema_api(&ty_ident, &fx.name)?,
        None => TokenStream::new(),
    };
    Ok(quote! {
        #ty_doc
        pub type #ty_ident = [u8; #size];
        #schema_api
    })
}

// =============================================================================
// Field type lowering (SPEC §3.1 / §3.2 / §6.2)
// =============================================================================

/// Lowers an Avro schema to its corresponding Rust type. `custom_name` carries
/// an inherited `"enum"` union name inward through arrays / maps /
/// optional wrappers.
#[allow(clippy::too_many_arguments)]
fn schema_to_rust_type(
    schema: &Schema,
    current_ns: &[String],
    registry: &HashMap<Name, &Schema>,
    box_targets: &HashSet<Name>,
    field_ctx: Option<FieldContext<'_>>,
    custom_name: Option<String>,
    sibling_items: &mut Vec<TokenStream>,
    logical: LogicalCtx<'_>,
) -> Result<TokenStream, GeneratorError> {
    if let Some(name) = custom_name
        .as_ref()
        .filter(|_| !matches!(schema, Schema::Array(_) | Schema::Map(_) | Schema::Union(_)))
    {
        // The attribute sits on the field, so name the field — not the nested
        // position the inherited name died at.
        let owner = match field_ctx {
            Some(ctx) => ctx.describe_field(),
            None => "this record field".to_string(),
        };
        return Err(GeneratorError::UnsupportedSchema {
            detail: format!(
                "the `\"enum\"` attribute (value: `\"{name}\"`) on {owner} does not \
                 ultimately enclose an enum-generating union; the name has nothing to \
                 apply to (SPEC §5.3)"
            ),
        });
    }

    Ok(match schema {
        Schema::Null => quote! { () },
        Schema::Boolean => quote! { bool },
        Schema::Int => quote! { i32 },
        Schema::Long => quote! { i64 },
        Schema::Float => quote! { f32 },
        Schema::Double => quote! { f64 },
        Schema::Bytes => quote! { Vec<u8> },
        Schema::String => quote! { String },
        Schema::Array(arr) => {
            // `Vec<T>` heap-allocates the element store, so cycles through an
            // array don't need boxing — reset `box_targets` for the recursion.
            // Arrays are transparent for union naming (SPEC §5.3): `custom_name`
            // propagates inward, while the *nested* field context withdraws the
            // field-root default.
            let empty = HashSet::new();
            let inner = schema_to_rust_type(
                &arr.items,
                current_ns,
                registry,
                &empty,
                field_ctx.map(|ctx| ctx.nested("an `array`")),
                custom_name,
                sibling_items,
                logical,
            )?;
            quote! { Vec<#inner> }
        }
        Schema::Map(map) => {
            let empty = HashSet::new();
            let inner = schema_to_rust_type(
                &map.types,
                current_ns,
                registry,
                &empty,
                field_ctx.map(|ctx| ctx.nested("a `map`")),
                custom_name,
                sibling_items,
                logical,
            )?;
            quote! { ::std::collections::HashMap<String, #inner> }
        }
        Schema::Union(u) => union_type(
            u,
            current_ns,
            registry,
            box_targets,
            field_ctx,
            custom_name,
            sibling_items,
            logical,
        )?,
        Schema::Record(rec) => {
            // SPEC §15.3 case 2: a record-level logical type suppresses the
            // struct; every reference becomes the mapped Rust type.
            if let Some(mapping) = logical.suppressed_record(&rec.name) {
                parse_suppressed_rust_type(mapping, &rec.name)?
            } else {
                let path = relative_path(current_ns, &rec.name);
                maybe_box(path, &rec.name, box_targets)
            }
        }
        Schema::Enum(en) => relative_path(current_ns, &en.name),
        Schema::Fixed(fx) => relative_path(current_ns, &fx.name),
        Schema::Ref { name } => {
            if let Some(mapping) = logical.suppressed_record(name) {
                parse_suppressed_rust_type(mapping, name)?
            } else {
                // After parse_list, the target should always be registered.
                if !registry.contains_key(name) {
                    return Err(GeneratorError::UnresolvedReference {
                        name: name.fullname(None),
                        referenced_from: format!("namespace `{}`", current_ns.join(".")),
                    });
                }
                let path = relative_path(current_ns, name);
                maybe_box(path, name, box_targets)
            }
        }

        // Logical types (SPEC §3.3). Date / Time / Timestamp variants are
        // transparent at the Rust type level — they ride their underlying
        // primitive (`i32` for day / ms-of-day, `i64` for the longer ones).
        // Logical-type semantics are recovered via the schema's own
        // `logicalType` metadata, which apache_avro's serde layer preserves.
        Schema::Date => quote! { i32 },
        Schema::TimeMillis => quote! { i32 },
        Schema::TimeMicros => quote! { i64 },
        Schema::TimestampMillis => quote! { i64 },
        Schema::TimestampMicros => quote! { i64 },
        Schema::TimestampNanos => quote! { i64 },
        Schema::LocalTimestampMillis => quote! { i64 },
        Schema::LocalTimestampMicros => quote! { i64 },
        Schema::LocalTimestampNanos => quote! { i64 },

        // Logical types with their own apache_avro / external Rust types.
        Schema::Uuid(_) => quote! { ::uuid::Uuid },
        Schema::Decimal(_) => quote! { ::apache_avro::Decimal },
        Schema::BigDecimal => quote! { ::apache_avro::BigDecimal },
        Schema::Duration(_) => quote! { ::apache_avro::Duration },
    })
}

/// Wrap `path` in `Box<…>` when `name` participates in a cycle that the
/// containing record also participates in. Direct references through `Option`
/// inherit this decision — see [`union_type`] for why `box_targets` is
/// threaded through there.
fn maybe_box(path: TokenStream, name: &Name, box_targets: &HashSet<Name>) -> TokenStream {
    if box_targets.contains(name) {
        quote! { Box<#path> }
    } else {
        path
    }
}

/// Resolve a union to a Rust type. `[null, T]` (or `[T, null]`) lowers to
/// `Option<T>` directly; everything else generates a sibling enum
/// (named by `custom_name` when present, else the field-root `{Record}Union`
/// default) and returns the enum's name.
#[allow(clippy::too_many_arguments)]
fn union_type(
    u: &UnionSchema,
    current_ns: &[String],
    registry: &HashMap<Name, &Schema>,
    box_targets: &HashSet<Name>,
    field_ctx: Option<FieldContext<'_>>,
    custom_name: Option<String>,
    sibling_items: &mut Vec<TokenStream>,
    logical: LogicalCtx<'_>,
) -> Result<TokenStream, GeneratorError> {
    let variants = u.variants();

    // Optional shape: `[null, T]` / `[T, null]` → `Option<T>`. Treat as a
    // pass-through and let the existing Box + helper machinery handle T. Both
    // `custom_name` and the field-root status pass through unchanged, so an
    // optional wrapper around a named nested union stays namable (SPEC §5.3).
    if let Some(payload) = optional_union_payload(u) {
        let inner = schema_to_rust_type(
            payload,
            current_ns,
            registry,
            box_targets,
            field_ctx,
            custom_name,
            sibling_items,
            logical,
        )?;
        return Ok(quote! { Option<#inner> });
    }

    // Multi-variant / non-nullable 2-arm: emit a sibling enum (SPEC §5.3).
    let enum_ident = union_enum_ident(custom_name.as_deref(), field_ctx)?;

    // Register with the collision pre-pass, if it is the one driving us; `None`
    // during real emission.
    if let Some(sink) = logical.union_sink {
        sink.borrow_mut().push(UnionTypeInfo {
            namespace: current_ns.join("."),
            rust_ident: enum_ident.to_string(),
            description: match field_ctx {
                Some(ctx) => ctx.describe_origin(),
                // No owning field in scope; see `FieldContext`.
                None => "an anonymous nested union".to_string(),
            },
        });
    }

    let enum_def = emit_union_enum(
        &enum_ident,
        variants,
        current_ns,
        registry,
        box_targets,
        field_ctx,
        sibling_items,
        logical,
    )?;
    sibling_items.push(enum_def);
    Ok(quote! { #enum_ident })
}

/// Resolve an extracted union enum's identifier, in precedence:
/// 1. `custom_name` (an `"enum"` attribute) — used verbatim, keyword-escaped but
///    **not** PascalCased, and validated (see [`validate_custom_enum_name`]).
/// 2. else, at a field-root position, the default `{ParentRecord}Union`.
/// 3. else (nested, no name) an actionable `UnsupportedSchema` error naming the
///    record field the unnamable union hangs off.
///
/// A verbatim name that is not `UpperCamelCase` warns under
/// `non_camel_case_types` in the consuming crate; no allow is emitted for it.
fn union_enum_ident(
    custom_name: Option<&str>,
    field_ctx: Option<FieldContext<'_>>,
) -> Result<Ident, GeneratorError> {
    if let Some(name) = custom_name {
        return Ok(escape_ident(&validate_custom_enum_name(name)?));
    }
    let Some(ctx) = field_ctx else {
        // No owning field in scope (see `FieldContext`), so the message cannot
        // name one. Kept as an error rather than `unreachable!` so a gap in the
        // pre-pass surfaces as a diagnostic instead of a panic.
        return Err(GeneratorError::UnsupportedSchema {
            detail: "an enum-generating union nested inside an array or map has no \
                     parent record field to derive an enum name from; name it \
                     explicitly with an `\"enum\"` attribute on the enclosing \
                     record field (SPEC §5.3)"
                .into(),
        });
    };
    match ctx.nested_in {
        // Field-root: the default `{ParentRecord}Union` applies.
        None => Ok(escape_ident(&format!(
            "{}Union",
            ctx.record_name.to_pascal_case()
        ))),
        // Nested with no inherited name: unnamable. Point at the exact field.
        Some(kind) => Err(GeneratorError::UnsupportedSchema {
            detail: format!(
                "the enum-generating union in {} has no name: it is nested inside \
                 {kind}, not at the field root, so there is no `{{Record}}Union` \
                 default to fall back on. Name it with an `\"enum\"` attribute on \
                 that record field (SPEC §5.3)",
                ctx.describe_field(),
            ),
        }),
    }
}

/// Validate a user-supplied `"enum"` custom-name value. Avro does
/// not constrain custom-attribute contents, so a value that is not a legal Rust
/// identifier — which would panic `Ident::new` — is rejected here.
///
/// Grammar only: a name like `String` / `Vec` / `Option` / `Box` is accepted but
/// shadows the prelude, so the generated module won't compile.
fn validate_custom_enum_name(name: &str) -> Result<String, GeneratorError> {
    // SPEC §5.3: `_` clears Avro's name regex but is a Rust wildcard, not a
    // binding, so `Ident::new` accepts it and `syn` later rejects `pub enum _`.
    if name == "_" {
        return Err(GeneratorError::UnsupportedSchema {
            detail: "`enum` custom union name \"_\" is a Rust wildcard, not a binding \
                     identifier; use `__` or a descriptive name (SPEC §5.3)"
                .into(),
        });
    }
    let is_valid = {
        let mut chars = name.chars();
        match chars.next() {
            Some(first) if first == '_' || first.is_ascii_alphabetic() => {
                chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
            }
            _ => false,
        }
    };
    if is_valid {
        Ok(name.to_string())
    } else {
        Err(GeneratorError::UnsupportedSchema {
            detail: format!(
                "`enum` custom union name {name:?} is not a valid Rust identifier; \
                 it must match `[A-Za-z_][A-Za-z0-9_]*` (SPEC §5.3)"
            ),
        })
    }
}

/// Read the `"enum"` union-naming attribute off a **record field**
/// object — the only placement the attribute is accepted in.
///
/// A present-but-non-string value is rejected rather than ignored, so a caller
/// who wrote one is not told to add the attribute they already have.
fn custom_enum_name(
    attributes: &BTreeMap<String, serde_json::Value>,
) -> Result<Option<String>, GeneratorError> {
    match attributes.get("enum") {
        None => Ok(None),
        Some(serde_json::Value::String(name)) => Ok(Some(name.clone())),
        Some(other) => Err(GeneratorError::UnsupportedSchema {
            detail: format!(
                "`enum` union-name attribute must be a string, but got {other}; \
                 name the union with a string identifier (SPEC §5.3)"
            ),
        }),
    }
}

/// Reject an `"enum"` union-name attribute on an `array` / `map` schema object,
/// which is a misplacement. Named types pass through untouched: a
/// stray key there is inert user metadata, not a naming hint.
fn reject_misplaced_enum_attr(schema: &Schema) -> Result<(), GeneratorError> {
    let (attributes, kind) = match schema {
        Schema::Array(arr) => (&arr.attributes, "an `array`"),
        Schema::Map(map) => (&map.attributes, "a `map`"),
        _ => return Ok(()),
    };
    if attributes.contains_key("enum") {
        return Err(GeneratorError::UnsupportedSchema {
            detail: format!(
                "`enum` union-name attribute found on {kind} schema object; it is only \
                 allowed on the record field that encloses the union. Move it to the field \
                 object (SPEC §5.3)"
            ),
        });
    }
    Ok(())
}

/// Emit the extracted union enum definition for `ident`, which may
/// be a custom name from the field's `"enum"` attribute. Variants are named
/// by their schema kind (`Int`, `String`, the named type's local name, …);
/// on duplicate names everything falls back to `Variant{N}` to guarantee
/// uniqueness. Each variant carries
/// `#[serde(rename = "<avro tag>")]` matching Avro's union-JSON encoding.
///
/// Variant *naming* always follows the arm's Avro type, never a custom logical
/// mapping on it: the tag on the wire is the Avro type, so
/// an annotated `string` arm stays `String(<mapped type>)`. `field_ctx` locates
/// the owning field for that recovery — see `arm_ctx` below.
#[allow(clippy::too_many_arguments)]
fn emit_union_enum(
    ident: &Ident,
    variants: &[Schema],
    current_ns: &[String],
    registry: &HashMap<Name, &Schema>,
    box_targets: &HashSet<Name>,
    field_ctx: Option<FieldContext<'_>>,
    sibling_items: &mut Vec<TokenStream>,
    logical: LogicalCtx<'_>,
) -> Result<TokenStream, GeneratorError> {
    let (variant_infos, _) = resolve_union_variants(variants)?;

    // SPEC §15.4: the enum gains `#[serde_as]` iff at least one arm resolves to
    // a converter-backed custom logical type — the same rule the owning struct
    // follows for its fields.
    let mut enum_needs_serde_as = false;

    // SPEC §15.3 case 6: recovery reads the *field's* raw arms, so it only
    // identifies this union at a field-root position. One collection deeper an
    // equal arity would silently hand an arm the wrong mapping — load-bearing,
    // see `union_arm_logical_does_not_leak_into_a_union_nested_in_a_container`.
    let arm_ctx = field_ctx.filter(|ctx| ctx.nested_in.is_none());

    let mut variant_tokens = Vec::with_capacity(variants.len());
    for (idx, info) in variant_infos.iter().enumerate() {
        let variant_ident = &info.variant_ident;
        let arm_logical = match arm_ctx {
            Some(ctx) => resolve_union_arm_logical(
                &info.schema,
                idx,
                variants.len(),
                ctx.record_fqn,
                ctx.field_name,
                logical,
            )?,
            None => None,
        };
        let (payload, serde_as_attr) = match &arm_logical {
            Some(ov) => {
                if ov.converter_ty.is_some() {
                    enum_needs_serde_as = true;
                }
                (ov.payload_type.clone(), ov.serde_as_attr.clone())
            }
            // Pass `field_ctx = None` and no `custom_name`: this union is the
            // field's one enum-generating union (SPEC §5.3), so no variant can
            // hold another one needing a name.
            None => (
                schema_to_rust_type(
                    &info.schema,
                    current_ns,
                    registry,
                    box_targets,
                    None,
                    None,
                    sibling_items,
                    logical,
                )?,
                None,
            ),
        };
        let rename = &info.avro_tag;
        variant_tokens.push(quote! {
            #[serde(rename = #rename)]
            #variant_ident(#serde_as_attr #payload)
        });
    }

    // SPEC §12: the union enum's schema is the `Schema::Union` of its
    // variants. Build it structurally from the variant schemas (synthetic
    // type — not in the registry, so construct the union directly).
    //
    // Named variants emit `build_*(defined)` calls resolved from this synthetic
    // enum's `current_ns` to each target type's namespace module. Only bind
    // `defined` when at least one variant is named and thus consumes it; an
    // all-primitive union references it nowhere, so binding it there would be a
    // genuinely unused local.
    let variant_ctors = variants
        .iter()
        .map(|v| emit_schema_ctor(v, current_ns))
        .collect::<Result<Vec<_>, _>>()?;
    let needs_defined = variants.iter().any(schema_ctor_uses_defined);
    let defined_binding = needs_defined.then(|| {
        quote! {
            // Synthetic type: `defined` starts empty; named variants are built
            // in full on first occurrence (SPEC §12).
            let defined = &mut ::std::collections::HashSet::<
                ::apache_avro::schema::Name,
            >::new();
        }
    });
    let schema_api = quote! {
        impl #ident {
            /// Statically constructed Avro schema for this union.
            pub fn schema() -> &'static ::apache_avro::Schema {
                static SCHEMA: ::std::sync::LazyLock<::apache_avro::Schema> =
                    ::std::sync::LazyLock::new(|| {
                        #defined_binding
                        ::apache_avro::Schema::Union(
                            ::apache_avro::schema::UnionSchema::new(
                                vec![ #( #variant_ctors ),* ]
                            )
                            .expect("vanavro: structurally constructed union is valid")
                        )
                    });
                &SCHEMA
            }

            /// Fingerprint of [`Self::schema`] under digest `D`.
            pub fn fingerprint<D: ::digest::Digest>(
            ) -> ::apache_avro::schema::SchemaFingerprint {
                Self::schema().fingerprint::<D>()
            }
        }

        impl ::apache_avro::AvroSchema for #ident {
            fn get_schema() -> ::apache_avro::Schema {
                Self::schema().clone()
            }
        }
    };

    // SPEC §15.4: `#[serde_as]` must precede `#[derive(...)]`.
    let serde_as_attr = enum_needs_serde_as.then(|| quote! { #[::serde_with::serde_as] });

    Ok(quote! {
        #serde_as_attr
        #[derive(
            Debug,
            Clone,
            PartialEq,
            ::serde::Serialize,
            ::serde::Deserialize,
        )]
        pub enum #ident {
            #( #variant_tokens ),*
        }
        #schema_api
    })
}

struct UnionVariantInfo {
    variant_ident: Ident,
    avro_tag: String,
    schema: Schema,
}

fn resolve_union_variants(
    variants: &[Schema],
) -> Result<(Vec<UnionVariantInfo>, bool), GeneratorError> {
    if variants.iter().any(|v| matches!(v, Schema::Duration(_))) {
        return Err(GeneratorError::UnsupportedSchema {
            detail: "`duration` logical type inside a multi-variant union is \
                     unsupported in v1: apache_avro::Schema::Duration discards \
                     the backing fixed's name, so the union's serde rename tag \
                     cannot be reconstructed"
                .into(),
        });
    }

    let candidates: Vec<UnionVariantNaming> = variants
        .iter()
        .enumerate()
        .map(union_variant_naming)
        .collect();

    let mut name_counts: HashMap<String, usize> = HashMap::new();
    for c in &candidates {
        *name_counts.entry(c.candidate_name.clone()).or_insert(0) += 1;
    }
    let force_indexed = name_counts.values().any(|&n| n > 1);

    let infos = variants
        .iter()
        .zip(candidates.iter())
        .enumerate()
        .map(|(idx, (variant, naming))| {
            let variant_ident = if force_indexed {
                escape_ident(&format!("Variant{idx}"))
            } else {
                escape_ident(&naming.candidate_name)
            };
            UnionVariantInfo {
                variant_ident,
                avro_tag: naming.avro_tag.clone(),
                schema: variant.clone(),
            }
        })
        .collect();

    Ok((infos, force_indexed))
}

struct UnionVariantNaming {
    /// PascalCase Rust candidate ident derived from the schema's kind /
    /// named-type name. Used as the variant name unless a sibling variant
    /// would produce the same string, in which case we fall back to
    /// `Variant{N}` for *all* variants of this union.
    candidate_name: String,
    /// Avro JSON union tag for `#[serde(rename = "…")]` — primitive name
    /// lowercase (`"int"`, `"string"`), `"array"` / `"map"` for collections,
    /// fully-qualified name for named types.
    avro_tag: String,
}

fn union_variant_naming((index, schema): (usize, &Schema)) -> UnionVariantNaming {
    let (candidate, tag) = match schema {
        Schema::Null => ("Null", "null".to_string()),
        Schema::Boolean => ("Boolean", "boolean".to_string()),
        Schema::Int | Schema::Date | Schema::TimeMillis => ("Int", "int".to_string()),
        Schema::Long
        | Schema::TimeMicros
        | Schema::TimestampMillis
        | Schema::TimestampMicros
        | Schema::TimestampNanos
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros
        | Schema::LocalTimestampNanos => ("Long", "long".to_string()),
        Schema::Float => ("Float", "float".to_string()),
        Schema::Double => ("Double", "double".to_string()),
        Schema::Bytes | Schema::Decimal(_) | Schema::BigDecimal => ("Bytes", "bytes".to_string()),
        Schema::String | Schema::Uuid(_) => ("String", "string".to_string()),
        Schema::Array(_) => ("Array", "array".to_string()),
        Schema::Map(_) => ("Map", "map".to_string()),
        Schema::Union(_) => {
            // Avro disallows nested unions; the codegen path would error
            // before we even reach this naming helper. Provide a safe
            // fallback regardless.
            return UnionVariantNaming {
                candidate_name: format!("Variant{index}"),
                avro_tag: format!("variant{index}"),
            };
        }
        Schema::Record(r) => return named_variant(&r.name),
        Schema::Enum(e) => return named_variant(&e.name),
        Schema::Fixed(f) => return named_variant(&f.name),
        Schema::Ref { name } => return named_variant(name),
        // Reached only via `emit_union_constructors` (optional
        // `[null, duration]`), where it names the constructor; multi-variant
        // unions reject `Duration` in `resolve_union_variants`.
        Schema::Duration(_) => ("Duration", "duration".to_string()),
    };
    UnionVariantNaming {
        candidate_name: candidate.to_string(),
        avro_tag: tag,
    }
}

/// Variant naming for a *named* union member (record/enum/fixed): a PascalCase,
/// ident-sanitised candidate plus the member's fully-qualified Avro name as the
/// serde tag (so the wire encoding keeps the original `namespace.name`).
fn named_variant(name: &Name) -> UnionVariantNaming {
    UnionVariantNaming {
        candidate_name: sanitize_ident_candidate(&name.name().to_pascal_case(), name.name()),
        avro_tag: name.fullname(None),
    }
}

// =============================================================================
// Serde helpers for bytes / fixed fields (SPEC §3.4)
// =============================================================================

fn field_serde_helper(schema: &Schema, registry: &HashMap<Name, &Schema>) -> Option<TokenStream> {
    match field_helper_kind(schema, registry) {
        HelperKind::None => None,
        HelperKind::Bytes => Some(quote! { #[serde(with = "::apache_avro::serde::bytes")] }),
        HelperKind::BytesOpt => Some(quote! { #[serde(with = "::apache_avro::serde::bytes_opt")] }),
        HelperKind::Fixed => Some(quote! { #[serde(with = "::apache_avro::serde::fixed")] }),
        HelperKind::FixedOpt => Some(quote! { #[serde(with = "::apache_avro::serde::fixed_opt")] }),
    }
}

enum HelperKind {
    None,
    Bytes,
    BytesOpt,
    Fixed,
    FixedOpt,
}

fn field_helper_kind(schema: &Schema, registry: &HashMap<Name, &Schema>) -> HelperKind {
    if let Some(payload) = optional_payload(schema) {
        return match payload_kind(payload, registry) {
            PayloadKind::Bytes => HelperKind::BytesOpt,
            PayloadKind::Fixed => HelperKind::FixedOpt,
            PayloadKind::Other => HelperKind::None,
        };
    }
    match payload_kind(schema, registry) {
        PayloadKind::Bytes => HelperKind::Bytes,
        PayloadKind::Fixed => HelperKind::Fixed,
        PayloadKind::Other => HelperKind::None,
    }
}

enum PayloadKind {
    Bytes,
    Fixed,
    Other,
}

fn payload_kind(schema: &Schema, registry: &HashMap<Name, &Schema>) -> PayloadKind {
    match schema {
        Schema::Bytes => PayloadKind::Bytes,
        Schema::Fixed(_) => PayloadKind::Fixed,
        Schema::Ref { name } => match registry.get(name) {
            Some(Schema::Fixed(_)) => PayloadKind::Fixed,
            _ => PayloadKind::Other,
        },
        _ => PayloadKind::Other,
    }
}

/// The single definition of an *optional* union: exactly two arms,
/// one of them `null`, in either order. Lowers to `Option<T>` and generates no
/// sibling enum. Every other union — 1 arm, 2 arms without `null`, or 3+ — is
/// enum-generating.
fn optional_union_payload(u: &UnionSchema) -> Option<&Schema> {
    let v = u.variants();
    if v.len() != 2 {
        return None;
    }
    match (&v[0], &v[1]) {
        (Schema::Null, payload) | (payload, Schema::Null) => Some(payload),
        _ => None,
    }
}

/// [`optional_union_payload`] lifted to an arbitrary schema: `None` unless
/// `schema` is itself an optional union.
fn optional_payload(schema: &Schema) -> Option<&Schema> {
    match schema {
        Schema::Union(u) => optional_union_payload(u),
        _ => None,
    }
}

// =============================================================================
// Doc comments (SPEC §5.1)
// =============================================================================

/// Render an Avro `doc` string as a sequence of outer doc attributes — one
/// `#[doc = " ..."]` per source line so multi-line docs render correctly as
/// stacked `///` comments through `prettyplease` / `rustdoc`.
///
/// A leading space is prepended to each line so the rendered `///` comment
/// reads `/// text` rather than `///text`, matching idiomatic rustdoc output.
/// Returns `None` for absent or empty docs so no attribute is emitted.
fn doc_attrs(doc: &Option<String>) -> Option<TokenStream> {
    let text = doc.as_deref()?;
    if text.is_empty() {
        return None;
    }
    let lines = text.split('\n').map(|line| {
        let with_space = format!(" {line}");
        quote! { #[doc = #with_space] }
    });
    Some(quote! { #( #lines )* })
}

// =============================================================================
// Aliases (SPEC §11.4)
// =============================================================================

/// Field aliases are plain `Vec<String>` (no namespace).
fn field_alias_attrs(aliases: &[String]) -> Option<TokenStream> {
    if aliases.is_empty() {
        return None;
    }
    let attrs = aliases.iter().map(|a| {
        quote! { #[serde(alias = #a)] }
    });
    Some(quote! { #( #attrs )* })
}

// =============================================================================
// Default values (SPEC §11.5)
// =============================================================================

/// Classification of a field's Avro default, computed once and consumed by
/// both the serde attribute ([`FieldDefault::serde_attr`]) and the optional
/// `bon::Builder` attribute ([`FieldDefault::builder_default_inner`]).
enum FieldDefault {
    /// No Avro default declared.
    None,
    /// Rides a `Default` impl: `null` on `Option`, `[]` on array, `{}` on map.
    Empty,
    /// A literal default backed by a generated `default_<rec>_<field>` fn.
    HelperFn(Ident),
}

impl FieldDefault {
    /// `#[serde(default …)]` for this field, if any.
    fn serde_attr(&self) -> Option<TokenStream> {
        match self {
            FieldDefault::None => None,
            FieldDefault::Empty => Some(quote! { #[serde(default)] }),
            FieldDefault::HelperFn(ident) => {
                let name = ident.to_string();
                Some(quote! { #[serde(default = #name)] })
            }
        }
    }

    /// Inner tokens for a `#[builder(…)]` default (e.g. `default` or
    /// `default = default_r_x()`) when the `bon::Builder` derive is enabled,
    /// or `None` when no builder default applies. The caller wraps these in
    /// `#[builder(…)]`, possibly merged with `into` / `with`.
    ///
    /// `is_option` is true for `Option<T>` fields, where bon already makes
    /// the setter optional (defaulting to `None`) and *rejects* an explicit
    /// `#[builder(default)]` ("`Option<_>` already implies a default of
    /// `None`"). So for optional fields we emit no builder default at all —
    /// which means a non-null Avro default on an optional field is honoured
    /// by serde-deserialization but **not** by the builder (the builder
    /// yields `None`). This gap is documented in the module header.
    fn builder_default_inner(&self, is_option: bool) -> Option<TokenStream> {
        if is_option {
            return None;
        }
        match self {
            FieldDefault::None => None,
            FieldDefault::Empty => Some(quote! { default }),
            FieldDefault::HelperFn(ident) => Some(quote! { default = #ident() }),
        }
    }
}

/// Decide the `#[serde(default …)]` attribute for a field given its Avro
/// `default` (raw `serde_json::Value`) and its resolved Rust type:
///
/// | Avro default            | Handling                                         |
/// | ----------------------- | ------------------------------------------------ |
/// | `null` on `["null", T]` | `#[serde(default)]` (→ `None`)                   |
/// | `[]` on array           | `#[serde(default)]` (→ `Vec::default`)           |
/// | `{}` on map             | `#[serde(default)]` (→ `HashMap::default`)       |
/// | scalar / non-empty lit  | `#[serde(default = "default_<rec>_<field>")]` +  |
/// |                         | a `fn default_<rec>_<field>() -> T`              |
///
/// The helper fn (when needed) is pushed onto `sibling_items` so it lands in
/// the same module as the owning struct, emitted `pub(super)`.
///
/// The helper is qualified `default_<record>_<field>` so two records in the
/// same module that both default a same-named field don't collide on the free
/// fn.
#[allow(clippy::too_many_arguments)]
fn field_default(
    record_name: &str,
    field_name: &str,
    default: Option<&serde_json::Value>,
    schema: &Schema,
    field_type: &TokenStream,
    current_ns: &[String],
    registry: &HashMap<Name, &Schema>,
    sibling_items: &mut Vec<TokenStream>,
) -> Result<FieldDefault, GeneratorError> {
    let Some(default) = default else {
        return Ok(FieldDefault::None);
    };

    use serde_json::Value as J;

    // Cases that ride a `Default` impl directly — no helper fn needed.
    match default {
        // `null` default. Only meaningful on an optional (`Option<T>`) field,
        // where `Option::default()` is `None`. A `null` default on a
        // non-optional field is not representable; leave it to apache_avro's
        // own validation rather than emit an attribute that won't compile.
        J::Null => {
            if optional_payload(schema).is_some() {
                return Ok(FieldDefault::Empty);
            }
            return Ok(FieldDefault::None);
        }
        // Empty array / empty object lean on `Vec::default` / `HashMap::default`.
        J::Array(a) if a.is_empty() && matches!(schema, Schema::Array(_)) => {
            return Ok(FieldDefault::Empty);
        }
        J::Object(o) if o.is_empty() && matches!(schema, Schema::Map(_)) => {
            return Ok(FieldDefault::Empty);
        }
        _ => {}
    }

    // Everything else needs a literal Rust expression in a helper fn.
    let expr = default_expr(default, schema, current_ns, registry).ok_or_else(|| {
        GeneratorError::UnsupportedSchema {
            detail: format!(
                "default value `{default}` for field `{record_name}.{field_name}` \
                 cannot be lowered to a Rust expression in v1. Common causes: a \
                 `decimal` / `uuid` / `duration` literal, a multi-variant \
                 (non-optional) union default, or a record-literal default that \
                 omits a field with no own default (or whose value is itself \
                 unrepresentable)"
            ),
        }
    })?;

    let fn_ident = escape_ident(&format!(
        "default_{}_{}",
        record_name.to_snake_case(),
        field_name.to_snake_case()
    ));
    sibling_items.push(quote! {
        pub(super) fn #fn_ident() -> #field_type {
            #expr
        }
    });
    Ok(FieldDefault::HelperFn(fn_ident))
}

/// Default handling for a field whose Rust type is replaced by a custom logical
/// type. `Default`-backed empties ride `#[serde(default)]`:
///
/// - `null` on an optional union → `#[serde(default)]` (→ `None`),
/// - `[]` on an array → `#[serde(default)]` (→ `Vec::default`),
/// - `{}` on a map → `#[serde(default)]` (→ `HashMap::default`).
///
/// A non-empty literal default is lowered through a generated helper fn.
/// vanavro can't construct the opaque mapped `rust_type` from a
/// JSON literal directly, so the helper deserializes the Avro default's JSON
/// through the type's own `Deserialize` impl, or — when the mapping registers a
/// `serde_with` converter — through `DeserializeAsWrap<FieldType, Converter>`.
/// The helper returns the field's full container type, so both
/// `#[serde(default = …)]` and the `bon::Builder` default consume it.
fn logical_field_default(
    record_name: &str,
    field_name: &str,
    default: Option<&serde_json::Value>,
    schema: &Schema,
    ov: &FieldLogicalOverride,
    sibling_items: &mut Vec<TokenStream>,
) -> Result<FieldDefault, GeneratorError> {
    use serde_json::Value as J;

    let Some(default) = default else {
        return Ok(FieldDefault::None);
    };

    match default {
        J::Null if optional_payload(schema).is_some() => return Ok(FieldDefault::Empty),
        // A `null` default on a non-optional custom-logical field is not
        // representable; leave it (apache-avro validates the schema itself).
        J::Null => return Ok(FieldDefault::None),
        J::Array(a) if a.is_empty() && matches!(schema, Schema::Array(_)) => {
            return Ok(FieldDefault::Empty);
        }
        J::Object(o) if o.is_empty() && matches!(schema, Schema::Map(_)) => {
            return Ok(FieldDefault::Empty);
        }
        _ => {}
    }

    // Non-empty literal: build the value at runtime from the default's JSON.
    let json_lit =
        serde_json::to_string(default).map_err(|e| GeneratorError::UnsupportedSchema {
            detail: format!(
                "default for field `{record_name}.{field_name}` could not be \
             re-serialized to JSON: {e}"
            ),
        })?;
    let msg = format!("invalid default `{default}` for field `{record_name}.{field_name}`");
    let field_type = &ov.field_type;
    let expr = match &ov.default_serde_as_ty {
        Some(conv_ty) => quote! {
            ::serde_json::from_str::<
                ::serde_with::de::DeserializeAsWrap<#field_type, #conv_ty>
            >(#json_lit)
                .expect(#msg)
                .into_inner()
        },
        None => quote! {
            ::serde_json::from_str::<#field_type>(#json_lit).expect(#msg)
        },
    };

    let fn_ident = escape_ident(&format!(
        "default_{}_{}",
        record_name.to_snake_case(),
        field_name.to_snake_case()
    ));
    sibling_items.push(quote! {
        pub(super) fn #fn_ident() -> #field_type {
            #expr
        }
    });
    Ok(FieldDefault::HelperFn(fn_ident))
}

/// Whether a field's schema lowers to an `Option<T>` (the optional
/// `["null", T]` / `[T, "null"]` union shape).
fn is_optional_field(schema: &Schema) -> bool {
    optional_payload(schema).is_some()
}

/// `#[builder(into)]` for fields whose Rust type benefits from an
/// `impl Into<…>` setter. Scoped to `String` and `Option<String>`: these
/// commonly receive `&str` / `String` at call sites, and `Into` makes that
/// ergonomic without the type-inference friction that blanket `into` causes
/// on primitives (`impl Into<i32>`) or collections.
fn builder_into_attr(schema: &Schema) -> bool {
    match schema {
        Schema::String => true,
        _ => matches!(optional_payload(schema), Some(Schema::String)),
    }
}

/// Lower a `serde_json::Value` default into a Rust expression matching the
/// field's resolved type. Returns `None` for shapes v1 can't yet render, so
/// the caller can raise a clean `UnsupportedSchema`:
///
/// - `decimal` / `uuid` / `duration` / `bigdecimal` literals,
/// - defaults targeting a multi-variant (non-optional) union enum.
///
/// Record literals **are** supported: a JSON-object default lowers to a struct
/// literal via [`record_default_expr`].
///
/// Union handling note: per the Avro spec a union default is matched against
/// the union's *first* variant. v1 only renders the `["null", T]` / `[T,
/// "null"]` optional shape (via [`optional_payload`], which is order-agnostic
/// because the resulting `Option<T>` is identical either way). Genuinely
/// multi-variant unions return `None`.
fn default_expr(
    value: &serde_json::Value,
    schema: &Schema,
    current_ns: &[String],
    registry: &HashMap<Name, &Schema>,
) -> Option<TokenStream> {
    use serde_json::Value as J;

    match schema {
        Schema::Boolean => match value {
            J::Bool(b) => Some(quote! { #b }),
            _ => None,
        },
        Schema::Int | Schema::Date | Schema::TimeMillis => {
            let n = value.as_i64()?;
            let lit = Literal::i32_suffixed(i32::try_from(n).ok()?);
            Some(quote! { #lit })
        }
        Schema::Long
        | Schema::TimeMicros
        | Schema::TimestampMillis
        | Schema::TimestampMicros
        | Schema::TimestampNanos
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros
        | Schema::LocalTimestampNanos => {
            let n = value.as_i64()?;
            let lit = Literal::i64_suffixed(n);
            Some(quote! { #lit })
        }
        Schema::Float => {
            let n = value.as_f64()?;
            let lit = Literal::f32_suffixed(n as f32);
            Some(quote! { #lit })
        }
        Schema::Double => {
            let n = value.as_f64()?;
            let lit = Literal::f64_suffixed(n);
            Some(quote! { #lit })
        }
        Schema::String => match value {
            J::String(s) => Some(quote! { #s.to_string() }),
            _ => None,
        },
        // Avro encodes a `bytes` / `fixed` default as a JSON string whose
        // chars are the latin-1 byte values (one char per byte).
        Schema::Bytes => {
            let bytes = avro_bytes_from_json(value)?;
            let lit = Literal::byte_string(&bytes);
            Some(quote! { #lit.to_vec() })
        }
        Schema::Fixed(fx) => {
            let bytes = avro_bytes_from_json(value)?;
            if bytes.len() != fx.size {
                return None;
            }
            let lit = Literal::byte_string(&bytes);
            Some(quote! { *#lit })
        }
        Schema::Array(arr) => {
            let J::Array(items) = value else {
                return None;
            };
            let elems = items
                .iter()
                .map(|v| default_expr(v, &arr.items, current_ns, registry))
                .collect::<Option<Vec<_>>>()?;
            Some(quote! { vec![ #( #elems ),* ] })
        }
        Schema::Map(map) => {
            let J::Object(entries) = value else {
                return None;
            };
            let inserts = entries
                .iter()
                .map(|(k, v)| {
                    let val = default_expr(v, &map.types, current_ns, registry)?;
                    Some(quote! { m.insert(#k.to_string(), #val); })
                })
                .collect::<Option<Vec<_>>>()?;
            Some(quote! {
                {
                    let mut m = ::std::collections::HashMap::new();
                    #( #inserts )*
                    m
                }
            })
        }
        Schema::Enum(en) => {
            let J::String(sym) = value else {
                return None;
            };
            if !en.symbols.iter().any(|s| s == sym) {
                return None;
            }
            let (variant_ident, _) = rust_variant_ident(sym);
            // `relative_path` resolves to the enum type; append the variant.
            let ty = relative_path(current_ns, &en.name);
            Some(quote! { #ty::#variant_ident })
        }
        // Optional `["null", T]` / `[T, "null"]` union. A `null` default
        // lowers to `None`; any other default lowers to `Some(<T default>)`.
        // Genuinely multi-variant unions have no `optional_payload` and so
        // return `None` (unsupported), as before.
        Schema::Union(_) => {
            let payload = optional_payload(schema)?;
            if matches!(value, J::Null) {
                Some(quote! { None })
            } else {
                let inner = default_expr(value, payload, current_ns, registry)?;
                Some(quote! { Some(#inner) })
            }
        }
        // Record literal (SPEC §11.5): the default is a JSON object naming
        // some-or-all of the record's fields. Lower it to a struct literal.
        Schema::Record(rec) => record_default_expr(rec, value, current_ns, registry),
        Schema::Ref { name } => {
            let target = registry.get(name)?;
            default_expr(value, target, current_ns, registry)
        }
        // decimal / uuid / duration, etc. are not rendered in v1.
        _ => None,
    }
}

/// Lower a record `default` (a JSON object) into a struct-literal expression.
///
/// Per the Avro spec a record default supplies values for the record's fields
/// keyed by field name. vanavro follows Avro's resolution rules: for each field
/// of the record,
///
/// - if the default object names the field, that value is lowered against the
///   field's schema;
/// - otherwise the field's **own** `default` is used (the missing-field
///   fallback), lowered the same way.
///
/// A field that is neither named in the object nor carries its own default
/// makes the record default unrepresentable → `None`. In practice this state is
/// unreachable through the public API: apache-avro rejects an incomplete record
/// default at parse time (`SchemaParse`), so it never reaches codegen. The `?`
/// on the field lookup is therefore a total-function safeguard rather than a
/// reachable path — it cannot be tightened to an `unwrap` without trading a
/// clean error for a panic. The reachable failure mode is a field value that
/// itself fails to lower (e.g. a nested `uuid`/`decimal`), which propagates
/// `None` and the caller turns into a clean `UnsupportedSchema`.
fn record_default_expr(
    rec: &RecordSchema,
    value: &serde_json::Value,
    current_ns: &[String],
    registry: &HashMap<Name, &Schema>,
) -> Option<TokenStream> {
    use serde_json::Value as J;

    let J::Object(obj) = value else {
        return None;
    };

    let ty = relative_path(current_ns, &rec.name);

    let field_inits = rec
        .fields
        .iter()
        .map(|field| {
            // Prefer the value named in the default object; fall back to the
            // field's own declared default.
            let field_value = obj.get(&field.name).or(field.default.as_ref())?;
            let expr = default_expr(field_value, &field.schema, current_ns, registry)?;
            // A struct-literal field init binds the *Rust* ident; the serde
            // wire-name rename is irrelevant here (it only affects (de)serial-
            // isation, not in-memory construction), so we discard it.
            let (ident, _) = rust_field_ident(&field.name);
            Some(quote! { #ident: #expr })
        })
        .collect::<Option<Vec<_>>>()?;

    Some(quote! { #ty { #( #field_inits ),* } })
}

/// Decode an Avro JSON `bytes` / `fixed` default: a string whose Unicode
/// scalar values are the individual byte values (Avro spec). Any char above
/// `0xFF` means the default isn't a valid byte sequence.
fn avro_bytes_from_json(value: &serde_json::Value) -> Option<Vec<u8>> {
    let s = value.as_str()?;
    s.chars()
        .map(|c| u8::try_from(c as u32).ok())
        .collect::<Option<Vec<u8>>>()
}

// =============================================================================
// Naming (SPEC §11.3 + §5.2 keyword escaping)
// =============================================================================

/// Rust strict + reserved keywords escapable via the raw-identifier prefix
/// (`r#keyword`). Sourced from the Rust Reference Appendix B —
/// <https://doc.rust-lang.org/reference/keywords.html>. Covers stabilised
/// keywords through edition 2024; revisit on every edition bump because
/// any new keyword not listed here will silently flow through
/// [`Ident::new`] and then fail downstream at `syn::parse` with an
/// opaque "expected identifier, found keyword" error.
const RUST_RAW_ESCAPABLE_KEYWORDS: &[&str] = &[
    // Strict keywords
    "as", "break", "const", "continue", "else", "enum", "extern", "false", "fn", "for", "if",
    "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref", "return", "static",
    "struct", "trait", "true", "type", "unsafe", "use", "where", "while", "async", "await", "dyn",
    // Reserved (not currently used but cannot be a bare identifier)
    "abstract", "become", "box", "do", "final", "macro", "override", "priv", "typeof", "unsized",
    "virtual", "yield", "try", "gen",
];

/// Keywords that are *not* legal as raw identifiers — they bind too deeply
/// into Rust's path resolution to coexist with `r#`. They get a
/// trailing-underscore escape for these. Verified empirically against
/// `proc_macro2::Ident::new_raw` panic behaviour.
const RUST_SELF_FAMILY_KEYWORDS: &[&str] = &["self", "Self", "super", "crate"];

/// Build a valid Rust [`Ident`] from a candidate identifier string, applying
/// keyword escaping.
///
/// - Self-family keywords (`self` / `Self` / `super` / `crate`) get a
///   trailing `_` since they can't be raw-escaped.
/// - Other strict + reserved keywords get the `r#` prefix via
///   [`Ident::new_raw`].
/// - Everything else passes through unchanged.
///
/// The self-family suffix can collide with a pre-existing identifier (e.g. an
/// Avro record with both `self` and `self_` fields produces two `self_`
/// fields); `check_name_collisions` reports that as `NameCollision`.
fn escape_ident(candidate: &str) -> Ident {
    if RUST_SELF_FAMILY_KEYWORDS.contains(&candidate) {
        Ident::new(&format!("{candidate}_"), Span::call_site())
    } else if RUST_RAW_ESCAPABLE_KEYWORDS.contains(&candidate) {
        Ident::new_raw(candidate, Span::call_site())
    } else {
        Ident::new(candidate, Span::call_site())
    }
}

/// Build a serde rename string when the Rust ident no longer matches the
/// original Avro name — either because case conversion changed it or
/// because keyword escaping fired.
fn maybe_rename(original_avro: &str, ident: &Ident) -> Option<String> {
    // `Ident`'s `Display` renders raw idents with their `r#` prefix, so this
    // string compare correctly treats `r#type` as differing from `type` and
    // triggers a rename. `proc_macro2::Ident` exposes no borrow-as-`str`, so
    // the `to_string()` allocation is unavoidable here — allow cmp_owned.
    #[allow(clippy::cmp_owned)]
    if ident.to_string() == original_avro {
        None
    } else {
        Some(original_avro.to_string())
    }
}

/// Repair a `heck`-converted name into a string that [`Ident::new`] will
/// accept as a (non-wildcard) binding identifier. Two distinct `heck`
/// artefacts can otherwise crash or mis-render token emission, both stemming
/// from Avro permitting names that are legal under its own regex
/// (`[A-Za-z_][A-Za-z0-9_]*`) but degenerate after case conversion:
///
/// 1. **All-underscore names** (`_`, `__`, … — all legal Avro names, fields,
///    and enum symbols). `heck` strips leading/trailing/duplicate underscores,
///    so these convert to the **empty string**; `Ident::new("")` panics, and a
///    bare `_` is a Rust *wildcard* rather than a binding in struct-field,
///    enum-variant, and module positions. Fallback: append `_` to
///    the **original** name — the same trailing-underscore escape the
///    self-family keywords use. `_` → `__`, `__` → `___`, etc.; each is a
///    valid binding and distinct per original length.
/// 2. **Leading-digit names** (e.g. `_1`, `_42x`). `heck` strips the leading
///    underscore, leaving a digit-initial string (`"1"`, `"42x"`);
///    `Ident::new("1")` panics (`Ident cannot be a number`). Fallback: prepend
///    a single `_`, yielding a valid identifier (`_1`, `_42x`).
///
/// Scope: this repairs every name admitted by apache-avro's **default** name
/// validator (regex `[A-Za-z_][A-Za-z0-9_]*`) so it cannot panic `Ident::new`.
/// It does **not** attempt to handle non-ASCII identifiers that are only
/// reachable by overriding the validator (`apache_avro::validator::set_*_validator`)
/// — e.g. a unicode-digit-initial name like `٠1`, or an emoji that `heck`
/// reduces to the empty string. Those remain a `proc-macro2` panic and are out
/// of scope for v1; vanavro relies on the default validator.
///
/// Collisions introduced by the repair (e.g. `_1_` and `__1__` both reduce to
/// `_1`) are **not** silently merged — they survive as identical candidate
/// strings and are reported by [`check_name_collisions`] as a clean
/// `NameCollision` (the general case-fold collision backstop), never a panic.
fn sanitize_ident_candidate(converted: &str, original: &str) -> String {
    if converted.is_empty() {
        format!("{original}_")
    } else if converted.starts_with(|c: char| c.is_ascii_digit()) {
        format!("_{converted}")
    } else {
        converted.to_string()
    }
}

/// PascalCase + keyword-escaped Rust type identifier for an Avro type name.
/// Unlike the field/variant builders this does **not** return a serde rename;
/// type-level renames are computed separately at the call site via
/// [`type_rename`].
fn rust_type_ident(name: &str) -> Ident {
    escape_ident(&sanitize_ident_candidate(&name.to_pascal_case(), name))
}

/// Serde `rename` string for a type whose Rust ident no longer matches its
/// original Avro name (case conversion, keyword escaping, or ident sanitising).
fn type_rename(original: &str, transformed: &Ident) -> Option<String> {
    maybe_rename(original, transformed)
}

/// snake_case + keyword-escaped Rust field identifier, paired with the serde
/// `rename` needed to map it back to the original Avro field name.
fn rust_field_ident(original: &str) -> (Ident, Option<String>) {
    let ident = escape_ident(&sanitize_ident_candidate(
        &original.to_snake_case(),
        original,
    ));
    let rename = maybe_rename(original, &ident);
    (ident, rename)
}

/// PascalCase + keyword-escaped Rust enum-variant identifier, paired with the
/// serde `rename` needed to map it back to the original Avro enum symbol.
fn rust_variant_ident(symbol: &str) -> (Ident, Option<String>) {
    let ident = escape_ident(&sanitize_ident_candidate(&symbol.to_pascal_case(), symbol));
    let rename = maybe_rename(symbol, &ident);
    (ident, rename)
}

/// Escape a namespace segment for use as a `pub mod` identifier or path
/// segment. The segment is converted to
/// `snake_case`; keyword escaping then runs on the converted form so a
/// segment like `Loop` (snake → `loop`) still becomes `r#loop`.
fn rust_module_ident(segment: &str) -> Ident {
    escape_ident(&sanitize_ident_candidate(&segment.to_snake_case(), segment))
}

// =============================================================================
// Post-conversion collision detection (SPEC §5.2)
// =============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NameProvenance<'a> {
    User(&'a str),
    GeneratedCarrier {
        record_name: &'a str,
    },
    /// An extracted union enum. Carries a description of where the
    /// union lives so a clash points at the offending record field.
    UnionEnum {
        description: &'a str,
    },
}

impl<'a> NameProvenance<'a> {
    fn describe(&self, ident: &str) -> String {
        match self {
            NameProvenance::User(original) => original.to_string(),
            NameProvenance::GeneratedCarrier { record_name } => {
                format!("{ident} (auto-generated carrier for record `{record_name}`)")
            }
            NameProvenance::UnionEnum { description } => {
                format!("{ident} (union enum for {description})")
            }
        }
    }
}

/// A resolved extracted union enum discovered by [`collect_union_types`].
struct UnionTypeInfo {
    /// Namespace the enum is emitted into (the owning type's namespace).
    namespace: String,
    /// Final Rust identifier of the union enum.
    rust_ident: String,
    /// Human-readable origin for `NameCollision` diagnostics.
    description: String,
}

/// Union-enum collision pre-pass: resolve every extracted union enum's namespace
/// and Rust ident by driving the *real* field lowering ([`schema_to_rust_type`])
/// with a collecting [`LogicalCtx::union_sink`], so name propagation can never
/// drift from what is emitted. Emitted tokens are discarded.
fn collect_union_types(
    named: &[&Schema],
    registry: &HashMap<Name, &Schema>,
    opts: CodegenOptions,
    logical: LogicalCtx<'_>,
) -> Result<Vec<UnionTypeInfo>, GeneratorError> {
    let collected = RefCell::new(Vec::new());
    let logical = LogicalCtx {
        union_sink: Some(&collected),
        ..logical
    };
    // Only the sink's registrations matter here; the emitted tokens are thrown
    // away. Reused across fields, cleared per field to bound the peak.
    let mut discard: Vec<TokenStream> = Vec::new();

    for &schema in named {
        let Schema::Record(rec) = schema else {
            continue;
        };
        let current_ns = ns_segments(rec.name.namespace());
        let record_fqn = fqn_string(&rec.name);
        let box_targets = HashSet::new();
        let collapses = opts.collapse_single_union && record_collapses(rec, logical)?;

        for field in &rec.fields {
            discard.clear();
            // A custom-logical override replaces the field's lowering with an
            // opaque mapped type — emission never runs `schema_to_rust_type` on
            // it, so no union enum is emitted; skip it here to match.
            if resolve_field_logical(field, &record_fqn, logical)?.is_some() {
                continue;
            }
            // A collapsed record's lone union is emitted as `{Record}`, not via
            // `union_type`; lower only its variants (field_ctx = None) so nested
            // named unions still register, matching `emit_collapsed_union_record`.
            if collapses && let Schema::Union(u) = &field.schema {
                for variant in u.variants() {
                    schema_to_rust_type(
                        variant,
                        &current_ns,
                        registry,
                        &box_targets,
                        None,
                        None,
                        &mut discard,
                        logical,
                    )?;
                }
                continue;
            }
            let ctx = FieldContext::root(rec.name.name(), &record_fqn, &field.name);
            // Seed as emission does so the pre-pass registers the real ident.
            let field_name = custom_enum_name(&field.custom_attributes)?;
            schema_to_rust_type(
                &field.schema,
                &current_ns,
                registry,
                &box_targets,
                Some(ctx),
                field_name,
                &mut discard,
                logical,
            )?;
        }
    }
    Ok(collected.into_inner())
}

/// Scan every record's fields, every enum's symbols, every namespace's type
/// names, and every namespace-sibling group's module segments for two distinct
/// Avro names that fold onto the same Rust identifier after case conversion
/// (PascalCase / snake_case) or keyword escaping. A namespace's type names are
/// additionally checked against its own child module segments, which share one
/// Rust namespace with them. Returns the *first* such collision (in
/// deterministic declaration order) as a [`GeneratorError::NameCollision`],
/// or `Ok(())` when all converted names are unique.
fn check_name_collisions<'a>(
    schemas: &[&'a Schema],
    union_types: &'a [UnionTypeInfo],
    opts: CodegenOptions,
    logical: LogicalCtx<'_>,
) -> Result<(), GeneratorError> {
    // Per-namespace: type names (PascalCase) and module-segment siblings.
    // Keyed by namespace string; value lists every (converted ident,
    // provenance) in declaration order, checked later by `check_local_group`.
    let mut types_by_ns: BTreeMap<String, Vec<(String, NameProvenance<'a>)>> = BTreeMap::new();
    // Sibling module segments keyed by *parent* namespace path.
    let mut segments_by_parent: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();

    for &schema in schemas {
        let Some(name) = schema.name() else {
            continue;
        };
        let ns = name.namespace().unwrap_or_default().to_string();

        // Type name collisions within a namespace (e.g. `my_type` + `MyType`).
        let type_ident = rust_type_ident(name.name()).to_string();
        types_by_ns
            .entry(ns.clone())
            .or_default()
            .push((type_ident, NameProvenance::User(name.name())));

        // Reserve synthetic carrier suffixes for single-union collapsed records
        // — only when the collapse is enabled (`collapse_single_union`).
        if opts.collapse_single_union
            && let Schema::Record(rec) = schema
            && record_collapses(rec, logical)?
        {
            let ty_ident = rust_type_ident(name.name());
            let repr_ident = format!("{ty_ident}Repr");
            let repr_union_ident = format!("{ty_ident}ReprUnion");
            let prov = NameProvenance::GeneratedCarrier {
                record_name: name.name(),
            };

            types_by_ns
                .entry(ns.clone())
                .or_default()
                .push((repr_ident, prov));
            types_by_ns
                .entry(ns.clone())
                .or_default()
                .push((repr_union_ident, prov));
        }

        // Record fields and enum symbols collide within their own scope.
        match schema {
            Schema::Record(rec) => {
                let fqn = fqn_string(&rec.name);
                check_local_group(
                    "fields in record",
                    &fqn,
                    rec.fields.iter().map(|f| {
                        (
                            rust_field_ident(&f.name).0.to_string(),
                            NameProvenance::User(&f.name),
                        )
                    }),
                )?;
            }
            Schema::Enum(en) => {
                let fqn = fqn_string(&en.name);
                check_local_group(
                    "symbols in enum",
                    &fqn,
                    en.symbols
                        .iter()
                        .map(|s| (rust_variant_ident(s).0.to_string(), NameProvenance::User(s))),
                )?;
            }
            _ => {}
        }

        // Namespace-segment sibling collisions (`OrgX` + `org_x` → `org_x`).
        // Each adjacent pair of segments shares a parent path; siblings under
        // the same parent must convert to distinct module idents.
        let segments = ns_segments(name.namespace());
        let mut parent = String::new();
        for seg in &segments {
            let module_ident = rust_module_ident(seg).to_string();
            let siblings = segments_by_parent.entry(parent.clone()).or_default();
            match siblings.get(&module_ident) {
                Some(first) if first != seg => {
                    return Err(GeneratorError::NameCollision {
                        kind: "namespace segments".into(),
                        scope: if parent.is_empty() {
                            "<root>".into()
                        } else {
                            parent.clone()
                        },
                        rust_ident: module_ident,
                        first: first.clone(),
                        second: seg.clone(),
                    });
                }
                _ => {
                    siblings.insert(module_ident, seg.clone());
                }
            }
            if parent.is_empty() {
                parent = seg.clone();
            } else {
                parent = format!("{parent}.{seg}");
            }
        }
    }

    // SPEC §5.3: register every extracted union enum into its namespace so a
    // clash with another union — or with a record / enum / fixed of the same
    // name — is reported here rather than emitting duplicate `pub enum`s.
    for u in union_types {
        types_by_ns.entry(u.namespace.clone()).or_default().push((
            u.rust_ident.clone(),
            NameProvenance::UnionEnum {
                description: &u.description,
            },
        ));
    }

    // Now resolve the deferred per-namespace type collisions, then the same
    // idents against the namespace's *child module* segments.
    for (ns, idents) in &types_by_ns {
        let scope = if ns.is_empty() { "<root>" } else { ns };
        check_local_group(
            "types in namespace",
            scope,
            idents.iter().map(|(ident, orig)| (ident.clone(), *orig)),
        )?;

        // Types and modules share one Rust namespace, so a type ident equal to
        // a child module ident is a redefinition (`E0428`) that neither check
        // above sees. PascalCase vs snake_case usually keeps them apart, but not
        // for a `sanitize_ident_candidate` repair (`_1`, `__`) nor for a
        // verbatim `"enum"` union name (SPEC §5.3).
        let Some(siblings) = segments_by_parent.get(ns) else {
            continue;
        };
        for (ident, provenance) in idents {
            if let Some(segment) = siblings.get(ident) {
                return Err(GeneratorError::NameCollision {
                    kind: "types and namespace segments".into(),
                    scope: scope.into(),
                    first: format!("{segment} (namespace segment)"),
                    second: provenance.describe(ident),
                    rust_ident: ident.clone(),
                });
            }
        }
    }

    Ok(())
}

/// Check a single local scope (a record's fields, an enum's symbols, or a
/// namespace's type names) for converted-ident collisions, preserving the
/// declaration order so the *first* duplicate is reported deterministically.
fn check_local_group<'a>(
    kind: &str,
    scope: &str,
    items: impl IntoIterator<Item = (String, NameProvenance<'a>)>,
) -> Result<(), GeneratorError> {
    let mut seen: BTreeMap<String, NameProvenance<'a>> = BTreeMap::new();
    for (rust_ident, original) in items {
        match seen.get(&rust_ident) {
            // Same original twice is not a *conversion* collision (Avro itself
            // would have rejected genuine duplicates); only differing
            // originals folding together count. Extracted union enums (SPEC §5.3)
            // are synthetic — each registration is a distinct enum, so a matching
            // ident is always a collision regardless of the origin description.
            Some(first)
                if first != &original || matches!(original, NameProvenance::UnionEnum { .. }) =>
            {
                return Err(GeneratorError::NameCollision {
                    kind: kind.into(),
                    scope: scope.into(),
                    first: first.describe(&rust_ident),
                    second: original.describe(&rust_ident),
                    rust_ident,
                });
            }
            _ => {
                seen.insert(rust_ident, original);
            }
        }
    }
    Ok(())
}

/// Render a `Name` as its dotted fully-qualified string for diagnostics.
fn fqn_string(name: &Name) -> String {
    name.fullname(None)
}

// =============================================================================
// Structural schema construction + per-type API (SPEC §4.1 Option A, §5.5, §12)
// =============================================================================

/// Emit the per-type `schema()` / `fingerprint::<D>()` API for a named type
/// (record / collapsed single-union record / unit enum). Extracted union enums
/// emit their own `impl` inline in [`emit_union_enum`].
///
/// `schema()` returns a `&'static ::apache_avro::Schema` backed by a
/// `LazyLock` whose initializer constructs the schema **structurally** (no
/// runtime string parsing), self-contained so its
/// `fingerprint()` matches the canonical form of the equivalent parsed schema.
///
/// The type's `build_*` schema-builder fn is emitted in this same namespace
/// module, so `schema()` calls it by bare ident.
fn emit_schema_api(ty_ident: &Ident, name: &Name) -> Result<TokenStream, GeneratorError> {
    let bid = builder_ident(name)?;
    Ok(quote! {
        impl #ty_ident {
            /// Statically constructed Avro schema for this type.
            pub fn schema() -> &'static ::apache_avro::Schema {
                static SCHEMA: ::std::sync::LazyLock<::apache_avro::Schema> =
                    ::std::sync::LazyLock::new(|| {
                        #bid(&mut ::std::collections::HashSet::new())
                    });
                &SCHEMA
            }

            /// Fingerprint of [`Self::schema`] under the supplied digest
            /// (e.g. `::apache_avro::rabin::Rabin`).
            pub fn fingerprint<D: ::digest::Digest>(
            ) -> ::apache_avro::schema::SchemaFingerprint {
                Self::schema().fingerprint::<D>()
            }
        }

        impl ::apache_avro::AvroSchema for #ty_ident {
            fn get_schema() -> ::apache_avro::Schema {
                Self::schema().clone()
            }
        }
    })
}

/// Per-type API for a `fixed` type, which lowers to a `pub type X = [u8; N];`
/// alias and therefore cannot host an `impl`. Emit sibling free functions
/// `{snake}_schema()` / `{snake}_fingerprint::<D>()` in the same module.
fn emit_fixed_schema_api(ty_ident: &Ident, name: &Name) -> Result<TokenStream, GeneratorError> {
    let bid = builder_ident(name)?;
    let schema_fn = escape_ident(&format!("{}_schema", ty_ident.to_string().to_snake_case()));
    let fingerprint_fn = escape_ident(&format!(
        "{}_fingerprint",
        ty_ident.to_string().to_snake_case()
    ));
    let schema_doc = format!(" Statically constructed Avro schema for [`{ty_ident}`].");
    let fp_doc = format!(" Fingerprint of [`{schema_fn}`] under digest `D`.");
    Ok(quote! {
        #[doc = #schema_doc]
        pub fn #schema_fn() -> &'static ::apache_avro::Schema {
            static SCHEMA: ::std::sync::LazyLock<::apache_avro::Schema> =
                ::std::sync::LazyLock::new(|| {
                    #bid(&mut ::std::collections::HashSet::new())
                });
            &SCHEMA
        }

        #[doc = #fp_doc]
        pub fn #fingerprint_fn<D: ::digest::Digest>(
        ) -> ::apache_avro::schema::SchemaFingerprint {
            #schema_fn().fingerprint::<D>()
        }
    })
}

/// Emit the shared `build_*` schema-builder fn for one named type, placed in its
/// own namespace module (`current_ns`, the module `emit_module_body` is filling)
/// rather than at a fixed root. Each fn constructs that type's `Schema` node and
/// threads a runtime `defined` set; named field/ref types are built by calling
/// *their* builders (resolved via [`builder_call_path`]), so the whole set is
/// O(total anonymous schema nodes) — no per-type inlining.
///
/// The builder prologue reproduces the old compile-time DFS: insert
/// the name into `defined`; on a repeat visit (recursion / diamond) return a
/// `Schema::Ref` back-edge instead of re-expanding. This keeps every
/// `fingerprint()` byte-identical to the previous inlined output.
///
/// Living in the type's own namespace module means a `build_*` symbol collides
/// across two `include!`d generated outputs exactly when the module-scoped types
/// already would — never independently of them, bar a referenced
/// suppressed record, which gets a builder but no Rust type.
///
/// Builders are `pub(crate)` rather than `pub`: they are an internal implementation detail of
/// `schema()`, reachable across namespace modules within the generated output
/// but not part of its public surface, so scoping them to the crate keeps that
/// surface minimal.
fn emit_schema_builder(
    name: &Name,
    registry: &HashMap<Name, &Schema>,
    current_ns: &[String],
) -> Result<TokenStream, GeneratorError> {
    let schema = registry
        .get(name)
        .ok_or_else(|| GeneratorError::UnsupportedSchema {
            detail: format!(
                "no registered schema for named type `{}`",
                name.fullname(None)
            ),
        })?;
    let bid = builder_ident(name)?;
    let name_ctor = emit_name(name);
    // The prologue already binds `let name = Name::new(…)` and inserts a clone
    // into `defined`, so the ctor moves that local for its `name:` field instead
    // of re-parsing the FQN through `Name::new` (a `regex_lite` validation, the
    // whole runtime regression). Emitting the `name` shorthand (rather than
    // `name: name`) keeps consumers' generated files clippy-clean under the
    // warn-by-default `redundant_field_names` lint.
    let name_field = quote! { name };
    let body = match schema {
        Schema::Record(rec) => emit_record_schema_ctor(rec, current_ns, name_field)?,
        Schema::Enum(en) => emit_enum_schema_ctor(en, name_field),
        Schema::Fixed(fx) => emit_fixed_schema_ctor(fx, name_field),
        other => {
            return Err(GeneratorError::UnsupportedSchema {
                detail: format!(
                    "schema-builder requested for non-named type `{}`: {other:?}",
                    name.fullname(None)
                ),
            });
        }
    };
    Ok(quote! {
        // Builder idents are PascalCase-derived (non_snake_case). `dead_code` is
        // belt-and-braces: only reachable builders are emitted (see
        // `reachable_builders`), but scope both allows per fn regardless.
        #[allow(dead_code, non_snake_case)]
        pub(crate) fn #bid(
            defined: &mut ::std::collections::HashSet<::apache_avro::schema::Name>,
        ) -> ::apache_avro::Schema {
            let name = #name_ctor;
            if !defined.insert(name.clone()) {
                return ::apache_avro::Schema::Ref { name };
            }
            #body
        }
    })
}

/// Recursively emit a Rust expression that constructs `schema` as an
/// `::apache_avro::Schema` value.
///
/// Named types (record / enum / fixed) are **not** inlined here: this fn emits
/// a call to the named type's builder fn (see `emit_schema_builder`), resolved
/// from the call site's namespace `current_ns` via [`builder_call_path`] and
/// threading the runtime `defined` set. The first-occurrence/back-reference
/// decision — which used to happen here at compile time — now happens at run
/// time inside the builder itself (its prologue inserts into `defined` and
/// returns `Schema::Ref { name }` on a repeat visit). This denormalised,
/// self-contained shape is what makes the fingerprint cross-language correct:
/// Avro's parsing canonical form treats the first textual occurrence of a name
/// as its definition and any later occurrence as a reference; deferring to the
/// builder reproduces that exactly while keeping generated output O(N) instead
/// of O(N^2).
fn emit_schema_ctor(schema: &Schema, current_ns: &[String]) -> Result<TokenStream, GeneratorError> {
    let tokens = match schema {
        Schema::Null => quote! { ::apache_avro::Schema::Null },
        Schema::Boolean => quote! { ::apache_avro::Schema::Boolean },
        Schema::Int => quote! { ::apache_avro::Schema::Int },
        Schema::Long => quote! { ::apache_avro::Schema::Long },
        Schema::Float => quote! { ::apache_avro::Schema::Float },
        Schema::Double => quote! { ::apache_avro::Schema::Double },
        Schema::Bytes => quote! { ::apache_avro::Schema::Bytes },
        Schema::String => quote! { ::apache_avro::Schema::String },
        Schema::Uuid(u) => emit_uuid_schema_ctor(u),
        Schema::Date => quote! { ::apache_avro::Schema::Date },
        Schema::TimeMillis => quote! { ::apache_avro::Schema::TimeMillis },
        Schema::TimeMicros => quote! { ::apache_avro::Schema::TimeMicros },
        Schema::TimestampMillis => quote! { ::apache_avro::Schema::TimestampMillis },
        Schema::TimestampMicros => quote! { ::apache_avro::Schema::TimestampMicros },
        Schema::TimestampNanos => quote! { ::apache_avro::Schema::TimestampNanos },
        Schema::LocalTimestampMillis => quote! { ::apache_avro::Schema::LocalTimestampMillis },
        Schema::LocalTimestampMicros => quote! { ::apache_avro::Schema::LocalTimestampMicros },
        Schema::LocalTimestampNanos => quote! { ::apache_avro::Schema::LocalTimestampNanos },
        Schema::BigDecimal => quote! { ::apache_avro::Schema::BigDecimal },
        Schema::Duration(fx) => {
            let name_ctor = emit_name(&fx.name);
            let inner = emit_fixed_schema_struct(fx, quote! { name: #name_ctor });
            quote! { ::apache_avro::Schema::Duration(#inner) }
        }

        Schema::Array(arr) => {
            let items = emit_schema_ctor(&arr.items, current_ns)?;
            let attrs = emit_attributes(&arr.attributes);
            quote! {
                ::apache_avro::Schema::Array(::apache_avro::schema::ArraySchema {
                    items: Box::new(#items),
                    attributes: #attrs,
                })
            }
        }
        Schema::Map(map) => {
            let types = emit_schema_ctor(&map.types, current_ns)?;
            let attrs = emit_attributes(&map.attributes);
            quote! {
                ::apache_avro::Schema::Map(::apache_avro::schema::MapSchema {
                    types: Box::new(#types),
                    attributes: #attrs,
                })
            }
        }
        Schema::Union(u) => {
            let variants = u
                .variants()
                .iter()
                .map(|v| emit_schema_ctor(v, current_ns))
                .collect::<Result<Vec<_>, _>>()?;
            quote! {
                ::apache_avro::Schema::Union(
                    ::apache_avro::schema::UnionSchema::new(vec![ #( #variants ),* ])
                        .expect("vanavro: structurally constructed union is valid")
                )
            }
        }
        Schema::Decimal(dec) => {
            let precision = Literal::usize_unsuffixed(dec.precision);
            let scale = Literal::usize_unsuffixed(dec.scale);
            let inner = emit_inner_decimal_schema(&dec.inner);
            quote! {
                ::apache_avro::Schema::Decimal(::apache_avro::schema::DecimalSchema {
                    precision: #precision,
                    scale: #scale,
                    inner: #inner,
                })
            }
        }

        // A named type: defer to its shared builder, resolved from the call
        // site's namespace `current_ns` to the type's own namespace module
        // (SPEC §12.1), threading the runtime `defined` set. The builder decides
        // inline-vs-back-reference at run time, reproducing the old per-tree DFS
        // exactly. `Schema::name()` returns `Some` for each of these variants.
        Schema::Record(_) | Schema::Enum(_) | Schema::Fixed(_) => {
            let name = schema
                .name()
                .expect("record / enum / fixed is a named schema");
            let call = builder_call_path(current_ns, name)?;
            quote! { #call(defined) }
        }

        // A bare reference resolves to the target's builder, exactly like a
        // named type. The target is always registered: `schema::parse` rejects
        // dangling refs with `UnresolvedReference` before codegen runs (see
        // `check_name_ref`), so a ref reaching here has an entry.
        Schema::Ref { name } => {
            let call = builder_call_path(current_ns, name)?;
            quote! { #call(defined) }
        }
    };
    Ok(tokens)
}

/// Whether [`emit_schema_ctor`] would emit a `build_*(defined)` call anywhere
/// in this schema tree — a named type or a bare ref, including one nested in an
/// array / map / union variant (all of which thread the same `defined` set).
/// Used to decide whether a union's `schema()` must bind `defined` at all: an
/// all-primitive union references it nowhere, so binding it would be a genuinely
/// unused local. A `Schema::Ref` counts even if unresolved — treating it as
/// consuming keeps this in lockstep with `emit_schema_ctor`'s resolved arm.
fn schema_ctor_uses_defined(schema: &Schema) -> bool {
    match schema {
        Schema::Record(_) | Schema::Enum(_) | Schema::Fixed(_) | Schema::Ref { .. } => true,
        Schema::Array(arr) => schema_ctor_uses_defined(&arr.items),
        Schema::Map(map) => schema_ctor_uses_defined(&map.types),
        Schema::Union(u) => u.variants().iter().any(schema_ctor_uses_defined),
        _ => false,
    }
}

/// `name_field` is the full `name:` field token stream. Inside a schema-builder
/// it is the `name` shorthand for the already-bound `name` local (moved, not
/// re-parsed, and shorthand to dodge the `redundant_field_names` lint in
/// consumers' generated files).
fn emit_record_schema_ctor(
    rec: &RecordSchema,
    current_ns: &[String],
    name_field: TokenStream,
) -> Result<TokenStream, GeneratorError> {
    let aliases = emit_aliases(&rec.aliases);
    let doc = emit_documentation(&rec.doc);
    let fields = rec
        .fields
        .iter()
        .map(|f| emit_record_field(f, current_ns))
        .collect::<Result<Vec<_>, _>>()?;
    let lookup = emit_lookup(rec);
    let attributes = emit_attributes(&rec.attributes);
    Ok(quote! {
        ::apache_avro::Schema::Record(::apache_avro::schema::RecordSchema {
            #name_field,
            aliases: #aliases,
            doc: #doc,
            fields: vec![ #( #fields ),* ],
            lookup: #lookup,
            attributes: #attributes,
        })
    })
}

/// See [`emit_record_schema_ctor`] for `name_field`.
fn emit_enum_schema_ctor(en: &EnumSchema, name_field: TokenStream) -> TokenStream {
    let aliases = emit_aliases(&en.aliases);
    let doc = emit_documentation(&en.doc);
    let symbols = en.symbols.iter().map(|s| quote! { #s.to_string() });
    let default = match &en.default {
        Some(d) => quote! { Some(#d.to_string()) },
        None => quote! { None },
    };
    let attributes = emit_attributes(&en.attributes);
    quote! {
        ::apache_avro::Schema::Enum(::apache_avro::schema::EnumSchema {
            #name_field,
            aliases: #aliases,
            doc: #doc,
            symbols: vec![ #( #symbols ),* ],
            default: #default,
            attributes: #attributes,
        })
    }
}

/// See [`emit_record_schema_ctor`] for `name_field`.
fn emit_fixed_schema_ctor(fx: &FixedSchema, name_field: TokenStream) -> TokenStream {
    let inner = emit_fixed_schema_struct(fx, name_field);
    quote! { ::apache_avro::Schema::Fixed(#inner) }
}

/// Emit the bare `FixedSchema { … }` struct literal (no `Schema::Fixed`
/// wrapper), reused by the named-fixed, `duration`, uuid-on-fixed, and
/// decimal-on-fixed ctors.
/// See [`emit_record_schema_ctor`] for `name_field` (inline callers — duration /
/// uuid-on-fixed / decimal-on-fixed — pass `name: #emit_name(...)`).
fn emit_fixed_schema_struct(fx: &FixedSchema, name_field: TokenStream) -> TokenStream {
    let aliases = emit_aliases(&fx.aliases);
    let doc = emit_documentation(&fx.doc);
    let size = Literal::usize_unsuffixed(fx.size);
    let attributes = emit_attributes(&fx.attributes);
    quote! {
        ::apache_avro::schema::FixedSchema {
            #name_field,
            aliases: #aliases,
            doc: #doc,
            size: #size,
            attributes: #attributes,
        }
    }
}

/// Emit the `UuidSchema` payload for a `Schema::Uuid` (string / bytes / fixed).
fn emit_uuid_schema_ctor(uuid: &apache_avro::schema::UuidSchema) -> TokenStream {
    use apache_avro::schema::UuidSchema;
    let inner = match uuid {
        UuidSchema::Bytes => quote! { ::apache_avro::schema::UuidSchema::Bytes },
        UuidSchema::String => quote! { ::apache_avro::schema::UuidSchema::String },
        UuidSchema::Fixed(fx) => {
            let name_ctor = emit_name(&fx.name);
            let fixed = emit_fixed_schema_struct(fx, quote! { name: #name_ctor });
            quote! { ::apache_avro::schema::UuidSchema::Fixed(#fixed) }
        }
    };
    quote! { ::apache_avro::Schema::Uuid(#inner) }
}

/// Emit the `InnerDecimalSchema` payload of a `Schema::Decimal` (bytes / fixed).
fn emit_inner_decimal_schema(inner: &apache_avro::schema::InnerDecimalSchema) -> TokenStream {
    use apache_avro::schema::InnerDecimalSchema;
    match inner {
        InnerDecimalSchema::Bytes => quote! { ::apache_avro::schema::InnerDecimalSchema::Bytes },
        InnerDecimalSchema::Fixed(fx) => {
            let name_ctor = emit_name(&fx.name);
            let fixed = emit_fixed_schema_struct(fx, quote! { name: #name_ctor });
            quote! { ::apache_avro::schema::InnerDecimalSchema::Fixed(#fixed) }
        }
    }
}

fn emit_record_field(
    field: &RecordField,
    current_ns: &[String],
) -> Result<TokenStream, GeneratorError> {
    let name = &field.name;
    let doc = emit_documentation(&field.doc);
    let alias_items = field.aliases.iter().map(|a| quote! { #a.to_string() });
    let aliases = quote! { vec![ #( #alias_items ),* ] };
    let default = match &field.default {
        Some(v) => {
            let val = emit_json_value(v);
            quote! { Some(#val) }
        }
        None => quote! { None },
    };
    let schema = emit_schema_ctor(&field.schema, current_ns)?;
    let custom = emit_field_attributes(&field.custom_attributes);
    Ok(quote! {
        ::apache_avro::schema::RecordField {
            name: #name.to_string(),
            doc: #doc,
            aliases: #aliases,
            default: #default,
            schema: #schema,
            custom_attributes: #custom,
        }
    })
}

fn emit_name(name: &Name) -> TokenStream {
    // `Name`'s fields are private; construct it from its fully-qualified string
    // via the public `Name::new` (which parses the namespace back out).
    let full = name.fullname(None);
    quote! {
        ::apache_avro::schema::Name::new(#full)
            .expect("vanavro: structurally valid schema name")
    }
}

/// Deterministic, **injective** mapping from an Avro fully-qualified name to a
/// unique Rust identifier for its schema builder fn (see `emit_schema_builder`).
///
/// The FQN alphabet is `[A-Za-z0-9_.]`. We escape the two separators so no two
/// distinct FQNs can fold together: `_` -> `_0`, `.` -> `_1`. This makes the
/// mangled **body** a prefix-free escape encoding — every `_` in it introduces
/// a two-char escape, so the body decodes back to exactly one FQN and distinct
/// FQNs yield distinct bodies. The constant 6-char `build_` prefix strips by
/// position (it is not part of the encoding), so the whole ident is injective;
/// its own bare `_` does not break that. The prefix's purpose is to guarantee a
/// *valid* ident regardless of the name: never empty, and never a bare keyword
/// (`build_loop` is fine where `loop` would not parse). Avro's own name regex
/// (`[A-Za-z_][A-Za-z0-9_]*`) already forbids a digit-initial segment.
///
/// An Avro schema is *runtime* input (H-PANICS-SIGNAL-BUGS), and a caller-
/// installed name validator may admit characters `syn` rejects as identifiers,
/// so parse rather than panic: a non-ident mangling surfaces as a clean
/// [`GeneratorError`] instead of an `Ident::new` panic deep in codegen.
///
/// Injectivity is over the *fully-qualified name*. apache-avro 0.22 stores a
/// `Name` as its fullname string, so two `Name`s with the same fullname compare
/// equal and injectivity over `Name` follows directly.
fn builder_ident(name: &Name) -> Result<Ident, GeneratorError> {
    let full = name.fullname(None);
    let mut mangled = String::from("build_");
    for ch in full.chars() {
        match ch {
            '_' => mangled.push_str("_0"),
            '.' => mangled.push_str("_1"),
            other => mangled.push(other),
        }
    }
    syn::parse_str::<Ident>(&mangled).map_err(|e| GeneratorError::UnsupportedSchema {
        detail: format!(
            "schema name `{full}` has no valid Rust builder identifier (`{mangled}`): {e}"
        ),
    })
}

fn emit_aliases(aliases: &Option<Vec<apache_avro::schema::Alias>>) -> TokenStream {
    match aliases {
        Some(list) => {
            let items = list.iter().map(|a| {
                // `Alias` is constructed from its (possibly namespaced) string
                // via the public fallible `Alias::new` (the infallible
                // `From<&str>` is absent in apache-avro 0.22; 0.21.0 still had it).
                let full = a.fullname(None);
                quote! {
                    ::apache_avro::schema::Alias::new(#full)
                        .expect("vanavro: structurally valid alias")
                }
            });
            quote! {
                Some(vec![ #( #items ),* ])
            }
        }
        None => quote! { None },
    }
}

fn emit_documentation(doc: &Option<String>) -> TokenStream {
    match doc {
        Some(d) => quote! { Some(#d.to_string()) },
        None => quote! { None },
    }
}

/// Build a `BTreeMap<String, serde_json::Value>` literal reproducing `attrs`
/// verbatim. Used for every schema object other than a record field: on those,
/// an `"enum"` key is either already rejected (`array` / `map`) or inert user
/// metadata (`record` / `enum` / `fixed`) that must survive into `schema()`.
/// Record **fields** go through [`emit_field_attributes`] instead.
fn emit_attributes(attrs: &BTreeMap<String, serde_json::Value>) -> TokenStream {
    emit_attribute_map(attrs.iter())
}

/// As [`emit_attributes`], for a **record field**'s `custom_attributes`: the
/// `"enum"` union-naming hint is vanavro-internal and is stripped here so it
/// never reaches the emitted `schema()`. Every other key,
/// including a sibling custom attribute on the same field, survives.
fn emit_field_attributes(attrs: &BTreeMap<String, serde_json::Value>) -> TokenStream {
    emit_attribute_map(attrs.iter().filter(|&(k, _)| k != "enum"))
}

fn emit_attribute_map<'a>(
    attrs: impl Iterator<Item = (&'a String, &'a serde_json::Value)>,
) -> TokenStream {
    let inserts: Vec<TokenStream> = attrs
        .map(|(k, v)| {
            let val = emit_json_value(v);
            quote! { (#k.to_string(), #val) }
        })
        .collect();
    if inserts.is_empty() {
        return quote! { ::std::collections::BTreeMap::new() };
    }
    quote! {
        ::std::collections::BTreeMap::from([ #( #inserts ),* ])
    }
}

/// Build the record `lookup` table (`field name -> position`) literal.
fn emit_lookup(rec: &RecordSchema) -> TokenStream {
    if rec.fields.is_empty() {
        return quote! { ::std::collections::BTreeMap::new() };
    }
    let entries = rec.fields.iter().enumerate().map(|(i, f)| {
        let name = &f.name;
        let pos = Literal::usize_unsuffixed(i);
        quote! { (#name.to_string(), #pos) }
    });
    quote! {
        ::std::collections::BTreeMap::from([ #( #entries ),* ])
    }
}

/// Emit a `serde_json::Value` literal mirroring `value`.
fn emit_json_value(value: &serde_json::Value) -> TokenStream {
    use serde_json::Value as J;
    match value {
        J::Null => quote! { ::serde_json::Value::Null },
        J::Bool(b) => quote! { ::serde_json::Value::Bool(#b) },
        J::Number(n) => {
            // Suffix the integer literals: `Number::from` is generic over every
            // integer width, so an unsuffixed literal would default to `i32` and
            // overflow for values outside its range (e.g. a `long` default).
            if let Some(i) = n.as_i64() {
                let lit = Literal::i64_suffixed(i);
                quote! { ::serde_json::Value::Number(::serde_json::Number::from(#lit)) }
            } else if let Some(u) = n.as_u64() {
                let lit = Literal::u64_suffixed(u);
                quote! { ::serde_json::Value::Number(::serde_json::Number::from(#lit)) }
            } else {
                let f = n.as_f64().unwrap_or(0.0);
                let lit = Literal::f64_unsuffixed(f);
                quote! {
                    ::serde_json::Value::Number(
                        ::serde_json::Number::from_f64(#lit)
                            .expect("vanavro: finite JSON number default")
                    )
                }
            }
        }
        J::String(s) => quote! { ::serde_json::Value::String(#s.to_string()) },
        J::Array(items) => {
            let elems = items.iter().map(emit_json_value);
            quote! { ::serde_json::Value::Array(vec![ #( #elems ),* ]) }
        }
        J::Object(map) => {
            let entries = map.iter().map(|(k, v)| {
                let val = emit_json_value(v);
                quote! { (#k.to_string(), #val) }
            });
            quote! {
                ::serde_json::Value::Object(
                    ::serde_json::Map::from_iter([ #( #entries ),* ])
                )
            }
        }
    }
}

// =============================================================================
// Relative module paths (SPEC §6.2)
// =============================================================================

fn relative_path(current_ns: &[String], target: &Name) -> TokenStream {
    let name_ident = rust_type_ident(target.name());
    module_relative_path(current_ns, target, name_ident)
}

/// `super::`-up / `mod::`-down module prefix from `current_ns` to `target`'s
/// namespace, terminated by `terminal`. Shared by [`relative_path`] (type refs)
/// and [`builder_call_path`] (schema-builder calls); the
/// two differ only in the terminal ident (the type vs its `build_*` fn).
fn module_relative_path(current_ns: &[String], target: &Name, terminal: Ident) -> TokenStream {
    let target_segments = ns_segments(target.namespace());
    let common = current_ns
        .iter()
        .zip(target_segments.iter())
        .take_while(|(a, b)| a == b)
        .count();
    let supers = current_ns.len() - common;
    let down: Vec<&String> = target_segments[common..].iter().collect();

    let super_tokens = (0..supers).map(|_| quote! { super:: });
    let down_tokens = down.iter().map(|seg| {
        let id = rust_module_ident(seg);
        quote! { #id :: }
    });

    quote! {
        #( #super_tokens )* #( #down_tokens )* #terminal
    }
}

/// Path to `target`'s schema-builder fn (`build_*`) from a call site in
/// namespace `current_ns`. Each builder lives in its type's own namespace
/// module, so the call resolves through the same relative-module
/// machinery as a type reference — no fixed root prefix, so nothing collides
/// across `include!`d outputs beyond what the module scoping already governs
/// (bar a referenced suppressed record).
fn builder_call_path(current_ns: &[String], target: &Name) -> Result<TokenStream, GeneratorError> {
    Ok(module_relative_path(
        current_ns,
        target,
        builder_ident(target)?,
    ))
}

// =============================================================================
// Eq + Hash uplift (SPEC §11.1)
// =============================================================================

/// Set of record names whose generated `struct` qualifies for the extra
/// `Eq + Hash` derives because every field's Rust type is itself `Eq + Hash`.
type EqHashRecords = HashSet<Name>;

/// Reject a `force_eq_hash` mapping applied to a `map` value position:
/// the field lowers to `HashMap<_, _>`, which is never
/// `Hash` regardless of the value type, so the flag could not take effect.
/// Surfaced as [`GeneratorError::ForceEqHashOnMap`] rather than ignored.
fn check_force_eq_hash_shapes(
    schemas: &[&Schema],
    logical: LogicalCtx<'_>,
) -> Result<(), GeneratorError> {
    for &schema in schemas {
        let Schema::Record(rec) = schema else {
            continue;
        };
        let record_fqn = fqn_string(&rec.name);
        for field in &rec.fields {
            if let Some((LogicalShape::Map, lt, mapping)) =
                field_logical_mapping(field, &record_fqn, logical)
                && mapping.eq_hash
            {
                return Err(GeneratorError::ForceEqHashOnMap {
                    logical_type: lt.to_string(),
                    field: field.name.clone(),
                    record: record_fqn,
                });
            }
        }
    }
    Ok(())
}

/// Decide which record structs may additionally derive `Eq + Hash`.
/// A record qualifies iff *every* field is `Eq + Hash`.
///
/// Record-typed fields make this mutually recursive: `A` qualifies only if
/// the records it references qualify. We compute the greatest fixpoint —
/// start assuming **all** records qualify, then repeatedly drop any record
/// that has a field which is definitely not `Eq + Hash`, until stable. This
/// is cycle-safe: a cycle of records whose every (non-record) field is
/// `Eq + Hash` stays in the set, which is correct because `derive(Eq, Hash)`
/// works fine across `Box`ed recursive references.
fn compute_eq_hash_records(
    schemas: &[&Schema],
    registry: &HashMap<Name, &Schema>,
    logical: LogicalCtx<'_>,
) -> EqHashRecords {
    // Seed with every record name; iteratively remove the disqualified.
    let mut qualifying: EqHashRecords = schemas
        .iter()
        .filter_map(|s| match s {
            Schema::Record(rec) => Some(rec.name.clone()),
            _ => None,
        })
        .collect();

    loop {
        let mut changed = false;
        // Snapshot to compare against while we mutate `qualifying`.
        let current = qualifying.clone();
        for &schema in schemas {
            let Schema::Record(rec) = schema else {
                continue;
            };
            if !qualifying.contains(&rec.name) {
                continue;
            }
            let record_fqn = fqn_string(&rec.name);
            let all_ok = rec.fields.iter().all(|f| {
                // SPEC §15: a custom-logical override replaces the field's Rust
                // type with an opaque `rust_type` whose `Eq`/`Hash` impls
                // vanavro can't see. Conservatively disqualify the record
                // (matching float / map / Decimal fields) *unless* the
                // registration forces the uplift. The shape still governs: a
                // `map` value lowers to `HashMap<_, _>`, which is never `Hash`
                // regardless of the value type, so it always disqualifies (as a
                // plain map field would) — a *forced* map value is rejected
                // upstream by `check_force_eq_hash_shapes`, so here it can only
                // be unforced. `Vec<Conv>` / `Option<Conv>` are `Eq + Hash` iff
                // `Conv` is, so the forced flag applies there.
                if let Some((shape, _, mapping)) = field_logical_mapping(f, &record_fqn, logical) {
                    return match shape {
                        LogicalShape::Map => false,
                        _ => mapping.eq_hash,
                    };
                }
                is_eq_hash(&f.schema, registry, &current)
            });
            if !all_ok {
                qualifying.remove(&rec.name);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    qualifying
}

/// Whether the Rust type lowered from `schema` is `Eq + Hash`, given the
/// current set of records assumed to qualify.
///
/// Notable non-qualifiers:
/// - `f32` / `f64` (float / double) — neither `Eq` nor `Hash`.
/// - `HashMap<_, _>` (Avro `map`) — `Eq` but std `HashMap` is not `Hash`.
/// - `::apache_avro::Decimal` / `Duration` — no `Hash` impl.
/// - `::apache_avro::BigDecimal` — implements `Hash`, but conservatively
///   treated as a non-qualifier.
/// - multi-variant union sibling enums — emitted without `Eq`/`Hash`,
///   so any field carrying one disqualifies its record.
fn is_eq_hash(
    schema: &Schema,
    registry: &HashMap<Name, &Schema>,
    qualifying: &EqHashRecords,
) -> bool {
    match schema {
        // `Eq + Hash` primitives and integer-backed logical types.
        Schema::Null
        | Schema::Boolean
        | Schema::Int
        | Schema::Long
        | Schema::Bytes
        | Schema::String
        | Schema::Uuid(_)
        | Schema::Date
        | Schema::TimeMillis
        | Schema::TimeMicros
        | Schema::TimestampMillis
        | Schema::TimestampMicros
        | Schema::TimestampNanos
        | Schema::LocalTimestampMillis
        | Schema::LocalTimestampMicros
        | Schema::LocalTimestampNanos => true,

        // Floating point: no `Eq`, no `Hash`.
        Schema::Float | Schema::Double => false,

        // `Decimal` / `Duration` lack a `Hash` impl; `BigDecimal` has one but
        // is conservatively excluded.
        Schema::Decimal(_) | Schema::BigDecimal | Schema::Duration(_) => false,

        // `Vec<T>` is `Eq + Hash` iff `T` is.
        Schema::Array(arr) => is_eq_hash(&arr.items, registry, qualifying),

        // `HashMap<String, T>` is never `Hash` (std impl), so a map field
        // always disqualifies the record.
        Schema::Map(_) => false,

        // Optional `[null, T]` → `Option<T>`: propagate to T. Any other
        // union becomes a sibling enum emitted without `Eq`/`Hash`, so it
        // disqualifies the owning record.
        Schema::Union(_) => match optional_payload(schema) {
            Some(payload) => is_eq_hash(payload, registry, qualifying),
            None => false,
        },

        // Unit enums always derive `Eq + Hash`; fixed lowers to `[u8; N]`
        // which is `Eq + Hash`.
        Schema::Enum(_) | Schema::Fixed(_) => true,

        // Inline / referenced records: look up the qualifying set.
        Schema::Record(rec) => qualifying.contains(&rec.name),
        Schema::Ref { name } => match registry.get(name) {
            Some(Schema::Record(_)) => qualifying.contains(name),
            // Enum / fixed refs are always `Eq + Hash`.
            Some(Schema::Enum(_)) | Some(Schema::Fixed(_)) => true,
            // Unknown ref: be conservative.
            _ => false,
        },
    }
}

// =============================================================================
// Cycle detection — Box insertion per SPEC §5.4
// =============================================================================

/// For each record, the set of names that record's fields reference where the
/// reference participates in an infinite-size cycle. Boxing only these
/// references breaks the cycle without over-boxing through `Vec` / `HashMap`,
/// which already heap-allocate their contents.
type CyclePartners = HashMap<Name, HashSet<Name>>;

fn build_cycle_partners(schemas: &[&Schema], registry: &HashMap<Name, &Schema>) -> CyclePartners {
    // Direct dependency graph: A → B iff A has a field whose Rust type
    // *directly* contains B (or `Option<B>`, since `Option` stores its payload
    // inline). Cycles through `Vec` / `HashMap` are intentionally excluded.
    let mut graph: HashMap<Name, HashSet<Name>> = HashMap::new();
    for &schema in schemas {
        if let Schema::Record(rec) = schema {
            let mut deps = HashSet::new();
            for field in &rec.fields {
                collect_direct_record_deps(&field.schema, registry, &mut deps);
            }
            graph.insert(rec.name.clone(), deps);
        }
    }

    // For each (from, to) edge, keep `to` only if it can reach back to `from`
    // through the same direct/Option graph — i.e. they share a non-trivial SCC.
    let mut partners = CyclePartners::new();
    for (from, deps) in &graph {
        let mut box_set: HashSet<Name> = HashSet::new();
        for to in deps {
            if can_reach(to, from, &graph) {
                box_set.insert(to.clone());
            }
        }
        partners.insert(from.clone(), box_set);
    }
    partners
}

fn collect_direct_record_deps(
    schema: &Schema,
    registry: &HashMap<Name, &Schema>,
    deps: &mut HashSet<Name>,
) {
    match schema {
        // Only record-typed refs contribute to cycles; enum / fixed refs
        // can't loop back through other records.
        Schema::Ref { name } if matches!(registry.get(name), Some(Schema::Record(_))) => {
            deps.insert(name.clone());
        }
        Schema::Record(rec) => {
            deps.insert(rec.name.clone());
        }
        Schema::Union(u) => {
            // Every variant of a union sits inline regardless of whether
            // the union becomes `Option<T>` (optional shape) or a custom
            // `{Record}Union` enum (multi-variant shape). A recursive
            // ref reachable through any arm needs the same `Box` treatment.
            for variant in u.variants() {
                collect_direct_record_deps(variant, registry, deps);
            }
        }
        _ => {
            // Vec/Map/primitives/logical types all break cycles structurally.
        }
    }
}

fn can_reach(from: &Name, to: &Name, graph: &HashMap<Name, HashSet<Name>>) -> bool {
    let mut stack: Vec<&Name> = vec![from];
    let mut visited: HashSet<&Name> = HashSet::new();
    while let Some(node) = stack.pop() {
        if !visited.insert(node) {
            continue;
        }
        if node == to {
            return true;
        }
        if let Some(deps) = graph.get(node) {
            stack.extend(deps);
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts() -> CodegenOptions {
        CodegenOptions {
            non_exhaustive: true,
            builder: false,
            collapse_single_union: true,
            allow_unregistered_logical_types: false,
        }
    }

    // Test shim mirroring the pre-§15 `generate(schemas, opts)` signature:
    // supplies an empty raw-name table and logical-type registry so the many
    // custom-logical-agnostic tests below keep their original call shape.
    fn generate(schemas: &[Schema], opts: CodegenOptions) -> Result<String, GeneratorError> {
        let raw = RawNameTable::default();
        let registry = LogicalTypeRegistry::default();
        super::generate(schemas, &raw, &registry, opts)
    }

    // Test shim mirroring the pre-§15 `schema::parse` return of `Vec<Schema>`.
    fn parse_schemas(
        schema_jsons: &[String],
        idl_path: Option<&std::path::Path>,
    ) -> Result<Vec<Schema>, GeneratorError> {
        crate::schema::parse(schema_jsons, idl_path).map(|r| r.schemas)
    }

    /// Helper: parse JSON inputs through schema::parse, then codegen, and
    /// return the formatted source.
    fn codegen_from(jsons: &[&str]) -> String {
        let owned: Vec<String> = jsons.iter().map(|s| s.to_string()).collect();
        let resolved = crate::schema::parse(&owned, None).expect("parse");
        let registry = LogicalTypeRegistry::default();
        super::generate(&resolved.schemas, &resolved.raw_names, &registry, opts()).expect("codegen")
    }

    /// Like [`codegen_from`] but with the `bon::Builder` opt-in enabled.
    fn codegen_with_builder(jsons: &[&str]) -> String {
        let owned: Vec<String> = jsons.iter().map(|s| s.to_string()).collect();
        let resolved = crate::schema::parse(&owned, None).expect("parse");
        let registry = LogicalTypeRegistry::default();
        super::generate(
            &resolved.schemas,
            &resolved.raw_names,
            &registry,
            CodegenOptions {
                non_exhaustive: true,
                builder: true,
                collapse_single_union: true,
                allow_unregistered_logical_types: false,
            },
        )
        .expect("codegen")
    }

    // Like [`codegen_from`] but with the SPEC §5.6 single-union-record collapse
    // disabled (CLI `--no-collapse-single-union`).
    fn codegen_no_collapse(jsons: &[&str]) -> String {
        let owned: Vec<String> = jsons.iter().map(|s| s.to_string()).collect();
        let resolved = crate::schema::parse(&owned, None).expect("parse");
        let registry = LogicalTypeRegistry::default();
        super::generate(
            &resolved.schemas,
            &resolved.raw_names,
            &registry,
            CodegenOptions {
                non_exhaustive: true,
                builder: false,
                collapse_single_union: false,
                allow_unregistered_logical_types: false,
            },
        )
        .expect("codegen")
    }

    // Like [`codegen_from`] but with a configured custom-logical-type registry
    // (SPEC §15).
    fn codegen_with_logical(jsons: &[&str], registry: &LogicalTypeRegistry) -> String {
        let owned: Vec<String> = jsons.iter().map(|s| s.to_string()).collect();
        let resolved = crate::schema::parse(&owned, None).expect("parse");
        super::generate(&resolved.schemas, &resolved.raw_names, registry, opts()).expect("codegen")
    }

    /// Whitespace-insensitive substring check (prettyplease may wrap tokens).
    fn squashed_contains(src: &str, needle: &str) -> bool {
        let s: String = src.chars().filter(|c| !c.is_whitespace()).collect();
        let n: String = needle.chars().filter(|c| !c.is_whitespace()).collect();
        s.contains(&n)
    }

    fn must_parse_as_file(src: &str) {
        syn::parse_str::<syn::File>(src).unwrap_or_else(|e| {
            panic!("generated source does not parse as syn::File: {e}\n---\n{src}\n---");
        });
    }

    #[test]
    fn builder_ident_is_injective_across_ambiguous_fqns() {
        // These two distinct FQNs would collide under a naive `.`->`_` mangle
        // (`x_.y` vs `x._y`); the escaping scheme must keep them distinct.
        let a = Name::new("x_.y").expect("name a");
        let b = Name::new("x._y").expect("name b");
        assert_ne!(
            builder_ident(&a).expect("ident a").to_string(),
            builder_ident(&b).expect("ident b").to_string(),
        );
        // Stable, expected shape for the common case.
        let node = Name::new("s.Node").expect("name node");
        assert_eq!(
            builder_ident(&node).expect("ident node").to_string(),
            "build_s_1Node"
        );
    }

    // ---- primitives + structural shapes ---------------------------------------

    #[test]
    fn emits_record_with_primitive_fields() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Basic",
            "namespace": "p",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"}
            ]
        }"#]);
        assert!(src.contains("pub struct Basic"), "{src}");
        assert!(src.contains("pub id: i64"), "{src}");
        assert!(src.contains("pub name: String"), "{src}");
        assert!(src.contains("Serialize"), "{src}");
        assert!(src.contains("Deserialize"), "{src}");
        must_parse_as_file(&src);
    }

    /// An inline (anonymous) record defined in a field position must be
    /// promoted to its own emitted struct so the referencing field's type
    /// resolves. Regression: previously the field referenced `Inner` but the
    /// struct was never emitted, yielding a dangling reference.
    #[test]
    fn inline_record_field_is_promoted_to_struct() {
        let src = codegen_from(&[r#"{
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
        }"#]);
        // Inline `Inner` inherits the enclosing namespace `anon`, so it lands
        // alongside `Outer` in the same module and is referenced by short path.
        assert!(src.contains("pub struct Outer"), "{src}");
        assert!(src.contains("pub struct Inner"), "{src}");
        assert!(src.contains("pub child: Inner"), "{src}");
        assert!(src.contains("pub v: i32"), "{src}");
        must_parse_as_file(&src);
    }

    /// An inline record with its *own* namespace must be emitted into the
    /// corresponding module, and the referencing field uses the relative
    /// `super::…` path.
    #[test]
    fn inline_record_with_own_namespace_lands_in_module() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Outer",
            "namespace": "anon",
            "fields": [{
                "name": "child",
                "type": {
                    "type": "record",
                    "name": "Inner",
                    "namespace": "other",
                    "fields": [{"name": "w", "type": "int"}]
                }
            }]
        }"#]);
        assert!(squashed_contains(&src, "pub mod other"), "{src}");
        assert!(squashed_contains(&src, "pub struct Inner"), "{src}");
        assert!(squashed_contains(&src, "pub w: i32"), "{src}");
        assert!(
            squashed_contains(&src, "pub child: super::other::Inner"),
            "relative path: {src}"
        );
        must_parse_as_file(&src);
    }

    /// Inline records nested inside another inline record (and inside arrays)
    /// must all be promoted transitively.
    #[test]
    fn nested_inline_records_are_promoted_transitively() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Outer",
            "namespace": "anon",
            "fields": [{
                "name": "mid",
                "type": {
                    "type": "record",
                    "name": "Mid",
                    "fields": [
                        {"name": "deep", "type": {
                            "type": "record", "name": "Deep",
                            "fields": [{"name": "x", "type": "int"}]
                        }},
                        {"name": "list", "type": {"type": "array", "items": {
                            "type": "record", "name": "Elem",
                            "fields": [{"name": "y", "type": "long"}]
                        }}}
                    ]
                }
            }]
        }"#]);
        for ty in ["Outer", "Mid", "Deep", "Elem"] {
            assert!(
                src.contains(&format!("pub struct {ty}")),
                "missing {ty}\n{src}"
            );
        }
        must_parse_as_file(&src);
    }

    #[test]
    fn emits_optional_field_as_option() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Opt",
            "namespace": "p",
            "fields": [{"name": "name", "type": ["null", "string"]}]
        }"#]);
        assert!(src.contains("Option<String>"), "{src}");
        must_parse_as_file(&src);
    }

    #[test]
    fn emits_bytes_field_with_serde_helper() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Blob",
            "namespace": "p",
            "fields": [{"name": "payload", "type": "bytes"}]
        }"#]);
        assert!(src.contains("serde::bytes"), "bytes helper expected: {src}");
        must_parse_as_file(&src);
    }

    // A field whose type is `["null", "bytes"]` lowers to `Option<Vec<u8>>`
    // and must carry the `_opt` flavour of the serde helper (SPEC §3.4).
    #[test]
    fn emits_optional_bytes_field_with_serde_helper_opt() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "MaybeBlob",
            "namespace": "p",
            "fields": [{"name": "payload", "type": ["null", "bytes"]}]
        }"#]);
        assert!(
            src.contains("serde::bytes_opt"),
            "bytes_opt helper expected: {src}"
        );
        assert!(src.contains("Option<Vec<u8>>"), "{src}");
        must_parse_as_file(&src);
    }

    /// A field referencing a top-level `Fixed` (via `Schema::Ref` resolved
    /// through the registry) must carry the `serde::fixed` helper.
    #[test]
    fn emits_fixed_referenced_field_with_serde_helper() {
        let src = codegen_from(&[
            r#"{"type":"fixed","name":"Md5","namespace":"f","size":16}"#,
            r#"{
                "type": "record",
                "name": "Stamped",
                "namespace": "f",
                "fields": [{"name": "hash", "type": "f.Md5"}]
            }"#,
        ]);
        assert!(
            src.contains("serde::fixed\"") && !src.contains("serde::fixed_opt"),
            "fixed (not _opt) helper expected: {src}"
        );
        must_parse_as_file(&src);
    }

    /// Same, but for `["null", FixedRef]` → `Option<Md5>` + `_opt` helper.
    #[test]
    fn emits_optional_fixed_referenced_field_with_serde_helper_opt() {
        let src = codegen_from(&[
            r#"{"type":"fixed","name":"Md5","namespace":"f","size":16}"#,
            r#"{
                "type": "record",
                "name": "Stamped",
                "namespace": "f",
                "fields": [{"name": "hash", "type": ["null", "f.Md5"]}]
            }"#,
        ]);
        assert!(
            src.contains("serde::fixed_opt"),
            "fixed_opt helper expected: {src}"
        );
        must_parse_as_file(&src);
    }

    #[test]
    fn emits_array_field_as_vec() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Bag",
            "namespace": "p",
            "fields": [{"name": "tags", "type": {"type": "array", "items": "int"}}]
        }"#]);
        assert!(src.contains("Vec<i32>"), "{src}");
        must_parse_as_file(&src);
    }

    #[test]
    fn emits_map_field_as_hashmap() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Dict",
            "namespace": "p",
            "fields": [{"name": "by_key", "type": {"type": "map", "values": "string"}}]
        }"#]);
        assert!(src.contains("HashMap<String, String>"), "{src}");
        must_parse_as_file(&src);
    }

    // ---- enum + non_exhaustive ------------------------------------------------

    #[test]
    fn emits_enum_with_unit_variants_and_non_exhaustive() {
        let src = codegen_from(&[r#"{
            "type": "enum",
            "name": "Suit",
            "namespace": "cards",
            "symbols": ["HEARTS", "DIAMONDS"]
        }"#]);
        assert!(src.contains("pub enum Suit"), "{src}");
        assert!(src.contains("#[non_exhaustive]"), "{src}");
        assert!(src.contains("Hearts"), "PascalCase variant: {src}");
        assert!(
            src.contains("rename = \"HEARTS\""),
            "rename preserves original: {src}"
        );
        must_parse_as_file(&src);
    }

    #[test]
    fn enum_omits_non_exhaustive_when_disabled() {
        let owned = vec![
            r#"{
            "type": "enum",
            "name": "Suit",
            "namespace": "cards",
            "symbols": ["HEARTS"]
        }"#
            .to_string(),
        ];
        let schemas = parse_schemas(&owned, None).expect("parse");
        let src = generate(
            &schemas,
            CodegenOptions {
                non_exhaustive: false,
                builder: false,
                collapse_single_union: true,
                allow_unregistered_logical_types: false,
            },
        )
        .expect("codegen");
        assert!(!src.contains("#[non_exhaustive]"), "{src}");
        must_parse_as_file(&src);
    }

    // ---- fixed ----------------------------------------------------------------

    #[test]
    fn emits_fixed_as_type_alias() {
        let src = codegen_from(&[r#"{
            "type": "fixed",
            "name": "Md5",
            "namespace": "crypto",
            "size": 16
        }"#]);
        assert!(src.contains("pub type Md5 = [u8; 16]"), "{src}");
        must_parse_as_file(&src);
    }

    // ---- namespaces -----------------------------------------------------------

    #[test]
    fn emits_namespaces_as_nested_modules() {
        let src = codegen_from(&[
            r#"{"type":"record","name":"A","namespace":"a.b","fields":[]}"#,
            r#"{"type":"record","name":"B","namespace":"a.c","fields":[]}"#,
        ]);
        assert!(src.contains("pub mod a"), "{src}");
        assert!(src.contains("pub mod b"), "{src}");
        assert!(src.contains("pub mod c"), "{src}");
        must_parse_as_file(&src);
    }

    #[test]
    fn cross_namespace_reference_uses_relative_super_path() {
        // X in a.b references Y in a.c — should emit super::c::Y.
        let src = codegen_from(&[
            r#"{"type":"record","name":"Y","namespace":"a.c","fields":[{"name":"v","type":"int"}]}"#,
            r#"{"type":"record","name":"X","namespace":"a.b","fields":[{"name":"y","type":"a.c.Y"}]}"#,
        ]);
        assert!(
            src.contains("super::c::Y"),
            "expected super::c::Y, got: {src}"
        );
        must_parse_as_file(&src);
    }

    // ---- camelCase rename -----------------------------------------------------

    #[test]
    fn field_with_avro_camel_case_gets_serde_rename() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "User",
            "namespace": "p",
            "fields": [{"name": "firstName", "type": "string"}]
        }"#]);
        assert!(src.contains("pub first_name: String"), "{src}");
        assert!(
            src.contains("rename = \"firstName\""),
            "expected rename: {src}"
        );
        must_parse_as_file(&src);
    }

    // ---- multi-variant unions (SPEC §5.3) ------------------------------------

    /// 3-arm primitive union → sibling `{Record}Union` enum with one
    /// variant per arm, each carrying its Avro JSON tag via serde rename.
    #[test]
    fn three_arm_primitive_union_lowers_to_sibling_enum() {
        // A second (non-union) field keeps the record off the single-union
        // collapse path (SPEC §5.6), so the sibling `{Record}Union` enum is the
        // thing under test here.
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Mixed",
            "namespace": "u",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "value", "type": ["int", "string", "long"]}
            ]
        }"#]);
        assert!(src.contains("pub enum MixedUnion"), "{src}");
        assert!(src.contains("Int(i32)"), "{src}");
        assert!(src.contains("String(String)"), "{src}");
        assert!(src.contains("Long(i64)"), "{src}");
        assert!(src.contains("rename = \"int\""), "{src}");
        assert!(src.contains("rename = \"long\""), "{src}");
        assert!(src.contains("rename = \"string\""), "{src}");
        // Parent record's field references the generated enum, not Option<T>.
        assert!(src.contains("pub value: MixedUnion"), "{src}");
        must_parse_as_file(&src);
    }

    /// Non-nullable 2-arm union is also a multi-variant case (it's not the
    /// `[null, T]` shape), so it generates an enum rather than an `Option`.
    #[test]
    fn non_nullable_two_arm_union_lowers_to_sibling_enum() {
        // Second field keeps this off the single-union collapse path (§5.6).
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Either",
            "namespace": "u",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "v", "type": ["int", "string"]}
            ]
        }"#]);
        assert!(src.contains("pub enum EitherUnion"), "{src}");
        assert!(src.contains("Int(i32)"), "{src}");
        assert!(src.contains("String(String)"), "{src}");
        assert!(!src.contains("Option<"), "must not be Option: {src}");
        must_parse_as_file(&src);
    }

    // ---- single-union-record collapse (vanavro extension, SPEC §5.6) -----------

    /// A single-field record wrapping a multi-variant union (incl. `null`)
    /// collapses to a single public enum that takes the record's own name
    /// `{Record}`; there is no wrapping struct and no `{Record}Union` sibling.
    /// A private carrier (`{Record}Repr` + `{Record}ReprUnion`) plus a
    /// `Serialize` impl (with an inline `Arm<'a>` adapter) and a
    /// `From` carry the unchanged wire shape.
    #[test]
    fn single_union_field_record_collapses_to_enum() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Scalar",
            "namespace": "u",
            "fields": [{"name": "choice", "type": ["int", "string", "null"]}]
        }"#]);
        // Public type is the `{Record}` enum; there is NO wrapping struct and
        // no `{Record}Union` sibling — the record name maps to the union enum.
        assert!(
            squashed_contains(&src, "pub enum Scalar {"),
            "public enum: {src}"
        );
        assert!(
            !src.contains("pub struct Scalar"),
            "no wrapping struct: {src}"
        );
        assert!(
            !squashed_contains(&src, "pub enum ScalarUnion {"),
            "no `ScalarUnion` enum — the record name maps to `Scalar`: {src}"
        );
        // Variants carry the union payloads directly.
        assert!(squashed_contains(&src, "Int(i32)"), "int variant: {src}");
        assert!(
            squashed_contains(&src, "String(String)"),
            "string variant: {src}"
        );
        assert!(squashed_contains(&src, "Null(())"), "null variant: {src}");
        // Deserialize routes through the private carrier; serialize is manual.
        assert!(
            squashed_contains(&src, r#"#[serde(try_from = "ScalarRepr")]"#),
            "try_from attr: {src}"
        );
        assert!(
            squashed_contains(&src, "impl ::serde::Serialize for Scalar"),
            "manual Serialize: {src}"
        );
        assert!(
            squashed_contains(&src, "struct ScalarRepr"),
            "carrier struct: {src}"
        );
        assert!(
            squashed_contains(&src, "impl ::std::convert::From<ScalarRepr> for Scalar"),
            "From impl: {src}"
        );
        // Serialize inlines a borrowing `Arm<'a>` adapter that drives serde's
        // externally-tagged `serialize_newtype_variant` per arm — no separate
        // `ReprUnionRef` view enum is emitted.
        assert!(
            !src.contains("ScalarReprUnionRef"),
            "no borrowing view enum: {src}"
        );
        assert!(
            squashed_contains(&src, "struct Arm<'a>(&'a Scalar)"),
            "inline borrowing Arm adapter: {src}"
        );
        assert!(
            squashed_contains(&src, "serializer.serialize_newtype_variant("),
            "externally-tagged newtype-variant encoding: {src}"
        );
        // Wire encoding unchanged: field key preserved.
        assert!(
            squashed_contains(&src, r#"st.serialize_field("choice", &Arm(self))"#),
            "field key preserved: {src}"
        );
        // The old fluent constructors are gone — callers use enum variants.
        assert!(!src.contains("pub fn int(val"), "no constructor fn: {src}");
        must_parse_as_file(&src);
    }

    /// A single-arm union (e.g. `["int"]`) collapses to a 1-variant enum.
    #[test]
    fn single_arm_union_field_record_collapses_to_enum() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "SingleArm",
            "namespace": "u",
            "fields": [{"name": "choice", "type": ["int"]}]
        }"#]);
        assert!(
            squashed_contains(&src, "pub enum SingleArm {"),
            "public enum: {src}"
        );
        assert!(squashed_contains(&src, "Int(i32)"), "int variant: {src}");
        assert!(
            squashed_contains(&src, r#"#[serde(try_from = "SingleArmRepr")]"#),
            "try_from attr: {src}"
        );
        must_parse_as_file(&src);
    }

    /// A user-defined type named `ScalarRepr` must collide with the synthetic carrier `ScalarRepr`
    /// for collapsing record `Scalar`.
    #[test]
    fn collapsed_union_carrier_collision_is_reported() {
        let err = codegen_result(&[
            r#"{"type":"record","name":"Scalar","namespace":"u","fields":[{"name":"choice","type":["int","string","null"]}]}"#,
            r#"{"type":"record","name":"ScalarRepr","namespace":"u","fields":[{"name":"x","type":"int"}]}"#,
        ])
        .expect_err("must collide");
        match err {
            GeneratorError::NameCollision {
                kind,
                scope,
                rust_ident,
                first,
                second,
            } => {
                assert_eq!(kind, "types in namespace");
                assert_eq!(scope, "u");
                assert_eq!(rust_ident, "ScalarRepr");
                let mut names = [first, second];
                names.sort();
                assert_eq!(names[0], "ScalarRepr");
                assert_eq!(
                    names[1],
                    "ScalarRepr (auto-generated carrier for record `Scalar`)"
                );
            }
            other => panic!("expected NameCollision, got {other:?}"),
        }
    }

    /// A record referencing a collapsed single-union record resolves the field
    /// to the collapsed enum's name `{Record}` (not a now-nonexistent struct).
    #[test]
    fn record_referencing_collapsed_union_resolves_to_enum_name() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Outer",
            "namespace": "u",
            "fields": [
                {"name": "inner", "type": {
                    "type": "record",
                    "name": "Scalar",
                    "fields": [{"name": "choice", "type": ["int", "string", "null"]}]
                }}
            ]
        }"#]);
        assert!(
            squashed_contains(&src, "pub inner: Scalar,"),
            "field references the collapsed enum by record name: {src}"
        );
        assert!(
            squashed_contains(&src, "pub enum Scalar {"),
            "collapsed enum is emitted: {src}"
        );
        must_parse_as_file(&src);
    }

    /// A recursive collapsed single-union record references itself by its own
    /// (collapsed) name in the boxed recursive arm — `Box<{Record}>`.
    #[test]
    fn recursive_collapsed_union_boxes_record_name() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Tree",
            "namespace": "u",
            "fields": [
                {"name": "node", "type": ["null", "int", "u.Tree"]}
            ]
        }"#]);
        assert!(
            squashed_contains(&src, "pub enum Tree {"),
            "collapsed enum is emitted: {src}"
        );
        assert!(
            squashed_contains(&src, "Tree(Box<Tree>)"),
            "recursive arm boxes the collapsed enum by record name: {src}"
        );
        must_parse_as_file(&src);
    }

    // SPEC §5.6 — with the collapse disabled (`collapse_single_union: false`),
    // a single-field enum-generating-union record keeps its ordinary struct
    // shape with the sibling `{Record}Union` enum (§5.3); the collapsed enum
    // and its synthetic carriers are NOT emitted.
    #[test]
    fn single_union_field_record_does_not_collapse_when_disabled() {
        let src = codegen_no_collapse(&[r#"{
            "type": "record",
            "name": "Scalar",
            "namespace": "u",
            "fields": [{"name": "choice", "type": ["int", "string", "null"]}]
        }"#]);
        // Ordinary struct over the per-record union enum (SPEC §5.3).
        assert!(
            squashed_contains(&src, "pub struct Scalar {"),
            "struct kept: {src}"
        );
        assert!(
            squashed_contains(&src, "pub enum ScalarUnion {"),
            "sibling union enum: {src}"
        );
        assert!(
            squashed_contains(&src, "pub choice: ScalarUnion,"),
            "field typed by the union enum: {src}"
        );
        // None of the collapse machinery is emitted.
        assert!(
            !squashed_contains(&src, "impl ::serde::Serialize for Scalar"),
            "no Serialize impl: {src}"
        );
        assert!(!src.contains("ScalarRepr"), "no synthetic carrier: {src}");
        assert!(
            !src.contains(r#"try_from = "ScalarRepr""#),
            "no try_from carrier attr: {src}"
        );
        must_parse_as_file(&src);
    }

    // SPEC §5.6 — with the collapse disabled, a record referencing a
    // single-union record still resolves the field to `{Record}` (now a
    // struct rather than the collapsed enum), so callers are unaffected.
    #[test]
    fn record_referencing_uncollapsed_union_resolves_to_struct_name() {
        let src = codegen_no_collapse(&[r#"{
            "type": "record",
            "name": "Outer",
            "namespace": "u",
            "fields": [
                {"name": "inner", "type": {
                    "type": "record",
                    "name": "Scalar",
                    "fields": [{"name": "choice", "type": ["int", "string", "null"]}]
                }}
            ]
        }"#]);
        assert!(
            squashed_contains(&src, "pub inner: Scalar,"),
            "field references the record by name: {src}"
        );
        assert!(
            squashed_contains(&src, "pub struct Scalar {"),
            "referenced record is a struct: {src}"
        );
        must_parse_as_file(&src);
    }

    // SPEC §5.6 — with the collapse disabled, a recursive single-union record
    // becomes an ordinary struct whose recursive union arm boxes the struct
    // (`Box<Tree>`), the standard §5.4 recursive shape.
    #[test]
    fn recursive_uncollapsed_union_boxes_struct_name() {
        let src = codegen_no_collapse(&[r#"{
            "type": "record",
            "name": "Tree",
            "namespace": "u",
            "fields": [
                {"name": "node", "type": ["null", "int", "u.Tree"]}
            ]
        }"#]);
        assert!(
            squashed_contains(&src, "pub struct Tree {"),
            "struct kept: {src}"
        );
        assert!(
            squashed_contains(&src, "pub enum TreeUnion {"),
            "sibling union enum: {src}"
        );
        assert!(
            squashed_contains(&src, "Tree(Box<Tree>)"),
            "recursive arm boxes the struct by name: {src}"
        );
        must_parse_as_file(&src);
    }

    /// A single-field record whose field is an `[null, T]` optional gains a
    /// non-null `Some`-wrapping constructor plus a nullary `null()`.
    #[test]
    fn single_optional_field_record_emits_some_and_null_constructors() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "MaybeName",
            "namespace": "u",
            "fields": [{"name": "name", "type": ["null", "string"]}]
        }"#]);
        // Field is Option, not a synthetic enum.
        assert!(src.contains("pub name: Option<String>"), "{src}");
        assert!(
            src.contains("pub fn string(val: String) -> Self"),
            "some ctor: {src}"
        );
        assert!(src.contains("Some(val)"), "wraps Some: {src}");
        assert!(src.contains("pub fn null() -> Self"), "null ctor: {src}");
        assert!(src.contains("None"), "null is None: {src}");
        must_parse_as_file(&src);
    }

    /// Multi-field records get no named constructors (no sensible single-arm
    /// shape) even if one field is a union.
    #[test]
    fn multi_field_record_gets_no_named_constructors() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Pair",
            "namespace": "u",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "choice", "type": ["int", "string"]}
            ]
        }"#]);
        assert!(!src.contains("pub fn int("), "no int ctor: {src}");
        assert!(!src.contains("pub fn string("), "no string ctor: {src}");
        must_parse_as_file(&src);
    }

    /// A single non-union field gets no named constructors (the union
    /// detection must not fire on a plain scalar).
    #[test]
    fn single_scalar_field_record_gets_no_named_constructors() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "JustLong",
            "namespace": "u",
            "fields": [{"name": "n", "type": "long"}]
        }"#]);
        assert!(!src.contains("pub fn long("), "no long ctor: {src}");
        must_parse_as_file(&src);
    }

    /// Union containing a named record produces a variant whose payload is
    /// the record's relative path, and whose Avro tag is its FQN.
    #[test]
    fn union_containing_named_record_emits_record_variant() {
        let src = codegen_from(&[
            r#"{"type":"record","name":"Payload","namespace":"u","fields":[{"name":"b","type":"bytes"}]}"#,
            r#"{
                "type": "record",
                "name": "Wrap",
                "namespace": "u",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "body", "type": ["null", "u.Payload", "string"]}
                ]
            }"#,
        ]);
        // Three-arm union, so an enum (not Option). Second field keeps the
        // record off the single-union collapse path (§5.6).
        assert!(src.contains("pub enum WrapUnion"), "{src}");
        assert!(src.contains("Null(())"), "null variant: {src}");
        assert!(src.contains("Payload(Payload)"), "payload variant: {src}");
        assert!(src.contains("String(String)"), "string variant: {src}");
        assert!(src.contains("rename = \"u.Payload\""), "FQN as tag: {src}");
        must_parse_as_file(&src);
    }

    // Two named records from different namespaces with the same local name
    // collide on the PascalCase candidate; SPEC §5.3 says fall back to
    // `Variant{N}` for every variant, mirroring SPEC §5.3's own cross-namespace
    // `Foo` example.
    #[test]
    fn union_with_name_collision_falls_back_to_variant_n() {
        let src = codegen_from(&[
            r#"{"type":"record","name":"Foo","namespace":"col.a","fields":[{"name":"x","type":"int"}]}"#,
            r#"{"type":"record","name":"Foo","namespace":"col.b","fields":[{"name":"y","type":"string"}]}"#,
            r#"{
                "type": "record",
                "name": "Holder",
                "namespace": "col.h",
                "fields": [{"name": "v", "type": ["col.a.Foo", "col.b.Foo"]}]
            }"#,
        ]);
        assert!(src.contains("Variant0("), "{src}");
        assert!(src.contains("Variant1("), "{src}");
        // The bare `Foo(` candidate must NOT appear since the fallback
        // applies to every variant of the union.
        assert!(
            !src.contains("Foo(super::a::Foo)"),
            "fallback should replace ALL candidates: {src}"
        );
        must_parse_as_file(&src);
    }

    /// A record whose only back-edge to itself goes through a multi-variant
    /// union (e.g. `union { int, Self }`) must still get its recursive
    /// payload boxed — cycle detection must walk every union variant, not
    /// just the optional `[null, T]` shape.
    #[test]
    fn recursive_ref_through_multi_variant_union_is_boxed() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Node",
            "namespace": "r",
            "fields": [
                {"name": "value", "type": "int"},
                {"name": "next",  "type": ["int", "r.Node"]}
            ]
        }"#]);
        // Inside the sibling enum, the Node arm must box itself.
        assert!(
            src.contains("Node(Box<Node>)"),
            "recursive variant must box: {src}"
        );
        must_parse_as_file(&src);
    }

    /// Logical types riding their underlying primitive (e.g. `uuid` →
    /// `string`, `date` → `int`) reuse the primitive's candidate name in a
    /// union — `["int", "uuid"]` collides on `Int`/`String` candidates only
    /// if the underlying primitives clash, otherwise stays unique.
    #[test]
    fn logical_types_in_union_use_underlying_primitive_candidate_names() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Tagged",
            "namespace": "u",
            "fields": [{
                "name": "v",
                "type": [
                    "int",
                    {"type": "string", "logicalType": "uuid"}
                ]
            }]
        }"#]);
        // Int payload is i32; Uuid payload is ::uuid::Uuid; candidates Int +
        // String don't collide so no Variant{N} fallback.
        assert!(src.contains("Int(i32)"), "{src}");
        assert!(src.contains("String(::uuid::Uuid)"), "{src}");
        assert!(!src.contains("Variant0"), "no fallback expected: {src}");
        must_parse_as_file(&src);
    }

    /// `Schema::Duration` discards the backing fixed's name in apache_avro,
    /// so we can't honestly reconstruct an Avro union JSON tag for it.
    /// v1 refuses cleanly rather than emit a wrong rename.
    #[test]
    fn duration_in_union_is_unsupported() {
        let owned = vec![
            r#"{
                "type": "record",
                "name": "R",
                "namespace": "u",
                "fields": [{
                    "name": "v",
                    "type": [
                        "int",
                        {
                            "type": "fixed",
                            "name": "Span",
                            "size": 12,
                            "logicalType": "duration"
                        }
                    ]
                }]
            }"#
            .to_string(),
        ];
        let schemas = parse_schemas(&owned, None).expect("parse");
        let err = generate(&schemas, opts()).expect_err("duration-in-union must error");
        match err {
            GeneratorError::UnsupportedSchema { detail } => {
                assert!(
                    detail.contains("duration"),
                    "error must point at duration: {detail}"
                );
            }
            other => panic!("expected UnsupportedSchema, got {other:?}"),
        }
    }

    // SPEC §5.3: two *unnamed* enum-generating union fields in one record both
    // fold onto the default `{Record}Union`, so the namespace collision pass
    // rejects the second as a `NameCollision`. The remedy is an `"enum"`
    // attribute on the record field.
    #[test]
    fn multiple_unnamed_union_fields_in_record_collide() {
        let owned = [r#"{
            "type": "record",
            "name": "TwoUnions",
            "namespace": "u",
            "fields": [
                {"name": "a", "type": ["int", "string"]},
                {"name": "b", "type": ["long", "bytes"]}
            ]
        }"#
        .to_string()];
        let schemas = parse_schemas(&owned, None).expect("parse");
        let err = generate(&schemas, opts()).expect_err("two unnamed union fields must collide");
        match err {
            GeneratorError::NameCollision {
                kind,
                scope,
                rust_ident,
                ..
            } => {
                assert_eq!(kind, "types in namespace");
                assert_eq!(scope, "u");
                assert_eq!(rust_ident, "TwoUnionsUnion");
            }
            other => panic!("expected NameCollision, got {other:?}"),
        }
    }

    // SPEC §5.3: naming one of two coexisting unions via an `"enum"` attribute
    // on its record field lets both live in the same record.
    #[test]
    fn two_union_fields_coexist_when_one_is_named() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "TwoUnions",
            "namespace": "u",
            "fields": [
                {"name": "a", "type": ["int", "string"]},
                {"name": "b", "enum": "BUnion", "type": {"type": "array", "items": ["long", "bytes"]}}
            ]
        }"#]);
        assert!(src.contains("enum TwoUnionsUnion"), "default name: {src}");
        assert!(src.contains("enum BUnion"), "custom name: {src}");
        must_parse_as_file(&src);
    }

    // SPEC §5.3 / §5.6: a *collapsed* single-union record emits its enum under
    // the record's own name `{Record}` — never `{Record}Union` — so a sibling
    // user type literally named `{Record}Union` must not phantom-collide with
    // the union collision registration.
    #[test]
    fn collapsed_union_record_does_not_phantom_collide_with_record_union_name() {
        let src = codegen_from(&[
            r#"{
                "type": "record",
                "name": "Foo",
                "namespace": "c",
                "fields": [{"name": "choice", "type": ["int", "string"]}]
            }"#,
            r#"{
                "type": "record",
                "name": "FooUnion",
                "namespace": "c",
                "fields": [{"name": "x", "type": "int"}]
            }"#,
        ]);
        // `Foo` collapses to a bare enum named `Foo`; `FooUnion` is an ordinary
        // struct. Neither collides.
        assert!(src.contains("enum Foo"), "collapsed enum: {src}");
        assert!(src.contains("struct FooUnion"), "sibling struct: {src}");
        must_parse_as_file(&src);
    }

    // SPEC §5.3: a custom-named union enum that clashes with a record in the
    // same namespace is reported as a `NameCollision`.
    #[test]
    fn custom_named_union_colliding_with_record_is_reported() {
        let owned = [
            r#"{
                "type": "record",
                "name": "Holder",
                "namespace": "c",
                "fields": [{
                    "name": "tags",
                    "enum": "Clash",
                    "type": {"type": "array", "items": ["int", "string"]}
                }]
            }"#
            .to_string(),
            r#"{
                "type": "record",
                "name": "Clash",
                "namespace": "c",
                "fields": [{"name": "x", "type": "int"}]
            }"#
            .to_string(),
        ];
        let schemas = parse_schemas(&owned, None).expect("parse");
        let err = generate(&schemas, opts()).expect_err("union vs record must collide");
        match err {
            GeneratorError::NameCollision {
                scope, rust_ident, ..
            } => {
                assert_eq!(scope, "c");
                assert_eq!(rust_ident, "Clash");
            }
            other => panic!("expected NameCollision, got {other:?}"),
        }
    }

    // SPEC §5.3: two custom-named unions that resolve to the same ident in one
    // namespace collide, and each side of the report identifies the record
    // field it came from, including the collection it is nested in.
    #[test]
    fn two_custom_named_unions_same_name_collide() {
        let owned = [r#"{
            "type": "record",
            "name": "R",
            "namespace": "c",
            "fields": [
                {"name": "a", "enum": "Dup", "type": {"type": "array", "items": ["int", "string"]}},
                {"name": "b", "enum": "Dup", "type": {"type": "map", "values": ["long", "double"]}}
            ]
        }"#
        .to_string()];
        let schemas = parse_schemas(&owned, None).expect("parse");
        let err = generate(&schemas, opts()).expect_err("two unions named Dup must collide");
        match err {
            GeneratorError::NameCollision {
                rust_ident,
                first,
                second,
                ..
            } => {
                assert_eq!(rust_ident, "Dup");
                assert_eq!(
                    first,
                    "Dup (union enum for record `R` field `a`, nested in an `array`)"
                );
                assert_eq!(
                    second,
                    "Dup (union enum for record `R` field `b`, nested in a `map`)"
                );
            }
            other => panic!("expected NameCollision, got {other:?}"),
        }
    }

    // SPEC §5.3: two *unnamed* field-root unions in one record fold onto the
    // same `{Record}Union` default, so the report must distinguish them by
    // field.
    #[test]
    fn colliding_default_named_unions_are_distinguished_by_field() {
        let owned = [r#"{
            "type": "record",
            "name": "R",
            "namespace": "c",
            "fields": [
                {"name": "a", "type": ["int", "string"]},
                {"name": "b", "type": ["long", "double"]}
            ]
        }"#
        .to_string()];
        let schemas = parse_schemas(&owned, None).expect("parse");
        match generate(&schemas, opts()).expect_err("two default-named unions must collide") {
            GeneratorError::NameCollision { first, second, .. } => {
                assert_eq!(first, "RUnion (union enum for record `R` field `a`)");
                assert_eq!(second, "RUnion (union enum for record `R` field `b`)");
            }
            other => panic!("expected NameCollision, got {other:?}"),
        }
    }

    // SPEC §5.3: a union enum named verbatim via `"enum"` is used as-is (not
    // PascalCased) but keyword-escaped when needed.
    #[test]
    fn custom_union_name_used_verbatim_and_keyword_escaped() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "R",
            "namespace": "c",
            "fields": [
                {"name": "a", "enum": "lower_snake", "type": {"type": "array", "items": ["int", "string"]}},
                {"name": "b", "enum": "match", "type": {"type": "map", "values": ["long", "double"]}}
            ]
        }"#]);
        // Verbatim (not PascalCased): `lower_snake` stays `lower_snake`.
        assert!(src.contains("enum lower_snake"), "verbatim name: {src}");
        // Keyword `match` gets the raw-identifier escape.
        assert!(src.contains("enum r#match"), "keyword escaped: {src}");
        // Both idents warn under `non_camel_case_types` in the consuming crate;
        // the name is the schema author's choice, so no allow is emitted.
        assert!(
            !src.contains("non_camel_case_types"),
            "no allow is emitted for a non-UpperCamelCase verbatim name: {src}"
        );
        must_parse_as_file(&src);
    }

    // SPEC §5.2: a name that shadows a prelude type is accepted — the check is
    // grammar-only. The result is a module that parses but does not compile
    // (here `String(String)` is `E0072`, and `tags` retargets at the enum).
    // Naming a union `String` is a user error; this pins that vanavro does not
    // try to catch it.
    #[test]
    fn verbatim_name_shadowing_prelude_is_accepted() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "R",
            "namespace": "c",
            "fields": [
                {"name": "a", "enum": "String", "type": {"type": "array", "items": ["string", "int"]}},
                {"name": "tags", "type": {"type": "array", "items": "string"}}
            ]
        }"#]);
        assert!(src.contains("enum String"), "shadowing enum emitted: {src}");
        assert!(src.contains("String(String)"), "self-recursive arm: {src}");
        assert!(
            src.contains("pub tags: Vec<String>"),
            "sibling retargeted at the enum: {src}"
        );
        // Parses fine; only type-checking fails.
        must_parse_as_file(&src);
    }

    // SPEC §5.3: a multi-variant union nested in an array or a map with no
    // `"enum"` attribute has no derivable enum name and is rejected. The error
    // must name the record, the field and the offending collection.
    #[test]
    fn nested_unnamed_union_in_a_collection_is_unsupported() {
        let cases = [
            (
                r#"{"type": "array", "items": ["int", "string"]}"#,
                "an `array`",
            ),
            (r#"{"type": "map", "values": ["int", "string"]}"#, "a `map`"),
            // Two collections deep: the innermost one is reported.
            (
                r#"{"type": "array", "items": {"type": "map", "values": ["int", "string"]}}"#,
                "a `map`",
            ),
            // Through a transparent `[null, T]` wrapper, which is not a descent
            // and so must not change the reported collection.
            (
                r#"["null", {"type": "array", "items": ["int", "string"]}]"#,
                "an `array`",
            ),
        ];
        for (ty, kind) in cases {
            let owned = [format!(
                r#"{{"type": "record", "name": "Nested", "namespace": "u",
                     "fields": [{{"name": "tags", "type": {ty}}}]}}"#
            )];
            let schemas = parse_schemas(&owned, None).expect("parse");
            match generate(&schemas, opts()) {
                Err(GeneratorError::UnsupportedSchema { detail }) => {
                    assert!(
                        detail.contains("record `Nested` field `tags`"),
                        "must locate the field for {ty}: {detail}"
                    );
                    assert!(
                        detail.contains(&format!("nested inside {kind}")),
                        "must name the collection for {ty}: {detail}"
                    );
                    assert!(
                        detail.contains("`\"enum\"` attribute"),
                        "must give the remedy for {ty}: {detail}"
                    );
                }
                other => panic!("expected UnsupportedSchema for {ty}, got {other:?}"),
            }
        }
    }

    // SPEC §5.3: a field-level `"enum"` attribute must ultimately reach an
    // enum-generating union. One that does not is rejected — an array of a
    // primitive, an array of an *optional* primitive (transparent, so still
    // nothing to name), or a plain scalar.
    #[test]
    fn misplaced_enum_not_enclosing_union_is_unsupported() {
        let cases = [
            r#"{"name": "f", "enum": "Foo", "type": {"type": "array", "items": "int"}}"#,
            r#"{"name": "f", "enum": "Foo", "type": {"type": "array", "items": ["null", "int"]}}"#,
            r#"{"name": "f", "enum": "Foo", "type": "int"}"#,
        ];
        for field in cases {
            let owned = [format!(
                r#"{{"type": "record", "name": "R", "namespace": "u", "fields": [{field}]}}"#
            )];
            let schemas = parse_schemas(&owned, None).expect("parse");
            match generate(&schemas, opts()) {
                Err(GeneratorError::UnsupportedSchema { detail }) => {
                    assert!(
                        detail.contains("does not ultimately enclose"),
                        "error message mismatch for {field}: {detail}"
                    );
                    // The attribute sits on the field, so the field — not the
                    // nested position the name died at — is what is named.
                    assert!(
                        detail.contains("on record `R` field `f`"),
                        "must locate the field for {field}: {detail}"
                    );
                }
                other => panic!("expected UnsupportedSchema for {field}, got {other:?}"),
            }
        }
    }

    // SPEC §5.3: an `"enum"` value is an arbitrary JSON string (Avro does not
    // validate custom-attribute contents). One that is not a legal Rust
    // identifier must produce a clean `UnsupportedSchema`, not a `proc-macro2`
    // `Ident::new` panic.
    #[test]
    fn illegal_enum_custom_name_is_unsupported_not_panic() {
        for bad in ["my-union!", "1Union", "has space", "unit.Name", ""] {
            // A second scalar field keeps the record from collapsing (§5.6)
            // before the name is validated.
            let owned = [format!(
                r#"{{"type": "record", "name": "R", "namespace": "u", "fields": [
                    {{"name": "f", "enum": {bad:?}, "type": ["int", "string"]}},
                    {{"name": "g", "type": "long"}}
                ]}}"#
            )];
            let schemas = parse_schemas(&owned, None).expect("parse");
            match generate(&schemas, opts()) {
                Err(GeneratorError::UnsupportedSchema { detail }) => assert!(
                    detail.contains("is not a valid Rust identifier"),
                    "error message mismatch for {bad:?}: {detail}"
                ),
                other => panic!("expected UnsupportedSchema for {bad:?}, got {other:?}"),
            }
        }
    }

    // SPEC §5.3: `_` matches Avro's name regex but is a Rust wildcard, not a
    // binding, so `pub enum _` cannot parse. It must be rejected here rather
    // than surfacing as an internal formatter error.
    #[test]
    fn underscore_enum_custom_name_is_unsupported() {
        let owned = [
            r#"{"type": "record", "name": "R", "namespace": "u", "fields": [
            {"name": "f", "enum": "_", "type": ["int", "string"]},
            {"name": "g", "type": "long"}
        ]}"#
            .to_string(),
        ];
        let schemas = parse_schemas(&owned, None).expect("parse");
        match generate(&schemas, opts()) {
            Err(GeneratorError::UnsupportedSchema { detail }) => {
                assert!(detail.contains("Rust wildcard"), "detail: {detail}");
                assert!(detail.contains("__"), "must suggest a remedy: {detail}");
            }
            other => panic!("expected UnsupportedSchema for `_`, got {other:?}"),
        }
    }

    // SPEC §5.3: only the bare `_` is degenerate — `__` is a valid binding and
    // stays accepted verbatim.
    #[test]
    fn double_underscore_enum_custom_name_is_accepted() {
        let src = codegen_from(&[
            r#"{"type": "record", "name": "R", "namespace": "u", "fields": [
            {"name": "f", "enum": "__", "type": ["int", "string"]},
            {"name": "g", "type": "long"}
        ]}"#,
        ]);
        assert!(squashed_contains(&src, "pub enum __ {"), "{src}");
        must_parse_as_file(&src);
    }

    // SPEC §5.3: a present-but-non-string `"enum"` union-name attribute is
    // rejected rather than silently ignored.
    #[test]
    fn non_string_enum_custom_name_is_unsupported() {
        for bad in ["5", "[\"A\"]", "true", "null", "{\"x\": 1}"] {
            let owned = [format!(
                r#"{{"type": "record", "name": "R", "namespace": "u", "fields": [
                    {{"name": "f", "enum": {bad}, "type": ["int", "string"]}},
                    {{"name": "g", "type": "long"}}
                ]}}"#
            )];
            let schemas = parse_schemas(&owned, None).expect("parse");
            match generate(&schemas, opts()) {
                Err(GeneratorError::UnsupportedSchema { detail }) => assert!(
                    detail.contains("must be a string"),
                    "error message mismatch for {bad}: {detail}"
                ),
                other => panic!("expected UnsupportedSchema for {bad}, got {other:?}"),
            }
        }
    }

    // SPEC §5.3 / §5.6: `"enum"` on a collapsing record's lone union field is
    // a conflict with the collapse name.
    #[test]
    fn field_level_enum_on_collapsing_record_errors() {
        let owned = [r#"{
            "type": "record",
            "name": "R",
            "namespace": "u",
            "fields": [
                {"name": "a", "enum": "Alpha", "type": ["int", "string"]}
            ]
        }"#
        .to_string()];
        let schemas = parse_schemas(&owned, None).expect("parse");
        match generate(&schemas, opts()) {
            Ok(src) => panic!("collapse + field enum must error, got: {src}"),
            Err(GeneratorError::UnsupportedSchema { detail }) => {
                assert!(detail.contains("collapses"), "detail: {detail}");
                // Both remedies must be offered: dropping the attribute, and
                // disabling the collapse to actually get the requested name.
                assert!(
                    detail.contains("--no-collapse-single-union"),
                    "must offer the collapse escape hatch: {detail}"
                );
                assert!(
                    detail.contains("pub enum Alpha"),
                    "must name what the escape hatch yields: {detail}"
                );
            }
            Err(other) => panic!("expected UnsupportedSchema, got {other:?}"),
        }

        // …and that escape hatch must genuinely work.
        let src = generate(
            &schemas,
            CodegenOptions {
                collapse_single_union: false,
                ..opts()
            },
        )
        .expect("the same schema must generate with the collapse disabled");
        assert!(squashed_contains(&src, "pub enum Alpha {"), "{src}");
        must_parse_as_file(&src);
    }

    // SPEC §5.3 / §5.6: an invalid `"enum"` value on a collapsing record's
    // lone union field is reported as the invalid name, not as a collapse
    // conflict whose escape hatch would promise an unemittable type.
    #[test]
    fn invalid_enum_name_on_collapsing_record_reports_the_name() {
        for bad in ["_", "my-union!", ""] {
            let owned = [format!(
                r#"{{"type": "record", "name": "R", "namespace": "u", "fields": [
                    {{"name": "a", "enum": "{bad}", "type": ["int", "string"]}}
                ]}}"#
            )];
            let schemas = parse_schemas(&owned, None).expect("parse");
            match generate(&schemas, opts()) {
                Err(GeneratorError::UnsupportedSchema { detail }) => {
                    assert!(
                        !detail.contains("--no-collapse-single-union"),
                        "must not offer an escape hatch to an unemittable name {bad:?}: {detail}"
                    );
                    // SPEC §5.3: the diagnostic names the owning record and field.
                    assert!(detail.contains("field `a`"), "{bad:?}: {detail}");
                    if bad == "_" {
                        assert!(detail.contains("Rust wildcard"), "detail: {detail}");
                    }
                }
                other => panic!("expected UnsupportedSchema for {bad:?}, got {other:?}"),
            }
        }
    }

    // SPEC §5.6 *Scope* / §5.3: a custom logical-type override on a one-field
    // record's lone union field blocks the collapse, so the record keeps its
    // struct and no enum is named after it — leaving nothing for a field-level
    // `"enum"` to conflict with.
    #[test]
    fn logical_override_blocks_collapse_so_field_enum_is_not_a_conflict() {
        let src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "Scalar",
                "namespace": "u",
                "fields": [
                    {"name": "choice", "enum": "MyU",
                     "type": {"type": ["int", "string"], "logicalType": "url"}}
                ]
            }"#],
            &registry_url(),
        );
        assert!(src.contains("pub choice: ::url::Url"), "{src}");
        assert!(!src.contains("enum MyU"), "no enum may be named: {src}");
        assert!(
            squashed_contains(&src, "pub struct Scalar {"),
            "the record must stay a struct: {src}"
        );
        must_parse_as_file(&src);
    }

    // SPEC §5.3 *Attribute placement*, accepted case 2: an override-carrying
    // field is never lowered, so even a non-string `"enum"` on it goes
    // unvalidated. The control asserts the rejection is intact without the
    // override.
    #[test]
    fn non_string_enum_is_ignored_on_a_logical_override_field() {
        let schema = |logical: &str| {
            format!(
                r#"{{
                    "type": "record",
                    "name": "Scalar",
                    "namespace": "u",
                    "fields": [
                        {{"name": "choice", "enum": 5,
                          "type": {{"type": ["int", "string"]{logical}}}}}
                    ]
                }}"#
            )
        };

        // With the override: silently ignored.
        let src = codegen_with_logical(&[&schema(r#", "logicalType": "url""#)], &registry_url());
        assert!(src.contains("pub choice: ::url::Url"), "{src}");
        must_parse_as_file(&src);

        // Control — same schema, no override: still rejected as a non-string.
        let err = codegen_with_logical_result(&[&schema("")], &registry_url())
            .expect_err("non-string `enum` must error without an override");
        match err {
            GeneratorError::UnsupportedSchema { detail } => {
                assert!(detail.contains("must be a string"), "detail: {detail}")
            }
            other => panic!("expected UnsupportedSchema, got {other:?}"),
        }
    }

    // SPEC §5.3: a record field's type tree may hold at most one
    // enum-generating union. Two are rejected both with one at the field root
    // and with neither at the field root.
    #[test]
    fn two_enum_generating_unions_in_one_field_is_unsupported() {
        let cases = [
            // Bare union at the field root + a union inside an array arm.
            r#"["int", {"type": "array", "items": ["long", "bytes"]}]"#,
            // Neither union at the field root: array items are a union, one of
            // whose arms is a map whose values are a second union.
            r#"{"type": "array", "items": ["int", {"type": "map", "values": ["long", "bytes"]}]}"#,
        ];
        for ty in cases {
            let owned = [format!(
                r#"{{"type": "record", "name": "R", "namespace": "u",
                     "fields": [{{"name": "f", "type": {ty}}}]}}"#
            )];
            let schemas = parse_schemas(&owned, None).expect("parse");
            match generate(&schemas, opts()) {
                Err(GeneratorError::UnsupportedSchema { detail }) => {
                    assert!(
                        detail.contains("contains 2 enum-generating unions"),
                        "must report the count for {ty}: {detail}"
                    );
                    assert!(
                        detail.contains("`f`") && detail.contains("`u.R`"),
                        "must name field and record for {ty}: {detail}"
                    );
                }
                other => panic!("expected UnsupportedSchema for {ty}, got {other:?}"),
            }
        }
    }

    // SPEC §5.3: optional `["null", T]` unions are transparent, so a field may
    // nest any number of them. Two stacked here produce
    // `Option<Vec<Option<String>>>` and no enum.
    #[test]
    fn stacked_optional_unions_are_transparent_and_generate_no_enum() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "R",
            "namespace": "u",
            "fields": [
                {"name": "f", "type": ["null", {"type": "array", "items": ["null", "string"]}]}
            ]
        }"#]);
        assert!(src.contains("pub f: Option<Vec<Option<String>>>"), "{src}");
        assert!(!src.contains("enum"), "no enum may be emitted: {src}");
        must_parse_as_file(&src);
    }

    // SPEC §5.3: naming propagates inward from the record field through a
    // transparent `["null", T]` wrapper and then through the array, reaching
    // the field's one enum-generating union.
    #[test]
    fn field_enum_names_union_through_transparent_optional_wrapper() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "R",
            "namespace": "u",
            "fields": [
                {"name": "f", "enum": "Kind",
                 "type": ["null", {"type": "array", "items": ["int", "string"]}]}
            ]
        }"#]);
        assert!(squashed_contains(&src, "pub enum Kind {"), "{src}");
        assert!(src.contains("pub f: Option<Vec<Kind>>"), "{src}");
        must_parse_as_file(&src);
    }

    // SPEC §5.3: a named type is a boundary for the budget, so a field holding
    // one union plus a named type whose own field holds another is accepted.
    #[test]
    fn union_behind_a_named_type_boundary_does_not_count() {
        // (a) via a type reference.
        let by_ref = codegen_from(&[
            r#"{
                "type": "record",
                "name": "Inner",
                "namespace": "u",
                "fields": [
                    {"name": "g", "enum": "GUnion", "type": ["long", "bytes"]},
                    {"name": "h", "type": "long"}
                ]
            }"#,
            r#"{
                "type": "record",
                "name": "R",
                "namespace": "u",
                "fields": [
                    {"name": "f", "type": ["int", "u.Inner"]},
                    {"name": "k", "type": "long"}
                ]
            }"#,
        ]);
        assert!(squashed_contains(&by_ref, "pub enum RUnion {"), "{by_ref}");
        assert!(squashed_contains(&by_ref, "pub enum GUnion {"), "{by_ref}");
        must_parse_as_file(&by_ref);

        // (b) via an inline (SPEC §10.3) record declaration in a union arm.
        let inline = codegen_from(&[r#"{
            "type": "record",
            "name": "R",
            "namespace": "u",
            "fields": [
                {"name": "f", "type": ["int", {
                    "type": "record",
                    "name": "Inline",
                    "fields": [
                        {"name": "g", "enum": "GUnion", "type": ["long", "bytes"]},
                        {"name": "h", "type": "long"}
                    ]
                }]},
                {"name": "k", "type": "long"}
            ]
        }"#]);
        assert!(squashed_contains(&inline, "pub enum RUnion {"), "{inline}");
        assert!(squashed_contains(&inline, "pub enum GUnion {"), "{inline}");
        must_parse_as_file(&inline);
    }

    // SPEC §5.3: the `"enum"` attribute is legal on a record field object
    // only. On an `array` or a `map` — whether reached directly or as a union
    // variant — it is a misplacement, and the error must name the offending
    // node kind and direct the caller to the field.
    #[test]
    fn enum_attribute_on_a_collection_is_unsupported() {
        let cases = [
            (
                r#"{"type": "array", "items": ["int", "string"], "enum": "X"}"#,
                "an `array`",
            ),
            (
                r#"{"type": "map", "values": ["int", "string"], "enum": "X"}"#,
                "a `map`",
            ),
            (
                r#"["null", {"type": "array", "items": ["int", "string"], "enum": "X"}]"#,
                "an `array`",
            ),
            (
                r#"["null", {"type": "map", "values": ["int", "string"], "enum": "X"}]"#,
                "a `map`",
            ),
        ];
        for (ty, kind) in cases {
            let owned = [format!(
                r#"{{"type": "record", "name": "R", "namespace": "u",
                     "fields": [{{"name": "f", "type": {ty}}}]}}"#
            )];
            let schemas = parse_schemas(&owned, None).expect("parse");
            match generate(&schemas, opts()) {
                Err(GeneratorError::UnsupportedSchema { detail }) => {
                    assert!(
                        detail.contains(&format!("found on {kind} schema object")),
                        "must name the offending node for {ty}: {detail}"
                    );
                    assert!(
                        detail.contains("Move it to the field object"),
                        "must give the remedy for {ty}: {detail}"
                    );
                }
                other => panic!("expected UnsupportedSchema for {ty}, got {other:?}"),
            }
        }
    }

    // SPEC §5.3 *Validation and diagnostics*: both structural rules run over
    // the full named set, so neither can be bypassed by hiding the violation
    // inside a §15.3-suppressed record.
    #[test]
    fn structural_rules_cover_a_logical_type_suppressed_record() {
        let mut r = LogicalTypeRegistry::default();
        r.insert(
            "wrapped".to_string(),
            AvroType::Record,
            "::my::Wrap".to_string(),
            None,
            None,
            false,
        );
        // (a) budget violation inside the suppressed record.
        let budget = codegen_with_logical_result(
            &[
                r#"{
                    "type": "record", "name": "Sup", "namespace": "u",
                    "logicalType": "wrapped",
                    "fields": [
                        {"name": "f", "type": ["int", {"type": "array", "items": ["long", "bytes"]}]}
                    ]
                }"#,
                r#"{
                    "type": "record", "name": "Host", "namespace": "u",
                    "fields": [{"name": "at", "type": "u.Sup"}]
                }"#,
            ],
            &r,
        )
        .expect_err("budget violation in a suppressed record must error");
        match budget {
            GeneratorError::UnsupportedSchema { detail } => assert!(
                detail.contains("contains 2 enum-generating unions") && detail.contains("`u.Sup`"),
                "detail: {detail}"
            ),
            other => panic!("expected UnsupportedSchema, got {other:?}"),
        }

        // (b) misplaced `"enum"` inside the suppressed record.
        let misplaced = codegen_with_logical_result(
            &[
                r#"{
                    "type": "record", "name": "Sup", "namespace": "u",
                    "logicalType": "wrapped",
                    "fields": [
                        {"name": "f",
                         "type": {"type": "array", "items": ["int", "string"], "enum": "X"}}
                    ]
                }"#,
                r#"{
                    "type": "record", "name": "Host", "namespace": "u",
                    "fields": [{"name": "at", "type": "u.Sup"}]
                }"#,
            ],
            &r,
        )
        .expect_err("misplaced `enum` in a suppressed record must error");
        match misplaced {
            GeneratorError::UnsupportedSchema { detail } => assert!(
                detail.contains("found on an `array` schema object"),
                "detail: {detail}"
            ),
            other => panic!("expected UnsupportedSchema, got {other:?}"),
        }
    }

    // SPEC §5.3: `"enum"` is a vanavro-internal naming hint, so it is stripped
    // from the field entry of the structural `schema()` ctor. A sibling custom
    // attribute must survive, proving the filter is keyed on the `"enum"` key
    // and does not drop the whole map.
    #[test]
    fn field_enum_attribute_is_stripped_from_emitted_schema() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "R",
            "namespace": "u",
            "fields": [
                {"name": "f", "enum": "MyU", "other": "kept", "type": ["int", "string"]},
                {"name": "g", "type": "long"}
            ]
        }"#]);
        // The name is used: the enum is emitted under it.
        assert!(squashed_contains(&src, "pub enum MyU {"), "{src}");
        // …but the attribute itself never reaches `custom_attributes`.
        assert!(
            !squashed_contains(&src, r#"("enum".to_string()"#),
            "`enum` must be stripped from schema(): {src}"
        );
        assert!(
            squashed_contains(&src, r#"("other".to_string()"#),
            "sibling custom attribute must survive: {src}"
        );
        must_parse_as_file(&src);
    }

    // SPEC §5.3 *Attribute placement*, accepted case 1: an `"enum"` key on a
    // `record` / `enum` / `fixed` schema object is inert user metadata and must
    // survive into the structural `schema()` untouched. No fingerprint test
    // would catch its loss — the canonical form drops unknown attributes.
    #[test]
    fn enum_attribute_on_a_named_type_is_not_stripped_from_emitted_schema() {
        let src = codegen_from(&[
            r#"{
                "type": "record",
                "name": "R",
                "namespace": "u",
                "enum": "record-level-metadata",
                "fields": [{"name": "f", "type": "long"}]
            }"#,
            r#"{
                "type": "enum",
                "name": "E",
                "namespace": "u",
                "enum": "enum-level-metadata",
                "symbols": ["A", "B"]
            }"#,
            r#"{
                "type": "fixed",
                "name": "F",
                "namespace": "u",
                "enum": "fixed-level-metadata",
                "size": 4
            }"#,
        ]);
        for value in [
            "record-level-metadata",
            "enum-level-metadata",
            "fixed-level-metadata",
        ] {
            assert!(
                src.contains(value),
                "`enum` on a named type is user metadata and must survive: {src}"
            );
        }
        must_parse_as_file(&src);
    }

    // End-to-end against the vendored `union.avdl`: `TestRecord` has a single
    // union field, so it collapses to a public enum that takes the record's own
    // name `TestRecord` (SPEC §5.6) — no wrapping struct, no `TestRecordUnion`.
    #[cfg(feature = "idl")]
    #[test]
    fn end_to_end_union_avdl() {
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/idl/union.avdl");
        let schemas = parse_schemas(&[], Some(&path)).expect("parse");
        let src = generate(&schemas, opts()).expect("codegen");
        assert!(squashed_contains(&src, "pub enum TestRecord {"), "{src}");
        // SR1 / SR2 PascalCase to Sr1 / Sr2 — both the struct name and the
        // variant payload type follow the converted identifier.
        assert!(src.contains("Sr1(Sr1)"), "{src}");
        assert!(src.contains("Sr2(Sr2)"), "{src}");
        assert!(
            src.contains("rename = \"org.apache.avro.gen.SR1\""),
            "FQN tag: {src}"
        );
        must_parse_as_file(&src);
    }

    // End-to-end against `named_unions.avdl`, the worked example covering
    // every `@enum` union-naming shape (SPEC §5.3).
    #[cfg(feature = "idl")]
    #[test]
    fn end_to_end_named_unions_avdl() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/idl/named_unions.avdl");
        let schemas = parse_schemas(&[], Some(&path)).expect("parse");
        let src = generate(&schemas, opts()).expect("codegen");

        // (1) field-root union -> default `<Record>Union`.
        assert!(
            squashed_contains(&src, "pub enum CatalogueUnion {"),
            "{src}"
        );
        assert!(src.contains("pub primary_key: CatalogueUnion"), "{src}");

        // (2) optional union -> Option, no enum.
        assert!(src.contains("pub nickname: Option<String>"), "{src}");

        // (3) single-variant, (4) 3+ variant, named on the field.
        assert!(squashed_contains(&src, "pub enum Single {"), "{src}");
        assert!(src.contains("pub single: Single"), "{src}");
        assert!(squashed_contains(&src, "pub enum Many {"), "{src}");
        assert!(src.contains("pub many: Many"), "{src}");

        // (5)-(8) each `@enum` names its extracted enum, threaded through the
        // right collection / Option wrapper.
        assert!(squashed_contains(&src, "pub enum Tag {"), "{src}");
        assert!(src.contains("pub tags: Vec<Tag>"), "{src}");

        assert!(squashed_contains(&src, "pub enum Score {"), "{src}");
        assert!(
            src.contains("pub scores: ::std::collections::HashMap<String, Score>"),
            "{src}"
        );

        assert!(squashed_contains(&src, "pub enum Weight {"), "{src}");
        assert!(
            src.contains(
                "pub optional_scores: Option<::std::collections::HashMap<String, Weight>>"
            ),
            "{src}"
        );

        assert!(squashed_contains(&src, "pub enum Kind {"), "{src}");
        assert!(
            src.contains("pub variant_array: Option<Vec<Kind>>"),
            "{src}"
        );

        // (9)/(10) two field-level `@enum`s name two bare-union fields.
        assert!(squashed_contains(&src, "pub enum Alpha {"), "{src}");
        assert!(src.contains("pub alpha: Alpha"), "{src}");
        assert!(squashed_contains(&src, "pub enum Beta {"), "{src}");
        assert!(src.contains("pub beta: Beta"), "{src}");

        must_parse_as_file(&src);
    }

    // ---- keyword escaping (SPEC §5.2) ----------------------------------------

    /// Rust strict keywords used as Avro field names get the raw-identifier
    /// prefix (`r#…`) plus a `#[serde(rename = "…")]` so the wire format
    /// keeps the original spelling.
    #[test]
    fn rust_keyword_field_name_gets_raw_prefix_and_rename() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Kw",
            "namespace": "k",
            "fields": [
                {"name": "type",  "type": "string"},
                {"name": "match", "type": "int"},
                {"name": "fn",    "type": "long"}
            ]
        }"#]);
        for (rust, avro) in [("r#type", "type"), ("r#match", "match"), ("r#fn", "fn")] {
            assert!(
                src.contains(&format!("pub {rust}:")),
                "expected `pub {rust}:` in: {src}"
            );
            assert!(
                src.contains(&format!("rename = \"{avro}\"")),
                "expected serde rename for `{avro}`: {src}"
            );
        }
        must_parse_as_file(&src);
    }

    // `self`-family keywords can't be raw-escaped — SPEC §5.2 says append `_`.
    // Field position; type and variant positions are covered separately.
    #[test]
    fn self_family_keyword_field_gets_underscore_suffix() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "S",
            "namespace": "k",
            "fields": [
                {"name": "self",  "type": "int"},
                {"name": "super", "type": "int"}
            ]
        }"#]);
        assert!(src.contains("pub self_: i32"), "{src}");
        assert!(src.contains("rename = \"self\""), "{src}");
        assert!(src.contains("pub super_: i32"), "{src}");
        assert!(src.contains("rename = \"super\""), "{src}");
        must_parse_as_file(&src);
    }

    // SPEC §5.2 `_` edge case: Avro permits a bare `_` (and `__`, …) as a
    // name/field/symbol. `heck` collapses all-underscore names to the empty
    // string, which would panic `Ident::new`; a bare `_` is also a Rust
    // wildcard rather than a binding. The fallback appends `_` to the
    // original, so `_` → `__` (a valid binding) with a serde rename back to
    // the wire name `_`.
    #[test]
    fn underscore_field_name_becomes_double_underscore() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "R",
            "namespace": "u",
            "fields": [
                {"name": "_",  "type": "int"},
                {"name": "__", "type": "long"}
            ]
        }"#]);
        // `_` -> `__`, `__` -> `___`; both distinct, neither a wildcard.
        assert!(src.contains("pub __: i32"), "{src}");
        assert!(src.contains("rename = \"_\""), "{src}");
        assert!(src.contains("pub ___: i64"), "{src}");
        assert!(src.contains("rename = \"__\""), "{src}");
        must_parse_as_file(&src);
    }

    // SPEC §5.2 leading-digit edge case: `heck` strips the leading underscore
    // of a legal Avro name like `_1`, leaving a digit-initial string that
    // would panic `Ident::new` (`Ident cannot be a number`). The fix prepends
    // `_`. `_1` -> `_1` (ident == wire name, so no rename); `_42x` -> `_42x`.
    #[test]
    fn leading_digit_field_name_is_prefixed_with_underscore() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "R",
            "namespace": "u",
            "fields": [
                {"name": "_1",   "type": "int"},
                {"name": "_42x", "type": "long"}
            ]
        }"#]);
        assert!(src.contains("pub _1: i32"), "{src}");
        assert!(src.contains("pub _42x: i64"), "{src}");
        // ident already equals the wire name, so no rename is needed.
        assert!(!src.contains("rename = \"_1\""), "{src}");
        must_parse_as_file(&src);
    }

    /// A leading-digit name whose repaired ident *differs* from the wire name
    /// must carry a `#[serde(rename)]`, or the wire encoding silently corrupts.
    /// `_1Abc` snake-cases to `1abc`, repairs to `_1abc` (≠ wire `_1Abc`).
    #[test]
    fn leading_digit_field_needing_rename_carries_one() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "R",
            "namespace": "u",
            "fields": [{"name": "_1Abc", "type": "int"}]
        }"#]);
        assert!(src.contains("pub _1abc: i32"), "{src}");
        assert!(src.contains("rename = \"_1Abc\""), "{src}");
        must_parse_as_file(&src);
    }

    /// A leading-digit type name (`_1` -> `_1`) and enum symbol must not panic
    /// and must produce valid, non-digit-initial identifiers.
    #[test]
    fn leading_digit_type_and_symbol_do_not_panic() {
        let src = codegen_from(&[
            r#"{"type":"record","name":"_1","namespace":"u","fields":[{"name":"v","type":"int"}]}"#,
            r#"{"type":"enum","name":"E","namespace":"u","symbols":["_2digit","OK"]}"#,
        ]);
        assert!(src.contains("pub struct _1"), "{src}");
        // `_2digit` snake/pascal -> `2digit` -> prefixed `_2digit`.
        assert!(squashed_contains(&src, "_2digit,"), "variant ident: {src}");
        must_parse_as_file(&src);
    }

    /// Repaired idents that fold together (`_1_` and `__1__` both reduce to
    /// `_1`) must raise a clean `NameCollision`, never panic `Ident::new`.
    #[test]
    fn leading_digit_collision_is_reported_not_panicked() {
        let owned = vec![
            r#"{
            "type": "record",
            "name": "R",
            "namespace": "u",
            "fields": [
                {"name": "_1_",   "type": "int"},
                {"name": "__1__", "type": "long"}
            ]
        }"#
            .to_string(),
        ];
        let schemas = parse_schemas(&owned, None).expect("parse");
        let err = generate(&schemas, opts()).expect_err("folded idents must collide");
        assert!(
            matches!(err, GeneratorError::NameCollision { .. }),
            "{err:?}"
        );
    }

    /// A record/enum/fixed type named entirely with underscores must still
    /// produce a non-wildcard type identifier.
    #[test]
    fn underscore_type_name_becomes_double_underscore() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "_",
            "namespace": "u",
            "fields": [{"name": "v", "type": "int"}]
        }"#]);
        assert!(src.contains("pub struct __"), "{src}");
        assert!(src.contains("rename = \"_\""), "{src}");
        must_parse_as_file(&src);
    }

    // A `fixed` type named `_` must not panic and must produce a non-wildcard
    // alias. The Rust ident differs from the Avro name with no rename, because
    // a `pub type` alias cannot carry serde attributes (SPEC §5.2 / §11.3).
    #[test]
    fn underscore_fixed_type_name_becomes_double_underscore() {
        let src = codegen_from(&[r#"{
            "type": "fixed",
            "name": "_",
            "namespace": "u",
            "size": 4
        }"#]);
        assert!(src.contains("pub type __ = [u8; 4]"), "{src}");
        must_parse_as_file(&src);
    }

    /// An enum symbol named `_` must become a non-wildcard variant `__` with a
    /// rename back to `_`. Asserts the concrete variant token, not a bare
    /// `contains("__")` (which `_`-internal idents could satisfy spuriously).
    #[test]
    fn underscore_enum_symbol_becomes_double_underscore() {
        let src = codegen_from(&[r#"{
            "type": "enum",
            "name": "E",
            "namespace": "u",
            "symbols": ["_", "A"]
        }"#]);
        assert!(src.contains("pub enum E"), "{src}");
        // The variant is the token `__`, immediately preceding the next
        // variant / the closing brace — match it anchored to a serde rename.
        assert!(squashed_contains(&src, "rename = \"_\")] __"), "{src}");
        must_parse_as_file(&src);
    }

    /// A namespace segment of only underscores must not emit `pub mod _`
    /// (a wildcard) nor panic on the empty converted segment.
    #[test]
    fn underscore_namespace_segment_becomes_double_underscore() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "R",
            "namespace": "_",
            "fields": [{"name": "v", "type": "int"}]
        }"#]);
        assert!(src.contains("pub mod __"), "{src}");
        must_parse_as_file(&src);
    }

    /// A `_`-named type used as a multi-variant union member must name its
    /// enum variant without panicking on the empty pascal-cased candidate,
    /// and the variant must be non-empty and carry its fully-qualified Avro
    /// tag as a serde rename (so the wire encoding stays `u2._`).
    #[test]
    fn underscore_named_type_in_union_variant() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Holder",
            "namespace": "u",
            "fields": [
                {"name": "x", "type": [
                    "null",
                    {"type":"record","name":"_","namespace":"u2","fields":[{"name":"v","type":"int"}]},
                    "int"
                ]}
            ]
        }"#]);
        // The `_`-named record's union variant pascal-cases to `__` (non-wild).
        assert!(squashed_contains(&src, "rename = \"u2._\")] __"), "{src}");
        must_parse_as_file(&src);
    }

    /// Type name `Self` (uppercase — a Rust keyword) → `Self_`.
    #[test]
    fn self_named_record_gets_underscore_suffix() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Self",
            "namespace": "k",
            "fields": [{"name": "v", "type": "int"}]
        }"#]);
        assert!(src.contains("pub struct Self_"), "{src}");
        assert!(src.contains("rename = \"Self\""), "{src}");
        must_parse_as_file(&src);
    }

    /// Enum symbol `SELF` pascal-cases to `Self`, which is a Rust keyword and
    /// must be escaped to `Self_`.
    #[test]
    fn enum_variant_colliding_with_self_keyword_is_escaped() {
        let src = codegen_from(&[r#"{
            "type": "enum",
            "name": "E",
            "namespace": "k",
            "symbols": ["SELF", "OTHER"]
        }"#]);
        assert!(src.contains("Self_"), "{src}");
        assert!(src.contains("rename = \"SELF\""), "{src}");
        must_parse_as_file(&src);
    }

    /// Lock the keyword lists against proc-macro2 behaviour drift: every
    /// entry in `RUST_RAW_ESCAPABLE_KEYWORDS` must survive `Ident::new_raw`,
    /// and every entry in `RUST_SELF_FAMILY_KEYWORDS` must trip its panic.
    /// If either invariant flips, our `escape_ident` would either drop
    /// keyword support silently or panic on legitimate input.
    #[test]
    fn keyword_lists_match_proc_macro2_raw_ident_behaviour() {
        use std::panic;

        for kw in RUST_RAW_ESCAPABLE_KEYWORDS {
            let res = panic::catch_unwind(|| Ident::new_raw(kw, Span::call_site()));
            assert!(
                res.is_ok(),
                "`{kw}` is in RUST_RAW_ESCAPABLE_KEYWORDS but proc-macro2 \
                 rejected it from Ident::new_raw — move it to \
                 RUST_SELF_FAMILY_KEYWORDS or drop it"
            );
        }

        for kw in RUST_SELF_FAMILY_KEYWORDS {
            let res = panic::catch_unwind(|| Ident::new_raw(kw, Span::call_site()));
            assert!(
                res.is_err(),
                "`{kw}` is in RUST_SELF_FAMILY_KEYWORDS but proc-macro2 \
                 accepted it as a raw ident — it may have been promoted; \
                 consider moving it to RUST_RAW_ESCAPABLE_KEYWORDS"
            );
        }
    }

    /// `extern` historically had quirky raw-ident handling; pin that it
    /// works at the field position so a regression in our keyword list (or
    /// in proc-macro2's panic surface) trips here.
    #[test]
    fn extern_keyword_field_works_as_raw_ident() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Linkage",
            "namespace": "k",
            "fields": [{"name": "extern", "type": "string"}]
        }"#]);
        assert!(src.contains("pub r#extern: String"), "{src}");
        assert!(src.contains("rename = \"extern\""), "{src}");
        must_parse_as_file(&src);
    }

    /// `gen` became reserved in edition 2024. We carry it in the list — pin
    /// the behaviour so removing it from the list breaks here, not via an
    /// opaque `syn::parse` error on downstream input.
    #[test]
    fn gen_keyword_field_is_raw_escaped() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "G",
            "namespace": "k",
            "fields": [{"name": "gen", "type": "int"}]
        }"#]);
        assert!(src.contains("pub r#gen: i32"), "{src}");
        assert!(src.contains("rename = \"gen\""), "{src}");
        must_parse_as_file(&src);
    }

    /// Avro field name `Loop` (PascalCase) snake-cases to `loop`, which is
    /// a keyword. Escaping must run on the *post-conversion* identifier.
    #[test]
    fn case_conversion_to_keyword_still_escapes() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "R",
            "namespace": "k",
            "fields": [{"name": "Loop", "type": "int"}]
        }"#]);
        assert!(src.contains("pub r#loop: i32"), "{src}");
        assert!(src.contains("rename = \"Loop\""), "{src}");
        must_parse_as_file(&src);
    }

    /// Avro field name `Crate` snake-cases to `crate`, which is in the
    /// self-family and must take a trailing-underscore (not `r#`) escape.
    #[test]
    fn case_conversion_to_self_family_uses_underscore_suffix() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "R",
            "namespace": "k",
            "fields": [{"name": "Crate", "type": "int"}]
        }"#]);
        assert!(src.contains("pub crate_: i32"), "{src}");
        assert!(src.contains("rename = \"Crate\""), "{src}");
        must_parse_as_file(&src);
    }

    // SPEC §11.3 requires namespace segments to convert to `snake_case`
    // before being used as `pub mod` identifiers. PascalCase Avro
    // namespaces must produce idiomatic Rust module names.
    #[test]
    fn pascal_case_namespace_segments_become_snake_case_modules() {
        let src = codegen_from(&[
            r#"{"type":"record","name":"Hello","namespace":"OrgX.MySvc","fields":[{"name":"v","type":"int"}]}"#,
        ]);
        assert!(src.contains("pub mod org_x"), "{src}");
        assert!(src.contains("pub mod my_svc"), "{src}");
        must_parse_as_file(&src);
    }

    /// A namespace segment that collides with a Rust keyword must be raw-
    /// escaped both at the `pub mod` declaration and in the relative path.
    #[test]
    fn keyword_namespace_segment_is_escaped_in_module_and_path() {
        let src = codegen_from(&[
            r#"{"type":"record","name":"Leaf","namespace":"org.type","fields":[{"name":"v","type":"int"}]}"#,
            r#"{"type":"record","name":"Holder","namespace":"org.consumer","fields":[{"name":"l","type":"org.type.Leaf"}]}"#,
        ]);
        assert!(
            src.contains("pub mod r#type"),
            "namespace segment must be raw-escaped at mod declaration: {src}"
        );
        assert!(
            src.contains("super::r#type::Leaf"),
            "namespace segment must be raw-escaped in cross-namespace path: {src}"
        );
        must_parse_as_file(&src);
    }

    // ---- recursion-via-Box (SPEC §5.4) ----------------------------------------

    /// Self-referential record: `next: ["null", Node]` must lower to
    /// `Option<Box<Node>>`, not `Option<Node>` (infinite-size).
    #[test]
    fn self_referential_record_boxes_the_optional_back_edge() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Node",
            "namespace": "r",
            "fields": [
                {"name": "value", "type": "int"},
                {"name": "next",  "type": ["null", "r.Node"], "default": null}
            ]
        }"#]);
        assert!(
            src.contains("Option<Box<Node>>"),
            "expected Option<Box<Node>>: {src}"
        );
        must_parse_as_file(&src);
    }

    /// Mutually recursive A → B → A: both back-edges must box. Both records
    /// share the same namespace, so the ref path is the bare name (no
    /// `super::`).
    #[test]
    fn mutually_recursive_records_box_both_back_edges() {
        let src = codegen_from(&[
            r#"{"type":"record","name":"A","namespace":"m","fields":[{"name":"b","type":["null","m.B"],"default":null}]}"#,
            r#"{"type":"record","name":"B","namespace":"m","fields":[{"name":"a","type":["null","m.A"],"default":null}]}"#,
        ]);
        assert!(src.contains("pub b: Option<Box<B>>"), "{src}");
        assert!(src.contains("pub a: Option<Box<A>>"), "{src}");
        must_parse_as_file(&src);
    }

    /// Cycle through `array` is structurally broken by `Vec`'s heap
    /// allocation — no `Box` needed.
    #[test]
    fn self_reference_through_array_does_not_box() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Tree",
            "namespace": "r",
            "fields": [{
                "name": "children",
                "type": {"type": "array", "items": "r.Tree"}
            }]
        }"#]);
        assert!(src.contains("Vec<Tree>"), "{src}");
        assert!(!src.contains("Box<"), "no Box expected through Vec: {src}");
        must_parse_as_file(&src);
    }

    /// Same for `map` — `HashMap` heap-allocates its values store.
    #[test]
    fn self_reference_through_map_does_not_box() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Forest",
            "namespace": "r",
            "fields": [{
                "name": "by_id",
                "type": {"type": "map", "values": "r.Forest"}
            }]
        }"#]);
        assert!(src.contains("HashMap<String, Forest>"), "{src}");
        assert!(!src.contains("Box<"), "no Box expected through Map: {src}");
        must_parse_as_file(&src);
    }

    /// Direct non-cyclic record reference must NOT be boxed (over-boxing
    /// guard).
    #[test]
    fn acyclic_record_reference_does_not_box() {
        let src = codegen_from(&[
            r#"{"type":"record","name":"Inner","namespace":"r","fields":[{"name":"v","type":"int"}]}"#,
            r#"{"type":"record","name":"Outer","namespace":"r","fields":[{"name":"i","type":["null","r.Inner"],"default":null}]}"#,
        ]);
        assert!(src.contains("Option<Inner>"), "{src}");
        assert!(
            !src.contains("Box<"),
            "no Box expected for acyclic ref: {src}"
        );
        must_parse_as_file(&src);
    }

    /// Transitive cycle A → B → C → A: every edge participates in the cycle,
    /// so all three back-edges must box. Guards the DFS-based `can_reach`
    /// pass against shallow-cycle-only bugs.
    #[test]
    fn long_cycle_a_b_c_a_boxes_every_back_edge() {
        let src = codegen_from(&[
            r#"{"type":"record","name":"A","namespace":"l","fields":[{"name":"b","type":["null","l.B"],"default":null}]}"#,
            r#"{"type":"record","name":"B","namespace":"l","fields":[{"name":"c","type":["null","l.C"],"default":null}]}"#,
            r#"{"type":"record","name":"C","namespace":"l","fields":[{"name":"a","type":["null","l.A"],"default":null}]}"#,
        ]);
        assert!(src.contains("pub b: Option<Box<B>>"), "{src}");
        assert!(src.contains("pub c: Option<Box<C>>"), "{src}");
        assert!(src.contains("pub a: Option<Box<A>>"), "{src}");
        must_parse_as_file(&src);
    }

    /// A single record with both a cyclic ref (self → self) and a non-cyclic
    /// ref (self → leaf record) — only the cyclic field gets boxed. Probes
    /// the per-record `box_targets` isolation.
    #[test]
    fn mixed_cyclic_and_acyclic_refs_only_box_the_cyclic_one() {
        let src = codegen_from(&[
            r#"{"type":"record","name":"Leaf","namespace":"mx","fields":[{"name":"v","type":"int"}]}"#,
            r#"{
                "type": "record",
                "name": "Mixed",
                "namespace": "mx",
                "fields": [
                    {"name": "self_ref", "type": ["null", "mx.Mixed"], "default": null},
                    {"name": "leaf",     "type": ["null", "mx.Leaf"],  "default": null}
                ]
            }"#,
        ]);
        assert!(
            src.contains("pub self_ref: Option<Box<Mixed>>"),
            "self-ref must box: {src}"
        );
        assert!(
            src.contains("pub leaf: Option<Leaf>"),
            "acyclic leaf ref must NOT box: {src}"
        );
        must_parse_as_file(&src);
    }

    /// End-to-end against the vendored `cycle.avdl`. Its two cycles —
    /// SampleNode→Vec<SamplePair>→SampleNode and SelfRef→Vec<SelfRef> — both
    /// route through a `Vec`, so no `Box` is needed. The asserts pin "no over-boxing" alongside the structural
    /// shapes.
    #[cfg(feature = "idl")]
    #[test]
    fn end_to_end_cycle_avdl() {
        let path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/idl/cycle.avdl");
        let schemas = parse_schemas(&[], Some(&path)).expect("parse");
        let src = generate(&schemas, opts()).expect("codegen");
        assert!(src.contains("Vec<SamplePair>"), "{src}");
        assert!(src.contains("Vec<SelfRef>"), "{src}");
        assert!(src.contains("pub node: SampleNode"), "{src}");
        assert!(
            !src.contains("Box<"),
            "all cycle.avdl back-edges route through Vec — no Box expected: {src}"
        );
        must_parse_as_file(&src);
    }

    // ---- logical types (SPEC §3.3) --------------------------------------------

    // `date` / `time-millis` / `time-micros` / `timestamp-*` / `local-timestamp-*`
    // all lower to their underlying primitive (i32 or i64) in Rust per SPEC §3.3.
    #[test]
    fn time_logical_types_lower_to_primitive_integers() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Times",
            "namespace": "lt",
            "fields": [
                {"name": "d",  "type": {"type": "int",  "logicalType": "date"}},
                {"name": "tm", "type": {"type": "int",  "logicalType": "time-millis"}},
                {"name": "tu", "type": {"type": "long", "logicalType": "time-micros"}},
                {"name": "ts_ms", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "ts_us", "type": {"type": "long", "logicalType": "timestamp-micros"}},
                {"name": "ts_ns", "type": {"type": "long", "logicalType": "timestamp-nanos"}},
                {"name": "lts_ms", "type": {"type": "long", "logicalType": "local-timestamp-millis"}},
                {"name": "lts_us", "type": {"type": "long", "logicalType": "local-timestamp-micros"}},
                {"name": "lts_ns", "type": {"type": "long", "logicalType": "local-timestamp-nanos"}}
            ]
        }"#]);
        assert!(src.contains("pub d: i32"), "{src}");
        assert!(src.contains("pub tm: i32"), "{src}");
        assert!(src.contains("pub tu: i64"), "{src}");
        assert!(src.contains("pub ts_ms: i64"), "{src}");
        assert!(src.contains("pub ts_us: i64"), "{src}");
        assert!(src.contains("pub ts_ns: i64"), "{src}");
        assert!(src.contains("pub lts_ms: i64"), "{src}");
        assert!(src.contains("pub lts_us: i64"), "{src}");
        assert!(src.contains("pub lts_ns: i64"), "{src}");
        must_parse_as_file(&src);
    }

    /// `uuid` → `::uuid::Uuid`.
    #[test]
    fn uuid_logical_type_lowers_to_uuid_crate_type() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Tagged",
            "namespace": "lt",
            "fields": [{"name": "id", "type": {"type": "string", "logicalType": "uuid"}}]
        }"#]);
        assert!(src.contains("pub id: ::uuid::Uuid"), "{src}");
        must_parse_as_file(&src);
    }

    /// `decimal` (on bytes) → `::apache_avro::Decimal`.
    #[test]
    fn decimal_logical_type_lowers_to_apache_avro_decimal() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Money",
            "namespace": "lt",
            "fields": [{
                "name": "amount",
                "type": {"type": "bytes", "logicalType": "decimal", "precision": 6, "scale": 2}
            }]
        }"#]);
        assert!(src.contains("pub amount: ::apache_avro::Decimal"), "{src}");
        must_parse_as_file(&src);
    }

    /// `duration` (on fixed-12) → `::apache_avro::Duration`.
    #[test]
    fn duration_logical_type_lowers_to_apache_avro_duration() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Span",
            "namespace": "lt",
            "fields": [{
                "name": "len",
                "type": {"type": "fixed", "name": "DurationFixed", "size": 12, "logicalType": "duration"}
            }]
        }"#]);
        assert!(src.contains("pub len: ::apache_avro::Duration"), "{src}");
        must_parse_as_file(&src);
    }

    /// End-to-end against the vendored `logicalTypes.avdl` fixture.
    #[cfg(feature = "idl")]
    #[test]
    fn end_to_end_logical_types_avdl() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/idl/logicalTypes.avdl");
        let schemas = parse_schemas(&[], Some(&path)).expect("parse");
        let src = generate(&schemas, opts()).expect("codegen");
        assert!(src.contains("pub struct LogicalTypeFields"), "{src}");
        assert!(src.contains("::uuid::Uuid"), "{src}");
        assert!(src.contains("::apache_avro::Decimal"), "{src}");
        // date / time / timestamp fields all land on i32 or i64.
        assert!(src.contains("pub a_date: i32"), "{src}");
        assert!(src.contains("pub a_time: i32"), "{src}");
        assert!(src.contains("pub a_timestamp: i64"), "{src}");
        must_parse_as_file(&src);
    }

    // ---- end-to-end Apache fixtures (primitives-only) -------------------------

    #[test]
    fn end_to_end_baz_and_foo_avsc() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/idl");
        let baz = std::fs::read_to_string(root.join("baz.avsc")).expect("baz");
        let foo = std::fs::read_to_string(root.join("foo.avsc")).expect("foo");
        let schemas = parse_schemas(&[baz, foo], None).expect("parse");
        let src = generate(&schemas, opts()).expect("codegen");
        // baz.avsc declares `ns.other.schema.Baz`; foo.avsc declares `org.foo.Foo`.
        assert!(src.contains("pub mod ns"), "{src}");
        assert!(src.contains("pub mod other"), "{src}");
        assert!(src.contains("pub mod org"), "{src}");
        assert!(src.contains("pub mod foo"), "{src}");
        assert!(src.contains("pub struct Baz"), "{src}");
        assert!(src.contains("pub struct Foo"), "{src}");
        must_parse_as_file(&src);
    }

    // ---- doc comments (SPEC §5.1) ---------------------------------------------

    /// Record + field `doc` strings render as outer doc attributes so
    /// prettyplease turns them into `///` comments.
    #[test]
    fn record_and_field_docs_render_as_doc_comments() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Documented",
            "namespace": "d",
            "doc": "A documented record.",
            "fields": [
                {"name": "id", "type": "long", "doc": "The identifier."}
            ]
        }"#]);
        assert!(src.contains("/// A documented record."), "{src}");
        assert!(src.contains("/// The identifier."), "{src}");
        must_parse_as_file(&src);
    }

    // Multi-line docs emit one `#[doc]` per line (SPEC §5.1).
    #[test]
    fn multi_line_doc_emits_one_attr_per_line() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Multi",
            "namespace": "d",
            "doc": "Line one.\nLine two.",
            "fields": [{"name": "v", "type": "int"}]
        }"#]);
        assert!(src.contains("/// Line one."), "{src}");
        assert!(src.contains("/// Line two."), "{src}");
        must_parse_as_file(&src);
    }

    /// Enum `doc` renders on the generated enum.
    #[test]
    fn enum_doc_renders_as_doc_comment() {
        let src = codegen_from(&[r#"{
            "type": "enum",
            "name": "Color",
            "namespace": "d",
            "doc": "A colour.",
            "symbols": ["RED", "GREEN"]
        }"#]);
        assert!(src.contains("/// A colour."), "{src}");
        must_parse_as_file(&src);
    }

    /// Fixed `doc` renders on the type alias.
    #[test]
    fn fixed_doc_renders_as_doc_comment() {
        let src = codegen_from(&[r#"{
            "type": "fixed",
            "name": "Hash16",
            "namespace": "d",
            "doc": "A 16-byte hash.",
            "size": 16
        }"#]);
        assert!(src.contains("/// A 16-byte hash."), "{src}");
        assert!(src.contains("pub type Hash16 = [u8; 16]"), "{src}");
        must_parse_as_file(&src);
    }

    // ---- aliases (SPEC §11.4) -------------------------------------------------

    /// Field aliases emit `#[serde(alias = "…")]`. Record-type aliases do NOT:
    /// serde's `alias` is a field/variant attribute, not a container one, so
    /// emitting it on the struct yields `unknown serde container attribute
    /// 'alias'`. Record-type aliases have no wire representation and survive
    /// only in the structural `Schema`.
    #[test]
    fn field_aliases_emit_serde_alias_record_aliases_do_not() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Aliased",
            "namespace": "a",
            "aliases": ["OldRecord", "LegacyRecord"],
            "fields": [
                {"name": "id", "type": "long", "aliases": ["identifier", "key"]}
            ]
        }"#]);
        assert!(src.contains("alias = \"identifier\""), "{src}");
        assert!(src.contains("alias = \"key\""), "{src}");
        // Record-type aliases must not appear as serde container attributes.
        assert!(!src.contains("alias = \"OldRecord\""), "{src}");
        assert!(!src.contains("alias = \"LegacyRecord\""), "{src}");
        must_parse_as_file(&src);
    }

    /// Enum-type aliases do NOT emit `#[serde(alias = "…")]` — same reasoning
    /// as record-type aliases (serde `alias` is not a container attribute).
    #[test]
    fn enum_aliases_do_not_emit_serde_alias() {
        let src = codegen_from(&[r#"{
            "type": "enum",
            "name": "Suit",
            "namespace": "a",
            "aliases": ["Suits"],
            "symbols": ["HEARTS"]
        }"#]);
        assert!(!src.contains("alias = \"Suits\""), "{src}");
        must_parse_as_file(&src);
    }

    // ---- defaults (SPEC §11.5) ------------------------------------------------

    /// `null` default on `["null", T]` → `#[serde(default)]` (no helper fn).
    #[test]
    fn null_default_on_optional_emits_bare_serde_default() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Opt",
            "namespace": "df",
            "fields": [{"name": "name", "type": ["null", "string"], "default": null}]
        }"#]);
        assert!(src.contains("#[serde(default)]"), "{src}");
        assert!(!src.contains("fn default_"), "no helper fn expected: {src}");
        must_parse_as_file(&src);
    }

    /// Empty-array default leans on `Vec::default` via bare `#[serde(default)]`.
    #[test]
    fn empty_array_default_emits_bare_serde_default() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Bag",
            "namespace": "df",
            "fields": [{
                "name": "tags",
                "type": {"type": "array", "items": "string"},
                "default": []
            }]
        }"#]);
        assert!(src.contains("#[serde(default)]"), "{src}");
        assert!(!src.contains("fn default_"), "no helper fn expected: {src}");
        must_parse_as_file(&src);
    }

    /// Empty-map default leans on `HashMap::default`.
    #[test]
    fn empty_map_default_emits_bare_serde_default() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Dict",
            "namespace": "df",
            "fields": [{
                "name": "by_key",
                "type": {"type": "map", "values": "int"},
                "default": {}
            }]
        }"#]);
        assert!(src.contains("#[serde(default)]"), "{src}");
        must_parse_as_file(&src);
    }

    /// Primitive literal default → `#[serde(default = "default_<rec>_<field>")]`
    /// plus a `pub(super)` helper fn returning the literal.
    #[test]
    fn primitive_literal_default_emits_helper_fn() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Cfg",
            "namespace": "df",
            "fields": [
                {"name": "retries", "type": "int", "default": 3},
                {"name": "name", "type": "string", "default": "anon"},
                {"name": "enabled", "type": "boolean", "default": true}
            ]
        }"#]);
        assert!(src.contains("default = \"default_cfg_retries\""), "{src}");
        assert!(
            src.contains("pub(super) fn default_cfg_retries() -> i32"),
            "{src}"
        );
        assert!(src.contains("3i32"), "{src}");
        assert!(
            src.contains("pub(super) fn default_cfg_name() -> String"),
            "{src}"
        );
        assert!(src.contains("\"anon\".to_string()"), "{src}");
        assert!(
            src.contains("pub(super) fn default_cfg_enabled() -> bool"),
            "{src}"
        );
        assert!(src.contains("true"), "{src}");
        must_parse_as_file(&src);
    }

    /// Non-null default on `["null", T]` lowers to `Some(<literal>)`.
    #[test]
    fn non_null_default_on_optional_wraps_in_some() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Opt2",
            "namespace": "df",
            "fields": [{"name": "count", "type": ["null", "long"], "default": 7}]
        }"#]);
        assert!(
            src.contains("pub(super) fn default_opt2_count() -> Option<i64>"),
            "{src}"
        );
        assert!(src.contains("Some(7i64)"), "{src}");
        must_parse_as_file(&src);
    }

    /// Enum-literal default constructs the variant.
    #[test]
    fn enum_literal_default_constructs_variant() {
        let src = codegen_from(&[
            r#"{"type":"enum","name":"Level","namespace":"df","symbols":["LOW","HIGH"]}"#,
            r#"{
                "type": "record",
                "name": "Setting",
                "namespace": "df",
                "fields": [{"name": "level", "type": "df.Level", "default": "LOW"}]
            }"#,
        ]);
        assert!(
            src.contains("pub(super) fn default_setting_level() -> Level"),
            "{src}"
        );
        assert!(src.contains("Level::Low"), "{src}");
        must_parse_as_file(&src);
    }

    /// Non-empty array default renders a `vec![…]` constructor.
    #[test]
    fn non_empty_array_default_constructs_vec() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Bag2",
            "namespace": "df",
            "fields": [{
                "name": "nums",
                "type": {"type": "array", "items": "int"},
                "default": [1, 2, 3]
            }]
        }"#]);
        assert!(
            src.contains("pub(super) fn default_bag2_nums() -> Vec<i32>"),
            "{src}"
        );
        assert!(src.contains("vec!["), "{src}");
        must_parse_as_file(&src);
    }

    /// A `bytes` field with a default keeps its `serde::bytes` helper and
    /// gains a `#[serde(default = …)]` whose fn rebuilds the byte vector from
    /// the Avro latin-1 JSON-string encoding. Both serde attrs must coexist
    /// and the output must compile.
    #[test]
    fn bytes_field_default_keeps_helper_and_emits_constructor() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "B",
            "namespace": "df",
            "fields": [{"name": "payload", "type": "bytes", "default": "\u0001\u0002"}]
        }"#]);
        assert!(src.contains("serde::bytes"), "{src}");
        assert!(
            src.contains("pub(super) fn default_b_payload() -> Vec<u8>"),
            "{src}"
        );
        assert!(src.contains("b\"\\x01\\x02\".to_vec()"), "{src}");
        must_parse_as_file(&src);
    }

    /// Union ordered `[T, "null"]` is still treated as optional: a non-null
    /// default lowers to `Some(<literal>)` regardless of variant order.
    #[test]
    fn t_then_null_union_default_wraps_in_some() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "O",
            "namespace": "df",
            "fields": [{"name": "name", "type": ["string", "null"], "default": "hi"}]
        }"#]);
        assert!(
            src.contains("pub(super) fn default_o_name() -> Option<String>"),
            "{src}"
        );
        assert!(src.contains("Some(\"hi\".to_string())"), "{src}");
        must_parse_as_file(&src);
    }

    /// A default targeting a multi-variant (non-optional) union enum is not
    /// lowered in v1 and raises a clean error rather than silently dropping
    /// the default.
    #[test]
    fn default_on_multi_variant_union_is_unsupported() {
        let owned = vec![
            r#"{
                "type": "record",
                "name": "M",
                "namespace": "df",
                "fields": [{"name": "v", "type": ["int", "string"], "default": 5}]
            }"#
            .to_string(),
        ];
        let schemas = parse_schemas(&owned, None).expect("parse");
        let err = generate(&schemas, opts()).expect_err("multi-variant union default must error");
        assert!(
            matches!(err, GeneratorError::UnsupportedSchema { .. }),
            "{err:?}"
        );
    }

    /// A default value v1 still can't lower (a `uuid` logical-type literal)
    /// raises a clean error rather than emitting unparseable Rust.
    #[test]
    fn unsupported_default_raises_clean_error() {
        let owned = vec![
            r#"{
                "type": "record",
                "name": "U",
                "namespace": "df",
                "fields": [{
                    "name": "x",
                    "type": {"type": "string", "logicalType": "uuid"},
                    "default": "00000000-0000-0000-0000-000000000000"
                }]
            }"#
            .to_string(),
        ];
        let schemas = parse_schemas(&owned, None).expect("parse");
        let err = generate(&schemas, opts()).expect_err("uuid-literal default must error");
        assert!(
            matches!(err, GeneratorError::UnsupportedSchema { .. }),
            "{err:?}"
        );
    }

    // SPEC §11.5 record literal default: a JSON-object default lowers to a
    // `pub(super) fn default_<rec>_<field>()` returning a struct literal,
    // wired via `#[serde(default = "…")]`.
    #[test]
    fn record_literal_default_lowers_to_struct_literal() {
        let src = codegen_from(&[
            r#"{"type":"record","name":"Inner","namespace":"df","fields":[
                {"name":"v","type":"int"},
                {"name":"w","type":"string"}
            ]}"#,
            r#"{"type":"record","name":"Outer","namespace":"df","fields":[
                {"name":"inner","type":"df.Inner","default":{"v":1,"w":"hi"}}
            ]}"#,
        ]);
        assert!(
            src.contains("#[serde(default = \"default_outer_inner\")]"),
            "{src}"
        );
        // Anchor on the helper body returning the struct literal — not a bare
        // `Inner {`, which would also match the struct *definition*
        // (`pub struct Inner {`). Match each field init independently because
        // prettyplease varies trailing-comma placement with layout.
        assert!(
            squashed_contains(&src, "-> Inner { Inner {"),
            "expected helper to return an `Inner` struct literal: {src}"
        );
        assert!(squashed_contains(&src, "v: 1i32"), "{src}");
        assert!(squashed_contains(&src, r#"w: "hi".to_string()"#), "{src}");
        must_parse_as_file(&src);
    }

    // SPEC §11.5 missing-field fallback: a record default object that omits a
    // field falls back to that field's own declared default.
    #[test]
    fn record_literal_default_uses_field_fallback() {
        let src = codegen_from(&[
            r#"{"type":"record","name":"Inner","namespace":"df","fields":[
                {"name":"v","type":"int"},
                {"name":"w","type":"string","default":"fallback"}
            ]}"#,
            r#"{"type":"record","name":"Outer","namespace":"df","fields":[
                {"name":"inner","type":"df.Inner","default":{"v":1}}
            ]}"#,
        ]);
        // `w` absent from the object -> uses Inner.w's own default "fallback".
        assert!(squashed_contains(&src, "v: 1i32"), "{src}");
        assert!(
            squashed_contains(&src, r#"w: "fallback".to_string()"#),
            "{src}"
        );
        must_parse_as_file(&src);
    }

    /// A nested record default and an optional field inside the record lower
    /// correctly when the optional field is *present*: `Some(..)`.
    #[test]
    fn record_literal_default_handles_nested_and_optional() {
        let src = codegen_from(&[
            r#"{"type":"record","name":"Leaf","namespace":"df","fields":[
                {"name":"n","type":"long"}
            ]}"#,
            r#"{"type":"record","name":"Mid","namespace":"df","fields":[
                {"name":"leaf","type":"df.Leaf"},
                {"name":"opt","type":["null","int"],"default":null}
            ]}"#,
            r#"{"type":"record","name":"Top","namespace":"df","fields":[
                {"name":"mid","type":"df.Mid","default":{"leaf":{"n":5},"opt":7}}
            ]}"#,
        ]);
        assert!(squashed_contains(&src, "Leaf { n: 5i64 }"), "{src}");
        assert!(squashed_contains(&src, "opt: Some(7i32)"), "{src}");
        must_parse_as_file(&src);
    }

    /// Regression for the `null`-on-optional bug fixed alongside record-literal
    /// defaults: a nested optional sub-field set to `null` in the record
    /// default object must lower to `None` (previously it fed `null` into the
    /// non-null payload and spuriously raised `UnsupportedSchema`). Nested
    /// record (and array / map element) defaults are what reach
    /// `default_expr`'s `Schema::Union` `null` arm, as the top-level `null` is
    /// short-circuited earlier in `field_default`.
    #[test]
    fn record_literal_default_lowers_nested_null_to_none() {
        let src = codegen_from(&[
            r#"{"type":"record","name":"Mid","namespace":"df","fields":[
                {"name":"opt","type":["null","int"]}
            ]}"#,
            r#"{"type":"record","name":"Top","namespace":"df","fields":[
                {"name":"mid","type":"df.Mid","default":{"opt":null}}
            ]}"#,
        ]);
        assert!(squashed_contains(&src, "opt: None"), "{src}");
        must_parse_as_file(&src);
    }

    // SPEC §11.5 error path: a record default whose nested value is itself
    // unrepresentable must raise a clean `UnsupportedSchema` rather than emit
    // a struct literal that won't compile.
    //
    // This exercises `record_default_expr`'s reachable `None` path. The *other*
    // `None` path (a field omitted from the default object with no own default)
    // is unreachable via the public API: apache-avro rejects an incomplete
    // record default at parse time (`SchemaParse`), so it never reaches
    // codegen — hence there is no test for it (there is nothing to test).
    #[test]
    fn record_default_with_unrepresentable_nested_field_errors() {
        // `Inner.w` is a `uuid` logical type; the record default supplies it, so
        // `record_default_expr` recurses into a value `default_expr` cannot
        // lower -> `None` -> `UnsupportedSchema`.
        let owned = vec![
            r#"{"type":"record","name":"Inner","namespace":"df","fields":[
                {"name":"v","type":"int"},
                {"name":"w","type":{"type":"string","logicalType":"uuid"}}
            ]}"#
            .to_string(),
            r#"{"type":"record","name":"Outer","namespace":"df","fields":[
                {"name":"inner","type":"df.Inner",
                 "default":{"v":1,"w":"00000000-0000-0000-0000-000000000000"}}
            ]}"#
            .to_string(),
        ];
        let schemas = parse_schemas(&owned, None).expect("parse");
        let err = generate(&schemas, opts()).expect_err("nested uuid default must error");
        assert!(
            matches!(err, GeneratorError::UnsupportedSchema { .. }),
            "{err:?}"
        );
    }

    // ---- Eq + Hash uplift (SPEC §11.1) ----------------------------------------

    /// Returns true iff the generated source contains a derive list with the
    /// `Eq + Hash` uplift. prettyplease may render the derive on one line or
    /// wrapped across many, so we strip all whitespace before matching the
    /// `PartialEq, Eq, Hash,` fragment — this avoids word-boundary false
    /// positives (`HashMap`, `PartialEq`) while being layout-insensitive.
    fn has_eq_hash_uplift(src: &str) -> bool {
        let squashed: String = src.chars().filter(|c| !c.is_whitespace()).collect();
        squashed.contains("PartialEq,Eq,Hash,")
    }

    fn count_eq_hash_uplifts(src: &str) -> usize {
        let squashed: String = src.chars().filter(|c| !c.is_whitespace()).collect();
        squashed.matches("PartialEq,Eq,Hash,").count()
    }

    /// A record whose fields are all `Eq + Hash` (ints / strings / bytes)
    /// gains the `Eq, Hash` derives.
    #[test]
    fn all_eq_hash_fields_uplift_record_derive() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Keyish",
            "namespace": "eh",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "blob", "type": "bytes"}
            ]
        }"#]);
        assert!(has_eq_hash_uplift(&src), "expected Eq+Hash uplift: {src}");
        must_parse_as_file(&src);
    }

    /// A `double` (or `float`) field has no `Eq`/`Hash`, so the record must
    /// NOT gain the uplift.
    #[test]
    fn float_field_blocks_eq_hash_uplift() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Measured",
            "namespace": "eh",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "ratio", "type": "double"}
            ]
        }"#]);
        assert!(!has_eq_hash_uplift(&src), "float must block uplift: {src}");
        must_parse_as_file(&src);
    }

    /// A `map` field is `Eq` but `HashMap` is not `Hash`, so the record must
    /// NOT gain the uplift (the whole `Eq + Hash` pair is gated together).
    #[test]
    fn map_field_blocks_eq_hash_uplift() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Dictish",
            "namespace": "eh",
            "fields": [{"name": "by_key", "type": {"type": "map", "values": "int"}}]
        }"#]);
        assert!(!has_eq_hash_uplift(&src), "map must block uplift: {src}");
        must_parse_as_file(&src);
    }

    /// `Vec<T>` of an `Eq + Hash` element keeps the record qualifying, but a
    /// `Vec<f64>` does not.
    #[test]
    fn array_propagates_element_eq_hash() {
        let ok = codegen_from(&[r#"{
            "type": "record",
            "name": "Tags",
            "namespace": "eh",
            "fields": [{"name": "xs", "type": {"type": "array", "items": "string"}}]
        }"#]);
        assert!(has_eq_hash_uplift(&ok), "Vec<String> uplifts: {ok}");
        must_parse_as_file(&ok);

        let bad = codegen_from(&[r#"{
            "type": "record",
            "name": "Floats",
            "namespace": "eh",
            "fields": [{"name": "xs", "type": {"type": "array", "items": "double"}}]
        }"#]);
        assert!(!has_eq_hash_uplift(&bad), "Vec<f64> blocks uplift: {bad}");
        must_parse_as_file(&bad);
    }

    /// A record referencing another record: the referrer qualifies only if
    /// the referent does. Here `Outer` holds `Inner` (all-int) → both uplift.
    #[test]
    fn record_ref_propagates_eq_hash_when_referent_qualifies() {
        let src = codegen_from(&[
            r#"{"type":"record","name":"Inner","namespace":"eh","fields":[{"name":"v","type":"int"}]}"#,
            r#"{"type":"record","name":"Outer","namespace":"eh","fields":[{"name":"inner","type":"eh.Inner"}]}"#,
        ]);
        assert_eq!(count_eq_hash_uplifts(&src), 2, "both records uplift: {src}");
        must_parse_as_file(&src);
    }

    /// If the referent is disqualified (has a float), the referrer is too.
    #[test]
    fn record_ref_blocks_eq_hash_when_referent_disqualifies() {
        let src = codegen_from(&[
            r#"{"type":"record","name":"Inner","namespace":"eh","fields":[{"name":"v","type":"double"}]}"#,
            r#"{"type":"record","name":"Outer","namespace":"eh","fields":[{"name":"inner","type":"eh.Inner"}]}"#,
        ]);
        assert!(
            !has_eq_hash_uplift(&src),
            "float in Inner blocks both: {src}"
        );
        must_parse_as_file(&src);
    }

    /// A self-referential record of otherwise `Eq + Hash` fields still
    /// qualifies — `derive(Eq, Hash)` works across `Box`ed back-edges. The
    /// fixpoint must not spuriously drop a pure-Eq+Hash cycle.
    #[test]
    fn recursive_eq_hash_record_still_uplifts() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Node",
            "namespace": "eh",
            "fields": [
                {"name": "value", "type": "long"},
                {"name": "next", "type": ["null", "eh.Node"], "default": null}
            ]
        }"#]);
        assert!(src.contains("Option<Box<Node>>"), "boxed back-edge: {src}");
        assert!(has_eq_hash_uplift(&src), "cycle uplifts: {src}");
        must_parse_as_file(&src);
    }

    /// A multi-variant union field lowers to a sibling enum emitted without
    /// `Eq`/`Hash`, so the owning record must not uplift.
    #[test]
    fn multi_variant_union_field_blocks_eq_hash_uplift() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Holder",
            "namespace": "eh",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "v", "type": ["int", "string", "long"]}
            ]
        }"#]);
        assert!(
            !has_eq_hash_uplift(&src),
            "union field blocks uplift: {src}"
        );
        must_parse_as_file(&src);
    }

    /// Fields referencing a top-level enum and fixed both lower to
    /// `Eq + Hash` types, so the owning record uplifts. Exercises the
    /// `Schema::Ref` → enum / fixed arms of `is_eq_hash`.
    #[test]
    fn enum_and_fixed_refs_keep_record_eq_hash() {
        let src = codegen_from(&[
            r#"{"type":"enum","name":"Color","namespace":"eh","symbols":["RED","GREEN"]}"#,
            r#"{"type":"fixed","name":"Id16","namespace":"eh","size":16}"#,
            r#"{
                "type": "record",
                "name": "Row",
                "namespace": "eh",
                "fields": [
                    {"name": "color", "type": "eh.Color"},
                    {"name": "id", "type": "eh.Id16"}
                ]
            }"#,
        ]);
        assert!(has_eq_hash_uplift(&src), "enum+fixed refs uplift: {src}");
        must_parse_as_file(&src);
    }

    // ---- bon::Builder opt-in (SPEC §2.1) --------------------------------------

    /// With the builder flag off, no `bon::Builder` derive or `#[builder(…)]`
    /// attributes appear.
    #[test]
    fn builder_disabled_emits_no_bon_artifacts() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "R",
            "namespace": "b",
            "fields": [{"name": "name", "type": "string"}]
        }"#]);
        assert!(!src.contains("bon::Builder"), "{src}");
        assert!(!src.contains("#[builder"), "{src}");
        must_parse_as_file(&src);
    }

    /// With the builder flag on, records gain `::bon::Builder` in the derive
    /// list and String fields gain `#[builder(into)]`.
    #[test]
    fn builder_enabled_adds_derive_and_into_on_string() {
        let src = codegen_with_builder(&[r#"{
            "type": "record",
            "name": "User",
            "namespace": "b",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"}
            ]
        }"#]);
        assert!(src.contains("::bon::Builder"), "derive expected: {src}");
        // `id` (i64) gets no `into`; `name` (String) does.
        assert!(
            squashed_contains(&src, "#[builder(into)]"),
            "into on String: {src}"
        );
        must_parse_as_file(&src);
    }

    /// `Option<String>` also gets `#[builder(into)]` (inner type is String),
    /// and must NOT get a builder default even when an Avro default exists —
    /// bon rejects `default` on `Option`.
    #[test]
    fn builder_into_on_optional_string_no_default() {
        let src = codegen_with_builder(&[r#"{
            "type": "record",
            "name": "Opt",
            "namespace": "b",
            "fields": [
                {"name": "nick", "type": ["null", "string"], "default": null}
            ]
        }"#]);
        assert!(
            squashed_contains(&src, "#[builder(into)]"),
            "into on Option<String>: {src}"
        );
        // No builder default on the Option field (bon would reject it).
        assert!(
            !squashed_contains(&src, "builder(default"),
            "no builder default on Option: {src}"
        );
        // serde still gets its default for the null union.
        assert!(
            src.contains("#[serde(default)]"),
            "serde default kept: {src}"
        );
        must_parse_as_file(&src);
    }

    /// A non-Option scalar default reuses the `default_<rec>_<field>` fn via
    /// `#[builder(default = default_cfg_retries())]`.
    #[test]
    fn builder_default_reuses_helper_fn() {
        let src = codegen_with_builder(&[r#"{
            "type": "record",
            "name": "Cfg",
            "namespace": "b",
            "fields": [
                {"name": "retries", "type": "int", "default": 3}
            ]
        }"#]);
        assert!(
            squashed_contains(&src, "#[builder(default = default_cfg_retries())]"),
            "builder default reuses helper fn: {src}"
        );
        // The helper fn is still emitted once.
        assert!(src.contains("fn default_cfg_retries() -> i32"), "{src}");
        must_parse_as_file(&src);
    }

    /// A String field with a literal default merges `into` + `default` into a
    /// single `#[builder(into, default = …)]`.
    #[test]
    fn builder_merges_into_and_default_on_string() {
        let src = codegen_with_builder(&[r#"{
            "type": "record",
            "name": "Cfg2",
            "namespace": "b",
            "fields": [
                {"name": "tag", "type": "string", "default": "anon"}
            ]
        }"#]);
        assert!(
            squashed_contains(&src, "#[builder(into, default = default_cfg2_tag())]"),
            "merged into + default: {src}"
        );
        must_parse_as_file(&src);
    }

    /// Empty-array default → `#[builder(default)]` (bare) on the non-Option
    /// Vec field.
    #[test]
    fn builder_bare_default_on_empty_array() {
        let src = codegen_with_builder(&[r#"{
            "type": "record",
            "name": "Bag",
            "namespace": "b",
            "fields": [
                {"name": "tags", "type": {"type": "array", "items": "string"}, "default": []}
            ]
        }"#]);
        assert!(
            squashed_contains(&src, "#[builder(default)]"),
            "bare builder default: {src}"
        );
        must_parse_as_file(&src);
    }

    /// Keyword field names (raw idents like `r#type`) must coexist with the
    /// builder attributes and the `default_<rec>_<field>` helper fn. bon
    /// accepts raw-ident fields (verified against bon 3.8.1); this pins that
    /// our emitted shape parses.
    #[test]
    fn builder_handles_keyword_field_names() {
        let src = codegen_with_builder(&[r#"{
            "type": "record",
            "name": "Kw",
            "namespace": "b",
            "fields": [
                {"name": "type", "type": "int", "default": 5},
                {"name": "match", "type": "string"}
            ]
        }"#]);
        assert!(src.contains("pub r#type: i32"), "{src}");
        assert!(src.contains("pub r#match: String"), "{src}");
        // helper fn name snake_cases the original (`type` is not a keyword as
        // part of `default_kw_type`).
        assert!(src.contains("fn default_kw_type() -> i32"), "{src}");
        assert!(
            squashed_contains(&src, "#[builder(default = default_kw_type())]"),
            "{src}"
        );
        // `match` (String) still gets into.
        assert!(squashed_contains(&src, "#[builder(into)]"), "{src}");
        must_parse_as_file(&src);
    }

    /// End-to-end: builder output across enum + record still parses and the
    /// enum does NOT receive a builder derive (only records do).
    #[test]
    fn builder_only_applies_to_records_not_enums() {
        let src = codegen_with_builder(&[
            r#"{"type":"enum","name":"E","namespace":"b","symbols":["A","B"]}"#,
            r#"{"type":"record","name":"Rec","namespace":"b","fields":[{"name":"x","type":"int"}]}"#,
        ]);
        // Exactly one bon::Builder derive (on the record).
        assert_eq!(src.matches("::bon::Builder").count(), 1, "{src}");
        must_parse_as_file(&src);
    }

    // ---- post-conversion collision detection (SPEC §5.2) ----------------------

    /// Run codegen and return the `Result`, so collision tests can assert the
    /// error variant without unwrapping.
    fn codegen_result(jsons: &[&str]) -> Result<String, GeneratorError> {
        let owned: Vec<String> = jsons.iter().map(|s| s.to_string()).collect();
        let schemas = parse_schemas(&owned, None).expect("parse");
        generate(&schemas, opts())
    }

    /// `firstName` and `first_name` both snake-case to `first_name` — a clean
    /// `NameCollision`, not unparseable Rust.
    #[test]
    fn field_snake_case_collision_is_reported() {
        let err = codegen_result(&[r#"{
            "type": "record",
            "name": "R",
            "namespace": "c",
            "fields": [
                {"name": "firstName", "type": "int"},
                {"name": "first_name", "type": "int"}
            ]
        }"#])
        .expect_err("must collide");
        match err {
            GeneratorError::NameCollision {
                kind,
                rust_ident,
                first,
                second,
                ..
            } => {
                assert_eq!(kind, "fields in record");
                assert_eq!(rust_ident, "first_name");
                assert_eq!(first, "firstName");
                assert_eq!(second, "first_name");
            }
            other => panic!("expected NameCollision, got {other:?}"),
        }
    }

    /// `self` and `self_` both fold to `self_` under the self-family escape.
    #[test]
    fn field_self_family_collision_is_reported() {
        let err = codegen_result(&[r#"{
            "type": "record",
            "name": "R",
            "namespace": "c",
            "fields": [
                {"name": "self", "type": "int"},
                {"name": "self_", "type": "int"}
            ]
        }"#])
        .expect_err("must collide");
        assert!(
            matches!(err, GeneratorError::NameCollision { ref rust_ident, .. } if rust_ident == "self_"),
            "{err:?}"
        );
    }

    /// `my_type` and `MyType` both pascal-case to `MyType` within a namespace.
    #[test]
    fn type_pascal_case_collision_is_reported() {
        let err = codegen_result(&[
            r#"{"type":"record","name":"my_type","namespace":"c","fields":[{"name":"v","type":"int"}]}"#,
            r#"{"type":"record","name":"MyType","namespace":"c","fields":[{"name":"v","type":"int"}]}"#,
        ])
        .expect_err("must collide");
        match err {
            GeneratorError::NameCollision {
                kind,
                scope,
                rust_ident,
                ..
            } => {
                assert_eq!(kind, "types in namespace");
                assert_eq!(scope, "c");
                assert_eq!(rust_ident, "MyType");
            }
            other => panic!("expected NameCollision, got {other:?}"),
        }
    }

    /// Enum symbols `LOW` and `Low` both pascal-case to `Low`.
    #[test]
    fn enum_symbol_collision_is_reported() {
        let err = codegen_result(&[
            r#"{"type":"enum","name":"E","namespace":"c","symbols":["LOW","Low"]}"#,
        ])
        .expect_err("must collide");
        assert!(
            matches!(err, GeneratorError::NameCollision { ref kind, .. } if kind == "symbols in enum"),
            "{err:?}"
        );
    }

    /// Namespace segments `OrgX` and `org_x` both snake-case to `org_x`,
    /// which would otherwise emit two `pub mod org_x` declarations.
    #[test]
    fn namespace_segment_collision_is_reported() {
        let err = codegen_result(&[
            r#"{"type":"record","name":"A","namespace":"OrgX","fields":[{"name":"v","type":"int"}]}"#,
            r#"{"type":"record","name":"B","namespace":"org_x","fields":[{"name":"v","type":"int"}]}"#,
        ])
        .expect_err("must collide");
        match err {
            GeneratorError::NameCollision {
                kind,
                scope,
                rust_ident,
                ..
            } => {
                assert_eq!(kind, "namespace segments");
                assert_eq!(scope, "<root>");
                assert_eq!(rust_ident, "org_x");
            }
            other => panic!("expected NameCollision, got {other:?}"),
        }
    }

    /// A nested namespace-segment collision reports the parent path as scope.
    #[test]
    fn nested_namespace_segment_collision_reports_parent() {
        let err = codegen_result(&[
            r#"{"type":"record","name":"A","namespace":"org.MySvc","fields":[{"name":"v","type":"int"}]}"#,
            r#"{"type":"record","name":"B","namespace":"org.my_svc","fields":[{"name":"v","type":"int"}]}"#,
        ])
        .expect_err("must collide");
        assert!(
            matches!(err, GeneratorError::NameCollision { ref scope, ref rust_ident, .. }
                if scope == "org" && rust_ident == "my_svc"),
            "{err:?}"
        );
    }

    /// Distinct names that DON'T collide after conversion still generate fine.
    #[test]
    fn non_colliding_names_generate_cleanly() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "R",
            "namespace": "c",
            "fields": [
                {"name": "firstName", "type": "int"},
                {"name": "lastName", "type": "int"}
            ]
        }"#]);
        assert!(src.contains("pub first_name: i32"), "{src}");
        assert!(src.contains("pub last_name: i32"), "{src}");
        must_parse_as_file(&src);
    }

    /// The same namespace reused across many types is NOT a collision (the
    /// segment recurs with an identical original spelling).
    #[test]
    fn repeated_identical_namespace_is_not_a_collision() {
        let src = codegen_from(&[
            r#"{"type":"record","name":"A","namespace":"org.foo","fields":[{"name":"v","type":"int"}]}"#,
            r#"{"type":"record","name":"B","namespace":"org.foo","fields":[{"name":"w","type":"int"}]}"#,
        ]);
        assert!(src.contains("pub mod org"), "{src}");
        assert!(src.contains("pub mod foo"), "{src}");
        must_parse_as_file(&src);
    }

    // SPEC §5.2: a type ident equal to a child module ident is a redefinition
    // (`E0428`). `_1` is case-invariant under the sanitizer repair, so record
    // `a._1` and namespace `a._1` land on one ident.
    #[test]
    fn type_colliding_with_sibling_module_is_reported() {
        let err = codegen_result(&[
            r#"{"type":"record","name":"_1","namespace":"a","fields":[{"name":"y","type":"int"}]}"#,
            r#"{"type":"record","name":"S","namespace":"a._1","fields":[{"name":"x","type":"int"}]}"#,
        ])
        .expect_err("must collide");
        match err {
            GeneratorError::NameCollision {
                kind,
                scope,
                rust_ident,
                first,
                second,
            } => {
                assert_eq!(kind, "types and namespace segments");
                assert_eq!(scope, "a");
                assert_eq!(rust_ident, "_1");
                assert_eq!(first, "_1 (namespace segment)");
                assert_eq!(second, "_1");
            }
            other => panic!("expected NameCollision, got {other:?}"),
        }
    }

    /// The same clash at the root, via the all-underscore repair (`_` → `__`).
    /// Here the segment's original spelling differs from its ident, so this is
    /// the case that pins both sides to the *original* Avro names — the ident
    /// is already reported as `rust_ident`.
    #[test]
    fn type_colliding_with_sibling_module_at_root_is_reported() {
        let err = codegen_result(&[
            r#"{"type":"record","name":"_","fields":[{"name":"y","type":"int"}]}"#,
            r#"{"type":"record","name":"S","namespace":"_","fields":[{"name":"x","type":"int"}]}"#,
        ])
        .expect_err("must collide");
        match err {
            GeneratorError::NameCollision {
                kind,
                scope,
                rust_ident,
                first,
                second,
            } => {
                assert_eq!(kind, "types and namespace segments");
                assert_eq!(scope, "<root>");
                assert_eq!(rust_ident, "__");
                assert_eq!(first, "_ (namespace segment)");
                assert_eq!(second, "_");
            }
            other => panic!("expected NameCollision, got {other:?}"),
        }
    }

    // SPEC §5.3: a verbatim `"enum"` union name is not PascalCased, so it can
    // equal a child module ident where an ordinary type name could not.
    #[test]
    fn union_enum_colliding_with_sibling_module_is_reported() {
        let err = codegen_result(&[
            r#"{"type":"record","name":"R","namespace":"a",
                "fields":[{"name":"f","enum":"b",
                           "type":{"type":"array","items":["int","string"]}}]}"#,
            r#"{"type":"record","name":"S","namespace":"a.b","fields":[{"name":"x","type":"int"}]}"#,
        ])
        .expect_err("must collide");
        match err {
            GeneratorError::NameCollision {
                kind,
                scope,
                rust_ident,
                first,
                second,
            } => {
                assert_eq!(kind, "types and namespace segments");
                assert_eq!(scope, "a");
                assert_eq!(rust_ident, "b");
                assert_eq!(first, "b (namespace segment)");
                assert_eq!(
                    second,
                    "b (union enum for record `R` field `f`, nested in an `array`)"
                );
            }
            other => panic!("expected NameCollision, got {other:?}"),
        }
    }

    /// A PascalCase type and a snake_case module sibling stay distinct — the
    /// cross-check must not over-fire on the ordinary case.
    #[test]
    fn pascal_type_and_snake_module_sibling_do_not_collide() {
        let src = codegen_from(&[
            r#"{"type":"record","name":"B","namespace":"a","fields":[{"name":"y","type":"int"}]}"#,
            r#"{"type":"record","name":"S","namespace":"a.b","fields":[{"name":"x","type":"int"}]}"#,
        ]);
        assert!(src.contains("pub struct B"), "{src}");
        assert!(src.contains("pub mod b"), "{src}");
        must_parse_as_file(&src);
    }

    // ---- per-type schema() / fingerprint() API (SPEC §12, §5.5) ---------------

    /// Records expose `schema()` (LazyLock-backed) and a generic
    /// `fingerprint::<D>()`.
    #[test]
    fn record_exposes_schema_and_fingerprint_api() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "R",
            "namespace": "api",
            "fields": [{"name": "v", "type": "int"}]
        }"#]);
        assert!(
            squashed_contains(&src, "impl R {"),
            "expected impl block: {src}"
        );
        assert!(
            squashed_contains(&src, "pub fn schema() -> &'static ::apache_avro::Schema"),
            "{src}"
        );
        assert!(
            squashed_contains(
                &src,
                "pub fn fingerprint<D: ::digest::Digest>() -> ::apache_avro::schema::SchemaFingerprint"
            ),
            "{src}"
        );
        assert!(
            squashed_contains(&src, "::std::sync::LazyLock::<::apache_avro::Schema>::new")
                || squashed_contains(&src, "::std::sync::LazyLock<::apache_avro::Schema>"),
            "LazyLock-backed static expected: {src}"
        );
        assert!(
            squashed_contains(&src, "Self::schema().fingerprint::<D>()"),
            "{src}"
        );
        // Implements the apache_avro::AvroSchema trait (idiomatic integration
        // with Writer / the derive ecosystem).
        assert!(
            squashed_contains(&src, "impl ::apache_avro::AvroSchema for R")
                && squashed_contains(
                    &src,
                    "fn get_schema() -> ::apache_avro::Schema { Self::schema().clone() }"
                ),
            "AvroSchema impl expected: {src}"
        );
        // Built structurally, not parsed from a string.
        assert!(
            !src.contains("parse_str"),
            "no runtime string parsing: {src}"
        );
        must_parse_as_file(&src);
    }

    /// Unit enums expose the same API.
    #[test]
    fn enum_exposes_schema_and_fingerprint_api() {
        let src =
            codegen_from(&[r#"{"type":"enum","name":"E","namespace":"api","symbols":["A","B"]}"#]);
        assert!(squashed_contains(&src, "impl E {"), "{src}");
        assert!(squashed_contains(&src, "pub fn schema()"), "{src}");
        assert!(
            squashed_contains(&src, "pub fn fingerprint<D: ::digest::Digest>()"),
            "{src}"
        );
        assert!(
            squashed_contains(
                &src,
                "::apache_avro::Schema::Enum(::apache_avro::schema::EnumSchema"
            ),
            "structural enum schema: {src}"
        );
        assert!(
            squashed_contains(&src, "impl ::apache_avro::AvroSchema for E"),
            "AvroSchema impl expected: {src}"
        );
        must_parse_as_file(&src);
    }

    /// Fixed lowers to a type alias, which can't host an `impl`; sibling free
    /// fns `{snake}_schema()` / `{snake}_fingerprint::<D>()` are emitted.
    #[test]
    fn fixed_exposes_sibling_schema_fns() {
        let src = codegen_from(&[r#"{"type":"fixed","name":"Md5","namespace":"api","size":16}"#]);
        assert!(
            squashed_contains(
                &src,
                "pub fn md5_schema() -> &'static ::apache_avro::Schema"
            ),
            "{src}"
        );
        assert!(
            squashed_contains(
                &src,
                "pub fn md5_fingerprint<D: ::digest::Digest>() -> ::apache_avro::schema::SchemaFingerprint"
            ),
            "{src}"
        );
        assert!(
            squashed_contains(
                &src,
                "::apache_avro::Schema::Fixed(::apache_avro::schema::FixedSchema"
            ),
            "{src}"
        );
        // No `impl Md5` (it's a type alias).
        assert!(!squashed_contains(&src, "impl Md5"), "{src}");
        must_parse_as_file(&src);
    }

    /// Multi-variant union enums expose `schema()` / `fingerprint()` too, and
    /// the schema is a `Schema::Union`. (Second field keeps `Holder` off the
    /// single-union collapse path so the sibling `HolderUnion` enum exists.)
    #[test]
    fn union_enum_exposes_schema_api() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Holder",
            "namespace": "api",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "v", "type": ["int", "string", "long"]}
            ]
        }"#]);
        // The union sibling enum is `HolderUnion`.
        assert!(squashed_contains(&src, "impl HolderUnion {"), "{src}");
        assert!(
            squashed_contains(&src, "pub fn fingerprint<D: ::digest::Digest>()"),
            "{src}"
        );
        assert!(
            squashed_contains(
                &src,
                "::apache_avro::Schema::Union(::apache_avro::schema::UnionSchema::new"
            ),
            "{src}"
        );
        assert!(
            squashed_contains(&src, "impl ::apache_avro::AvroSchema for HolderUnion"),
            "{src}"
        );
        must_parse_as_file(&src);
    }

    /// Count occurrences of `needle` in whitespace-squashed `src`.
    fn count_ctors(src: &str, needle: &str) -> usize {
        let squashed: String = src.chars().filter(|c| !c.is_whitespace()).collect();
        let n: String = needle.chars().filter(|c| !c.is_whitespace()).collect();
        squashed.matches(&n).count()
    }

    const RECORD_CTOR: &str = "::apache_avro::Schema::Record(::apache_avro::schema::RecordSchema";
    const ENUM_CTOR: &str = "::apache_avro::Schema::Enum(::apache_avro::schema::EnumSchema";
    const FIXED_STRUCT: &str = "::apache_avro::schema::FixedSchema{";

    /// A dependency chain A0 -> A1 -> … -> A(N-1) must generate **exactly N**
    /// record-schema constructions — one per shared builder — not O(N^2). Before
    /// the shared-builder refactor each type's `schema()` inlined all downstream
    /// types, so the count grew as N*(N+1)/2. Enums and fixeds each get one
    /// builder too, and the substring proxies are guarded so a `prettyplease`
    /// spelling change can't silently drop the count to 0 and still "pass".
    #[test]
    fn schema_builders_are_linear_not_quadratic() {
        const N: usize = 20;
        let mut jsons: Vec<String> = Vec::new();
        for i in 0..N {
            // Each record refers to the next by *name* (exercises the ref arm);
            // the last carries an inline enum + fixed plus a leaf primitive.
            let field = if i + 1 < N {
                format!(r#"{{"name":"next","type":"c.A{}"}}"#, i + 1)
            } else {
                concat!(
                    r#"{"name":"color","type":{"type":"enum","name":"Hue","symbols":["R","G"]}},"#,
                    r#"{"name":"tag","type":{"type":"fixed","name":"Tag4","size":4}},"#,
                    r#"{"name":"leaf","type":"int"}"#
                )
                .to_string()
            };
            jsons.push(format!(
                r#"{{"type":"record","name":"A{i}","namespace":"c","fields":[{field}]}}"#
            ));
        }
        let refs: Vec<&str> = jsons.iter().map(String::as_str).collect();
        let src = codegen_from(&refs);

        let record_ctors = count_ctors(&src, RECORD_CTOR);
        let enum_ctors = count_ctors(&src, ENUM_CTOR);
        let fixed_structs = count_ctors(&src, FIXED_STRUCT);
        // Guard the proxies themselves: if the emitted spelling ever changes, these fire instead of
        // the counts silently going to 0.
        assert!(record_ctors > 0, "record ctor proxy matched nothing: {src}");
        assert!(enum_ctors > 0, "enum ctor proxy matched nothing: {src}");
        assert!(
            fixed_structs > 0,
            "fixed struct proxy matched nothing: {src}"
        );
        // Exactly one construction per shared builder — the whole point of the
        // refactor. Quadratic would be N*(N+1)/2 == 210 records.
        assert_eq!(
            record_ctors, N,
            "expected exactly {N} record ctors (one per builder), got {record_ctors}: {src}"
        );
        assert_eq!(enum_ctors, 1, "the single inline enum builds once: {src}");
        assert_eq!(
            fixed_structs, 1,
            "the single inline fixed builds once: {src}"
        );
        must_parse_as_file(&src);
    }

    /// A diamond (`Root -> {L, R} -> Shared`) still builds each type exactly
    /// once: `Shared` is referenced from two fields but has a single shared
    /// builder, so inlining regressions (which would duplicate it) are caught.
    #[test]
    fn schema_builders_diamond_builds_each_type_once() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "Root",
            "namespace": "d",
            "fields": [
                {"name": "left", "type": {
                    "type": "record", "name": "L",
                    "fields": [{"name": "s", "type": {
                        "type": "record", "name": "Shared",
                        "fields": [{"name": "id", "type": "long"}]
                    }}]
                }},
                {"name": "right", "type": {
                    "type": "record", "name": "R",
                    "fields": [{"name": "s", "type": "d.Shared"}]
                }}
            ]
        }"#]);
        // Root, L, R, Shared => exactly 4 record ctors (one builder each), not 5
        // (which is what re-inlining the twice-referenced `Shared` would give).
        assert_eq!(
            count_ctors(&src, RECORD_CTOR),
            4,
            "diamond must build each of Root/L/R/Shared once: {src}"
        );
        must_parse_as_file(&src);
    }

    // A recursive record's builder returns a `Schema::Ref` back-edge on its
    // second visit (no infinite emission), and a cross-type reference calls the
    // referenced type's shared builder rather than inlining it (SPEC §12).
    #[test]
    fn recursive_and_cross_ref_schema_construction() {
        let src = codegen_from(&[
            r#"{"type":"enum","name":"Color","namespace":"s","symbols":["RED","GREEN"]}"#,
            r#"{
                "type": "record",
                "name": "Node",
                "namespace": "s",
                "fields": [
                    {"name": "next", "type": ["null", "s.Node"], "default": null},
                    {"name": "color", "type": "s.Color"}
                ]
            }"#,
        ]);
        // Node's builder guards against re-expansion: on a repeat visit it
        // returns a `Schema::Ref` back-edge built from the runtime `name` local.
        assert!(
            squashed_contains(
                &src,
                "if !defined.insert(name.clone()) { return ::apache_avro::Schema::Ref { name }; }"
            ),
            "builder must emit an insert-guarded back-reference: {src}"
        );
        // Color is defined exactly once (its shared builder), not inlined into
        // every referrer: exactly one EnumSchema construction in the output.
        let squashed: String = src.chars().filter(|c| !c.is_whitespace()).collect();
        let enum_ctors = squashed
            .matches("::apache_avro::Schema::Enum(::apache_avro::schema::EnumSchema")
            .count();
        assert_eq!(
            enum_ctors, 1,
            "Color enum should be constructed once in its shared builder: {src}"
        );
        // Node's builder calls Color's builder rather than inlining it.
        let color_builder =
            builder_ident(&::apache_avro::schema::Name::new("s.Color").expect("name"))
                .expect("builder ident")
                .to_string();
        assert!(
            squashed_contains(&src, &format!("{color_builder}(defined)")),
            "Node's builder must call Color's builder: {src}"
        );
        must_parse_as_file(&src);
    }

    /// Sibling items (type definition, `impl` blocks, the `AvroSchema` impl)
    /// are separated by a single blank line for readability.
    #[test]
    fn sibling_items_separated_by_blank_line() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "R",
            "namespace": "sp",
            "fields": [{"name": "v", "type": "int"}]
        }"#]);
        // struct closes, blank line, then `impl R`.
        assert!(
            src.contains("}\n\n    impl R {"),
            "blank line between struct and impl: {src}"
        );
        // inherent impl closes, blank line, then the trait impl.
        assert!(
            src.contains("}\n\n    impl ::apache_avro::AvroSchema for R {"),
            "blank line before AvroSchema impl: {src}"
        );
        // Struct fields must NOT be blank-separated.
        assert!(
            !src.contains("pub v: i32,\n\n"),
            "fields must not be split: {src}"
        );
        must_parse_as_file(&src);
    }

    /// Field defaults are preserved in the structural schema as
    /// `serde_json::Value` literals.
    #[test]
    fn schema_preserves_field_default_as_json_value() {
        let src = codegen_from(&[r#"{
            "type": "record",
            "name": "D",
            "namespace": "api",
            "fields": [{"name": "n", "type": "int", "default": 7}]
        }"#]);
        assert!(
            squashed_contains(&src, "default : Some(::serde_json::Value::Number"),
            "default preserved as serde_json value: {src}"
        );
        must_parse_as_file(&src);
    }

    // ---- custom logical types (SPEC §15) --------------------------------------

    use crate::logical::AvroType;

    fn registry_url() -> LogicalTypeRegistry {
        let mut r = LogicalTypeRegistry::default();
        r.insert(
            "url".to_string(),
            AvroType::String,
            "::url::Url".to_string(),
            None,
            None,
            false,
        );
        r
    }

    // SPEC §15.3 case 1: a primitive `string` field annotated
    // `@logicalType("url")` maps to the registered `::url::Url` with no
    // `serde_as` (converterless).
    #[test]
    fn custom_logical_bare_string_maps_to_rust_type() {
        let src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "Link",
                "namespace": "web",
                "fields": [
                    {"name": "href", "type": {"type": "string", "logicalType": "url"}}
                ]
            }"#],
            &registry_url(),
        );
        assert!(src.contains("pub href: ::url::Url"), "{src}");
        assert!(
            !src.contains("serde_as"),
            "no serde_as without converter: {src}"
        );
        must_parse_as_file(&src);
    }

    /// `codegen_with_logical`, but returns the `Result` so negative tests can
    /// assert on the error instead of unwrapping.
    fn codegen_with_logical_result(
        jsons: &[&str],
        registry: &LogicalTypeRegistry,
    ) -> Result<String, GeneratorError> {
        let owned: Vec<String> = jsons.iter().map(|s| s.to_string()).collect();
        let resolved = crate::schema::parse(&owned, None).expect("parse");
        super::generate(&resolved.schemas, &resolved.raw_names, registry, opts())
    }

    // SPEC §15.6: a `logicalType` that is neither standard nor registered is
    // surfaced as `UnknownLogicalType`, not silently lowered to the base type,
    // so typos (here `url` with no registration) are caught.
    #[test]
    fn unregistered_custom_logical_is_surfaced() {
        let err = codegen_with_logical_result(
            &[r#"{
                "type": "record",
                "name": "Link",
                "namespace": "web",
                "fields": [
                    {"name": "href", "type": {"type": "string", "logicalType": "url"}}
                ]
            }"#],
            &LogicalTypeRegistry::default(),
        )
        .expect_err("unregistered logical type must error");
        assert!(
            matches!(
                &err,
                GeneratorError::UnknownLogicalType { logical_type, field, .. }
                    if logical_type == "url" && field == "href"
            ),
            "{err:?}"
        );
    }

    // SPEC §15.6 opt-out: with `allow_unregistered_logical_types`, an unregistered
    // non-standard `logicalType` is tolerated and the field lowers to its base
    // type (`String` here) instead of raising `UnknownLogicalType`.
    #[test]
    fn unregistered_custom_logical_allowed_lowers_to_base_type() {
        let owned = vec![
            r#"{
                "type": "record",
                "name": "Link",
                "namespace": "web",
                "fields": [
                    {"name": "href", "type": {"type": "string", "logicalType": "url"}}
                ]
            }"#
            .to_string(),
        ];
        let resolved = crate::schema::parse(&owned, None).expect("parse");
        let registry = LogicalTypeRegistry::default();
        let src = super::generate(
            &resolved.schemas,
            &resolved.raw_names,
            &registry,
            CodegenOptions {
                non_exhaustive: true,
                builder: false,
                collapse_single_union: true,
                allow_unregistered_logical_types: true,
            },
        )
        .expect("unknown logical type must be tolerated");
        assert!(src.contains("pub href: String"), "{src}");
    }

    // SPEC §15.3 case 3: array items carrying the annotation → `Vec<rust_type>`.
    #[test]
    fn custom_logical_array_items_map_to_vec_of_rust_type() {
        let src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "Links",
                "namespace": "web",
                "fields": [
                    {"name": "hrefs", "type": {
                        "type": "array",
                        "items": {"type": "string", "logicalType": "url"}
                    }}
                ]
            }"#],
            &registry_url(),
        );
        assert!(
            squashed_contains(&src, "pub hrefs: Vec<::url::Url>"),
            "{src}"
        );
        must_parse_as_file(&src);
    }

    // SPEC §15.3 case 4: map values carrying the annotation →
    // `HashMap<String, rust_type>`.
    #[test]
    fn custom_logical_map_values_map_to_hashmap_of_rust_type() {
        let src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "LinkMap",
                "namespace": "web",
                "fields": [
                    {"name": "by_key", "type": {
                        "type": "map",
                        "values": {"type": "string", "logicalType": "url"}
                    }}
                ]
            }"#],
            &registry_url(),
        );
        assert!(
            squashed_contains(
                &src,
                "pub by_key: ::std::collections::HashMap<String, ::url::Url>"
            ),
            "{src}"
        );
        must_parse_as_file(&src);
    }

    // SPEC §15.3 case 5: the non-null arm of `["null", T]` carrying the
    // annotation → `Option<rust_type>`.
    #[test]
    fn custom_logical_optional_union_maps_to_option_of_rust_type() {
        let src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "MaybeLink",
                "namespace": "web",
                "fields": [
                    {"name": "href", "type": [
                        "null",
                        {"type": "string", "logicalType": "url"}
                    ], "default": null}
                ]
            }"#],
            &registry_url(),
        );
        assert!(
            squashed_contains(&src, "pub href: Option<::url::Url>"),
            "{src}"
        );
        must_parse_as_file(&src);
    }

    // A converter-backed `email` mapping plus a converter-backed *record-level*
    // `money` mapping — the two shapes a multi-variant union arm can resolve
    // through (SPEC §15.3 case 6 and case 2).
    fn registry_union_arms() -> LogicalTypeRegistry {
        let mut r = LogicalTypeRegistry::default();
        r.insert(
            "email".to_string(),
            AvroType::String,
            "crate::Email".to_string(),
            Some("crate::EmailAsString".to_string()),
            None,
            false,
        );
        r.insert(
            "money".to_string(),
            AvroType::Record,
            "crate::Money".to_string(),
            Some("crate::MoneyConv".to_string()),
            None,
            false,
        );
        r
    }

    // A record holding a 4-arm union (`null` / annotated `string` / a
    // record-level-mapped ref / `int`) plus `extra`, whose presence keeps the
    // record off the §5.6 collapse path. Shared by the case-6 tests so the
    // collapsed and non-collapsed shapes are driven from the same schema.
    const UNION_ARMS_SCHEMA: &str = r#"[
        {
            "type": "record",
            "name": "Money",
            "namespace": "pay",
            "logicalType": "money",
            "fields": [{"name": "cents", "type": "long"}]
        },
        {
            "type": "record",
            "name": "Target",
            "namespace": "pay",
            "fields": [
                {"name": "value", "type": [
                    "null",
                    {"type": "string", "logicalType": "email"},
                    "pay.Money",
                    "int"
                ]},
                {"name": "extra", "type": "string"}
            ]
        }
    ]"#;

    // SPEC §15.3 case 6 / §15.4: every annotated arm of a multi-variant union
    // takes the mapped `rust_type` as its payload and carries the mapping's
    // converter, with the enum-level `#[serde_as]` that `serde_with` requires.
    // A record-level mapping (case 2) reaches the arm the same way, converter
    // included — the type alone was never enough.
    #[test]
    fn custom_logical_union_arms_map_payload_and_converter() {
        let src = codegen_with_logical(&[UNION_ARMS_SCHEMA], &registry_union_arms());
        assert!(
            src.contains("#[::serde_with::serde_as]"),
            "enum serde_as: {src}"
        );
        assert!(
            squashed_contains(
                &src,
                r#"String(#[serde_as(as = "crate::EmailAsString")] crate::Email)"#,
            ),
            "annotated primitive arm: {src}"
        );
        assert!(
            squashed_contains(
                &src,
                r#"Money(#[serde_as(as = "crate::MoneyConv")] crate::Money)"#,
            ),
            "suppressed-record arm keeps its converter: {src}"
        );
        // Unannotated arms are untouched, and variant naming still follows the
        // Avro type rather than the mapping (§15.3 case 6).
        assert!(squashed_contains(&src, "Int(i32)"), "{src}");
        assert!(squashed_contains(&src, "Null(())"), "{src}");
        must_parse_as_file(&src);
    }

    // SPEC §15.3 case 6: a converterless mapping on a union arm still replaces
    // the payload type, and emits no `serde_as` anywhere.
    #[test]
    fn custom_logical_union_arm_without_converter_emits_no_serde_as() {
        let src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "Target",
                "namespace": "web",
                "fields": [
                    {"name": "value", "type": [
                        "null",
                        {"type": "string", "logicalType": "url"},
                        "int"
                    ]},
                    {"name": "extra", "type": "string"}
                ]
            }"#],
            &registry_url(),
        );
        assert!(squashed_contains(&src, "String(::url::Url)"), "{src}");
        assert!(!src.contains("serde_as"), "no serde_as: {src}");
        must_parse_as_file(&src);
    }

    // SPEC §15.3 case 6 × §5.6: under the collapse the converter lands on the
    // private carrier for deserialization and inside `SerializeAsWrap` for
    // serialization — the public enum carries neither, since its `Deserialize`
    // routes through the carrier and its `Serialize` is written by hand.
    #[test]
    fn custom_logical_collapsed_union_arm_wires_both_directions() {
        // The same union as `UNION_ARMS_SCHEMA`, minus `extra`, so `Target`
        // collapses to a bare enum.
        let src = codegen_with_logical(
            &[r#"[
                {
                    "type": "record",
                    "name": "Money",
                    "namespace": "pay",
                    "logicalType": "money",
                    "fields": [{"name": "cents", "type": "long"}]
                },
                {
                    "type": "record",
                    "name": "Target",
                    "namespace": "pay",
                    "fields": [
                        {"name": "value", "type": [
                            "null",
                            {"type": "string", "logicalType": "email"},
                            "pay.Money",
                            "int"
                        ]}
                    ]
                }
            ]"#],
            &registry_union_arms(),
        );
        // Public enum: mapped payloads, no serde glue.
        let public = src.split("impl ::serde::Serialize").next().expect("prefix");
        assert!(
            squashed_contains(public, "String(crate::Email)")
                && squashed_contains(public, "Money(crate::Money)"),
            "public enum payloads: {src}"
        );
        assert!(
            !public.contains("serde_as"),
            "public enum needs no serde_as: {src}"
        );
        // Serialize: converter-backed arms wrapped, plain arms passed through.
        // Truncated before the closing `>`: prettyplease breaks the turbofish
        // across lines and leaves a trailing comma, which squashing preserves.
        assert!(
            squashed_contains(
                &src,
                "::serde_with::ser::SerializeAsWrap::<crate::Email, crate::EmailAsString",
            ),
            "wrapped primitive arm: {src}"
        );
        assert!(
            squashed_contains(
                &src,
                "::serde_with::ser::SerializeAsWrap::<crate::Money, crate::MoneyConv",
            ),
            "wrapped record arm: {src}"
        );
        assert!(
            squashed_contains(
                &src,
                r#"serialize_newtype_variant("Target", 3u32, "int", val)"#
            ),
            "unmapped arm passed through unwrapped: {src}"
        );
        // Deserialize carrier: serde_as on the enum and on each mapped arm.
        let carrier = src
            .split("enum TargetReprUnion")
            .nth(1)
            .expect("carrier enum");
        assert!(
            squashed_contains(
                carrier,
                r#"String(#[serde_as(as = "crate::EmailAsString")] crate::Email)"#,
            ),
            "carrier arm serde_as: {src}"
        );
        assert!(
            squashed_contains(
                carrier,
                r#"Money(#[serde_as(as = "crate::MoneyConv")] crate::Money)"#,
            ),
            "carrier Money arm serde_as: {src}"
        );
        assert!(
            src.split("enum TargetReprUnion")
                .next()
                .expect("prefix")
                .ends_with("#[::serde_with::serde_as]\n    #[derive(::serde::Deserialize)]\n    "),
            "carrier enum serde_as immediately precedes its derive: {src}"
        );
        must_parse_as_file(&src);
    }

    // SPEC §15.6 in union-arm position: an annotation matching neither a
    // standard logical type nor a registration is surfaced rather than
    // silently dropped, and names the field and record it sits in.
    #[test]
    fn unregistered_union_arm_logical_is_surfaced() {
        let err = codegen_with_logical_result(
            &[r#"{
                "type": "record",
                "name": "Target",
                "namespace": "pay",
                "fields": [
                    {"name": "value", "type": [
                        "null",
                        {"type": "string", "logicalType": "email"},
                        "int"
                    ]},
                    {"name": "extra", "type": "string"}
                ]
            }"#],
            &LogicalTypeRegistry::default(),
        )
        .expect_err("unregistered union-arm logical type must error");
        assert!(
            matches!(
                &err,
                GeneratorError::UnknownLogicalType { logical_type, field, record }
                    if logical_type == "email" && field == "value" && record == "pay.Target"
            ),
            "{err:?}"
        );
    }

    // SPEC §15.3 case 6, deliberate limit: an `array` arm keeps standard
    // lowering. The annotation sits on the items, where union-arm recovery
    // does not reach, so the arm stays `Vec<String>`.
    #[test]
    fn custom_logical_union_array_arm_keeps_standard_lowering() {
        let src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "Target",
                "namespace": "web",
                "fields": [
                    {"name": "value", "type": [
                        "null",
                        {"type": "array", "items": {"type": "string", "logicalType": "url"}},
                        "int"
                    ]},
                    {"name": "extra", "type": "string"}
                ]
            }"#],
            &registry_url(),
        );
        assert!(squashed_contains(&src, "Array(Vec<String>)"), "{src}");
        must_parse_as_file(&src);
    }

    // SPEC §15.3 case 6, deliberate limit: an arm annotation is recovered only
    // for a union sitting directly on the field. Nested in an `array` it is
    // ignored — and, unlike every other position, an unregistered one raises
    // no `UnknownLogicalType` either, since recovery reads nothing to reject.
    #[test]
    fn custom_logical_union_arm_is_recovered_only_at_field_root() {
        // Nested in an `array`: annotation ignored, registered or not.
        for registry in [registry_url(), LogicalTypeRegistry::default()] {
            let src = codegen_with_logical(
                &[r#"{
                    "type": "record", "name": "Target", "namespace": "web",
                    "fields": [
                        {"name": "value", "enum": "Payload", "type": {
                            "type": "array",
                            "items": [{"type": "string", "logicalType": "url"}, "int"]
                        }}
                    ]
                }"#],
                &registry,
            );
            assert!(
                squashed_contains(&src, r#"#[serde(rename = "string")] String(String)"#)
                    && !src.contains("::url::Url"),
                "nested arm keeps standard lowering: {src}"
            );
            must_parse_as_file(&src);
        }

        // Control: the same union on the field itself does map. `extra` keeps
        // the record off the §5.6 collapse path, which rejects an `"enum"`
        // attribute on a lone union field (§5.3).
        let root = codegen_with_logical(
            &[r#"{
                "type": "record", "name": "Target", "namespace": "web",
                "fields": [
                    {"name": "value", "enum": "Payload", "type": [
                        {"type": "string", "logicalType": "url"}, "int"
                    ]},
                    {"name": "extra", "type": "string"}
                ]
            }"#],
            &registry_url(),
        );
        assert!(
            squashed_contains(&root, "String(::url::Url)"),
            "field-root arm maps: {root}"
        );
        must_parse_as_file(&root);
    }

    // SPEC §15.3 case 6: why recovery must be keyed to the union being
    // lowered, not just to the field. The field below is
    // `[array<[int, boolean]>, null]` — two outer arms, and an inner union of
    // two arms behind the `array`. Only the *outer* `null` arm is annotated,
    // so both inner arms must keep their Avro types; the matching arity is
    // what makes reading the field's arms produce `Boolean(::url::Url)`.
    //
    // The annotation sits on the `null` arm because an annotated non-null arm
    // would be consumed by case 5 first, collapsing the field to
    // `Option<Url>` before any union is lowered.
    #[test]
    fn union_arm_logical_does_not_leak_into_a_union_nested_in_a_container() {
        let src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "Target",
                "namespace": "web",
                "fields": [
                    {"name": "value", "enum": "Payload", "type": [
                        {"type": "array", "items": ["int", "boolean"]},
                        {"type": "null", "logicalType": "url"}
                    ]},
                    {"name": "extra", "type": "string"}
                ]
            }"#],
            &registry_url(),
        );
        assert!(
            squashed_contains(&src, r#"#[serde(rename = "boolean")] Boolean(bool)"#),
            "the inner `boolean` arm must keep its own type: {src}"
        );
        assert!(
            !src.contains("::url::Url"),
            "no arm of the nested union may pick up the outer arm's mapping: {src}"
        );
        must_parse_as_file(&src);
    }

    // SPEC §15.4: a converter-backed mapping emits a struct-level
    // `#[serde_as]` and a field-level `#[serde_as(as = "Conv")]`, and
    // suppresses any bytes/fixed helper.
    #[test]
    fn custom_logical_with_converter_emits_serde_as() {
        let mut r = LogicalTypeRegistry::default();
        r.insert(
            "wgs84".to_string(),
            AvroType::Bytes,
            "::geo::Wgs84".to_string(),
            Some("crate::serde_wgs84::Wgs84".to_string()),
            None,
            false,
        );
        let src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "Position",
                "namespace": "geo",
                "fields": [
                    {"name": "loc", "type": {"type": "bytes", "logicalType": "wgs84"}}
                ]
            }"#],
            &r,
        );
        assert!(
            src.contains("#[::serde_with::serde_as]"),
            "struct serde_as: {src}"
        );
        // Exact spelling — the `as = "…"` literal must be clean (no spaces
        // around `::`), since `serde_with` parses the literal as a Rust type.
        assert!(
            src.contains(r#"#[serde_as(as = "crate::serde_wgs84::Wgs84")]"#),
            "field serde_as exact spelling: {src}"
        );
        assert!(src.contains("pub loc: ::geo::Wgs84"), "{src}");
        // The standard bytes helper must be suppressed in favour of the converter.
        assert!(
            !src.contains("serde::bytes"),
            "bytes helper suppressed: {src}"
        );
        must_parse_as_file(&src);
    }

    // SPEC §15.4 converter shaping for array: `#[serde_as(as = "Vec<Conv>")]`.
    #[test]
    fn custom_logical_converter_array_shape() {
        let mut r = LogicalTypeRegistry::default();
        r.insert(
            "wgs84".to_string(),
            AvroType::Bytes,
            "::geo::Wgs84".to_string(),
            Some("crate::serde_wgs84::Wgs84".to_string()),
            None,
            false,
        );
        let src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "Track",
                "namespace": "geo",
                "fields": [
                    {"name": "points", "type": {
                        "type": "array",
                        "items": {"type": "bytes", "logicalType": "wgs84"}
                    }}
                ]
            }"#],
            &r,
        );
        assert!(
            src.contains(r#"as = "Vec<crate::serde_wgs84::Wgs84>""#),
            "array converter exact spelling: {src}"
        );
        assert!(
            squashed_contains(&src, "pub points: Vec<::geo::Wgs84>"),
            "{src}"
        );
        must_parse_as_file(&src);
    }

    // SPEC §15.3 case 2: a *named record* carrying a top-level `logicalType`
    // is not generated; references to it become the mapped Rust type.
    #[test]
    fn custom_logical_record_is_suppressed_and_referenced_as_rust_type() {
        let mut r = LogicalTypeRegistry::default();
        r.insert(
            "wgs84".to_string(),
            AvroType::Record,
            "::sguaba::systems::Wgs84".to_string(),
            None,
            None,
            false,
        );
        let src = codegen_with_logical(
            &[
                r#"{
                    "type": "record",
                    "name": "Wgs84Rec",
                    "namespace": "geo",
                    "logicalType": "wgs84",
                    "fields": [
                        {"name": "lat", "type": "double"},
                        {"name": "lon", "type": "double"}
                    ]
                }"#,
                r#"{
                    "type": "record",
                    "name": "Waypoint",
                    "namespace": "geo",
                    "fields": [
                        {"name": "at", "type": "geo.Wgs84Rec"}
                    ]
                }"#,
            ],
            &r,
        );
        // The suppressed record's struct must not be emitted.
        assert!(
            !src.contains("pub struct Wgs84Rec"),
            "suppressed struct: {src}"
        );
        // The referencing field uses the mapped type.
        assert!(src.contains("pub at: ::sguaba::systems::Wgs84"), "{src}");
        must_parse_as_file(&src);
    }

    // A suppressed logical-type record that *nothing*
    // references must emit **no** `build_*` schema-builder — it has no Rust
    // type, so a builder would be a lone root-scope-ish symbol that collides
    // when two generated outputs sharing that FQN are `include!`d together.
    // (A *referenced* suppressed record still gets its builder — SPEC §12.1 /
    // §15.5 — which this test does not cover.)
    #[test]
    fn unreferenced_suppressed_record_emits_no_builder() {
        let mut r = LogicalTypeRegistry::default();
        r.insert(
            "money".to_string(),
            AvroType::Record,
            "::rusty_money::Money".to_string(),
            None,
            None,
            false,
        );
        // `shared.Money` is suppressed and referenced by nothing else in the
        // input: no struct, and — the regression — no builder.
        let src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "Money",
                "namespace": "shared",
                "logicalType": "money",
                "fields": [
                    {"name": "amount", "type": "long"},
                    {"name": "currency", "type": "string"}
                ]
            }"#],
            &r,
        );
        let builder =
            builder_ident(&::apache_avro::schema::Name::new("shared.Money").expect("name"))
                .expect("builder ident")
                .to_string();
        assert!(
            !squashed_contains(&src, &format!("fn {builder}")),
            "unreferenced suppressed record must not emit a builder ({builder}): {src}"
        );
        must_parse_as_file(&src);
    }

    // Last-write-wins on duplicate registration (SPEC §15.1).
    #[test]
    fn duplicate_registration_last_write_wins() {
        let mut r = LogicalTypeRegistry::default();
        r.insert(
            "u".to_string(),
            AvroType::String,
            "::a::A".to_string(),
            None,
            None,
            false,
        );
        r.insert(
            "u".to_string(),
            AvroType::String,
            "::b::B".to_string(),
            None,
            None,
            false,
        );
        let src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "R",
                "namespace": "n",
                "fields": [{"name": "f", "type": {"type": "string", "logicalType": "u"}}]
            }"#],
            &r,
        );
        assert!(src.contains("pub f: ::b::B"), "{src}");
        assert!(!src.contains("::a::A"), "{src}");
        must_parse_as_file(&src);
    }

    // SPEC §15 / §11.5: a non-empty literal default on a converterless
    // custom-logical field lowers to a helper fn that deserializes the JSON
    // default straight into the mapped `rust_type`.
    #[test]
    fn custom_logical_non_empty_default_emits_deserialize_helper() {
        let src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "L",
                "namespace": "w",
                "fields": [
                    {"name": "href", "type": {"type": "string", "logicalType": "url"},
                     "default": "http://x"}
                ]
            }"#],
            &registry_url(),
        );
        assert!(
            squashed_contains(&src, "pub(super) fn default_l_href() -> ::url::Url"),
            "{src}"
        );
        assert!(
            squashed_contains(
                &src,
                "::serde_json::from_str::<::url::Url>(\"\\\"http://x\\\"\")"
            ),
            "{src}"
        );
        assert!(
            squashed_contains(&src, "#[serde(default = \"default_l_href\")]"),
            "{src}"
        );
        must_parse_as_file(&src);
    }

    /// A converter-backed custom-logical field with a non-empty default lowers
    /// the default through `DeserializeAsWrap<FieldType, Converter>`. Mirrors
    /// the real-world `long`-backed timeout field that motivated this support.
    #[test]
    fn custom_logical_converter_default_uses_deserialize_as_wrap() {
        let mut r = LogicalTypeRegistry::default();
        r.insert(
            "millis".to_string(),
            AvroType::Long,
            "::std::time::Duration".to_string(),
            Some("crate::DurationMillis".to_string()),
            None,
            false,
        );
        let src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "Control",
                "namespace": "p",
                "fields": [
                    {"name": "timeout",
                     "type": {"type": "long", "logicalType": "millis"},
                     "default": 5000000000}
                ]
            }"#],
            &r,
        );
        assert!(
            squashed_contains(
                &src,
                "::serde_with::de::DeserializeAsWrap<::std::time::Duration, crate::DurationMillis",
            ),
            "{src}"
        );
        assert!(squashed_contains(&src, "(\"5000000000\").expect"), "{src}");
        assert!(squashed_contains(&src, ".into_inner()"), "{src}");
        must_parse_as_file(&src);
    }

    /// Container-shaped custom-logical defaults: the helper's deserialize target
    /// is the field's full container type, and a converter is shaped to match —
    /// `DeserializeAsWrap<Option<T>, Option<Conv>>` for an optional field and
    /// `DeserializeAsWrap<Vec<T>, Vec<Conv>>` for an array field.
    #[test]
    fn custom_logical_container_defaults_lower_through_container_type() {
        let mut r = LogicalTypeRegistry::default();
        r.insert(
            "email".to_string(),
            AvroType::String,
            "crate::Email".to_string(),
            Some("crate::EmailAsString".to_string()),
            None,
            false,
        );
        let src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "Contacts",
                "namespace": "c",
                "fields": [
                    {"name": "primary",
                     "type": [{"type": "string", "logicalType": "email"}, "null"],
                     "default": "a@b"},
                    {"name": "all",
                     "type": {"type": "array", "items": {"type": "string", "logicalType": "email"}},
                     "default": ["a@b"]}
                ]
            }"#],
            &r,
        );
        assert!(
            squashed_contains(
                &src,
                "fn default_contacts_primary() -> Option<crate::Email>"
            ),
            "{src}"
        );
        assert!(
            squashed_contains(
                &src,
                "DeserializeAsWrap<Option<crate::Email>, Option<crate::EmailAsString",
            ),
            "{src}"
        );
        assert!(
            squashed_contains(&src, "fn default_contacts_all() -> Vec<crate::Email>"),
            "{src}"
        );
        assert!(
            squashed_contains(
                &src,
                "DeserializeAsWrap<Vec<crate::Email>, Vec<crate::EmailAsString",
            ),
            "{src}"
        );
        must_parse_as_file(&src);
    }

    // SPEC §11.1 + §15: a custom-logical field's mapped Rust type has
    // `Eq`/`Hash` impls vanavro can't see, so the owning record must NOT get
    // the `Eq + Hash` uplift (would otherwise emit a derive that fails when
    // the mapped type isn't `Eq + Hash`). A `bytes` field would normally
    // qualify, so this isolates the override's effect.
    #[test]
    fn custom_logical_field_disqualifies_eq_hash_uplift() {
        let mut r = LogicalTypeRegistry::default();
        r.insert(
            "wgs84".to_string(),
            AvroType::Bytes,
            "::geo::Wgs84".to_string(),
            None,
            None,
            false,
        );
        let src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "Position",
                "namespace": "geo",
                "fields": [{"name": "loc", "type": {"type": "bytes", "logicalType": "wgs84"}}]
            }"#],
            &r,
        );
        // The struct derive must not include the `Eq, Hash` uplift. (Note
        // `PartialEq` always present — match the uplift's exact `Hash` token,
        // which only the uplift introduces.)
        let derive_block = src
            .split("pub struct Position")
            .next()
            .expect("text before struct");
        assert!(
            !derive_block.contains("Hash"),
            "custom-logical field must suppress Eq/Hash uplift: {src}"
        );
        // Sanity: without the override, the same bytes record WOULD qualify.
        let plain = codegen_from(&[r#"{
            "type": "record",
            "name": "Position",
            "namespace": "geo",
            "fields": [{"name": "loc", "type": "bytes"}]
        }"#]);
        let plain_derive = plain
            .split("pub struct Position")
            .next()
            .expect("text before struct");
        assert!(
            plain_derive.contains("Hash"),
            "plain bytes record should qualify for Eq/Hash: {plain}"
        );
        must_parse_as_file(&src);
    }

    // SPEC §11.1 + §15: `force_eq_hash` makes a bare custom-logical field
    // (whose `bytes` base would normally qualify) keep the uplift instead of
    // disqualifying the record.
    #[test]
    fn custom_logical_force_eq_hash_restores_uplift() {
        let mut r = LogicalTypeRegistry::default();
        r.insert(
            "wgs84".to_string(),
            AvroType::Bytes,
            "::geo::Wgs84".to_string(),
            None,
            None,
            true,
        );
        let src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "Position",
                "namespace": "geo",
                "fields": [{"name": "loc", "type": {"type": "bytes", "logicalType": "wgs84"}}]
            }"#],
            &r,
        );
        assert!(
            has_eq_hash_uplift(&src),
            "force_eq_hash must restore the uplift: {src}"
        );
        must_parse_as_file(&src);
    }

    /// `force_eq_hash` propagates through `array` (→ `Vec<RustTy>`, which is
    /// `Eq + Hash` iff `RustTy` is) and `["null", T]` (→ `Option<RustTy>`).
    #[test]
    fn custom_logical_force_eq_hash_array_and_optional_uplift() {
        let mut r = LogicalTypeRegistry::default();
        r.insert(
            "url".to_string(),
            AvroType::String,
            "::url::Url".to_string(),
            None,
            None,
            true,
        );
        let array_src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "Links",
                "namespace": "web",
                "fields": [{"name": "hrefs",
                    "type": {"type": "array",
                        "items": {"type": "string", "logicalType": "url"}}}]
            }"#],
            &r,
        );
        assert!(
            has_eq_hash_uplift(&array_src),
            "array of forced custom-logical must uplift: {array_src}"
        );
        must_parse_as_file(&array_src);
        let opt_src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "MaybeLink",
                "namespace": "web",
                "fields": [{"name": "href",
                    "type": ["null", {"type": "string", "logicalType": "url"}]}]
            }"#],
            &r,
        );
        assert!(
            has_eq_hash_uplift(&opt_src),
            "optional forced custom-logical must uplift: {opt_src}"
        );
        must_parse_as_file(&opt_src);
    }

    /// `force_eq_hash` on a `map` *value* is a contradiction (the field is a
    /// `HashMap`, which is never `Hash`), so codegen rejects it with
    /// [`GeneratorError::ForceEqHashOnMap`] rather than silently dropping the
    /// uplift or emitting an invalid `derive(Hash)`.
    #[test]
    fn custom_logical_force_eq_hash_map_value_errors() {
        let mut r = LogicalTypeRegistry::default();
        r.insert(
            "url".to_string(),
            AvroType::String,
            "::url::Url".to_string(),
            None,
            None,
            true,
        );
        let err = codegen_with_logical_result(
            &[r#"{
                "type": "record",
                "name": "Holder",
                "namespace": "m",
                "fields": [{"name": "vals",
                    "type": {"type": "map",
                        "values": {"type": "string", "logicalType": "url"}}}]
            }"#],
            &r,
        )
        .expect_err("force_eq_hash on a map value must error");
        match err {
            GeneratorError::ForceEqHashOnMap {
                logical_type,
                field,
                record,
            } => {
                assert_eq!(logical_type, "url");
                assert_eq!(field, "vals");
                assert_eq!(record, "m.Holder");
            }
            other => panic!("expected ForceEqHashOnMap, got {other:?}"),
        }
    }

    /// A map value whose mapping does *not* force the uplift simply blocks it
    /// (no error) — the contradiction only arises when `force_eq_hash` is set.
    #[test]
    fn custom_logical_unforced_map_value_blocks_without_error() {
        let mut r = LogicalTypeRegistry::default();
        r.insert(
            "url".to_string(),
            AvroType::String,
            "::url::Url".to_string(),
            None,
            None,
            false,
        );
        let src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "Holder",
                "namespace": "m",
                "fields": [{"name": "vals",
                    "type": {"type": "map",
                        "values": {"type": "string", "logicalType": "url"}}}]
            }"#],
            &r,
        );
        assert!(
            !has_eq_hash_uplift(&src),
            "unforced map value must block the uplift: {src}"
        );
        must_parse_as_file(&src);
    }

    /// `force_eq_hash` only vouches for its own field — a sibling that is
    /// independently not `Eq + Hash` (a `double`) still disqualifies the record.
    #[test]
    fn custom_logical_force_eq_hash_with_disqualifying_sibling_blocks() {
        let mut r = LogicalTypeRegistry::default();
        r.insert(
            "url".to_string(),
            AvroType::String,
            "::url::Url".to_string(),
            None,
            None,
            true,
        );
        let src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "Mixed",
                "namespace": "m",
                "fields": [
                    {"name": "href", "type": {"type": "string", "logicalType": "url"}},
                    {"name": "ratio", "type": "double"}
                ]
            }"#],
            &r,
        );
        assert!(
            !has_eq_hash_uplift(&src),
            "a non-Eq/Hash sibling must still block the uplift: {src}"
        );
        must_parse_as_file(&src);
    }

    /// A `null` default on an optional custom-logical field rides
    /// `#[serde(default)]` (→ `None`) and compiles cleanly.
    #[test]
    fn custom_logical_optional_null_default_is_serde_default() {
        let src = codegen_with_logical(
            &[r#"{
                "type": "record",
                "name": "MaybeLink",
                "namespace": "web",
                "fields": [
                    {"name": "href", "type": [
                        "null",
                        {"type": "string", "logicalType": "url"}
                    ], "default": null}
                ]
            }"#],
            &registry_url(),
        );
        assert!(
            squashed_contains(&src, "pub href: Option<::url::Url>"),
            "{src}"
        );
        assert!(src.contains("#[serde(default)]"), "{src}");
        must_parse_as_file(&src);
    }
}
