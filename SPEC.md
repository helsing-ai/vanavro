# Specification for VanAvro Rewrite (SPEC.md)

This document provides a comprehensive technical specification to guide the rewrite of `vanavro` (the Apache Avro-to-Rust code generator). It details the core mechanics of the crate, identifies critical architecture flaws and dead decisions to be avoided in the rewrite, and defines the structural, type-mapping, and testing requirements necessary to deliver a highly robust, performance-focused, and correct code generator.

---

## 1. Core Purpose & Features

`vanavro` is a build-time and CLI utility designed to generate type-safe, idiomatic Rust struct and enum definitions from Apache Avro schemas (`.avsc` JSON and `.avdl` IDL protocols).

The generated Rust types use the `apache-avro` crate's `serde` APIs for seamless, low-overhead binary serialization and deserialization.

### Key Goals for the Rewrite

- **Schema Correctness:** The primary focus is generating flawless, standard-compliant schema representations statically associated with each type.
- **Serde Integration:** Leverage standard `serde::Serialize` and `serde::Deserialize` derives along with `apache-avro`'s serialization helpers.
- **Custom Logical Types:** Allow callers to map arbitrary Avro `logicalType` annotations onto user-supplied Rust types (e.g. `::url::Url`, `::sguaba::systems::Wgs84`, `::serde_json::Value`), optionally driven by a `serde_with` converter. See **Section 15**.
- **Robustness & Predictability:** Eliminate fragile runtime parsing of individual types' schema strings, replace fragile filesystem operations, and properly resolve recursive/self-referential types.

---

## 2. Current Public Interfaces

This section defines the CLI and builder API currently supported by the rewrite. The crate starts at version `0.1.0`; no source-compatibility guarantee is made between pre-`1.0` releases, and the API may evolve before `1.0`.

### 2.1. CLI Interface

The CLI tool uses `clap` to process input schemas and protocols. Its current arguments and options are:

| Flag / Option                        | Description                                                                                                                                                                                                                                                        |
| :----------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--schemas <FILE...>`                | Input Avro schema files (`.avsc` JSON format). Can accept multiple files. Grouped with `--idl` as mutually exclusive.                                                                                                                                              |
| `--idl <FILE>`                       | Input Avro IDL file (`.avdl` format). Mutually exclusive with `--schemas`.                                                                                                                                                                                         |
| `-o`, `--output <FILE>`              | Output file path (defaults to writing to `stdout` if not specified).                                                                                                                                                                                               |
| `--builder`                          | Derive `bon::Builder` for generated record types to enable ergonomic construction.                                                                                                                                                                                 |
| `--no-non-exhaustive`                | Disable adding `#[non_exhaustive]` to generated enums. (By default, enums should be generated with `#[non_exhaustive]` to allow backward-compatible changes).                                                                                                      |
| `--no-collapse-single-union`         | Disable the single-union-record collapse of **Section 5.6**. (By default such records collapse to a bare enum; this keeps them as a struct with a sibling `{Record}Union` enum).                                                                                   |
| `--allow-unregistered-logical-types` | Tolerate a `logicalType` that matches no standard logical type and no registered custom mapping: ignore the annotation and lower the field to its base type instead of raising `UnknownLogicalType` (**Section 15.6**). By default such an annotation is an error. |

The legacy `--split-schemas` flag is deliberately **not** provided: its `include_str!`-based implementation is broken (see **Section 4.2**) and output is always a single file.

### 2.2. Builder Interface

The programmatic builder pattern allows integration in `build.rs` scripts. `VanAvroBuilder` is not hand-written: it is generated by `#[derive(bon::Builder)]` on `VanAvro`, plus two extension setters that accumulate repeatable inputs:

```rust
#[derive(bon::Builder)]
pub struct VanAvro {
    #[builder(field)] schema_jsons: Vec<String>,          // via `.schema_json(..)` (repeatable)
    #[builder(field)] logical_types: LogicalTypeRegistry, // via `.with_logical_type(..)` (§15)
    #[builder(into)]  idl_path: Option<PathBuf>,          // `.idl_path(..)` / `.maybe_idl_path(..)`
    #[builder(default)]        builder: bool,
    #[builder(default = true)] non_exhaustive: bool,
    #[builder(default = true)] collapse_single_union: bool,
    #[builder(default)]        allow_unregistered_logical_types: bool,
}

impl<S: van_avro_builder::State> VanAvroBuilder<S> {
    pub fn schema_json(self, schema: impl Into<String>) -> Self;
    pub fn with_logical_type(self, logical_type: CustomLogicalType) -> Self;
    // plus the bon-generated setters: idl_path / maybe_idl_path, builder,
    // non_exhaustive, collapse_single_union, allow_unregistered_logical_types
}

impl<S: van_avro_builder::IsComplete> VanAvroBuilder<S> {
    pub fn build(self) -> VanAvro;
    pub fn generate(self) -> Result<String, GeneratorError>;
    pub fn generate_to_writer<W: Write>(self, writer: &mut W) -> Result<(), GeneratorError>;
    pub fn generate_to_path(self, path: impl AsRef<Path>) -> Result<(), GeneratorError>;
}

impl VanAvro {
    pub fn generate(&self) -> Result<String, GeneratorError>;
    pub fn generate_to_writer<W: Write>(&self, writer: &mut W) -> Result<(), GeneratorError>;
    pub fn generate_to_path(&self, path: impl AsRef<Path>) -> Result<(), GeneratorError>;
}
```

See **Section 15** for the custom-logical-type registration semantics.

The legacy `register_logical_type` and `register_logical_type_with` convenience methods are not part of the current builder API. Register mappings with `.with_logical_type(CustomLogicalType::builder(...).build())` instead; compatibility wrappers for earlier API drafts are not required before `1.0`.

`CustomLogicalType` is a `bon`-built value carrying the `logicalType` name, the
decorated `AvroType`, the mapped `rust_type`, an optional `converter`, an optional
`builder_with` (`bon` setter customization, §15.9), and an optional
`force_eq_hash` override (§15.1).

---

## 3. Core Avro to Rust Type Mappings

All standard Avro types must be systematically mapped to idiomatic Rust types that align with `apache-avro`'s serde format requirements.

### 3.1. Primitive Types

| Avro Type | Rust Type | Notes / Serde Attributes                                       |
| :-------- | :-------- | :------------------------------------------------------------- |
| `null`    | `()`      | Represented directly or as `Option` wrapper                    |
| `boolean` | `bool`    |                                                                |
| `int`     | `i32`     |                                                                |
| `long`    | `i64`     |                                                                |
| `float`   | `f32`     |                                                                |
| `double`  | `f64`     |                                                                |
| `bytes`   | `Vec<u8>` | **Must** have `#[serde(with = "::apache_avro::serde::bytes")]` |
| `string`  | `String`  |                                                                |

### 3.2. Complex & Container Types

| Avro Type            | Rust Type             | Notes / Serde Attributes / Structure                                                                                                              |
| :------------------- | :-------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------ |
| `array`              | `Vec<T>`              |                                                                                                                                                   |
| `map`                | `HashMap<String, T>`  | Key is always `String`                                                                                                                            |
| `fixed`              | `[u8; N]`             | Generates a type alias: `pub type TypeName = [u8; N];`. Fields referencing it **must** derive `#[serde(with = "::apache_avro::serde::fixed")]`.   |
| `record`             | `struct`              | Generates a Rust struct.                                                                                                                          |
| `enum`               | `enum`                | Generates a unit-variant Rust enum (unless non-unit union helper). Enums should derive `Copy`, `Hash`, `Ord`, `Eq`, and `PartialOrd`.             |
| `union` (optional)   | `Option<T>`           | When union is exactly `["null", T]` or `[T, "null"]`. If `bytes` or `fixed`, must use helper attributes `serde::bytes_opt` or `serde::fixed_opt`. |
| `union` (multi-type) | Generated custom enum | For unions with $>2$ variants, or non-nullable unions. (See **Section 5.3** for layout).                                                          |

### 3.3. Standard Logical Types

The rewrite will support standard Avro logical types natively using standard Rust types compatible with `apache-avro`'s parser:

| Avro Logical Type        | Underlying Avro Type | Generated Rust Type                                              |
| :----------------------- | :------------------- | :--------------------------------------------------------------- |
| `uuid`                   | `string`             | `::uuid::Uuid` (requires `uuid` dependency with `serde` feature) |
| `date`                   | `int`                | `i32` (days since Unix epoch)                                    |
| `time-millis`            | `int`                | `i32` (ms since midnight)                                        |
| `time-micros`            | `long`               | `i64` (μs since midnight)                                        |
| `timestamp-millis`       | `long`               | `i64` (ms since Unix epoch)                                      |
| `timestamp-micros`       | `long`               | `i64` (μs since Unix epoch)                                      |
| `timestamp-nanos`        | `long`               | `i64` (ns since Unix epoch)                                      |
| `local-timestamp-millis` | `long`               | `i64`                                                            |
| `local-timestamp-micros` | `long`               | `i64`                                                            |
| `local-timestamp-nanos`  | `long`               | `i64`                                                            |
| `decimal`                | `bytes` / `fixed`    | `::apache_avro::Decimal`                                         |
| `duration`               | `fixed` (size 12)    | `::apache_avro::Duration`                                        |

The `uuid` row lists the canonical underlying type. Codegen maps every `apache_avro::Schema::Uuid` to `::uuid::Uuid` regardless of its backing type, so a `uuid` on `fixed` (size 16) or `bytes` that `apache-avro` accepts also lowers to `::uuid::Uuid`; only the `string`-backed form is covered by the round-trip tests.

**`duration` inside an enum-generating union (v1 limitation).** A `duration` arm is supported in an optional union (`["null", duration]` → `Option<::apache_avro::Duration>`), but any enum-generating union (§5.3) with a `duration` arm raises `UnsupportedSchema`. `apache_avro::Schema::Duration` discards the name of the backing `fixed`, and that name is the arm's serde rename tag, so codegen cannot reconstruct the tag. Rejecting the schema is preferable to emitting a wrong tag that fails at runtime. Workaround: wrap the `duration` in a named record and use the record as the union arm.

### 3.4. Required Serde Helpers for Bytes & Fixed Fields

Under standard Rust/Serde setups, `Vec<u8>` or array types are serialized as sequences of individual numbers (e.g., JSON arrays). In Avro's binary protocol, bytes and fixed-size types are encoded directly as single compact byte arrays.
Therefore, the rewrite **must** decorate these types with `apache-avro`'s custom serde modules using the `#[serde(with = "...")]` helper attributes:

- **`bytes` fields (e.g. `Vec<u8>`):** Use `#[serde(with = "::apache_avro::serde::bytes")]`
- **Optional `bytes` fields (e.g. `Option<Vec<u8>>`):** Use `#[serde(with = "::apache_avro::serde::bytes_opt")]`
- **`fixed` fields (e.g. array type aliased as `[u8; N]`):** Use `#[serde(with = "::apache_avro::serde::fixed")]`
- **Optional `fixed` fields (e.g. `Option<[u8; N]>`):** Use `#[serde(with = "::apache_avro::serde::fixed_opt")]`

> _Upstream note (`apache-avro` 0.22):_ these helpers moved from the crate-root `serde_avro_*` functions to the `::apache_avro::serde::{bytes, bytes_opt, fixed, fixed_opt}` modules. The old `serde_avro_bytes` / `serde_avro_fixed` paths no longer exist.

---

## 4. Post-Mortem: Wrong and Dead Decisions to Avoid

Several fundamental design mistakes in the original codebase caused extreme fragility, compilation failures, or bloated code. These **must** be designed out of the rewrite.

### 4.1. Runtime Parsing of Standalone Type Schemas (CRITICAL FLAW)

- **The Original Flaw:** Each generated struct or enum carried a static `LazyLock<Schema>` that parsed its own inlined JSON schema string on first access using `Schema::parse_str(&schema_json)`.
- **Why this is wrong:**
  1. **Fragility on Cross-References:** If Record `A` references Record `B` from a different namespace, parsing `A`'s schema in isolation will **fail** because `B`'s definition is not part of `A`'s JSON block. To work around this, the original crate tried to "fully inline" all nested references into the JSON.
  2. **Inlining Bloat:** Fully inlining dependent types results in massive, duplicate JSON strings embedded inside multiple static definitions, severely bloating the final compiled binary.
  3. **Recursive Failure:** For recursive or mutually recursive types (e.g. `Node` referencing itself in `next: Option<Box<Node>>`), fully inlining is mathematically impossible and causes infinite loops during code generation unless truncated. If truncated to a string name like `"Node"`, the standalone parser fails at runtime because the context containing `"Node"`'s definition is missing.
  4. **Performance Overhead:** Parsing large string-based schemas at runtime incurs parsing overhead and risks deferred panics at runtime.

- **The Correct Architectural Solution:**
  - **Option A: Static/Structural Schema Construction (Recommended):** Instead of string parsing at runtime, construct the `Schema` AST structurally using Rust code constructors at compile-time/runtime inside the `LazyLock`. Since `apache_avro::Schema` is a public enum, we can generate the nesting structurally (e.g., `Schema::Record(RecordSchema { ... })`). This is 100% correct, type-safe, has zero parsing overhead, and handles recursion trivially without string refs.
  - **Option B: Shared Central Parsing Registry:** Alternatively, generate a single shared static array or table containing **all** parsed schemas. Use `Schema::parse_list` to parse all schemas together in a single, topologically sorted batch. Then, individual type definitions index or look up their specific `Schema` reference from this central, valid registry.

- **Implementation note:** Option A is implemented, and remains **structural**
  (`schema()` never parses a string at runtime). It is factored into one shared
  builder fn per named type to keep generated output O(N) rather than O(N²);
  that factoring — and the symbol-collision constraint it implies — is specified
  normatively in [§12.1](#121-on-shared-builder-factoring).

### 4.2. Broken `split_schemas` with `include_str!` Pathing (CRITICAL FLAW)

- **The Original Flaw:** The `--split-schemas` option wrote `.avsc` files next to the generated Rust file in `OUT_DIR` and loaded them using `include_str!("TypeName.avsc")`.
- **Why this is wrong:** In Rust, `include_str!` resolves paths relative to the **using source file**, not the file containing the `include!` macro.
  In 99% of `build.rs` integrations, code is generated in `OUT_DIR` and included via `include!(concat!(env!("OUT_DIR"), "/generated.rs"))` in `src/lib.rs` or `src/main.rs`.
  Thus, when the compiler processes `src/lib.rs`, the `include_str!("TypeName.avsc")` inside the generated code will look for `src/TypeName.avsc`, NOT `OUT_DIR/TypeName.avsc`. This causes immediate compile-time errors.
- **The Correct Architectural Solution:** Do not use relative `include_str!` inside generated code.
  - If schema isolation is needed, embed schemas as clean inline string literals inside the Rust file.
  - Or, generate absolute paths for `include_str!` at build-time using `env!("OUT_DIR")` dynamically (e.g. `include_str!(concat!(env!("OUT_DIR"), "/TypeName.avsc"))`). This ensures perfect compilation regardless of where the code is included.

### 4.3. Complex Out-of-Band Raw JSON Traversals (CONSTRAIN, DON'T DELETE)

- **The Constraint:** `apache-avro` discards unknown `logicalType` annotations on primitive types when it parses a schema. Because custom logical types are a supported feature (§15), the parser **must** retain the raw `serde_json::Value` of every input alongside the parsed `Schema` so it can recover those annotations (`get_field_logical_type`, `get_record_logical_type`, `get_array_item_logical_type`, `get_map_value_logical_type`, `get_union_variant_logical_type`, `get_union_arm_logical_type`).
- **Why this was historically fragile:** The original traversal reinvented scope-aware lookup, namespace inheritance, and type-reference resolution in an ad-hoc way, duplicating logic across many helpers.
- **The Correct Architectural Solution:** Keep the raw-JSON lookup **narrowly scoped to logical-type recovery only**. All other code generation (type mapping, references, recursion, schema construction) relies strictly on `apache-avro`'s parsed `Schema` AST. The raw-JSON helpers are the single, contained mechanism for the one thing the AST cannot express (custom `logicalType` on primitives), and they must share one reference-resolution routine rather than re-implementing it per call site.

---

## 5. Schema Correctness & Generation Guidelines

To guarantee correctness, the code generator must satisfy the following constraints.

### 5.1. Preservation of Original Schema Attributes

- **Defaults:** Default values on fields must be correctly parsed and utilized. For optional fields (`["null", T]`), `null` default maps to `None`. For primitives, they must map to their respective Rust default expressions.
- **Documentation:** Field and record `doc` attributes must be extracted. Multi-line doc strings must be parsed and generated as `#[doc = " ... "]` (outer doc attributes) so that tools like `prettyplease` and `rustdoc` render them correctly as triple-slash (`///`) doc comments.
- **Preserving Field Attributes:** `RecordField.order` and `RecordField.position` are absent from the `RecordField` API in `apache-avro` 0.22 (dropped upstream after 0.21.0), so they are not exposed by the parsed `Schema`. The generator therefore no longer emits field `order` (the former `emit_record_field_order`) or a `position` literal — there is nothing upstream to read them from. Revisit if a future upstream restores these fields.

### 5.2. Escaping Rust Keywords and Preserving Serde Renaming

Rust strict keywords (`type`, `match`, `fn`, `loop`, etc.) and special self-referential terms (`self`, `Self`, `super`, `crate`) cannot be used as identifiers directly.

1. **Escaping:**
   - Strict keywords must be escaped with the raw identifier prefix: `r#keyword` (e.g., `r#type`).
   - Special self keywords must be appended with an underscore suffix: `self_`, `super_`, etc.
2. **Serde Rename:** For any escaped field or variant, a `#[serde(rename = "original_name")]` attribute **must** be generated to ensure the schema serializer uses the exact original field name expected by Avro.

#### Edge cases

- **Degenerate `heck` conversions** _(implemented — `sanitize_ident_candidate`)_: Avro's name regex (`[A-Za-z_][A-Za-z0-9_]*`) admits names that `heck` case-conversion turns into strings `Ident::new` rejects. Two shapes are repaired before token emission:
  - **All-underscore names** (`_`, `__`, …, all legal as names, fields, and enum symbols). `heck` strips leading/trailing/duplicate underscores, collapsing these to the empty string, which panics `Ident::new`; a bare `_` is also a Rust wildcard rather than a binding in struct-field, enum-variant, and module positions. The generator appends `_` to the **original** name (the same trailing-underscore escape the self-family keywords use): `_` → `__`, `__` → `___`, etc.
  - **Leading-digit names** (`_1`, `_42x`, …). `heck` strips the leading underscore, leaving a digit-initial string (`1`, `42x`); `Ident::new("1")` panics (`Ident cannot be a number`). The generator prepends a single `_`: `_1` → `_1`, `_42x` → `_42x`.

  In both shapes each repaired result is a valid non-wildcard identifier and carries a `#[serde(rename = "…")]` back to the original wire name whenever it differs — except for a `fixed` name, which lowers to a `pub type` alias (§11.1) that cannot carry serde attributes. That is harmless: a fixed's name never appears in the binary encoding, and the original name survives in the structural `schema()` (§5.5). The repair covers every name admitted by Avro's **default** name validator; it does not handle non-ASCII identifiers reachable only by overriding the validator (`apache_avro::validator::set_*_validator`), which remain out of scope for v1. Repaired names that fold together (e.g. `_1_` and `__1__` both reduce to `_1`) are not silently merged — `check_name_collisions` reports them as a clean `NameCollision` (§8.1) rather than panicking.

- **Post-conversion name collisions** _(implemented — `check_name_collisions`)_: case conversion (PascalCase / snake_case) and the self-family suffix can fold distinct Avro names onto the same Rust identifier. A uniqueness pass runs before token emission and raises `NameCollision` (§8.1) instead of emitting unparseable Rust. Detected scopes:
  - Fields `firstName` and `first_name` both snake-case to `first_name` (per record).
  - Types `my_type` and `MyType` both pascal-case to `MyType` (per namespace).
  - Namespace segments `OrgX` and `org_x` both snake-case to `org_x`, which would produce duplicate `pub mod org_x { … }` declarations (per namespace-sibling group).
  - Fields `self` and `self_` both become `self_` under the self-family escape (per record).
  - Enum symbols `LOW` and `Low` both pascal-case to `Low` (per enum).
  - A type name equal to a **child module ident** of the same namespace: record `a._1` beside namespace `a._1`, both `_1`, which Rust rejects as a redefinition (`E0428`) since types and modules share one namespace (per namespace). PascalCase-vs-snake_case normally separates the two, but not for the case-invariant repairs above, nor for a verbatim `"enum"` union name (§5.3), which is not PascalCased.
    Multi-variant union variant-name collisions are exempt: they self-heal via the indexed `Variant{N}` fallback (§5.3) rather than erroring.

Names are not checked against the Rust prelude. Since the generator emits `String`, `Vec`, `Option` and `Box` unqualified, a type named after one of them shadows the prelude in that module and the output will not compile.

### 5.3. Multi-type (Non-Optional) Union Layouts

A union generates a custom enum unless it is an **optional union** — exactly a 2-variant union of `null` and one other type (`[null, T]` or `[T, null]`), which lowers to `Option<T>` instead. Every other union is enum-generating: single variant, two variants that don't include `null`, or three or more variants.

#### Naming the extracted enum

The enum's name is, in order of precedence:

1. **The `"enum"` custom attribute on the record field**, used **verbatim** — keyword-escaped (§5.2) but **not** PascalCased, and never repaired into a legal identifier. The value must therefore already match `[A-Za-z_][A-Za-z0-9_]*`; `"my-union!"`, `"1Union"`, `""`, a non-string (`"enum": 5`), and the bare `"_"` (a Rust wildcard, so `pub enum _` cannot parse — `"__"` is fine) are all rejected with `UnsupportedSchema`.
   Validation checks identifier grammar only. A name that is not `UpperCamelCase` warns under `non_camel_case_types` in the consuming crate, and one that shadows a prelude type — `String`, `Vec`, `Option`, `Box` — will not compile (§5.2).
2. **Otherwise** the default `[ParentRecord]Union`, PascalCased — available only at a **field-root** position.

A name propagates **inward** from the field: `array` / `map` pass it into their `items` / `values` at any depth, and optional `[null, T]` unions pass it into `T`. Only the optional wrapper also passes on the _field-root_ status, so a union under a collection has no default and fails with `UnsupportedSchema` unless the field names it.

#### Attribute placement

The `"enum"` attribute is legal on a **record field object only**:

```json
{
  "name": "f",
  "enum": "MyUnion",
  "type": { "type": "array", "items": ["int", "string"] }
}
```

A bare Avro union is a JSON array with nowhere to carry an attribute of its own, so the name rides the field. In Avro IDL that is the post-type annotation `union { int, string } @enum("MyUnion") f;`; written _before_ the type it binds to the type and is discarded by the IDL compiler.

Three placements are rejected with `UnsupportedSchema`:

- **On an `array` / `map` schema object**, reached directly or through a union arm. The check tests for the key's **presence**, so it fires regardless of the value's type.
- **On a field that encloses no enum-generating union** — a plain scalar, an array of a primitive, an optional primitive like `["null", "int"]`. There is nothing to name.
- **On the lone union field of a §5.6-collapsing record**, where the collapse already names the enum after the record. For a valid name the diagnostic offers both remedies — drop the attribute and take `[Record]`, or disable the collapse and get `[Name]`; an invalid value is reported as invalid instead, since the second remedy would promise a name that cannot be emitted.

Three placements are accepted with no diagnostic:

1. **On a `record` / `enum` / `fixed` schema object** — no naming hint can live there, so the key is ordinary user metadata: neither diagnosed nor stripped from `schema()`.
2. **On a field carrying a custom logical-type override (§15.3)**, whose mapped type replaces the field's whole lowering, so the value is never read. Still stripped from `schema()`, which filters on the key alone.
3. **Anywhere inside a §15.3-suppressed record**, which is never lowered.

#### One enum-generating union per record field

A record field's type tree must contain **at most one** enum-generating union — the limit that gives the field's `"enum"` name exactly one destination. More is rejected with `UnsupportedSchema`, naming the record, the field and the count. Optional unions, arrays and maps are transparent and cost nothing; counting stops at a `record` / `enum` / `fixed` / type reference, so a union behind a named type (inline §10.3 declarations included) is budgeted against _that_ type's field.

| Field type                                                 | Count | Outcome                                                             |
| :--------------------------------------------------------- | :---- | :------------------------------------------------------------------ |
| `["int", "string"]`                                        | 1     | field-root — defaults to `[ParentRecord]Union` (or collapses, §5.6) |
| `["null", "string"]`                                       | 0     | `Option<String>`, no enum emitted                                   |
| `["null", {"type": "array", "items": ["null", "string"]}]` | 0     | `Option<Vec<Option<String>>>`, no enum emitted                      |
| `{"type": "array", "items": ["int", "string"]}`            | 1     | within budget, but not field-root — needs an `"enum"` on the field  |
| `["int", {"type": "array", "items": ["long", "bytes"]}]`   | 2     | **rejected** — over budget                                          |
| `["int", "Inner"]`, where `Inner` has a union field        | 1     | accepted — `Inner`'s union is budgeted against `Inner`              |

The remedies for an over-budget field are to reduce one union to the optional `[null, T]` shape, or to move the inner one behind a named record and reference it.

#### Validation and diagnostics

The two **structural** rules — the one-union budget and the `array` / `map` placement restriction — are enforced by a pre-pass over every named record's field, ahead of all lowering, so neither can be bypassed by a custom logical-type override, by §15.3 record suppression, or by an inline declaration. Every other §5.3 diagnostic is raised while a field is lowered, and so does not run for a field the generator never lowers (accepted cases 2 and 3 above). Only the first violation is reported.

Every diagnostic about a union names the owning **record and field**, plus the innermost `array` / `map` for a union reached through a collection, and carries that same origin into the §5.2 collision report so both sides of a `NameCollision` are distinguishable. A diagnostic about the attribute's _value_ quotes the value and adds no location, except on the §5.6-collapse path, where it is prefixed with the record and field.

#### Schema preservation

The `"enum"` key is a vanavro-internal hint, so it is stripped from the **field entry** of the structural `schema()` ctor (§5.5, §12) and never reaches the wire schema. Stripping is scoped to that one site; on any other schema object the key is user metadata and is reproduced verbatim. Fingerprints are unaffected either way — the Parsing Canonical Form drops unknown attributes.

#### Name collisions across unions

Every extracted union enum is registered in the namespace-wide collision pass (§5.2, `check_name_collisions`). Two unions resolving to the same name in one namespace — or a union colliding with a record / enum / fixed there — fail with `NameCollision` (§8.1) rather than emitting duplicate `pub enum` definitions. In particular, two **unnamed** enum-generating union fields in one record both fold onto `[ParentRecord]Union`; the remedy is an explicit `"enum"` attribute on one of the fields.

#### Other requirements

- Each extracted enum must derive `Serialize` and `Deserialize`.
- Each variant must map to one of the union's types.
- **Crucial for Serde Integration:** Each variant must carry `#[serde(rename = "avro_type_name")]` matching Avro's union tag conventions (e.g. `#[serde(rename = "int")]` or `#[serde(rename = "com.example.NestedRecord")]`).
- Uniqueness: when two variants produce the same candidate Rust ident (e.g. two named records `com.a.Foo` and `com.b.Foo` from different namespaces — Avro permits this), fall back to indexed `Variant0`, `Variant1`, … for **every** variant in that union so the naming stays consistent.

### 5.4. Recursive / Self-Referential Types

Self-referential or mutually recursive schemas must be supported safely:

- Identify cycles in types (e.g. record `Node` having a field with type `Node` or a union containing `Node`).
- Break cycles in the generated types by boxing the inner type: `Option<Box<Node>>`.
- The schema definition must resolve the self-reference as a symbol string (e.g., `"Node"`) in its structural layout without causing infinite loops.
  - _v1 limitation:_ the cycle-breaking pass boxes **every** record reference that participates in a cycle through inline positions (direct or `Option`), not the minimal feedback-arc set. A single `Box` per cycle would suffice for type-checking, but computing the minimum feedback arc set is NP-hard; we accept one extra `Box` per cycle edge in exchange for a linear-time pass. Cycles routed through `Vec` / `HashMap` are left alone since those types already heap-allocate.

### 5.5. Schema Fingerprinting & Single-Object Encoding

For integration with message queues (e.g., Kafka) and RPC systems, support for Schema Fingerprints and Single-Object Encoding is essential:

1. **Fingerprint Method:** Every generated record struct and enum must expose a generic `fingerprint` method:

   ```rust
   pub fn fingerprint<D: ::digest::Digest>() -> ::apache_avro::schema::SchemaFingerprint {
       Self::schema().fingerprint::<D>()
   }
   ```

2. **Single-Object Encoding Compatibility:** Single-Object Encoding embeds a 2-byte marker (`[0xC1, 0x01]`) followed by an 8-byte Rabin fingerprint of the schema (little-endian), followed by the binary Avro representation of the record. The generated schemas must be perfectly correct so that the calculated fingerprints match expected values exactly during cross-language or inter-platform verification.

### 5.6. Single-Union-Record Collapse

A record with exactly one field, where that field is an enum-generating (non-optional) union, collapses into a single public `enum` named after the record itself (**`[Record]`**), with one variant per union arm. There is no wrapping struct.

This collapse is enabled by default but is caller-toggled via `collapse_single_union` (builder default `true`; CLI `--no-collapse-single-union`). When disabled, such a record is generated as an ordinary struct over its single field, and its enum-generating union lowers to the sibling `[Record]Union` enum exactly as in §5.3 — no collapse, no synthetic carriers. References to the record (including recursive self-references) resolve to `[Record]` in either mode, so callers of the referencing type are unaffected by the toggle.

```rust
// record Scalar { choice: ["int","string","boolean","null"] }
pub enum Scalar { Int(i32), String(String), Boolean(bool), Null(()) }
```

- **Scope:** Applies only when the record has exactly one field and that field is an enum-generating union. Does not apply to:
  - Optional `[null, T]` single-field records (stays a struct over `Option<T>` with fluent constructors).
  - A single field with a custom logical-type override (§15) (field type becomes the opaque mapped type).

  A field-level `"enum"` attribute (§5.3) on the lone union field is a **conflict**, not an override: the collapse already names the enum after the record, so generation fails with `UnsupportedSchema`. The check fires only when the collapse actually happens — not under either exemption above, nor when `collapse_single_union` is off — and only for a value that is a legal name (§5.3); an invalid one is reported as an invalid name instead.

- **Variant naming:** Reuses §5.3's variant-naming and collision rules — one variant per union arm, with the indexed `Variant{N}` fallback applied to every arm when two would otherwise collide — so the collapsed enum, its private carriers, and the serde rename tags stay consistent with the non-collapsed `[Record]Union` form.
- **Serialization:** Emitted via a manual, borrowing `serde::Serialize` that drives `serialize_struct(.., 1)` + `serialize_field(<field>, …)` from `&self`. The field value is a nested `Arm<'a>(&'a [Record])` adapter whose own `Serialize` calls `serialize_newtype_variant(<record>, <index>, <tag>, <payload>)` per arm, borrowing the payload in place to avoid cloning heap payloads. This reproduces, byte-for-byte, serde-derive's externally-tagged encoding of the non-collapsed `[Record]Union`. The `null` arm carries `()` and is encoded as `serialize_newtype_variant(.., &())` — matching serde-derive's treatment of a `Null(())` newtype variant — **not** `serialize_unit_variant`.
- **Deserialization:** Routes through `#[serde(try_from = "[Record]Repr")]`, where the private carrier `[Record]Repr` holds a private `[Record]ReprUnion` enum. A generated `TryFrom` moves the payload into the public enum without extra allocation.
- **Constructors:** Callers construct variants directly (e.g., `Scalar::Int(7)`).

---

## 6. Namespaces and Module Organization

Avro namespaces represent logical hierarchies using dot-separation (e.g., `com.example.hr`).

### 6.1. Module Generation

The generator must map these hierarchies directly to nested, public modules in Rust:

```rust
pub mod com {
    pub mod example {
        pub mod hr {
            pub struct Employee { ... }
        }
    }
}
```

### 6.2. Relative Path Calculations

When a type `A` in namespace `com.example.hr` references a type `B` in namespace `com.example.location`:

- Compute the common namespace ancestor (`com.example`).
- Navigate up from `A`'s scope using `super::super` (one `super` for `hr`, one for `example`).
- Navigate down to `B`'s scope using `location::Address`.
- The fully resolved relative path must be generated as: `super::super::location::Address`.
- If a target type has no namespace (root namespace), navigate all the way to root using appropriate `super::` sequences, or refer directly using absolute paths if referencing absolute workspace symbols.

---

## 7. Testing Strategy & Java Interoperability

Verification of correctness is a primary objective. The rewrite must preserve the comprehensive suite of tests.

### 7.1. Rust Roundtrip Tests

Implemented in `vanavro-tests/tests/roundtrip.rs` (one test per corpus fixture; the sibling `determinism.rs`, `fingerprint.rs`, `java_interop.rs` and `snapshots.rs` cover §14 and §7.2 / §7.4):

- Construct Rust struct instances with extreme edge cases (max/min boundaries, empty collections, complex unicode emojis, deep recursion).
- Serialize to binary format using `apache_avro::Writer`.
- Deserialize using `apache_avro::Reader` into the generated Rust types.
- Assert equality: `original == deserialized`.

### 7.2. Java Interoperability Roundtrip (The Reference Standard)

The ultimate test of correctness is compatibility with the reference Apache Avro Java implementation.

#### Mechanics of the Java Roundtrip

1. **Rust Serialization:** Instantiates the generated Rust struct, serializes it, and wraps it in an Avro Object Container File format (which embeds the schema in its header).
2. **Execution of `avro-tools`:** Calls the official `avro-tools` JAR wrapper found on `PATH` using `Command`:

   ```bash
   avro-tools cat <input_container.avro> <output_container.avro>
   ```

   _Note: `avro-tools cat` reads the container file, decodes the records using Java's deserializer, validates them against the embedded schema, and encodes them back using Java's serializer._

3. **Rust Deserialization:** The output file is read and deserialized back into the Rust generated struct.
4. **Validation:** Asserts that the original Rust struct is strictly equal to the Java-roundtripped struct.

#### Negative & Robustness Testing

- **Corruption Detection — _not implemented in v1_:** An integration test should purposely flip bits in the serialized container payload and verify that `avro-tools` successfully rejects the corrupted stream, proving that the Java validation is active. No such test exists yet; the sync-marker check below is the only evidence that Java actually re-encoded the data.
- **Sync Marker Verification:** Verify that `avro-tools` actually re-encoded the data by validating that the file sync markers in the output file differ from the input file, ensuring it did not simply pass bytes through.

### 7.3. Test Environment Integration

- **Cargo Nextest:** The suite must be fully compatible with `cargo nextest`.
- **`avro-tools` on `PATH`:** The repository ships no Nix flake or other packaging for `avro-tools` / a JRE. The live `avro-tools cat` step of `java_interop.rs` runs only when `avro-tools` is on `PATH` and is skipped (with a note on stderr) otherwise; the checked-in-fixture assertions of §7.4 always run. `fixtures/regenerate.sh` fails fast when `avro-tools` is missing.

### 7.4. Checked-In Reference Fixtures

Serialization and deserialization tests **must** validate the Rust implementation against canonical reference payloads produced once by the Java `avro-tools` CLI and committed to the repository — not regenerated on every test run.

- **Fixture Generation (one-time, offline):** For each Java-validated fixture stem (one `<stem>.avsc` + `<stem>.input.json` pair, not one per named type), use `avro-tools` to emit:
  - A binary Avro Object Container File (`<stem>.avro`) via `avro-tools fromjson` from the JSON input.
  - A canonical JSON payload (`<stem>.json`) via `avro-tools tojson` from that container.
    Both files are checked into `vanavro-tests/fixtures/reference/`; the source `.avsc` and `.input.json` sit one level up in `vanavro-tests/fixtures/`.
- **Regeneration Script:** `vanavro-tests/fixtures/regenerate.sh` drives `avro-tools` to rebuild the fixtures. It is invoked manually when the corpus changes; the test suite does **not** run it.
- **Test Assertions:** Rust tests load each checked-in fixture and assert:
  - Deserializing the `.avro` container with the generated Rust type yields the expected struct.
  - Re-serializing that struct produces a binary stream byte-equal to the fixture (or at minimum decodes back to the same logical value when sync markers / compression differ).
  - Deserializing the `.json` fixture (via `apache_avro::from_value` over the canonical Avro JSON form) yields the same struct. **Not implemented:** `apache-avro` 0.22 ships no schema-aware Avro-JSON decoder (only binary `from_avro_datum` and Object-Container `Reader`), and the generic `Value::from(serde_json::Value)` route produces untyped maps that `from_value` rejects for `record` / `union` shapes. The `.json` fixtures are therefore retained as reviewable documentation of each container's contents rather than as a test input; revisit if a future `apache-avro` exposes a JSON decoder.
- **Why Checked-In:** Pinning Java-produced fixtures guarantees that regressions in the Rust codegen are caught against a stable reference, removes the JRE / `avro-tools` dependency from the default test path, and makes cross-language correctness diffs reviewable in version control.

---

## 8. Error Model

The crate exposes a single error type `GeneratorError` re-exported from the crate root, implementing `std::error::Error` via `thiserror` and `miette::Diagnostic` for rich CLI diagnostics.

### 8.1. Variants

| Variant                                                                    | Cause                                                                                                                                                                                    |
| :------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Io(std::io::Error)`                                                       | Filesystem read/write of input or output files.                                                                                                                                          |
| `SchemaParse { source: String, inner: apache_avro::Error }`                | `.avsc` JSON failed to parse; `source` is `"<schemas[N]>"` for the N-th input, or `"<merged registry>"` for the combined cross-reference pass. Inputs are JSON strings, so no file path is available. |
| `IdlParse { source: String, inner: Box<dyn Error + Send + Sync> }`         | `.avdl` file failed to parse; `source` is the IDL path and `inner` the pre-rendered `avdl` diagnostic. Only exists with the `idl` feature.                                                |
| `DuplicateName { name: String, first: String, second: String }`            | Same fully-qualified name declared in two inputs with non-equal schemas.                                                                                                                 |
| `UnresolvedReference { name: String, referenced_from: String }`            | Type reference cannot be resolved against the merged name table.                                                                                                                         |
| `UnsupportedSchema { detail: String }`                                     | Input the generator cannot represent: a non-named top-level input, a missing `name`, `.avdl` input without the `idl` feature, a record field holding more than one enum-generating union, or a misplaced, missing, or invalid `"enum"` union name (§5.3). RPC `messages` are ignored (§9.3), not rejected. |
| `UnknownLogicalType { logical_type: String, field: String, record: String }` | A `logicalType` matches no standard logical type and no registered custom mapping, and `allow_unregistered_logical_types` is off (§15.6).                                              |
| `InvalidRustType { logical_type: String, detail: String }`                 | A registered custom logical type (§15) supplied a `rust_type` / `converter` / `builder_with` that does not parse as a Rust type / path / token stream. `logical_type` is the `logicalType` name, or the record's fully-qualified name for a record-level mapping. |
| `ForceEqHashOnMap { logical_type: String, field: String, record: String }` | A `force_eq_hash` mapping (§15) is used in a map-value position, where the `HashMap` field can never be `Hash` (§11.1).                                                                  |
| `NameCollision { kind, scope, rust_ident, first, second }`                 | Two distinct Avro names fold onto the same Rust identifier after case conversion / keyword escaping.                                                                                     |
| `Codegen(syn::Error)`                                                      | Reserved for an internal invariant violation while emitting `TokenStream`. Currently never constructed.                                                                                  |
| `Format(syn::Error)`                                                       | `prettyplease` failed to parse generated tokens (bug).                                                                                                                                   |

### 8.2. Reporting & Atomicity

- CLI renders errors with `miette`'s graphical handler; library users receive structured `GeneratorError` directly.
- Source spans are **not** surfaced through `miette::Diagnostic`: `GeneratorError` carries no `#[diagnostic]` labels. The `avdl` diagnostic (source span, label, help) is pre-rendered to text inside `IdlParse::inner`; `apache-avro` errors carry no spans.
- `generate_to_path` must write to a sibling temp file and atomically rename on success — never leave a partial output on failure.

---

## 9. IDL (`.avdl`) Handling

`.avdl` parsing uses the [`avdl`](https://crates.io/crates/avdl) crate (already a workspace dependency). The IDL pipeline normalises into the same in-memory `Schema` name table used by `.avsc` inputs; code generation then runs once over the unified set.

### 9.1. Parser

- Use `avdl`'s top-level parse entry point — do not write a hand-rolled tokenizer.
- The resulting AST's named types (records, enums, fixed) are lowered into `apache_avro::Schema` values and inserted into the global name table (§10).
- IDL annotations on declarations and fields (`@namespace`, `@aliases`, `@order`, `@logicalType`) **must** survive lowering and appear on the resulting `Schema`.
- IDL `/** ... */` doc comments **must** flow into the same `#[doc = " ..."]` emission as `.avsc` `doc`.

### 9.2. Imports

The IDL `import` statement supports three forms:

- `import schema "rel/path.avsc";` — parse as JSON `.avsc`, register types.
- `import idl "rel/path.avdl";` — recursively parse and register.
- `import protocol "rel/path.avpr";` — parse the protocol JSON, register declared types.

Resolution rules:

1. Paths resolve relative to the importing `.avdl` file.
2. The same absolute path imported twice is parsed once (memoised).
3. Cycles between imports are permitted because types merge into the global name table; identical re-declarations are no-ops, conflicting ones raise `DuplicateName`.

(Feeding transitively imported file paths to `cargo::rerun-if-changed` was
originally planned but is not implemented in v1 — see §13.1.)

### 9.3. Protocols & Messages

v1 focuses on **types**, not RPC:

- A `protocol Foo { ... }` contributes its declared records / enums / fixed and applies its `@namespace`, but generates **no** trait, client, or service skeleton.
- `messages` blocks (request / response / errors / oneway) are parsed and validated but **not** code-generated; they are silently ignored.
- This limitation must be documented in the README so it is not a surprise.

### 9.4. Nullable-Union Shorthand (`T?`)

Avro IDL provides a shorthand for the common union of `null` with one other schema: `T?` is equivalent to `union { null, T }`. The shorthand and longhand forms are interchangeable, with or without a default. The following fields are all equivalent (the first three) / equivalent pairs (the last two):

```avdl
record RecordWithUnion {
  union { null, string } optionalString1 = null;
  string? optionalString2 = null;
  string? optionalString3; // No default value

  union { string, null } optionalString4 = "something";
  string? optionalString5 = "something else";
}
```

- Each lowers to a 2-arm union of `null` and `T`, which then maps to `Option<T>` per §3.1 / §3.2.
- `avdl` preserves the **declared** arm order: `T?` and `union { null, T }` are null-first, while `union { T, null }` is `T`-first. Both orderings are recognised as the optional-T shape (§3.2) and produce identical `Option<T>` output.

---

## 10. Multi-Schema Resolution

`--schemas <FILE...>` and repeated `schema_json()` builder calls feed a single name registry shared with `.avdl` inputs (§9.1).

### 10.1. Name Table Construction

1. Parse each input independently into JSON. A `.avsc` input is either a single named type (object) or a flat top-level array whose entries are each registered as a named type; any other top-level shape (a primitive, or an array containing a non-object entry) raises `UnsupportedSchema`, as does an object that is not a named record / enum / fixed or lacks a `name`.
2. Insert each top-level named definition (record, enum, fixed) into a `BTreeMap<String, RegistryEntry>` keyed by fully-qualified Avro name (`namespace.name`, or just `name` when no namespace).
3. After all inputs are parsed, feed the table to `Schema::parse_list` to validate cross-references in a single topologically sorted pass.

### 10.2. Conflict & Ordering Rules

- **Duplicate names:** two inputs declaring the same fully-qualified name raise `DuplicateName` unless the parsed `Schema` values compare structurally equal (post-canonicalisation), in which case the second is a silent no-op.
  - _v1 limitation:_ equality is determined by a recursive sorted-key JSON comparison rather than full Avro canonical form. Two declarations that differ only in surface syntax (e.g. shorthand `"long"` vs `{"type": "long"}`, namespace split out of `name`, alias ordering, default-value formatting) may be flagged as a conflict even when logically identical. Tighten when a real-world input trips it.
- **Input order is irrelevant:** resolution is by name. Output order follows §11.7 (deterministic traversal), not input order.
- **Unresolved refs:** any reference still unresolved after the global pass raises `UnresolvedReference` with the referencing site.

### 10.3. Anonymous Inline Types

Inline records/enums/fixed (declared within a field rather than at the top level) keep their definition inline at the use site and are **not** promoted to the global name table.

---

## 11. Generated Type Conventions

### 11.1. Default Derives

| Construct                          | Derives                                                                                                                                                                                                                                                                             |
| :--------------------------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Record `struct`                    | `Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize` (plus `Eq, Hash` when all fields are themselves `Eq + Hash`; plus `bon::Builder` when `--builder`). A custom-logical field (§15) blocks the `Eq, Hash` uplift unless its registration sets `force_eq_hash` (§15.2). |
| Enum (unit variants)               | `Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize`.                                                                                                                                                                                   |
| Union enum (multi-variant payload) | `Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize` (no blanket `Copy`/`Eq`/`Hash` because variants may carry non-trivial payloads).                                                                                                                                    |
| Fixed type alias                   | n/a (`pub type X = [u8; N];`).                                                                                                                                                                                                                                                      |

`Default` is **not** derived automatically; defaults are surfaced via per-field `#[serde(default = "...")]` plus `Option<T>` (§11.5).

### 11.2. Visibility

All generated items are `pub`; namespace modules are `pub mod`. Non-configurable in v1 — downstream re-exports control external visibility.

- **Exception:** the shared `build_*` schema-builder fns (§12.1) are `pub(crate)`, not `pub`. They are an internal implementation detail of `schema()` — reachable across the generated output's namespace modules but not part of its public surface — so they are deliberately scoped to the crate.

### 11.3. Naming Conventions

| Avro source                | Rust identifier          | Rename attribute                                   |
| :------------------------- | :----------------------- | :------------------------------------------------- |
| Record / enum / fixed name | `PascalCase`             | `#[serde(rename = "OriginalName")]` if changed (none for fixed, see below). |
| Record field name          | `snake_case`             | `#[serde(rename = "originalName")]` if changed.    |
| Enum symbol                | `PascalCase`             | `#[serde(rename = "ORIGINAL_SYMBOL")]` if changed. |
| Namespace segment          | `snake_case` module name | n/a.                                               |

Rust keyword / `self`-family escaping (§5.2) is applied **after** case conversion.

A `fixed` lowers to a `pub type` alias, which cannot carry serde attributes, so a fixed whose Rust ident differs from its Avro name gets no rename. The name is not part of the binary encoding, and it is preserved in the structural `schema()`.

### 11.4. Aliases

Avro field `aliases` are emitted as one `#[serde(alias = "...")]` per alias on the matching record field. Record, enum and fixed **type** aliases emit no serde attribute: serde's `alias` is a field/variant attribute with no container-level form, a `pub type` alias (fixed) cannot carry serde attributes at all, and a type name has no wire representation anyway. All aliases — field and type — are included in the structural `Schema` built per §5.5.

### 11.5. Default Values

| Avro default            | Generated handling                                                                                |
| :---------------------- | :------------------------------------------------------------------------------------------------ |
| `null` on `["null", T]` | `Option<T>` field with `#[serde(default)]`.                                                       |
| Primitive literal       | `#[serde(default = "default_<record>_<field>")]` plus `fn default_<record>_<field>() -> T { <literal> }`. |
| `[]` on array           | `#[serde(default)]` (relies on `Vec::default`).                                                   |
| `{}` on map             | `#[serde(default)]` (relies on `HashMap::default`).                                               |
| Record / enum literal   | `fn default_<record>_<field>()` constructing the value.                                           |

`<record>` and `<field>` are the snake-cased record and field names (the record prefix keeps helpers of same-named fields in different records in one module distinct). Default helpers are emitted as `pub(super)` items in the same module as the owning type.

### 11.6. File Header

Generated output carries **no** file header: no `// @generated` banner and no module-level `#![allow(...)]`. The output is meant to be `include!`d into a module of the consumer's crate, where an inner attribute would be invalid mid-file; the including site applies whatever lint attributes it needs (e.g. a crate-level `#![allow(...)]` above the `include!`). Output therefore starts directly with the first item (typically a `pub mod`).

### 11.7. Determinism

Output **must** be byte-identical across runs for identical inputs:

- Iterate the name registry in sorted fully-qualified-name order.
- Within a record, preserve Avro field declaration order (Avro semantics depend on it).
- Within a union, preserve Avro variant order.
- Replace any `HashMap` iteration at emit sites with `BTreeMap` or an explicit sort.

### 11.8. `bon::Builder` Field Attributes

With `--builder` (§2.1), each record field may carry a single merged `#[builder(…)]` attribute:

- `into` on fields of Rust type `String` or `Option<String>` (an Avro `string`, or a `["null", "string"]` optional union), so setters accept `impl Into<String>`. It is scoped to these types to avoid the inference friction blanket `into` causes on primitives and collections, and is skipped on custom-logical fields (§15), whose mapped type may not be `From<&str>`.
- `with = <text>` from a custom logical type's `builder_with` (§15.9).
- `default = …` reusing the §11.5 helper (bare `default` for `[]` / `{}`); omitted on `Option` fields, which `bon` already treats as optional.

`into` and `with` are mutually exclusive by construction, so `bon`'s `into` + `with` conflict cannot arise.

---

## 12. Generated Per-Type API

Every generated record `struct`, unit enum, and union enum exposes:

```rust
impl T {
    /// Statically constructed Avro schema for this type.
    pub fn schema() -> &'static ::apache_avro::Schema;

    /// Fingerprint of `schema()` under the supplied digest (e.g. `::apache_avro::rabin::Rabin`).
    pub fn fingerprint<D: ::digest::Digest>() -> ::apache_avro::schema::SchemaFingerprint;
}
```

- `schema()` returns a reference into a `LazyLock<Schema>` built structurally per §4.1 Option A — no string parsing at runtime.
- `fingerprint()` is a thin wrapper over `schema().fingerprint::<D>()`.
- Fixed type aliases (`pub type X = [u8; N];`) cannot carry an `impl`; emit sibling free functions `pub fn x_schema() -> &'static Schema` and `pub fn x_fingerprint<D: Digest>() -> SchemaFingerprint` in the same module.

### 12.1. O(N) shared-builder factoring

The `LazyLock` initializer does **not** build the `Schema` tree inline at the
call site. Re-emitting each named type's constructor inline at every reference
site would make generated output (and its compile time) O(N²) in the schema
graph, since a type referenced from many places has its full expansion
duplicated at each site. Instead, construction is factored into **one shared
builder fn per named type**:

- Each named type gets a `build_<mangled>` fn (`<mangled>` injectively derived
  from the Avro fully-qualified name), emitted **inside that type's own
  namespace module**, beside the generated type — not at a shared root and not
  in a fixed wrapper module.
- A builder is emitted only for a type **reachable from an emitted type**. An
  unreferenced suppressed logical-type record (§15.3 case 2) produces no Rust
  type and therefore no builder.
- `schema()` calls its type's builder with a fresh `HashSet<Name>` "defined"
  set (bare when co-located; cross-type references resolve through the same
  relative-module path as a type reference, §6.2).
- The builder's prologue —
  `if !defined.insert(name.clone()) { return Schema::Ref { name }; }` —
  reproduces the compile-time DFS's first-occurrence/back-reference decision at
  run time. The materialized `&'static Schema` and its `fingerprint()` are
  therefore **byte-identical** to the fully-inlined construction: this is a
  code-shape change only, with no wire or fingerprint impact.
- **Symbol-collision constraint:** because each builder lives in its type's
  namespace module, a `build_*` symbol for an _emitted_ type collides across two
  generated outputs `include!`d into one crate scope _exactly_ when that type's
  module-scoped definition already would — never independently of it. The one
  exception is a **referenced** suppressed record (§15.3 case 2): its Rust type
  is elided but its builder is still emitted (§15.5 needs it to reconstruct the
  record in `schema()`), so its `build_*` fn — plus the namespace module holding
  it, when no emitted type shares that namespace — is a lone symbol with no
  accompanying type, and can collide independently of any type collision. An
  _unreferenced_ suppressed record produces neither a Rust type nor a builder
  (the reachability rule above), so it introduces nothing that can collide.
- **Visibility:** builders are `pub(crate)` (the §11.2 deviation) — internal to
  `schema()`, reachable across the generated output's modules but not public.
- **Runtime stack depth:** builders recurse at run time (each calls its
  referenced types' builders), where the old inlined form recursed only at
  compile time. A very deep schema graph can therefore overflow the stack
  inside the `LazyLock` initializer on a small-stack thread — unbounded by the
  caller. The default 8 MiB main-thread stack accommodates thousands of levels;
  a schema deep enough to matter is pathological, but callers driving `schema()`
  from a constrained thread should size the stack accordingly.

---

## 13. Build Integration & Dependencies

### 13.1. `build.rs` Helpers — _not implemented in v1_

The original intent was for the builder to emit cargo directives when run from a
build script:

- `cargo::rerun-if-changed=<path>` for every input `.avsc` / `.avdl` **and** every transitively imported IDL file (§9.2).
- `cargo::rerun-if-env-changed=VANAVRO_*` for any env vars the builder reads.

This is **deliberately not provided** in v1:

- The builder reads **no** `VANAVRO_*` environment variables, so there is
  nothing to emit a `rerun-if-env-changed` directive for.
- `schema_json` inputs are in-memory strings with no associated path, so they
  cannot be tracked with `rerun-if-changed`.
- For `.avdl` inputs the `avdl` crate's `Idl2Schemata` API exposes only the
  extracted schemas, not the set of files it read, so transitively imported IDL
  files (§9.2) cannot be enumerated. Emitting `rerun-if-changed` for only the
  top-level `.avdl` would silently miss imported files and cause stale rebuilds
  — worse than emitting nothing.

Downstream `build.rs` consumers should emit their own `rerun-if-changed`
directives for the input files they pass to vanavro (see the `vanavro-tests`
build script for an example).

### 13.2. Required Downstream Dependencies

The README must document the minimum `Cargo.toml` for consumers of generated code (generated output carries no banner, §11.6).

Always required (referenced by every generated type):

```toml
apache-avro = "0.22"
serde       = { version = "1", features = ["derive"] }
digest      = "0.11"   # generated `fingerprint::<D>()` is bound on `digest::Digest`
```

> _Version note:_ generated code targets the public crates.io `apache-avro` **0.22**. Earlier releases are incompatible: the public `0.21` crate lacks the `::apache_avro::serde::{bytes, bytes_opt, fixed, fixed_opt}` modules that every `bytes` / `fixed` field references (§3.4), so generated output fails to compile against it (its API also differs elsewhere, e.g. `Schema::Uuid` is a unit variant). vanavro previously depended on a fork of `apache-avro` to get that API ahead of upstream; upstream 0.22.0 ships identical sources, so the fork is no longer needed. `digest` is `0.11` (its transitive `block-buffer` / `crypto-common` / `hybrid-array` graph differs from `0.10`).

`digest` is required to **compile** generated code, not merely to call `fingerprint()`: every type emits `fn fingerprint<D: ::digest::Digest>()`, whose generic bound needs the crate in scope regardless of whether it is ever called.

Conditionally required (only when the corresponding feature appears):

```toml
uuid       = { version = "1", features = ["serde"] }   # iff any type uses logical `uuid`
serde_json = "1"                                       # iff any field has a `default`, or any
                                                       #     schema carries custom attributes
                                                       #     (embedded in `schema()` as
                                                       #      `serde_json::Value` literals)
bon        = "3"                                        # iff generated with `--builder`
serde_with = "3"                                        # iff any registered custom logical type
                                                        #     (§15) supplies a converter; the
                                                        #     generated code emits `#[serde_as]`
```

Plus any crate named by a registered custom logical type's `rust_type` / `converter` (e.g. `url`, `sguaba`, `serde_json`). These are the consumer's responsibility — vanavro emits the path verbatim and does not add the dependency.

Codegen must only reference crates whose corresponding Avro feature appears in the input — e.g. do not emit `::uuid::Uuid` paths unless a `uuid` logical type is present, and do not emit `#[serde_as]` / `serde_with` references unless at least one converter-backed custom logical type is actually used.

### 13.3. Crate Feature Flags (`vanavro` itself)

| Feature   | Default | Effect                                                                                              |
| :-------- | :------ | :-------------------------------------------------------------------------------------------------- |
| `idl`     | on      | Pulls in `avdl` and enables `.avdl` parsing. Disabling shrinks the dep tree for `.avsc`-only users. |
| `cli`     | on      | Compiles the `vanavro` binary (pulls in `clap` and `miette/fancy`). Library-only consumers can disable. |

`VanAvroBuilder` and the `.builder(bool)` / `--builder` option (`bon::Builder` derives on generated records) are always available; they are not feature-gated.

### 13.4. MSRV & Edition

- Edition: `2024` (matches workspace).
- MSRV: not currently pinned. The crate requires at least the Rust release that
  stabilised let-chains (1.88), which is above the edition-2024 floor (1.85).
  _Not yet implemented:_ a `rust-version` key in `Cargo.toml` and a CI job that
  builds against it. Until both exist, treat the MSRV as "current stable" and do
  not rely on an older toolchain.

---

## 14. Testing Additions

These augment §7.

### 14.1. Snapshot Tests

- Use `insta` (or equivalent) to snapshot the full generated output for a fixed corpus of representative `.avsc` inputs (`.avdl` inputs are covered by end-to-end generation tests in `codegen.rs`, not snapshots).
- Snapshots live in `vanavro-tests/tests/snapshots/` and are reviewed in code review.
- A snapshot diff fails the test; the reviewer decides whether to accept the diff or fix the codegen.

### 14.2. Determinism Test

A test runs the generator twice on the same input and `assert_eq!`s the two outputs byte-for-byte — catches accidental `HashMap`-iteration leaks.

### 14.3. Cross-Language Fingerprint Test

For each named type in the test corpus:

1. Compute the Rabin fingerprint from Rust via `T::fingerprint::<Rabin>()`.
2. Compute it from the same `.avsc` via `avro-tools` (`avro-tools fingerprint --fingerprint CRC-64-AVRO <schema.avsc>`; Avro's 64-bit "Rabin" fingerprint is the `CRC-64-AVRO` algorithm, and `avro-tools` 1.12 does not accept the literal name `Rabin`).
3. Assert equality.

This is the strongest possible check that the structurally built `Schema` matches what the reference Java implementation produces byte-canonically.

> **Known upstream caveat (logical types).** `apache-avro` 0.22's `Schema::canonical_form()` emits a nested type object for logical-type fields (`"type":{"type":"int"}`) instead of the bare primitive (`"type":"int"`) mandated by the Avro Parsing Canonical Form and produced by Java. Consequently the Rust Rabin fingerprint of any schema using a logical type does **not** match `avro-tools`. This is purely an upstream `canonical_form()` defect — it affects `Schema::parse_str` and vanavro's structural `schema()` identically — so the cross-language test asserts the **known mismatch** for the logical-type fixture and flips to an equality assertion once upstream is fixed. The internal-consistency layer (structural `schema()` fingerprint == `parse_str` fingerprint) still passes for logical types.

> Because `fingerprint::<D>()` is part of the generated public API (§12), this caveat is user-visible and must be stated in the README's Limitations table: until upstream is fixed, do not compare a Rust fingerprint against a Java/other-language fingerprint for any schema containing a standard logical type.

---

## 15. Custom Logical Types

In addition to the **standard** logical types of §3.3 (which map to fixed Rust types like `::uuid::Uuid`), vanavro supports **custom** logical types: caller-defined mappings from an arbitrary Avro `logicalType` annotation onto a user-supplied Rust type. This is how a schema can carry, for example, a `string` annotated `@logicalType("url")` that generates a `::url::Url` field, or a `record` annotated `@logicalType("wgs84")` that collapses to `::sguaba::systems::Wgs84`.

Custom logical types are an explicit, opt-in feature configured on the builder; nothing is mapped unless the caller registers it.

### 15.1. Registration API

A mapping is built as a `CustomLogicalType` value and registered with `with_logical_type`, keyed by the `logicalType` string:

```rust
#[derive(Builder)]
pub struct CustomLogicalType {
    #[builder(start_fn, into)] name: String,        // e.g. "url"      — start_fn arg 1
    #[builder(start_fn)]       avro_type: AvroType,  // e.g. AvroType::String — start_fn arg 2
    #[builder(start_fn, into)] rust_type: String,    // e.g. "::url::Url" — start_fn arg 3
    #[builder(into)]           converter: Option<String>, // optional serde_with adapter (§15.4)
    #[builder(into)]           builder_with: Option<String>, // optional bon `#[builder(with = …)]` text (§15.9)
    #[builder(default)]        force_eq_hash: bool,  // optional Eq + Hash uplift override (§11.1)
}

impl VanAvroBuilder {
    pub fn with_logical_type(self, logical_type: CustomLogicalType) -> Self;
}
```

```rust
// converterless, asserts Eq + Hash so it doesn't block the §11.1 uplift:
.with_logical_type(
    CustomLogicalType::builder("url", AvroType::String, "::url::Url")
        .force_eq_hash(true)
        .build(),
)
// converter-backed:
.with_logical_type(
    CustomLogicalType::builder("wgs84", AvroType::Record, "::sguaba::systems::Wgs84")
        .converter("crate::serde_wgs84::Wgs84")
        .build(),
)
// builder-ergonomics: attach a `bon` conversion so callers can pass a `&str`
// and get validation for free (§15.9). Only affects code generated with `--builder`.
.with_logical_type(
    CustomLogicalType::builder("url", AvroType::String, "::url::Url")
        .builder_with("|s: &str| ::url::Url::parse(s)")
        .build(),
)
```

`AvroType` is a public enum re-exported from the crate root enumerating the Avro types that may carry a `logicalType`: `Null`, `Boolean`, `Int`, `Long`, `Float`, `Double`, `Bytes`, `String`, `Record`, `Enum`, `Fixed`. It exists so the registration states which underlying Avro shape the annotation decorates.

Registrations are stored in an insertion-keyed map; registering the same `logicalType` name twice replaces the earlier entry (last-write-wins).

### 15.2. `CustomLogicalType` fields

| Field           | Required | Meaning                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| :-------------- | :------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `rust_type`     | yes      | The Rust type path emitted for any field whose schema carries this `logicalType`. Emitted verbatim — must be a valid path.                                                                                                                                                                                                                                                                                                                                                                                                                            |
| `converter`     | no       | A `serde_with` adapter type. When present, the field gains a `#[serde_as(as = "<converter>")]` attribute (§15.4). When absent, the `rust_type` is expected to (de)serialize correctly under `apache-avro` on its own (e.g. `::url::Url`).                                                                                                                                                                                                                                                                                                             |
| `builder_with`  | no       | Text emitted verbatim as a `bon` `#[builder(with = <text>)]` attribute on every field resolving to this mapping, **only** when generation runs with `--builder` (§2.1). Lets callers attach a conversion closure / function path for validation and ergonomic construction. Purely a `bon::Builder` concern: it never affects serde, derives, or the wire schema, and is a silent no-op without `--builder`. See §15.9.                                                                                                                               |
| `force_eq_hash` | no       | Override forcing the §11.1 `Eq + Hash` uplift to treat this field as `Eq + Hash` despite the opaque `rust_type`. Defaults to `false`; when `false` a field carrying this mapping disqualifies its owning record from the uplift (vanavro can't inspect the opaque type's impls). Set `true` only when the `rust_type` genuinely implements both. Setting `true` on a mapping used in **map-value** position is a contradiction (the field lowers to a `HashMap`, which is never `Hash`) and fails generation with `GeneratorError::ForceEqHashOnMap`. |

### 15.3. Where Custom Logical Types May Appear

The annotation is recovered from the **raw JSON** of the input (§4.3), because `apache-avro` drops unknown `logicalType` on primitives. A registered mapping is applied in all of the following positions:

1. **Primitive field** — a record field whose type is a primitive (or a `{"type": ..., "logicalType": ...}` wrapper). Detected via `get_field_logical_type(record, field)`.
2. **Record type** — a _named record_ carrying a top-level `logicalType`. The record's own struct is **not generated** at all; every reference to it (direct field, array item, map value, union variant) becomes the mapped `rust_type`. Detected via `get_record_logical_type(record)`.
3. **Array items** — `array<T>` where `T` carries the annotation → `Vec<rust_type>`. Detected via `get_array_item_logical_type`.
4. **Map values** — `map<T>` where `T` carries the annotation → `HashMap<String, rust_type>`. Detected via `get_map_value_logical_type`.
5. **Optional union variant** — exactly `["null", T]` / `[T, "null"]` where the non-null variant carries the annotation → `Option<rust_type>`. Detected via `get_union_variant_logical_type`.
6. **Multi-variant union arm** — any arm of an enum-generating union (§5.3) whose node carries the annotation → the generated enum's variant takes `rust_type` as its payload. Detected via `get_union_arm_logical_type`, which reads the annotation off the arm at its declaration index. This is the position through which case 2 reaches a union variant, and it carries the mapping's **converter** as well as its `rust_type` (§15.4).

Detection resolves type references (`from_idl` schemas use type refs rather than inline definitions) through a single shared reference-resolution routine.

Four deliberate limits on case 6:

- **Only a field-root union** (the mirror image of the `array` / `map` limit below: there a container sits inside a union arm, here a union sits inside a container). Recovery indexes the arms of the **field's** raw `type` node by position, guarded only by an arity check — which identifies the right union only when the union sits directly on the field. A union reached through an `array` / `map` — namable since §5.3 gained the `"enum"` attribute — is therefore not recovered: the annotation is ignored and, as below, raises no `UnknownLogicalType`, though a **record-level** mapping (case 2) still applies, `rust_type` only. Equal arities make the check no substitute for identifying the union:

  ```json
  {
    "name": "f",
    "enum": "P",
    "type": [
      { "type": "array", "items": ["int", "boolean"] },
      { "type": "null", "logicalType": "url" }
    ]
  }
  ```

  Both unions have two arms, so reading the field's arms would give the inner union's `boolean` arm the outer `null` arm's mapping — `Boolean(Url)`. Lowering therefore attempts recovery only at a field-root position. Lifting this means walking the raw node down the same `array` / `map` path lowering takes, keeping recovery keyed to the union actually being lowered.

- **Variant naming follows Avro, not the mapping.** The variant's Rust name and its `#[serde(rename = …)]` tag both come from the arm's Avro type (§5.3), because the tag is what the wire carries. An annotated `string` arm is therefore `String(<rust_type>)`, not `Email(<rust_type>)`.
- **`array` / `map` arms keep standard lowering.** In field position the annotation is read off a container's _items_ / _values_, not off the container node (cases 3 and 4), and there is no recovery for a container nested inside a union arm. An annotation on a _primitive_ inside such an arm is therefore ignored, and (unlike every other position) does not raise `UnknownLogicalType`. A **record-level** mapping (case 2) still applies there, because suppression happens during type lowering rather than through annotation recovery — but only its `rust_type`, not its converter, which is the same gap §15.4 has for any suppressed record reached outside cases 1–6. Registering a mapping for the container node itself is not supported in v1; `AvroType` (§15.1) has no `array` / `map` member to declare it with.
- **`builder_with` does not apply.** Case 6 carries a mapping's `rust_type` and `converter` onto the arm, but not its `builder_with` (§15.9), which is silently dropped there. `bon` generates setters for record _fields_, and the field owning the union has the generated `{Record}Union` enum as its type, not the mapped type — so the registered text, which produces the mapped type, could not type-check against the setter even if one were emitted; the arm itself is an enum variant, which `bon` does not generate a setter for at all. Under the §5.6 collapse the record becomes a bare enum with no builder whatsoever. A mapping used **only** in arm position therefore emits no `#[builder(with = …)]` anywhere, narrowing §15.2's "every field resolving to this mapping" to the positions where a field-level setter exists (cases 1 and 3–5). Callers construct the variant directly.

### 15.4. Generated serde Glue (`serde_with`)

When a used mapping has a `converter`, generated code integrates `serde_with`:

- The owning record `struct` gains a `#[serde_as]` attribute (placed before `#[derive(...)]`). A struct gets it iff at least one of its fields resolves to a converter-backed custom logical type (`record_needs_serde_as`).
- The field gains a `#[serde_as(as = "<converter>")]` attribute, shaped to its container:
  - bare field: `#[serde_as(as = "Conv")]`
  - array: `#[serde_as(as = "Vec<Conv>")]`
  - map: `#[serde_as(as = "HashMap<_, Conv>")]`
  - optional union: `#[serde_as(as = "Option<Conv>")]`
- Because `#[serde_as]` is present, the field's standard bytes/fixed serde helpers (§3.4) are **suppressed** for that field — the converter is the single source of (de)serialization truth.

When a used mapping has **no** `converter`, no `#[serde_as]` is emitted; the `rust_type` is referenced directly and must already round-trip under `apache-avro`'s serde.

A **multi-variant union arm** (§15.3 case 6) is a bare position — there is no container to shape the converter into — and the same rule applies one level down, to the generated enum rather than the owning struct:

- The generated `{Record}Union` enum gains `#[serde_as]` iff at least one arm resolves to a converter-backed mapping; the arm's payload gains `#[serde_as(as = "Conv")]`.
- Under the §5.6 collapse the public enum needs neither: its `Deserialize` routes through the private `{Record}Repr` carrier, so `#[serde_as]` and the per-arm attribute go on `{Record}ReprUnion`, and its hand-written `Serialize` — which has no attribute to hang a converter on — wraps the payload in `serde_with::ser::SerializeAsWrap<rust_type, Conv>` instead. Both directions therefore go through the converter, and the wire bytes stay identical to the non-collapsed form.

### 15.5. Schema Preservation

The mapping is a **codegen-time / Rust-type concern only** — it must not alter the wire schema:

- **Record-level logical types (§15.3 case 2) are preserved** in the structural `schema()`: a record mapped to a custom type still appears in the embedded schema with its full field list plus its `logicalType` (it rides `RecordSchema.attributes`); only its **Rust struct** is elided.
- **Field / array-item / map-value / union-arm custom logical types on a _primitive_ are NOT preserved in the embedded `schema()` in v1** — a known limitation of `apache-avro` 0.22, not a vanavro choice. The parser discards an unknown `logicalType` on a primitive at parse time (§4.3), and `apache_avro::Schema` has **no variant** capable of carrying an arbitrary `logicalType` on a primitive (only the fixed known-logical variants `Uuid` / `Date` / `Decimal` / … exist). A bare `Schema::String` serialises as `"string"` with nowhere to re-attach the annotation. Consequently the embedded `schema()` (and any container-file header or schema-registry JSON derived from it) shows the bare underlying primitive, not the annotated type.
  - **Fingerprints and single-object encoding are unaffected:** the Avro Parsing Canonical Form strips `logicalType` regardless (it affects `parse_str` and vanavro's structural `schema()` identically), so the Rabin fingerprint is the same with or without the annotation. An earlier draft of this section wrongly cited "fingerprints" as a beneficiary of preservation; the real (unmet) concern is only the full-JSON schema seen by a non-Rust reader.
  - **Revisit when** `apache-avro` gains a generic logical-type schema node, or when vanavro recovers annotations during parse (§4.3) and stamps them back onto the `Schema` AST before codegen.

### 15.6. Restrictions & Error Behaviour

- **Defaults are deserialized.** A field that carries a registered custom logical type **and** has a non-empty Avro `default` lowers the default through a generated helper fn that deserializes the default's JSON into the mapped type — directly via its `Deserialize` impl, or through `serde_with::de::DeserializeAsWrap<FieldType, Converter>` when the mapping registers a converter. The helper returns the field's full container type, so `#[serde(default = …)]` and the `bon::Builder` default both consume it. `null` / `[]` / `{}` defaults continue to ride `#[serde(default)]`. (The `String`→`#[builder(into)]` ergonomics of §11.8 remain skipped for custom-logical-type fields, since the mapped type may not be `From<&str>`; a mapping may instead supply `builder_with` (§15.9) to attach its own `bon` conversion.)
  - _Limitation:_ the helper `.expect()`s the deserialization, so it **panics at runtime** if the registered `rust_type` / converter cannot deserialize the schema's default value. vanavro cannot pre-validate this — the mapped type is opaque at codegen time — and serde's `default = "fn"` hook requires an infallible `fn() -> T`, so the panic is unavoidable. Register a `rust_type` whose `Deserialize` accepts the default's JSON shape.
- **Unregistered annotations.** A `logicalType` that matches no standard logical type and no registered custom mapping raises `GeneratorError::UnknownLogicalType` rather than silently lowering to the base type, so typos in either the schema or the registration are caught instead of shipping the wrong Rust type downstream. A malformed-but-named standard type (e.g. an out-of-range `decimal`) is not "unknown" — it matches a standard name and follows §3.3 lowering.
  - _Opt-out:_ setting `allow_unregistered_logical_types` (builder default `false`; CLI `--allow-unregistered-logical-types`) suppresses this error. When enabled, an unregistered non-standard `logicalType` is ignored and the field lowers to its base type, exactly as `apache-avro` does when it drops the annotation at parse time. Use this to consume schemas carrying third-party annotations you do not need mapped, at the cost of losing the typo-detection guarantee.
- **Invalid `rust_type` path.** A `rust_type` / `converter` that does not parse as a Rust path, or a `builder_with` that does not tokenize, raises `GeneratorError::InvalidRustType`.
- **`force_eq_hash` on a map value.** A mapping with `force_eq_hash = true` used in a map-value position raises `GeneratorError::ForceEqHashOnMap`. The field lowers to `HashMap<_, _>`, which is never `Hash` regardless of the value type, so the flag could not take effect — rejecting it surfaces the contradiction instead of silently producing a record without the uplift.

### 15.7. Worked Example (IDL)

```avdl
protocol ExampleProtocol {
    @namespace("example")
    record UserProfile {
        @logicalType("uuid")  string id;        // standard  → ::uuid::Uuid
        @logicalType("url")   string website;   // custom    → ::url::Url
    }

    @namespace("geo")
    @logicalType("wgs84")                        // whole record mapped away
    record Wgs84Coordinate { double latitude_degrees; double longitude_degrees; double altitude_meters; }

    @namespace("geo")
    record Route {
        string name;
        array<Wgs84Coordinate> waypoints;        // → Vec<::sguaba::systems::Wgs84>
    }
}
```

```rust
VanAvro::builder()
    .builder(true) // enable `bon::Builder`, so `builder_with` takes effect
    .idl_path("schemas/example.avdl")
    .with_logical_type(
        CustomLogicalType::builder("url", AvroType::String, "::url::Url")
            .builder_with("|s: &str| ::url::Url::parse(s)")
            .build(),
    )
    .with_logical_type(
        CustomLogicalType::builder("wgs84", AvroType::Record, "::sguaba::systems::Wgs84")
            .converter("crate::serde_wgs84::Wgs84")
            .build(),
    )
    .generate_to_path(out_dir.join("generated.rs"))?;
```

The generated `geo::Route` carries `#[serde_as]`, its `waypoints` field is `Vec<::sguaba::systems::Wgs84>` with `#[serde_as(as = "Vec<crate::serde_wgs84::Wgs84>")]`, no `Wgs84Coordinate` struct is emitted, and `Route::schema()` still embeds the original `wgs84`-annotated record. Because `--builder` is active, `example::UserProfile.website` (type `::url::Url`) additionally carries `#[builder(with = |s: &str| ::url::Url::parse(s))]`, so callers construct it with `UserProfile::builder().website("https://…")?`.

### 15.8. Testing

- Roundtrip tests (§7.1) instantiate records with custom-logical-type fields (converterless `::url::Url`, converter-backed `Email`, and the record-level `Money` / `Cents` mappings), serialize/deserialize through `apache-avro`, and assert equality — including the array (`Vec<Url>`) and optional (`Option<Url>`) shapes.
- Generation tests assert that a custom-logical-type field with a non-empty `default` emits the deserializing default helper of §15.6, both converterless and converter-backed.
- A negative test asserts that an unregistered, non-standard `logicalType` raises `GeneratorError::UnknownLogicalType` (§15.6), in both field and multi-variant-union-arm position (§15.3 case 6).
- Roundtrip tests cover every arm shape of a multi-variant union (§15.3 case 6) — converter-backed primitive, suppressed record (case 2), unmapped primitive, and `null` — in both the `{Record}Union` and the §5.6 collapsed shape. The collapsed shape runs through the single-object encoding as well, so a `Serialize` / `Deserialize` asymmetry in the converter wiring cannot hide.
- A snapshot / generation test asserts that a mapping with `builder_with` generated under `--builder` emits `#[builder(with = <text>)]` verbatim on the matching field (across bare, array, and optional positions, §15.9), and that the same mapping generated **without** `--builder` emits no such attribute (silent no-op).

### 15.9. Builder Ergonomics (`builder_with`)

When a mapping sets `builder_with` **and** generation runs with `--builder` (§2.1), every generated record field that resolves to this custom logical type carries a `#[builder(with = <text>)]` attribute, with `<text>` emitted verbatim. This lets callers attach a `bon` conversion closure or function path (a validating constructor, an ergonomic `impl Into<…>` adapter, etc.) to cut boilerplate and overhead at construction sites. It is the custom-logical-type replacement for the automatic `#[builder(into)]` that §11.8 applies to plain `String` fields but deliberately skips for opaque mapped types (§15.6).

- **Verbatim emission.** vanavro does not parse or rewrite the text; it is the caller's responsibility that it is valid `bon` `with` syntax — an infallible closure (`|s: &str| … `), a fallible closure returning `Result<_[, E]>` (which makes the setter fallible), or a function path such as `FromIterator::from_iter` / `Some`.
- **Container-dependent signature (caller's concern).** What the `with` value must produce depends on the field's position (§15.3):
  - **bare field** (`rust_type`) → the closure returns `rust_type`;
  - **optional union** (`["null", T]` / `[T, "null"]` → `Option<rust_type>`) → the closure returns `rust_type`; `bon` wraps the result in `Some`, so the **same** text works for bare and optional positions;
  - **array** (`Vec<rust_type>`) → the closure returns `Vec<rust_type>`, or uses the `FromIterator::from_iter` sugar to accept `impl IntoIterator`;
  - **map value** (`HashMap<String, rust_type>`) → the closure returns the whole map (or uses `from_iter`).

  vanavro emits the identical text in every position, so a single mapping applied across positions with incompatible shapes will not type-check; register distinct `logicalType`s if a mapping is used in mismatched containers. Record-level custom logical types (§15.3 case 2) suppress the owning struct entirely, so `builder_with` applies at the **referencing** field's container shape, not the mapped-away record.

  A **multi-variant union arm** (§15.3 case 6) is absent from that list deliberately: the field there is typed by the generated `{Record}Union` enum rather than the mapped type, so no setter exists that the registered text could satisfy, and `builder_with` is dropped. See the fourth bullet of §15.3's case-6 limits for why there is no defensible place to put it.

- **No `--builder` ⇒ silent no-op.** `builder_with` only affects the `bon::Builder` derive; without `--builder` there is no builder to attach to and the value is ignored (no error, no diagnostic).
- **Not a validation guarantee.** `builder_with` runs **only** at construction through the generated builder. Values arriving via deserialization (Avro decode) — and any value assigned directly rather than through the builder — bypass it entirely, and it is a complete no-op without `--builder`. A fallible `builder_with` therefore validates the values a caller _constructs_, not the values a type can _hold_. An invariant that must hold for **every** value of the mapped type belongs in `converter` (a validating `serde_with::DeserializeAs`, §15.4) or in the `rust_type`'s own `Deserialize` impl, not in `builder_with`. Use `builder_with` for construction ergonomics; use `converter` / `Deserialize` for wire-level correctness.
- **Interaction with `into` and `default`.** `bon` rejects `#[builder(into)]` together with `#[builder(with)]`; because custom-logical fields never receive the automatic `into` (§15.6), no conflict arises. A `builder_with` and a field `default` (§15.6) merge into one `#[builder(with = …, default = …)]` attribute, following the same per-field attribute merge vanavro already performs for `into` / `default`.
- **Orthogonal to serde glue and derives.** `builder_with` is independent of `converter` / `#[serde_as]` (§15.4) and of `force_eq_hash` (§11.1); a mapping may combine them freely. It never alters the wire schema (§15.5).
- **No new dependency.** `#[builder(with)]` is part of `bon` (already required whenever `--builder` is used, §13.2); `builder_with` adds nothing to the consumer's `Cargo.toml`.
