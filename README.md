# VanAvro

A Rust code generator for Apache Avro schemas. Generate type-safe Rust structs
and enums from Avro JSON (`.avsc`) and IDL (`.avdl`) files, with serde-based
serialization through `apache-avro`.

> **On quality**: This crate was written with heavy LLM assistance, but that
> is not a statement about its quality — the two are decoupled by design:
>
> - **A reference oracle.** Correctness is checked against the Apache Avro Java
>   implementation. Generated code round-trips through the `apache-avro` runtime
>   and is verified against Java Avro, so behaviour is anchored to the canonical
>   implementation rather than to our own assumptions.
> - **A test corpus built for coverage.** The suite reuses the upstream Java
>   `.avdl` schemas and adds further schemas targeting edge cases those don't
>   exercise. Snapshot tests pin the generated output; line coverage sits at
>   ~95%.
> - **A hardened generation process.** Two generator iterations were kept
>   separate, with the second written without access to the first so early
>   mistakes were not carried forward. Each step was followed by a deliberate,
>   uncompromising review, with findings addressed — some through manual
>   intervention.

## Features

- **Type-safe code generation** — Rust structs and enums from Avro schemas
- **Full Avro type support** — records, enums, arrays, maps, unions, fixed, and all primitives
- **Logical types** — UUID, date, time, timestamp, local-timestamp, decimal, duration (see [Limitations](#limitations))
- **Custom logical types** — map arbitrary `logicalType` annotations onto your own Rust types, optionally via a `serde_with` converter
- **Recursive types** — self-referential schemas (linked lists, trees) via boxing
- **Namespaces** — Avro namespaces and cross-namespace references
- **Schema embedding** — each type carries its `schema()` and a `fingerprint::<D>()` (see [Limitations](#limitations) for logical types)
- **Avro IDL** — generate from `.avdl` via the bundled parser (no external tools)
- **Java interoperability** — verified against the Apache Avro Java implementation

## Installation

Add `vanavro` as a build dependency:

```toml
[build-dependencies]
vanavro = { version = "0.1", default-features = false }
```

The default features are `cli` (the `vanavro` binary) and `idl` (`.avdl`
input). A build script needs neither unless it reads Avro IDL; in that case
add `features = ["idl"]`.

The generated code references these crates. Always required:

```toml
[dependencies]
apache-avro = "0.22"
serde       = { version = "1", features = ["derive"] }
digest      = "0.11" # generated `fingerprint::<D: Digest>()` is bound on `digest::Digest`
```

`digest` is needed to **compile** the generated code, not just to call
`fingerprint()`: every type emits `fn fingerprint<D: digest::Digest>()`, whose
bound requires the crate in scope even if you never call it.

Add the rest only when the corresponding input/option is present (the generator
never references a crate it doesn't use):

```toml
uuid       = { version = "1", features = ["serde"] } # iff any `uuid` logical type
serde_json = "1"                                     # iff any field has a `default`, or
                                                     #     any schema has custom attributes
bon        = "3"                                     # iff generated with `.builder(true)`
serde_with = "3"                                     # iff any registered custom logical type
                                                     #     (§15) supplies a converter
```

Plus any crate named by a registered custom logical type's `rust_type` /
`converter` (e.g. `url`, `sguaba`) — vanavro emits the path verbatim and
does not add the dependency for you.

## Usage

### Build script

```rust
use std::path::PathBuf;
use vanavro::VanAvro;

fn main() {
    println!("cargo:rerun-if-changed=schemas/my_schema.avsc");

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let schema = std::fs::read_to_string("schemas/my_schema.avsc").unwrap();

    VanAvro::builder()
        .schema_json(schema)
        .generate_to_path(out_dir.join("my_schema.rs"))
        .unwrap();
}
```

### Using generated types

```rust
mod my_schema {
    include!(concat!(env!("OUT_DIR"), "/my_schema.rs"));
}

use my_schema::MyRecord;
use apache_avro::{Writer, Reader, from_value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let record = MyRecord { name: "Alice".to_string(), age: 30 };

    let mut writer = Writer::new(MyRecord::schema(), Vec::new())?;
    writer.append_ser(&record)?;
    let bytes = writer.into_inner()?;

    let reader = Reader::new(&bytes[..])?;
    for value in reader {
        let decoded: MyRecord = from_value(&value?)?;
        assert_eq!(record, decoded);
    }
    Ok(())
}
```

To split a large output into reviewable files, call `.generate_to_tree(path)`
instead of `.generate_to_path(path)`: `my_schema.rs` then holds only the root
module, and each namespace module goes into its own file under `my_schema/`,
mirroring the Rust module tree (`my_schema/com.rs`, `my_schema/com/example.rs`,
…). The `include!` above works unchanged.

### Multiple schemas with shared types

Call `.schema_json` repeatedly on one builder so shared types are defined once:

```rust
VanAvro::builder()
    .schema_json(schema_a)
    .schema_json(schema_b)
    .generate_to_path(out_dir.join("schemas.rs"))
    .unwrap();
```

### Avro IDL

```rust
VanAvro::builder()
    .idl_path("schemas/protocol.avdl")
    .generate_to_path(out_dir.join("protocol.rs"))
    .unwrap();
```

## Builder configuration

| Method                                                           | Description                                                   |
| ---------------------------------------------------------------- | ------------------------------------------------------------- |
| `.schema_json(json)`                                             | Add an `.avsc` JSON string (repeatable)                       |
| `.idl_path(path)`                                                | Set an `.avdl` IDL file path                                  |
| `.builder(bool)`                                                 | Derive `bon::Builder` on record structs (default: `false`)    |
| `.non_exhaustive(bool)`                                          | Emit `#[non_exhaustive]` on enums (default: `true`)           |
| `.collapse_single_union(bool)`                                   | Collapse a single-union-record to an enum (default: `true`)   |
| `.with_logical_type(CustomLogicalType)`                          | Map a custom `logicalType` onto a Rust type (repeatable)      |
| `.allow_unregistered_logical_types(bool)`                        | Ignore unknown `logicalType`s instead of erroring (default: `false`) |
| `.generate()`                                                    | Generate and return a `String`                                |
| `.generate_to_path(path)`                                        | Generate and atomically write to a file                       |
| `.generate_to_tree(path)`                                        | Generate and atomically write one file per namespace module   |
| `.generate_to_writer(writer)`                                    | Generate and stream to a writer                               |

## Custom logical types

Map an arbitrary Avro `logicalType` annotation onto a Rust type of your choice.
Nothing is mapped unless you register it.

```rust
use vanavro::{AvroType, CustomLogicalType, VanAvro};

let src = VanAvro::builder()
    .schema_json(schema_json)
    // a `string` annotated `logicalType: "url"` → `::url::Url`
    .with_logical_type(
        CustomLogicalType::builder("url", AvroType::String, "::url::Url").build(),
    )
    // a `record` annotated `logicalType: "wgs84"` → `::sguaba::systems::Wgs84`,
    // (de)serialised through a `serde_with` converter
    .with_logical_type(
        CustomLogicalType::builder("wgs84", AvroType::Record, "::sguaba::systems::Wgs84")
            .converter("crate::serde_wgs84::Wgs84")
            .build(),
    )
    .generate()?;
```

- Applies to a primitive field, array items, map values, the non-null arm of an
  `["null", T]` union, an arm of a multi-variant union, and a _named record_
  carrying a top-level `logicalType` (whose struct is then suppressed and replaced by the mapped type everywhere).
- With a converter, the owning struct gains `#[serde_as]` and the field gains
  `#[serde_as(as = "…")]` (shaped to `Vec` / `HashMap` / `Option` containers);
  add `serde_with = "3"` to the consumer's dependencies.
- Without a converter, the `rust_type` must round-trip under `apache-avro`'s
  serde on its own (e.g. `::url::Url`).
- Registering the same name twice is last-write-wins.

## Type mappings

| Avro Type | Rust Type                     | &nbsp; | Avro Logical Type           | Rust Type               |
| --------- | ----------------------------- | ------ | --------------------------- | ----------------------- |
| `null`    | `()`                          |        | `uuid`                      | `uuid::Uuid`            |
| `boolean` | `bool`                        |        | `date`                      | `i32`                   |
| `int`     | `i32`                         |        | `time-millis`               | `i32`                   |
| `long`    | `i64`                         |        | `time-micros`               | `i64`                   |
| `float`   | `f32`                         |        | `timestamp-{millis,micros}` | `i64`                   |
| `double`  | `f64`                         |        | `local-timestamp-*`         | `i64`                   |
| `bytes`   | `Vec<u8>`                     |        | `decimal`                   | `apache_avro::Decimal`  |
| `string`  | `String`                      |        | `duration`                  | `apache_avro::Duration` |
| `array`   | `Vec<T>`                      |        |                             |                         |
| `map`     | `HashMap<String, T>`          |        |                             |                         |
| `record`  | `struct`                      |        |                             |                         |
| `enum`    | `enum`                        |        |                             |                         |
| `fixed`   | `[u8; N]`                     |        |                             |                         |
| `union`   | `Option<T>` or generated enum |        |                             |                         |

## Generated code

Each record becomes a struct (nested in `pub mod` blocks mirroring its
namespace) with serde derives and a schema API:

```rust
#[derive(Debug, Clone, PartialEq, ::serde::Serialize, ::serde::Deserialize)]
pub struct MyRecord {
    pub name: String,
    pub age: i32,
}

impl MyRecord {
    pub fn schema() -> &'static apache_avro::Schema { /* ... */ }
    pub fn fingerprint<D: digest::Digest>() -> apache_avro::schema::SchemaFingerprint { /* ... */ }
}

impl apache_avro::AvroSchema for MyRecord { /* get_schema() */ }
```

`Eq` and `Hash` are added when every field supports them; enums also get
`Copy` (unit variants) and `#[non_exhaustive]`.

## Development

```bash
cargo nextest run                                        # tests
cargo nextest run -p vanavro-tests --test java_interop   # Java interop (needs `avro-tools` on PATH)
```

### `avro-tools` for the full test suite

The Java interop tests (`vanavro-tests/tests/java_interop.rs`) check generated
code against the Apache Avro Java implementation, through the `avro-tools` CLI.
Running the full suite requires `avro-tools` (and a JRE) on `PATH`:

```bash
avro-tools --help   # must resolve
```

Without it, the suite still passes but is **incomplete**:

- The live `avro-tools cat` round-trip (Java decodes and re-encodes each
  container) is skipped, with a `skipping avro-tools cat …` note on stderr.
- The checked-in reference fixtures in `vanavro-tests/fixtures/reference/` are
  still decoded and compared, so Java-produced bytes are verified either way.

Regenerating those fixtures (`vanavro-tests/fixtures/regenerate.sh`, run by hand
whenever the fixture schemas change) also needs `avro-tools` and exits with an
error if it is missing.

## Limitations

| Feature                  | Status                                                                                                                                                                                                                                                                                                                                                                                                           |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **RPC protocols**        | Type definitions are generated; `messages`/RPC scaffolding is not                                                                                                                                                                                                                                                                                                                                                |
| **Error types**          | Generated as plain structs (no RPC error scaffolding)                                                                                                                                                                                                                                                                                                                                                            |
| **UUID on `fixed`**      | Mapped to `uuid::Uuid` like `string`-backed `uuid`; only the `string` form is round-trip tested                                                                                                                                                                                                                                                                                                                              |
| **`duration` in unions** | Supported as `Option<Duration>` in `["null", duration]`; any other union with a `duration` arm is rejected (`UnsupportedSchema`) because apache-avro 0.22 drops the backing fixed's name, which is the union tag. Wrap it in a named record instead |
| **Fingerprints with logical types** | `fingerprint::<D>()` does **not** match Java / `avro-tools` for schemas containing a standard logical type: apache-avro 0.22's `canonical_form()` deviates from the Parsing Canonical Form for logical-type fields. Rust-to-Rust comparisons are consistent; cross-language comparisons of such schemas are not |
| **JSON encoding**        | Binary only — apache-avro 0.22 has no schema-aware Avro-JSON decoder                                                                                                                                                                                                                                                                                                                                             |
| **Schema evolution**     | Aliases emitted as `#[serde(alias)]`; no full reader/writer resolution                                                                                                                                                                                                                                                                                                                                           |
| **Custom logical types** | Supported via `.with_logical_type`. A custom `logicalType` on a _primitive_ (field / array item / map value / optional-union arm) maps the **Rust type** only; it is **not** carried in the embedded `schema()` (apache-avro 0.22 has no AST slot for it — fingerprints are unaffected, but a non-Rust reader of the embedded schema sees the bare primitive). Record-level mappings _are_ preserved. |

## License

Licensed under either the [MIT License](LICENSE-MIT) or
[Apache License, Version 2.0](LICENSE-APACHE), at your option.
