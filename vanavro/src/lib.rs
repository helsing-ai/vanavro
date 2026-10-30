//! `vanavro` — generate idiomatic Rust types from Apache Avro schemas.
//!
//! Public API surface:
//!
//! - [`VanAvro`] — the configured generator. Construct via [`VanAvro::builder`].
//! - [`VanAvroBuilder`] — `bon`-derived fluent builder.
//! - [`GeneratorError`] — single error type returned from every generation
//!   call, suitable for `miette` rendering.

use std::io::Write;
use std::path::{Path, PathBuf};

use bon::Builder;
use miette::Diagnostic;
use thiserror::Error;

mod codegen;
mod logical;
mod schema;
mod split;

pub use logical::AvroType;

/// A custom Avro `logicalType` mapping, built fluently and handed to
/// [`VanAvroBuilder::with_logical_type`].
///
/// Construct via the `bon`-generated builder: the three positional arguments to
/// [`CustomLogicalType::builder`] are the `logicalType` name, the Avro shape it
/// decorates, and the Rust type emitted for fields carrying it. The optional
/// `converter`, `builder_with`, and `force_eq_hash` setters cover the
/// `serde_with` adapter, the `bon` `#[builder(with = …)]` text,
/// and the `Eq + Hash` uplift override respectively.
///
/// ```
/// use vanavro::{AvroType, CustomLogicalType};
///
/// // Plain mapping (round-trips under `apache-avro` on its own):
/// let _url = CustomLogicalType::builder("url", AvroType::String, "::url::Url").build();
///
/// // Converter-backed, and forced `Eq + Hash` so it doesn't block the uplift:
/// let _wgs84 = CustomLogicalType::builder("wgs84", AvroType::Bytes, "::geo::Wgs84")
///     .converter("crate::serde_wgs84::Wgs84")
///     .force_eq_hash(true)
///     .build();
/// ```
#[derive(Debug, Clone, Builder)]
pub struct CustomLogicalType {
    /// The `logicalType` string this mapping is keyed on.
    #[builder(start_fn, into)]
    name: String,
    /// The Avro shape the annotation decorates.
    #[builder(start_fn)]
    avro_type: AvroType,
    /// Rust type path emitted verbatim for fields carrying this `logicalType`.
    #[builder(start_fn, into)]
    rust_type: String,
    /// Optional `serde_with` converter routing (de)serialisation.
    #[builder(into)]
    converter: Option<String>,
    /// Optional `bon` `#[builder(with = …)]` text, emitted verbatim on every
    /// field resolving to this mapping when generation runs with `--builder`,
    /// except multi-variant union arms. Lets callers
    /// attach a conversion closure / function path for validation and
    /// ergonomic construction. It never affects serde,
    /// derives, or the wire schema, and is a silent no-op without `--builder`.
    #[builder(into)]
    builder_with: Option<String>,
    /// Force the owning record's `Eq + Hash` uplift to treat this field as
    /// `Eq + Hash`, even though `rust_type` is opaque to vanavro.
    /// Defaults to `false` (conservative); set `true` only when `rust_type`
    /// genuinely implements both. Setting this on a mapping used in a map-value
    /// position is a contradiction (the field is a `HashMap`, never `Hash`) and
    /// fails generation with [`GeneratorError::ForceEqHashOnMap`].
    #[builder(default)]
    force_eq_hash: bool,
}

/// Configured generator. Instantiate with [`VanAvro::builder`].
///
/// ```
/// use vanavro::VanAvro;
///
/// let _cfg = VanAvro::builder()
///     .schema_json("{}")
///     .build();
/// ```
#[derive(Debug, Clone, Builder)]
pub struct VanAvro {
    /// Accumulated `.avsc` JSON inputs. Populated via the
    /// [`VanAvroBuilder::schema_json`] extension setter.
    #[builder(field)]
    schema_jsons: Vec<String>,

    /// Caller-registered custom logical types. Populated via the
    /// [`VanAvroBuilder::with_logical_type`] extension setter.
    #[builder(field)]
    logical_types: logical::LogicalTypeRegistry,

    /// Path to an optional `.avdl` IDL input.
    #[builder(into)]
    idl_path: Option<PathBuf>,

    /// Whether to derive `bon::Builder` on generated record structs. Read by
    /// the codegen pipeline via `codegen_options`.
    #[builder(default)]
    builder: bool,

    /// Whether to emit `#[non_exhaustive]` on generated enums. Defaults to
    /// `true`. Read by the codegen pipeline via `codegen_options`.
    #[builder(default = true)]
    non_exhaustive: bool,

    /// Whether to collapse a single-field enum-generating-union record into a
    /// bare public enum. Defaults to `true`; set `false` to keep
    /// such records as ordinary structs with a sibling `{Record}Union` enum.
    /// Read by the codegen pipeline via `codegen_options`.
    #[builder(default = true)]
    collapse_single_union: bool,

    /// Whether to tolerate a `logicalType` that matches no standard logical type
    /// and no registered custom mapping. Defaults to `false`, which
    /// raises [`GeneratorError::UnknownLogicalType`] so typos are caught. When
    /// `true`, such an annotation is ignored and the field lowers to its base
    /// type, exactly as `apache-avro` does when it drops the annotation. Read by
    /// the codegen pipeline via `codegen_options`.
    #[builder(default)]
    allow_unregistered_logical_types: bool,
}

impl<S: van_avro_builder::State> VanAvroBuilder<S> {
    /// Append an `.avsc` JSON schema. Can be called repeatedly.
    pub fn schema_json(mut self, schema: impl Into<String>) -> Self {
        self.schema_jsons.push(schema.into());
        self
    }

    /// Register a custom Avro `logicalType` mapping, built fluently
    /// via [`CustomLogicalType`].
    ///
    /// Registering the same `logicalType` name twice replaces the earlier entry
    /// (last-write-wins).
    ///
    /// ```
    /// use vanavro::{AvroType, CustomLogicalType, VanAvro};
    ///
    /// let _cfg = VanAvro::builder().with_logical_type(
    ///     CustomLogicalType::builder("url", AvroType::String, "::url::Url").build(),
    /// );
    /// ```
    pub fn with_logical_type(mut self, logical_type: CustomLogicalType) -> Self {
        self.logical_types.insert(
            logical_type.name,
            logical_type.avro_type,
            logical_type.rust_type,
            logical_type.converter,
            logical_type.builder_with,
            logical_type.force_eq_hash,
        );
        self
    }
}

impl<S: van_avro_builder::IsComplete> VanAvroBuilder<S> {
    /// Run the generator and return the formatted Rust source.
    pub fn generate(self) -> Result<String, GeneratorError> {
        self.build().generate()
    }

    /// Run the generator and stream output into `writer`.
    pub fn generate_to_writer<W: Write>(self, writer: &mut W) -> Result<(), GeneratorError> {
        self.build().generate_to_writer(writer)
    }

    /// Run the generator and atomically write output to `path`, split into one
    /// file per namespace module when `split` is `true`. See
    /// [`VanAvro::generate_to_path`].
    pub fn generate_to_path(
        self,
        path: impl AsRef<Path>,
        split: bool,
    ) -> Result<(), GeneratorError> {
        self.build().generate_to_path(path, split)
    }
}

impl VanAvro {
    /// Run the generator and return the formatted Rust source.
    pub fn generate(&self) -> Result<String, GeneratorError> {
        Ok(codegen::format_file(&self.generate_file()?))
    }

    /// Run the generator and stream output into `writer`.
    pub fn generate_to_writer<W: Write>(&self, writer: &mut W) -> Result<(), GeneratorError> {
        let source = self.generate()?;
        writer.write_all(source.as_bytes())?;
        Ok(())
    }

    /// Run the generator and atomically write output to `path`.
    ///
    /// Each file is written through a sibling temp file and renamed on
    /// success, and nothing is written at all if generation fails.
    ///
    /// Without `split`, the whole output is written to `path` as one file.
    ///
    /// With `split`, `path` holds the root module, and each namespace module is
    /// written to its own file under a directory named after `path`'s stem:
    /// `out/gen.rs` declares `#[path = "gen/com.rs"] pub mod com;`,
    /// `out/gen/com.rs` declares `#[path = "com/example.rs"] pub mod example;`,
    /// and so on. The explicit `#[path]` attributes keep the root file
    /// `include!`-able. Files left over from a previous run whose namespace no
    /// longer exists are not removed; they are unreferenced and do not affect
    /// compilation. Fails with [`GeneratorError::InvalidOutputPath`] if `path`
    /// has no extension or a non-UTF-8 stem.
    pub fn generate_to_path(
        &self,
        path: impl AsRef<Path>,
        split: bool,
    ) -> Result<(), GeneratorError> {
        let path = path.as_ref();
        let file = self.generate_file()?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        if !split {
            return write_atomically(parent, path, &codegen::format_file(&file));
        }

        // SPEC §8.3: validate the path before writing anything, then write each
        // module file atomically (SPEC §8.2).
        let stem = split_stem(path)?;
        let (root, modules) = split::split(file, stem);
        let module_dir = parent.join(stem);
        for (relative, module) in &modules {
            let target = module_dir.join(relative);
            let dir = target.parent().unwrap_or(&module_dir);
            std::fs::create_dir_all(dir)?;
            write_atomically(dir, &target, &codegen::format_file(module))?;
        }
        // The root goes last, so it never references a module file that has
        // not been written yet.
        write_atomically(parent, path, &codegen::format_file(&root))
    }

    fn generate_file(&self) -> Result<syn::File, GeneratorError> {
        let resolved = schema::parse(&self.schema_jsons, self.idl_path.as_deref())?;
        codegen::generate_file(
            &resolved.schemas,
            &resolved.raw_names,
            &self.logical_types,
            self.codegen_options(),
        )
    }

    fn codegen_options(&self) -> codegen::CodegenOptions {
        codegen::CodegenOptions {
            builder: self.builder,
            non_exhaustive: self.non_exhaustive,
            collapse_single_union: self.collapse_single_union,
            allow_unregistered_logical_types: self.allow_unregistered_logical_types,
        }
    }
}

/// Write `source` to `path` through a temp file in `dir`, renamed on success.
fn write_atomically(dir: &Path, path: &Path, source: &str) -> Result<(), GeneratorError> {
    let mut tmp = tempfile::NamedTempFile::new_in(dir)?;
    tmp.write_all(source.as_bytes())?;
    tmp.flush()?;
    tmp.persist(path).map_err(|e| GeneratorError::Io(e.error))?;
    Ok(())
}

/// The file stem of a split output's root, which names both the module
/// directory beside it and the `#[path]` prefix of its module declarations.
fn split_stem(path: &Path) -> Result<&str, GeneratorError> {
    let invalid = |detail: &str| GeneratorError::InvalidOutputPath {
        path: path.to_path_buf(),
        detail: detail.to_string(),
    };
    // Without an extension the module directory `<stem>/` would be `path`
    // itself.
    if path.extension().is_none() {
        return Err(invalid(
            "a split output needs a file extension (e.g. `.rs`), since its modules \
             are written to a directory named after the file stem; add one or \
             write a single file by not splitting the output",
        ));
    }
    path.file_stem().and_then(|s| s.to_str()).ok_or_else(|| {
        invalid("the file stem must be valid UTF-8 to be used in `#[path]` attributes")
    })
}

/// Errors surfaced from any `generate*` call.
#[derive(Debug, Error, Diagnostic)]
pub enum GeneratorError {
    /// Filesystem read/write of an input or output file failed.
    #[error("I/O error")]
    Io(#[from] std::io::Error),

    /// An `.avsc` document failed to parse.
    #[error("failed to parse Avro schema from {source}")]
    SchemaParse {
        /// Origin label: `"<schemas[N]>"` for the N-th `.avsc` input, or
        /// `"<merged registry>"` for the combined cross-reference pass.
        source: String,
        /// Underlying parser error.
        #[source]
        inner: apache_avro::Error,
    },

    /// An `.avdl` document failed to parse.
    ///
    /// `avdl` does not export a concrete error type; the underlying diagnostic
    /// is preserved as a boxed `Error` for rich rendering.
    #[cfg(feature = "idl")]
    #[error("failed to parse Avro IDL from {source}")]
    IdlParse {
        /// Origin label — typically the input path.
        source: String,
        /// Underlying diagnostic from the `avdl` parser.
        #[source]
        inner: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Two inputs declared the same fully-qualified name with non-equal schemas.
    #[error("duplicate name `{name}` declared in {first} and {second}")]
    DuplicateName {
        name: String,
        first: String,
        second: String,
    },

    /// A type reference could not be resolved against the merged name table.
    #[error("unresolved reference to `{name}` from {referenced_from}")]
    UnresolvedReference {
        name: String,
        referenced_from: String,
    },

    /// Input the generator cannot represent: a non-named top-level schema, a
    /// missing `name`, an unsupported union shape, or `.avdl` input
    /// without the `idl` feature.
    #[error("unsupported schema: {detail}")]
    UnsupportedSchema { detail: String },

    /// A `logicalType` annotation matched no standard logical type and no
    /// registered custom mapping. Surfaced rather than silently
    /// lowering to the base type, so a typo in either the schema or the
    /// registration is caught instead of shipping the wrong Rust type.
    #[error(
        "unknown logical type `{logical_type}` on field `{field}` of `{record}`: \
         not a standard logical type and no custom mapping is registered"
    )]
    UnknownLogicalType {
        /// The unrecognized `logicalType` name.
        logical_type: String,
        /// The field carrying the annotation.
        field: String,
        /// The enclosing record's fully-qualified name.
        record: String,
    },

    /// A custom logical type set `force_eq_hash` but is used in a
    /// `map` value position, where the field lowers to a `HashMap` that can
    /// never be `Hash` regardless of the value type. Surfaced
    /// rather than silently ignoring the flag, so the contradictory request is
    /// caught instead of quietly producing a record without the uplift.
    #[error(
        "custom logical type `{logical_type}` sets force_eq_hash but is used in a \
         map-value position on field `{field}` of `{record}`: the field is a \
         `HashMap`, which is never `Hash`"
    )]
    ForceEqHashOnMap {
        /// The `logicalType` name whose mapping forces the uplift.
        logical_type: String,
        /// The map-typed field carrying the annotation.
        field: String,
        /// The enclosing record's fully-qualified name.
        record: String,
    },

    /// A registered custom logical type supplied a `rust_type`,
    /// `converter` or `builder_with` that does not parse as a Rust path / type
    /// / token stream. Surfaced as a clean error instead of panicking codegen,
    /// since these strings come straight from the public builder API.
    #[error("invalid custom logical type for `{logical_type}`: {detail}")]
    InvalidRustType {
        /// The `logicalType` name whose mapping is invalid, or the record's
        /// fully-qualified name for a record-level mapping.
        logical_type: String,
        /// What failed to parse (the offending path and parser message).
        detail: String,
    },

    /// Two distinct Avro names fold onto the same Rust identifier after
    /// case conversion (PascalCase / snake_case) or keyword escaping, so the
    /// generated code would not compile. `kind` names the scope
    /// (e.g. `"fields in record"`, `"types in namespace"`), `scope` is the
    /// owning record / namespace / enum, and `first` / `second` are the two
    /// original Avro names that collide on `rust_ident`.
    #[error(
        "name collision: {kind} `{scope}` map `{first}` and `{second}` \
         onto the same Rust identifier `{rust_ident}`"
    )]
    NameCollision {
        /// What kind of scope the collision occurred in.
        kind: String,
        /// The owning record FQN / namespace / enum FQN / `"<root>"`.
        scope: String,
        /// The Rust identifier both Avro names converge on.
        rust_ident: String,
        /// First (earlier-declared) Avro name.
        first: String,
        /// Second (later-declared) Avro name.
        second: String,
    },

    /// The output path given to [`VanAvro::generate_to_path`] cannot hold a
    /// split output (see the `split` argument).
    #[error("invalid output path `{}`: {detail}", path.display())]
    InvalidOutputPath {
        /// The requested output path.
        path: PathBuf,
        /// Why it cannot be used, and how to fix it.
        detail: String,
    },

    /// Internal invariant violation while emitting `TokenStream`. Always a bug.
    /// Currently never constructed; reserved.
    #[error("internal code generation error")]
    Codegen(#[source] syn::Error),

    /// `prettyplease` failed to parse generated tokens. Always a bug.
    #[error("internal formatter error")]
    Format(#[source] syn::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Layout-insensitive check for the `Eq + Hash` derive uplift: prettyplease
    /// may wrap the derive list, and a bare `.contains("Hash")` would false-match
    /// `HashMap` / `PartialEq`. Mirrors the codegen test module's helper.
    fn has_eq_hash_uplift(src: &str) -> bool {
        let squashed: String = src.chars().filter(|c| !c.is_whitespace()).collect();
        squashed.contains("PartialEq,Eq,Hash,")
    }

    #[test]
    fn builder_accumulates_schemas() {
        let v = VanAvro::builder()
            .schema_json("{}")
            .schema_json(String::from("{}"))
            .idl_path("foo.avdl")
            .builder(true)
            .non_exhaustive(false)
            .collapse_single_union(false)
            .build();

        assert_eq!(v.schema_jsons.len(), 2);
        assert_eq!(v.idl_path.as_deref(), Some(Path::new("foo.avdl")));
        assert!(v.builder);
        assert!(!v.non_exhaustive);
        assert!(!v.collapse_single_union);
    }

    #[test]
    fn builder_defaults() {
        let v = VanAvro::builder().build();
        assert!(v.schema_jsons.is_empty());
        assert!(v.idl_path.is_none());
        assert!(!v.builder);
        assert!(
            v.non_exhaustive,
            "SPEC §2.1: non_exhaustive defaults to true"
        );
        assert!(
            v.collapse_single_union,
            "SPEC §5.6: collapse_single_union defaults to true"
        );
    }

    // SPEC §15 end-to-end: a registered custom logical type flows from the
    // public builder API through to the generated field type.
    #[test]
    fn with_logical_type_maps_field_through_public_api() {
        let src = VanAvro::builder()
            .schema_json(
                r#"{"type":"record","name":"Link","namespace":"web","fields":[
                    {"name":"href","type":{"type":"string","logicalType":"url"}}
                ]}"#,
            )
            .with_logical_type(
                CustomLogicalType::builder("url", AvroType::String, "::url::Url").build(),
            )
            .build()
            .generate()
            .expect("generate succeeds");
        assert!(src.contains("pub href: ::url::Url"), "{src}");
    }

    // SPEC §15.4 end-to-end: a converter-backed mapping emits `serde_with`
    // glue through the public builder API.
    #[test]
    fn with_logical_type_with_converter_through_public_api() {
        let src = VanAvro::builder()
            .schema_json(
                r#"{"type":"record","name":"Position","namespace":"geo","fields":[
                    {"name":"loc","type":{"type":"bytes","logicalType":"wgs84"}}
                ]}"#,
            )
            .with_logical_type(
                CustomLogicalType::builder("wgs84", AvroType::Bytes, "::geo::Wgs84")
                    .converter("crate::serde_wgs84::Wgs84")
                    .build(),
            )
            .build()
            .generate()
            .expect("generate succeeds");
        assert!(src.contains("pub loc: ::geo::Wgs84"), "{src}");
        assert!(src.contains("serde_as"), "{src}");
    }

    // SPEC §15.8 / §15.9 — a mapping with `builder_with` emits
    // `#[builder(with = …)]` verbatim on the matching field across bare, array,
    // and optional positions, but **only** under `--builder`; without the
    // builder derive it is a silent no-op.
    #[test]
    fn builder_with_emitted_only_under_builder() {
        // One record exercising all three container shapes (§15.3 cases 1/3/5).
        let schema = r#"{"type":"record","name":"Link","namespace":"web","fields":[
            {"name":"href","type":{"type":"string","logicalType":"url"}},
            {"name":"mirrors","type":{"type":"array","items":{"type":"string","logicalType":"url"}}},
            {"name":"canonical","type":["null",{"type":"string","logicalType":"url"}]}
        ]}"#;
        let generate = |builder: bool| {
            VanAvro::builder()
                .schema_json(schema)
                .builder(builder)
                .with_logical_type(
                    CustomLogicalType::builder("url", AvroType::String, "::url::Url")
                        .builder_with("|s: &str| ::url::Url::parse(s)")
                        .build(),
                )
                .build()
                .generate()
                .expect("generate succeeds")
        };
        let with_builder = generate(true);
        // The verbatim text appears once per position (bare / array / optional).
        assert_eq!(
            with_builder.matches("::url::Url::parse(s)").count(),
            3,
            "builder_with must be emitted verbatim in all three positions: {with_builder}"
        );
        // prettyplease may space the attribute either way depending on version.
        assert!(
            with_builder.contains("# [builder (with =")
                || with_builder.contains("#[builder(with ="),
            "builder_with must land inside a #[builder(with = …)] attr: {with_builder}"
        );
        let without_builder = generate(false);
        assert!(
            !without_builder.contains("builder"),
            "builder_with must be a silent no-op without --builder: {without_builder}"
        );
    }

    // SPEC §15.3 case 6 / §15.9 — `builder_with` is deliberately **not**
    // emitted for a mapping that only lands on a multi-variant union arm. The
    // field is typed by the generated `{Record}Union` enum, so there is no
    // setter the registered text could satisfy; the arm is an enum variant,
    // which `bon` gives no setter at all. The arm still gets the mapped type,
    // so this pins the *absence* of the builder attribute rather than the
    // absence of the mapping.
    #[test]
    fn builder_with_is_dropped_on_a_union_arm() {
        let src = VanAvro::builder()
            .schema_json(
                r#"{"type":"record","name":"Link","namespace":"web","fields":[
                    {"name":"target","type":[
                        "null",
                        {"type":"string","logicalType":"url"},
                        "int"
                    ]},
                    {"name":"label","type":"string"}
                ]}"#,
            )
            .builder(true)
            .with_logical_type(
                CustomLogicalType::builder("url", AvroType::String, "::url::Url")
                    .builder_with("|s: &str| ::url::Url::parse(s)")
                    .build(),
            )
            .build()
            .generate()
            .expect("generate succeeds");
        assert!(
            src.contains("String(::url::Url)"),
            "the arm must still take the mapped type: {src}"
        );
        assert!(
            !src.contains("::url::Url::parse(s)"),
            "builder_with has no home in arm position and must be dropped: {src}"
        );
        // Sanity: the record does derive a builder, so the absence above is the
        // arm limit rather than a missing derive.
        assert!(src.contains("::bon::Builder"), "{src}");
    }

    // SPEC §15.9 — a `builder_with` and a field `default` merge into one
    // `#[builder(with = …, default = …)]` attribute (the same per-field merge
    // vanavro already performs for `into` / `default`).
    #[test]
    fn builder_with_merges_with_default() {
        let src = VanAvro::builder()
            .schema_json(
                r#"{"type":"record","name":"Link","namespace":"web","fields":[
                    {"name":"href","type":{"type":"string","logicalType":"url"},
                     "default":"https://example.com/"}
                ]}"#,
            )
            .builder(true)
            .with_logical_type(
                CustomLogicalType::builder("url", AvroType::String, "::url::Url")
                    .builder_with("|s: &str| ::url::Url::parse(s)")
                    .build(),
            )
            .build()
            .generate()
            .expect("generate succeeds");
        // Both parts must live in a single #[builder(...)] attribute.
        let squashed: String = src.chars().filter(|c| !c.is_whitespace()).collect();
        assert!(
            squashed.contains("#[builder(with=|s:&str|::url::Url::parse(s),default="),
            "with + default must merge into one attribute: {src}"
        );
    }

    // SPEC §15.9 — a `builder_with` text that does not even tokenize surfaces a
    // clean [`GeneratorError::InvalidRustType`], never a downstream `Format`
    // panic (the text comes straight from the public builder API).
    #[test]
    fn invalid_builder_with_errors_cleanly() {
        let err = VanAvro::builder()
            .schema_json(
                r#"{"type":"record","name":"Link","namespace":"web","fields":[
                    {"name":"href","type":{"type":"string","logicalType":"url"}}
                ]}"#,
            )
            .builder(true)
            // An unbalanced delimiter cannot tokenize.
            .with_logical_type(
                CustomLogicalType::builder("url", AvroType::String, "::url::Url")
                    .builder_with("|s: &str| (::url::Url::parse(s)")
                    .build(),
            )
            .build()
            .generate()
            .expect_err("unparseable builder_with must error, not panic");
        match err {
            GeneratorError::InvalidRustType {
                logical_type,
                detail,
            } => {
                assert_eq!(logical_type, "url");
                assert!(detail.contains("builder_with"), "{detail}");
            }
            other => panic!("expected InvalidRustType, got {other:?}"),
        }
    }

    // SPEC §11.1 + §15: a custom-logical field's mapped Rust type is opaque, so
    // by default it blocks the owning record's `Eq + Hash` uplift. Setting
    // `force_eq_hash(true)` on the registration restores it. The single-field
    // record would otherwise qualify (a bare `string`), isolating the override.
    #[test]
    fn force_eq_hash_restores_uplift_for_custom_logical_field() {
        let schema = r#"{"type":"record","name":"Link","namespace":"web","fields":[
            {"name":"href","type":{"type":"string","logicalType":"url"}}
        ]}"#;
        let generate = |force_eq_hash: bool| {
            VanAvro::builder()
                .schema_json(schema)
                .with_logical_type(
                    CustomLogicalType::builder("url", AvroType::String, "::url::Url")
                        .force_eq_hash(force_eq_hash)
                        .build(),
                )
                .build()
                .generate()
                .expect("generate succeeds")
        };
        assert!(
            !has_eq_hash_uplift(&generate(false)),
            "opaque custom-logical field must block the uplift by default"
        );
        assert!(
            has_eq_hash_uplift(&generate(true)),
            "force_eq_hash(true) must restore the uplift"
        );
    }

    // A converter-backed mapping with `force_eq_hash(true)` must emit both the
    // `#[serde_as]` glue (SPEC §15.4) and the `Eq + Hash` uplift (SPEC §11.1).
    #[test]
    fn converter_with_force_eq_hash_emits_serde_as_and_uplift() {
        let src = VanAvro::builder()
            .schema_json(
                r#"{"type":"record","name":"Position","namespace":"geo","fields":[
                    {"name":"loc","type":{"type":"bytes","logicalType":"wgs84"}}
                ]}"#,
            )
            .with_logical_type(
                CustomLogicalType::builder("wgs84", AvroType::Bytes, "::geo::Wgs84")
                    .converter("crate::serde_wgs84::Wgs84")
                    .force_eq_hash(true)
                    .build(),
            )
            .build()
            .generate()
            .expect("generate succeeds");
        assert!(src.contains("serde_as"), "{src}");
        assert!(has_eq_hash_uplift(&src), "force_eq_hash must uplift: {src}");
    }

    // SPEC §15.6 — a registered `rust_type` that does not parse as a Rust path
    // must surface a clean [`GeneratorError::InvalidRustType`], never panic
    // codegen (the string comes straight from the public builder API).
    #[test]
    fn invalid_rust_type_path_errors_cleanly() {
        let err = VanAvro::builder()
            .schema_json(
                r#"{"type":"record","name":"Link","namespace":"web","fields":[
                    {"name":"href","type":{"type":"string","logicalType":"url"}}
                ]}"#,
            )
            // Deliberately unparseable as a Rust path.
            .with_logical_type(
                CustomLogicalType::builder("url", AvroType::String, "not a::valid path!!!").build(),
            )
            .build()
            .generate()
            .expect_err("invalid rust_type must error, not panic");
        match err {
            GeneratorError::InvalidRustType {
                logical_type,
                detail,
            } => {
                assert_eq!(logical_type, "url");
                assert!(detail.contains("rust_type"), "{detail}");
            }
            other => panic!("expected InvalidRustType, got {other:?}"),
        }
    }

    // SPEC §15.6 — an invalid `converter_type` is likewise a clean error.
    #[test]
    fn invalid_converter_type_path_errors_cleanly() {
        let err = VanAvro::builder()
            .schema_json(
                r#"{"type":"record","name":"Position","namespace":"geo","fields":[
                    {"name":"loc","type":{"type":"string","logicalType":"wgs84"}}
                ]}"#,
            )
            .with_logical_type(
                CustomLogicalType::builder("wgs84", AvroType::String, "::geo::Wgs84")
                    .converter("this is::not a converter!!!")
                    .build(),
            )
            .build()
            .generate()
            .expect_err("invalid converter_type must error, not panic");
        match err {
            // `::geo::Wgs84` is a valid rust_type, so the *converter* must be the
            // culprit — assert that, so the test can't pass for the wrong reason.
            GeneratorError::InvalidRustType {
                logical_type,
                detail,
            } => {
                assert_eq!(logical_type, "wgs84");
                assert!(detail.contains("converter_type"), "{detail}");
            }
            other => panic!("expected InvalidRustType, got {other:?}"),
        }
    }

    // SPEC §15.3 case 2 + §15.6 — a record-level logical type (the suppressed-
    // record path, distinct from the field-level path above) with an invalid
    // `rust_type` must also error cleanly rather than panic.
    #[test]
    fn invalid_record_level_rust_type_errors_cleanly() {
        let err = VanAvro::builder()
            // `Geo` carries a record-level `logicalType`, so its struct is
            // suppressed. Reaching the suppressed-record path
            // (`parse_suppressed_rust_type`) needs a position none of the §15.3
            // detectors cover, since each of those resolves the mapping — and
            // its converter — itself. A *nested* array qualifies: the array-item
            // detector (case 3) reads the outer array's items, which is the
            // inner array node and carries no annotation, so the inner `Geo`
            // ref is only resolved during type lowering.
            .schema_json(
                r#"{"type":"record","name":"Geo","namespace":"g","logicalType":"wgs84","fields":[
                    {"name":"lat","type":"double"},
                    {"name":"lon","type":"double"}
                ]}"#,
            )
            .schema_json(
                r#"{"type":"record","name":"Route","namespace":"g","fields":[
                    {"name":"legs","type":{"type":"array","items":{
                        "type":"array","items":"g.Geo"
                    }}}
                ]}"#,
            )
            // Record-level mapping (AvroType::Record) with an unparseable type.
            .with_logical_type(
                CustomLogicalType::builder("wgs84", AvroType::Record, "not a::type!!!").build(),
            )
            .build()
            .generate()
            .expect_err("invalid record-level rust_type must error, not panic");
        match err {
            GeneratorError::InvalidRustType {
                logical_type,
                detail,
            } => {
                assert_eq!(logical_type, "g.Geo");
                assert!(detail.contains("record-level"), "{detail}");
            }
            other => panic!("expected InvalidRustType, got {other:?}"),
        }
    }

    // SPEC §8.2 happy path — `generate_to_path` writes the formatted source
    // to the requested path.
    #[test]
    fn generate_to_path_writes_source_at_target() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let target = tmp.path().join("generated.rs");
        VanAvro::builder()
            .schema_json(
                r#"{"type":"record","name":"R","namespace":"t","fields":[{"name":"x","type":"int"}]}"#,
            )
            .build()
            .generate_to_path(&target, false)
            .expect("generate_to_path succeeds");
        let written = std::fs::read_to_string(&target).expect("read back");
        assert!(written.contains("pub mod t {"), "{written}");
        assert!(written.contains("pub struct R"), "{written}");
        assert!(written.contains("pub x: i32"), "{written}");
        assert!(
            !tmp.path().join("generated").exists(),
            "an unsplit output must not create a module directory"
        );
    }

    // SPEC §8.3 — `split` writes the root module to the requested path and
    // each namespace module to its own file under `<stem>/`, wired up with
    // `#[path]` declarations relative to the declaring file.
    #[test]
    fn generate_to_path_splits_modules_into_files() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let target = tmp.path().join("generated.rs");
        VanAvro::builder()
            .schema_json(r#"{"type":"record","name":"Root","fields":[{"name":"x","type":"int"}]}"#)
            .schema_json(
                r#"{"type":"record","name":"Deep","namespace":"com.example","fields":[
                    {"name":"e","type":"com.E"}
                ]}"#,
            )
            .schema_json(r#"{"type":"enum","name":"E","namespace":"com","symbols":["A"]}"#)
            .build()
            .generate_to_path(&target, true)
            .expect("generate_to_path succeeds");

        let read = |p: &Path| {
            std::fs::read_to_string(p).unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
        };
        let root = read(&target);
        assert!(root.contains("pub struct Root"), "{root}");
        assert!(
            root.contains("#[path = \"generated/com.rs\"]\npub mod com;"),
            "{root}"
        );
        assert!(!root.contains("pub enum E"), "{root}");

        let com = read(&tmp.path().join("generated/com.rs"));
        assert!(com.contains("pub enum E"), "{com}");
        assert!(
            com.contains("#[path = \"com/example.rs\"]\npub mod example;"),
            "{com}"
        );
        assert!(!com.contains("pub struct Deep"), "{com}");

        let example = read(&tmp.path().join("generated/com/example.rs"));
        assert!(example.contains("pub struct Deep"), "{example}");
        // Cross-module references are unchanged by the split.
        assert!(example.contains("pub e: super::E"), "{example}");
    }

    // SPEC §8.1 / §8.3 — a split output's modules go in a directory named after the file stem, so
    // an extensionless path (whose stem *is* the path) is rejected up front,
    // with an actionable message and nothing written.
    #[test]
    fn split_rejects_extensionless_path() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let target = tmp.path().join("generated");
        let err = VanAvro::builder()
            .schema_json(
                r#"{"type":"record","name":"R","namespace":"t","fields":[{"name":"x","type":"int"}]}"#,
            )
            .build()
            .generate_to_path(&target, true)
            .expect_err("extensionless split path must be rejected");
        match &err {
            GeneratorError::InvalidOutputPath { path, detail } => {
                assert_eq!(path, &target);
                assert!(detail.contains("extension"), "{detail}");
                assert!(detail.contains("split"), "{detail}");
            }
            other => panic!("expected InvalidOutputPath, got {other:?}"),
        }
        assert!(!target.exists(), "nothing must be written: {target:?}");
    }

    // SPEC §8.2 atomicity — on codegen failure the target path must not
    // exist (no partial file left behind). We trigger the failure with a
    // multi-variant union nested inside an array, which v1 codegen rejects
    // because there's no SPEC-prescribed enum name for the inner union.
    #[test]
    fn generate_to_path_leaves_no_partial_file_on_failure() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let target = tmp.path().join("should_not_exist.rs");
        let err = VanAvro::builder()
            .schema_json(
                r#"{"type":"record","name":"R","namespace":"t","fields":[
                    {"name":"x","type":{"type":"array","items":["int","string","long"]}}
                ]}"#,
            )
            .build()
            .generate_to_path(&target, true)
            .expect_err("nested multi-variant union must fail codegen");
        assert!(matches!(err, GeneratorError::UnsupportedSchema { .. }));
        assert!(
            !target.exists(),
            "target path must not exist after failure: {target:?}"
        );
    }
}
