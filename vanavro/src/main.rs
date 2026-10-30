use std::path::PathBuf;

use clap::{ArgGroup, Parser};
use vanavro::{GeneratorError, VanAvro};

/// Generate idiomatic Rust types from Apache Avro schemas.
#[derive(Debug, Parser)]
#[command(name = "vanavro", version, about, long_about = None)]
#[command(group(ArgGroup::new("input").required(true).args(["schemas", "idl"])))]
struct Cli {
    /// Input Avro schema files (`.avsc` JSON). Mutually exclusive with `--idl`.
    #[arg(long, num_args = 1.., value_name = "FILE", group = "input")]
    schemas: Vec<PathBuf>,

    /// Input Avro IDL file (`.avdl`). Mutually exclusive with `--schemas`.
    #[arg(long, value_name = "FILE", group = "input")]
    idl: Option<PathBuf>,

    /// Output file path. Defaults to stdout when omitted.
    #[arg(short, long, value_name = "FILE")]
    output: Option<PathBuf>,

    /// Split `--output` into one file per namespace module, under a directory
    /// named after the output's file stem.
    #[arg(long, requires = "output")]
    split: bool,

    /// Derive `bon::Builder` on generated record structs.
    #[arg(long)]
    builder: bool,

    /// Disable the default `#[non_exhaustive]` attribute on generated enums.
    #[arg(long = "no-non-exhaustive")]
    no_non_exhaustive: bool,

    /// Disable the default collapse of a single-field enum-generating-union
    /// record into a bare enum; keep it as a struct with a `{Record}Union` enum.
    #[arg(long = "no-collapse-single-union")]
    no_collapse_single_union: bool,

    /// Tolerate a `logicalType` that matches no standard logical type and no
    /// registered custom mapping: ignore the annotation and lower the field to
    /// its base type instead of erroring.
    #[arg(long = "allow-unregistered-logical-types")]
    allow_unregistered_logical_types: bool,
}

fn main() -> miette::Result<()> {
    let cli = Cli::parse();
    run(cli).map_err(miette::Report::new)
}

fn run(cli: Cli) -> Result<(), GeneratorError> {
    // Set every typestate-tracked setter exactly once so the typestate is
    // fixed before we enter the accumulator loop. `maybe_idl_path` is the
    // bon-generated `Option`-flavoured setter — it lets us pass `cli.idl`
    // (which may be `None`) without conditionally changing the builder type.
    let mut builder = VanAvro::builder()
        .builder(cli.builder)
        .non_exhaustive(!cli.no_non_exhaustive)
        .collapse_single_union(!cli.no_collapse_single_union)
        .allow_unregistered_logical_types(cli.allow_unregistered_logical_types)
        .maybe_idl_path(cli.idl);

    for path in &cli.schemas {
        let json = std::fs::read_to_string(path)?;
        builder = builder.schema_json(json);
    }

    match cli.output {
        Some(path) => builder.generate_to_path(path, cli.split),
        None => {
            let mut stdout = std::io::stdout().lock();
            builder.generate_to_writer(&mut stdout)
        }
    }
}
