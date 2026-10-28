#!/usr/bin/env bash
#
# Regenerate the checked-in Java reference fixtures (SPEC §7.4).
#
# For each `<stem>.avsc` + `<stem>.input.json` pair this drives the official
# Apache `avro-tools` CLI to emit, into `reference/`:
#   - `<stem>.avro`  — an Avro Object Container File (schema embedded in header)
#   - `<stem>.json`  — the canonical Avro JSON datum (one record), via `tojson`
#
# These are committed and consumed by `tests/java_interop.rs`. The test suite
# does NOT run this script; invoke it manually, with `avro-tools` on PATH,
# whenever the corpus changes, then review + commit the diff.
#
# Usage:  ./regenerate.sh
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
out="$here/reference"
mkdir -p "$out"

if ! command -v avro-tools >/dev/null 2>&1; then
  echo "error: avro-tools not on PATH" >&2
  exit 1
fi

stems=(primitives collections unions nested recursive logical defaults scalar tree named_unions)

for stem in "${stems[@]}"; do
  schema="$here/$stem.avsc"
  input="$here/$stem.input.json"
  echo "regenerating $stem"
  avro-tools fromjson --schema-file "$schema" "$input" >"$out/$stem.avro" 2>/dev/null
  avro-tools tojson "$out/$stem.avro" >"$out/$stem.json" 2>/dev/null
done

echo "done -> $out"
