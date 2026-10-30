# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- The CLI accepts a new argument, `--split` (requires `--output`), to split the
  generated output.

### Changed

- **Breaking:** `generate_to_path` takes a second `split: bool` argument. Pass
  `false` to keep the previous single-file output. When `split` is `true`, one
  file per namespace module, mirroring the Rust module tree, is generated.

## [0.1.0]

- Initial release
