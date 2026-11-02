# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

## [0.1.1] - 2026-11-02

### Added

- `generate_to_tree(path)` on the builder and on `VanAvro`: like
  `generate_to_path`, but writes one file per namespace module, mirroring the
  Rust module tree.
- The CLI accepts a new argument, `--split` (requires `--output`), to split the
  generated output.

## [0.1.0]

- Initial release
