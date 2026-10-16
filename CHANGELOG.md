# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Releases
before 0.0.3 are only recorded as git tags.

## [Unreleased]

## [0.0.7] - 2026-10-16

### Fixed

- The `join` step rebuilt every output batch from JSON rows and inferred the types again, so
  a column could change type between batches and a sink failed with "column types must match
  schema types" (for example a right-side Boolean that was all NULL in a batch without a
  match became Utf8), and timestamps and decimals came out as text. The join now gathers the
  input arrays with Arrow `take`: every column keeps its type and every batch of one join
  has the same schema. Output columns are the left columns in order, then the right non-key
  columns (before, the order followed the JSON keys). Unmatched right rows of a `full` join
  use that same schema.

## [0.0.6] - 2026-10-16

### Added

- Expression function `if(cond, then, else)` (alias `iif`): `then` where `cond` is true,
  otherwise `else`. A NULL condition counts as false, like SQL `CASE WHEN`. The Arrow type is
  kept when both branches share it, otherwise the result is Utf8. Together with
  `regexp_like` this keeps a value only for whitelisted keys, e.g.
  `if(is_null(value) or regexp_like(key, '^(?:a|b)$'), value, 'redacted')`.

## [0.0.5] - 2026-10-14

### Added

- Expression functions `regexp_like(col, pattern)` (aliases `regex_like`, `rlike`) and
  `regexp_replace(col, pattern, replacement)` (alias `regex_replace`), with Rust regex syntax.
  NULL stays NULL; `regexp_replace` leaves values that do not match unchanged, so it can
  replace phone numbers in a column while keeping other text. Pattern and replacement must
  be string literals.

## [0.0.4] - 2026-10-14

### Fixed

- A column from a value injection (`schema.arrow.columns.<col>.value`) or a `map` step took
  its nullability from the batch it was computed on. The sink keeps the schema of the first
  batch, so when that batch had no NULLs and a later one had, the load failed ("Column ...
  is declared as non-nullable but contains null values" on MSSQL, a NOT NULL violation on an
  auto-created PostgreSQL table). Values computed from batch columns are now always
  nullable; constants (literals, env vars, `now()`) keep their own nullability, and an
  explicit `nullable:` still wins.

## [0.0.3] - 2026-10-14

### Fixed

- Comparing two strings in an expression (`==`, `!=`, `<`, `<=`, `>`, `>=`) was false on every
  row, so a `filter` condition such as `status == "active"` or `lower(name) == "x"` kept
  nothing. The comparison tried numbers first, and the string-to-number cast turns every
  non-number into NULL instead of failing. Two string operands (Utf8, LargeUtf8, Utf8View or
  a dictionary of strings) now compare as strings; any other pair still compares as numbers
  first.
