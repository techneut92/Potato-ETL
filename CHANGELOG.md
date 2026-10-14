# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Releases
before 0.0.3 are only recorded as git tags.

## [Unreleased]

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
