# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Releases
before 0.0.2-alpha.3 are only recorded as git tags.

## [Unreleased]

## [0.0.2-alpha.3] - 2026-10-14

### Fixed

- Comparing two strings in an expression (`==`, `!=`, `<`, `<=`, `>`, `>=`) was false on every
  row, so a `filter` condition such as `status == "active"` or `lower(name) == "x"` kept
  nothing. The comparison tried numbers first, and the string-to-number cast turns every
  non-number into NULL instead of failing. Two string operands (Utf8, LargeUtf8, Utf8View or
  a dictionary of strings) now compare as strings; any other pair still compares as numbers
  first.
