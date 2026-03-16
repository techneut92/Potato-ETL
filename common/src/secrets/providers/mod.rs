//! Secret manager provider trait and implementations.

pub mod hashicorp;
pub mod azure;
pub mod gcp;

use std::collections::HashMap;

/// A secret provider that can fetch secrets from a remote secret manager.
///
/// Each provider implementation handles authentication, caching, and
/// error handling for its specific secret manager API.
#[async_trait::async_trait]
pub trait SecretProvider: Send + Sync {
    /// Fetch a single secret by its provider-specific path.
    ///
    /// Returns the secret value as a JSON-like map of field → value.
    /// For single-value secrets, the map contains one entry with key `""` (empty string).
    /// For structured secrets (e.g. Vault KV v2 data), the map contains all fields.
    ///
    /// Implementations should cache results to avoid redundant API calls
    /// when multiple fields reference the same secret path.
    async fn get_secret(&self, path: &str) -> anyhow::Result<HashMap<String, String>>;

    /// Human-readable provider name for error messages.
    fn provider_name(&self) -> &str;
}

/// Parses a secret value string into a field map.
///
/// Resolution order:
/// 1. Try JSON (`serde_json`) — fast, exact.
/// 2. Try YAML (`serde_yaml_ng`) — catches YAML-formatted secrets.
/// 3. Fall back to plain string under the empty key `""`.
///
/// Top-level mapping keys are extracted directly.  Nested structures are
/// **also** flattened with dot-notation keys (e.g. `optional_params.sslmode`),
/// so that `#optional_params.sslmode` field references work.  The top-level
/// key itself is still present with the re-serialised value for whole-object
/// extraction.
pub(crate) fn parse_structured_value(value: &str) -> HashMap<String, String> {
    // ── 1. Try JSON ──────────────────────────────────────────────────────────
    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(value) {
        if let serde_json::Value::Object(map) = parsed {
            let mut result = HashMap::new();
            flatten_json(&map, "", &mut result);
            tracing::debug!(
                parse_method = "json",
                fields = result.len(),
                field_keys = ?result.keys().collect::<Vec<_>>(),
                "parse_structured_value: parsed as JSON object"
            );
            return result;
        }
        // Parsed as JSON but not an object (e.g. bare string, number) → plain value.
        tracing::debug!(
            parse_method = "json",
            "parse_structured_value: parsed as JSON non-object (scalar/array)"
        );
        let mut m = HashMap::new();
        m.insert(String::new(), value.to_string());
        return m;
    }

    // ── 2. Try YAML ──────────────────────────────────────────────────────────
    if let Ok(parsed) = serde_yaml_ng::from_str::<serde_yaml_ng::Value>(value) {
        if let serde_yaml_ng::Value::Mapping(map) = parsed {
            let mut result = HashMap::new();
            flatten_yaml(&map, "", &mut result);
            if !result.is_empty() {
                tracing::debug!(
                    parse_method = "yaml",
                    fields = result.len(),
                    field_keys = ?result.keys().collect::<Vec<_>>(),
                    "parse_structured_value: parsed as YAML mapping"
                );
                return result;
            }
        }
        // YAML parsed but not a mapping → plain value.
    }

    // ── 3. Plain string ──────────────────────────────────────────────────────
    tracing::debug!(
        parse_method = "plain",
        "parse_structured_value: falling back to plain string"
    );
    let mut m = HashMap::new();
    m.insert(String::new(), value.to_string());
    m
}

/// Recursively flattens a JSON object into dot-separated keys.
///
/// For `{"a": 1, "b": {"c": "x", "d": {"e": true}}}` this produces:
///
/// | Key       | Value    |
/// |-----------|----------|
/// | `a`       | `1`      |
/// | `b`       | `{"c":"x","d":{"e":true}}` |
/// | `b.c`     | `x`      |
/// | `b.d`     | `{"e":true}` |
/// | `b.d.e`   | `true`   |
///
/// Both the top-level key (with the re-serialised value) and the nested
/// dot-notation keys are present, so `#b` and `#b.c` both work.
fn flatten_json(
    map: &serde_json::Map<String, serde_json::Value>,
    prefix: &str,
    out: &mut HashMap<String, String>,
) {
    for (k, v) in map {
        let full_key = if prefix.is_empty() {
            k.clone()
        } else {
            format!("{prefix}.{k}")
        };

        match v {
            serde_json::Value::String(s) => {
                out.insert(full_key, s.clone());
            }
            serde_json::Value::Object(nested) => {
                // Insert the whole nested object as a re-serialised string
                // so `#parent` still works.
                out.insert(full_key.clone(), v.to_string());
                // Recurse for dot-notation access.
                flatten_json(nested, &full_key, out);
            }
            other => {
                out.insert(full_key, other.to_string());
            }
        }
    }
}

/// Recursively flattens a YAML mapping into dot-separated keys.
///
/// For `{"a": 1, "b": {"c": "x", "d": {"e": true}}}` this produces:
///
/// | Key       | Value    |
/// |-----------|----------|
/// | `a`       | `1`      |
/// | `b`       | `{"c":"x","d":{"e":true}}` |
/// | `b.c`     | `x`      |
/// | `b.d`     | `{"e":true}` |
/// | `b.d.e`   | `true`   |
///
/// Both the top-level key (with the re-serialised value) and the nested
/// dot-notation keys are present, so `#b` and `#b.c` both work.
fn flatten_yaml(
    map: &serde_yaml_ng::Mapping,
    prefix: &str,
    out: &mut HashMap<String, String>,
) {
    for (k, v) in map {
        let key = match k {
            serde_yaml_ng::Value::String(s) => s.clone(),
            other => match serde_yaml_ng::to_string(other) {
                Ok(s) => s.trim().to_string(),
                Err(_) => continue,
            },
        };
        let full_key = if prefix.is_empty() {
            key
        } else {
            format!("{prefix}.{key}")
        };

        match v {
            serde_yaml_ng::Value::String(s) => {
                out.insert(full_key, s.clone());
            }
            serde_yaml_ng::Value::Mapping(nested) => {
                // Insert the whole nested mapping as a re-serialised string
                // so `#parent` still works.
                let serialised = serde_yaml_ng::to_string(v)
                    .unwrap_or_default()
                    .trim()
                    .to_string();
                out.insert(full_key.clone(), serialised);
                // Recurse for dot-notation access.
                flatten_yaml(nested, &full_key, out);
            }
            serde_yaml_ng::Value::Bool(b) => {
                out.insert(full_key, b.to_string());
            }
            serde_yaml_ng::Value::Number(n) => {
                out.insert(full_key, n.to_string());
            }
            serde_yaml_ng::Value::Null => {
                out.insert(full_key, String::new());
            }
            other => {
                let serialised = serde_yaml_ng::to_string(other)
                    .unwrap_or_default()
                    .trim()
                    .to_string();
                out.insert(full_key, serialised);
            }
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_flat_json() {
        let m = parse_structured_value(r#"{"host":"db.local","port":5432,"password":"s3cret"}"#);
        assert_eq!(m.get("host").unwrap(), "db.local");
        assert_eq!(m.get("port").unwrap(), "5432");
        assert_eq!(m.get("password").unwrap(), "s3cret");
    }

    #[test]
    fn parse_nested_json_dot_notation() {
        let m = parse_structured_value(
            r#"{"host":"db.local","optional_params":{"sslmode":"require","timeout":"30"}}"#
        );
        // Top-level keys.
        assert_eq!(m.get("host").unwrap(), "db.local");
        // Nested object is present as re-serialised JSON.
        assert!(m.get("optional_params").is_some());
        // Dot-notation keys for nested fields.
        assert_eq!(
            m.get("optional_params.sslmode").unwrap(), "require",
            "dot-notation key should be present"
        );
        assert_eq!(
            m.get("optional_params.timeout").unwrap(), "30",
            "dot-notation key should be present"
        );
    }

    #[test]
    fn parse_deeply_nested_json() {
        let m = parse_structured_value(
            r#"{"a":{"b":{"c":"deep"}}}"#
        );
        assert!(m.get("a").is_some());
        assert!(m.get("a.b").is_some());
        assert_eq!(m.get("a.b.c").unwrap(), "deep");
    }

    #[test]
    fn parse_plain_string_secret() {
        let m = parse_structured_value("just-a-password");
        assert_eq!(m.len(), 1);
        assert_eq!(m.get("").unwrap(), "just-a-password");
    }

    #[test]
    fn parse_yaml_structured() {
        let m = parse_structured_value("host: db.local\nport: 5432\n");
        assert_eq!(m.get("host").unwrap(), "db.local");
        assert_eq!(m.get("port").unwrap(), "5432");
    }

    #[test]
    fn parse_yaml_nested_dot_notation() {
        let m = parse_structured_value(
            "host: db.local\noptional_params:\n  sslmode: require\n  timeout: 30\n"
        );
        assert_eq!(m.get("host").unwrap(), "db.local");
        assert!(m.get("optional_params").is_some());
        assert_eq!(
            m.get("optional_params.sslmode").unwrap(), "require",
            "YAML dot-notation key should be present"
        );
        assert_eq!(
            m.get("optional_params.timeout").unwrap(), "30",
            "YAML dot-notation key should be present"
        );
    }
}