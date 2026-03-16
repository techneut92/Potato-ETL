//! Secret resolution for YAML/JSON value trees.
//!
//! Walks a `serde_yaml_ng::Value` tree, finds all `secret::*` string references,
//! batch-fetches them from the appropriate providers, and substitutes the
//! resolved values back into the tree.
//!
//! ## Resolution modes
//!
//! 1. **Field reference** (`secret::vault/prod/pg#password`):
//!    The string is replaced with the value of the `password` field from the
//!    secret at path `prod/pg`.
//!
//! 2. **Whole-value reference** (`secret::vault/prod/pg_connection`):
//!    - If the secret is a JSON object, the string node is replaced with the
//!      parsed YAML mapping (expanding into the parent structure).
//!    - If the secret is a plain string, the string node is replaced with
//!      that string value.

use std::collections::{HashMap, HashSet};

use serde_yaml_ng::Value;

use super::refs::{SecretRef, SecretProviderKind, parse_secret_ref, is_secret_ref};
use super::providers::SecretProvider;
use crate::config::secrets::SecretsConfig;

use super::providers::hashicorp::HashiCorpVaultProvider;
use super::providers::azure::AzureKeyVaultProvider;
use super::providers::gcp::GcpSecretManagerProvider;

/// Resolves all `secret::*` references in a YAML value tree.
///
/// This is the main entry point for secret resolution.  It:
/// 1. Scans the value tree for `secret::*` strings
/// 2. Builds providers from the `SecretsConfig`
/// 3. Batch-fetches all unique secret paths (deduplicated per provider)
/// 4. Substitutes resolved values back into the tree
///
/// Returns the modified value tree with all secrets resolved.
pub async fn resolve_secrets_in_value(
    mut value: Value,
    secrets_config: &SecretsConfig,
) -> anyhow::Result<Value> {
    // 1. Collect all secret references from the value tree.
    let refs = collect_secret_refs(&value);
    if refs.is_empty() {
        return Ok(value);
    }

    tracing::info!(
        count = refs.len(),
        "Resolving secret references"
    );

    // 2. Build providers for each referenced provider kind.
    let needed_providers: HashSet<SecretProviderKind> = refs.iter()
        .map(|r| r.provider.clone())
        .collect();

    let mut providers: HashMap<SecretProviderKind, Box<dyn SecretProvider>> = HashMap::new();

    for kind in &needed_providers {
        let provider: Box<dyn SecretProvider> = match kind {
            SecretProviderKind::HashiCorpVault => {
                let config = secrets_config.vault.as_ref().ok_or_else(|| anyhow::anyhow!(
                    "Secret references target HashiCorp Vault but no `secrets.vault` \
                     config block is defined. Add a `secrets:` section to your pipeline YAML."
                ))?;
                Box::new(HashiCorpVaultProvider::from_config(config)?)
            }
            SecretProviderKind::AzureKeyVault => {
                let config = secrets_config.azure.as_ref().ok_or_else(|| anyhow::anyhow!(
                    "Secret references target Azure Key Vault but no `secrets.azure` \
                     config block is defined. Add a `secrets:` section to your pipeline YAML."
                ))?;
                Box::new(AzureKeyVaultProvider::from_config(config)?)
            }
            SecretProviderKind::GoogleSecretManager => {
                let config = secrets_config.gcp.as_ref().ok_or_else(|| anyhow::anyhow!(
                    "Secret references target Google Secret Manager but no `secrets.gcp` \
                     config block is defined. Add a `secrets:` section to your pipeline YAML."
                ))?;
                Box::new(GcpSecretManagerProvider::from_config(config)?)
            }
        };
        providers.insert(kind.clone(), provider);
    }

    // 3. Deduplicate and batch-fetch secrets.
    //    Group by (provider, path) to avoid fetching the same secret twice.
    let mut unique_paths: HashMap<(SecretProviderKind, String), HashMap<String, String>> =
        HashMap::new();

    for secret_ref in &refs {
        let key = (secret_ref.provider.clone(), secret_ref.path.clone());
        if unique_paths.contains_key(&key) {
            continue; // Already fetched or will be fetched.
        }
        // Placeholder — will be filled below.
        unique_paths.insert(key, HashMap::new());
    }

    // Fetch all unique secrets.
    for ((provider_kind, path), result_map) in unique_paths.iter_mut() {
        let provider = providers.get(provider_kind).unwrap();
        tracing::debug!(
            provider = provider.provider_name(),
            path = %path,
            "secrets: fetching secret"
        );
        match provider.get_secret(path).await {
            Ok(data) => {
                tracing::debug!(
                    provider = provider.provider_name(),
                    path = %path,
                    fields = data.len(),
                    field_keys = ?data.keys().collect::<Vec<_>>(),
                    "secrets: fetched secret successfully"
                );
                *result_map = data;
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "Failed to resolve secret from {} at path '{}': {}",
                    provider.provider_name(), path, e
                ));
            }
        }
    }

    // 4. Build a resolution map: full ref string → resolved value.
    let mut resolution: HashMap<String, ResolvedValue> = HashMap::new();

    for secret_ref in &refs {
        let key = (secret_ref.provider.clone(), secret_ref.path.clone());
        let data = unique_paths.get(&key).unwrap();

        let resolved = if let Some(field) = &secret_ref.field {
            // Field reference: extract specific field from the secret.
            tracing::debug!(
                secret = %secret_ref,
                field = %field,
                available_fields = ?data.keys().collect::<Vec<_>>(),
                "secrets: resolving field from secret"
            );
            let val = data.get(field).ok_or_else(|| {
                let available: Vec<&str> = data.keys().map(|k| k.as_str()).collect();
                anyhow::anyhow!(
                    "Secret '{}' at path '{}' does not contain field '{}'. \
                     Available fields: {:?}",
                    secret_ref, secret_ref.path, field, available
                )
            })?;
            ResolvedValue::String(val.clone())
        } else {
            // Whole-value reference.
            if data.len() == 1 && data.contains_key("") {
                // Single-value secret — use the value directly.
                let val = data.get("").unwrap();
                // Try to parse as YAML/JSON to expand structured secrets.
                if let Ok(parsed) = serde_yaml_ng::from_str::<Value>(val) {
                    if parsed.is_mapping() {
                        ResolvedValue::Mapping(parsed)
                    } else {
                        ResolvedValue::String(val.clone())
                    }
                } else {
                    ResolvedValue::String(val.clone())
                }
            } else {
                // Multi-field secret — reconstruct as a YAML mapping.
                let mut mapping = serde_yaml_ng::Mapping::new();
                for (k, v) in data {
                    mapping.insert(
                        Value::String(k.clone()),
                        Value::String(v.clone()),
                    );
                }
                ResolvedValue::Mapping(Value::Mapping(mapping))
            }
        };

        resolution.insert(secret_ref.to_string(), resolved);
    }

    // 5. Walk the tree and substitute.
    substitute_secrets(&mut value, &resolution);

    tracing::info!(
        resolved = resolution.len(),
        "Secret references resolved"
    );

    Ok(value)
}

/// A resolved secret value — either a plain string or a structured mapping.
enum ResolvedValue {
    /// Replace the string node with this string value.
    String(String),
    /// Replace the string node with this YAML mapping (for whole-connection secrets).
    Mapping(Value),
}

/// Recursively collects all `SecretRef`s from a YAML value tree.
fn collect_secret_refs(value: &Value) -> Vec<SecretRef> {
    let mut refs = Vec::new();
    collect_refs_recursive(value, &mut refs);
    refs
}

fn collect_refs_recursive(value: &Value, refs: &mut Vec<SecretRef>) {
    match value {
        Value::String(s) => {
            if is_secret_ref(s) {
                if let Some(r) = parse_secret_ref(s) {
                    refs.push(r);
                }
            }
        }
        Value::Mapping(map) => {
            for (_, v) in map {
                collect_refs_recursive(v, refs);
            }
        }
        Value::Sequence(seq) => {
            for v in seq {
                collect_refs_recursive(v, refs);
            }
        }
        _ => {}
    }
}

/// Recursively substitutes resolved secret values into the YAML tree.
fn substitute_secrets(value: &mut Value, resolution: &HashMap<String, ResolvedValue>) {
    match value {
        Value::String(s) => {
            if is_secret_ref(s) {
                if let Some(parsed) = parse_secret_ref(s) {
                    let ref_str = parsed.to_string();
                    if let Some(resolved) = resolution.get(&ref_str) {
                        match resolved {
                            ResolvedValue::String(val) => {
                                *value = coerce_yaml_scalar(val);
                            }
                            ResolvedValue::Mapping(mapping) => {
                                *value = mapping.clone();
                            }
                        }
                    }
                }
            }
        }
        Value::Mapping(map) => {
            // Collect keys that need mapping expansion (whole-connection refs).
            let keys_to_expand: Vec<Value> = map.iter()
                .filter_map(|(k, v)| {
                    if let Value::String(s) = v {
                        if is_secret_ref(s) {
                            if let Some(parsed) = parse_secret_ref(s) {
                                if let Some(ResolvedValue::Mapping(_)) = resolution.get(&parsed.to_string()) {
                                    return Some(k.clone());
                                }
                            }
                        }
                    }
                    None
                })
                .collect();

            // Expand mapping values.
            for key in keys_to_expand {
                if let Some(Value::String(s)) = map.get(&key) {
                    if let Some(parsed) = parse_secret_ref(s) {
                        if let Some(ResolvedValue::Mapping(mapping)) = resolution.get(&parsed.to_string()) {
                            map.insert(key, mapping.clone());
                        }
                    }
                }
            }

            // Recurse into remaining values.
            for (_, v) in map.iter_mut() {
                substitute_secrets(v, resolution);
            }
        }
        Value::Sequence(seq) => {
            for v in seq.iter_mut() {
                substitute_secrets(v, resolution);
            }
        }
        _ => {}
    }
}

/// Coerces a resolved secret string into the most appropriate YAML scalar.
///
/// This is critical for secrets that resolve to non-string types (numbers,
/// booleans) — without coercion, `port: "secret::azure/...#port"` would
/// resolve to `Value::String("5432")` which `serde` cannot deserialize
/// into `u16`.
///
/// Resolution order:
/// 1. Boolean literals (`true`, `false`, case-insensitive)
/// 2. Integer (parsed as `i64`)
/// 3. Float (parsed as `f64`, only if it contains `.` to avoid int→float)
/// 4. Fall back to string
fn coerce_yaml_scalar(s: &str) -> Value {
    // ── Boolean ──────────────────────────────────────────────────────────────
    match s.to_lowercase().as_str() {
        "true"  => return Value::Bool(true),
        "false" => return Value::Bool(false),
        _ => {}
    }

    // ── Integer ──────────────────────────────────────────────────────────────
    if let Ok(n) = s.parse::<i64>() {
        return Value::Number(n.into());
    }

    // ── Float (only if it looks like a float, not a plain integer) ────────
    if s.contains('.') {
        if let Ok(f) = s.parse::<f64>() {
            return Value::Number(serde_yaml_ng::Number::from(f));
        }
    }

    // ── String fallback ──────────────────────────────────────────────────────
    Value::String(s.to_string())
}

/// Extracts the `secrets:` config from a raw YAML value tree.
///
/// This is called before full deserialization so we can build providers
/// and resolve secret references before the typed config is constructed.
pub fn extract_secrets_config(value: &Value) -> anyhow::Result<SecretsConfig> {
    match value {
        Value::Mapping(map) => {
            if let Some(secrets_val) = map.get(&Value::String("secrets".into())) {
                serde_yaml_ng::from_value(secrets_val.clone())
                    .map_err(|e| anyhow::anyhow!("Failed to parse `secrets:` config: {e}"))
            } else {
                Ok(SecretsConfig::default())
            }
        }
        _ => Ok(SecretsConfig::default()),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collect_refs_from_yaml() {
        let yaml = r#"
connections:
  pg:
    driver: postgres
    host: localhost
    auth:
      type: user_pass
      username: "secret::vault/prod/pg#username"
      password: "secret::vault/prod/pg#password"
  redis:
    host: "secret::azure/my-vault/redis-host"
"#;
        let value: Value = serde_yaml_ng::from_str(yaml).unwrap();
        let refs = collect_secret_refs(&value);

        assert_eq!(refs.len(), 3);
        assert_eq!(refs[0].path, "prod/pg");
        assert_eq!(refs[0].field.as_deref(), Some("username"));
        assert_eq!(refs[1].path, "prod/pg");
        assert_eq!(refs[1].field.as_deref(), Some("password"));
        assert_eq!(refs[2].path, "my-vault/redis-host");
    }

    #[test]
    fn substitute_string_refs() {
        let mut value: Value = serde_yaml_ng::from_str(r#"
password: "secret::vault/db#password"
host: plain-host
"#).unwrap();

        let mut resolution = HashMap::new();
        resolution.insert(
            "secret::vault/db#password".to_string(),
            ResolvedValue::String("s3cret!".to_string()),
        );

        substitute_secrets(&mut value, &resolution);

        let map = value.as_mapping().unwrap();
        let pw = map.get(&Value::String("password".into())).unwrap();
        assert_eq!(pw.as_str().unwrap(), "s3cret!");

        let host = map.get(&Value::String("host".into())).unwrap();
        assert_eq!(host.as_str().unwrap(), "plain-host");
    }

    #[test]
    fn substitute_mapping_ref() {
        let mut value: Value = serde_yaml_ng::from_str(r#"
connections:
  pg: "secret::vault/prod/pg_conn"
"#).unwrap();

        let mut inner_mapping = serde_yaml_ng::Mapping::new();
        inner_mapping.insert(Value::String("driver".into()), Value::String("postgres".into()));
        inner_mapping.insert(Value::String("host".into()), Value::String("db.internal".into()));

        let mut resolution = HashMap::new();
        resolution.insert(
            "secret::vault/prod/pg_conn".to_string(),
            ResolvedValue::Mapping(Value::Mapping(inner_mapping)),
        );

        substitute_secrets(&mut value, &resolution);

        let conns = value.as_mapping().unwrap()
            .get(&Value::String("connections".into())).unwrap()
            .as_mapping().unwrap();
        let pg = conns.get(&Value::String("pg".into())).unwrap();
        let pg_map = pg.as_mapping().unwrap();
        assert_eq!(
            pg_map.get(&Value::String("driver".into())).unwrap().as_str().unwrap(),
            "postgres"
        );
    }

    #[test]
    fn extract_empty_secrets_config() {
        let value: Value = serde_yaml_ng::from_str("connections: {}").unwrap();
        let config = extract_secrets_config(&value).unwrap();
        assert!(config.is_empty());
    }

    #[test]
    fn extract_vault_secrets_config() {
        let yaml = r#"
secrets:
  vault:
    address: https://vault.test:8200
    auth:
      method: token
      token: test-token
    mount: secret
"#;
        let value: Value = serde_yaml_ng::from_str(yaml).unwrap();
        let config = extract_secrets_config(&value).unwrap();
        assert!(config.vault.is_some());
        assert_eq!(config.vault.unwrap().address.unwrap(), "https://vault.test:8200");
    }

    // ── coerce_yaml_scalar tests ─────────────────────────────────────────────

    #[test]
    fn coerce_integer() {
        match coerce_yaml_scalar("5432") {
            Value::Number(n) => assert_eq!(n.as_u64(), Some(5432)),
            other => panic!("expected Number, got {other:?}"),
        }
    }

    #[test]
    fn coerce_negative_integer() {
        match coerce_yaml_scalar("-42") {
            Value::Number(n) => assert_eq!(n.as_i64(), Some(-42)),
            other => panic!("expected Number, got {other:?}"),
        }
    }

    #[test]
    fn coerce_float() {
        match coerce_yaml_scalar("3.14") {
            Value::Number(n) => assert!((n.as_f64().unwrap() - 3.14).abs() < f64::EPSILON),
            other => panic!("expected Number, got {other:?}"),
        }
    }

    #[test]
    fn coerce_bool_true() {
        assert_eq!(coerce_yaml_scalar("true"), Value::Bool(true));
        assert_eq!(coerce_yaml_scalar("True"), Value::Bool(true));
        assert_eq!(coerce_yaml_scalar("TRUE"), Value::Bool(true));
    }

    #[test]
    fn coerce_bool_false() {
        assert_eq!(coerce_yaml_scalar("false"), Value::Bool(false));
    }

    #[test]
    fn coerce_string_fallback() {
        // A password like "s3cret!" must remain a string.
        match coerce_yaml_scalar("s3cret!") {
            Value::String(s) => assert_eq!(s, "s3cret!"),
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn coerce_string_that_looks_numeric_but_isnt() {
        // Port ranges, version strings etc. should stay strings.
        match coerce_yaml_scalar("5432-5433") {
            Value::String(s) => assert_eq!(s, "5432-5433"),
            other => panic!("expected String, got {other:?}"),
        }
    }

    #[test]
    fn substitute_coerces_port_to_number() {
        let mut value: Value = serde_yaml_ng::from_str(r#"
port: "secret::vault/db#port"
host: "secret::vault/db#host"
"#).unwrap();

        let mut resolution = HashMap::new();
        resolution.insert(
            "secret::vault/db#port".to_string(),
            ResolvedValue::String("5432".to_string()),
        );
        resolution.insert(
            "secret::vault/db#host".to_string(),
            ResolvedValue::String("db.internal".to_string()),
        );

        substitute_secrets(&mut value, &resolution);

        let map = value.as_mapping().unwrap();
        let port = map.get(&Value::String("port".into())).unwrap();
        // Port should be a number, not a string.
        assert_eq!(port.as_u64(), Some(5432), "port should be coerced to u64");

        let host = map.get(&Value::String("host".into())).unwrap();
        assert_eq!(host.as_str().unwrap(), "db.internal");
    }
}