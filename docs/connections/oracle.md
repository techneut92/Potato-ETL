# Oracle Database

Driver: `oracle`
Default port: `1521`
Feature flag: `oracle`

## Connection modes

Oracle supports three connection modes: host + service name, host + SID, and TNS.

### Host + service name (preferred for Oracle 12c+)

```yaml
connections:
  oracle_db:
    driver: oracle
    host: oracle.example.com
    port: 1521                    # optional -- defaults to 1521
    service: XEPDB1
    auth:
      type: user_pass
      username: hr
      password: "hr_s3cr3t"
```

### Host + SID (legacy Oracle instances)

```yaml
connections:
  oracle_legacy:
    driver: oracle
    host: oracle.example.com
    port: 1521
    sid: XE
    auth:
      type: user_pass
      username: hr
      password: "hr_s3cr3t"
```

### TNS (alias or full descriptor)

```yaml
connections:
  oracle_tns:
    driver: oracle
    tns: XEPDB1_PROD              # TNS alias from tnsnames.ora
    auth:
      type: user_pass
      username: hr
      password: "hr_s3cr3t"
```

## Supported auth types

- `user_pass` -- username + password
- `kerberos` -- Kerberos with principal + optional keytab
- `certificate` -- mTLS with client cert/key and optional CA

## Options

Oracle has no connection-level options at this time. Tuning is done via the TNS descriptor or at the step level using [step driver options](../steps/driver-options.md).

## Examples

**Service name with environment variable password:**

```yaml
connections:
  oracle_prod:
    driver: oracle
    host: oracle-prod.example.com
    service: PRODDB
    auth:
      type: user_pass
      username: etl_svc
      password: "${ORACLE_PASSWORD}"
```

**Kerberos auth:**

```yaml
connections:
  oracle_krb:
    driver: oracle
    host: oracle.corp.local
    service: CORPDB
    auth:
      type: kerberos
      principal: "etl_svc@CORP.LOCAL"
      keytab: "/etc/krb5.keytab"
```
