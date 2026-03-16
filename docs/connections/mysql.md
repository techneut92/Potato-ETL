# MySQL / Aurora / MariaDB

Driver: `mysql` (aliases: `aurora`, `mariadb`)
Default port: `3306`
Feature flag: `mysql`

All three are wire-compatible and use the same driver. Specify `driver: aurora` or `driver: mariadb` as aliases to make your config self-documenting.

## Connection

```yaml
connections:
  my_mysql:
    driver: mysql
    host: db.example.com
    port: 3306                    # optional -- defaults to 3306
    database: mydb
    auth:
      type: user_pass
      username: etl_user
      password: "etl_pass"
    options:                      # optional
      ssl_mode: required
```

## Supported auth types

- `user_pass` -- username + password
- `certificate` -- mTLS with client cert/key and optional CA
- `aws_iam` -- AWS IAM token auth (Aurora/RDS)

## Options

All fields are optional. Omitting `options:` entirely is valid.

```yaml
options:
  ssl_mode: required        # disabled | preferred | required
  connect_timeout: 30       # seconds
  charset: utf8mb4          # character set (default: utf8mb4)
  init_sql:                 # SQL executed on each new pool connection
    - "SET SESSION group_concat_max_len = 1048576"
```

| Option | Default | Description |
|---|---|---|
| `ssl_mode` | server default | SSL mode: `disabled`, `preferred`, `required` |
| `connect_timeout` | driver default | Seconds before a connection attempt times out |
| `charset` | `utf8mb4` | MySQL character set |
| `init_sql` | `[]` | List of SQL statements executed on each new connection in the pool. Use for session-level variables like `group_concat_max_len`, `wait_timeout`, etc. |

## Examples

**Basic connection:**

```yaml
connections:
  mysql:
    driver: mysql
    host: localhost
    database: app_db
    auth:
      type: user_pass
      username: etl_user
      password: "etl_pass"
```

**Aurora with IAM:**

```yaml
connections:
  aurora_db:
    driver: aurora
    host: cluster.eu-west-1.rds.amazonaws.com
    database: mydb
    auth:
      type: aws_iam
      username: "etl_user"
      region: "eu-west-1"
```

**MariaDB with SSL:**

```yaml
connections:
  mariadb:
    driver: mariadb
    host: mariadb.example.com
    database: mydb
    auth:
      type: user_pass
      username: etl_user
      password: "${MYSQL_PASSWORD}"
    options:
      ssl_mode: required
      charset: utf8mb4
```