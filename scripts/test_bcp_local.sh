#!/usr/bin/env bash
# =============================================================================
# test_bcp_local.sh — Manual BCP/ODBC probe for the potato_etl MSSQL sink
#
# Tests every layer that the Rust sink exercises, without needing a build:
#   1. BCP binary presence and version
#   2. ODBC driver presence
#   3. sqlcmd connectivity
#   4. Staging table CREATE
#   5. BCP ingest in character mode (-c -t\t -r\n), with -u for trust_cert
#   6. NULL round-trip (empty field → SQL NULL)
#   7. INSERT…SELECT with NULLIF + CONVERT(VARBINARY) pattern
#   8. Binary 0x<hex> round-trip
#
# Key lesson: bcp 18 uses
#   -S host,port  -u   (trust server cert, three-part db.schema.table name)
# NOT the old DSN approach (-D dsn_name).
# No -d flag — bcp rejects "-d database" when a three-part name is used.
#
# Usage:
#   ./scripts/test_bcp_local.sh [OPTIONS]
#
# Options (all have defaults matching docker-compose.yml):
#   -H HOST        SQL Server host          (default: localhost)
#   -P PORT        SQL Server port          (default: 1433)
#   -d DATABASE    Database name            (default: etl_target)
#   -u USER        Login username           (default: sa)
#   -p PASSWORD    Login password           (default: YourStrong@Passw0rd)
#   -b BCP_BIN     Path to bcp binary       (default: auto-discover)
#   -s SCHEMA      Schema name              (default: dbo)
#   --no-trust     Omit -u (for servers with valid/trusted certs)
#   --keep         Keep staging tables after test (for manual inspection)
# =============================================================================

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
HOST="localhost"
PORT="1433"
DATABASE="etl_target"
USER="sa"
PASS="YourStrong@Passw0rd"
SCHEMA="dbo"
BCP_BIN=""
TRUST_CERT=true
KEEP_TABLE=false

# ── Arg parsing ───────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        -H) HOST="$2";     shift 2 ;;
        -P) PORT="$2";     shift 2 ;;
        -d) DATABASE="$2"; shift 2 ;;
        -u) USER="$2";     shift 2 ;;
        -p) PASS="$2";     shift 2 ;;
        -b) BCP_BIN="$2";  shift 2 ;;
        -s) SCHEMA="$2";   shift 2 ;;
        --no-trust) TRUST_CERT=false; shift ;;
        --keep)     KEEP_TABLE=true;  shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'
ok()   { echo -e "${GREEN}  ✓${RESET} $*"; }
fail() { echo -e "${RED}  ✗${RESET} $*"; }
info() { echo -e "${CYAN}  →${RESET} $*"; }
hdr()  { echo -e "\n${BOLD}${YELLOW}══ $* ══${RESET}"; }

# ── 1. Discover bcp binary ────────────────────────────────────────────────────
hdr "1. BCP binary"
if [[ -z "$BCP_BIN" ]]; then
    for candidate in \
        /opt/mssql-tools18/bin/bcp \
        /opt/mssql-tools/bin/bcp \
        /usr/local/bin/bcp \
        /usr/bin/bcp
    do
        if [[ -x "$candidate" ]]; then
            BCP_BIN="$candidate"
            break
        fi
    done
    if [[ -z "$BCP_BIN" ]]; then
        BCP_BIN=$(command -v bcp 2>/dev/null || true)
    fi
fi

if [[ -z "$BCP_BIN" ]]; then
    fail "bcp not found. Install mssql-tools18:"
    echo "    Ubuntu/Debian: sudo apt install mssql-tools18"
    echo "    RHEL/Fedora:   sudo dnf install mssql-tools18"
    exit 1
fi
ok "bcp binary: $BCP_BIN"

# bcp prints version info to stdout and exits 0 with -v
BCP_VER=$("$BCP_BIN" -v 2>&1 | grep -i "version\|bcp" | head -1 || true)
info "version: ${BCP_VER:-unknown}"

# Detect whether this bcp supports -u (TrustServerCertificate flag).
# All mssql-tools18 releases support it; mssql-tools17 does not.
#
# NOTE: bcp exits non-zero when run with no arguments or unknown flags.
# With `set -o pipefail` a pipeline like `bcp 2>&1 | grep -q` returns bcp's
# exit code even when grep finds the pattern — so the `if` condition would be
# false even though -u IS present.  Capture the output first, then grep.
BCP_USAGE=$("$BCP_BIN" 2>&1 || true)
if echo "$BCP_USAGE" | grep -q -- '-u '; then
    ok "-u flag (TrustServerCertificate) supported"
else
    if [[ "$TRUST_CERT" == "true" ]]; then
        fail "This bcp does not support -u. Trust-cert connections will fail."
        echo "  Install mssql-tools18: sudo apt install mssql-tools18"
        exit 1
    fi
fi

# ── 2. ODBC driver ────────────────────────────────────────────────────────────
hdr "2. ODBC driver"
DRIVER_PATH=""
for glob in \
    "/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18*.so*" \
    "/usr/lib/x86_64-linux-gnu/libmsodbcsql-18*.so*" \
    "/usr/lib/libmsodbcsql-18*.so*"
do
    FOUND=$(ls $glob 2>/dev/null | head -1 || true)
    if [[ -n "$FOUND" ]]; then
        DRIVER_PATH="$FOUND"
        break
    fi
done

if [[ -z "$DRIVER_PATH" ]]; then
    fail "ODBC Driver 18 for SQL Server not found."
    echo "    Ubuntu/Debian: sudo apt install msodbcsql18"
    echo "    RHEL/Fedora:   sudo dnf install msodbcsql18"
    exit 1
fi
ok "ODBC driver: $DRIVER_PATH"

if odbcinst -q -d -n "ODBC Driver 18 for SQL Server" &>/dev/null; then
    ok "ODBC Driver 18 is registered in odbcinst.ini"
else
    fail "ODBC Driver 18 NOT registered. You may need to run:"
    echo "    sudo odbcinst -i -d -f /opt/microsoft/msodbcsql18/etc/odbcinst.ini"
fi

# ── Unique staging / target table names ───────────────────────────────────────
RAND=$(cat /proc/sys/kernel/random/uuid 2>/dev/null | tr -d '-' | head -c 16 \
       || uuidgen 2>/dev/null | tr -d '-' | head -c 16 \
       || printf '%016x' $((RANDOM * RANDOM * RANDOM)))
STAGING_BARE="_etl_bcp_test_probe_${RAND}"
STAGING_FULL="[${SCHEMA}].[${STAGING_BARE}]"
BCP_TABLE="${DATABASE}.${SCHEMA}.${STAGING_BARE}"
TARGET_BARE="_etl_bcp_test_target_${RAND}"
TARGET_FULL="[${SCHEMA}].[${TARGET_BARE}]"

info "Staging : $STAGING_FULL"
info "Target  : $TARGET_FULL"

# ── sqlcmd helper ─────────────────────────────────────────────────────────────
#
# Searches for sqlcmd in (in order):
#   1. Same directory as $BCP_BIN  — mssql-tools18 ships both in the same dir
#   2. /opt/mssql-tools18/bin/     — standard apt/dnf install path
#   3. /opt/mssql-tools/bin/       — older tools17 path
#   4. /usr/local/bin, /usr/bin
#   5. $PATH via command -v
#
# Falls back to a warning only if none of these work.
#
# Trust cert flag: sqlcmd18 uses -C; bcp18 uses -u.  They differ.
sqlcmd_run() {
    local bin=""
    local bcp_dir
    bcp_dir=$(dirname "$BCP_BIN")
    for candidate in \
        "${bcp_dir}/sqlcmd" \
        "/opt/mssql-tools18/bin/sqlcmd" \
        "/opt/mssql-tools/bin/sqlcmd" \
        "/usr/local/bin/sqlcmd" \
        "/usr/bin/sqlcmd"
    do
        if [[ -x "$candidate" ]]; then
            bin="$candidate"
            break
        fi
    done
    if [[ -z "$bin" ]]; then
        bin=$(command -v sqlcmd18 2>/dev/null \
              || command -v sqlcmd  2>/dev/null \
              || true)
    fi
    if [[ -z "$bin" ]]; then
        echo "(sqlcmd not available — skipping SQL-side check)"
        echo "  Add /opt/mssql-tools18/bin to PATH, or:"
        echo "    sudo ln -s /opt/mssql-tools18/bin/sqlcmd /usr/local/bin/sqlcmd"
        return 0
    fi
    local trust_flag=()
    [[ "$TRUST_CERT" == "true" ]] && trust_flag=(-C)
    "$bin" -S "${HOST},${PORT}" -d "$DATABASE" -U "$USER" -P "$PASS" \
        "${trust_flag[@]}" -l 10 -Q "$1" 2>&1
}

# ── 3. sqlcmd connectivity ────────────────────────────────────────────────────
hdr "3. Connectivity (sqlcmd)"
# Discover sqlcmd at top level so we can report which binary we found.
SQLCMD_FOUND=""
_bcp_dir=$(dirname "$BCP_BIN")
for _c in \
    "${_bcp_dir}/sqlcmd" \
    "/opt/mssql-tools18/bin/sqlcmd" \
    "/opt/mssql-tools/bin/sqlcmd" \
    "/usr/local/bin/sqlcmd" \
    "/usr/bin/sqlcmd"
do
    if [[ -x "$_c" ]]; then
        SQLCMD_FOUND="$_c"
        break
    fi
done
if [[ -z "$SQLCMD_FOUND" ]]; then
    SQLCMD_FOUND=$(command -v sqlcmd18 2>/dev/null \
                   || command -v sqlcmd  2>/dev/null \
                   || true)
fi

if [[ -z "$SQLCMD_FOUND" ]]; then
    fail "sqlcmd not found — DDL steps will be skipped and BCP will fail with error 208."
    echo "  Fix: sudo ln -s /opt/mssql-tools18/bin/sqlcmd /usr/local/bin/sqlcmd"
    echo "  Or:  export PATH=\$PATH:/opt/mssql-tools18/bin"
else
    ok "sqlcmd binary: $SQLCMD_FOUND"
    CONN_OUT=$(sqlcmd_run "SELECT 1 AS probe")
    if echo "$CONN_OUT" | grep -q "1"; then
        ok "sqlcmd connectivity: OK"
    else
        fail "sqlcmd connection failed — output:"
        echo "$CONN_OUT" | sed 's/^/    /'
        echo "  Check -H / -P / -u / -p args and that the database exists."
        exit 1
    fi
fi

# Cleanup on exit
cleanup() {
    if [[ "$KEEP_TABLE" != "true" ]]; then
        sqlcmd_run "IF OBJECT_ID('${STAGING_FULL}','U') IS NOT NULL DROP TABLE ${STAGING_FULL}" \
            > /dev/null 2>&1 || true
        sqlcmd_run "IF OBJECT_ID('${TARGET_FULL}','U') IS NOT NULL DROP TABLE ${TARGET_FULL}" \
            > /dev/null 2>&1 || true
        info "Test tables dropped."
    else
        info "Test tables kept (--keep): $STAGING_FULL  /  $TARGET_FULL"
    fi
}
trap cleanup EXIT

# ── 4. Create staging table (all NVARCHAR(MAX)) ───────────────────────────────
hdr "4. Staging table DDL"
DDL="CREATE TABLE ${STAGING_FULL} (
    [col_int]    NVARCHAR(MAX) NULL,
    [col_bigint] NVARCHAR(MAX) NULL,
    [col_float]  NVARCHAR(MAX) NULL,
    [col_bool]   NVARCHAR(MAX) NULL,
    [col_date]   NVARCHAR(MAX) NULL,
    [col_time]   NVARCHAR(MAX) NULL,
    [col_ts]     NVARCHAR(MAX) NULL,
    [col_str]    NVARCHAR(MAX) NULL,
    [col_binary] NVARCHAR(MAX) NULL
)"
info "DDL:\n$(echo "$DDL" | sed 's/^/    /')"
RESULT=$(sqlcmd_run "$DDL")
if echo "$RESULT" | grep -qi "error\|failed\|msg [0-9]"; then
    fail "CREATE TABLE failed:\n$(echo "$RESULT" | sed 's/^/    /')"
    exit 1
fi
ok "Staging table created"

# ── 5. BCP ingest ─────────────────────────────────────────────────────────────
hdr "5. BCP ingest (character mode, -c -t\\t -r\\n)"

# TSV rows matching batch_to_tsv output format exactly:
#   Row 1: all non-NULL typed values
#   Row 2: all NULL (empty fields between TABs)
#   Row 3: edge cases (zeros, epoch, embedded whitespace sanitized to space)
TSV=$(printf '%s\n%s\n%s\n' \
    $'42\t9007199254740992\t3.14159265358979\t1\t2024-06-15\t14:30:59.123456\t2024-06-15 14:30:59.123456\thello world\tdeadbeef' \
    $'\t\t\t\t\t\t\t\t' \
    $'0\t0\t0.0\t0\t1970-01-01\t00:00:00.000000\t1970-01-01 00:00:00.000000\tembedded  space\t00')

info "TSV rows (^I=TAB, \$=EOL):"
echo "$TSV" | cat -A | sed 's/^/    /'

# Build bcp args — mirrors BcpProcess::spawn exactly:
#   bcp database.schema.table in /path -S host,port -U user -P pass -c -t \t -r \n -b N -h TABLOCK [-u]
#
# NOTE: no -d flag  — bcp rejects "-d database" when a three-part name is used.
# NOTE: no "-" path — Microsoft's bcp on Linux does NOT support "-" as stdin;
#       it literally tries open("-", O_RDONLY) → "Unable to open BCP host data-file".
#       The Rust sink uses a temp file (earlier versions used a FIFO, but bcp
#       on Linux mishandles partial pipe reads).  Same approach here.
#
# NOTE: -t '\t' and -r '\n' use single quotes so the shell passes the two-character
#       escape sequence strings to bcp.  bcp interprets \t and \n itself.
#       Passing actual tab (0x09) or newline (0x0A) bytes would break delimiter
#       recognition.  (The Rust sink uses "\\t" / "\\n" in Command::arg for the
#       same reason — bypassing shell quoting requires explicit escape sequences.)
BCP_DATA=$(mktemp)
printf '%s\n' "$TSV" > "$BCP_DATA"
BCP_ARGS=("$BCP_TABLE" "in" "$BCP_DATA"
    "-S" "${HOST},${PORT}"
    "-U" "$USER" "-P" "$PASS"
    "-c" "-t" '\t' "-r" '\n'
    "-b" "1000" "-h" "TABLOCK")

[[ "$TRUST_CERT" == "true" ]] && BCP_ARGS+=("-u")

printf '  → Command: %s %s\n' "$BCP_BIN" "${BCP_ARGS[*]}"
echo ""

BCP_OUT=$("$BCP_BIN" "${BCP_ARGS[@]}" 2>&1) && BCP_EXIT=0 || BCP_EXIT=$?
echo "$BCP_OUT" | sed 's/^/    /'

if [[ "$BCP_EXIT" -ne 0 ]]; then
    fail "bcp exited with code $BCP_EXIT (output above)"
    echo ""
    echo "  Checklist:"
    echo "    Login failed          → check -u USER / -p PASSWORD"
    echo "    SSL/cert error        → add --no-trust if cert is valid, or check -u flag"
    echo "    Table not found (208) → sqlcmd CREATE TABLE failed (step 4)"
    echo "    Column count mismatch → TSV column count != staging table column count"
    exit 1
fi
ok "bcp ingested 3 rows into $STAGING_FULL"

# ── 6. NULL round-trip ────────────────────────────────────────────────────────
hdr "6. NULL round-trip"
NULL_SQL="SELECT COUNT(*) AS null_rows FROM ${STAGING_FULL}
WHERE col_int IS NULL AND col_date IS NULL AND col_binary IS NULL"
NULL_OUT=$(sqlcmd_run "$NULL_SQL")
info "sqlcmd output:\n$(echo "$NULL_OUT" | sed 's/^/    /')"
if echo "$NULL_OUT" | grep -q "[^0-9]1[^0-9]"; then
    ok "NULL row found — empty fields → SQL NULL  ✓"
else
    fail "NULL row not found.  bcp may not be treating empty fields as NULL."
    echo "  Ensure the staging table uses NVARCHAR(MAX) NULL (not NOT NULL)."
fi

# ── 7. INSERT…SELECT into typed target table ──────────────────────────────────
hdr "7. INSERT…SELECT (NULLIF + CONVERT — mirrors insert_select_sql)"
TARGET_DDL="CREATE TABLE ${TARGET_FULL} (
    [col_int]    INT            NULL,
    [col_bigint] BIGINT         NULL,
    [col_float]  FLOAT          NULL,
    [col_bool]   BIT            NULL,
    [col_date]   DATE           NULL,
    [col_time]   TIME(7)        NULL,
    [col_ts]     DATETIME2(6)   NULL,
    [col_str]    NVARCHAR(500)  NULL,
    [col_binary] VARBINARY(MAX) NULL
)"
sqlcmd_run "$TARGET_DDL" > /dev/null 2>&1 || true

INSERT_SQL="INSERT INTO ${TARGET_FULL}
    ([col_int],[col_bigint],[col_float],[col_bool],
     [col_date],[col_time],[col_ts],[col_str],[col_binary])
SELECT
    NULLIF([col_int],    ''),
    NULLIF([col_bigint], ''),
    NULLIF([col_float],  ''),
    NULLIF([col_bool],   ''),
    NULLIF([col_date],   ''),
    NULLIF([col_time],   ''),
    NULLIF([col_ts],     ''),
    NULLIF([col_str],    ''),
    CONVERT(VARBINARY(MAX), NULLIF([col_binary], ''), 2)
FROM ${STAGING_FULL}"

INSERT_OUT=$(sqlcmd_run "$INSERT_SQL")
info "INSERT output:\n$(echo "$INSERT_OUT" | sed 's/^/    /')"

if echo "$INSERT_OUT" | grep -qi "error\|invalid\|conversion\|msg [0-9]"; then
    fail "INSERT…SELECT raised an error (see above)"
    echo "  This usually means a type conversion failed."
    echo "  Re-run with --keep and inspect $STAGING_FULL in sqlcmd."
else
    ok "INSERT…SELECT: 3 rows transferred to typed target  ✓"
fi

# ── 8. Binary hex round-trip ──────────────────────────────────────────────────
hdr "8. Binary hex round-trip (no 0x prefix)"
HEX_SQL="SELECT CONVERT(VARCHAR(MAX), col_binary, 1) AS hex_val
FROM ${TARGET_FULL}
WHERE col_binary IS NOT NULL
ORDER BY hex_val DESC"
HEX_OUT=$(sqlcmd_run "$HEX_SQL")
info "Binary as hex:\n$(echo "$HEX_OUT" | sed 's/^/    /')"

if echo "$HEX_OUT" | grep -qi "DEADBEEF\|deadbeef"; then
    ok "Binary round-trip: 0xDEADBEEF survived CONVERT style 2  ✓"
else
    fail "Binary round-trip: expected 0xDEADBEEF (got: $HEX_OUT)"
    echo "  Check that CONVERT(VARBINARY(MAX), NULLIF(col,''), 2) is in the INSERT…SELECT."
fi

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${GREEN}══ All checks passed ══${RESET}"
echo ""
echo "  BCP binary : $BCP_BIN  ($BCP_VER)"
echo "  Connection : ${HOST},${PORT}  db=${DATABASE}"
echo "  trust_cert : $TRUST_CERT  ($([ "$TRUST_CERT" == true ] && echo '-u passed' || echo 'omitted'))"
echo ""
echo "  If the full Rust pipeline still fails, rebuild and run with:"
echo "    RUST_LOG=debug cargo run ... 2>&1 | grep -A 20 'bcp exited'"
echo "  The stdout/stderr from bcp will now be captured in the error message."
