-- PostgreSQL seed for potato_etl postgres examples
-- Showcases a wide range of Postgres-native types so the Arrow type-mapping
-- and LogicalType middleware can be exercised end-to-end.
--
-- Run:
--   psql -U etl_user -d etl_demo -f examples/cli/postgres/seed.sql
--
-- Or with cargo run:
--   cargo run -p potato-etl-cli --features postgres -- validate \
--     --config examples/cli/postgres/01_copy.yaml

-- ── Schema ────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS orders         CASCADE;
DROP TABLE IF EXISTS employees      CASCADE;
DROP TABLE IF EXISTS departments    CASCADE;
DROP TABLE IF EXISTS type_showcase  CASCADE;

-- ── departments ───────────────────────────────────────────────────────────────

CREATE TABLE departments (
    id          SERIAL       PRIMARY KEY,
    code        VARCHAR(10)  NOT NULL UNIQUE,
    name        VARCHAR(150) NOT NULL
);

INSERT INTO departments (code, name) VALUES
    ('ENG',  'Engineering'),
    ('MKT',  'Marketing'),
    ('HR',   'Human Resources'),
    ('FIN',  'Finance'),
    ('OPS',  'Operations');

-- ── employees ─────────────────────────────────────────────────────────────────

CREATE TABLE employees (
    id              SERIAL          PRIMARY KEY,
    department_id   INT             NOT NULL REFERENCES departments(id),
    name            TEXT            NOT NULL,
    email           TEXT            NOT NULL UNIQUE,
    status          TEXT            NOT NULL DEFAULT 'active'
                                    CHECK (status IN ('active', 'inactive', 'on_leave')),
    salary          NUMERIC(12, 2)  NOT NULL,
    hire_date       DATE            NOT NULL,
    last_login      TIMESTAMPTZ,
    is_manager      BOOLEAN         NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT now()
);

INSERT INTO employees
    (department_id, name, email, status, salary, hire_date, last_login, is_manager)
VALUES
    (1, 'Alice de Vries',  'alice@example.com',  'active',   85000.00, '2019-03-15', now() - interval '1 day',    true),
    (1, 'Bob Janssen',     'bob@example.com',    'active',   78000.00, '2020-06-01', now() - interval '3 days',   false),
    (2, 'Carol Smit',      'carol@example.com',  'active',   65000.00, '2021-01-10', now() - interval '2 days',   true),
    (1, 'David van Dam',   'david@example.com',  'inactive', 72000.00, '2018-11-20', NULL,                        false),
    (3, 'Eva Bakker',      'eva@example.com',    'active',   60000.00, '2022-04-05', now() - interval '1 hour',   false),
    (2, 'Frank Peters',    'frank@example.com',  'inactive', 55000.00, '2017-09-12', NULL,                        false),
    (1, 'Grace Visser',    'grace@example.com',  'active',   92000.00, '2016-07-22', now() - interval '5 hours',  true),
    (3, 'Henk de Boer',   'henk@example.com',   'active',   58000.00, '2023-01-30', now() - interval '30 mins',  false),
    (1, 'Iris Mulder',     'iris@example.com',   'active',   88000.00, '2020-09-14', now() - interval '2 hours',  false),
    (2, 'Jan Vermeer',     'jan@example.com',    'on_leave', 67000.00, '2019-12-01', now() - interval '7 days',   false),
    (4, 'Karen Linden',    'karen@example.com',  'active',   95000.00, '2015-05-03', now() - interval '4 hours',  true),
    (5, 'Lars Fontijn',    'lars@example.com',   'active',   71000.00, '2021-08-18', now() - interval '6 days',   false);

-- ── orders ────────────────────────────────────────────────────────────────────

CREATE TABLE orders (
    id              SERIAL          PRIMARY KEY,
    employee_id     INT             NOT NULL REFERENCES employees(id),
    amount          NUMERIC(10, 2)  NOT NULL,
    currency        CHAR(3)         NOT NULL DEFAULT 'EUR',
    status          TEXT            NOT NULL DEFAULT 'pending'
                                    CHECK (status IN ('pending', 'paid', 'cancelled')),
    placed_at       TIMESTAMPTZ     NOT NULL DEFAULT now(),
    notes           TEXT
);

INSERT INTO orders (employee_id, amount, currency, status, placed_at, notes) VALUES
    (1,  1250.00, 'EUR', 'paid',      now() - interval '10 days', 'Office supplies'),
    (1,   340.50, 'EUR', 'paid',      now() - interval '5 days',  NULL),
    (3,  4999.99, 'USD', 'pending',   now() - interval '2 days',  'Laptop upgrade'),
    (5,   125.00, 'EUR', 'cancelled', now() - interval '8 days',  'Duplicate order'),
    (7,  8750.00, 'EUR', 'paid',      now() - interval '1 day',   'Conference sponsorship'),
    (9,  2200.00, 'GBP', 'pending',   now() - interval '3 days',  NULL),
    (11, 5500.00, 'EUR', 'paid',      now() - interval '6 days',  'Team training');

-- ── type_showcase ─────────────────────────────────────────────────────────────
-- One row per interesting Postgres type.  This table is the subject of
-- 04_types_showcase.yaml — verifying that Arrow mappings round-trip correctly.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE type_showcase (
    -- Identifiers
    id              SERIAL              PRIMARY KEY,
    uid             UUID                NOT NULL DEFAULT uuid_generate_v4(),

    -- Text family
    short_code      CHAR(5),
    label           VARCHAR(100),
    description     TEXT,

    -- Numeric family
    qty             INTEGER,
    big_count       BIGINT,
    price           NUMERIC(12, 4),
    rate            REAL,
    factor          DOUBLE PRECISION,

    -- Boolean
    is_active       BOOLEAN             NOT NULL DEFAULT true,

    -- Date / time
    event_date      DATE,
    event_time      TIME,
    event_ts        TIMESTAMP,
    event_tstz      TIMESTAMPTZ,

    -- Semi-structured
    tags            TEXT[],                          -- text array
    metadata        JSONB,                           -- binary JSON

    -- Network types (map to Utf8 via LogicalType::ip / macaddr)
    ip_addr         INET,
    mac_addr        MACADDR,

    -- Binary
    fingerprint     BYTEA
);

INSERT INTO type_showcase
    (short_code, label, description, qty, big_count, price, rate, factor,
     is_active, event_date, event_time, event_ts, event_tstz,
     tags, metadata, ip_addr, mac_addr, fingerprint)
VALUES
    ('PG001', 'Alpha record', 'Full type coverage row',
     42, 9999999999, 3.14159, 2.718::REAL, 1.41421356237,
     true,
     '2024-06-15', '14:30:00', '2024-06-15 14:30:00', '2024-06-15 14:30:00+02',
     ARRAY['etl', 'test', 'postgres'],
     '{"env": "demo", "version": 3, "tags": ["a", "b"]}'::jsonb,
     '192.168.1.100'::INET, '08:00:27:ab:cd:ef'::MACADDR,
     '\xDEADBEEF'::BYTEA),

    ('PG002', 'Beta record', 'Row with several NULLs',
     NULL, 0, 0.00, 0.0::REAL, 0.0,
     false,
     '2023-01-01', NULL, NULL, '2023-01-01 00:00:00+00',
     ARRAY['null-test'],
     NULL,
     NULL, NULL,
     NULL),

    ('PG003', 'Gamma record', 'Negative and boundary values',
     -1, -9223372036854775808, 99999999.9999, 3.4028235e+38::REAL, 1.7976931348623157e+308,
     true,
     '1970-01-01', '00:00:00', '1970-01-01 00:00:00', '1970-01-01 00:00:00+00',
     ARRAY[]::TEXT[],
     '{"nested": {"deep": {"value": 42}}}'::jsonb,
     '10.0.0.1'::INET, 'ff:ff:ff:ff:ff:ff'::MACADDR,
     '\x00'::BYTEA);

-- ── Target tables (written to by the pipeline examples) ───────────────────────

DROP TABLE IF EXISTS employees_active    CASCADE;
DROP TABLE IF EXISTS employees_history   CASCADE;
DROP TABLE IF EXISTS dept_salary_summary CASCADE;

CREATE TABLE employees_active (
    id            INT,
    name          TEXT,
    email         TEXT,
    department_id INT,
    salary        NUMERIC(12, 2),
    hire_date     DATE,
    is_manager    BOOLEAN,
    bonus         NUMERIC(12, 2)
);

-- SCD2 history table — created by 03_scd2.yaml (create_table: if_not_exists),
-- but defined here for reference so the column layout is visible.
CREATE TABLE employees_history (
    scd_id      BIGSERIAL       PRIMARY KEY,
    id          INT             NOT NULL,
    name        TEXT,
    email       TEXT,
    salary      NUMERIC(12, 2),
    status      TEXT,
    valid_from  TIMESTAMPTZ     NOT NULL DEFAULT now(),
    valid_to    TIMESTAMPTZ,
    is_current  BOOLEAN         NOT NULL DEFAULT true
);
CREATE INDEX idx_emp_hist_key ON employees_history (id, is_current);

CREATE TABLE dept_salary_summary (
    department_id   INT,
    headcount       BIGINT,
    avg_salary      DOUBLE PRECISION,
    min_salary      DOUBLE PRECISION,
    max_salary      DOUBLE PRECISION,
    total_payroll   DOUBLE PRECISION
);

SELECT 'Seed complete: ' || count(*) || ' employees' AS result FROM employees;
