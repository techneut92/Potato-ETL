-- Multi-source example: Postgres side
-- Creates the employees source table and the enriched output target.
--
-- Run:
--   psql -U etl_user -d etl_demo -f examples/cli/multi_source/seed_pg.sql

DROP TABLE IF EXISTS employees         CASCADE;
DROP TABLE IF EXISTS employees_enriched CASCADE;

CREATE TABLE employees (
    id            SERIAL          PRIMARY KEY,
    department_id INT             NOT NULL,
    name          TEXT            NOT NULL,
    email         TEXT            NOT NULL UNIQUE,
    status        TEXT            NOT NULL DEFAULT 'active',
    salary        NUMERIC(12, 2)  NOT NULL,
    hire_date     DATE            NOT NULL,
    is_manager    BOOLEAN         NOT NULL DEFAULT false
);

INSERT INTO employees
    (department_id, name, email, status, salary, hire_date, is_manager)
VALUES
    (1, 'Alice de Vries',  'alice@example.com',  'active',   85000, '2019-03-15', true),
    (1, 'Bob Janssen',     'bob@example.com',    'active',   78000, '2020-06-01', false),
    (2, 'Carol Smit',      'carol@example.com',  'active',   65000, '2021-01-10', true),
    (1, 'David van Dam',   'david@example.com',  'inactive', 72000, '2018-11-20', false),
    (3, 'Eva Bakker',      'eva@example.com',    'active',   60000, '2022-04-05', false),
    (1, 'Grace Visser',    'grace@example.com',  'active',   92000, '2016-07-22', true),
    (3, 'Henk de Boer',   'henk@example.com',   'active',   58000, '2023-01-30', false),
    (1, 'Iris Mulder',     'iris@example.com',   'active',   88000, '2020-09-14', false);

CREATE TABLE employees_enriched (
    id            INT,
    name          TEXT,
    email         TEXT,
    department_id INT,
    department_name TEXT,
    salary        NUMERIC(12, 2),
    bonus         NUMERIC(12, 2),
    is_manager    BOOLEAN
);

SELECT 'PG seed complete: ' || count(*) || ' employees' AS result FROM employees;
