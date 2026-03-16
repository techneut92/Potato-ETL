-- MSSQL seed for potato_etl mssql examples
--
-- Run:
--   sqlcmd -S localhost -U sa -P 'YourPassword1!' -d etl_demo \
--          -i examples/cli/mssql/seed.sql
--
-- Or with Docker:
--   docker exec -i mssql_container \
--     /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'YourPassword1!' \
--     -d etl_demo -i /examples/cli/mssql/seed.sql

IF OBJECT_ID('dbo.orders',              'U') IS NOT NULL DROP TABLE dbo.orders;
IF OBJECT_ID('dbo.employees',           'U') IS NOT NULL DROP TABLE dbo.employees;
IF OBJECT_ID('dbo.departments',         'U') IS NOT NULL DROP TABLE dbo.departments;
IF OBJECT_ID('dbo.employees_active',    'U') IS NOT NULL DROP TABLE dbo.employees_active;
IF OBJECT_ID('dbo.employees_history',   'U') IS NOT NULL DROP TABLE dbo.employees_history;
IF OBJECT_ID('dbo.dept_salary_summary', 'U') IS NOT NULL DROP TABLE dbo.dept_salary_summary;
GO

-- ── departments ───────────────────────────────────────────────────────────────

CREATE TABLE dbo.departments (
    id    INT           IDENTITY(1,1) PRIMARY KEY,
    code  NVARCHAR(10)  NOT NULL,
    name  NVARCHAR(150) NOT NULL
);

INSERT INTO dbo.departments (code, name) VALUES
    (N'ENG',  N'Engineering'),
    (N'MKT',  N'Marketing'),
    (N'HR',   N'Human Resources'),
    (N'FIN',  N'Finance'),
    (N'OPS',  N'Operations');
GO

-- ── employees ─────────────────────────────────────────────────────────────────

CREATE TABLE dbo.employees (
    id            INT            IDENTITY(1,1) PRIMARY KEY,
    department_id INT            NOT NULL,
    name          NVARCHAR(200)  NOT NULL,
    email         NVARCHAR(255)  NOT NULL,
    status        NVARCHAR(20)   NOT NULL DEFAULT 'active',
    salary        FLOAT          NOT NULL,
    hire_date     DATE           NOT NULL,
    last_login    DATETIMEOFFSET NULL,
    is_manager    BIT            NOT NULL DEFAULT 0,
    created_at    DATETIME2      NOT NULL DEFAULT SYSDATETIME()
);

INSERT INTO dbo.employees
    (department_id, name, email, status, salary, hire_date, last_login, is_manager)
VALUES
    (1, N'Alice de Vries',  N'alice@example.com',  N'active',   85000, '2019-03-15', SYSDATETIMEOFFSET(), 1),
    (1, N'Bob Janssen',     N'bob@example.com',    N'active',   78000, '2020-06-01', SYSDATETIMEOFFSET(), 0),
    (2, N'Carol Smit',      N'carol@example.com',  N'active',   65000, '2021-01-10', SYSDATETIMEOFFSET(), 1),
    (1, N'David van Dam',   N'david@example.com',  N'inactive', 72000, '2018-11-20', NULL,                0),
    (3, N'Eva Bakker',      N'eva@example.com',    N'active',   60000, '2022-04-05', SYSDATETIMEOFFSET(), 0),
    (1, N'Grace Visser',    N'grace@example.com',  N'active',   92000, '2016-07-22', SYSDATETIMEOFFSET(), 1),
    (3, N'Henk de Boer',   N'henk@example.com',   N'active',   58000, '2023-01-30', SYSDATETIMEOFFSET(), 0),
    (1, N'Iris Mulder',     N'iris@example.com',   N'active',   88000, '2020-09-14', SYSDATETIMEOFFSET(), 0),
    (2, N'Jan Vermeer',     N'jan@example.com',    N'inactive', 67000, '2019-12-01', NULL,                0);
GO

-- ── orders ────────────────────────────────────────────────────────────────────

CREATE TABLE dbo.orders (
    id          INT           IDENTITY(1,1) PRIMARY KEY,
    employee_id INT           NOT NULL,
    amount      DECIMAL(10,2) NOT NULL,
    currency    CHAR(3)       NOT NULL DEFAULT 'EUR',
    status      NVARCHAR(20)  NOT NULL DEFAULT 'pending',
    placed_at   DATETIME2     NOT NULL DEFAULT SYSDATETIME(),
    notes       NVARCHAR(MAX) NULL
);

INSERT INTO dbo.orders (employee_id, amount, currency, status, notes) VALUES
    (1, 1250.00, 'EUR', 'paid',      N'Office supplies'),
    (3, 4999.99, 'USD', 'pending',   N'Laptop upgrade'),
    (5,  125.00, 'EUR', 'cancelled', N'Duplicate order'),
    (7, 8750.00, 'EUR', 'paid',      N'Conference sponsorship');
GO

-- ── Target tables ─────────────────────────────────────────────────────────────

CREATE TABLE dbo.employees_active (
    id            INT,
    name          NVARCHAR(200),
    email         NVARCHAR(255),
    department_id INT,
    salary        FLOAT,
    hire_date     DATE,
    is_manager    BIT,
    bonus         FLOAT
);

-- SCD2 history — is_current as BIT (MSSQL has no BOOLEAN before 2022 compat)
CREATE TABLE dbo.employees_history (
    scd_id     BIGINT         IDENTITY(1,1) PRIMARY KEY,
    id         INT            NOT NULL,
    name       NVARCHAR(200),
    email      NVARCHAR(255),
    salary     FLOAT,
    status     NVARCHAR(20),
    valid_from DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET(),
    valid_to   DATETIMEOFFSET NULL,
    is_current BIT            NOT NULL DEFAULT 1
);
CREATE INDEX idx_emp_hist_key ON dbo.employees_history (id, is_current);

CREATE TABLE dbo.dept_salary_summary (
    department_id INT,
    headcount     BIGINT,
    avg_salary    FLOAT,
    min_salary    FLOAT,
    max_salary    FLOAT,
    total_payroll FLOAT
);
GO

SELECT N'MSSQL seed complete: ' + CAST(COUNT(*) AS NVARCHAR) + N' employees'
  AS result FROM dbo.employees;
GO
