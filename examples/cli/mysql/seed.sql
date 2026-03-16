-- MySQL / MariaDB / Aurora seed for potato_etl mysql examples
--
-- Run (MySQL):
--   mysql -u etl_user -petl_pass etl_demo < examples/cli/mysql/seed.sql
--
-- Run (MariaDB):
--   mariadb -u etl_user -petl_pass etl_demo < examples/cli/mysql/seed.sql

DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS employees;
DROP TABLE IF EXISTS departments;
DROP TABLE IF EXISTS employees_active;
DROP TABLE IF EXISTS employees_history;

-- ── departments ───────────────────────────────────────────────────────────────

CREATE TABLE departments (
    id    INT          NOT NULL AUTO_INCREMENT PRIMARY KEY,
    code  VARCHAR(10)  NOT NULL,
    name  VARCHAR(150) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO departments (code, name) VALUES
    ('ENG', 'Engineering'),
    ('MKT', 'Marketing'),
    ('HR',  'Human Resources'),
    ('FIN', 'Finance');

-- ── employees ─────────────────────────────────────────────────────────────────

CREATE TABLE employees (
    id            INT            NOT NULL AUTO_INCREMENT PRIMARY KEY,
    department_id INT            NOT NULL,
    name          VARCHAR(200)   NOT NULL,
    email         VARCHAR(255)   NOT NULL UNIQUE,
    status        ENUM('active','inactive','on_leave') NOT NULL DEFAULT 'active',
    salary        DECIMAL(12,2)  NOT NULL,
    hire_date     DATE           NOT NULL,
    last_login    DATETIME,
    is_manager    TINYINT(1)     NOT NULL DEFAULT 0,
    created_at    TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO employees
    (department_id, name, email, status, salary, hire_date, is_manager)
VALUES
    (1, 'Alice de Vries',  'alice@example.com',  'active',   85000.00, '2019-03-15', 1),
    (1, 'Bob Janssen',     'bob@example.com',    'active',   78000.00, '2020-06-01', 0),
    (2, 'Carol Smit',      'carol@example.com',  'active',   65000.00, '2021-01-10', 1),
    (1, 'David van Dam',   'david@example.com',  'inactive', 72000.00, '2018-11-20', 0),
    (3, 'Eva Bakker',      'eva@example.com',    'active',   60000.00, '2022-04-05', 0),
    (1, 'Grace Visser',    'grace@example.com',  'active',   92000.00, '2016-07-22', 1),
    (3, 'Henk de Boer',   'henk@example.com',   'active',   58000.00, '2023-01-30', 0),
    (1, 'Iris Mulder',     'iris@example.com',   'active',   88000.00, '2020-09-14', 0);

-- ── Target tables ─────────────────────────────────────────────────────────────

CREATE TABLE employees_active (
    id            INT,
    name          VARCHAR(200),
    email         VARCHAR(255),
    department_id INT,
    salary        DECIMAL(12,2),
    hire_date     DATE,
    is_manager    TINYINT(1),
    bonus         DECIMAL(12,2)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE employees_history (
    scd_id     BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    id         INT          NOT NULL,
    name       VARCHAR(200),
    salary     DECIMAL(12,2),
    status     VARCHAR(20),
    valid_from DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to   DATETIME,
    is_current TINYINT(1)   NOT NULL DEFAULT 1,
    INDEX idx_emp_hist_key (id, is_current)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SELECT CONCAT('MySQL seed complete: ', COUNT(*), ' employees') AS result
FROM employees;
