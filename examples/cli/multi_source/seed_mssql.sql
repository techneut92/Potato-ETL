-- Multi-source example: MSSQL side
-- Creates the departments lookup table that is joined with Postgres employees.
--
-- Run:
--   sqlcmd -S localhost -U sa -P 'YourPassword1!' -d etl_demo \
--          -i examples/cli/multi_source/seed_mssql.sql

IF OBJECT_ID('dbo.departments', 'U') IS NOT NULL DROP TABLE dbo.departments;
GO

CREATE TABLE dbo.departments (
    id    INT           IDENTITY(1,1) PRIMARY KEY,
    code  NVARCHAR(10)  NOT NULL,
    name  NVARCHAR(150) NOT NULL,
    head_count_budget INT NOT NULL DEFAULT 0
);

INSERT INTO dbo.departments (code, name, head_count_budget) VALUES
    (N'ENG', N'Engineering',     50),
    (N'MKT', N'Marketing',       15),
    (N'HR',  N'Human Resources', 10),
    (N'FIN', N'Finance',         12),
    (N'OPS', N'Operations',      20);
GO

SELECT N'MSSQL seed complete: ' + CAST(COUNT(*) AS NVARCHAR) + N' departments'
  AS result FROM dbo.departments;
GO
