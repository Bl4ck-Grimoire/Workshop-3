-- Read-only user for Power BI (Docker runs this file once, when the database is created).
CREATE USER IF NOT EXISTS 'powerbi'@'%' IDENTIFIED BY 'powerbi';
GRANT SELECT ON happiness.* TO 'powerbi'@'%';
