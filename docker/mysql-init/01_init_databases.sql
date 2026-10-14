-- =====================================================================
-- MySQL Initialization Script for Workshop 2
-- Creates 3 databases: airflow_db, grammy_source, dw
-- Configures users with least privilege
-- =====================================================================

-- Databases
CREATE DATABASE IF NOT EXISTS `airflow_db`
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE DATABASE IF NOT EXISTS `grammy_source`
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE DATABASE IF NOT EXISTS `dw`
  CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Airflow metadata database user
CREATE USER IF NOT EXISTS 'airflow_user'@'%' IDENTIFIED BY 'airflow_pass';
GRANT ALL PRIVILEGES ON `airflow_db`.* TO 'airflow_user'@'%';

-- Grammy source database user (for source prep and raw inspection)
CREATE USER IF NOT EXISTS 'grammy_user'@'%' IDENTIFIED BY 'grammy_pass';
GRANT ALL PRIVILEGES ON `grammy_source`.* TO 'grammy_user'@'%';

-- DW owner user
CREATE USER IF NOT EXISTS 'dw_user'@'%' IDENTIFIED BY 'dw_pass';
GRANT ALL PRIVILEGES ON `dw`.* TO 'dw_user'@'%';

-- ETL pipeline user (reads grammy_source, reads/writes dw)
CREATE USER IF NOT EXISTS 'etl_user'@'%' IDENTIFIED BY 'etl_pass';
GRANT SELECT ON `grammy_source`.* TO 'etl_user'@'%';
GRANT ALL PRIVILEGES ON `dw`.* TO 'etl_user'@'%';

-- BI read-only user (only views/tables in dw)
CREATE USER IF NOT EXISTS 'bi_user'@'%' IDENTIFIED BY 'bi_pass';
GRANT SELECT ON `dw`.* TO 'bi_user'@'%';

FLUSH PRIVILEGES;
