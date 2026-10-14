-- =====================================================================
-- Grammy Source Setup DDL
-- Database: grammy_source
-- Table: grammy_awards
-- Control: grammy_source_control
--
-- NOTE: This script is NON-DESTRUCTIVE (CREATE TABLE IF NOT EXISTS).
-- Running it against an already-populated database will NOT drop or
-- truncate grammy_awards.  To perform a full reset, use the clearly
-- labelled RESET block at the bottom of this file (commented out by
-- default and must be run deliberately).
-- =====================================================================

USE `grammy_source`;

CREATE TABLE IF NOT EXISTS `grammy_awards` (
    `year` INT NOT NULL,
    `title` VARCHAR(255) NULL,
    `published_at` DATETIME NULL,
    `updated_at` DATETIME NULL,
    `category` VARCHAR(255) NOT NULL,
    `nominee` VARCHAR(500) NULL,
    `artist` VARCHAR(500) NULL,
    `workers` TEXT NULL,
    `img` TEXT NULL,
    `winner` BOOLEAN NOT NULL DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Control table recording source import reconciliation count (feeds DQ07)
CREATE TABLE IF NOT EXISTS `grammy_source_control` (
    `source_name` VARCHAR(64) PRIMARY KEY,
    `expected_rows` INT NOT NULL,
    `imported_rows` INT NOT NULL,
    `reconciled` BOOLEAN NOT NULL DEFAULT 0,
    `imported_at` DATETIME NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =====================================================================
-- RESET BLOCK (DESTRUCTIVE -- uncomment only when a full reload is needed)
-- =====================================================================
-- TRUNCATE TABLE `grammy_awards`;
-- DELETE FROM `grammy_source_control` WHERE source_name = 'the_grammy_awards.csv';
