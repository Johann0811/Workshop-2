-- =====================================================================
-- Dimensional Data Warehouse Schema (Star Schema)
-- Database: dw
-- Grain: One row of FACT_TRACK_GENRE represents one Spotify track
--        (track_id) listed under one genre (track_genre).
-- =====================================================================

USE `dw`;

-- 1. DIM_TRACK
CREATE TABLE IF NOT EXISTS `DIM_TRACK` (
    `track_key` INT AUTO_INCREMENT PRIMARY KEY,
    `track_id` CHAR(22) NOT NULL UNIQUE,
    `track_name` VARCHAR(600) NULL,
    `album_name` VARCHAR(300) NULL,
    `artists_raw` VARCHAR(1000) NULL,
    `n_artists` TINYINT NOT NULL DEFAULT 1
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. DIM_ARTIST
CREATE TABLE IF NOT EXISTS `DIM_ARTIST` (
    `artist_key` INT AUTO_INCREMENT PRIMARY KEY,
    `artist_key_norm` VARCHAR(150) NOT NULL UNIQUE,
    `artist_name` VARCHAR(150) NOT NULL,
    `grammy_wins_1958_2018` SMALLINT NOT NULL DEFAULT 0,
    `grammy_records_2019` SMALLINT NOT NULL DEFAULT 0,
    `grammy_status` VARCHAR(30) NOT NULL,
    `has_grammy_record` TINYINT NOT NULL DEFAULT 0,
    `wins_band` VARCHAR(20) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 3. DIM_GENRE
CREATE TABLE IF NOT EXISTS `DIM_GENRE` (
    `genre_key` INT AUTO_INCREMENT PRIMARY KEY,
    `track_genre` VARCHAR(50) NOT NULL UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 4. FACT_TRACK_GENRE
CREATE TABLE IF NOT EXISTS `FACT_TRACK_GENRE` (
    `track_key` INT NOT NULL,
    `genre_key` INT NOT NULL,
    `artist_key` INT NOT NULL,
    `popularity` TINYINT NOT NULL,
    `is_top10_global` TINYINT NOT NULL DEFAULT 0,
    `is_zero_traction` TINYINT NOT NULL DEFAULT 0,
    `source_row_id` INT NOT NULL,
    `etl_batch_id` VARCHAR(40) NOT NULL,
    `loaded_at` DATETIME NOT NULL,
    PRIMARY KEY (`track_key`, `genre_key`),
    CONSTRAINT `fk_fact_track` FOREIGN KEY (`track_key`)
        REFERENCES `DIM_TRACK` (`track_key`)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT `fk_fact_genre` FOREIGN KEY (`genre_key`)
        REFERENCES `DIM_GENRE` (`genre_key`)
        ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT `fk_fact_artist` FOREIGN KEY (`artist_key`)
        REFERENCES `DIM_ARTIST` (`artist_key`)
        ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 5. Control and Audit Tables
CREATE TABLE IF NOT EXISTS `etl_control_batch` (
    `batch_id` VARCHAR(40) PRIMARY KEY,
    `started_at` DATETIME NOT NULL,
    `completed_at` DATETIME NULL,
    `raw_spotify_rows` INT NOT NULL DEFAULT 0,
    `exact_copies_removed` INT NOT NULL DEFAULT 0,
    `quarantined_rows` INT NOT NULL DEFAULT 0,
    `candidate_fact_rows` INT NOT NULL DEFAULT 0,
    `fact_rows_loaded` INT NOT NULL DEFAULT 0,
    `dim_track_count` INT NOT NULL DEFAULT 0,
    `dim_artist_count` INT NOT NULL DEFAULT 0,
    `dim_genre_count` INT NOT NULL DEFAULT 0,
    `grammy_imported_rows` INT NOT NULL DEFAULT 0,
    `grammy_extracted_rows` INT NOT NULL DEFAULT 0,
    `grammy_resolvable_rows` INT NOT NULL DEFAULT 0,
    `grammy_matched_rows` INT NOT NULL DEFAULT 0,
    `grammy_match_rate_pct` DECIMAL(5, 2) NOT NULL DEFAULT 0.00,
    `p90_popularity_threshold` TINYINT NOT NULL DEFAULT 0,
    `status` VARCHAR(20) NOT NULL DEFAULT 'RUNNING'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `quarantine_spotify` (
    `quarantine_id` INT AUTO_INCREMENT PRIMARY KEY,
    `etl_batch_id` VARCHAR(40) NOT NULL,
    `source_row_id` INT NULL,
    `track_id` VARCHAR(30) NULL,
    `reason` VARCHAR(50) NOT NULL,
    `raw_data` TEXT NULL,
    `quarantined_at` DATETIME NOT NULL,
    UNIQUE KEY `uq_quarantine_record` (`source_row_id`, `reason`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
