-- =====================================================================
-- KPI Views on the Dimensional Data Warehouse
-- Database: dw
-- Views support R1-R3 / K1-K6
-- All view names in UPPERCASE
-- =====================================================================

USE `dw`;

-- Base view joining Fact and Dimensions
CREATE OR REPLACE VIEW `VW_KPI_BASE` AS
SELECT
    g.track_genre,
    f.popularity,
    f.is_top10_global,
    f.is_zero_traction,
    a.artist_key,
    a.artist_key_norm,
    a.artist_name,
    a.grammy_status,
    a.grammy_wins_1958_2018,
    a.grammy_records_2019,
    a.wins_band,
    a.has_grammy_record,
    CASE
        WHEN a.grammy_wins_1958_2018 >= 1 THEN 'with_award'
        WHEN a.grammy_status = 'no record' THEN 'no_record'
        ELSE 'only_2019'
    END AS r2_group
FROM `FACT_TRACK_GENRE` f
JOIN `DIM_ARTIST` a ON a.artist_key = f.artist_key
JOIN `DIM_GENRE`  g ON g.genre_key  = f.genre_key;

-- ---------------------------------------------------------------------
-- K1: Percentage of tracks by artists with a Grammy record, by genre (R1)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `VW_KPI_1_GENRE_CONCENTRATION` AS
SELECT
    track_genre,
    COUNT(*) AS total_tracks,
    SUM(has_grammy_record) AS tracks_with_grammy_record,
    ROUND(AVG(has_grammy_record) * 100, 2) AS k1_pct_with_grammy_record
FROM `VW_KPI_BASE`
GROUP BY track_genre
ORDER BY k1_pct_with_grammy_record DESC, total_tracks DESC;

-- ---------------------------------------------------------------------
-- Helper: Ranked Popularity for Median Computation in MySQL 8
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `VW_KPI_POPULARITY_RANKED` AS
SELECT
    r2_group,
    wins_band,
    track_genre,
    popularity,
    is_top10_global,
    is_zero_traction,
    has_grammy_record,
    ROW_NUMBER() OVER (PARTITION BY r2_group ORDER BY popularity) AS row_num_r2,
    COUNT(*) OVER (PARTITION BY r2_group) AS total_count_r2,
    ROW_NUMBER() OVER (PARTITION BY wins_band ORDER BY popularity) AS row_num_band,
    COUNT(*) OVER (PARTITION BY wins_band) AS total_count_band
FROM `VW_KPI_BASE`;

-- ---------------------------------------------------------------------
-- K2: Mean and Median Popularity: with_award vs no_record (R2)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `VW_KPI_2_POPULARITY_COMPARISON` AS
WITH stats AS (
    SELECT
        r2_group,
        COUNT(*) AS total_tracks,
        ROUND(AVG(popularity), 2) AS mean_popularity
    FROM `VW_KPI_BASE`
    WHERE r2_group IN ('with_award', 'no_record')
    GROUP BY r2_group
),
medians AS (
    SELECT
        r2_group,
        ROUND(AVG(popularity), 2) AS median_popularity
    FROM `VW_KPI_POPULARITY_RANKED`
    WHERE r2_group IN ('with_award', 'no_record')
      AND row_num_r2 IN (FLOOR((total_count_r2 + 1) / 2), CEIL((total_count_r2 + 1) / 2))
    GROUP BY r2_group
)
SELECT
    s.r2_group,
    s.total_tracks,
    s.mean_popularity,
    m.median_popularity
FROM stats s
JOIN medians m ON s.r2_group = m.r2_group;

-- ---------------------------------------------------------------------
-- K3: Top 10% share by group and ratio (with_award / no_record) (R2)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `VW_KPI_3_TOP10_RATIO` AS
WITH group_top10 AS (
    SELECT
        r2_group,
        COUNT(*) AS total_tracks,
        SUM(is_top10_global) AS top10_tracks,
        ROUND(AVG(is_top10_global) * 100, 2) AS pct_top10
    FROM `VW_KPI_BASE`
    WHERE r2_group IN ('with_award', 'no_record')
    GROUP BY r2_group
)
SELECT
    g.r2_group,
    g.total_tracks,
    g.top10_tracks,
    g.pct_top10,
    ROUND(g.pct_top10 / NULLIF((SELECT pct_top10 FROM group_top10 WHERE r2_group = 'no_record'), 0), 2) AS ratio_vs_no_record
FROM group_top10 g;

-- ---------------------------------------------------------------------
-- K4: Popularity and Top 10% by award band with sample status (R2)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `VW_KPI_4_AWARD_BANDS` AS
WITH band_stats AS (
    SELECT
        wins_band,
        COUNT(DISTINCT artist_key) AS distinct_artists,
        COUNT(*) AS total_tracks,
        ROUND(AVG(popularity), 2) AS mean_popularity,
        ROUND(AVG(is_top10_global) * 100, 2) AS pct_top10
    FROM `VW_KPI_BASE`
    WHERE r2_group = 'with_award'
    GROUP BY wins_band
),
band_medians AS (
    SELECT
        wins_band,
        ROUND(AVG(popularity), 2) AS median_popularity
    FROM `VW_KPI_POPULARITY_RANKED`
    WHERE wins_band IN ('1', '2-4', '5-9', '10+')
      AND row_num_band IN (FLOOR((total_count_band + 1) / 2), CEIL((total_count_band + 1) / 2))
    GROUP BY wins_band
)
SELECT
    b.wins_band,
    b.distinct_artists,
    b.total_tracks,
    b.mean_popularity,
    m.median_popularity,
    b.pct_top10,
    CASE
        WHEN b.distinct_artists >= 30 THEN 'ok'
        WHEN b.distinct_artists > 0 THEN 'small sample'
        ELSE 'no data'
    END AS sample_status
FROM band_stats b
LEFT JOIN band_medians m ON b.wins_band = m.wins_band
ORDER BY FIELD(b.wins_band, '1', '2-4', '5-9', '10+');

-- ---------------------------------------------------------------------
-- K5: Zero traction percentage: Grammy record vs rest (R3)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `VW_KPI_5_ZERO_TRACTION` AS
SELECT
    CASE WHEN has_grammy_record = 1 THEN 'artists_with_grammy' ELSE 'rest_of_catalog' END AS catalog_segment,
    has_grammy_record,
    COUNT(*) AS total_tracks,
    SUM(is_zero_traction) AS zero_traction_tracks,
    ROUND(AVG(is_zero_traction) * 100, 2) AS k5_pct_zero_traction
FROM `VW_KPI_BASE`
GROUP BY has_grammy_record
ORDER BY has_grammy_record DESC;

-- ---------------------------------------------------------------------
-- K6: Zero traction by genre for artists with a Grammy record (R3)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW `VW_KPI_6_ZERO_TRACTION_BY_GENRE` AS
SELECT
    track_genre,
    COUNT(*) AS total_genre_tracks,
    SUM(has_grammy_record) AS grammy_artist_tracks,
    SUM(CASE WHEN has_grammy_record = 1 AND is_zero_traction = 1 THEN 1 ELSE 0 END) AS zero_traction_grammy_tracks,
    ROUND(
        SUM(CASE WHEN has_grammy_record = 1 AND is_zero_traction = 1 THEN 1 ELSE 0 END) * 100.0 /
        NULLIF(SUM(has_grammy_record), 0),
        2
    ) AS k6_pct_zero_traction,
    CASE
        WHEN SUM(has_grammy_record) >= 30 THEN 'ok'
        WHEN SUM(has_grammy_record) > 0 THEN 'small sample'
        ELSE 'no data'
    END AS sample_status
FROM `VW_KPI_BASE`
GROUP BY track_genre
ORDER BY k6_pct_zero_traction DESC, grammy_artist_tracks DESC;
