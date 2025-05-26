-- Gold Layer DDL Statements
-- Using default Databricks catalog

-- Key Descriptions:
-- Dimension Tables - Keys:
--   1. dim_artists:
--      - Primary Key: artist_key (STRING)
--      - Natural Key: artist_id (STRING)
--      - Purpose: Handles historical changes in artist attributes (SCD Type 2)
--
--   2. dim_albums:
--      - Primary Key: album_key (STRING)
--      - Natural Key: album_id (STRING)
--      - Foreign Key: artist_id (references dim_artists)
--      - Purpose: Handles historical changes in album attributes (SCD Type 2)
--
--   3. dim_tracks:
--      - Primary Key: track_key (STRING)
--      - Natural Key: track_id (STRING)
--      - Foreign Keys: album_id, artist_id
--      - Purpose: Handles historical changes in track attributes (SCD Type 2)
--
--   4. dim_dates:
--      - Primary Key: date_key (INTEGER)
--      - Natural Key: calendar_date (DATE)
--      - Purpose: Date dimension for time-based analysis

-- Dimension Tables

-- Artist Dimension
CREATE TABLE IF NOT EXISTS default.dim_artists (
    artist_key STRING,
    artist_id STRING,
    artist_name STRING,
    external_url STRING,
    is_current BOOLEAN,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    hash_key STRING,
    source_system STRING,
    last_updated TIMESTAMP
) USING DELTA
LOCATION 'abfss://warehouse@your_storage_account.dfs.core.windows.net/dim_artists';

-- Album Dimension
CREATE TABLE IF NOT EXISTS default.dim_albums (
    album_key STRING,
    album_id STRING,
    album_name STRING,
    release_date TIMESTAMP,
    total_tracks INT,
    url STRING,
    artist_id STRING,
    is_current BOOLEAN,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    hash_key STRING,
    source_system STRING,
    last_updated TIMESTAMP
) USING DELTA
LOCATION 'abfss://warehouse@your_storage_account.dfs.core.windows.net/dim_albums';

-- Track Dimension
CREATE TABLE IF NOT EXISTS default.dim_tracks (
    track_key STRING,
    track_id STRING,
    track_name STRING,
    duration_ms INT,
    url STRING,
    popularity INT,
    song_added STRING,
    album_id STRING,
    artist_id STRING,
    is_current BOOLEAN,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    source_system STRING,
    last_updated TIMESTAMP
) USING DELTA
LOCATION 'abfss://warehouse@your_storage_account.dfs.core.windows.net/dim_tracks';

-- Date Dimension
CREATE TABLE IF NOT EXISTS default.dim_dates (
    calendar_date DATE,
    date_key INT,
    year INT,
    month INT,
    day_of_month INT,
    day_of_week INT,
    week_of_year INT,
    quarter INT
) USING DELTA
LOCATION 'abfss://warehouse@your_storage_account.dfs.core.windows.net/dim_dates';

-- Fact Tables

-- Track Plays Fact Table
CREATE TABLE IF NOT EXISTS default.fact_track_plays (
    track_key STRING,
    artist_key STRING,
    album_key STRING,
    popularity INT,
    date_key INT,
    play_count INT,
    last_updated TIMESTAMP
) USING DELTA
LOCATION 'abfss://warehouse@your_storage_account.dfs.core.windows.net/fact_track_plays'; 