-- Silver Layer DDL Statements
-- Using default Databricks catalog

-- Key Descriptions:
-- Natural Keys (Primary Keys):
--   1. silver_artists: artist_id (STRING) - Natural key from source + hash_key for change tracking
--   2. silver_albums: album_id (STRING) - Natural key from source + hash_key for change tracking
--   3. silver_tracks: track_id (STRING) - Natural key from source
-- Note: Silver layer maintains natural keys and adds hash_key for change detection
--       No surrogate keys are used at this layer, but includes SCD Type 2 tracking fields
--       (valid_from, valid_to, is_current) for artists and albums

-- Silver Artists Table
CREATE TABLE IF NOT EXISTS default.silver_artists (
    artist_id STRING,
    artist_name STRING,
    external_url STRING,
    extraction_date TIMESTAMP,
    source_file STRING,
    hash_key STRING,
    is_current BOOLEAN,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP
) USING DELTA
LOCATION 'abfss://transformed@your_storage_account.dfs.core.windows.net/artists';

-- Silver Albums Table
CREATE TABLE IF NOT EXISTS default.silver_albums (
    album_id STRING,
    name STRING,
    release_date TIMESTAMP,
    total_tracks INT,
    url STRING,
    artist_id STRING,
    extraction_date TIMESTAMP,
    source_file STRING,
    hash_key STRING,
    is_current BOOLEAN,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP
) USING DELTA
LOCATION 'abfss://transformed@your_storage_account.dfs.core.windows.net/albums';

-- Silver Tracks Table
CREATE TABLE IF NOT EXISTS default.silver_tracks (
    track_id STRING,
    name STRING,
    duration_ms INT,
    url STRING,
    popularity INT,
    song_added STRING,
    album_id STRING,
    artist_id STRING,
    extraction_date TIMESTAMP,
    source_file STRING
) USING DELTA
LOCATION 'abfss://transformed@your_storage_account.dfs.core.windows.net/tracks'; 