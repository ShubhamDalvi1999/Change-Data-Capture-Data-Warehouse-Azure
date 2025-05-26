-- Raw Layer DDL Statements
-- Using default Databricks catalog

-- Key Descriptions:
-- Natural Keys (Primary Keys):
--   1. raw_artists: artist_id (STRING) - Unique identifier from Spotify for each artist
--   2. raw_albums: album_id (STRING) - Unique identifier from Spotify for each album
--   3. raw_tracks: song_id (STRING) - Unique identifier from Spotify for each track
-- Note: Raw layer uses only natural keys from source system, no surrogate keys

-- Raw Artists Table
CREATE TABLE IF NOT EXISTS default.raw_artists (
    artist_id STRING,
    artist_name STRING,
    external_url STRING,
    extraction_date TIMESTAMP
) USING DELTA
LOCATION 'abfss://raw@your_storage_account.dfs.core.windows.net/artists';

-- Raw Albums Table
CREATE TABLE IF NOT EXISTS default.raw_albums (
    album_id STRING,
    name STRING,
    release_date STRING,
    total_tracks STRING,
    url STRING,
    artist_id STRING,
    extraction_date STRING
) USING DELTA
LOCATION 'abfss://raw@your_storage_account.dfs.core.windows.net/albums';

-- Raw Tracks Table
CREATE TABLE IF NOT EXISTS default.raw_tracks (
    song_id STRING,
    song_name STRING,
    duration_ms STRING,
    url STRING,
    popularity STRING,
    song_added STRING,
    album_id STRING,
    artist_id STRING,
    extraction_date STRING
) USING DELTA
LOCATION 'abfss://raw@your_storage_account.dfs.core.windows.net/tracks'; 