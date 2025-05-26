# Databricks notebook source
# MAGIC %md
# MAGIC ### IMPORTS

# COMMAND ----------

!pip install spotipy

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, ArrayType, BooleanType, TimestampType
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import time
import re
from datetime import datetime
import pyspark.sql.functions as F
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from spotipy.exceptions import SpotifyException

# COMMAND ----------

# MAGIC %md
# MAGIC ### Utility Functions for spotify client , getting data to process

# COMMAND ----------

def _get_storage_paths():
    """Get storage paths for Spotify ETL pipeline using mounted ADLS storage"""
    base_path = "/mnt/spotify"
    return {
        "raw_data": f"{base_path}/raw",
        "toprocess_data": f"{base_path}/toprocess",
        "processed_data": f"{base_path}/processed",
        "transformed_data": f"{base_path}/transformed",
        "warehouse": f"{base_path}/data_warehouse",
        "quality_reports": f"{base_path}/qualityreports"
    }

# COMMAND ----------

def _initialize_spotify_client():
    """
    Initialize Spotify API client with credentials
    
    Returns:
        Initialized Spotify client
    """
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials
    
    
    # Try to get credentials from Databricks secrets
    try:
        client_id = dbutils.secrets.get(scope="datascope", key="spotifyclientid")
        client_secret = dbutils.secrets.get(scope="datascope", key="spotifyclientsecret")
    except Exception as e:
        # Fallback to hardcoded values for testing or development
        print(f"[ERROR] Could not retrieve secrets: {str(e)}")
        print("Using fallback credentials for development")
    
    # Initialize Spotify client
    auth_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    return spotipy.Spotify(auth_manager=auth_manager)

# COMMAND ----------

def _get_files_to_process(spark, entity_type):
    """
    Get list of files to process from the toprocess folder.

    Args:
        spark: SparkSession
        entity_type: Type of entity to process (artists, albums, tracks)

    Returns:
        List of file paths to process
    """
    storage_paths = _get_storage_paths()
    folder_name = "songs" if entity_type == "tracks" else entity_type
    base_path = f"{storage_paths['toprocess_data']}/{folder_name}"

    try:
        # ⚙️ Handle tracks (songs) with special logic for latest repartitioned parquet folder
        if entity_type == "tracks":
            all_folders = dbutils.fs.ls(base_path)
            pattern = re.compile(r"repartitioned_(\d{8}_\d{6})")
            parquet_folders = []

            for folder in all_folders:
                match = pattern.search(folder.name)
                if match:
                    folder_path = folder.path
                    file_list = dbutils.fs.ls(folder_path)
                    parquet_files = [f.path for f in file_list if f.path.endswith(".parquet")]
                    if parquet_files:
                        timestamp = datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
                        parquet_folders.append((timestamp, folder_path, parquet_files))

            if not parquet_folders:
                print(f"[ERROR] No valid 'repartitioned_' folders with parquet files found in: {base_path}")
                return []

            # Sort by timestamp descending and return parquet files from the newest folder
            parquet_folders.sort(reverse=True)
            latest_ts, latest_folder_path, latest_files = parquet_folders[0]
            print(f"[INFO] Using latest folder for tracks: {latest_folder_path}")
            return latest_files

        # ✅ Handle albums and artists normally
        else:
            files = dbutils.fs.ls(base_path)
            return [f.path for f in files if f.path.endswith(".json") or f.path.endswith(".parquet") or f.path.endswith(".csv")]

    except Exception as e:
        print(f"[ERROR] Error listing files in {base_path}: {str(e)}")
        return []


# COMMAND ----------

track_files = _get_files_to_process(spark, "tracks")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation logic

# COMMAND ----------

# MAGIC %md
# MAGIC #### ARTIST

# COMMAND ----------

def check_artist_schema(spark, path=None):
    """
    Utility function to check the schema of artist data.
    
    Args:
        spark: SparkSession
        path: Optional path to artist data. If None, uses default path.
        
    Returns:
        DataFrame schema
    """
    if path is None:
        storage_paths = _get_storage_paths()
        path = f"{storage_paths['toprocess_data']}/artists"
    
    try:
        # List all CSV/JSON/Parquet files
        files = [f for f in dbutils.fs.ls(path) if f.path.endswith((".csv", ".json", ".parquet"))]
        
        if not files:
            print(f"No data files found in {path}")
            return None
            
        # Read first file to check schema
        sample_file = files[0].path
        
        if sample_file.endswith(".csv"):
            df = spark.read.csv(sample_file, header=True, inferSchema=True)
        elif sample_file.endswith(".json"):
            df = spark.read.json(sample_file)
        else:
            df = spark.read.parquet(sample_file)
            
        # Print schema details
        print(f"Schema for artist data in {sample_file}:")
        df.printSchema()
        
        # Print column names and available columns
        columns = df.columns
        print(f"Available columns: {', '.join(columns)}")
        
        # Check for critical columns
        critical_columns = ["artist_id", "artist_name"]
        missing = [col for col in critical_columns if col not in columns]
        if missing:
            print(f"WARNING: Missing critical columns: {', '.join(missing)}")
        
        return df.schema
    except Exception as e:
        print(f"Error checking artist schema: {str(e)}")
        return None

# COMMAND ----------

check_artist_schema(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Processing Artists

# COMMAND ----------

def _transform_artist_data(spark):
    """
    Transform raw artist data into a processed format with SCD Type 2 patterns.
    
    Args:
        spark: SparkSession
        
    Returns:
        Path to transformed artists data
    """
    print("Transforming artist data")
    
    # Get storage paths
    storage_paths = _get_storage_paths()
    toprocess_path = storage_paths["toprocess_data"]
    transformed_path = f"{storage_paths['transformed_data']}/artists"
    
    # Get files to process
    artist_files = _get_files_to_process(spark, "artists")
    
    if not artist_files:
        print("No artist files found to process")
        return None
    
    # Define artist schema - updated to match actual data
    artist_schema = StructType([
        StructField("artist_id", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("external_url", StringType(), True),
        StructField("extraction_date", TimestampType(), True)
    ])
    
    # Read and combine all artist files
    artist_dfs = []
    
    for file_path in artist_files:
        try:
            if file_path.endswith(".json"):
                df = spark.read.json(file_path, schema=artist_schema)
            elif file_path.endswith(".csv"):
                df = spark.read.csv(file_path, header=True, inferSchema=True)
                # Convert string extraction_date to timestamp if needed
                if "extraction_date" in df.columns and df.schema["extraction_date"].dataType != TimestampType():
                    df = df.withColumn("extraction_date", F.to_timestamp("extraction_date"))
            else:
                df = spark.read.parquet(file_path)
            
            # Add source file column
            df = df.withColumn("source_file", F.lit(file_path))
            artist_dfs.append(df)
        except Exception as e:
            print(f"[WARNING] Error reading {file_path}: {str(e)}")
    
    if not artist_dfs:
        print("No valid artist data found")
        return None
    
    # Combine all dataframes
    artists_df = artist_dfs[0]
    for df in artist_dfs[1:]:
        artists_df = artists_df.unionByName(df, allowMissingColumns=True)
    
    # Deduplicate artists by taking most recent
    artists_df = artists_df.withColumn(
        "row_num", 
        F.row_number().over(
            Window.partitionBy("artist_id").orderBy(F.desc("extraction_date"))
        )
    )
    
    artists_df = artists_df.filter(F.col("row_num") == 1).drop("row_num")
    
    # Add CDC-like columns for tracking changes - updated to use only available columns
    artists_df = artists_df.withColumn(
        "hash_key", 
        F.sha2(
            F.concat(
                F.col("artist_id"),
                F.col("artist_name"),
                F.coalesce(F.col("external_url"), F.lit(""))
            ), 
            256
        )
    ).withColumn(
        "is_current", 
        F.lit(True)
    ).withColumn(
        "valid_from", 
        F.current_timestamp()
    ).withColumn(
        "valid_to", 
        F.lit(None).cast("timestamp")
    )
    
    # Check if target exists to implement SCD Type 2
    try:
        if DeltaTable.isDeltaTable(spark, transformed_path):
            # Target exists, perform merge
            target_delta = DeltaTable.forPath(spark, transformed_path)
            
            # First, expire current records that will be updated
            target_delta.alias("target").merge(
                artists_df.alias("source"),
                "target.artist_id = source.artist_id AND target.is_current = true AND target.hash_key <> source.hash_key"
            ).whenMatchedUpdate(
                set={
                    "is_current": "false",
                    "valid_to": "current_timestamp()"
                }
            ).execute()
            
            # Only insert records that don't exist or have changed
            artists_df.createOrReplaceTempView("staged_artists")
            existing_df = target_delta.toDF()
            existing_df.createOrReplaceTempView("existing_artists")
            
            new_artists_df = spark.sql("""
            SELECT s.* 
            FROM staged_artists s 
            LEFT JOIN existing_artists e 
                ON s.artist_id = e.artist_id AND e.is_current = true
            WHERE e.artist_id IS NULL OR e.hash_key <> s.hash_key
            """)
            
            # Write new records
            count_new = new_artists_df.count()
            if count_new > 0:
                new_artists_df.write.format("delta").mode("append").save(transformed_path)
                print(f"[SUCCESS] Added {count_new} new/updated artist records")
            else:
                print("[INFO] No artist updates required")
        else:
            # Target doesn't exist, create it
            count_created = artists_df.count()
            artists_df.write.format("delta").mode("overwrite").save(transformed_path)
            print(f"[SUCCESS] Created new artists table with {count_created} records")
    except Exception as e:
        print(f"Error transforming artists: {str(e)}")
        #traceback.print_exc()
        return None
    
    return transformed_path

# COMMAND ----------

# MAGIC %md
# MAGIC #### ALBUM

# COMMAND ----------

def check_album_schema(spark, path=None):
    """
    Utility function to check the schema of album data from the latest CSV file.
    
    Args:
        spark: SparkSession
        path: Optional path to album data. If None, uses default path.
        
    Returns:
        DataFrame schema
    """
    if path is None:
        storage_paths = _get_storage_paths()
        path = f"{storage_paths['toprocess_data']}/albums"

    try:
        # List only CSV files
        csv_files = [f for f in dbutils.fs.ls(path) if f.path.endswith(".csv")]

        if not csv_files:
            print(f" No CSV files found in {path}")
            return None

        # Sort files by last modified time (descending)
        csv_files.sort(key=lambda x: x.modificationTime, reverse=True)

        print(" Found CSV files (latest first):")
        for f in csv_files:
            print(f" - {f.path} (last modified: {f.modificationTime})")

        # Use the latest file
        latest_file = csv_files[0].path
        print(f"\n Using latest CSV file: {latest_file}")

        # Read the latest file
        df = spark.read.csv(latest_file, header=True, inferSchema=True)

        # Print schema
        print("Schema:")
        df.printSchema()

        # List columns
        columns = df.columns
        print(f"Available columns: {', '.join(columns)}")

        # Check for critical columns
        critical_columns = ["album_id", "name", "artist_id"]
        missing = [col for col in critical_columns if col not in columns]
        if missing:
            print(f"WARNING: Missing critical columns: {', '.join(missing)}")

        return df.schema

    except Exception as e:
        print(f"Error checking album schema: {str(e)}")
        return None


# COMMAND ----------

check_album_schema(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Processing Albums

# COMMAND ----------

def _transform_album_data(spark):
    """
    Transform raw album data into a processed format with SCD Type 2 patterns.
    
    Args:
        spark: SparkSession
        
    Returns:
        Path to transformed albums data
    """
    print("Transforming album data")
    
    # Get storage paths
    storage_paths = _get_storage_paths()
    toprocess_path = storage_paths["toprocess_data"]
    transformed_path = f"{storage_paths['transformed_data']}/albums"
    
    # Get files to process
    album_files = _get_files_to_process(spark, "albums")
    
    if not album_files:
        print("No album files found to process")
        return None
    
    # Define album schema - updated release_date to TimestampType
    album_schema = StructType([
        StructField("album_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("release_date", TimestampType(), True),
        StructField("total_tracks", IntegerType(), True),
        StructField("url", StringType(), True),
        StructField("artist_id", StringType(), True),
        StructField("extraction_date", TimestampType(), True)
    ])
    
    # Read and combine all album files
    album_dfs = []
    
    for file_path in album_files:
        try:
            if file_path.endswith(".json"):
                df = spark.read.json(file_path, schema=album_schema)
            elif file_path.endswith(".csv"):
                df = spark.read.csv(file_path, header=True, inferSchema=True)
                # Convert string extraction_date to timestamp if needed
                if "release_date" in df.columns and df.schema["release_date"].dataType != TimestampType():
                    df = df.withColumn("release_date", F.to_timestamp("release_date"))
                if "extraction_date" in df.columns and df.schema["extraction_date"].dataType != TimestampType():
                    df = df.withColumn("extraction_date", F.to_timestamp("extraction_date"))
            else:
                df = spark.read.parquet(file_path)
            
            # Add source file column
            df = df.withColumn("source_file", F.lit(file_path))
            album_dfs.append(df)
        except Exception as e:
            print(f"[WARNING] Error reading {file_path}: {str(e)}")
    
    if not album_dfs:
        print("No valid album data found")
        return None
    
    # Combine all dataframes
    albums_df = album_dfs[0]
    for df in album_dfs[1:]:
        albums_df = albums_df.unionByName(df, allowMissingColumns=True)
    
    #Enforcing schema.. making sure its is correct :
    albums_df = albums_df.select(
        F.col("album_id").cast(StringType()).alias("album_id"),
        F.col("name").cast(StringType()).alias("name"),
        F.col("release_date").cast(TimestampType()).alias("release_date"),
        F.col("total_tracks").cast(IntegerType()).alias("total_tracks"),
        F.col("url").cast(StringType()).alias("url"),
        F.col("artist_id").cast(StringType()).alias("artist_id"),
        F.col("extraction_date").cast(TimestampType()).alias("extraction_date"),
        F.col("source_file")
    )

    # Deduplicate albums by taking most recent
    albums_df = albums_df.withColumn(
        "row_num", 
        F.row_number().over(
            Window.partitionBy("album_id").orderBy(F.desc("extraction_date"))
        )
    )
    
    albums_df = albums_df.filter(F.col("row_num") == 1).drop("row_num")
    
    # Add CDC-like columns for tracking changes
    albums_df = albums_df.withColumn(
        "hash_key", 
        F.sha2(
            F.concat(
                F.col("album_id"),
                F.col("name"),
                F.coalesce(F.col("release_date"), F.lit("")),
                F.coalesce(F.col("total_tracks"), F.lit(-1))
            ), 
            256
        )
    ).withColumn(
        "is_current", 
        F.lit(True)
    ).withColumn(
        "valid_from", 
        F.current_timestamp()
    ).withColumn(
        "valid_to", 
        F.lit(None).cast("timestamp")
    )
    
    
    # Check if target exists to implement SCD Type 2
    try:
        if DeltaTable.isDeltaTable(spark, transformed_path):
            # Target exists, perform merge
            target_delta = DeltaTable.forPath(spark, transformed_path)
            
            # First, expire current records that will be updated
            target_delta.alias("target").merge(
                albums_df.alias("source"),
                "target.album_id = source.album_id AND target.is_current = true AND target.hash_key <> source.hash_key"
            ).whenMatchedUpdate(
                set={
                    "is_current": "false",
                    "valid_to": "current_timestamp()"
                }
            ).execute()
            
            # Only insert records that don't exist or have changed
            albums_df.createOrReplaceTempView("staged_albums")
            existing_df = target_delta.toDF()
            existing_df.createOrReplaceTempView("existing_albums")
            
            new_albums_df = spark.sql("""
            SELECT s.* 
            FROM staged_albums s 
            LEFT JOIN existing_albums e 
                ON s.album_id = e.album_id AND e.is_current = true
            WHERE e.album_id IS NULL OR e.hash_key <> s.hash_key
            """)
            
            # Write new records with accurate counts
            count_new = new_albums_df.count()
            if count_new > 0:
                new_albums_df.write.format("delta").mode("append").save(transformed_path)
                print(f"[SUCCESS] Added {count_new} new/updated album records")
            else:
                print("[INFO] No album updates required")
        else:
            # Target doesn't exist, create it
            count_created = albums_df.count()
            albums_df.write.format("delta").mode("overwrite").save(transformed_path)
            print(f"[SUCCESS] Created new albums table with {count_created} records")
    except Exception as e:
        print(f"Error transforming albums: {str(e)}")
        #traceback.print_exc()
        return None
    
    return transformed_path

# COMMAND ----------

# MAGIC %md
# MAGIC #### TRACKS

# COMMAND ----------

import re
from datetime import datetime

def check_track_schema(spark, path=None):
    """
    Check schema from latest repartitioned parquet folder in /songs.
    Only considers folders with parquet files.

    Args:
        spark: SparkSession
        path: Optional full path to a specific folder.

    Returns:
        DataFrame schema if found, else None
    """
    try:
        if path is None:
            storage_paths = _get_storage_paths()
            base_path = f"{storage_paths['toprocess_data']}/songs"

            # Step 1: List folders starting with 'repartitioned_'
            all_folders = dbutils.fs.ls(base_path)
            pattern = re.compile(r"repartitioned_(\d{8}_\d{6})")
            parquet_folders = []

            for folder in all_folders:
                match = pattern.search(folder.name)
                if match:
                    folder_path = folder.path
                    # Check if it contains at least one .parquet file
                    file_list = dbutils.fs.ls(folder_path)
                    parquet_files = [f.path for f in file_list if f.path.endswith(".parquet")]
                    if parquet_files:
                        timestamp = datetime.strptime(match.group(1), "%Y%m%d_%H%M%S")
                        parquet_folders.append((timestamp, folder_path, parquet_files))

            if not parquet_folders:
                print(f"[ERROR] No valid 'repartitioned_' folders with parquet files found in: {base_path}")
                return None

            # Step 2: Sort folders by timestamp descending
            parquet_folders.sort(reverse=True)
            latest_folder_ts, latest_folder_path, latest_parquet_files = parquet_folders[0]

            print("Valid parquet folders found (newest first):")
            for ts, folder, _ in parquet_folders:
                print(f" - {folder} (timestamp: {ts})")

            print(f"\nUsing latest folder: {latest_folder_path}")
            sample_file = latest_parquet_files[0]
        else:
            # Path provided manually — find first parquet file inside
            file_list = dbutils.fs.ls(path)
            parquet_files = [f.path for f in file_list if f.path.endswith(".parquet")]
            if not parquet_files:
                print(f"[ERROR] No parquet files found in: {path}")
                return None
            sample_file = parquet_files[0]
            print(f"[INFO] Using manually specified folder: {path}")

        # Step 3: Read and inspect schema
        df = spark.read.parquet(sample_file)
        print(f"\n Reading file: {sample_file}")
        print("Schema:")
        df.printSchema()

        print("Available columns:", ", ".join(df.columns))
        return df.schema

    except Exception as e:
        print(f"[ERROR] Error checking track schema: {str(e)}")
        return None


# COMMAND ----------

check_track_schema(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Processing Tracks

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, BooleanType, TimestampType
)
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Define expected schema
tracks_schema = StructType([
    StructField("track_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("album_id", StringType(), True),
    StructField("artist_id", StringType(), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("explicit", BooleanType(), True),
    StructField("url", StringType(), True),
    StructField("popularity", IntegerType(), True),
    StructField("track_number", IntegerType(), True),
    StructField("disc_number", IntegerType(), True),
    StructField("extraction_date", TimestampType(), True)
])

def _transform_track_data(spark):
    print("Transforming track data (no Spotify API)")

    storage_paths = _get_storage_paths()
    transformed_path = f"{storage_paths['transformed_data']}/tracks"

    track_files = _get_files_to_process(spark, "tracks")
    if not track_files:
        print("[ERROR] No track files found to process")
        return None

    track_dfs = []
    for file_path in track_files:
        try:
            if file_path.endswith(".json"):
                df = spark.read.json(file_path)
            elif file_path.endswith(".csv"):
                df = spark.read.csv(file_path, header=True, inferSchema=True)
                if "extraction_date" in df.columns and df.schema["extraction_date"].dataType != TimestampType():
                    df = df.withColumn("extraction_date", F.to_timestamp("extraction_date"))
            else:
                df = spark.read.parquet(file_path)

            # Rename fields to match expected schema
            if "song_id" in df.columns:
                df = df.withColumnRenamed("song_id", "track_id")
            if "song_name" in df.columns:
                df = df.withColumnRenamed("song_name", "name")

            df = df.withColumn("source_file", F.lit(file_path))
            track_dfs.append(df)
        except Exception as e:
            print(f"[WARNING] Error reading {file_path}: {str(e)}")

    if not track_dfs:
        print("[ERROR] No valid track data found")
        return None

    # Combine all DataFrames
    tracks_df = track_dfs[0]
    for df in track_dfs[1:]:
        tracks_df = tracks_df.unionByName(df, allowMissingColumns=True)

    # Enforce Schema
    tracks_df = tracks_df.select(
        F.col("track_id").cast(StringType()).alias("track_id"),
        F.col("name").cast(StringType()).alias("name"),
        F.col("duration_ms").cast(IntegerType()).alias("duration_ms"),
        F.col("url").cast(StringType()).alias("url"),
        F.col("popularity").cast(IntegerType()).alias("popularity"),
        F.col("song_added").cast(StringType()).alias("song_added"),
        F.col("album_id").cast(StringType()).alias("album_id"),
        F.col("artist_id").cast(StringType()).alias("artist_id"),
        F.col("extraction_date").cast(TimestampType()).alias("extraction_date"),
        F.col("source_file")
    )


    # Drop duplicates
    tracks_df = tracks_df.dropDuplicates(["track_id"])

    # Fill missing timestamp if not present
    if "extraction_date" not in tracks_df.columns:
        tracks_df = tracks_df.withColumn("extraction_date", F.current_timestamp())

    try:
        if DeltaTable.isDeltaTable(spark, transformed_path):
            existing_df = spark.read.format("delta").load(transformed_path)
            existing_df.createOrReplaceTempView("existing_tracks")
            tracks_df.createOrReplaceTempView("new_tracks")

            merged_df = spark.sql("""
                SELECT n.* 
                FROM new_tracks n 
                LEFT JOIN existing_tracks e ON n.track_id = e.track_id
                WHERE e.track_id IS NULL
            """)

            new_count = merged_df.count()
            if new_count > 0:
                merged_df.write.format("delta").mode("append").save(transformed_path)
                print(f"[SUCCESS] Added {new_count} new track records")
            else:
                print("[INFO] No new tracks to add")
        else:
            init_count = tracks_df.count()
            tracks_df.write.format("delta").mode("overwrite").save(transformed_path)
            print(f"[SUCCESS] Created new tracks table with {init_count} records")
    except Exception as e:
        print(f"[ERROR] Error transforming tracks: {str(e)}")
        return None

    return transformed_path


# COMMAND ----------

# MAGIC %md
# MAGIC ### Runing Transformation for all the datasets

# COMMAND ----------

def transform_spotify_data(spark):
    """
    Transform Spotify data for all entities.
    
    Args:
        spark: SparkSession
                 
    Returns:
        Dictionary with paths to transformed data
    """
    transformed_paths = {}
    
    print("Starting transformation of Spotify data")
    start_time = time.time()
    
    try:
        # Transform each entity type
        artists_path = _transform_artist_data(spark)
        if artists_path:
            transformed_paths["artists"] = artists_path
        
        albums_path = _transform_album_data(spark)
        if albums_path:
            transformed_paths["albums"] = albums_path
        
        tracks_path = _transform_track_data(spark)
        if tracks_path:
            transformed_paths["tracks"] = tracks_path
    except Exception as e:
        print(f"Error in transformation process: {str(e)}")
        #traceback.print_exc()
    
    # Calculate execution time
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Transformation completed in {execution_time:.2f} seconds")
    
    return transformed_paths

# COMMAND ----------

def run_spotify_transformation(create_managed_tables_flag=False):
    """
    Main function to transform Spotify data.
    
    Args:
        create_managed_tables_flag: Whether to create managed tables in silver schema
        
    Returns:
        Dictionary with transformation results
    """
    # When running in Databricks, use the existing SparkSession 
    transformed_paths = transform_spotify_data(spark)
    
    print(f"Transformed data paths: {transformed_paths}")
    
    
    
    # Create result dictionary with detailed information
    result = {
        "transformed_paths": transformed_paths,
        "timestamp": datetime.now().isoformat()
    }
    

    # Log results
    print("\n=== Transformation Summary ===")
    print(f"Transformed Paths: {transformed_paths}")
    print(f"Transformation completed at: {result['timestamp']}")
    
    return result

# COMMAND ----------

run_spotify_transformation()