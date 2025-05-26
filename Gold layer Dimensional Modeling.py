# Databricks notebook source
# MAGIC %md
# MAGIC ### Imports 

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, concat, sha2, 
    datediff, year, month, dayofmonth, 
    dayofweek, weekofyear, quarter, to_date
)
from pyspark.sql.types import IntegerType, StringType, DateType
from delta.tables import DeltaTable
from typing import Dict
import time
from datetime import datetime, date, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking schema of transformed data

# COMMAND ----------

def _get_storage_paths():
    """Get storage paths for Spotify ETL pipeline using mounted ADLS storage"""
    base_path = "/mnt/spotify"
    return {
        "raw_data": f"{base_path}/raw",
        "toprocess_data": f"{base_path}/toprocess",
        "processed_data": f"{base_path}/processed",
        "transformed_data": f"{base_path}/transformed",
        "warehouse": f"{base_path}/warehouse",
        "quality_reports": f"{base_path}/qualityreports"
    }

# COMMAND ----------

# MAGIC %md
# MAGIC #### ARTIST SCHEMA from transformed layer

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
        path = f"{storage_paths['transformed_data']}/artists"
    
    try:
        # Detect Delta table
        is_delta = False
        try:
            # Quick check: does _delta_log folder exist?
            delta_log_path = f"{path}/_delta_log"
            if dbutils.fs.ls(delta_log_path):
                is_delta = True
        except:
            pass

        if is_delta:
            print(f"[SCHEMA CHECK] Detected Delta table at {path}")
            df = spark.read.format("delta").load(path)
        else:
            # List all CSV/JSON/Parquet files
            files = [f for f in dbutils.fs.ls(path) if f.path.endswith((".csv", ".json", ".parquet"))]
            if not files:
                print(f"No data files found in {path}")
                return None
            sample_file = files[0].path

            if sample_file.endswith(".csv"):
                df = spark.read.csv(sample_file, header=True, inferSchema=True)
            elif sample_file.endswith(".json"):
                df = spark.read.json(sample_file)
            else:
                df = spark.read.parquet(sample_file)

        # Print schema details
        print(f"[SCHEMA CHECK] Schema for artist data in {path}:")
        df.printSchema()

        # Print column names and available columns
        columns = df.columns
        print(f"[SCHEMA CHECK] Available columns: {', '.join(columns)}")

        # Check for critical columns
        critical_columns = ["artist_id", "artist_name"]
        missing = [col for col in critical_columns if col not in columns]
        if missing:
            print(f"[SCHEMA CHECK WARNING] Missing critical columns: {', '.join(missing)}")

        return df.schema

    except Exception as e:
        print(f"[SCHEMA CHECK ERROR] Error checking artist schema: {str(e)}")
        return None


# COMMAND ----------

check_artist_schema(spark)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ALBUM SCHEMA from transformed layer

# COMMAND ----------

def check_album_schema(spark, path=None):
    """
    Utility function to check the schema of album data.
    
    Args:
        spark: SparkSession
        path: Optional path to album data. If None, uses default path.
        
    Returns:
        DataFrame schema
    """
    if path is None:
        storage_paths = _get_storage_paths()
        path = f"{storage_paths['transformed_data']}/albums"
    
    try:
        # Detect Delta table
        is_delta = False
        try:
            delta_log_path = f"{path}/_delta_log"
            if dbutils.fs.ls(delta_log_path):
                is_delta = True
        except:
            pass

        if is_delta:
            print(f"[SCHEMA CHECK] Detected Delta table at {path}")
            df = spark.read.format("delta").load(path)
        else:
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
        print(f"Schema for album data in {path}:")
        df.printSchema()
        
        # Print column names and available columns
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

df = spark.read.format('delta').load('/mnt/spotify/transformed/albums'); 
df.printSchema()

# COMMAND ----------

df = spark.read.format('delta').load('/mnt/spotify/warehouse/dim_albums'); 
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### TRACK SCHEMA from transformed layer

# COMMAND ----------

def check_track_schema(spark, path=None):
    """
    Utility function to check the schema of track data.
    
    Args:
        spark: SparkSession
        path: Optional path to track data. If None, uses default path.
        
    Returns:
        DataFrame schema
    """
    if path is None:
        storage_paths = _get_storage_paths()
        path = f"{storage_paths['transformed_data']}/tracks"
    
    try:
        # Detect Delta table
        is_delta = False
        try:
            delta_log_path = f"{path}/_delta_log"
            if dbutils.fs.ls(delta_log_path):
                is_delta = True
        except:
            pass

        if is_delta:
            print(f"[SCHEMA CHECK] Detected Delta table at {path}")
            df = spark.read.format("delta").load(path)
        else:
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
        print(f"Schema for track data in {path}:")
        df.printSchema()
        
        # Print column names and available columns
        columns = df.columns
        print(f"Available columns: {', '.join(columns)}")
        
        # Check for critical columns
        critical_columns = ["track_id", "name", "artist_id"]
        missing = [col for col in critical_columns if col not in columns]
        if missing:
            print(f"WARNING: Missing critical columns: {', '.join(missing)}")
        
        return df.schema
    except Exception as e:
        print(f"Error checking track schema: {str(e)}")
        return None

# COMMAND ----------

check_track_schema(spark)

# COMMAND ----------

def load_dim_artists(spark: SparkSession) -> None:
    """
    Load artist dimension from transformed data.
    
    Args:
        spark: SparkSession
    """
    print("Loading artist dimension")
    
    # Get storage paths
    storage_paths = _get_storage_paths()
    transformed_path = f"{storage_paths['transformed_data']}/artists"
    warehouse_path = f"{storage_paths['warehouse']}/dim_artists"
    
    # Check if source exists
    if not DeltaTable.isDeltaTable(spark, transformed_path):
        print(f"Source table {transformed_path} does not exist")
        return
    
    # Read source data
    artist_df = spark.read.format("delta").load(transformed_path)
    
    # Add surrogate key and source system
    artist_df = artist_df.withColumn(
        "artist_key", 
        sha2(concat(col("artist_id"), lit("spotify")), 256)
    ).withColumn(
        "source_system", 
        lit("spotify")
    ).withColumn(
        "last_updated",
        current_timestamp()
    )
    
    # Check if target exists
    if DeltaTable.isDeltaTable(spark, warehouse_path):
        # Target exists, perform merge (SCD Type 2)
        print(f"Target table {warehouse_path} exists, performing merge")
        
        # Create temp view for merge
        artist_df.createOrReplaceTempView("staged_artists")
        
        # Get target as Delta table
        target_delta = DeltaTable.forPath(spark, warehouse_path)
        
        # Perform merge
        target_delta.alias("target").merge(
            artist_df.alias("source"),
            "target.artist_id = source.artist_id"
        ).whenMatchedUpdate(
            condition="target.hash_key <> source.hash_key AND target.is_current = true",
            set={
                "is_current": "false",
                "valid_to": "current_timestamp()",
                "last_updated": "source.last_updated"
            }
        ).whenNotMatchedInsert(
            values={
                "artist_key": "source.artist_key",
                "artist_id": "source.artist_id",
                "artist_name": "source.artist_name",
                "external_url": "source.external_url",
                "is_current": "true",
                "valid_from": "current_timestamp()",
                "valid_to": "null",
                "hash_key": "source.hash_key",
                "source_system": "source.source_system",
                "last_updated": "source.last_updated"
            }
        ).execute()
        
        # Insert new versions for changed records
        spark.sql(f"""
        INSERT INTO delta.`{warehouse_path}`
        SELECT 
            source.artist_key,
            source.artist_id,
            source.artist_name,
            source.external_url,
            true as is_current,
            current_timestamp() as valid_from,
            null as valid_to,
            source.hash_key,
            source.source_system,
            source.last_updated
        FROM staged_artists source
        JOIN delta.`{warehouse_path}` target
        ON source.artist_id = target.artist_id
        WHERE target.is_current = false AND target.valid_to >= current_date()
        """)
    else:
        # Target doesn't exist, create it
        print(f"Target table {warehouse_path} does not exist, creating it")
        
        # Select relevant columns and add SCD Type 2 columns
        artist_df = artist_df.select(
            "artist_key",
            "artist_id",
            "artist_name",
            "external_url",
            "is_current",
            "valid_from",
            "valid_to",
            "hash_key",
            "source_system",
            "last_updated"
        )
        
        # Write to target
        artist_df.write.format("delta").mode("overwrite").save(warehouse_path)
    
    print(f"Artist dimension loaded successfully")

# COMMAND ----------

def load_dim_albums(spark: SparkSession) -> None:
    """
    Load album dimension from transformed data.
    
    Args:
        spark: SparkSession
    """
    print("Loading album dimension")
    
    # Get storage paths
    storage_paths = _get_storage_paths()
    transformed_path = f"{storage_paths['transformed_data']}/albums"
    warehouse_path = f"{storage_paths['warehouse']}/dim_albums"
    
    print(f"Source path: {transformed_path}")
    print(f"Target path: {warehouse_path}")
    
    # Check if source exists
    if not DeltaTable.isDeltaTable(spark, transformed_path):
        print(f"Source table {transformed_path} does not exist")
        return
    
    # Read source data
    album_df = spark.read.format("delta").load(transformed_path)
    print(f"Source records count: {album_df.count()}")
    
    # Only keep the one “current” row per album to avoid multiple source matches !
    from pyspark.sql.functions import col
    album_df = (
        album_df
          .filter(col("is_current") == True)
          .dropDuplicates(["album_id"])
    )
    print(f"Filtered current records count: {album_df.count()}")

    # Add surrogate key and source system, and rename columns to match target
    album_df = album_df.withColumn(
        "album_key", 
        sha2(concat(col("album_id"), lit("spotify")), 256)
    ).withColumn(
        "source_system", 
        lit("spotify")
    ).withColumn(
        "last_updated",
        current_timestamp()
    ).withColumnRenamed("name", "album_name")  # Rename to match target schema
    
    # Check if target exists
    if DeltaTable.isDeltaTable(spark, warehouse_path):
        # Target exists, perform merge (SCD Type 2)
        print(f"Target table {warehouse_path} exists, performing merge")
        
        # Get target as Delta table
        target_delta = DeltaTable.forPath(spark, warehouse_path)
        print("Target table schema:")
        target_delta.toDF().printSchema()
        
        # Create temp view for merge
        album_df.createOrReplaceTempView("staged_albums")
        print("Source table schema:")
        album_df.printSchema()
        
        # Perform merge
        target_delta.alias("target").merge(
            album_df.alias("source"),
            "target.album_id = source.album_id"
        ).whenMatchedUpdate(
            condition="target.hash_key <> source.hash_key AND target.is_current = true",
            set={
                "is_current": "false",
                "valid_to": "current_timestamp()",
                "last_updated": "source.last_updated"
            }
        ).whenNotMatchedInsert(
            values={
                "album_key": "source.album_key",
                "album_id": "source.album_id",
                "album_name": "source.album_name",
                "release_date": "source.release_date",
                "total_tracks": "source.total_tracks",
                "url": "source.url",
                "artist_id": "source.artist_id",
                "is_current": "true",
                "valid_from": "current_timestamp()",
                "valid_to": "null",
                "hash_key": "source.hash_key",
                "source_system": "source.source_system",
                "last_updated": "source.last_updated"
            }
        ).execute()
        
        # Verify records after merge
        print(f"Records in target after merge: {spark.read.format('delta').load(warehouse_path).count()}")
        
    else:
        # Target doesn't exist, create it
        print(f"Target table {warehouse_path} does not exist, creating it")
        
        # Select relevant columns and add SCD Type 2 columns
        album_df = album_df.select(
            "album_key",
            "album_id",
            "album_name",
            "release_date",
            "total_tracks",
            "url",
            "artist_id",
            lit(True).alias("is_current"),
            current_timestamp().alias("valid_from"),
            lit(None).cast("timestamp").alias("valid_to"),
            "hash_key",
            "source_system",
            "last_updated"
        )
        
        print("Writing initial data to target")
        print("Schema to be written:")
        album_df.printSchema()
        print(f"Number of records to write: {album_df.count()}")
        
        # Write to target
        album_df.write.format("delta").mode("overwrite").save(warehouse_path)
        
        # Verify write
        print(f"Records written to target: {spark.read.format('delta').load(warehouse_path).count()}")
    
    print(f"Album dimension loaded successfully")

# COMMAND ----------

def load_dim_tracks(spark: SparkSession) -> None:
    """
    Load track dimension from transformed data.
    
    Args:
        spark: SparkSession
    """
    print("Loading track dimension")
    
    # Get storage paths
    storage_paths = _get_storage_paths()
    transformed_path = f"{storage_paths['transformed_data']}/tracks"
    warehouse_path = f"{storage_paths['warehouse']}/dim_tracks"
    
    # Check if source exists
    if not DeltaTable.isDeltaTable(spark, transformed_path):
        print(f"Source table {transformed_path} does not exist")
        return
    
    # Read source data
    track_df = spark.read.format("delta").load(transformed_path)
    
    # Add surrogate key and SCD columns (since tracks don't have SCD columns in the source)
    track_df = track_df.withColumn(
        "track_key", 
        sha2(concat(col("track_id"), lit("spotify")), 256)
    ).withColumn(
        "source_system", 
        lit("spotify")
    ).withColumn(
        "is_current", 
        lit(True)
    ).withColumn(
        "valid_from", 
        current_timestamp()
    ).withColumn(
        "valid_to", 
        lit(None).cast("timestamp")
    ).withColumn(
        "track_name",
        col("name")
    )
    
    # Check if target exists
    if DeltaTable.isDeltaTable(spark, warehouse_path):
        # Target exists, perform merge
        print(f"Target table {warehouse_path} exists, performing merge")
        
        # Get current data in target
        target_df = spark.read.format("delta").load(warehouse_path)
        
        # Create temp views for merge
        track_df.createOrReplaceTempView("staged_tracks")
        target_df.createOrReplaceTempView("target_tracks")
        
        # Identify tracks to update
        updated_tracks = spark.sql("""
        SELECT staged.* 
        FROM staged_tracks staged
        LEFT JOIN target_tracks target ON staged.track_id = target.track_id
        WHERE target.track_id IS NULL
        """)
        
        # Append new tracks
        if updated_tracks.count() > 0:
            updated_tracks.select(
                "track_key",
                "track_id",
                "track_name",
                "duration_ms",
                "url",
                "popularity",
                "song_added",
                "album_id",
                "artist_id",
                "is_current",
                "valid_from",
                "valid_to",
                "source_system",
                current_timestamp().alias("last_updated")
            ).write.format("delta").mode("append").save(warehouse_path)
    else:
        # Target doesn't exist, create it
        print(f"Target table {warehouse_path} does not exist, creating it")
        
        # Select relevant columns for dimension table
        track_df = track_df.select(
            "track_key",
            "track_id",
            "track_name",
            "duration_ms",
            "url",
            "popularity",
            "song_added",
            "album_id",
            "artist_id",
            "is_current",
            "valid_from",
            "valid_to",
            "source_system",
            current_timestamp().alias("last_updated")
        )
        
        # Write to target
        track_df.write.format("delta").mode("overwrite").save(warehouse_path)
    
    print(f"Track dimension loaded successfully")

# COMMAND ----------

def load_dim_dates(spark: SparkSession) -> None:
    """
    Load date dimension table. This is a static dimension.
    
    Args:
        spark: SparkSession
    """
    print("Loading date dimension")
    
    # Get storage paths
    storage_paths = _get_storage_paths()
    warehouse_path = f"{storage_paths['warehouse']}/dim_dates"
    
    # Check if table already exists and has data
    if DeltaTable.isDeltaTable(spark, warehouse_path):
        count = spark.read.format("delta").load(warehouse_path).count()
        if count > 0:
            print(f"Date dimension already exists with {count} records")
            return
    
    # Generate date dimension for 10 years (5 years back to 5 years forward)
    start_date = (datetime.now().date().replace(day=1, month=1) - 
                 timedelta(days=5*365))
    end_date = start_date.replace(year=start_date.year + 10)
    
    # Generate list of dates
    date_range_df = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{start_date}', 'yyyy-MM-dd'),
        to_date('{end_date}', 'yyyy-MM-dd'),
        interval 1 day
    )) as calendar_date
    """)
    
    # Create date dimension
    date_df = date_range_df.select(
        col("calendar_date"),
        concat(
            year(col("calendar_date")),
            when(month(col("calendar_date")) < 10, concat(lit("0"), month(col("calendar_date"))))
            .otherwise(month(col("calendar_date"))),
            when(dayofmonth(col("calendar_date")) < 10, concat(lit("0"), dayofmonth(col("calendar_date"))))
            .otherwise(dayofmonth(col("calendar_date")))
        ).cast(IntegerType()).alias("date_key"),
        year(col("calendar_date")).alias("year"),
        month(col("calendar_date")).alias("month"),
        dayofmonth(col("calendar_date")).alias("day_of_month"),
        dayofweek(col("calendar_date")).alias("day_of_week"),
        weekofyear(col("calendar_date")).alias("week_of_year"),
        quarter(col("calendar_date")).alias("quarter")
    )
    
    # Write to target
    date_df.write.format("delta").mode("overwrite").save(warehouse_path)
    
    print(f"Date dimension loaded with {date_df.count()} records")

# COMMAND ----------

def load_fact_track_plays(spark: SparkSession) -> None:
    """
    Load fact table for track play metrics. For example purposes, 
    we'll simply use track popularity as a proxy for plays.
    
    Args:
        spark: SparkSession
    """
    print("Loading track plays fact table")
    
    # Get storage paths
    storage_paths = _get_storage_paths()
    tracks_path = f"{storage_paths['transformed_data']}/tracks"
    artists_path = f"{storage_paths['transformed_data']}/artists"
    albums_path = f"{storage_paths['transformed_data']}/albums"
    warehouse_path = f"{storage_paths['warehouse']}/fact_track_plays"
    
    # Check if sources exist
    if (not DeltaTable.isDeltaTable(spark, tracks_path) or
        not DeltaTable.isDeltaTable(spark, artists_path) or
        not DeltaTable.isDeltaTable(spark, albums_path)):
        print("One or more source tables do not exist")
        return
    
    # Read source data
    track_df = spark.read.format("delta").load(tracks_path)
    artist_df = spark.read.format("delta").load(artists_path)
    album_df = spark.read.format("delta").load(albums_path)
    
    # Create date key
    today = date.today()
    date_key = int(today.strftime("%Y%m%d"))
    
    # Create surrogate keys
    track_df = track_df.withColumn(
        "track_key", 
        sha2(concat(col("track_id"), lit("spotify")), 256)
    )
    artist_df = artist_df.withColumn(
        "artist_key", 
        sha2(concat(col("artist_id"), lit("spotify")), 256)
    ).filter(
        col("is_current") == True
    )
    album_df = album_df.withColumn(
        "album_key", 
        sha2(concat(col("album_id"), lit("spotify")), 256)
    ).filter(
        col("is_current") == True
    )
    
    # Join to get keys for fact table
    fact_df = track_df.join(
        artist_df.select("artist_id", "artist_key"),
        "artist_id",
        "left"
    ).join(
        album_df.select("album_id", "album_key"),
        "album_id",
        "left"
    )
    
    # Create fact table
    fact_df = fact_df.select(
        col("track_key"),
        col("artist_key"),
        col("album_key"),
        col("popularity"),
        lit(date_key).cast(IntegerType()).alias("date_key"),
        lit(1).cast(IntegerType()).alias("play_count"),
        current_timestamp().alias("last_updated")
    )
    
    # Write to target
    if DeltaTable.isDeltaTable(spark, warehouse_path):
        # Check if we already have data for today
        existing_df = spark.read.format("delta").load(warehouse_path)
        today_data = existing_df.filter(col("date_key") == date_key)
        
        if today_data.count() > 0:
            # Update existing data
            existing_df.createOrReplaceTempView("existing_facts")
            fact_df.createOrReplaceTempView("new_facts")
            
            # Delete today's data
            DeltaTable.forPath(spark, warehouse_path).delete(
                f"date_key = {date_key}"
            )
            
            # Insert new data
            fact_df.write.format("delta").mode("append").save(warehouse_path)
        else:
            # Append new data
            fact_df.write.format("delta").mode("append").save(warehouse_path)
    else:
        # Create new table
        fact_df.write.format("delta").mode("overwrite").save(warehouse_path)
    
    print(f"Track plays fact table loaded with {fact_df.count()} records")


# COMMAND ----------

def load_gold_layer(spark: SparkSession) -> Dict[str, str]:
    """
    Load transformed Spotify data into dimensional and fact tables in the Gold layer.
    
    Args:
        spark: SparkSession
    
    Returns:
        Dictionary with paths to loaded data
    """
    loaded_paths = {}
    
    print("Starting Gold layer loading process for Spotify data")
    
    try:
        # Create directories if they don't exist
        storage_paths = _get_storage_paths()
        warehouse_path = storage_paths["warehouse"]
        dbutils.fs.mkdirs(warehouse_path)
        
        # Load dimensions
        load_dim_artists(spark)
        load_dim_albums(spark)
        load_dim_tracks(spark)
        load_dim_dates(spark)
        
        # Load facts
        load_fact_track_plays(spark)
        
        # Return paths
        loaded_paths["dim_artists"] = f"{warehouse_path}/dim_artists"
        loaded_paths["dim_albums"] = f"{warehouse_path}/dim_albums"
        loaded_paths["dim_tracks"] = f"{warehouse_path}/dim_tracks"
        loaded_paths["dim_dates"] = f"{warehouse_path}/dim_dates"
        loaded_paths["fact_track_plays"] = f"{warehouse_path}/fact_track_plays"
        
        print("Successfully loaded all data to Gold layer warehouse")
        
    except Exception as e:
        print(f"Error loading data to Gold layer: {str(e)}")
        #traceback.print_exc()
    
    return loaded_paths

# COMMAND ----------

def main():
    """Main function to load data to the Gold layer data warehouse."""
    start_time = time.time()
    
    # Use existing SparkSession in Databricks
    spark = SparkSession.builder.getOrCreate()
    
    # Load data
    loaded_paths = load_gold_layer(spark)
    
    # Record execution time
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Log completion
    print(f"Gold layer loading completed in {execution_time:.2f} seconds")
    print(f"Loaded data paths: {loaded_paths}")
    
    return loaded_paths

# COMMAND ----------

main()

# COMMAND ----------

print("Checking transformed data:"); 
print("\nArtists:"); 
print(spark.read.format("delta").load("/mnt/spotify/transformed/artists").count()); 
print("\nAlbums:"); 
print(spark.read.format("delta").load("/mnt/spotify/transformed/albums").count()); 
print("\nTracks:"); 
print(spark.read.format("delta").load("/mnt/spotify/transformed/tracks").count())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/spotify/warehouse")  )

# COMMAND ----------

dbutils.fs.ls("/mnt/spotify/warehouse/dim_artists/_delta_log")

# COMMAND ----------

dbutils.fs.rm("/mnt/spotify/data_warehouse", recurse=True)

# COMMAND ----------

dbutils.fs.ls("/mnt/spotify")


# COMMAND ----------

display(dbutils.fs.mounts())


# COMMAND ----------

print("Checking DataWarehouse data:"); 
print("\nArtists:"); 
print(spark.read.format("delta").load("/mnt/spotify/warehouse/dim_artists").count()); 
print("\nAlbums:"); 
print(spark.read.format("delta").load("/mnt/spotify/warehouse/dim_albums").count()); 
print("\nTracks:"); 
print(spark.read.format("delta").load("/mnt/spotify/warehouse/dim_tracks").count());
print("\nFact_tracks_plays:"); 
print(spark.read.format("delta").load("/mnt/spotify/warehouse/fact_track_plays").count());

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storing the final data model in MongoDB database

# COMMAND ----------

!pip install pymongo

# COMMAND ----------

from pymongo import MongoClient
import pandas as pd
from pyspark.sql import functions as F
import datetime

# MongoDB connection
MONGO_URI = "mongodb+srv://your-mongo-account:your-mongo-password@clusterproject.usams.mongodb.net/?retryWrites=true&w=majority&appName=ClusterProject"
client = MongoClient(MONGO_URI)
db = client["spotify_analytics"]

# Helper function
def sync_to_mongodb(delta_path, collection_name, spark_session):
    try:
        print(f"\n🔄 Syncing: {collection_name}")
        
        # Load Spark DataFrame
        df_spark = spark_session.read.format("delta").load(delta_path)
        
        # Basic EDA
        print(" Schema:")
        df_spark.printSchema()
        print(" Row Count:", df_spark.count())
        print(" Null Summary:")
        nulls = df_spark.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df_spark.columns])
        nulls.show(truncate=False)
        
        # Clean datetime columns BEFORE converting to Pandas
        for field in df_spark.schema.fields:
            if "TimestampType" in str(field.dataType):
                col_name = field.name
                df_spark = df_spark.withColumn(
                    col_name,
                    F.when(F.col(col_name).isNull(), None)
                    .when(F.col(col_name) < F.lit("1900-01-01"), None)
                    .otherwise(F.col(col_name))
                )

        # Convert to Pandas
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
        df_pandas = df_spark.toPandas()


        # Clean and fill datetime columns
        current_ts = datetime.datetime.utcnow()
        timestamp_cols = df_pandas.select_dtypes(include=["datetimetz", "datetime64[ns]"]).columns

        # Clean datetime columns in Pandas
        for col in df_pandas.select_dtypes(include=["datetimetz", "datetime64[ns]"]).columns:
            df_pandas[col] = pd.to_datetime(df_pandas[col], errors='coerce')  # convert + coerce errors
            df_pandas[col] = df_pandas[col].dt.tz_localize(None)  # remove timezone
            df_pandas[col] = df_pandas[col].fillna(current_ts)

        # Final check and drop any rows with invalid timestamps 
        df_pandas_clean = df_pandas.dropna(axis=0, how='all')  # drop fully empty rows

        # Insert into MongoDB
        collection = db[collection_name]
        collection.delete_many({})
        if not df_pandas_clean.empty:
            collection.insert_many(df_pandas_clean.to_dict("records"))
            print(f"Uploaded {len(df_pandas_clean)} documents to '{collection_name}'")
        else:
            print(f"No data found for '{collection_name}'")

    except Exception as e:
        print(f"Error uploading to '{collection_name}': {str(e)}")

# COMMAND ----------

# Warehouse paths
warehouse_base = "/mnt/spotify/warehouse"
collections = {
    "artists": f"{warehouse_base}/dim_artists",
    "albums": f"{warehouse_base}/dim_albums",
    "tracks": f"{warehouse_base}/dim_tracks",
    "track_plays": f"{warehouse_base}/fact_track_plays"
}

# Run syncing
for collection_name, path in collections.items():
    sync_to_mongodb(path, collection_name, spark)
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analysis 

# COMMAND ----------

from pymongo import MongoClient

client = MongoClient("mongodb+srv://shubhamdworkmail:Mongodb%40irl@clusterproject.usams.mongodb.net/?retryWrites=true&w=majority&appName=ClusterProject")
db = client["spotify_analytics"]


# COMMAND ----------

# MAGIC %md
# MAGIC ### 1: Artist with the Highest Average Popularity

# COMMAND ----------

print("Total tracks in Mongo:", db.tracks.count_documents({}))
print("One sample track doc:", db.tracks.find_one())


# COMMAND ----------

# 1. Count and sample
print("tracks count:", db.tracks.count_documents({}))
sample = db.tracks.find_one()
print("sample doc:", sample)

# 2. Try a simpler aggregation to see any output
pipeline_test = [
    {"$group": {"_id": "$artist_id", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 3}
]
for doc in db.tracks.aggregate(pipeline_test):
    print(doc)


# COMMAND ----------

pipeline = [
  { "$group": {
      "_id": "$artist_id",
      "avg_popularity": { "$avg": "$popularity" }
    }
  },
  { "$lookup": {
      "from": "artists",
      "localField": "_id",
      "foreignField": "artist_id",
      "as": "artist_info"
    }
  },
  { "$unwind": "$artist_info" },
  { "$project": {
      "artist_name":    "$artist_info.artist_name",
      "avg_popularity": 1
    }
  },
  { "$sort": { "avg_popularity": -1 } },
  { "$limit": 10 }
]

result = db.tracks.aggregate(pipeline)
for doc in result:
    print(doc)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2: Popularity Change Over Time (song_added field)

# COMMAND ----------

bad_docs = db.tracks.find({
    "$or": [
        { "song_added": { "$type": "int" } },
        { "song_added": { "$type": "double" } },
        { "song_added": { "$type": "object" } },
        { "song_added": { "$type": "array" } },
        { "song_added": { "$regex": "^(?!\\d{4}-\\d{2}-\\d{2})" } },  # not starting with YYYY-MM-DD
        { "song_added": { "$exists": True, "$not": { "$type": "date" } } },
        { "song_added": { "$regex": "^(http|www\\.)" } },
        { "song_added": { "$type": "null" } },
        { "song_added": { "$eq": "" } },
        { "song_added": { "$not": { "$regex": "^\\d{4}" } } }  # does not start with 4-digit year
    ]
}, {"song_added": 1})
for doc in bad_docs:
    print(doc)


# COMMAND ----------

pipeline = [
    {
        "$match": {
            "song_added": {"$ne": None},
            "song_added": {"$regex": r"^\d{4}-"}  # Keeps only ISO date-like strings
        }
    },
    {
        "$addFields": {
            "year_added": {"$substr": ["$song_added", 0, 4]}
        }
    },
    {
        "$group": {
            "_id": "$year_added",
            "avg_popularity": {"$avg": "$popularity"}
        }
    },
    {
        "$match": {
            "_id": {"$regex": "^[0-9]{4}$"}  # Keep only 4-digit years
        }
    },
    {
        "$sort": {"_id": 1}
    }
]

result = db.tracks.aggregate(pipeline)
for doc in result:
    print(doc)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3: Top 5 Albums with Highest Total Track Popularity

# COMMAND ----------

pipeline = [
    {
        "$group": {
            "_id": "$album_id",
            "total_popularity": {"$sum": "$popularity"}
        }
    },
    {
        "$sort": {"total_popularity": -1}
    },
    {
        "$limit": 5
    },
    {
        "$lookup": {
            "from": "albums",
            "localField": "_id",
            "foreignField": "album_id",
            "as": "album_info"
        }
    },
    {
        "$unwind": "$album_info"
    },
    {
        "$project": {
            "album_name": "$album_info.album_name",
            "total_popularity": 1
        }
    }
]
result = db.tracks.aggregate(pipeline)
for doc in result:
    print(doc)

