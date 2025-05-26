# Spotify Data Engineering Project

A Databricks-based ETL pipeline for processing and analyzing Spotify music data using Azure Data Lake Storage.

## Overview

This project implements a scalable data pipeline that extracts, transforms, and loads Spotify music data (artists, albums, tracks) through multiple processing stages. It leverages Databricks for data processing and Azure Data Lake Storage for persistent storage using the Delta Lake format.

## Architecture

![Data Pipeline](https://github.com/user-attachments/assets/7d758de2-14e3-4db8-ba8d-771b4674269b)

![pipeline](https://github.com/user-attachments/assets/ff249eb4-4bf1-4ff3-92d9-9caaed5df0e2)



```
Raw Data → To Process → Processed → Transformed → Data Warehouse
```

- **Raw**: Initial data ingestion point
- **ToProcess**: Data staged for transformation
- **Processed**: Cleaned and validated data
- **Transformed**: Business-ready data with SCD Type 2 patterns applied
- **Warehouse**: Analytics-ready data models
- **QualityReports**: Data quality validation reports

## Setup

### Prerequisites

- Azure Databricks workspace
- Azure Data Lake Storage Gen2 account
- Azure service principal with appropriate permissions
- Databricks secret scope connected to Azure Key Vault

### Storage Configuration

1. Create the following containers in your Azure Storage account:
   - raw
   - toprocess
   - processed
   - transformed
   - warehouse
   - qualityreports
   - logs

2. Store the following secrets in your Azure Key Vault:
   - clientid
   - secretid
   - tenantid
   - spotifyclientid
   - spotifyclientsecret

### Mounting Storage

Run the `Mount_Access_check.py` notebook to mount all required containers:

```python
# Define Azure resources and containers
storage_account_name = "your_storage_account"
secret_scope_name = "datascope"
    
# Define containers to mount
containers = [
        {"name": "raw", "mount_point": "/mnt/spotify/raw"},
        {"name": "toprocess", "mount_point": "/mnt/spotify/toprocess"},
        {"name": "processed", "mount_point": "/mnt/spotify/processed"},
        {"name": "transformed", "mount_point": "/mnt/spotify/transformed"},
        {"name": "warehouse", "mount_point": "/mnt/spotify/warehouse"},
        {"name": "qualityreports", "mount_point": "/mnt/spotify/qualityreports"},
        {"name": "logs", "mount_point": "/mnt/spotify/logs"}
    ]
    
# Mount containers and display results
success = mount_all_containers(containers, storage_account_name, secret_scope_name)
```

## Data Transformation

Run the `Transform_Spotify_Data_From_Raw.py` notebook to transform data from the raw layer:

```python
run_spotify_transformation()
```

This will:
1. Process artist data with SCD Type 2 patterns
2. Process album data with SCD Type 2 patterns
3. Process track data

## Data Models

### Artists
- artist_id (string)
- artist_name (string)
- external_url (string)
- extraction_date (timestamp)
- hash_key (SHA-256 hash for change detection)
- is_current (boolean)
- valid_from (timestamp)
- valid_to (timestamp)

### Albums
- album_id (string)
- name (string)
- release_date (timestamp)
- total_tracks (integer)
- url (string)
- artist_id (string)
- extraction_date (timestamp)
- hash_key (SHA-256 hash for change detection)
- is_current (boolean)
- valid_from (timestamp)
- valid_to (timestamp)

### Tracks
- track_id (string)
- name (string)
- album_id (string)
- artist_id (string)
- duration_ms (integer)
- explicit (boolean)
- url (string)
- popularity (integer)
- track_number (integer)
- disc_number (integer)
- extraction_date (timestamp)

## Dependencies

- Databricks Runtime
- PySpark
- Delta Lake
- Spotipy (Python Spotify API client)

## Workflow

1. Raw Spotify data is stored in the raw container
2. Data is copied to the toprocess container
3. Run the transformation notebook to process the data
4. Transformed data is stored in Delta Lake format in the transformed container
5. Data can be further loaded into the data warehouse for analytics

## Notes

- The project implements SCD Type 2 for artist and album data to track historical changes
- Track data is processed using a simpler approach with duplicate elimination
- The pipeline expects data in JSON, CSV, or Parquet format 
