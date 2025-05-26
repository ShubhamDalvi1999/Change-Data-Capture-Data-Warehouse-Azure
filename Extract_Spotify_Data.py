# Databricks notebook source
# MAGIC %md
# MAGIC ### IMPORTS

# COMMAND ----------

!pip install spotipy

# COMMAND ----------

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime
import pandas as pd

# COMMAND ----------

def display_mounts():
    """Display current mount points"""
    print("\n=== Current Mount Points ===")
    for mount in dbutils.fs.mounts():
        print(f"  {mount.mountPoint} -> {mount.source}")
display_mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extract Playlist data

# COMMAND ----------

def fetch_playlist_tracks(sp, playlist_id):
    """Retrieve all tracks from a Spotify playlist."""
    try:
        tracks = []
        results = sp.playlist_tracks(playlist_id)
        tracks.extend(results['items'])
        while results['next']:
            results = sp.next(results)
            tracks.extend(results['items'])
        return tracks
    except Exception as e:
        print(f"Error retrieving tracks for playlist {playlist_id}: {str(e)}")
        return []

# COMMAND ----------

# MAGIC %md
# MAGIC ### For each Playlist Extract Artist

# COMMAND ----------

def extract_unique_artists(tracks, extraction_date):
    """Extract unique artists from playlist tracks."""
    artist_dict = {}
    for item in tracks:
        track = item.get('track')
        if track:
            for artist in track.get('artists', []):
                artist_id = artist['id']
                if artist_id not in artist_dict:
                    artist_dict[artist_id] = {
                        'artist_id': artist_id,
                        'artist_name': artist['name'],
                        'external_url': artist['external_urls']['spotify'],
                        'extraction_date': extraction_date
                    }
    return artist_dict

# COMMAND ----------

# MAGIC %md
# MAGIC ### For each Playlist Extract Album

# COMMAND ----------

def extract_album_data(tracks, extraction_date):
    """Extract album data from playlist tracks."""
    album_list = []
    for item in tracks:
        track = item.get('track')
        if track and track.get('album'):
            album = track['album']
            album_element = {
                'album_id': album.get('id'),
                'name': album.get('name'),
                'release_date': album.get('release_date'),
                'total_tracks': album.get('total_tracks'),
                'url': album.get('external_urls', {}).get('spotify'),
                'artist_id': album.get('artists', [{}])[0].get('id') if album.get('artists') else None,
                'extraction_date': extraction_date
            }
            album_list.append(album_element)
    return album_list

# COMMAND ----------

# MAGIC %md
# MAGIC ### For each Playlist Extract Songs

# COMMAND ----------

def extract_song_data(tracks, extraction_date):
    """Extract song data from playlist tracks."""
    song_list = []
    for item in tracks:
        track = item.get('track')
        if track:
            song_element = {
                'song_id': track.get('id'),
                'song_name': track.get('name'),
                'duration_ms': track.get('duration_ms'),
                'url': track.get('external_urls', {}).get('spotify'),
                'popularity': track.get('popularity'),
                'song_added': item.get('added_at'),
                'album_id': track.get('album', {}).get('id') if track.get('album') else None,
                'artist_id': track.get('artists', [{}])[0].get('id') if track.get('artists') else None,
                'extraction_date': extraction_date
            }
            song_list.append(song_element)
    return song_list

# COMMAND ----------

# MAGIC %md
# MAGIC ### Utility functions for paths and storage in adls

# COMMAND ----------

def get_storage_paths():
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

def save_data_to_adls(data, entity_type, timestamp):
    """
    Save data to Azure Data Lake Storage in the raw container
    
    Args:
        data: Dictionary or list of dictionaries containing data
        entity_type: Type of entity (artists, albums, songs)
        timestamp: Timestamp string for the filename
    
    Returns:
        Path where the data was saved
    """
    try:
        # Get storage path
        storage_paths = get_storage_paths()
        raw_path = storage_paths["raw_data"]
        
        # Create directory if it doesn't exist
        entity_path = f"{raw_path}/{entity_type}"
        dbutils.fs.mkdirs(entity_path)
        
        # Create the full path with filename
        filename = f"{entity_type}_{timestamp}.csv"
        full_path = f"{entity_path}/{filename}"
        
        # Convert data to pandas DataFrame (depends on the structure)
        if entity_type == "artists":
            # Convert dict of dicts to list of dicts
            artists_list = list(data.values())
            df = pd.DataFrame(artists_list)
        else:
            # For albums and songs, which are already lists
            df = pd.DataFrame(data)
        
        # Convert to CSV string
        csv_str = df.to_csv(index=False)
        
        # Write to ADLS
        dbutils.fs.put(full_path, csv_str, overwrite=True)
        
        print(f"Data saved to ADLS at {full_path}")
        return full_path
    
    except Exception as e:
        print(f"Error saving {entity_type} to ADLS: {str(e)}")
        return None

# COMMAND ----------

def verify_adls_save(data, entity_type, file_path):
    """
    Verify that data was correctly saved to ADLS by comparing counts.
    
    Args:
        data: Original data that was saved
        entity_type: Type of entity (artists, albums, songs)
        file_path: Path where the data was saved
        
    Returns:
        Dictionary with verification results
    """
    try:
        # Get count of original data
        original_count = len(data.values()) if isinstance(data, dict) else len(data)
        
        # Read back the data using Spark to verify
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        saved_count = df.count()
        
        # Check if counts match
        counts_match = original_count == saved_count
        
        result = {
            "file_exists": True,
            "original_count": original_count,
            "saved_count": saved_count,
            "counts_match": counts_match,
            "file_path": file_path
        }
        
        if counts_match:
            print(f"Successfully verified {saved_count} records in {file_path}")
        else:
            print(f"Count mismatch for {entity_type}: Original={original_count}, Saved={saved_count}")
            
        return result
        
    except Exception as e:
        print(f"Error verifying save: {str(e)}")
        return {
            "file_exists": verify_file_exists(file_path),
            "error": str(e),
            "file_path": file_path
        }

# COMMAND ----------

def verify_file_exists(file_path):
    """
    Verify that a file exists in ADLS.
    
    Args:
        file_path: Path to verify
        
    Returns:
        Boolean indicating if file exists
    """
    try:
        file_info = dbutils.fs.ls(file_path)
        return True
    except Exception as e:
        print(f"File does not exist: {file_path}, Error: {str(e)}")
        return False

# COMMAND ----------

def copy_to_toprocess_folder_with_bucketing(entity_type, timestamp):
    """
    Copy a file from raw to toprocess folder with bucketing for songs
    
    Args:
        entity_type: Type of entity (artists, albums, songs)
        timestamp: Timestamp string for the filename
        
    Returns:
        Path to the copied file or directory
    """
    try:
        # Get paths
        storage_paths = get_storage_paths()
        source_path = f"{storage_paths['raw_data']}/{entity_type}/{entity_type}_{timestamp}.csv"
        dest_dir = f"{storage_paths['toprocess_data']}/{entity_type}"
        
        # Ensure destination directory exists
        dbutils.fs.mkdirs(dest_dir)
        
        # Process differently based on entity type
        if entity_type == "songs":
            print(f"Organizing {entity_type} data by artist_id into 10 partitions")
            
            # Read the raw CSV data into a DataFrame
            df = spark.read.csv(source_path, header=True, inferSchema=True)
            
            # Use mount point paths for consistency
            organized_dir_name = f"repartitioned_{timestamp}"
            dest_path = f"{dest_dir}/{organized_dir_name}"
            
            # Print paths for debugging
            print(f"Source path: {source_path}")
            print(f"Destination path: {dest_path}")
            
            # Use repartition to create exactly 10 partitions distributed by artist_id
            # This is simpler than hash partitioning and doesn't create partition directories
            df.repartition(10, "artist_id") \
              .write.format("parquet") \
              .mode("overwrite") \
              .save(dest_path)
            
            print(f"Saved repartitioned {entity_type} data to: {dest_path}")
            return dest_path
        else:
            # For other entity types, just copy the file as before
            dest_path = f"{dest_dir}/{entity_type}_{timestamp}.csv"
            dbutils.fs.cp(source_path, dest_path)
            print(f"Copied {entity_type} data to toprocess folder: {dest_path}")
            return dest_path
        
    except Exception as e:
        print(f"Error in organizing/copying {entity_type} file: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetching playlists automatically 

# COMMAND ----------

def fetch_spotify_playlists(sp, limit=50):
    """
    Fetch at least 50 public playlists from Spotify
    
    Args:
        sp: Authenticated Spotify client
        limit: Minimum number of playlists to fetch
        
    Returns:
        List of playlist IDs
    """
    try:
        all_playlists = []
        user_playlists = []
        count_user_playlists_failed = 0
        search_playlists = []
        count_search_fails = 0
        
        # Method 1: Get playlists from popular Spotify users
        if len(all_playlists) < limit:
            print("Fetching playlists from popular users...")
            popular_users = [
                'spotify',               # Official Spotify account
                'spotifycharts',         # Spotify Charts
                'digster.fm',            # Universal Music Group curation
                'filtr_official',        # Sony Music's playlist brand
                'topsify',               # Warner Music's playlist brand
                'indiefy',               # Independent music curator
                'edm',                   # EDM playlist curator
                'rapcaviar',             # Spotify's hip-hop brand
                'mint',                  # Spotify's EDM brand
                'peacefulpiano',         # Spotify classical & chill
                'rockthis',              # Spotify's rock music
                'spotify_uk_',           # Spotify UK
                'spotify_es',            # Spotify Spain
                'spotify_france',        # Spotify France
                'spotify_brasil',        # Spotify Brazil
                'spotify_italia',        # Spotify Italy
                'spotify_japan',         # Spotify Japan
                'spotify_india',         # Spotify India
                'spotify_mexico',        # Spotify Mexico
                'spotify_sweden',        # Spotify Sweden
                'spotify_germany',       # Spotify Germany
                'spotify_nordic',        # Spotify Nordic region
                'spotify_africa',        # Spotify Africa
                'spotify_asia',          # Spotify Asia
                'spotify_arabia',        # Spotify MENA region
                'spotify_canada',        # Spotify Canada
                'spotify_australia',     # Spotify Australia
                'spotify_latino',        # Spotify Latin music
                'spotify_philippines',   # Spotify Philippines
                'spotify_thailand',      # Spotify Thailand
                'spotify_singapore',     # Spotify Singapore
                'spotify_vietnam',       # Spotify Vietnam
            ]
            
            for username in popular_users:
                try:
                    results = sp.user_playlists(username, limit=500)
                    for playlist in results['items']:
                        if playlist.get('id'):  # Ensure we have an ID
                            user_playlists.append({
                                'id': playlist['id'],
                                'name': playlist.get('name', 'Unknown'),
                                'source': f"user_{username}"
                            })
                    
                    # Break early if we have enough playlists combined
                    if len(all_playlists) + len(user_playlists) >= limit:
                        break
                except Exception as e:
                    count_user_playlists_failed+=1
                    #print(f"Error getting playlists for user {username}: {str(e)}")
                    continue
                    
            all_playlists.extend(user_playlists)
        
        print(f"Fetching playlists from popular Spotify users: {count_user_playlists_failed} retrivals failed , {len(user_playlists)} playlists retrived successfully")
            
        # Method 2: Search for playlists (if we still need more)
        if len(all_playlists) < limit:
            print("Searching for additional playlists...")
            search_terms = [
                'top hits', 'popular', 'viral', 'trending', 'new music',
                'rock', 'pop', 'hip hop', 'rap', 'country', 'edm', 'indie',
                'workout', 'party', 'chill', 'focus', 'sleep', 'jazz'
            ]
            
            
            for term in search_terms:
                try:
                    results = sp.search(q=term, type='playlist', limit=10)
                    if 'playlists' in results and 'items' in results['playlists']:
                        for playlist in results['playlists']['items']:
                            search_playlists.append({
                                'id': playlist['id'],
                                'name': playlist['name'],
                                'source': f"search_{term}"
                            })
                    
                    # Break early if we have enough playlists combined
                    if len(all_playlists) + len(search_playlists) >= limit:
                        break
                except Exception as e:
                    count_search_fails+=1
                    #print(f"Error searching for playlists with term '{term}': {str(e)}")
                    continue
            
            all_playlists.extend(search_playlists)
        
        print(f"Fetching playlists from Search : {count_search_fails} retrivals failed , {len(search_playlists)} playlists retrived successfully")

        # Log information about the playlists we found
        print(f"Founding {len(all_playlists)} playlists:")
        
        # Remove duplicates by ID (if any)
        unique_playlists = {}
        for playlist in all_playlists:
            if playlist['id'] not in unique_playlists:
                unique_playlists[playlist['id']] = playlist

        print(f"Found {len(unique_playlists)} unique playlists:")
        # Return just the playlist IDs (limited to what was requested)
        playlist_ids = list(unique_playlists.keys())
        return playlist_ids[:limit]  # Ensure we don't return more than requested
    
    except Exception as e:
        #print(f"Error fetching playlists: {str(e)}")
        return []

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Main Extraction Logic to call all functions

# COMMAND ----------

def process_spotify_playlist(sp, playlist_id, index, all_artists, all_albums, all_songs, extraction_timestamp):
    """Process a single playlist and extract artists, albums, and songs."""
    try:
        # Get playlist name for better feedback
        playlist_info = sp.playlist(playlist_id, fields="name")
        playlist_name = playlist_info.get('name', f"Playlist {index+1}")
        
        print(f"Processing playlist: {playlist_name}")
        tracks = fetch_playlist_tracks(sp, playlist_id)
        if not tracks:
            print(f"No tracks found in playlist {playlist_name}")
            return 0, 0, 0
            
        # Extract data
        unique_artists = extract_unique_artists(tracks, extraction_timestamp)
        albums = extract_album_data(tracks, extraction_timestamp)
        songs = extract_song_data(tracks, extraction_timestamp)
        
        # Update collections
        initial_artists_count = len(all_artists)
        all_artists.update(unique_artists)
        
        # Add all albums and songs to their respective lists
        all_albums.extend(albums)
        all_songs.extend(songs)
        
        return len(unique_artists), len(albums), len(songs)
    except Exception as e:
        print(f"Error processing playlist {playlist_id}: {str(e)}")
        return 0, 0, 0

# COMMAND ----------

def extract_spotify_data():
    """Main function to extract artist, album, and song data from Spotify playlists."""
    # Get Spotify API credentials from Databricks secrets
    try:
        client_id = dbutils.secrets.get(scope="datascope", key="spotifyclientid")
        client_secret = dbutils.secrets.get(scope="datascope", key="spotifyclientsecret")
    except Exception as e:
        print(f"Error getting secrets: {str(e)}")
        return

    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    # Define playlists to process
    playlists = [
        '5muSk2zfQ3LI70S64jbrX7',  # Top English songs of all time playlist
        '0QW1hpOGFSM6o3kkmJ8gfy',  # New Indian artists playlist
        '0i2S0eEdftTrmLKueMWUKX',  # Best Hindi songs 2000-2025 playlist
        '3bDJLJzvUBxBV4C7mezz6p',  # Top 50 Hindi songs 2025 playlist
        '7qxn6GsFH77ghVtKzOnAYA',  # All Argit Sing Songs Playlist
        '3IpDoXyKOPgxJvUJYsagyM',  # Travel Songs Playlist
        '1sDbCcCj7wIGM8WByTi347',  # Road Trip Songs Playlist
        '0Gd0yQzB6wttuaLlawHlYI',  # All Out 90s playlist
        '1sDbCcCj7wIGM8WByTi347',  # Road Trips with friends playlist
        '3bDJLJzvUBxBV4C7mezz6p',  # Top 50 Hindi April 2025 
        '5C4TlFmHiMBW0E7z7Hmpal',  # New
        '2jdTwiNfzbOEh1nviJvSrm',   # Most Listened
        '30nuXwBZZBNUQ5kZhKgN4p'    #Cheap Thrils
    ]
    
    # OR fetech playlists automatically
    playlists = fetch_spotify_playlists(sp, limit=1440)
    
    # Create a timestamp for the filename and for the extraction_date field
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    extraction_timestamp = datetime.now().isoformat()
    
    all_artists = {}
    all_albums = []
    all_songs = []
    successful_playlists = 0
    
    # Process each playlist
    for index, playlist_id in enumerate(playlists):
        artists_count, albums_count, songs_count = process_spotify_playlist(
            sp, playlist_id, index, all_artists, all_albums, all_songs, extraction_timestamp
        )
        if artists_count > 0:
            successful_playlists += 1
    
    # Summary statistics
    print(f"\n=== Processing Summary ===")
    print(f"Processed {len(playlists)} playlists, {successful_playlists} successful")
    print(f"Found {len(all_artists)} unique artists across all playlists")
    print(f"Found {len(all_albums)} albums across all playlists")
    print(f"Found {len(all_songs)} songs across all playlists")
    print()
    
    # Save all data to ADLS and track results
    saved_files = []
    verification_results = {}
    
    if all_artists:
        # Save to ADLS
        artists_path = save_data_to_adls(all_artists, "artists", timestamp)
        if artists_path:
            saved_files.append({"type": "artists", "path": artists_path})
            verification_results["artists_raw"] = verify_adls_save(all_artists, "artists", artists_path)
        print(f"Artists saved to ADLS: {artists_path}")
    else:
        print("No artists found to save.")
        
    if all_albums:
        # Save to ADLS
        albums_path = save_data_to_adls(all_albums, "albums", timestamp)
        if albums_path:
            saved_files.append({"type": "albums", "path": albums_path})
            verification_results["albums_raw"] = verify_adls_save(all_albums, "albums", albums_path)
        print(f"Albums saved to ADLS: {albums_path}")
    else:
        print("No albums found to save.")
        
    if all_songs:
        # Save to ADLS
        songs_path = save_data_to_adls(all_songs, "songs", timestamp)
        if songs_path:
            saved_files.append({"type": "songs", "path": songs_path})
            verification_results["songs_raw"] = verify_adls_save(all_songs, "songs", songs_path)
        print(f"Songs saved to ADLS: {songs_path}")
    else:
        print("No songs found to save.")
    
    # Move files to toprocess folder and track results
    toprocess_files = []
    
    for file_info in saved_files:
        entity_type = file_info["type"]
        # Use the function that supports bucketing for songs
        dest_path = copy_to_toprocess_folder_with_bucketing(entity_type, timestamp)
        if dest_path:
            toprocess_files.append({"type": entity_type, "path": dest_path})
            
            # For bucketed data, we verify differently
            if entity_type == "songs":
                # For songs, just verify that the directory exists
                verification_results[f"{entity_type}_toprocess"] = {"file_exists": verify_file_exists(dest_path), "path": dest_path}
                
                # Read back the bucketed data to count records for verification
                try:
                    repartitioned_df = spark.read.parquet(dest_path)
                    repartitioned_count = repartitioned_df.count()
                    original_count = len(all_songs)
                    counts_match = repartitioned_count == original_count
                    
                    verification_results[f"{entity_type}_repartitioned"] = {
                        "original_count": original_count,
                        "repartitioned_count": repartitioned_count,
                        "counts_match": counts_match
                    }
                    
                    if counts_match:
                        print(f"Successfully verified {repartitioned_count} records in repartitioned data")
                    else:
                        print(f"Count mismatch for repartitioned {entity_type}: Original={original_count}, Repartitioned={repartitioned_count}")
                except Exception as e:
                    print(f"Error verifying repartitioned data: {str(e)}")
                    verification_results[f"{entity_type}_repartitioned"] = {"error": str(e)}
            else:
                # For other entity types, verify as before
                verification_results[f"{entity_type}_toprocess"] = {"file_exists": verify_file_exists(dest_path), "path": dest_path}
    
    # Final execution summary
    print("\n=== Extraction Result Summary ===")
    print(f"Raw files saved: {len(saved_files)}")
    print(f"Files copied to toprocess: {len(toprocess_files)}")
    
    # Return comprehensive results
    return {
        "extraction_timestamp": extraction_timestamp,
        "artists_count": len(all_artists),
        "albums_count": len(all_albums),
        "songs_count": len(all_songs),
        "raw_files": saved_files,
        "toprocess_files": toprocess_files,
        "verification_results": verification_results
    }

# COMMAND ----------

result = extract_spotify_data()
print(f"Extraction completed successfully: {result['artists_count']} artists, {result['albums_count']} albums, {result['songs_count']} songs")


# COMMAND ----------

