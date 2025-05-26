# Databricks notebook source
# MAGIC %md
# MAGIC ## Accessing Azure Data Lake using Service Principal

# COMMAND ----------

clientid = "--confidentially removed--" 
tenantid = "--confidentially removed--"
secretid = "--confidentially removed--"


# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.your_storage_account.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.your_storage_account.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.your_storage_account.dfs.core.windows.net", clientid)
spark.conf.set("fs.azure.account.oauth2.client.secret.your_storage_account.dfs.core.windows.net", secretid)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.your_storage_account.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenantid}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@your_storage_account.dfs.core.windows.net/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mounting Azure Data Lake Storage to Databricks

# COMMAND ----------

# GET SECRET
def get_secret(key_name: str, secret_scope_name: str):
    try:
        return dbutils.secrets.get(scope=secret_scope_name, key=key_name)
    except Exception as e:
        print(f"Error getting secret {key_name}: {str(e)}")

# COMMAND ----------

# MOUNT CONTAINERS
def mount_all_containers(containers: list, storage_account_name: str, secret_scope_name: str ):
    """Mount all required containers for the Spotify ETL pipeline
    
    Args:
        containers: List of container dictionaries with 'name' and 'mount_point' keys
        storage_account_name: Azure Storage account name
        secret_scope_name: Databricks secret scope name linked to Azure Key Vault
    """
    try:
        # Create OAuth configuration
        configs = {
            "fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": get_secret("clientid", secret_scope_name),
            "fs.azure.account.oauth2.client.secret": get_secret("secretid", secret_scope_name),
            "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{get_secret('tenantid', secret_scope_name)}/oauth2/token"
        }
        
        # Verify all required values are present
        missing_values = [k for k, v in configs.items() if v is None]
        if missing_values:
            print(f"Missing OAuth configuration values: {', '.join(missing_values)}")
            return False
        
        # Track mount results
        successful_mounts = []
        failed_mounts = []
        
        # Mount each container
        for container in containers:
            mount_point = container["mount_point"]
            container_name = container["name"]
            
            try:
                # Skip if already mounted
                if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
                    print(f" Already mounted: {mount_point}")
                    successful_mounts.append(container_name)
                    continue
                
                # Mount the storage
                source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"
                print(f"Mounting {source} to {mount_point}")
                
                dbutils.fs.mount(
                    source=source,
                    mount_point=mount_point,
                    extra_configs=configs
                )
                
                # Verify the mount was successful
                if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
                    print(f" Successfully mounted: {mount_point}")
                    successful_mounts.append(container_name)
                else:
                    print(f" Mount failed for: {mount_point}")
                    failed_mounts.append(container_name)
                    
            except Exception as e:
                print(f" Error mounting {container_name}: {str(e)}")
                failed_mounts.append(container_name)
        
        # Print summary
        print(f"\n=== Mount Summary ===")
        if successful_mounts:
            print(f" Successfully mounted: {', '.join(successful_mounts)}")
        if failed_mounts:
            print(f" Failed to mount: {', '.join(failed_mounts)}")
        
        # All mounts successful if no failures
        return len(failed_mounts) == 0
        
    except Exception as e:
        print(f"Error in mount operation: {str(e)}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ### POC for accessing secrets

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

display(dbutils.secrets.listScopes())
dbutils.secrets.list(scope="datascope")

# COMMAND ----------

dbutils.secrets.get(scope="datascope", key="secretid")


# COMMAND ----------

def display_mounts():
    """Display current mount points"""
    print("\n=== Current Mount Points ===")
    for mount in dbutils.fs.mounts():
        print(f"  {mount.mountPoint} -> {mount.source}")
display_mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mounting all the containers

# COMMAND ----------

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