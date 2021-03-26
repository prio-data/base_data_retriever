import os
import environs
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.appconfiguration import AzureAppConfigurationClient
import requests

env = environs.Env()
env.read_env()

PROD = env.bool("PRODUCTION","true")
KEY_VAULT_URL = env.str("KEY_VAULT_URL")

secret_client = SecretClient(KEY_VAULT_URL,DefaultAzureCredential())
get_secret = lambda k: secret_client.get_secret(k).value
app_config_client = AzureAppConfigurationClient.from_connection_string(
            get_secret("appconfig-connection-string")
        )
get_config = lambda k: app_config_client.get_configuration_setting(k).value

DB_USER = get_secret("db-user")
DB_PASSWORD = get_secret("db-password")
BLOB_STORAGE_CONNECTION_STRING = get_secret(
        "blob-storage-connection-string"
        )

DB_NAME = get_config("base-db-name")
DB_SCHEMA = get_config("base-data-schema")
DB_HOST = get_config("db-host")

BLOB_STORAGE_GEN_CACHE_CONTAINER = get_config(
        "blob-storage-generic-cache"
        )

LOG_LEVEL = get_config("log-level")
