"""
Required settings:
* Env:
    - KEY_VAULT_URL
* Secrets:
    - DB_USER
    - DB_PASSWORD
    - BLOB_STORAGE_CONNECTION_STRING
* Config:
    - BASE_DB_NAME
    - BASE_DATA_SCHEMA
    - DB_HOST
    - BLOB_STORAGE_GENERIC_CACHE
    - LOG_LEVEL
"""
import environs
from fitin import views_config

env = environs.Env()
env.read_env()
config = views_config(env.str("KEY_VAULT_URL"))
