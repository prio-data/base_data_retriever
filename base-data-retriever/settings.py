import os
from environs import Env
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.appconfiguration import AzureAppConfigurationClient
import requests

env = Env()
env.read_env()

PROD = env.bool("PRODUCTION","true")

def get_dev_kv(k):
    return requests.get(os.path.join(env.str("REST_ENV_URL",""),k)).content.decode()

if PROD:
    secret_client = SecretClient(env.str("KEY_VAULT_URL"),DefaultAzureCredential())
    app_config_client = AzureAppConfigurationClient.from_connection_string(
                get_secret("appconfig-connection-string")
            )
    get_secret = lambda k: secret_client.get_secret(k).value
    get_config = lambda k: app_config_client.get_configuration_setting(k).value
else:
    get_secret = get_dev_kv
    get_config = get_dev_kv

DB_USER = get_secret("db-user")
DB_PASSWORD = get_secret("db-password")

TIME_CASTER_URL = get_config("time-caster-url")
DB_NAME = get_config("base-db-name")
DB_SCHEMA = get_config("base-data-schema")
DB_HOST = get_config("db-host")

CACHE_DIR = env.str("CACHE_DIR","bdr-cache")
