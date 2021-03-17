from environs import Env
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.appconfiguration import AzureAppConfigurationClient

env = Env()
env.read_env()

# = = = = = = = = = = = = = = = = = = = = =
# SECRETS = = = = = = = = = = = = = = = = =
# = = = = = = = = = = = = = = = = = = = = =

secret_client = SecretClient(env.str("KEY_VAULT_URL"),DefaultAzureCredential())
get_secret = lambda k: secret_client.get_secret(k).value

DB_USER = get_secret("db-user")
DB_PASSWORD = get_secret("db-password")

# = = = = = = = = = = = = = = = = = = = = =
# CONFIG= = = = = = = = = = = = = = = = = =
# = = = = = = = = = = = = = = = = = = = = =

app_config_client = AzureAppConfigurationClient.from_connection_string(
            get_secret("app-settings-connection-string")
        )

get_config = lambda k: app_config_client.get_configuration_setting(k).value

TIME_CASTER_URL = get_config("time-caster-url")
DB_NAME = get_config("base-db-name")
DB_SCHEMA = get_config("base-data-schema")
DB_HOST = get_config("db-host")
