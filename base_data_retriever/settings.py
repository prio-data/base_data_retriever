from environs import Env
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from azure.appconfiguration import AzureAppConfigurationClient

env = Env()
env.read_env()

APP_CONFIG_CONNECTION_STRING = (SecretClient(env.str("KEY_VAULT_URL"),DefaultAzureCredential())
    .get_secret("app-settings-connection-string")
    .value
    )

app_config_client = AzureAppConfigurationClient.from_connection_string(APP_CONFIG_CONNECTION_STRING)

TIME_CASTER_URL = app_config_client.get_configuration_setting("time-caster-url").value

DB_SCHEMA = app_config_client.get_configuration_setting("base-data-retriever-schema").value
