from environs import Env

env = Env()
env.read_env()

APP_CONFIG_CONNECTION_STRING = env.str("APP_CONFIG_CONNECTION_STRING")
TIMECASTER_URL = env.str("TIMECASTER_URL")
DB_SCHEMA = env.str("DB_SCHEMA")
