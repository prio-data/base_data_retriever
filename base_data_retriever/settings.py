"""
settings
========

These are the required environment settings that the service needs to operate.
Most have defaults that may or may not be helpful.

The LOA database is by default defined to be the same as the base DB, but it is
useful to use a separate database for storing LOAs since the LOA database
schema is managed by Alembic.
"""
from typing import Optional
import environs

env = environs.Env()
env.read_env()

BASE_DB_HOST                    = env.str("BASE_DATA_RETRIEVER_DB_HOST","127.0.0.1")
BASE_DB_PORT                    = env.int("BASE_DATA_RETRIEVER_DB_PORT",5432)
BASE_DB_USER                    = env.str("BASE_DATA_RETRIEVER_DB_USER","postgres")
BASE_DB_NAME                    = env.str("BASE_DATA_RETRIEVER_DB_NAME","postgres")
BASE_DB_PASSWORD: Optional[str] = env.str("BASE_DATA_RETRIEVER_DB_PASSWORD",None)
BASE_DB_SCHEMA: Optional[str]   = env.str("BASE_DATA_RETRIEVER_DB_SCHEMA",None)
BASE_DB_SSLMODE                 = env.str("BASE_DATA_RETRIEVER_DB_SSLMODE","allow")

LOA_DB_HOST                     = env.str("BASE_DATA_RETRIEVER_LOA_DB_HOST",BASE_DB_HOST)
LOA_DB_PORT                     = env.int("BASE_DATA_RETRIEVER_LOA_DB_PORT",BASE_DB_PORT)
LOA_DB_USER                     = env.str("BASE_DATA_RETRIEVER_LOA_DB_USER",BASE_DB_USER)
LOA_DB_NAME                     = env.str("BASE_DATA_RETRIEVER_LOA_DB_NAME",BASE_DB_NAME)
LOA_DB_PASSWORD: Optional[str]  = env.str("BASE_DATA_RETRIEVER_LOA_DB_PASSWORD",BASE_DB_PASSWORD)
LOA_DB_SCHEMA                   = env.str("BASE_DATA_RETRIEVER_LOA_DB_SCHEMA","public")
LOA_DB_SSLMODE                  = env.str("BASE_DATA_RETRIEVER_LOA_DB_SSLMODE",BASE_DB_SSLMODE)

LOG_LEVEL                       = env.str("LOG_LEVEL", "WARNING")
