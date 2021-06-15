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

"""
LOA metadata

Might eventually be replaced by API calls to a service that manages these.
"""
LOAS = {
    "priogrid_month":{
        "index_columns":[
                "priogrid_month.month_id",
                "priogrid_month.priogrid_gid",
            ],
    },
    "country_month":{
        "index_columns":[
                "country_month.month_id",
                "country.gwcode",
            ],
    },
}

def index_columns(loa:str):
    return [c.split(".") for c in LOAS[loa]["index_columns"]]

def grouping_column(loa:str):
    return LOAS[loa]["grouping_column"].split(".")
