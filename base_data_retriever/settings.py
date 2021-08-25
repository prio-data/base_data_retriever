"""
Required settings:
* Config:
    - DB_HOST
    - DB_PORT
    - DB_NAME
    - DB_USER
    - DB_SCHEMA
"""

import environs
env = environs.Env()
env.read_env()
config = env

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
