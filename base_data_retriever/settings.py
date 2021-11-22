"""
Required settings:
* Config:
    - DB_HOST
    - DB_PORT
    - DB_NAME
    - DB_USER
    - DB_SCHEMA
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

"""
LOA metadata

Might eventually be replaced by API calls to a service that manages these.
"""

DEFAULT_LOAS = {
    "priogrid_month":{
        "index_columns":[
                "priogrid_month.month_id",
                "priogrid_month.priogrid_gid",
            ],
    },
    "priogrid_year":{
        "index_columns":[
                "priogrid_year.year_id",
                "priogrid_year.priogrid_gid",
            ],
    },

    "country_month":{
        "index_columns":[
                "country_month.month_id",
                "country_month.country_id",
            ],
    },
    "country_year":{
        "index_columns":[
                "country_year.year_id",
                "country_year.country_id",
            ]

    },

    "actor_month":{
        "index_columns":[
            "actor_month.month_id",
            "actor_month.actor_id",
        ]
    },

    "actor_year":{
        "index_columns":[
            "actor_year.year_id",
            "actor_year.actor_id",
        ]
    },
}
