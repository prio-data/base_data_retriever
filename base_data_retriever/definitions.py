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

"""
Aggregation functions

The list of allowed aggregation functions.
"""
AGGREGATION_FUNCTIONS = [
        "sum",
        "max",
        "min",
        "avg",
    ]
