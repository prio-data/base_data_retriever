"""
LOA metadata

Might eventually be replaced by API calls to a service that manages these.
"""
LOAS = {
    "priogrid_month":{
        "index_columns":[
                "month.id",
                "priogrid.gid",
            ],
    },
    "country_month":{
        "index_columns":[
                "month.id",
                "country.gwcode",
            ],
    },
}

def index_columns(loa:str):
    return [c.split(".") for c in LOAS[loa]["index_columns"]]

def grouping_column(loa:str):
    return LOAS[loa]["grouping_column"].split(".")
