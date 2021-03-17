"""
Internal API for getting data from the DB with a simple, clear interface.
"""
import io
from contextlib import closing
import fastapi

import pandas as pd

from query_planning import compose_join, join_network, query_with_ops
from db import Session
from loa import index_columns
from metadata import get_reflected_metadata
from exceptions import QueryError,ConfigError

app = fastapi.FastAPI()

@app.get("/{loa}/{var}/{agg}/")
def get_variable_value(loa: str, var: str, agg: str):
    metadata = get_reflected_metadata()

    network = join_network(metadata.tables)
    table,variable = var.split(".")

    with closing(Session()) as sess:
        try:
            query = query_with_ops(sess.query(), compose_join,network,
                    loa, table, variable, index_columns(loa),agg)
        except QueryError as qe:
            return fastapi.Response(str(qe),status_code=400)
        except ConfigError as ce:
            return fastapi.Response(str(ce),status_code=500)

        bytes_buffer = io.BytesIO()

        dataframe = pd.DataFrame(query.all())
        dataframe = dataframe.set_index(["_".join((tbl,col)) for tbl,col in index_columns(loa)])
        dataframe.to_parquet(bytes_buffer)

        return fastapi.Response(bytes_buffer.getvalue(),media_type="application/octet-stream")

