"""
Internal API for getting data from the DB with a simple, clear interface.
"""
import io
from contextlib import closing
import fastapi
import querying 
import logging
from query_planning import compose_join, join_network, query_with_ops
from db import Session
from loa import index_columns
from metadata import get_reflected_metadata
from exceptions import QueryError,ConfigError
import pandas as pd

app = fastapi.FastAPI()

#app.get("/{loa}/{var}/{year}/")(querying.variable_query)

@app.get("/{loa}/{var}/{year}/{agg}/")
def get_variable_value(loa: str, var: str, year: int, agg: str):
    metadata = get_reflected_metadata()

    network = join_network(metadata.tables)
    table,variable = var.split(".")
    year_table = [tbl for tbl in metadata.tables.values() if tbl.name == "year"][0]

    with closing(Session()) as sess:
        try:
            query = query_with_ops(sess.query(), compose_join,network,
                    loa, table, variable, index_columns(loa)+[("year","year")],agg)
        except QueryError as qe:
            return fastapi.Response(str(qe),status_code=400)
        except ConfigError as ce:
            return fastapi.Response(str(ce),status_code=500)
        query = query.filter(year_table.c.year == year)
        bytes_buffer = io.BytesIO()
        pd.DataFrame(query.all()).to_parquet(bytes_buffer)
        return fastapi.Response(bytes_buffer.getvalue(),media_type="application/octet-stream")

