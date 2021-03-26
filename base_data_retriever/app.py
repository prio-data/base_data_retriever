"""
Internal API for getting data from the DB with a simple, clear interface.
"""
import io
import logging
from contextlib import closing

import fastapi
import pandas as pd

from query_planning import compose_join, join_network, query_with_ops
from db import Session
from loa import index_columns
from metadata import get_reflected_metadata
from exceptions import QueryError,ConfigError
from settings import config

try:
    logging.basicConfig(level=getattr(logging,config("LOG_LEVEL")))
except AttributeError:
    pass

logger = logging.getLogger(__name__)

app = fastapi.FastAPI()

@app.get("/{loa}/{var}/{agg}/")
def get_variable_value(loa: str, var: str, agg: str):
    logger.info("Getting join composition prerequisites")
    metadata = get_reflected_metadata()
    network = join_network(metadata.tables)

    table,variable = var.split(".")
    with closing(Session()) as sess:
        logger.info("Composing query")
        try:
            query = query_with_ops(sess.query(), compose_join,network,
                    loa, table, variable, index_columns(loa),agg)
        except QueryError as qe:
            logger.error("QueryError: %s",str(qe))
            return fastapi.Response(str(qe),status_code=400)
        except ConfigError as ce:
            logger.error("ConfigError: %s",str(ce))
            return fastapi.Response(str(ce),status_code=500)

        bytes_buffer = io.BytesIO()

        logger.debug("Executing %s",str(query))

        logger.info("Fetching data")
        dataframe = pd.read_sql_query(query.statement,sess.connection())
        logger.info("Got %s rows",str(dataframe.shape[0]))

        try:
            loa_indices = ["_".join((tbl,col)) for tbl,col in index_columns(loa)]
            dataframe.set_index(loa_indices,inplace=True)
        except KeyError:
            missing_idx = set(loa_indices).difference(dataframe.columns)
            logger.error("Missing index columns: %s",", ".join(missing_idx))
            return fastapi.Response("Couldn't set index.", status_code=500)
    
        logger.debug("Sorting dataframe")
        dataframe.sort_index(inplace=True)

        dataframe.to_parquet(bytes_buffer)
        return fastapi.Response(bytes_buffer.getvalue(),media_type="application/octet-stream")
