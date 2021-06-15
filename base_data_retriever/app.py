"""
Internal API for getting data from the DB with a simple, clear interface.
"""
import io
import logging
from contextlib import closing

from fastapi import Depends, Response
import fastapi
import pandas as pd

from base_data_retriever import __version__

from . import settings, exceptions, reflection, db, query_planning

try:
    logging.basicConfig(level=getattr(logging,settings.config("LOG_LEVEL")))
except AttributeError:
    pass

logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
logger.setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

app = fastapi.FastAPI()

with closing(db.Session()) as sess:
    metadata = reflection.reflect_metadata(
            sess.connection(),
            settings.config("BASE_DATA_SCHEMA"))

def get_sess():
    session = db.Session()
    try:
        yield session
    finally:
        session.close()

@app.get("/{loa}/{var}/{agg}/")
def get_variable_value(
        loa: str,
        var: str,
        agg: str,
        session = Depends(get_sess)):
    network = query_planning.join_network(metadata.tables)

    table,variable = var.split(".")

    logger.info("Composing query")
    try:
        query = query_planning.query_with_ops(session.query(), query_planning.compose_join,network,
                loa, table, variable, settings.index_columns(loa),agg)
    except exceptions.QueryError as qe:
        logger.error("exceptions.QueryError: %s",str(qe))
        return fastapi.Response(str(qe),status_code=400)
    except exceptions.ConfigError as ce:
        logger.error("exceptions.ConfigError: %s",str(ce))
        return fastapi.Response(str(ce),status_code=500)

    bytes_buffer = io.BytesIO()

    logger.debug("Executing %s",str(query))

    logger.info("Fetching data")
    dataframe = pd.read_sql_query(query.statement,session.connection())
    logger.info("Got %s rows",str(dataframe.shape[0]))

    try:
        loa_indices = ["_".join((tbl,col)) for tbl,col in settings.index_columns(loa)]
        dataframe.set_index(loa_indices,inplace=True)
    except KeyError:
        missing_idx = set(loa_indices).difference(dataframe.columns)
        logger.error("Missing index columns: %s",", ".join(missing_idx))
        return fastapi.Response("Couldn't set index.", status_code=500)
    logger.debug("Sorting dataframe")
    dataframe.sort_index(inplace=True)

    dataframe.to_parquet(bytes_buffer)
    return fastapi.Response(bytes_buffer.getvalue(),media_type="application/octet-stream")

@app.get("/")
def handshake():
    return {
        "version": __version__
        }

@app.get("/tables/")
def list_tables():
    return {
        "tables": [table.name for table in metadata.tables.values()]
    }

@app.get("/tables/{table_name}/")
def show_table(table_name: str):
    table = [table for table in metadata.tables.values() if table.name == table_name]

    if table:
        table,*_ = table
        return {
            "name": table_name,
            "columns": [column.name for column in table.columns]
        }
    else:
        return Response(status_code = 404)
