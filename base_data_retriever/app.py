"""
Internal API for getting data from the DB with a simple, clear interface.
"""
import io
import logging

from environs import EnvError
from fastapi import Depends, Response
import fastapi
import pandas as pd
import views_schema as schema

import views_query_planning
from base_data_retriever import __version__

from . import settings, exceptions, db, models

try:
    logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL))
except EnvError:
    pass

logger = logging.getLogger(__name__)

base_dblayer = db.BaseDatabaseLayer(
            host     = settings.BASE_DB_HOST,
            port     = settings.BASE_DB_PORT,
            user     = settings.BASE_DB_USER,
            name     = settings.BASE_DB_NAME,
            sslmode  = settings.BASE_DB_SSLMODE,
            password = settings.BASE_DB_PASSWORD,
        )

loa_dblayer = db.DatabaseLayer(
            host     = settings.LOA_DB_HOST,
            port     = settings.LOA_DB_PORT,
            user     = settings.LOA_DB_USER,
            name     = settings.LOA_DB_NAME,
            sslmode  = settings.LOA_DB_SSLMODE,
            password = settings.LOA_DB_PASSWORD,
            metadata = models.Base.metadata
        )

logger = logging.getLogger(__name__)

app = fastapi.FastAPI()

@app.get("/fetch/{loa}/{var}/{agg}/")
def get_variable_value(
        loa: str,
        var: str,
        agg: str,
        base_session = Depends(base_dblayer.session_dependency),
        loa_session = Depends(loa_dblayer.session_dependency)
        ):

    loa = loa_session.query(models.LevelOfAnalysis).get(loa)
    if loa is None:
        if loa in settings.DEFAULT_LOAS:
            logger.info(f"LOA {loa} defined from defaults.")
            time_index, unit_index = settings.DEFAULT_LOAS[loa]["index_columns"]
            loa = models.LevelOfAnalysis(
                    name = loa,
                    time_index = time_index,
                    unit_index = unit_index,
                )
            loa_session.add(loa)
            loa_session.commit()
        else:
            logger.warning(f"LOA {loa} requested, but not found")
            return Response(f"Loa {loa} is not defined", status_code = 400)

    table,variable = var.split(".")

    try:
        query = views_query_planning.query_with_ops(
                base_session.query(),
                views_query_planning.compose_join,
                base_dblayer.join_network,
                loa,
                table,
                variable,
                (loa.time_index, loa.unit_index),
                agg)

    except exceptions.QueryError as qe:
        logger.error("exceptions.QueryError: %s",str(qe))
        return fastapi.Response(str(qe),status_code=400)
    except exceptions.ConfigError as ce:
        logger.error("exceptions.ConfigError: %s",str(ce))
        return fastapi.Response(str(ce),status_code=500)
    except exceptions.AggregationNameError as ane:
        return fastapi.Response(str(ane),status_code=400)

    bytes_buffer = io.BytesIO()

    logger.debug("Executing %s",str(query))

    logger.info("Fetching data")
    dataframe = pd.read_sql_query(query.statement,base_session.connection())
    logger.info("Got %s rows",str(dataframe.shape[0]))

    try:
        loa_indices = ["_".join((loa.name, col)) for col in (loa.time_index, loa.unit_index)]
        dataframe.set_index(loa_indices,inplace=True)
    except KeyError:
        missing_idx = set(loa_indices).difference(dataframe.columns)
        logger.error("Missing index columns: %s",", ".join(missing_idx))
        return fastapi.Response("Couldn't set index.", status_code=500)
    logger.debug("Sorting dataframe")
    dataframe.sort_index(inplace=True)

    dataframe.to_parquet(bytes_buffer)
    return fastapi.Response(bytes_buffer.getvalue(),media_type="application/octet-stream")

@app.get("/loa/")
def list_levels_of_analysis(session = Depends(loa_dblayer.session_dependency)):
    loas = [mdl.pydantic() for mdl in session.query(models.LevelOfAnalysis).all()]
    return {"levels_of_analysis": loas}

@app.post("/loa/")
def define_level_of_analysis(session = Depends(loa_dblayer.session_dependency)):
    pass

@app.get("/loa/{name:str}/")
def level_of_analysis_detail(name: str, session = Depends(loa_dblayer.session_dependency)):
    level_of_analysis = session.query(models.LevelOfAnalysis).get(name)
    if level_of_analysis is None:
        return Response(status_code = 404)
    else:
        return level_of_analysis.pydantic()

@app.get("/")
def handshake():
    return {
        "version": __version__
        }

@app.get("/tables")
def list_tables()-> schema.DocumentationEntry:
    table_names = [tbl.name for tbl in base_dblayer.tables.values()]
    tables = [schema.DocumentationEntry(name=n, path=n) for n in table_names]
    return {
        "name": "tables",
        "entries": tables
        }

@app.get("/tables/{table_name}")
def show_table(table_name: str)-> schema.DocumentationEntry:
    table = [table for table in base_dblayer.tables.values() if table.name == table_name]

    if table:
        table,*_ = table
        column_names = [column.name for column in table.columns]
        columns = [schema.DocumentationEntry(name = n, path = n) for n in column_names]
        return {
            "name": table_name,
            "entries": columns
        }
    else:
        return Response(status_code = 404)

@app.get("/tables/{table_name}/{column_name}")
def show_column(table_name: str, column_name: str)-> schema.DocumentationEntry:
    table = [table for table in base_dblayer.tables.values() if table.name == table_name]
    if table and (column := [column for column in table[0].columns if column.name == column_name]):
        column,*_ = column
        return {
                "name": column.name
            }
    else:
        return Response(status_code = 404)
