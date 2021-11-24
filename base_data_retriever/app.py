"""
app
===

Internal API for getting data from the DB with a simple, clear interface.
"""
from typing import Optional
import io
import logging

from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session
from environs import EnvError
from fastapi import Depends, Response
import fastapi
import pandas as pd
import views_schema as schema
import views_query_planning

from base_data_retriever import __version__
from . import settings, db, models

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
            schema   = settings.BASE_DB_SCHEMA
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

async def with_loa_db_session()-> Session:
    try:
        con = loa_dblayer.Session()
        yield con
    finally:
        con.close()

async def with_base_db_connection()-> Connection:
    try:
        con = base_dblayer.connect()
        yield con
    finally:
        con.close()

async def with_loa_model(loa_name: str, session: Session = Depends(with_loa_db_session)) -> models.LevelOfAnalysis:
    loa = session.query(models.LevelOfAnalysis).get(loa_name)

    if loa is None:
        if loa_name in settings.DEFAULT_LOAS:
            logger.info(f"LOA {loa_name} defined from defaults.")
            time_index, unit_index = settings.DEFAULT_LOAS[loa_name]["index_columns"]
            loa = models.LevelOfAnalysis(
                    name = loa_name,
                    time_index = time_index,
                    unit_index = unit_index,
                )
            session.add(loa)
            session.commit()
        else:
            logger.warning(f"LOA {loa_name} requested, but not found")

    yield loa


async def with_query_composer(loa: models.LevelOfAnalysis = Depends(with_loa_model))-> Optional[views_query_planning.QueryComposer]:
    if loa is None:
        yield loa
    else:
        yield views_query_planning.QueryComposer(base_dblayer.join_network, loa.name, loa.time_index, loa.unit_index)

app = fastapi.FastAPI()

@app.get("/fetch/{loa_name}/{variable}/{aggregation_function}/")
def get_variable_value(
        variable: str,
        aggregation_function: str,
        composer: Optional[views_query_planning.QueryComposer] = Depends(with_query_composer),
        database_connection = Depends(with_base_db_connection),
        )-> Response:

    def error_response(message: str):
        return Response(message, status_code = 400)

    def data_response(query: str):
        logger.debug("Executing %s",str(query))
        dataframe = pd.read_sql_query(query, database_connection)
        logger.info(f"Got {dataframe.shape[0]} rows")
        bytes_buffer = io.BytesIO()
        dataframe.to_parquet(bytes_buffer)
        return Response(bytes_buffer.getvalue(), media_type="application/octet-stream")

    if composer is None:
        return Response("LOA not defined", status_code = 404)

    try:
        table, column = variable.split(".")
    except ValueError:
        return Response(
                f"Failed to parse variable {variable}. "
                "It should be in the format table.column",
                status_code=400)

    query = composer.expression(table, column, aggregation_function)
    return query.either(error_response, data_response)

@app.get("/loa/")
def list_levels_of_analysis(session = Depends(loa_dblayer.session_dependency)):
    loas = [mdl.pydantic() for mdl in session.query(models.LevelOfAnalysis).all()]
    return {"levels_of_analysis": loas}

@app.post("/loa/")
def define_level_of_analysis(session = Depends(loa_dblayer.session_dependency)):
    session
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
