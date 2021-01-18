"""
Internal API for getting data from the DB with a simple, clear interface.
"""
from contextlib import closing
import os
import re

from typing import Literal,List
from datetime import date

import fastapi
from fastapi.responses import Response
from fastapi import Request

from sqlalchemy.orm import Query
from sqlalchemy.exc import ProgrammingError

import pandas as pd

from env import env 
from db import retrieve,Session
from serialization import serialize,MIME

DEFAULT_START_DATE = env.date("DEFAULT_START_DATE","1980-01-01")
DEFAULT_END_DATE = env.date("DEFAULT_END_DATE","2020-01-01")

app = fastapi.FastAPI()

def hyperlink(request,path,name):
    base = f"{request.url.scheme}://{request.url.hostname}:{request.url.port}"
    return os.path.join(base,path,name)+"/"
def sanitize(q):
    return re.sub("[^a-zA_Z0-9_]","",q)

# DATA RETRIEVAL ==========================================

def get(columns: List[str],
        start_date=env.date("DEFAULT_START_DATE","1980-01-01"),
        end_date=date.today()):

    format = "parquet"
    mime = MIME[format]

    query = retrieve(start_date,end_date,columns)
    content = serialize(query,format)

    return Response(content=content,media_type=mime)

@app.get("/queryset/{qs}")
def expanding_queryset(qs:str,
        start_date:date=DEFAULT_START_DATE,
        end_date:date=DEFAULT_END_DATE):
    columns = [qs]
    return get(columns,start_date,end_date)

@app.get("/variable/{var}") 
def wrapping_var(var:str,
        start_date:date=DEFAULT_START_DATE,
        end_date:date=DEFAULT_END_DATE):
    columns = [var]
    return get(columns,start_date,end_date)

# INTROSPECTION ===========================================

@app.get("/table/{table_name}/{column_name}/summary")
def column_summary(request:Request,table_name: str,column_name: str):
    with closing(Session()) as sess:
        safe_column_name,safe_table_name = (sanitize(q) for q in (column_name,table_name))
        try:
            query = sess.execute(f"""
                SELECT 
                    max({safe_column_name}) AS max,
                    min({safe_column_name}) AS min,
                    count({safe_column_name}) AS cnt
                FROM {env('DB_SCHEMA')}.{safe_table_name}
                WHERE {safe_column_name} IS NOT NULL

            """)
        except ProgrammingError:
            return Response(status_code=404)
        else:
            column_max,column_min,value_count = query.fetchone()
    return {
        "table":hyperlink(request,"table",table_name),
        "column_name":column_name,
        "min":column_min,
        "max":column_max,
        "count":value_count
    }

@app.get("/table/{table_name}/{column_name}/")
def column_detail(request:Request,table_name: str,column_name: str):
    with closing(Session()) as sess:
        res = sess.execute("""
            SELECT 1 FROM information_schema.columns
                WHERE column_name=:column_name
                AND table_name=:table_name
        """,{"table_name":table_name,"column_name":column_name}).fetchone()
    if res is None:
        return Response(status_code=404)
    else:
        tablelink = hyperlink(request,"table",table_name)
        return {
            "table": tablelink,
            "summary": os.path.join(tablelink,column_name,"summary")
    }

@app.get("/table/{name}/")
def table_detail(request:Request,name:str):
    """
    Retrieve metadata about a table
    """
    with closing(Session()) as sess:
        query = sess.execute("""
            SELECT column_name, data_type FROM information_schema.columns
                WHERE table_name = :name
        """,{"name":name})
        columns = query.fetchall()
        if len(columns) == 0:
            return Response(status_code=404)

    serialized = []
    for column_name,data_type in columns:
        serialized.append({
            "column":hyperlink(request,f"table/{name}",column_name),
            "column_name":column_name,
            "type":data_type,
        })

    return {"data":serialized}

@app.get("/table/")
def table_list(request:Request):
    """
    Retrieve list of tables from infoschema
    """
    with closing(Session()) as sess:
        query = sess.execute("""
            SELECT table_name FROM information_schema.tables
                WHERE table_schema = :schema 
        """,{"schema":env("DB_SCHEMA")})
        tables = query.fetchall()

    serialized = []
    for table_name,*_ in tables:
        serialized.append({
            "table":hyperlink(request,"table",table_name),
            "table_name":table_name
        })
    
    return {"data": serialized}
