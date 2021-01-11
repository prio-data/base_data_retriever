"""
Internal API for getting data from the DB with a simple, clear interface.
"""
import os

from typing import Literal 
from datetime import date

import fastapi
from fastapi.responses import Response

from sqlalchemy.orm import Query

import pandas as pd

from env import env 
from db import retrieve
from serialization import serialize,MIME

app = fastapi.FastAPI()

@app.get("/")
def get(start_date=env.date("DEFAULT_START_DATE","1980-01-01"),
        end_date=date.today(),
        column: str = "ged_best_ns",
        format: Literal["csv","json","parquet"] = "csv",
        download: bool = False):

    mime = MIME[format]

    query = retrieve(start_date,end_date,[column])
    content = serialize(query,format)

    headers = {}
    if download:
        fname = f"data.{format}"
        headers["Content-Disposition"] = f"attachment; filename=\"{fname}\""
    else:
        headers["Content-Disposition"] = "inline" 

    return Response(content=content,media_type=mime,headers=headers)

"""
@app.post("/")
def get(query:Query):
    query = retrieve(query.start_date,query.end_date,[query.columns])
    content = serialize(query,format)
    return Response(content=content,media_type=mime)
    """
