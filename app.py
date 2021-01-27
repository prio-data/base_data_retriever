"""
Internal API for getting data from the DB with a simple, clear interface.
"""
from typing import Optional,List
from datetime import date
import fastapi
from pydantic import ValidationError
from pydantic import BaseModel
from fastapi import Response,Request
import querying 

app = fastapi.FastAPI()

app.post("/{loa}/")(querying.json_query)
app.get("/{loa}/{var}/")(querying.variable_query)
