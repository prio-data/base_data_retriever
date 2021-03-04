"""
Internal API for getting data from the DB with a simple, clear interface.
"""
import fastapi
import querying 
import logging

app = fastapi.FastAPI()

app.get("/{loa}/{var}/{year}/")(querying.variable_query)
