"""
Internal API for getting data from the DB with a simple, clear interface.
"""
import fastapi

from uoa import uoa_list,uoa_post,uoa_detail
from retrieval import query,column
from decorators import parquet_bytes,csv_text

app = fastapi.FastAPI()

app.get("/uoa/{name}")(uoa_detail)
app.get("/uoa/")(uoa_list)
app.post("/uoa/")(uoa_post)
app.post("/data/{uoa}")(csv_text(query))
app.get("/data/{uoa}/{column}")(csv_text(column))
