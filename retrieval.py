from typing import List
from contextlib import closing

from pydantic import BaseModel
import sqlalchemy as sa
from db import engine,Session
from env import env

meta = sa.MetaData(schema=env("UOA_SCHEMA"))

class Query(BaseModel):
    columns: List[str]

def query(uoa:str,query:Query):
    """
    Retrieve queried data, filtering as needed
    """
    table = sa.Table(uoa,meta,autoload_with=engine)
    with closing(Session()) as sess:
        columns = [table.c[col] for col in query.columns]
        q = sess.query(*columns)
        # q = q.filter(... TODO!!
        data = q.all()
    return data 

def column(uoa:str,column:str):
    q = Query(columns=[column])
    data = query(uoa,q)
    return data
