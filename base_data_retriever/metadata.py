import os
import pickle
from db import engine
from sqlalchemy import MetaData
from settings import DB_SCHEMA
from cache import cache

def get_reflected_metadata():
    try:
        meta = cache.get("database-reflection")
    except KeyError:
        meta = MetaData(bind=engine,schema=DB_SCHEMA)
        meta.reflect()
        cache.set("database-reflection",meta)
    return meta
