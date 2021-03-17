import os
import pickle
from db import engine
from sqlalchemy import MetaData
from settings import DB_SCHEMA


def get_reflected_metadata():
    cache_file = "/tmp/md.pckl"
    if not os.path.exists(cache_file):
        meta = MetaData(bind=engine,schema=DB_SCHEMA)
        meta.reflect()
        with open(cache_file,"wb") as f:
            pickle.dump(meta,f)
    else:
        with open(cache_file,"rb") as f:
            meta = pickle.load(f)
    return meta
