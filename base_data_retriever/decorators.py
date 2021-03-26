from functools import wraps
from fastapi import Response
from io import BytesIO,StringIO

import pandas as pd

def parquet_bytes(fn):
    @wraps(fn)
    def inner(*args,**kwargs):
        pqbuffer = BytesIO()
        result = fn(*args,**kwargs)
        df = pd.DataFrame(result)
        df.to_parquet(pqbuffer)
        return Response(content=pqbuffer.getvalue(),media_type="application/octet-stream")
    return inner

def csv_text(fn):
    @wraps(fn)
    def inner(*args,**kwargs):
        csvbuffer = StringIO()
        result = fn(*args,**kwargs)
        df = pd.DataFrame(result)
        df.to_csv(csvbuffer,index=False)
        return Response(content=csvbuffer.getvalue())
    return inner
