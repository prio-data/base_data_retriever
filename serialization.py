import io
import json
from sqlalchemy.orm import Query

import pandas as pd

MIME = {
    "csv":"text/plain",
    "json":"application/json",
    "parquet":"application/octet-stream"
    }

def query_to_csv(q: Query):
    d = pd.DataFrame(q)
    return d.to_csv(index=False)

def query_to_json(q:Query):
    names = [cd["name"] for cd in q.column_descriptions]

    l = []
    for r in q.all():
        l.append({k:v for k,v in zip(names,r)})

    return json.dumps(l)

def query_to_parquet(q:Query):
    d = pd.DataFrame(q)
    bio = io.BytesIO()
    d.to_parquet(bio, compression="gzip")
    return bio.getvalue()

def serialize(q,format):
    serializers = {
            "csv":query_to_csv,
            "json":query_to_json,
            "parquet":query_to_parquet,
            }

    try:
        return serializers[format](q)
    except KeyError:
        raise ValueError(f"No serialization method registered for {self.format}")
