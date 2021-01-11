import os
from contextlib import closing

import fastapi

import psycopg2

from pydantic import BaseModel
from environs import Env

env = Env()
env.read_env()

app = fastapi.FastAPI()

connectionString = f"""
    host={env("DB_HOST")}
    port={env("DB_PORT")}
    user={env("DB_USER")}
    dbname={env("DB_DBNAME")}
    dbname={env("DB_DBNAME")}
    sslrootcert={os.path.join(env("BASE"),env("DB_SSL_ROOTCERT"))}
    sslcert={os.path.join(env("BASE"),env("DB_SSL_CERT"))}
    sslkey={os.path.join(env("BASE"),env("DB_SSL_KEY"))}
"""

con=psycopg2.connect(connectionString)

@app.get("/")
def echo():
    with closing(psycopg2.connect(connectionString)) as con:
        c = con.cursor()
        c.execute("SELECT version()")
        return str(c.fetchone())
