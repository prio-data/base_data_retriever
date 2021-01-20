import os

from contextlib import closing

from fastapi import Response

from sqlalchemy import create_engine,MetaData,Table,inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from psycopg2 import connect

from calls import cast_date_to_mid
from env import env

ID_COLUMNS = ["gid","month_id"]

connection_string = f"""
    host={env("DB_HOST")}
    port={env("DB_PORT")}
    user={env("DB_USER")}
    dbname={env("DB_DBNAME")}
    dbname={env("DB_DBNAME")}
    sslrootcert={os.path.join(env("BASE"),env("DB_SSL_ROOTCERT"))}
    sslcert={os.path.join(env("BASE"),env("DB_SSL_CERT"))}
    sslkey={os.path.join(env("BASE"),env("DB_SSL_KEY"))}
    """

def getconn():
    return connect(connection_string)

engine = create_engine("postgresql+psycopg2://",creator=getconn)

staging_meta = MetaData(schema=env("STAGING_SCHEMA"))
uoa_meta = MetaData(schema=env("UOA_SCHEMA"))

Base = declarative_base()

inspector = inspect(engine)
Session = sessionmaker(bind=engine)
