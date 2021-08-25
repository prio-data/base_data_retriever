import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from .settings import config

connection_string = ("postgresql+psycopg2://"
        f"{config('DB_USER')}@{config('DB_HOST')}:{config('DB_PORT')}/{config('DB_NAME')}"
        )

if os.path.exists(os.path.expanduser("~/.postgresql")):
    connection_string += "?sslmode=require"

engine = create_engine(connection_string)

Base = declarative_base()
Session = sessionmaker(bind=engine)
