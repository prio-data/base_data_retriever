import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from .settings import config

def get_con():
    return psycopg2.connect(
            f"host={config('DB_HOST')} "
            f"port={config('DB_PORT')} "
            f"user={config('DB_USER')} "
            f"dbname={config('DB_NAME')} "
            f"sslmode=allow "
        )

engine = create_engine("postgresql+psycopg2://", creator = get_con)

Base = declarative_base()
Session = sessionmaker(bind=engine)
