from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from postgres_azure_certificate_auth import sec_con
from . import settings

sec = {
        "sslcert": settings.config("SSL_CERT"),
        "sslkey": settings.config("SSL_KEY"),
        "sslrootcert": settings.config("SSL_ROOT_CERT"),
        "password": settings.config("DB_PASSWORD"),
    }
con = {
        "host": settings.config("DB_HOST"),
        "user": settings.config("DB_USER"),
    }

def get_con():
    return sec_con(sec,con,dbname=settings.config("BASE_DB_NAME"))

engine = create_engine("postgresql://",creator=get_con)
Base = declarative_base()
Session = sessionmaker(bind=engine)
