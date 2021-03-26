from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from postgres_azure_certificate_auth import sec_con
from settings import config

sec = {
        "sslcert": config("SSL_CERT"),
        "sslkey": config("SSL_KEY"),
        "sslrootcert": config("SSL_ROOT_CERT"),
        "password": config("DB_PASSWORD"),
    }
con = {
        "host": config("DB_HOST"),
        "user": config("DB_USER"),
    }

def get_con():
    return sec_con(sec,con,dbname=config("BASE_DB_NAME"))

engine = create_engine("postgresql://",creator=get_con)
Base = declarative_base()
Session = sessionmaker(bind=engine)
