
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import settings

def connection_string(host,user,password,dbname):
    return f"postgresql+psycopg2://{user}:{password}@{host}/{dbname}?sslmode=require"

engine = create_engine(connection_string(
        user = settings.DB_USER,password=settings.DB_PASSWORD,
        host = settings.DB_HOST,dbname=settings.DB_NAME
    ))
        
Base = declarative_base()
Session = sessionmaker(bind=engine)
