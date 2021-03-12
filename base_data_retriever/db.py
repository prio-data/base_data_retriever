
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from postgres_azure_certificate_auth import secure_connect_ac
import settings

def getconn():
    return secure_connect_ac(settings.APP_CONFIG_CONNECTION_STRING)

engine = create_engine("postgresql+psycopg2://",
        creator=getconn,
        pool_pre_ping=True)
        
Base = declarative_base()
Session = sessionmaker(bind=engine)
