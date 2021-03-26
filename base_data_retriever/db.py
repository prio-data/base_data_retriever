import os
import requests

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from postgres_azure_certificate_auth import sec_con,AppConfig,KeyvaultSecrets
import settings

if settings.PROD:
    secrets = KeyvaultSecrets(settings.KEY_VAULT_URL)
    config = AppConfig(secrets["appconfig-connection-string"])

    dbname = config["dbname"]
    def get_con():
        return sec_con(secrets,config,dbname=dbname)
else:
    cert_rq = requests.get("https://www.digicert.com/CACerts/BaltimoreCyberTrustRoot.crt.pem")
    secrets = {
        "sslrootcert":cert_rq.content.decode(),
        "password":settings.DB_PASSWORD,
    }
    config = {
        "host":settings.DB_HOST,
        "user":settings.DB_USER,
    }

    def get_con():
        return sec_con(secrets,config,auth="password",dbname=settings.DB_NAME)

engine = create_engine("postgresql://",creator=get_con)
Base = declarative_base()
Session = sessionmaker(bind=engine)
