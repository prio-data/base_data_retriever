import os

from contextlib import closing

from sqlalchemy import create_engine,MetaData,Table
from sqlalchemy.orm import sessionmaker
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

meta = MetaData()
engine = create_engine("postgresql+psycopg2://",creator=getconn)
Session = sessionmaker(bind=engine)

Pgm = Table(env("DB_PRIOGRID_VIEW","pgm_test"),meta,autoload_with=engine)

def retrieve(start_date,end_date,columns=[]):
    startmid,endmid = (cast_date_to_mid(d) for d in (start_date,end_date))
    table_columns = [getattr(Pgm.columns,c) for c in ID_COLUMNS]

    for c in columns:
        try:
            table_columns.append(getattr(Pgm.columns,c))
        except AttributeError:
            return Response(status_code=400)

    with closing(Session()) as sess:
        res = (sess.query(*table_columns)
                .filter(Pgm.columns.month_id >= startmid,Pgm.columns.month_id <= endmid)
            )
    return res
